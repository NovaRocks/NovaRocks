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
//! `FragmentDispatcher`. `RemoteDispatcher` routes per-instance to BEs over
//! gRPC.
//!
//! At a single backend (all-in-one / 1FE+1BE), the scheduler produces one
//! instance per fragment and this path reproduces the prior single-instance
//! wiring exactly.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::app_config::PlanWireFormat;
use crate::common::ids::SlotId;
use crate::exec::chunk::schema_thrift::chunk_slot_schema_from_type_desc;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::novarocks_logging::debug;
use crate::runtime::dispatcher::{FetchOutcome, FragmentDispatcher, FragmentSubmission};
use crate::runtime::exec_params::{ExecPlanFragmentParamOptions, build_exec_plan_fragment_params};
use crate::runtime::query_options::QueryOptions;
use crate::runtime::query_state::QueryState;
use crate::runtime::runtime_filter_params::RuntimeFilterParams;
use crate::runtime::scheduler::{
    FragmentInstancePlacement, FragmentScheduler, topological_sort_bottom_up,
};
use crate::runtime::write_coordinator::{
    WriteAbortInput, WriteCommitInput, WriteCoordinator, WriterKey, register_query,
    unregister_query,
};
use crate::sql::analysis::cte::CteId;
use crate::sql::codegen::{
    FragmentEdge, FragmentEdgeKind, FragmentId, LoweredFragmentEdge, MultiFragmentBuildResult,
    RuntimeFilterPlanResult,
};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::{DataPartition, PartitionKind};
use crate::thrift::data_sinks;
use crate::thrift::partitions;
use crate::thrift::planner;
use crate::thrift::types;

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
    pub(crate) fragment_profiles: Vec<crate::thrift::runtime_profile::TRuntimeProfileTree>,
}

pub(crate) struct NativePlanSidecars {
    pub(crate) fragments_by_id:
        std::collections::BTreeMap<FragmentId, crate::proto::plan::PlanFragment>,
}

#[derive(Clone)]
struct CompatCteConsumer {
    fragment_id: FragmentId,
    exchange_node_id: i32,
    partition: partitions::TDataPartition,
    output_slot_ids: Vec<i32>,
}

type FragmentEdgeKey = (FragmentId, FragmentId, i32);

pub(crate) fn prepare_native_plan_sidecars(
    native_plan: &crate::sql::planner::DistributedPlan,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<NativePlanSidecars, String> {
    let encoded = crate::sql::codegen::proto_encode::plan::encode_distributed_plan_with_context(
        native_plan,
        crate::sql::codegen::proto_encode::plan::NativePlanEncodeContext { mv_refresh_ctx },
    )?;
    let mut fragments_by_id = BTreeMap::new();
    for fragment in encoded.fragments {
        if fragments_by_id
            .insert(fragment.fragment_id, fragment)
            .is_some()
        {
            return Err("native DistributedPlan encoded duplicate fragment ids".to_string());
        }
    }
    Ok(NativePlanSidecars { fragments_by_id })
}

pub(crate) fn prepare_native_plan_sidecars_for_current_wire_format(
    native_plan: &crate::sql::planner::DistributedPlan,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<Option<NativePlanSidecars>, String> {
    match current_plan_wire_format() {
        #[cfg(feature = "compat")]
        PlanWireFormat::Thrift => Ok(None),
        PlanWireFormat::Proto => {
            prepare_native_plan_sidecars(native_plan, mv_refresh_ctx).map(Some)
        }
    }
}

/// Coordinates multi-fragment query execution across one or more backends.
///
/// Drives all fragment wiring from [`FragmentScheduler`] placements and submits
/// every instance through the `FragmentDispatcher`. Results are collected by
/// polling the dispatcher for the root fragment's chunks.
pub(crate) struct ExecutionCoordinator {
    build_result: MultiFragmentBuildResult,
    native_sidecars: Option<NativePlanSidecars>,
    dispatcher: Arc<dyn FragmentDispatcher>,
    scheduler: Arc<FragmentScheduler>,
    query_options: Option<QueryOptions>,
}

impl ExecutionCoordinator {
    pub(crate) fn new(
        build_result: MultiFragmentBuildResult,
        dispatcher: Arc<dyn FragmentDispatcher>,
        scheduler: Arc<FragmentScheduler>,
        query_options: Option<QueryOptions>,
    ) -> Self {
        Self {
            build_result,
            native_sidecars: None,
            dispatcher,
            scheduler,
            query_options,
        }
    }

    pub(crate) fn new_with_native_plan_sidecars(
        build_result: MultiFragmentBuildResult,
        native_sidecars: NativePlanSidecars,
        dispatcher: Arc<dyn FragmentDispatcher>,
        scheduler: Arc<FragmentScheduler>,
        query_options: Option<QueryOptions>,
    ) -> Self {
        Self::new_with_optional_native_plan_sidecars(
            build_result,
            Some(native_sidecars),
            dispatcher,
            scheduler,
            query_options,
        )
    }

    pub(crate) fn new_with_optional_native_plan_sidecars(
        build_result: MultiFragmentBuildResult,
        native_sidecars: Option<NativePlanSidecars>,
        dispatcher: Arc<dyn FragmentDispatcher>,
        scheduler: Arc<FragmentScheduler>,
        query_options: Option<QueryOptions>,
    ) -> Self {
        Self {
            build_result,
            native_sidecars,
            dispatcher,
            scheduler,
            query_options,
        }
    }

    pub(crate) fn execute_with_write_outcome(self) -> Result<CoordinatedQueryResult, String> {
        self.execute_with_profile_collection(false)
    }

    pub(crate) fn execute_with_profile_outcome(self) -> Result<CoordinatedQueryResult, String> {
        self.execute_with_profile_collection(true)
    }

    fn execute_with_profile_collection(
        self,
        collect_profiles: bool,
    ) -> Result<CoordinatedQueryResult, String> {
        let MultiFragmentBuildResult {
            mut fragment_results,
            root_fragment_id,
            edges,
            lowered_edges,
            boundary_schemas: _,
            rf_plan,
        } = self.build_result;
        let native_sidecars = self.native_sidecars;
        let query_options = self.query_options;
        let dispatcher = self.dispatcher;
        let scheduler = self.scheduler;
        let plan_wire_format = current_plan_wire_format();
        let native_fragments_by_id =
            native_fragment_sidecars(plan_wire_format, native_sidecars.as_ref())?;
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
                    FragmentEdgeKind::IcebergChangeStreamRouter { .. } => {
                        "IcebergChangeStreamRouter"
                    }
                },
                partition_type_for_data_partition(&e.output_partition),
            );
        }

        let live = scheduler.live_backend_entries().to_vec();
        let mut plan =
            scheduler.assign_with_live(&fragment_results, &edges, query_id.clone(), &live)?;
        scheduler.fill_destinations_with_live(&mut plan, &edges, &live)?;
        if let Some(rf) = rf_plan.as_ref() {
            scheduler.fill_runtime_filter_params_with_live(&mut plan, rf, &live)?;
        }
        scheduler.fill_per_exch_num_senders(&mut plan, &edges);
        let execution_root_fragment_id = plan.root_fragment_id;
        let compat_partition_by_edge = build_compat_partition_by_edge(&edges, lowered_edges)?;

        // ---------------------------------------------------------------
        // 2. Build per-edge / CTE consumer indices used for sink wiring.
        // ---------------------------------------------------------------
        // Stream producer fragment id -> its single outgoing plain stream edge.
        let stream_edge_by_source = build_stream_edge_by_source(&edges)?;
        let router_edge_groups = group_router_edges_by_source(&edges)?;
        let mut router_edges_by_source: BTreeMap<FragmentId, (i32, Vec<&FragmentEdge>)> =
            BTreeMap::new();
        for ((source_fragment_id, router_group_id), branch_edges) in router_edge_groups {
            if router_edges_by_source
                .insert(source_fragment_id, (router_group_id, branch_edges))
                .is_some()
            {
                return Err(format!(
                    "fragment {source_fragment_id} has multiple Iceberg change-stream router groups; \
                     one source fragment can only use one router sink template"
                ));
            }
        }
        // CTE id -> native consumer sidecars: (consumer_fragment_id, exchange_node_id,
        // native partition, output_slot_ids, logical producer column ids).
        let mut cte_consumers: BTreeMap<
            CteId,
            Vec<(
                FragmentId,
                i32,
                crate::proto::plan::DataPartition,
                Vec<i32>,
                Vec<ColumnId>,
            )>,
        > = BTreeMap::new();
        // CTE id -> compat thrift consumers used only for TMultiCastDataStreamSink.
        let mut compat_cte_consumers: BTreeMap<CteId, Vec<CompatCteConsumer>> = BTreeMap::new();

        for e in &edges {
            match &e.edge_kind {
                FragmentEdgeKind::Stream => {}
                FragmentEdgeKind::CteMulticast {
                    cte_id,
                    receive_producer_column_ids,
                } => {
                    let native_partition =
                        crate::sql::codegen::proto_encode::plan::encode_data_partition(
                            &e.output_partition,
                        )?;
                    cte_consumers.entry(*cte_id).or_default().push((
                        e.target_fragment_id,
                        e.target_exchange_node_id,
                        native_partition,
                        e.output_slot_ids.clone(),
                        receive_producer_column_ids.clone(),
                    ));
                    compat_cte_consumers
                        .entry(*cte_id)
                        .or_default()
                        .push(CompatCteConsumer {
                            fragment_id: e.target_fragment_id,
                            exchange_node_id: e.target_exchange_node_id,
                            partition: compat_partition_for_edge(&compat_partition_by_edge, e)?,
                            output_slot_ids: e.output_slot_ids.clone(),
                        });
                }
                FragmentEdgeKind::IcebergChangeStreamRouter { .. } => {}
            }
        }
        // CTE consumers may also be expressed via `cte_exchange_nodes` on the
        // consumer fragment when no explicit edge carries them.
        for fr in &fragment_results {
            for (cte_id, exchange_node_id, receive_producer_column_ids) in &fr.cte_exchange_nodes {
                let consumers = cte_consumers.entry(*cte_id).or_default();
                if !consumers
                    .iter()
                    .any(|(fid, nid, _, _, _)| *fid == fr.fragment_id && *nid == *exchange_node_id)
                {
                    consumers.push((
                        fr.fragment_id,
                        *exchange_node_id,
                        crate::proto::plan::DataPartition {
                            kind: crate::proto::plan::PartitionKind::Unpartitioned as i32,
                            exprs: Vec::new(),
                        },
                        Vec::new(),
                        receive_producer_column_ids.clone(),
                    ));
                }
                let compat_consumers = compat_cte_consumers.entry(*cte_id).or_default();
                if !compat_consumers.iter().any(|consumer| {
                    consumer.fragment_id == fr.fragment_id
                        && consumer.exchange_node_id == *exchange_node_id
                }) {
                    compat_consumers.push(CompatCteConsumer {
                        fragment_id: fr.fragment_id,
                        exchange_node_id: *exchange_node_id,
                        partition: unpartitioned_partition(),
                        output_slot_ids: Vec::new(),
                    });
                }
            }
        }

        // ---------------------------------------------------------------
        // 3. Inject the designated runtime-filter merge node into descriptors.
        // ---------------------------------------------------------------
        // The merge node is the backend that hosts the execution anchor. For
        // result roots this is the single root instance; for write-only DAGs it
        // is the first instance of the selected writer anchor.
        let merge_addr = backend_to_network_addr(&live, plan.root_backend_idx)?;
        if rf_plan.is_some() {
            inject_runtime_filter_merge_nodes(&mut fragment_results, &merge_addr);
        }

        // ---------------------------------------------------------------
        // 4. Translate every placement into a fragment params and submit.
        // ---------------------------------------------------------------
        // Honor a per-session `SET pipeline_dop = N` override; 0/None
        // means auto (cores/2).
        let session_dop = query_options
            .as_ref()
            .and_then(|opts| opts.pipeline_dop)
            .unwrap_or(0);
        let pipeline_dop = crate::runtime::dispatcher::compute_pipeline_dop(session_dop);
        let needs_fragment_status_report =
            dispatcher.needs_fragment_status_report() || collect_profiles;
        let mut novarocks_report_addr: Option<types::TNetworkAddress> = None;
        let mut novarocks_report_endpoint: Option<crate::runtime::endpoint::RuntimeEndpoint> = None;

        // Snapshot the per-consumer-fragment instance destinations for CTE
        // multicast sub-sinks (each consumer fans out to all of its instances).
        let consumer_dests: BTreeMap<
            FragmentId,
            Vec<crate::runtime::endpoint::FragmentDestination>,
        > = plan
            .by_fragment
            .iter()
            .map(|(fid, insts)| {
                let dests = insts
                    .iter()
                    .map(|inst| {
                        crate::runtime::endpoint::FragmentDestination::new(
                            inst.finst_id.clone(),
                            inst.endpoint.clone(),
                        )
                    })
                    .collect();
                (*fid, dests)
            })
            .collect();

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
        let mut submissions_by_fragment: BTreeMap<FragmentId, Vec<(usize, FragmentSubmission)>> =
            BTreeMap::new();
        let mut expected_writers = Vec::new();

        for (&fragment_id, placements) in &plan.by_fragment {
            let fr = *fr_by_id
                .get(&fragment_id)
                .ok_or_else(|| format!("fragment {fragment_id} missing from build results"))?;
            let is_root = fragment_id == execution_root_fragment_id;
            let stream_edge = stream_edge_by_source.get(&fragment_id).copied();
            let router_edges = router_edges_by_source.get(&fragment_id);
            let is_terminal_write = stream_edge.is_none()
                && router_edges.is_none()
                && fr.cte_id.is_none()
                && fr.output_kind.is_terminal_write();

            // Classify the fragment once.
            if !is_root
                && !is_terminal_write
                && fr.cte_id.is_none()
                && stream_edge.is_none()
                && router_edges.is_none()
            {
                return Err(format!(
                    "fragment {fragment_id} is neither root, CTE producer, stream producer, nor \
                     Iceberg change-stream router producer or terminal write fragment; \
                     stream fan-out is not supported in standalone coordinator"
                ));
            }
            ensure_native_sidecar_sink_supported(
                plan_wire_format,
                fragment_id,
                is_root,
                is_terminal_write,
                stream_edge.is_some(),
                router_edges.is_some(),
                fr.cte_id.is_some(),
            )?;

            for placement in placements {
                // Build the output sink for this fragment class.
                let (output_sink, fragment_partition, exec_destinations) = if is_root {
                    (fr.output_sink.clone(), unpartitioned_partition(), None)
                } else if let Some(edge) = stream_edge {
                    let compat_partition =
                        compat_partition_for_edge(&compat_partition_by_edge, edge)?;
                    let stream_sink = build_stream_sink_for_edge(edge, compat_partition.clone());
                    let output_sink = wrap_data_stream_sink(stream_sink);
                    let exec_destinations = placement
                        .destinations
                        .iter()
                        .map(exec_destination_from_runtime)
                        .collect();
                    (output_sink, compat_partition, Some(exec_destinations))
                } else if let Some((router_group_id, branch_edges)) = router_edges {
                    let template = fr
                        .output_sink
                        .iceberg_change_stream_router_sink
                        .as_ref()
                        .ok_or_else(|| {
                            format!(
                                "fragment {fragment_id} is router source for group {router_group_id} \
                                 but output sink is missing ICEBERG_CHANGE_STREAM_ROUTER_SINK payload"
                            )
                        })?;
                    let output_sink = wrap_iceberg_change_stream_router_sink(
                        template,
                        branch_edges,
                        &compat_partition_by_edge,
                        &plan.by_fragment,
                    )?;
                    // Router branches carry their own destinations in the sink payload.
                    (output_sink, unpartitioned_partition(), None)
                } else if is_terminal_write {
                    (fr.output_sink.clone(), unpartitioned_partition(), None)
                } else {
                    // CTE producer.
                    let cte_id = fr
                        .cte_id
                        .ok_or_else(|| "CTE fragment missing cte_id".to_string())?;
                    let consumers = compat_cte_consumers
                        .get(&cte_id)
                        .cloned()
                        .unwrap_or_default();
                    if consumers.is_empty() {
                        return Err(format!("CTE fragment (cte_id={cte_id}) has no consumers"));
                    }
                    let mut sinks = Vec::with_capacity(consumers.len());
                    let mut destinations = Vec::with_capacity(consumers.len());
                    for consumer in &consumers {
                        let stream_sink = data_sinks::TDataStreamSink::new(
                            consumer.exchange_node_id,
                            consumer.partition.clone(),
                            None::<bool>,
                            None::<bool>,
                            None::<i32>,
                            stream_sink_output_columns(&consumer.output_slot_ids),
                            None::<i64>,
                        );
                        sinks.push(stream_sink);
                        let dests = consumer_dests
                            .get(&consumer.fragment_id)
                            .ok_or_else(|| {
                                format!(
                                    "CTE consumer fragment {} has no placements",
                                    consumer.fragment_id
                                )
                            })?
                            .iter()
                            .map(exec_destination_from_runtime)
                            .collect();
                        destinations.push(dests);
                    }
                    let multi_cast_sink =
                        data_sinks::TMultiCastDataStreamSink::new(sinks, destinations);
                    let output_sink = wrap_multi_cast_sink(multi_cast_sink);
                    // Multicast carries its own destinations on the sub-sinks.
                    (output_sink, unpartitioned_partition(), None)
                };

                let fragment_has_write_sink = is_terminal_write;
                let (fragment_report_addr, fragment_report_endpoint) =
                    if fragment_has_write_sink || needs_fragment_status_report {
                        if novarocks_report_addr.is_none() || novarocks_report_endpoint.is_none() {
                            let endpoint = local_coordinator_report_endpoint()?;
                            novarocks_report_addr = Some(endpoint.to_network_address());
                            novarocks_report_endpoint = Some(endpoint);
                        }
                        (
                            novarocks_report_addr.clone(),
                            novarocks_report_endpoint.clone(),
                        )
                    } else {
                        (None, None)
                    };
                // Align with StarRocks: a write/data-sink fragment (iceberg/hive/olap load/insert)
                // runs at the lower sink DOP curve so it doesn't starve query CPU; compute fragments
                // keep cores/2 (`pipeline_dop`). A `SET pipeline_dop = N` override pins both.
                let fragment_dop = if fragment_has_write_sink {
                    crate::runtime::dispatcher::compute_sink_pipeline_dop(session_dop)
                } else {
                    pipeline_dop
                };

                let thrift_fragment = planner::TPlanFragment::new(
                    Some(fr.plan.clone()),
                    fr.output_exprs.clone(),
                    Some(output_sink),
                    fragment_partition,
                    None::<i64>,
                    None::<i64>,
                    fr.query_global_dicts.clone(),
                    None::<Vec<crate::thrift::data::TGlobalDict>>,
                    None::<planner::TCacheParam>,
                    fr.query_global_dict_exprs.clone(),
                    None::<planner::TGroupExecutionParam>,
                );

                let mut exec_params = fr.exec_params.clone();
                exec_params.query_id = query_id.clone();
                exec_params.fragment_instance_id = placement.finst_id.clone();
                exec_params.per_node_scan_ranges =
                    compat_scan_ranges_for_placement(fr, placement, placements.len())?;
                exec_params.per_exch_num_senders = placement.per_exch_num_senders.clone();
                exec_params.destinations = exec_destinations;
                if let Some(rf) = rf_plan.as_ref() {
                    let rf_params = build_instance_runtime_filter_params(
                        rf,
                        &placement.runtime_filter_prober_params,
                        &instance_counts,
                    );
                    exec_params.runtime_filter_params = Some(rf_params.to_thrift());
                }

                let compat_query_options = query_options.as_ref().map(QueryOptions::to_thrift);
                let params = build_exec_plan_fragment_params(
                    fr,
                    thrift_fragment,
                    exec_params,
                    compat_query_options,
                    fragment_dop,
                    ExecPlanFragmentParamOptions {
                        backend_num: Some(placement.instance_index as i32),
                        novarocks_report_addr: fragment_report_addr,
                        novarocks_typed_result_sink: is_root && needs_fragment_status_report,
                    },
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

                let submission = match plan_wire_format {
                    #[cfg(feature = "compat")]
                    PlanWireFormat::Thrift => FragmentSubmission::thrift_only(params),
                    PlanWireFormat::Proto => {
                        let mut native_fragment =
                            native_fragments_by_id.get(&fragment_id).cloned().ok_or_else(
                                || {
                                    format!(
                                        "native plan sidecar missing fragment {fragment_id} while plan_wire_format=proto"
                                    )
                                },
                            )?;
                        if !is_root && !is_terminal_write && stream_edge.is_none() {
                            if let Some((router_group_id, branch_edges)) = router_edges {
                                patch_native_iceberg_change_stream_router_sink(
                                    &mut native_fragment,
                                    fragment_id,
                                    *router_group_id,
                                    branch_edges,
                                    &plan.by_fragment,
                                )?;
                            } else if let Some(cte_id) = fr.cte_id {
                                let consumers =
                                    cte_consumers.get(&cte_id).cloned().unwrap_or_default();
                                patch_native_cte_multicast_sink(
                                    &mut native_fragment,
                                    fragment_id,
                                    cte_id,
                                    &consumers,
                                    &consumer_dests,
                                )?;
                            }
                        }
                        params.params.as_ref().ok_or_else(|| {
                            format!(
                                "fragment {fragment_id} missing exec params while plan_wire_format=proto"
                            )
                        })?;
                        let native_rf_builder_number = runtime_filter_builder_number_for_instance(
                            rf_plan.as_ref(),
                            &instance_counts,
                        );
                        let native_rf_max_size = if rf_plan.is_some() {
                            16_i64 * 1024 * 1024
                        } else {
                            0
                        };
                        let native_instance_params =
                            crate::sql::codegen::proto_encode::instance::encode_instance_params(
                                &query_id,
                                placement,
                                query_options.as_ref(),
                                &placement.runtime_filter_prober_params,
                                &native_rf_builder_number,
                                native_rf_max_size,
                                placement.instance_index as i32,
                                fragment_report_endpoint.as_ref(),
                                params.novarocks_typed_result_sink.unwrap_or(false),
                            )?;
                        FragmentSubmission::with_native(
                            params,
                            native_fragment,
                            native_instance_params,
                        )
                    }
                };

                submissions_by_fragment
                    .entry(fragment_id)
                    .or_default()
                    .push((placement.backend_idx, submission));
            }
        }

        if !submissions_by_fragment.contains_key(&execution_root_fragment_id) {
            return Err("root fragment produced no placement".to_string());
        }
        let mut submissions: Vec<(usize, FragmentSubmission)> = Vec::new();
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
        let root_fragment = fr_by_id
            .get(&execution_root_fragment_id)
            .ok_or_else(|| "root fragment not found in build results".to_string())?;
        let expected_root_chunk_schema =
            if root_uses_typed_result_sink(&submissions, &plan.root_finst_id)? {
                Some(build_root_expected_chunk_schema(root_fragment)?)
            } else {
                None
            };

        let fetch_result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            submissions,
            plan.root_backend_idx,
            plan.root_finst_id.clone(),
            &query_id,
            timeout_ms,
            expected_root_chunk_schema.as_ref(),
            write_coordinator.as_ref(),
            collect_profiles,
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

        let chunks = align_fetch_chunks_to_output_columns(
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
            fragment_profiles: fetch_result.fragment_profiles,
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

fn build_root_expected_chunk_schema(
    root_fragment: &crate::sql::codegen::FragmentBuildResult,
) -> Result<ChunkSchemaRef, String> {
    let output_columns = &root_fragment.output_columns;
    if output_columns.is_empty() {
        if root_fragment
            .output_exprs
            .as_ref()
            .is_some_and(|exprs| !exprs.is_empty())
        {
            return Err(
                "root typed result metadata mismatch: output_exprs present for zero output columns"
                    .to_string(),
            );
        }
        return Ok(Arc::new(ChunkSchema::empty()));
    }

    let output_exprs = root_fragment
        .output_exprs
        .as_ref()
        .ok_or_else(|| "root typed result requires output_exprs metadata".to_string())?;
    if output_exprs.len() != output_columns.len() {
        return Err(format!(
            "root typed result output expr count mismatch: exprs={} columns={}",
            output_exprs.len(),
            output_columns.len()
        ));
    }

    let mut output_specs = Vec::with_capacity(output_columns.len());
    for (idx, (expr, output)) in output_exprs.iter().zip(output_columns.iter()).enumerate() {
        if expr.nodes.len() != 1 {
            return Err(format!(
                "root typed result output expr {idx} must be a single SLOT_REF node, got {} nodes",
                expr.nodes.len()
            ));
        }
        let node = &expr.nodes[0];
        if node.node_type != crate::thrift::exprs::TExprNodeType::SLOT_REF {
            return Err(format!(
                "root typed result output expr {idx} must be SLOT_REF, got {:?}",
                node.node_type
            ));
        }
        let slot_ref = node
            .slot_ref
            .as_ref()
            .ok_or_else(|| format!("root typed result output expr {idx} missing slot_ref"))?;
        let slot_id = u32::try_from(slot_ref.slot_id).map_err(|_| {
            format!(
                "root typed result output expr {idx} has negative slot id {}",
                slot_ref.slot_id
            )
        })?;
        output_specs.push((
            SlotId::new(slot_id),
            output.name.clone(),
            output.nullable,
            node.type_.clone(),
        ));
    }

    let mut seen_slot_ids = BTreeSet::new();
    let has_duplicate_slot_ids = output_specs
        .iter()
        .any(|(slot_id, _, _, _)| !seen_slot_ids.insert(slot_id.as_u32()));

    let mut slots = Vec::with_capacity(output_specs.len());
    for (idx, (slot_id, name, nullable, type_desc)) in output_specs.into_iter().enumerate() {
        let output_slot_id = if has_duplicate_slot_ids {
            let positional = u32::try_from(idx + 1)
                .map_err(|_| "too many root typed result output columns".to_string())?;
            SlotId::new(positional)
        } else {
            slot_id
        };
        slots.push(
            chunk_slot_schema_from_type_desc(output_slot_id, name, nullable, type_desc, None)
                .map_err(|e| {
                    format!("build root typed result slot schema at index {idx} failed: {e}")
                })?,
        );
    }

    ChunkSchema::try_new(slots).map(Arc::new)
}

fn align_fetch_chunks_to_output_columns(
    chunks: Vec<Chunk>,
    output_columns: &[crate::sql::codegen::OutputColumn],
) -> Result<Vec<Chunk>, String> {
    chunks
        .into_iter()
        .map(|chunk| align_fetch_chunk_to_output_columns(chunk, output_columns))
        .collect()
}

fn align_fetch_chunk_to_output_columns(
    chunk: Chunk,
    output_columns: &[crate::sql::codegen::OutputColumn],
) -> Result<Chunk, String> {
    if chunk.batch.num_columns() != output_columns.len() {
        return Err(format!(
            "typed root result column count mismatch: chunk has {}, output metadata has {}",
            chunk.batch.num_columns(),
            output_columns.len()
        ));
    }
    if chunk.chunk_schema().slots().len() != output_columns.len() {
        return Err(format!(
            "typed root result slot count mismatch: chunk schema has {}, output metadata has {}",
            chunk.chunk_schema().slots().len(),
            output_columns.len()
        ));
    }

    let mut fields = Vec::with_capacity(output_columns.len());
    let mut arrays = Vec::with_capacity(output_columns.len());
    for (idx, output) in output_columns.iter().enumerate() {
        let array =
            align_typed_root_array(idx, chunk.batch.column(idx).clone(), &output.data_type)?;
        if let Err(mismatch) = crate::exec::chunk::type_compatibility::check_exact(
            &output.data_type,
            array.data_type(),
        ) {
            return Err(format!(
                "typed root result column {idx} type mismatch: output={:?} chunk={:?} ({:?})",
                output.data_type,
                array.data_type(),
                mismatch.kind
            ));
        }
        fields.push(Field::new(
            output.name.clone(),
            array.data_type().clone(),
            output.nullable || array.null_count() > 0,
        ));
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("align typed root result batch failed: {e}"))?;
    let chunk_schema = chunk
        .chunk_schema()
        .with_fields_in_order(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect(),
        )
        .map(Arc::new)?;
    Chunk::try_new_with_chunk_schema(batch, chunk_schema)
}

fn align_typed_root_array(
    idx: usize,
    array: ArrayRef,
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    if crate::exec::chunk::type_compatibility::check_exact(output_type, array.data_type()).is_ok() {
        return Ok(array);
    }
    if !same_unit_timestamp_metadata_mismatch(output_type, array.data_type()) {
        return Ok(array);
    }
    crate::exec::chunk::type_compatibility::retag_column(&array, output_type).map_err(|mismatch| {
        format!(
            "typed root result column {idx} timestamp metadata retag failed: output={:?} chunk={:?} ({:?})",
            output_type,
            array.data_type(),
            mismatch.kind
        )
    })
}

fn same_unit_timestamp_metadata_mismatch(expected: &DataType, actual: &DataType) -> bool {
    matches!(
        (expected, actual),
        (DataType::Timestamp(expected_unit, _), DataType::Timestamp(actual_unit, _))
            if expected_unit == actual_unit
    )
}

/// An `UNPARTITIONED` data partition (the common default).
fn unpartitioned_partition() -> partitions::TDataPartition {
    partitions::TDataPartition::new(
        partitions::TPartitionType::UNPARTITIONED,
        None::<Vec<crate::thrift::exprs::TExpr>>,
        None::<Vec<partitions::TRangePartition>>,
        None::<Vec<partitions::TBucketProperty>>,
    )
}

fn partition_type_for_data_partition(partition: &DataPartition) -> partitions::TPartitionType {
    match partition.kind {
        PartitionKind::Unpartitioned => partitions::TPartitionType::UNPARTITIONED,
        PartitionKind::Random => partitions::TPartitionType::RANDOM,
        PartitionKind::Hash => partitions::TPartitionType::HASH_PARTITIONED,
    }
}

fn fragment_edge_key(edge: &FragmentEdge) -> FragmentEdgeKey {
    (
        edge.source_fragment_id,
        edge.target_fragment_id,
        edge.target_exchange_node_id,
    )
}

fn build_compat_partition_by_edge(
    edges: &[FragmentEdge],
    lowered_edges: Vec<LoweredFragmentEdge>,
) -> Result<BTreeMap<FragmentEdgeKey, partitions::TDataPartition>, String> {
    let mut by_key = BTreeMap::new();
    for lowered in lowered_edges {
        let key = fragment_edge_key(&lowered.edge);
        if by_key.insert(key, lowered.compat_partition).is_some() {
            return Err(format!(
                "lowered edge sidecars contain duplicate edge source={} target={} exchange={}",
                key.0, key.1, key.2
            ));
        }
    }
    for edge in edges {
        let key = fragment_edge_key(edge);
        if !by_key.contains_key(&key) {
            return Err(format!(
                "fragment edge source={} target={} exchange={} is missing lowered compat partition",
                key.0, key.1, key.2
            ));
        }
    }
    Ok(by_key)
}

fn compat_partition_for_edge(
    compat_partition_by_edge: &BTreeMap<FragmentEdgeKey, partitions::TDataPartition>,
    edge: &FragmentEdge,
) -> Result<partitions::TDataPartition, String> {
    let key = fragment_edge_key(edge);
    compat_partition_by_edge.get(&key).cloned().ok_or_else(|| {
        format!(
            "fragment edge source={} target={} exchange={} is missing lowered compat partition",
            key.0, key.1, key.2
        )
    })
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
        None::<data_sinks::TIcebergChangeStreamRouterSink>,
    )
}

fn build_stream_sink_for_edge(
    edge: &FragmentEdge,
    compat_partition: partitions::TDataPartition,
) -> data_sinks::TDataStreamSink {
    data_sinks::TDataStreamSink::new(
        edge.target_exchange_node_id,
        compat_partition,
        None::<bool>,
        None::<bool>,
        None::<i32>,
        stream_sink_output_columns(&edge.output_slot_ids),
        None::<i64>,
    )
}

fn stream_sink_output_columns(output_slot_ids: &[i32]) -> Option<Vec<i32>> {
    if output_slot_ids.is_empty() {
        None
    } else {
        Some(output_slot_ids.to_vec())
    }
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
        None::<data_sinks::TIcebergChangeStreamRouterSink>,
    )
}

fn build_stream_edge_by_source<'a>(
    edges: &'a [FragmentEdge],
) -> Result<BTreeMap<FragmentId, &'a FragmentEdge>, String> {
    let router_sources: BTreeSet<FragmentId> = edges
        .iter()
        .filter_map(|edge| {
            matches!(
                edge.edge_kind,
                FragmentEdgeKind::IcebergChangeStreamRouter { .. }
            )
            .then_some(edge.source_fragment_id)
        })
        .collect();
    let mut stream_edge_by_source = BTreeMap::new();
    for edge in edges {
        if !matches!(edge.edge_kind, FragmentEdgeKind::Stream) {
            continue;
        }
        if router_sources.contains(&edge.source_fragment_id) {
            return Err(format!(
                "fragment {} has both plain Stream and Iceberg change-stream router edges",
                edge.source_fragment_id
            ));
        }
        if stream_edge_by_source
            .insert(edge.source_fragment_id, edge)
            .is_some()
        {
            return Err(format!(
                "fragment {} has multiple outgoing stream edges; stream fan-out is not supported",
                edge.source_fragment_id
            ));
        }
    }
    Ok(stream_edge_by_source)
}

fn group_router_edges_by_source<'a>(
    edges: &'a [FragmentEdge],
) -> Result<BTreeMap<(FragmentId, i32), Vec<&'a FragmentEdge>>, String> {
    let stream_sources: BTreeSet<FragmentId> = edges
        .iter()
        .filter_map(|edge| {
            matches!(edge.edge_kind, FragmentEdgeKind::Stream).then_some(edge.source_fragment_id)
        })
        .collect();
    let mut grouped: BTreeMap<(FragmentId, i32), Vec<&FragmentEdge>> = BTreeMap::new();
    let mut branch_ids_by_group: BTreeMap<(FragmentId, i32), BTreeSet<i32>> = BTreeMap::new();
    let mut branch_kinds_by_group = BTreeMap::new();
    let mut target_exchanges_by_group = BTreeMap::new();

    for edge in edges {
        let FragmentEdgeKind::IcebergChangeStreamRouter {
            router_group_id,
            branch_id,
            branch_kind,
        } = edge.edge_kind
        else {
            continue;
        };
        if stream_sources.contains(&edge.source_fragment_id) {
            return Err(format!(
                "fragment {} has both plain Stream and Iceberg change-stream router edges",
                edge.source_fragment_id
            ));
        }
        let key = (edge.source_fragment_id, router_group_id);
        if !branch_ids_by_group
            .entry(key)
            .or_default()
            .insert(branch_id)
        {
            return Err(format!(
                "Iceberg change-stream router group source={} group={} repeats branch_id {}",
                edge.source_fragment_id, router_group_id, branch_id
            ));
        }
        if !branch_kinds_by_group
            .entry(key)
            .or_insert_with(BTreeSet::new)
            .insert(branch_kind)
        {
            return Err(format!(
                "Iceberg change-stream router group source={} group={} repeats branch_kind {:?}",
                edge.source_fragment_id, router_group_id, branch_kind
            ));
        }
        let target_exchange = (edge.target_fragment_id, edge.target_exchange_node_id);
        if !target_exchanges_by_group
            .entry(key)
            .or_insert_with(BTreeSet::new)
            .insert(target_exchange)
        {
            return Err(format!(
                "Iceberg change-stream router group source={} group={} repeats target exchange \
                 fragment={} node={}",
                edge.source_fragment_id,
                router_group_id,
                edge.target_fragment_id,
                edge.target_exchange_node_id
            ));
        }
        grouped.entry(key).or_default().push(edge);
    }

    Ok(grouped)
}

fn wrap_iceberg_change_stream_router_sink(
    template: &data_sinks::TIcebergChangeStreamRouterSink,
    branch_edges: &[&FragmentEdge],
    compat_partition_by_edge: &BTreeMap<FragmentEdgeKey, partitions::TDataPartition>,
    placements: &BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
) -> Result<data_sinks::TDataSink, String> {
    if branch_edges.is_empty() {
        return Err("Iceberg change-stream router sink has no branch edges".to_string());
    }

    let mut branches = Vec::with_capacity(branch_edges.len());
    for edge in branch_edges {
        let FragmentEdgeKind::IcebergChangeStreamRouter {
            router_group_id,
            branch_id,
            branch_kind,
        } = edge.edge_kind
        else {
            return Err(format!(
                "fragment {} edge to fragment {} is not an Iceberg change-stream router edge",
                edge.source_fragment_id, edge.target_fragment_id
            ));
        };
        let template_branch = template
            .branches
            .iter()
            .find(|branch| {
                branch.branch_id == branch_id
                    && crate::sql::common::branch_kind_from_thrift(branch.branch_kind)
                        .is_ok_and(|template_kind| template_kind == branch_kind)
            })
            .ok_or_else(|| {
                format!(
                    "Iceberg change-stream router source={} group={} branch_id={} branch_kind={:?} \
                     has no matching template branch",
                    edge.source_fragment_id, router_group_id, branch_id, branch_kind
                )
            })?;

        let mut stream_sink = template_branch.stream_sink.clone();
        stream_sink.dest_node_id = edge.target_exchange_node_id;
        stream_sink.output_partition = compat_partition_for_edge(compat_partition_by_edge, edge)?;
        let destinations = placements
            .get(&edge.target_fragment_id)
            .ok_or_else(|| {
                format!(
                    "Iceberg change-stream router source={} group={} branch_id={} target fragment {} \
                     has no placements",
                    edge.source_fragment_id,
                    router_group_id,
                    branch_id,
                    edge.target_fragment_id
                )
            })?
            .iter()
            .map(|placement| {
                let destination = crate::runtime::endpoint::FragmentDestination::new(
                    placement.finst_id.clone(),
                    placement.endpoint.clone(),
                );
                exec_destination_from_runtime(&destination)
            })
            .collect();

        branches.push(data_sinks::TIcebergChangeStreamRouterBranch::new(
            branch_id,
            template_branch.branch_kind,
            stream_sink,
            destinations,
        ));
    }

    let router_sink = data_sinks::TIcebergChangeStreamRouterSink::new(
        template.change_op_slot_id,
        template.data_route_slot_id,
        branches,
    );

    Ok(data_sinks::TDataSink::new(
        data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK,
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
        Some(router_sink),
    ))
}

fn compat_scan_ranges_for_placement(
    fragment: &crate::sql::codegen::FragmentBuildResult,
    placement: &FragmentInstancePlacement,
    placement_count: usize,
) -> Result<BTreeMap<i32, Vec<crate::thrift::internal_service::TScanRangeParams>>, String> {
    let mut assigned =
        crate::runtime::scan_range::thrift_scan_range_map_from_native(&placement.scan_ranges)?;

    let compat_ranges = &fragment.exec_params.per_node_scan_ranges;
    if compat_ranges.is_empty() {
        return Ok(assigned);
    }
    if placement_count == 0 {
        return Err(format!(
            "fragment {} has scan ranges but no placements",
            fragment.fragment_id
        ));
    }

    for (node_id, ranges) in compat_ranges {
        if assigned.contains_key(node_id) {
            continue;
        }
        assigned.insert(
            *node_id,
            ranges
                .iter()
                .enumerate()
                .filter_map(|(idx, range)| {
                    (idx % placement_count == placement.instance_index).then_some(range.clone())
                })
                .collect::<Vec<_>>(),
        );
    }
    Ok(assigned)
}

fn is_write_sink(params: &crate::thrift::internal_service::TExecPlanFragmentParams) -> bool {
    params
        .fragment
        .as_ref()
        .and_then(|fragment| fragment.output_sink.as_ref())
        .map(compat_data_sink_requires_write_report)
        .unwrap_or(false)
}

fn compat_data_sink_requires_write_report(sink: &data_sinks::TDataSink) -> bool {
    matches!(
        sink.type_,
        data_sinks::TDataSinkType::ICEBERG_TABLE_SINK
            | data_sinks::TDataSinkType::ICEBERG_DELETE_SINK
            | data_sinks::TDataSinkType::ICEBERG_DV_SINK
            | data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK
            | data_sinks::TDataSinkType::HIVE_TABLE_SINK
            | data_sinks::TDataSinkType::OLAP_TABLE_SINK
    )
}

fn uses_result_buffer_sink(
    params: &crate::thrift::internal_service::TExecPlanFragmentParams,
) -> bool {
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
    submissions: &[(usize, FragmentSubmission)],
    root_finst_id: &types::TUniqueId,
) -> Result<bool, String> {
    let root = submissions
        .iter()
        .map(|(_, submission)| submission.thrift_params())
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

fn root_uses_typed_result_sink(
    submissions: &[(usize, FragmentSubmission)],
    root_finst_id: &types::TUniqueId,
) -> Result<bool, String> {
    let root = submissions
        .iter()
        .map(|(_, submission)| submission.thrift_params())
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
    Ok(root.novarocks_typed_result_sink.unwrap_or(false))
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

fn local_coordinator_report_endpoint() -> Result<crate::runtime::endpoint::RuntimeEndpoint, String>
{
    let addr = local_coordinator_report_addr()?;
    crate::runtime::endpoint::RuntimeEndpoint::from_network_address(&addr)
}

fn current_plan_wire_format() -> PlanWireFormat {
    crate::novarocks_config::config()
        .map(|cfg| cfg.runtime.plan_wire_format)
        .unwrap_or(PlanWireFormat::Proto)
}

fn ensure_native_sidecar_sink_supported(
    wire_format: PlanWireFormat,
    fragment_id: FragmentId,
    is_root: bool,
    is_terminal_write: bool,
    has_stream_edge: bool,
    has_router_edges: bool,
    has_cte_id: bool,
) -> Result<(), String> {
    if wire_format != PlanWireFormat::Proto
        || is_root
        || is_terminal_write
        || has_stream_edge
        || has_router_edges
        || has_cte_id
    {
        return Ok(());
    }

    let dynamic_sink = "dynamic fragment sink";
    Err(format!(
        "plan_wire_format=proto cannot encode {dynamic_sink} for fragment {fragment_id}; \
         the native sink contract must carry dynamic destinations before this fragment can use proto submission"
    ))
}

fn native_fragment_sidecars(
    wire_format: PlanWireFormat,
    native_sidecars: Option<&NativePlanSidecars>,
) -> Result<BTreeMap<FragmentId, crate::proto::plan::PlanFragment>, String> {
    match wire_format {
        #[cfg(feature = "compat")]
        PlanWireFormat::Thrift => Ok(BTreeMap::new()),
        PlanWireFormat::Proto => {
            let native_sidecars = native_sidecars.ok_or_else(|| {
                "plan_wire_format=proto requires native sidecars encoded from a native DistributedPlan, but this execution path only supplied thrift fragments".to_string()
            })?;
            Ok(native_sidecars.fragments_by_id.clone())
        }
    }
}

fn patch_native_iceberg_change_stream_router_sink(
    fragment: &mut crate::proto::plan::PlanFragment,
    fragment_id: FragmentId,
    router_group_id: i32,
    branch_edges: &[&FragmentEdge],
    placements: &BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
) -> Result<(), String> {
    if branch_edges.is_empty() {
        return Err("native Iceberg change-stream router sink has no branch edges".to_string());
    }
    let router = match fragment.sink.as_mut().and_then(|sink| sink.kind.as_mut()) {
        Some(crate::proto::plan::data_sink::Kind::IcebergChangeStreamRouter(router)) => router,
        _ => {
            return Err(format!(
                "fragment {fragment_id} is router source for group {router_group_id} but native \
                 sidecar is missing ICEBERG_CHANGE_STREAM_ROUTER_SINK"
            ));
        }
    };

    for edge in branch_edges {
        let FragmentEdgeKind::IcebergChangeStreamRouter {
            router_group_id: edge_group_id,
            branch_id,
            branch_kind,
        } = edge.edge_kind
        else {
            return Err(format!(
                "fragment {} edge to fragment {} is not an Iceberg change-stream router edge",
                edge.source_fragment_id, edge.target_fragment_id
            ));
        };
        if edge_group_id != router_group_id {
            return Err(format!(
                "native Iceberg change-stream router source={} expected group={} but edge uses group={}",
                fragment_id, router_group_id, edge_group_id
            ));
        }

        let route = router
            .branches
            .iter_mut()
            .find(|route| {
                route.branch_id == branch_id
                    && native_change_stream_branch_kind(route.branch_kind)
                        .is_ok_and(|route_kind| route_kind == branch_kind)
            })
            .ok_or_else(|| {
                format!(
                    "native Iceberg change-stream router source={} group={} branch_id={} \
                     branch_kind={:?} has no matching branch route",
                    fragment_id, router_group_id, branch_id, branch_kind
                )
            })?;
        route.target_fragment_id = edge.target_fragment_id;
        route.target_exchange_node_id = edge.target_exchange_node_id;

        if route.output_partition.is_none() {
            return Err(format!(
                "native Iceberg change-stream router source={} group={} branch_id={} \
                 branch_kind={:?} missing output_partition from native encoder",
                fragment_id, router_group_id, branch_id, branch_kind
            ));
        }

        let dests = placements.get(&edge.target_fragment_id).ok_or_else(|| {
            format!(
                "native Iceberg change-stream router source={} group={} branch_id={} target \
                 fragment {} has no placements",
                fragment_id, router_group_id, branch_id, edge.target_fragment_id
            )
        })?;
        route.destinations = Some(crate::proto::plan::StreamDestinationList {
            destinations: dests
                .iter()
                .map(|placement| {
                    native_stream_destination(&crate::runtime::endpoint::FragmentDestination::new(
                        placement.finst_id.clone(),
                        placement.endpoint.clone(),
                    ))
                })
                .collect(),
        });
    }

    debug!(
        "patched native Iceberg change-stream router sink: fragment={} group={} branches={}",
        fragment_id,
        router_group_id,
        branch_edges.len()
    );
    Ok(())
}

fn native_change_stream_branch_kind(
    value: i32,
) -> Result<crate::sql::common::ChangeStreamBranchKind, String> {
    match crate::proto::plan::ChangeStreamBranchKind::try_from(value)
        .map_err(|_| format!("unknown native ChangeStreamBranchKind value {value}"))?
    {
        crate::proto::plan::ChangeStreamBranchKind::DeleteDv => {
            Ok(crate::sql::common::ChangeStreamBranchKind::DeleteDv)
        }
        crate::proto::plan::ChangeStreamBranchKind::ReuseData => {
            Ok(crate::sql::common::ChangeStreamBranchKind::ReuseData)
        }
        crate::proto::plan::ChangeStreamBranchKind::FreshData => {
            Ok(crate::sql::common::ChangeStreamBranchKind::FreshData)
        }
        crate::proto::plan::ChangeStreamBranchKind::Unspecified => {
            Err("native ChangeStreamBranchKind is unspecified".to_string())
        }
    }
}

fn patch_native_cte_multicast_sink(
    fragment: &mut crate::proto::plan::PlanFragment,
    fragment_id: FragmentId,
    cte_id: CteId,
    consumers: &[(
        FragmentId,
        i32,
        crate::proto::plan::DataPartition,
        Vec<i32>,
        Vec<ColumnId>,
    )],
    consumer_dests: &BTreeMap<FragmentId, Vec<crate::runtime::endpoint::FragmentDestination>>,
) -> Result<(), String> {
    if consumers.is_empty() {
        return Err(format!("CTE fragment (cte_id={cte_id}) has no consumers"));
    }
    let mut sinks = Vec::with_capacity(consumers.len());
    let mut destinations = Vec::with_capacity(consumers.len());
    for (
        consumer_fragment_id,
        exchange_node_id,
        partition,
        output_slot_ids,
        receive_producer_column_ids,
    ) in consumers
    {
        let sink_output_columns = native_cte_multicast_sink_output_columns(
            fragment,
            cte_id,
            *consumer_fragment_id,
            *exchange_node_id,
            output_slot_ids,
            receive_producer_column_ids,
        )?;
        sinks.push(crate::proto::plan::DataStreamSink {
            dest_node_id: *exchange_node_id,
            output_partition: Some(partition.clone()),
            output_columns: sink_output_columns,
            limit: None,
        });
        let dests = consumer_dests.get(consumer_fragment_id).ok_or_else(|| {
            format!("CTE consumer fragment {consumer_fragment_id} has no placements")
        })?;
        destinations.push(crate::proto::plan::StreamDestinationList {
            destinations: dests.iter().map(native_stream_destination).collect(),
        });
    }
    fragment.sink = Some(crate::proto::plan::DataSink {
        kind: Some(crate::proto::plan::data_sink::Kind::MultiCastDataStream(
            crate::proto::plan::MultiCastDataStreamSink {
                sinks,
                destinations,
            },
        )),
    });
    debug!(
        "patched native CTE multicast sink: fragment={} cte_id={} sinks={}",
        fragment_id,
        cte_id,
        consumers.len()
    );
    Ok(())
}

fn exec_destination_from_runtime(
    src: &crate::runtime::endpoint::FragmentDestination,
) -> data_sinks::TPlanFragmentDestination {
    data_sinks::TPlanFragmentDestination::new(
        src.finst_id().clone(),
        None::<types::TNetworkAddress>,
        Some(src.endpoint().to_network_address()),
        None::<i32>,
    )
}

fn native_stream_destination(
    src: &crate::runtime::endpoint::FragmentDestination,
) -> crate::proto::plan::StreamDestination {
    crate::proto::plan::StreamDestination {
        finst_id: Some(crate::proto::common::UniqueId {
            hi: src.finst_id().hi,
            lo: src.finst_id().lo,
        }),
        endpoint: src.endpoint().as_host_port(),
    }
}

fn native_cte_multicast_sink_output_columns(
    fragment: &crate::proto::plan::PlanFragment,
    cte_id: CteId,
    consumer_fragment_id: FragmentId,
    exchange_node_id: i32,
    requested_output_slot_ids: &[i32],
    receive_producer_column_ids: &[ColumnId],
) -> Result<Vec<i32>, String> {
    if requested_output_slot_ids.is_empty() {
        return Ok(Vec::new());
    }

    let root_columns =
        crate::sql::codegen::proto_encode::plan::encoded_fragment_root_output_columns(fragment)?;
    let root_slot_ids = root_columns
        .iter()
        .map(|column| {
            i32::try_from(column.column_id).map_err(|_| {
                format!(
                    "native CTE source output column {} cannot convert to slot id",
                    column.column_id
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let root_slot_id_set = root_slot_ids.iter().copied().collect::<BTreeSet<_>>();
    if requested_output_slot_ids
        .iter()
        .all(|slot_id| root_slot_id_set.contains(slot_id))
    {
        return Ok(requested_output_slot_ids.to_vec());
    }
    if receive_producer_column_ids.len() == requested_output_slot_ids.len()
        && let Some(mapped) = receive_producer_column_ids
            .iter()
            .map(|column_id| {
                let slot_id = i32::try_from(column_id.0).ok()?;
                root_slot_id_set.contains(&slot_id).then_some(slot_id)
            })
            .collect::<Option<Vec<_>>>()
    {
        return Ok(mapped);
    }
    let contract_slot_map =
        native_cte_multicast_contract_slot_map(fragment, &root_columns, &root_slot_id_set);
    if let Some(mapped) = requested_output_slot_ids
        .iter()
        .map(|slot_id| contract_slot_map.get(slot_id).copied())
        .collect::<Option<Vec<_>>>()
    {
        return Ok(mapped);
    }
    if requested_output_slot_ids.len() == root_slot_ids.len() {
        return Ok(root_slot_ids);
    }
    Err(format!(
        "native CTE multicast sink output columns for cte_id={cte_id} consumer_fragment={consumer_fragment_id} exchange_node_id={exchange_node_id} ({requested_output_slot_ids:?}) do not match source root output columns ({root_slot_ids:?})"
    ))
}

fn native_cte_multicast_contract_slot_map(
    fragment: &crate::proto::plan::PlanFragment,
    root_columns: &[crate::proto::common::OutputColumn],
    root_slot_id_set: &BTreeSet<i32>,
) -> BTreeMap<i32, i32> {
    let mut map = BTreeMap::new();

    if fragment.output_exprs.len() == fragment.output_columns.len() {
        for (output, expr) in fragment
            .output_columns
            .iter()
            .zip(fragment.output_exprs.iter())
        {
            let Some(crate::proto::expr::expr::Kind::ColumnRef(column_ref)) = expr.kind.as_ref()
            else {
                continue;
            };
            let Ok(contract_id) = i32::try_from(output.column_id) else {
                continue;
            };
            let Ok(root_id) = i32::try_from(column_ref.column_id) else {
                continue;
            };
            if root_slot_id_set.contains(&root_id) {
                map.insert(contract_id, root_id);
            }
        }
    }

    for output in &fragment.output_columns {
        let Ok(contract_id) = i32::try_from(output.column_id) else {
            continue;
        };
        if map.contains_key(&contract_id) {
            continue;
        }
        let mut matches = root_columns.iter().filter(|root| {
            root.name == output.name
                && root.nullable == output.nullable
                && root.r#type == output.r#type
        });
        let Some(root) = matches.next() else {
            continue;
        };
        if matches.next().is_some() {
            continue;
        }
        if let Ok(root_id) = i32::try_from(root.column_id) {
            map.insert(contract_id, root_id);
        }
    }

    if fragment.output_columns.len() <= root_columns.len() {
        for (output, root) in fragment.output_columns.iter().zip(root_columns.iter()) {
            let Ok(contract_id) = i32::try_from(output.column_id) else {
                continue;
            };
            if map.contains_key(&contract_id) {
                continue;
            }
            if let Ok(root_id) = i32::try_from(root.column_id) {
                map.insert(contract_id, root_id);
            }
        }
    }

    map
}

/// Assemble the per-instance runtime filter routing params from
/// scheduler-provided prober params plus the global builder-number map.
///
/// `instance_counts` maps fragment id to the number of instances the scheduler
/// assigned to it. For each build fragment, every filter id it produces must
/// wait for exactly that many partial filters before the merge node broadcasts.
/// Hardcoding 1 here would cause the merge to broadcast after the first
/// partial, silently dropping N-1 partials and producing an incomplete bloom
/// filter at N > 1 instances (wrong join results).
fn build_instance_runtime_filter_params(
    rf_plan: &RuntimeFilterPlanResult,
    id_to_prober_params: &BTreeMap<
        i32,
        Vec<crate::runtime::endpoint::RuntimeFilterProberDestination>,
    >,
    instance_counts: &BTreeMap<FragmentId, usize>,
) -> RuntimeFilterParams {
    let builder_number = runtime_filter_builder_number_for_instance(Some(rf_plan), instance_counts);
    RuntimeFilterParams::new(
        id_to_prober_params.clone(),
        builder_number,
        Some(16_i64 * 1024 * 1024),
    )
}

fn runtime_filter_builder_number_for_instance(
    rf_plan: Option<&RuntimeFilterPlanResult>,
    instance_counts: &BTreeMap<FragmentId, usize>,
) -> BTreeMap<i32, i32> {
    let mut builder_number = BTreeMap::new();
    if let Some(rf_plan) = rf_plan {
        for (build_frag_id, filter_ids) in &rf_plan.build_side_filters {
            let n_builders = instance_counts
                .get(build_frag_id)
                .map(|&n| n as i32)
                .unwrap_or(1);
            for filter_id in filter_ids {
                builder_number.insert(*filter_id, n_builders);
            }
        }
    }
    builder_number
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

#[derive(Default)]
struct StandaloneQueryProfileRegistry {
    active: BTreeSet<(i64, i64)>,
    profiles: BTreeMap<
        (i64, i64),
        BTreeMap<(i64, i64), crate::thrift::runtime_profile::TRuntimeProfileTree>,
    >,
}

fn standalone_query_profiles() -> &'static Mutex<StandaloneQueryProfileRegistry> {
    static REGISTRY: OnceLock<Mutex<StandaloneQueryProfileRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(StandaloneQueryProfileRegistry::default()))
}

pub(crate) fn record_standalone_query_profile_report(
    params: &crate::thrift::frontend_service::TReportExecStatusParams,
) -> Result<bool, String> {
    let Some(query_id) = params.query_id.as_ref() else {
        return Ok(false);
    };
    let key = query_failure_key(query_id);
    let mut guard = standalone_query_profiles()
        .lock()
        .expect("standalone query profile registry lock");
    if !guard.active.contains(&key) {
        return Ok(false);
    }

    let done = params.done.unwrap_or(false);
    let status = params
        .status
        .as_ref()
        .ok_or_else(|| "TReportExecStatusParams missing status".to_string())?;
    if done
        && status.status_code == crate::thrift::status_code::TStatusCode::OK
        && let Some(profile) = params.profile.as_ref()
    {
        let finst_id = params
            .fragment_instance_id
            .as_ref()
            .ok_or_else(|| "TReportExecStatusParams missing fragment_instance_id".to_string())?;
        guard
            .profiles
            .entry(key)
            .or_default()
            .insert(query_failure_key(finst_id), profile.clone());
    }
    Ok(true)
}

pub(crate) fn record_native_standalone_query_profile_report(
    report: &crate::proto::novarocks::ExecStatusReport,
) -> Result<bool, String> {
    let Some(query_id) = report.query_id.as_ref() else {
        return Ok(false);
    };
    let key = (query_id.hi, query_id.lo);
    let mut guard = standalone_query_profiles()
        .lock()
        .expect("standalone query profile registry lock");
    if !guard.active.contains(&key) {
        return Ok(false);
    }

    let Some(status) = report.status.as_ref() else {
        return Err("ExecStatusReport missing status".to_string());
    };
    if report.done
        && status.code == 0
        && let Some(profile) = report.profile.as_ref()
    {
        let Some(finst_id) = report.fragment_instance_id.as_ref() else {
            return Err("ExecStatusReport missing fragment_instance_id".to_string());
        };
        let thrift = crate::runtime::profile::native_profile_tree_to_thrift(profile)?;
        guard
            .profiles
            .entry(key)
            .or_default()
            .insert((finst_id.hi, finst_id.lo), thrift);
    }
    Ok(true)
}

fn standalone_query_profile_count(query_id: &types::TUniqueId) -> usize {
    standalone_query_profiles()
        .lock()
        .expect("standalone query profile registry lock")
        .profiles
        .get(&query_failure_key(query_id))
        .map(BTreeMap::len)
        .unwrap_or(0)
}

fn take_standalone_query_profiles(
    query_id: &types::TUniqueId,
) -> Vec<crate::thrift::runtime_profile::TRuntimeProfileTree> {
    standalone_query_profiles()
        .lock()
        .expect("standalone query profile registry lock")
        .profiles
        .remove(&query_failure_key(query_id))
        .map(|profiles| profiles.into_values().collect())
        .unwrap_or_default()
}

struct StandaloneQueryProfileGuard {
    key: (i64, i64),
}

impl StandaloneQueryProfileGuard {
    fn register(query_id: &types::TUniqueId) -> Self {
        let key = query_failure_key(query_id);
        let mut guard = standalone_query_profiles()
            .lock()
            .expect("standalone query profile registry lock");
        guard.profiles.remove(&key);
        guard.active.insert(key);
        Self { key }
    }
}

impl Drop for StandaloneQueryProfileGuard {
    fn drop(&mut self) {
        let mut guard = standalone_query_profiles()
            .lock()
            .expect("standalone query profile registry lock");
        guard.active.remove(&self.key);
        guard.profiles.remove(&self.key);
    }
}

#[derive(Debug)]
pub(crate) struct SubmitAndFetchResult {
    pub(crate) chunks: Vec<crate::exec::chunk::Chunk>,
    pub(crate) write_commit: Option<WriteCommitInput>,
    pub(crate) write_abort: Option<WriteAbortInput>,
    pub(crate) fragment_profiles: Vec<crate::thrift::runtime_profile::TRuntimeProfileTree>,
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
    submissions: Vec<(usize, FragmentSubmission)>,
    root_backend_idx: usize,
    root_finst_id: types::TUniqueId,
    query_id: &types::TUniqueId,
    timeout_ms: i64,
    expected_root_chunk_schema: Option<&ChunkSchemaRef>,
    write_coordinator: Option<&Arc<Mutex<WriteCoordinator>>>,
    collect_profiles: bool,
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
    let _failure_guard = StandaloneQueryFailureGuard::register(query_id);
    let _profile_guard = collect_profiles.then(|| StandaloneQueryProfileGuard::register(query_id));

    for (backend_idx, submission) in submissions {
        let finst_id = submission
            .thrift_params()
            .params
            .as_ref()
            .map(|ep| types::TUniqueId::new(ep.fragment_instance_id.hi, ep.fragment_instance_id.lo))
            .unwrap_or_else(|| types::TUniqueId::new(0, 0));
        if let Err(e) = dispatcher.submit_fragment_submission(backend_idx, submission) {
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
            if let Some(write) = write_coordinator
                && let Err(e) = poll_write_failure_and_cancel(write, tracker, dispatcher.as_ref())
            {
                let abort = write.lock().expect("write coordinator lock").abort_input();
                let Some(abort) = abort else {
                    return Err(e);
                };
                return Ok(SubmitAndFetchResult {
                    chunks,
                    write_commit: None,
                    write_abort: Some(abort),
                    fragment_profiles: Vec::new(),
                });
            }
            if let Some(err) = take_standalone_query_failure(query_id) {
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
            match dispatcher.fetch_result(
                root_backend_idx,
                root_finst_id.clone(),
                fetch_wait_ms,
                expected_root_chunk_schema,
            ) {
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

    let fragment_profiles = if collect_profiles {
        wait_for_profile_reports(
            query_id,
            tracker.by_backend.values().map(Vec::len).sum(),
            tracker,
            dispatcher.as_ref(),
            deadline,
            timeout_ms,
            runtime_query_id,
        )?
    } else {
        Vec::new()
    };

    Ok(SubmitAndFetchResult {
        chunks,
        write_commit,
        write_abort,
        fragment_profiles,
    })
}

fn wait_for_profile_reports(
    query_id: &types::TUniqueId,
    expected_reports: usize,
    tracker: &InFlightTracker,
    dispatcher: &dyn FragmentDispatcher,
    deadline: std::time::Instant,
    timeout_ms: i64,
    runtime_query_id: crate::runtime::query_context::QueryId,
) -> Result<Vec<crate::thrift::runtime_profile::TRuntimeProfileTree>, String> {
    const PROFILE_REPORT_POLL_INTERVAL_MS: i64 = 10;

    if expected_reports == 0 {
        return Ok(Vec::new());
    }

    loop {
        let received = standalone_query_profile_count(query_id);
        if received >= expected_reports {
            return Ok(take_standalone_query_profiles(query_id));
        }

        if let Some(err) = take_standalone_query_failure(query_id) {
            tracker.cancel_all(dispatcher);
            return Err(err);
        }
        if crate::runtime::query_cancel::client_disconnected() {
            tracker.cancel_all(dispatcher);
            return Err("client disconnected".to_string());
        }
        if crate::runtime::query_state::in_flight_table().state(runtime_query_id)
            == Some(QueryState::Failed)
        {
            let reason = crate::runtime::query_state::in_flight_table()
                .failure_reason(runtime_query_id)
                .unwrap_or_else(|| format!("query {} failed", runtime_query_id));
            tracker.cancel_all(dispatcher);
            return Err(reason);
        }

        let now = std::time::Instant::now();
        if now >= deadline {
            tracker.cancel_all(dispatcher);
            return Err(format!(
                "query timed out after {timeout_ms} ms waiting for fragment profile reports: received {received} of {expected_reports}"
            ));
        }

        let remaining_ms = deadline
            .saturating_duration_since(now)
            .as_millis()
            .min(i64::MAX as u128) as i64;
        let sleep_ms = remaining_ms.clamp(1, PROFILE_REPORT_POLL_INTERVAL_MS);
        std::thread::sleep(std::time::Duration::from_millis(sleep_ms as u64));
    }
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
    use crate::runtime::profile::ProfileUnit;
    use crate::runtime::write_coordinator::{
        FragmentExecStatusReport, WriteCoordinator, WriterKey, write_registry_test_guard,
    };
    use crate::thrift::{status, status_code};
    use arrow::array::{
        Array, ArrayRef, BinaryArray, Decimal128Array, FixedSizeBinaryArray, Int32Array,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    #[cfg(feature = "compat")]
    #[test]
    fn native_fragment_sidecars_thrift_allows_missing_native_sidecars() {
        let sidecars = native_fragment_sidecars(PlanWireFormat::Thrift, None)
            .expect("thrift mode must not require native sidecars");

        assert!(sidecars.is_empty());
        let empty_sidecars = NativePlanSidecars {
            fragments_by_id: BTreeMap::new(),
        };
        let sidecars = native_fragment_sidecars(PlanWireFormat::Thrift, Some(&empty_sidecars))
            .expect("thrift mode must allow empty native sidecars");

        assert!(sidecars.is_empty());
    }

    #[test]
    fn native_fragment_sidecars_proto_requires_native_sidecars() {
        let err = native_fragment_sidecars(PlanWireFormat::Proto, None)
            .expect_err("proto mode must require native sidecars");

        assert!(err.contains("plan_wire_format=proto"), "{err}");
        assert!(
            err.contains("native DistributedPlan") || err.contains("native sidecar"),
            "{err}"
        );
    }

    #[cfg(feature = "compat")]
    #[test]
    fn native_sidecar_sink_support_allows_thrift_dynamic_stream_sink() {
        ensure_native_sidecar_sink_supported(
            PlanWireFormat::Thrift,
            7,
            false,
            false,
            true,
            false,
            false,
        )
        .expect("thrift mode owns dynamic sink wiring");
    }

    #[test]
    fn native_sidecar_sink_support_allows_proto_dynamic_stream_sink() {
        ensure_native_sidecar_sink_supported(
            PlanWireFormat::Proto,
            7,
            false,
            false,
            true,
            false,
            false,
        )
        .expect("proto mode carries native DATA_STREAM_SINK sidecars");
    }

    #[test]
    fn native_sidecar_sink_support_allows_proto_router_and_cte_sinks() {
        ensure_native_sidecar_sink_supported(
            PlanWireFormat::Proto,
            8,
            false,
            false,
            false,
            true,
            false,
        )
        .expect("proto mode patches native ICEBERG_CHANGE_STREAM_ROUTER_SINK sidecars");

        ensure_native_sidecar_sink_supported(
            PlanWireFormat::Proto,
            9,
            false,
            false,
            false,
            false,
            true,
        )
        .expect("proto mode patches native MULTI_CAST_DATA_STREAM_SINK sidecars");
    }

    #[test]
    fn typed_root_alignment_renames_fields_and_widens_runtime_nullability() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wire_i",
            DataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
        )
        .expect("typed batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(7)])
                .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        let columns = vec![crate::sql::codegen::OutputColumn {
            name: "col1".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }];

        let chunks =
            align_fetch_chunks_to_output_columns(vec![chunk], &columns).expect("align chunks");
        let batch = &chunks[0].batch;
        assert_eq!(batch.schema().field(0).name(), "col1");
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
    fn typed_root_alignment_rejects_decimal_precision_drift() {
        let decimal = Decimal128Array::from(vec![Some(100_000_000_000_000_000_000_i128), None])
            .with_precision_and_scale(38, 2)
            .expect("decimal array");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wire_price",
            DataType::Decimal128(38, 2),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(decimal) as ArrayRef])
            .expect("typed batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(11)])
                .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        let columns = vec![crate::sql::codegen::OutputColumn {
            name: "price".to_string(),
            data_type: DataType::Decimal128(20, 2),
            nullable: true,
        }];

        let err = align_fetch_chunks_to_output_columns(vec![chunk], &columns)
            .expect_err("typed root must reject decimal precision drift");

        assert!(
            err.contains("typed root result column 0 type mismatch"),
            "{err}"
        );
        assert!(err.contains("Decimal128(20, 2)"), "{err}");
        assert!(err.contains("Decimal128(38, 2)"), "{err}");
    }

    #[test]
    fn typed_root_alignment_retags_same_unit_timestamp_timezone_metadata() {
        let timestamp =
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1_234_i64), None])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wire_ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![timestamp]).expect("typed batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(12)])
                .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        let target_type = DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()));
        let columns = vec![crate::sql::codegen::OutputColumn {
            name: "ts".to_string(),
            data_type: target_type.clone(),
            nullable: true,
        }];

        let chunks =
            align_fetch_chunks_to_output_columns(vec![chunk], &columns).expect("align chunks");
        let batch = &chunks[0].batch;
        assert_eq!(batch.schema().field(0).name(), "ts");
        assert_eq!(batch.schema().field(0).data_type(), &target_type);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp micros");
        assert_eq!(values.value(0), 1_234);
        assert!(values.is_null(1));
    }

    #[test]
    fn typed_root_alignment_preserves_largeint_type() {
        let largeint = crate::common::largeint::array_from_i128(&[Some(128), Some(-5)])
            .expect("largeint array");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wire_big",
            DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![largeint]).expect("typed batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(12)])
                .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        let columns = vec![crate::sql::codegen::OutputColumn {
            name: "big_value".to_string(),
            data_type: DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH),
            nullable: true,
        }];

        let chunks =
            align_fetch_chunks_to_output_columns(vec![chunk], &columns).expect("align chunks");
        let batch = &chunks[0].batch;
        assert_eq!(batch.schema().field(0).name(), "big_value");
        let largeint_values = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("largeint array");
        assert_eq!(
            crate::common::largeint::i128_from_be_bytes(largeint_values.value(0)).unwrap(),
            128
        );
        assert_eq!(
            crate::common::largeint::i128_from_be_bytes(largeint_values.value(1)).unwrap(),
            -5
        );
    }

    #[test]
    fn typed_root_alignment_rejects_binary_mysql_text_for_complex_decimal_and_largeint() {
        let make_binary_chunk = || {
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
            .expect("binary batch");
            let chunk_schema =
                ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(0)])
                    .expect("chunk schema");
            Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk")
        };

        for target_type in [
            DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into()),
            DataType::Decimal128(20, 2),
            DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH),
        ] {
            let columns = vec![crate::sql::codegen::OutputColumn {
                name: "payload".to_string(),
                data_type: target_type,
                nullable: true,
            }];
            let err = align_fetch_chunks_to_output_columns(vec![make_binary_chunk()], &columns)
                .expect_err("binary mysql text must not be coerced at typed root");
            assert!(err.contains("type mismatch"), "{err}");
        }
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
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn submit_fragment(
            &self,
            _backend_idx: usize,
            params: crate::thrift::internal_service::TExecPlanFragmentParams,
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
            _expected_chunk_schema: Option<&ChunkSchemaRef>,
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
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn submit_fragment(
            &self,
            _backend_idx: usize,
            params: crate::thrift::internal_service::TExecPlanFragmentParams,
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
            _expected_chunk_schema: Option<&ChunkSchemaRef>,
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
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn submit_fragment(
            &self,
            _backend_idx: usize,
            params: crate::thrift::internal_service::TExecPlanFragmentParams,
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
            _expected_chunk_schema: Option<&ChunkSchemaRef>,
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
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn submit_fragment(
            &self,
            _backend_idx: usize,
            params: crate::thrift::internal_service::TExecPlanFragmentParams,
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
            _expected_chunk_schema: Option<&ChunkSchemaRef>,
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
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn submit_fragment(
            &self,
            _backend_idx: usize,
            params: crate::thrift::internal_service::TExecPlanFragmentParams,
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
            _expected_chunk_schema: Option<&ChunkSchemaRef>,
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
    ) -> crate::thrift::internal_service::TExecPlanFragmentParams {
        use crate::thrift::{data_sinks, internal_service, partitions, types};

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
            None::<data_sinks::TIcebergChangeStreamRouterSink>,
        );
        let fragment = crate::thrift::planner::TPlanFragment::new(
            None::<crate::thrift::plan_nodes::TPlan>,
            None::<Vec<crate::thrift::exprs::TExpr>>,
            Some(result_sink),
            partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None::<Vec<crate::thrift::exprs::TExpr>>,
                None::<Vec<partitions::TRangePartition>>,
                None::<Vec<partitions::TBucketProperty>>,
            ),
            None::<i64>,
            None::<i64>,
            None::<Vec<crate::thrift::data::TGlobalDict>>,
            None::<Vec<crate::thrift::data::TGlobalDict>>,
            None::<crate::thrift::planner::TCacheParam>,
            None::<std::collections::BTreeMap<i32, crate::thrift::exprs::TExpr>>,
            None::<crate::thrift::planner::TGroupExecutionParam>,
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
            None::<crate::thrift::descriptors::TDescriptorTable>,
            Some(exec_params),
            None::<types::TNetworkAddress>,
            None::<i32>,
            None::<internal_service::TQueryGlobals>,
            None,
            None::<bool>,
            None::<types::TResourceInfo>,
            None::<String>,
            None::<String>,
            None::<i64>,
            None::<internal_service::TLoadErrorHubInfo>,
            None::<bool>,
            None::<i32>,
            None::<std::collections::BTreeMap<types::TPlanNodeId, i32>>,
            None::<crate::thrift::work_group::TWorkGroup>,
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
            None::<bool>,
            None::<bool>, // novarocks_generated_plan
        )
    }

    fn make_params_with_sink_type(
        sink_type: data_sinks::TDataSinkType,
    ) -> crate::thrift::internal_service::TExecPlanFragmentParams {
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
    ) -> crate::thrift::internal_service::TExecPlanFragmentParams {
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

    fn fake_stream_edge(
        source_fragment_id: FragmentId,
        target_fragment_id: FragmentId,
        target_exchange_node_id: i32,
    ) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id,
            target_fragment_id,
            target_exchange_node_id,
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            stream_kind: crate::sql::codegen::FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }
    }

    fn fake_router_edge(
        source_fragment_id: FragmentId,
        target_fragment_id: FragmentId,
        target_exchange_node_id: i32,
        router_group_id: i32,
        branch_id: i32,
        branch_kind: crate::sql::common::ChangeStreamBranchKind,
    ) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id,
            target_fragment_id,
            target_exchange_node_id,
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            stream_kind: crate::sql::codegen::FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::IcebergChangeStreamRouter {
                router_group_id,
                branch_id,
                branch_kind,
            },
            output_slot_ids: Vec::new(),
        }
    }

    fn hash_key_expr_for_test(column_id: u32, column: &str) -> crate::sql::analysis::TypedExpr {
        crate::sql::analysis::TypedExpr {
            kind: crate::sql::analysis::ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(column_id),
                qualifier: None,
                column: column.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn assert_single_slot_ref_partition_expr(
        partition: &partitions::TDataPartition,
        expected_slot_id: i32,
    ) {
        assert_eq!(
            partition.type_,
            partitions::TPartitionType::HASH_PARTITIONED
        );
        let exprs = partition
            .partition_exprs
            .as_ref()
            .expect("hash partition should carry compiled partition exprs");
        assert_eq!(exprs.len(), 1);
        let node = exprs[0].nodes.first().expect("slot-ref expr node");
        assert_eq!(
            node.node_type,
            crate::thrift::exprs::TExprNodeType::SLOT_REF
        );
        assert_eq!(
            node.slot_ref.as_ref().expect("slot_ref").slot_id,
            expected_slot_id
        );
    }

    fn compiled_hash_partition_for_test(slot_id: i32) -> partitions::TDataPartition {
        partitions::TDataPartition::new(
            partitions::TPartitionType::HASH_PARTITIONED,
            Some(vec![
                crate::sql::codegen::expr_compiler::build_slot_ref_texpr(
                    slot_id,
                    1,
                    crate::lower::compat::type_lowering::scalar_type_desc(
                        crate::thrift::types::TPrimitiveType::BIGINT,
                    ),
                ),
            ]),
            None::<Vec<partitions::TRangePartition>>,
            None::<Vec<partitions::TBucketProperty>>,
        )
    }

    fn compat_partition_map_for_test(
        edge: &FragmentEdge,
        partition: partitions::TDataPartition,
    ) -> BTreeMap<FragmentEdgeKey, partitions::TDataPartition> {
        BTreeMap::from([(fragment_edge_key(edge), partition)])
    }

    fn empty_router_sink_for_test() -> data_sinks::TIcebergChangeStreamRouterSink {
        data_sinks::TIcebergChangeStreamRouterSink::new(1, None::<i32>, vec![])
    }

    fn empty_data_sink_for_test(sink_type: data_sinks::TDataSinkType) -> data_sinks::TDataSink {
        data_sinks::TDataSink::new(
            sink_type,
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
            None::<data_sinks::TIcebergChangeStreamRouterSink>,
        )
    }

    fn fake_placement(
        fragment_id: FragmentId,
        instance_index: usize,
        finst_lo: i64,
        host: &str,
    ) -> FragmentInstancePlacement {
        FragmentInstancePlacement {
            fragment_id,
            instance_index,
            finst_id: types::TUniqueId::new(11, finst_lo),
            backend_idx: instance_index,
            endpoint: crate::runtime::endpoint::RuntimeEndpoint::new(host, 9010)
                .expect("test endpoint"),
            scan_ranges: BTreeMap::new(),
            destinations: Vec::new(),
            runtime_filter_prober_params: BTreeMap::new(),
            per_exch_num_senders: BTreeMap::new(),
        }
    }

    fn native_file_scan_range_for_test(marker: i32) -> crate::runtime::scan_range::ScanRangeParams {
        let mut params = crate::runtime::scan_range::ScanRangeParams::file(
            crate::runtime::scan_range::FileScanRange {
                file_format: crate::runtime::scan_range::FileFormat::Parquet,
                full_path: Some(format!("s3://bucket/native-{marker}.parquet")),
                relative_path: None,
                table_id: None,
                offset: 0,
                length: 1,
                file_length: 1,
                delete_files: Vec::new(),
                deletion_vector_descriptor: None,
                first_row_id: None,
                data_sequence_number: None,
                modification_time: None,
                datacache_options: None,
                included_positions: Vec::new(),
                serialized_split: None,
                use_iceberg_jni_metadata_reader: false,
                ivm_change_op: None,
                file_pruning_min_max_values: None,
                extended_columns: None,
            },
        );
        params.volume_id = Some(marker);
        params
    }

    fn compat_scan_range_for_test(
        marker: i32,
    ) -> crate::thrift::internal_service::TScanRangeParams {
        crate::thrift::internal_service::TScanRangeParams::new(
            crate::thrift::plan_nodes::TScanRange::new(
                None::<crate::thrift::plan_nodes::TInternalScanRange>,
                None::<Vec<u8>>,
                None::<crate::thrift::plan_nodes::TBrokerScanRange>,
                None::<crate::thrift::plan_nodes::TEsScanRange>,
                None::<crate::thrift::plan_nodes::THdfsScanRange>,
                None::<crate::thrift::plan_nodes::TBinlogScanRange>,
                None::<crate::thrift::plan_nodes::TBenchmarkScanRange>,
            ),
            Some(marker),
            Some(false),
            Some(false),
        )
    }

    fn fragment_for_scan_range_merge_test(
        compat_ranges: BTreeMap<i32, Vec<crate::thrift::internal_service::TScanRangeParams>>,
    ) -> crate::sql::codegen::FragmentBuildResult {
        let mut params = make_params_with_finst(1, 1);
        let exec_params = params
            .params
            .as_mut()
            .expect("exec params for scan range merge test");
        exec_params.per_node_scan_ranges = compat_ranges;
        crate::sql::codegen::FragmentBuildResult {
            fragment_id: 0,
            has_scan_nodes: false,
            output_kind: crate::sql::codegen::FragmentOutputKind::Result,
            plan: crate::thrift::plan_nodes::TPlan::new(Vec::new()),
            desc_tbl: crate::thrift::descriptors::TDescriptorTable::new(
                Vec::new(),
                Vec::new(),
                Vec::new(),
                false,
            ),
            exec_params: exec_params.clone(),
            native_scan_ranges: BTreeMap::new(),
            output_sink: empty_data_sink_for_test(data_sinks::TDataSinkType::RESULT_SINK),
            output_exprs: None,
            output_columns: Vec::new(),
            boundary_schemas: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
            query_global_dicts: None,
            query_global_dict_exprs: None,
        }
    }

    #[test]
    fn compat_scan_ranges_for_placement_merges_native_and_compat_only_nodes() {
        let fragment = fragment_for_scan_range_merge_test(BTreeMap::from([
            (10, vec![compat_scan_range_for_test(999)]),
            (
                20,
                vec![
                    compat_scan_range_for_test(201),
                    compat_scan_range_for_test(202),
                    compat_scan_range_for_test(203),
                ],
            ),
        ]));
        let mut placement = fake_placement(0, 1, 200, "10.0.0.20");
        placement
            .scan_ranges
            .insert(10, vec![native_file_scan_range_for_test(101)]);

        let merged = compat_scan_ranges_for_placement(&fragment, &placement, 2)
            .expect("merge native and compat scan ranges");

        assert_eq!(
            merged[&10]
                .iter()
                .map(|range| range.volume_id.expect("native marker"))
                .collect::<Vec<_>>(),
            vec![101],
            "native scan range key must not be replaced by compat projection"
        );
        assert_eq!(
            merged[&20]
                .iter()
                .map(|range| range.volume_id.expect("compat marker"))
                .collect::<Vec<_>>(),
            vec![202],
            "compat-only scan range key must still be assigned round-robin"
        );
    }

    fn fake_destination(
        finst_lo: i64,
        host: &str,
    ) -> crate::runtime::endpoint::FragmentDestination {
        crate::runtime::endpoint::FragmentDestination::new(
            types::TUniqueId::new(11, finst_lo),
            crate::runtime::endpoint::RuntimeEndpoint::new(host, 9010).expect("test endpoint"),
        )
    }

    fn is_write_sink_for_test(
        params: &crate::thrift::internal_service::TExecPlanFragmentParams,
    ) -> bool {
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

    fn profile_report_params(
        query_id: types::TUniqueId,
        finst_id: types::TUniqueId,
        profile: crate::thrift::runtime_profile::TRuntimeProfileTree,
    ) -> crate::thrift::frontend_service::TReportExecStatusParams {
        crate::thrift::frontend_service::TReportExecStatusParams::new(
            crate::thrift::frontend_service::FrontendServiceVersion::V1,
            Some(query_id),
            Some(0),
            Some(finst_id),
            Some(ok_status()),
            Some(true),
            Some(profile),
            Option::<Vec<String>>::None,
            Option::<Vec<String>>::None,
            None,
            None,
            Option::<Vec<String>>::None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn profile_tree_for_plan_node(
        node_id: i32,
    ) -> crate::thrift::runtime_profile::TRuntimeProfileTree {
        let profiler = crate::runtime::profile::Profiler::new("fragment");
        let common = profiler
            .child(format!("SCAN (plan_node_id={node_id})"))
            .child("CommonMetrics");
        common.counter_set("PullRowNum", ProfileUnit::Unit, 3);
        common.counter_set("OperatorTotalTime", ProfileUnit::TimeNs, 1_000);
        common.counter_set("OperatorPeakMemoryUsage", ProfileUnit::Bytes, 64);
        profiler.to_thrift_tree()
    }

    // -----------------------------------------------------------------------
    // Original regression tests
    // -----------------------------------------------------------------------

    /// Wrap a list of params as single-backend submissions (backend_idx=0).
    fn single_backend(
        params: Vec<crate::thrift::internal_service::TExecPlanFragmentParams>,
    ) -> Vec<(usize, FragmentSubmission)> {
        params
            .into_iter()
            .map(|p| (0usize, FragmentSubmission::thrift_only(p)))
            .collect()
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
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn submit_fragment(
            &self,
            _backend_idx: usize,
            _params: crate::thrift::internal_service::TExecPlanFragmentParams,
        ) -> Result<(), String> {
            Ok(())
        }

        fn fetch_result(
            &self,
            _backend_idx: usize,
            _finst_id: types::TUniqueId,
            _max_wait_ms: i64,
            _expected_chunk_schema: Option<&ChunkSchemaRef>,
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
            data_sinks::TDataSinkType::ICEBERG_DV_SINK,
            data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK,
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
    fn coordinator_groups_multiple_router_edges_from_same_source() {
        use crate::sql::common::ChangeStreamBranchKind;

        let edges = vec![
            fake_router_edge(1, 2, 11, 7, 0, ChangeStreamBranchKind::DeleteDv),
            fake_router_edge(1, 3, 12, 7, 1, ChangeStreamBranchKind::ReuseData),
        ];
        let grouped = group_router_edges_by_source(&edges).expect("router groups");
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[&(1, 7)].len(), 2);
    }

    #[test]
    fn coordinator_rejects_router_edges_with_duplicate_target_exchange() {
        use crate::sql::common::ChangeStreamBranchKind;

        let edges = vec![
            fake_router_edge(1, 2, 11, 7, 0, ChangeStreamBranchKind::DeleteDv),
            fake_router_edge(1, 2, 11, 7, 1, ChangeStreamBranchKind::ReuseData),
        ];
        let err = group_router_edges_by_source(&edges).expect_err("duplicate target exchange");
        assert!(
            err.contains("repeats target exchange"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn coordinator_still_rejects_multiple_plain_stream_edges_from_same_source() {
        let edges = vec![fake_stream_edge(1, 2, 11), fake_stream_edge(1, 3, 12)];
        let err = build_stream_edge_by_source(&edges).expect_err("multiple plain streams");
        assert!(err.contains("multiple outgoing stream edges"));
    }

    #[test]
    fn plain_stream_sink_carries_edge_output_columns() {
        let mut edge = fake_stream_edge(1, 2, 77);
        edge.output_slot_ids = vec![31, 12, 9];
        edge.output_partition = DataPartition::hash(vec![hash_key_expr_for_test(1, "bucket")]);

        let sink = build_stream_sink_for_edge(&edge, compiled_hash_partition_for_test(31));

        assert_eq!(sink.dest_node_id, 77);
        assert_eq!(sink.output_columns, Some(vec![31, 12, 9]));
        assert_single_slot_ref_partition_expr(&sink.output_partition, 31);
    }

    #[test]
    fn plain_stream_sink_omits_empty_output_columns() {
        let edge = fake_stream_edge(1, 2, 77);

        let sink = build_stream_sink_for_edge(&edge, unpartitioned_partition());

        assert_eq!(sink.output_columns, None);
    }

    #[test]
    fn router_sink_does_not_require_write_report() {
        let sink = data_sinks::TDataSink {
            type_: data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK,
            iceberg_change_stream_router_sink: Some(empty_router_sink_for_test()),
            ..empty_data_sink_for_test(data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK)
        };
        assert!(!compat_data_sink_requires_write_report(&sink));
    }

    #[test]
    fn router_sink_wrapper_uses_edge_exchange_node_and_preserves_projection() {
        use crate::sql::common::ChangeStreamBranchKind;

        let mut edge = fake_router_edge(1, 2, 77, 7, 0, ChangeStreamBranchKind::DeleteDv);
        edge.output_partition = DataPartition::hash(vec![hash_key_expr_for_test(1, "bucket")]);
        let placeholder_partition = unpartitioned_partition();
        let template_stream_sink = data_sinks::TDataStreamSink::new(
            999,
            placeholder_partition,
            None::<bool>,
            None::<bool>,
            None::<i32>,
            Some(vec![10, 11]),
            None::<i64>,
        );
        let template = data_sinks::TIcebergChangeStreamRouterSink::new(
            3,
            Some(4),
            vec![data_sinks::TIcebergChangeStreamRouterBranch::new(
                0,
                data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV,
                template_stream_sink,
                vec![],
            )],
        );
        let placements = BTreeMap::from([(
            2,
            vec![
                fake_placement(2, 0, 200, "10.0.0.20"),
                fake_placement(2, 1, 201, "10.0.0.21"),
            ],
        )]);
        let compat_partitions =
            compat_partition_map_for_test(&edge, compiled_hash_partition_for_test(10));

        let wrapped = wrap_iceberg_change_stream_router_sink(
            &template,
            &[&edge],
            &compat_partitions,
            &placements,
        )
        .expect("wrap");
        let router = wrapped
            .iceberg_change_stream_router_sink
            .as_ref()
            .expect("router sink");
        assert_eq!(router.change_op_slot_id, 3);
        assert_eq!(router.data_route_slot_id, Some(4));
        assert_eq!(router.branches.len(), 1);
        let branch = &router.branches[0];
        assert_eq!(
            branch.stream_sink.dest_node_id, 77,
            "branch stream sink must route to the edge exchange node"
        );
        assert_eq!(branch.stream_sink.output_columns, Some(vec![10, 11]));
        assert_eq!(
            branch.stream_sink.output_partition.type_,
            partitions::TPartitionType::HASH_PARTITIONED
        );
        assert_single_slot_ref_partition_expr(&branch.stream_sink.output_partition, 10);
        assert_eq!(branch.destinations.len(), 2);
        assert_eq!(branch.destinations[0].fragment_instance_id.lo, 200);
        assert_eq!(
            branch.destinations[0]
                .brpc_server
                .as_ref()
                .expect("brpc")
                .hostname,
            "10.0.0.20"
        );
        assert_eq!(branch.destinations[1].fragment_instance_id.lo, 201);
    }

    #[test]
    fn native_router_patch_preserves_static_output_partition() {
        use crate::proto::expr;
        use crate::proto::plan as native_plan;
        use crate::sql::common::ChangeStreamBranchKind;

        let mut edge = fake_router_edge(1, 2, 77, 7, 0, ChangeStreamBranchKind::DeleteDv);
        edge.output_partition = DataPartition::hash(Vec::new());
        let mut fragment = native_plan::PlanFragment {
            fragment_id: 1,
            root: None,
            data_partition: Some(native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            }),
            output_partition: Some(native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            }),
            sink: Some(native_plan::DataSink {
                kind: Some(native_plan::data_sink::Kind::IcebergChangeStreamRouter(
                    native_plan::IcebergChangeStreamRouterSink {
                        group_id: 7,
                        change_op_output_ordinal: 0,
                        data_route_output_ordinal: None,
                        branches: vec![native_plan::IcebergChangeStreamBranchRoute {
                            branch_id: 0,
                            branch_kind: native_plan::ChangeStreamBranchKind::DeleteDv as i32,
                            target_fragment_id: 0,
                            target_exchange_node_id: -1,
                            output_ordinals: vec![0],
                            output_partition_ordinals: vec![0],
                            output_partition: Some(native_plan::DataPartition {
                                kind: native_plan::PartitionKind::Hash as i32,
                                exprs: vec![expr::Expr {
                                    r#type: None,
                                    nullable: false,
                                    kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                                        column_id: 42,
                                        qualifier: None,
                                        column: Some("bucket".to_string()),
                                    })),
                                }],
                            }),
                            destinations: None,
                        }],
                    },
                )),
            }),
            output_exprs: Vec::new(),
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let placements = BTreeMap::from([(2, vec![fake_placement(2, 0, 200, "10.0.0.20")])]);

        patch_native_iceberg_change_stream_router_sink(&mut fragment, 1, 7, &[&edge], &placements)
            .expect("patch native router sink");

        let Some(native_plan::data_sink::Kind::IcebergChangeStreamRouter(router)) =
            fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected native router sink");
        };
        let branch = router.branches.first().expect("router branch");
        assert_eq!(branch.target_fragment_id, 2);
        assert_eq!(branch.target_exchange_node_id, 77);
        assert_eq!(
            branch
                .destinations
                .as_ref()
                .expect("destinations")
                .destinations
                .len(),
            1
        );
        let partition = branch
            .output_partition
            .as_ref()
            .expect("static output partition");
        assert_eq!(partition.kind, native_plan::PartitionKind::Hash as i32);
        let [expr] = partition.exprs.as_slice() else {
            panic!("expected one partition expr");
        };
        let Some(expr::expr::Kind::ColumnRef(column_ref)) = expr.kind.as_ref() else {
            panic!("expected preserved column ref");
        };
        assert_eq!(column_ref.column_id, 42);
    }

    #[test]
    fn native_router_patch_rejects_missing_output_partition() {
        use crate::proto::plan as native_plan;
        use crate::sql::common::ChangeStreamBranchKind;

        let edge = fake_router_edge(1, 2, 77, 7, 0, ChangeStreamBranchKind::DeleteDv);
        let mut fragment = native_plan::PlanFragment {
            fragment_id: 1,
            root: None,
            data_partition: Some(native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            }),
            output_partition: Some(native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            }),
            sink: Some(native_plan::DataSink {
                kind: Some(native_plan::data_sink::Kind::IcebergChangeStreamRouter(
                    native_plan::IcebergChangeStreamRouterSink {
                        group_id: 7,
                        change_op_output_ordinal: 0,
                        data_route_output_ordinal: None,
                        branches: vec![native_plan::IcebergChangeStreamBranchRoute {
                            branch_id: 0,
                            branch_kind: native_plan::ChangeStreamBranchKind::DeleteDv as i32,
                            target_fragment_id: 0,
                            target_exchange_node_id: -1,
                            output_ordinals: vec![0],
                            output_partition_ordinals: vec![0],
                            output_partition: None,
                            destinations: None,
                        }],
                    },
                )),
            }),
            output_exprs: Vec::new(),
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let placements = BTreeMap::new();

        let err = patch_native_iceberg_change_stream_router_sink(
            &mut fragment,
            1,
            7,
            &[&edge],
            &placements,
        )
        .expect_err("native router patch must not reconstruct partition from thrift");

        assert!(
            err.contains("missing output_partition from native encoder"),
            "{err}"
        );
    }

    #[test]
    fn native_cte_multicast_patch_uses_source_root_output_slots() {
        use crate::proto::{common, expr, plan as native_plan};

        fn native_scalar_type(prim: common::PrimitiveType) -> common::TypeDesc {
            common::TypeDesc {
                kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                    r#type: prim as i32,
                    len: None,
                    precision: None,
                    scale: None,
                    time_unit: None,
                })),
            }
        }

        let mut fragment = native_plan::PlanFragment {
            fragment_id: 1,
            root: Some(native_plan::DistributedNode {
                node_id: 5,
                fragment_id: 1,
                tuple_ids: Vec::new(),
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                payload: Some(native_plan::distributed_node::Payload::Physical(
                    native_plan::PlanNode {
                        output_columns: Vec::new(),
                        kind: Some(native_plan::plan_node::Kind::Project(
                            native_plan::ProjectNode {
                                items: vec![native_plan::ProjectItem {
                                    expr: Some(expr::Expr {
                                        r#type: Some(native_scalar_type(
                                            common::PrimitiveType::Bigint,
                                        )),
                                        nullable: true,
                                        kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                                            column_id: 20,
                                            qualifier: None,
                                            column: Some("sum(income)".to_string()),
                                        })),
                                    }),
                                    output_name: "total".to_string(),
                                    output_column_id: 10,
                                }],
                                output_qualifier: None,
                            },
                        )),
                    },
                )),
            }),
            data_partition: Some(native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            }),
            output_partition: Some(native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            }),
            sink: None,
            output_exprs: Vec::new(),
            output_columns: Vec::new(),
            cte_id: Some(3),
            cte_exchange_nodes: Vec::new(),
        };
        let consumers = vec![(
            2,
            77,
            native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            },
            vec![13],
            vec![ColumnId::new_for_test(13)],
        )];
        let destinations = BTreeMap::from([(2, vec![fake_destination(200, "10.0.0.20")])]);

        patch_native_cte_multicast_sink(&mut fragment, 1, 3, &consumers, &destinations)
            .expect("patch native cte sink");

        let Some(native_plan::data_sink::Kind::MultiCastDataStream(sink)) =
            fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected native multicast sink");
        };
        assert_eq!(sink.sinks.len(), 1);
        assert_eq!(sink.sinks[0].output_columns, vec![10]);
    }

    #[test]
    fn native_cte_multicast_patch_maps_contract_subset_to_source_root_slots() {
        use crate::proto::{common, plan as native_plan};

        fn native_scalar_type(prim: common::PrimitiveType) -> common::TypeDesc {
            common::TypeDesc {
                kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                    r#type: prim as i32,
                    len: None,
                    precision: None,
                    scale: None,
                    time_unit: None,
                })),
            }
        }

        fn native_output_column(column_id: u32, name: &str) -> common::OutputColumn {
            common::OutputColumn {
                column_id,
                name: name.to_string(),
                r#type: Some(native_scalar_type(common::PrimitiveType::Bigint)),
                nullable: false,
                is_internal: false,
            }
        }

        let root_a = native_output_column(1, "a");
        let root_b = native_output_column(2, "b");
        let root_c = native_output_column(3, "c");
        let mut fragment = native_plan::PlanFragment {
            fragment_id: 1,
            root: Some(native_plan::DistributedNode {
                node_id: 5,
                fragment_id: 1,
                tuple_ids: Vec::new(),
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                payload: Some(native_plan::distributed_node::Payload::Physical(
                    native_plan::PlanNode {
                        output_columns: vec![root_a, root_b, root_c],
                        kind: Some(native_plan::plan_node::Kind::Filter(
                            native_plan::FilterNode { predicate: None },
                        )),
                    },
                )),
            }),
            data_partition: Some(native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            }),
            output_partition: Some(native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            }),
            sink: None,
            output_exprs: Vec::new(),
            output_columns: vec![native_output_column(6, "a"), native_output_column(8, "c")],
            cte_id: Some(3),
            cte_exchange_nodes: Vec::new(),
        };
        let consumers = vec![(
            2,
            77,
            native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            },
            vec![6, 8],
            vec![ColumnId::new_for_test(1), ColumnId::new_for_test(3)],
        )];
        let destinations = BTreeMap::from([(2, vec![fake_destination(200, "10.0.0.20")])]);

        patch_native_cte_multicast_sink(&mut fragment, 1, 3, &consumers, &destinations)
            .expect("patch native cte sink");

        let Some(native_plan::data_sink::Kind::MultiCastDataStream(sink)) =
            fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected native multicast sink");
        };
        assert_eq!(sink.sinks.len(), 1);
        assert_eq!(sink.sinks[0].output_columns, vec![1, 3]);
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
            None,
            Some(&write),
            false,
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
            None,
            Some(&write),
            false,
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
            None,
            Some(&write),
            false,
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
            None,
            false,
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
    fn collect_profiles_waits_for_final_report_after_root_eof() {
        let query_id = id(787, 1);
        let root_finst_id = types::TUniqueId::new(787, 10);
        let (eof_tx, eof_rx) = std::sync::mpsc::channel();
        let inner = EofSignalDispatcher::new(eof_tx);
        let dispatcher: Arc<dyn FragmentDispatcher> = inner;
        let report_query_id = query_id.clone();
        let report_finst_id = root_finst_id.clone();
        let report_thread = std::thread::spawn(move || {
            eof_rx.recv().expect("root EOF signal");
            let accepted = record_standalone_query_profile_report(&profile_report_params(
                report_query_id,
                report_finst_id,
                profile_tree_for_plan_node(2),
            ))
            .expect("record profile report");
            assert!(accepted, "profile report must match active query collector");
        });

        let params = single_backend(vec![make_params_with_finst(787, 10)]);
        let mut tracker = InFlightTracker::default();
        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            &query_id,
            1_000,
            None,
            None,
            true,
        )
        .expect("profile final report should be collected after root EOF");

        report_thread.join().expect("profile report thread");
        assert_eq!(result.fragment_profiles.len(), 1);
        assert!(result.write_commit.is_none());
        assert!(result.write_abort.is_none());
    }

    #[test]
    fn duplicate_profile_reports_for_same_fragment_count_once() {
        let query_id = id(788, 1);
        let finst_id = id(788, 10);
        let _guard = StandaloneQueryProfileGuard::register(&query_id);
        let params = profile_report_params(
            query_id.clone(),
            finst_id.clone(),
            profile_tree_for_plan_node(2),
        );

        assert!(
            record_standalone_query_profile_report(&params).expect("first thrift profile report")
        );
        assert!(
            record_standalone_query_profile_report(&params)
                .expect("duplicate thrift profile report")
        );
        assert_eq!(
            standalone_query_profile_count(&query_id),
            1,
            "duplicate thrift profile reports from the same finst must be idempotent"
        );

        let taken = take_standalone_query_profiles(&query_id);
        assert_eq!(taken.len(), 1);

        let native_report = crate::proto::novarocks::ExecStatusReport {
            query_id: Some(crate::proto::common::UniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            }),
            fragment_instance_id: Some(crate::proto::common::UniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            backend_num: 0,
            status: Some(crate::proto::common::Status {
                code: 0,
                message: String::new(),
            }),
            done: true,
            iceberg_commits: Vec::new(),
            loaded_rows: 0,
            sink_load_bytes: 0,
            filtered_rows: 0,
            profile: Some(crate::runtime::profile::RuntimeProfile::new("FragmentRoot").to_proto()),
        };
        assert!(
            record_native_standalone_query_profile_report(&native_report)
                .expect("first native profile report")
        );
        assert!(
            record_native_standalone_query_profile_report(&native_report)
                .expect("duplicate native profile report")
        );
        assert_eq!(
            standalone_query_profile_count(&query_id),
            1,
            "duplicate native profile reports from the same finst must be idempotent"
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
            None,
            Some(&write),
            false,
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
            None,
            Some(&write),
            false,
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
            fragment_profiles: Vec::new(),
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
            None,
            false,
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
            None,
            false,
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
            None,
            false,
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
            None,
            false,
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
            None,
            false,
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
            None,
            false,
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
                    None,
                    false,
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
