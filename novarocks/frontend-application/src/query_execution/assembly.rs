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

//! Role-neutral native submission assembly helpers.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatchOptions};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::query_execution::native_fragment::NativeFragmentAttachment;
use crate::query_execution::preparation::{
    PreparedFragmentRole, PreparedFragmentSet, PreparedOutputColumn,
};
use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
use novarocks_execution::exec::chunk::Chunk;
use novarocks_sql::plan_read::{ColumnId, CteId, FragmentEdge, FragmentEdgeKind, FragmentId};
use tracing::debug;

pub(crate) fn align_fetch_chunks_to_output_columns(
    chunks: Vec<Chunk>,
    output_columns: &[PreparedOutputColumn],
) -> Result<Vec<Chunk>, String> {
    chunks
        .into_iter()
        .map(|chunk| align_fetch_chunk_to_output_columns(chunk, output_columns))
        .collect()
}

fn align_fetch_chunk_to_output_columns(
    chunk: Chunk,
    output_columns: &[PreparedOutputColumn],
) -> Result<Chunk, String> {
    let row_count = chunk.batch.num_rows();
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
        if let Err(mismatch) = novarocks_execution::exec::chunk::type_compatibility::check_exact(
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

    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
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
    if novarocks_execution::exec::chunk::type_compatibility::check_exact(
        output_type,
        array.data_type(),
    )
    .is_ok()
    {
        return Ok(array);
    }
    if !same_unit_timestamp_metadata_mismatch(output_type, array.data_type()) {
        return Ok(array);
    }
    novarocks_execution::exec::chunk::type_compatibility::retag_column(&array, output_type).map_err(|mismatch| {
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

// Index each plain `Stream` producer fragment to its single outgoing stream
// edge. This is an infallible projection of the sealed edge set: the planner
// seal (`validate_source_edge_shape`) already rejects plain-stream fan-out and
// any plain/router mix, so at most one plain stream edge exists per source and
// the insert never overwrites. Re-adding a shape check here would duplicate a
// planner-owned decision (guarded by `planner_topology_contract`).
pub fn build_stream_edge_by_source(edges: &[FragmentEdge]) -> BTreeMap<FragmentId, &FragmentEdge> {
    let mut stream_edge_by_source = BTreeMap::new();
    for edge in edges {
        if !matches!(edge.edge_kind, FragmentEdgeKind::Stream) {
            continue;
        }
        stream_edge_by_source.insert(edge.source_fragment_id, edge);
    }
    stream_edge_by_source
}

// Group Iceberg change-stream router edges by (source fragment, router group).
// This is an infallible projection of the sealed edge set: the planner seal
// (`validate_source_edge_shape`) already owns plain/router mix rejection and the
// per-(source, group) branch_id / branch_kind / target-exchange uniqueness that
// this used to re-check, so grouping here only collects the sealed branches. Re-
// adding a shape check here would duplicate a planner-owned decision (guarded by
// `planner_topology_contract`).
pub fn group_router_edges_by_source(
    edges: &[FragmentEdge],
) -> BTreeMap<(FragmentId, i32), Vec<&FragmentEdge>> {
    let mut grouped: BTreeMap<(FragmentId, i32), Vec<&FragmentEdge>> = BTreeMap::new();
    for edge in edges {
        let FragmentEdgeKind::ChangeStreamRouter {
            router_group_id, ..
        } = edge.edge_kind
        else {
            continue;
        };
        grouped
            .entry((edge.source_fragment_id, router_group_id))
            .or_default()
            .push(edge);
    }
    grouped
}
pub fn ensure_native_fragment_sink_supported(
    fragment_id: FragmentId,
    is_root: bool,
    has_stream_edge: bool,
    has_router_edges: bool,
    has_cte_id: bool,
) -> Result<(), String> {
    if is_root || has_stream_edge || has_router_edges || has_cte_id {
        return Ok(());
    }

    let dynamic_sink = "dynamic fragment sink";
    Err(format!(
        "native submission cannot encode {dynamic_sink} for fragment {fragment_id}; \
         the native sink contract must carry dynamic destinations before this fragment can be submitted"
    ))
}

#[allow(
    dead_code,
    reason = "Retained for staged query-execution contract and lifecycle integration."
)]
pub(crate) fn validate_fragment_output_kind(
    fragment_id: FragmentId,
    is_root: bool,
    is_producer: bool,
    output_kind: PreparedFragmentRole,
) -> Result<(), String> {
    if is_root {
        return match output_kind {
            PreparedFragmentRole::Result => Ok(()),
            PreparedFragmentRole::NonTerminal => Err(format!(
                "root fragment {fragment_id} must have Result output kind"
            )),
        };
    }
    if is_producer {
        return (output_kind == PreparedFragmentRole::NonTerminal)
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "producer fragment {fragment_id} must have NonTerminal output kind, got {output_kind:?}"
                )
            });
    }
    Ok(())
}

pub(crate) fn validate_prepared_native_payloads(
    prepared: &PreparedFragmentSet,
    native_bundle: &NativeFragmentAttachment,
) -> Result<(), String> {
    let prepared_ids = prepared.fragment_ids();
    for (fragment_id, fragment) in native_bundle.fragments_in_id_order() {
        if fragment.fragment_id != fragment_id {
            return Err(format!(
                "native fragment bundle key {fragment_id} does not match encoded fragment id {}",
                fragment.fragment_id
            ));
        }
    }
    for fragment_id in &prepared_ids {
        native_bundle.get(*fragment_id).ok_or_else(|| {
            format!("native fragment bundle missing prepared fragment id={fragment_id}")
        })?;
        let fragment = prepared
            .fragment(*fragment_id)
            .ok_or_else(|| format!("prepared fragment set missing id={fragment_id}"))?;
        for (index, boundary) in fragment
            .boundary_projection()
            .contracts()
            .iter()
            .enumerate()
        {
            if !prepared_ids.contains(&boundary.fragment_id) {
                return Err(format!(
                    "prepared boundary {index} for fragment {fragment_id} references missing fragment id={}",
                    boundary.fragment_id
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_artifact_fragment_sets(
    prepared: &PreparedFragmentSet,
    native_bundle: &NativeFragmentAttachment,
    scheduling: &SchedulingPlan,
) -> Result<(), String> {
    let expected = prepared.fragment_ids();
    let native = native_bundle.fragment_ids().collect::<BTreeSet<_>>();
    if native != expected {
        return Err(fragment_set_mismatch("native", &expected, &native));
    }
    let scheduled = scheduling.fragment_ids().collect::<BTreeSet<_>>();
    if scheduled != expected {
        return Err(fragment_set_mismatch("scheduled", &expected, &scheduled));
    }
    Ok(())
}

fn fragment_set_mismatch(
    label: &str,
    expected: &BTreeSet<FragmentId>,
    actual: &BTreeSet<FragmentId>,
) -> String {
    let missing = expected.difference(actual).copied().collect::<Vec<_>>();
    let unknown = actual.difference(expected).copied().collect::<Vec<_>>();
    format!(
        "{label} fragment ids mismatch: expected={expected:?} actual={actual:?} missing={missing:?} unknown={unknown:?}"
    )
}

pub(crate) fn validate_scheduling_placements(plan: &SchedulingPlan) -> Result<(), String> {
    for (&fragment_id, placements) in &plan.by_fragment {
        if placements.is_empty() {
            return Err(format!(
                "native scheduling plan fragment {fragment_id} has no placements"
            ));
        }
        for (placement_index, placement) in placements.iter().enumerate() {
            if placement.fragment_id != fragment_id {
                return Err(format!(
                    "native scheduling plan map key {fragment_id} does not match placement \
                     {placement_index} fragment_id {}",
                    placement.fragment_id
                ));
            }
        }
    }
    Ok(())
}

/// Applies only placement to an already-encoded provider-neutral connector
/// source. The traversal deliberately has no provider branch and never
pub fn patch_native_change_stream_router_sink(
    fragment: &mut novarocks_proto_models::plan::PlanFragment,
    fragment_id: FragmentId,
    router_group_id: i32,
    branch_edges: &[&FragmentEdge],
    source: &FragmentInstancePlacement,
    placements: &BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
) -> Result<(), String> {
    let mut patched_fragment = fragment.clone();
    patch_native_change_stream_router_sink_in_place(
        &mut patched_fragment,
        fragment_id,
        router_group_id,
        branch_edges,
        source,
        placements,
    )?;
    *fragment = patched_fragment;
    Ok(())
}

fn patch_native_change_stream_router_sink_in_place(
    fragment: &mut novarocks_proto_models::plan::PlanFragment,
    fragment_id: FragmentId,
    router_group_id: i32,
    branch_edges: &[&FragmentEdge],
    source: &FragmentInstancePlacement,
    placements: &BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
) -> Result<(), String> {
    if branch_edges.is_empty() {
        return Err("native Iceberg change-stream router sink has no branch edges".to_string());
    }
    let router = match fragment.sink.as_mut().and_then(|sink| sink.kind.as_mut()) {
        Some(novarocks_proto_models::plan::data_sink::Kind::ChangeStreamRouter(router)) => router,
        _ => {
            return Err(format!(
                "fragment {fragment_id} is router source for group {router_group_id} but native \
                 fragment payload is missing CHANGE_STREAM_ROUTER_SINK"
            ));
        }
    };

    if router.group_id != router_group_id {
        return Err(format!(
            "native Iceberg change-stream router source={fragment_id} expected group={router_group_id} \
             but encoded group={}",
            router.group_id
        ));
    }

    let mut edge_route_ids = BTreeSet::new();
    for edge in branch_edges {
        let FragmentEdgeKind::ChangeStreamRouter {
            router_group_id: edge_group_id,
            route_id,
        } = &edge.edge_kind
        else {
            return Err(format!(
                "fragment {} edge to fragment {} is not an Iceberg change-stream router edge",
                edge.source_fragment_id, edge.target_fragment_id
            ));
        };
        if *edge_group_id != router_group_id {
            return Err(format!(
                "native Iceberg change-stream router source={} expected group={} but edge uses group={}",
                fragment_id, router_group_id, edge_group_id
            ));
        }
        if !edge_route_ids.insert(route_id.to_bytes()) {
            return Err(format!(
                "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
                 has duplicate opaque route id"
            ));
        }
    }

    let mut encoded_route_ids = BTreeSet::new();
    for route in &router.routes {
        let route_id: [u8; 32] = route.route_id.as_slice().try_into().map_err(|_| {
            format!("native row-mutation router source={fragment_id} group={router_group_id} has non-32-byte route id")
        })?;
        if !encoded_route_ids.insert(route_id) {
            return Err(format!(
                "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
                 has duplicate encoded opaque route id"
            ));
        }
    }

    if encoded_route_ids != edge_route_ids {
        return Err(format!(
            "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
             route id set mismatch: encoded={encoded_route_ids:?}, branch_edges={edge_route_ids:?}"
        ));
    }

    for edge in branch_edges {
        let FragmentEdgeKind::ChangeStreamRouter {
            router_group_id: edge_group_id,
            route_id,
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
            .routes
            .iter_mut()
            .find(|route| route.route_id.as_slice() == route_id.to_bytes())
            .ok_or_else(|| {
                format!(
                    "native row-mutation router source={} group={} has no matching route",
                    fragment_id, router_group_id
                )
            })?;
        route.target_fragment_id = edge.target_fragment_id;
        route.target_exchange_node_id = edge.target_exchange_node_id;

        if route.output_partition.is_none() {
            return Err(format!(
                "native row-mutation router source={} group={} missing output_partition from native encoder",
                fragment_id, router_group_id
            ));
        }

        let dests = placements.get(&edge.target_fragment_id).ok_or_else(|| {
            format!(
                "native row-mutation router source={} group={} target \
                 fragment {} has no placements",
                fragment_id, router_group_id, edge.target_fragment_id
            )
        })?;
        let sources = placements.get(&fragment_id).ok_or_else(|| {
            format!("native row-mutation router source={fragment_id} has no source placements")
        })?;
        route.destinations = Some(novarocks_proto_models::plan::StreamDestinationList {
            destinations: native_destinations_for_source(source, sources, dests)?
                .iter()
                .map(native_stream_destination)
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

type CteMulticastConsumer = (
    FragmentId,
    i32,
    novarocks_proto_models::plan::DataPartition,
    Vec<i32>,
    Vec<ColumnId>,
);

pub fn patch_native_cte_multicast_sink(
    fragment: &mut novarocks_proto_models::plan::PlanFragment,
    fragment_id: FragmentId,
    cte_id: CteId,
    consumers: &[CteMulticastConsumer],
    source: &FragmentInstancePlacement,
    placements: &BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
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
        sinks.push(novarocks_proto_models::plan::DataStreamSink {
            dest_node_id: *exchange_node_id,
            output_partition: Some(partition.clone()),
            output_columns: sink_output_columns,
            limit: None,
        });
        let dests = placements.get(consumer_fragment_id).ok_or_else(|| {
            format!("CTE consumer fragment {consumer_fragment_id} has no placements")
        })?;
        let sources = placements
            .get(&fragment_id)
            .ok_or_else(|| format!("CTE producer fragment {fragment_id} has no placements"))?;
        destinations.push(novarocks_proto_models::plan::StreamDestinationList {
            destinations: native_destinations_for_source(source, sources, dests)?
                .iter()
                .map(native_stream_destination)
                .collect(),
        });
    }
    fragment.sink = Some(novarocks_proto_models::plan::DataSink {
        kind: Some(
            novarocks_proto_models::plan::data_sink::Kind::MultiCastDataStream(
                novarocks_proto_models::plan::MultiCastDataStreamSink {
                    sinks,
                    destinations,
                },
            ),
        ),
    });
    debug!(
        "patched native CTE multicast sink: fragment={} cte_id={} sinks={}",
        fragment_id,
        cte_id,
        consumers.len()
    );
    Ok(())
}

fn native_stream_destination(
    src: &novarocks_execution::runtime::endpoint::FragmentDestination,
) -> novarocks_proto_models::plan::StreamDestination {
    novarocks_proto_models::plan::StreamDestination {
        finst_id: Some(novarocks_proto_models::common::UniqueId {
            hi: src.finst_id().high(),
            lo: src.finst_id().low(),
        }),
        endpoint: src.endpoint().as_host_port(),
        source_finst_id: Some(novarocks_proto_models::common::UniqueId {
            hi: src.source_finst_id().high(),
            lo: src.source_finst_id().low(),
        }),
        sender_ordinal: src.sender_ordinal(),
        sender_count: src.sender_count(),
    }
}

fn native_destinations_for_source(
    source: &FragmentInstancePlacement,
    sources: &[FragmentInstancePlacement],
    destinations: &[FragmentInstancePlacement],
) -> Result<Vec<novarocks_execution::runtime::endpoint::FragmentDestination>, String> {
    let sender_count = u32::try_from(sources.len())
        .map_err(|_| "native exchange sender count exceeds u32 width".to_string())?;
    let sender_ordinal = sources
        .iter()
        .position(|candidate| candidate.finst_id == source.finst_id)
        .ok_or_else(|| {
            "native exchange source placement is absent from its fragment schedule".to_string()
        })?;
    let sender_ordinal = u32::try_from(sender_ordinal)
        .map_err(|_| "native exchange sender ordinal exceeds u32 width".to_string())?;
    destinations
        .iter()
        .map(|destination| {
            novarocks_execution::runtime::endpoint::FragmentDestination::new(
                destination.finst_id,
                destination.endpoint.clone(),
                source.finst_id,
                sender_ordinal,
                sender_count,
            )
        })
        .collect()
}

fn native_cte_multicast_sink_output_columns(
    fragment: &novarocks_proto_models::plan::PlanFragment,
    cte_id: CteId,
    consumer_fragment_id: FragmentId,
    exchange_node_id: i32,
    requested_output_slot_ids: &[i32],
    receive_producer_column_ids: &[ColumnId],
) -> Result<Vec<i32>, String> {
    if requested_output_slot_ids.is_empty() {
        return Ok(Vec::new());
    }

    // The CTE producer fragment is sealed with `DataSink::Noop`, so the planner
    // seal (CGO-9C Task 2, `finalize_fragment_output_columns`) adopts the
    // producer root's wire output wholesale into `fragment.output_columns`. That
    // sealed contract is the authoritative producer-root output; read it directly
    // rather than re-walking the encoded tree (the retired
    // `encoded_fragment_root_output_columns` read-walk, deleted in CGO-9C Task 5).
    let root_columns = fragment.output_columns.clone();
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
    fragment: &novarocks_proto_models::plan::PlanFragment,
    root_columns: &[novarocks_proto_models::common::OutputColumn],
    root_slot_id_set: &BTreeSet<i32>,
) -> BTreeMap<i32, i32> {
    let mut map = BTreeMap::new();

    if fragment.output_exprs.len() == fragment.output_columns.len() {
        for (output, expr) in fragment
            .output_columns
            .iter()
            .zip(fragment.output_exprs.iter())
        {
            let Some(novarocks_proto_models::expr::expr::Kind::ColumnRef(column_ref)) =
                expr.kind.as_ref()
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::{Decimal128Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_spi::connector::ConnectorWriteRouteId;

    use super::*;
    use novarocks_execution::exec::chunk::ChunkSchema;
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_proto_models::plan as native_plan;
    use novarocks_sql::plan_read::{DataPartition, FragmentStreamKind};
    use novarocks_types::SlotId;
    use novarocks_types::UniqueId;

    fn placement(fragment_id: FragmentId, instance_lo: i64) -> FragmentInstancePlacement {
        FragmentInstancePlacement {
            fragment_id,
            instance_index: 0,
            finst_id: UniqueId::new(92_000, instance_lo),
            backend_idx: 0,
            endpoint: RuntimeEndpoint::new("10.0.0.2", 9030).unwrap(),
            scan_ranges: BTreeMap::new(),
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        }
    }

    fn router_edge(target_fragment_id: FragmentId) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id: 1,
            target_fragment_id,
            target_exchange_node_id: 77,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::ChangeStreamRouter {
                router_group_id: 7,
                route_id: ConnectorWriteRouteId::from_bytes([7; 32]),
            },
            output_slot_ids: vec![10],
        }
    }

    fn router_fragment() -> native_plan::PlanFragment {
        native_plan::PlanFragment {
            fragment_id: 1,
            sink: Some(native_plan::DataSink {
                kind: Some(native_plan::data_sink::Kind::ChangeStreamRouter(
                    native_plan::ChangeStreamRouterSink {
                        group_id: 7,
                        effect_output_ordinal: 0,
                        routes: vec![native_plan::ChangeStreamBranchRoute {
                            target_fragment_id: 0,
                            target_exchange_node_id: -1,
                            output_partition_ordinals: Vec::new(),
                            output_partition: Some(native_plan::DataPartition {
                                kind: native_plan::PartitionKind::Unpartitioned as i32,
                                exprs: Vec::new(),
                            }),
                            destinations: None,
                            route_id: vec![7; 32],
                            write_target_ordinal: 0,
                            accepted_effects: vec![native_plan::RowMutationEffect::Delete as i32],
                            input_ordinals: vec![0],
                        }],
                    },
                )),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn validates_native_sink_and_output_roles() {
        ensure_native_fragment_sink_supported(7, false, true, false, false)
            .expect("stream sink is supported");
        ensure_native_fragment_sink_supported(8, false, false, true, false)
            .expect("router sink is supported");
        ensure_native_fragment_sink_supported(9, false, false, false, true)
            .expect("CTE multicast sink is supported");
        assert!(
            ensure_native_fragment_sink_supported(10, false, false, false, false)
                .expect_err("unowned dynamic sink must be rejected")
                .contains("dynamic fragment sink")
        );

        validate_fragment_output_kind(1, true, false, PreparedFragmentRole::Result)
            .expect("result root");
        assert!(
            validate_fragment_output_kind(1, true, false, PreparedFragmentRole::NonTerminal)
                .expect_err("root cannot be nonterminal")
                .contains("root fragment 1")
        );
        assert!(
            validate_fragment_output_kind(2, false, true, PreparedFragmentRole::Result)
                .expect_err("producer must be nonterminal")
                .contains("producer fragment 2")
        );
    }

    #[test]
    fn rejects_invalid_scheduling_placements_before_assembly() {
        let empty = SchedulingPlan {
            root_fragment_id: 7,
            by_fragment: BTreeMap::from([(3, Vec::new()), (7, vec![placement(7, 7)])]),
            root_finst_id: UniqueId::new(92_000, 7),
            root_backend_idx: 0,
        };
        assert!(
            validate_scheduling_placements(&empty)
                .expect_err("empty placements must fail")
                .contains("fragment 3 has no placements")
        );

        let drift = SchedulingPlan {
            root_fragment_id: 7,
            by_fragment: BTreeMap::from([(7, vec![placement(8, 7)])]),
            root_finst_id: UniqueId::new(92_000, 7),
            root_backend_idx: 0,
        };
        let error =
            validate_scheduling_placements(&drift).expect_err("placement id drift must fail");
        assert!(error.contains("map key 7"), "{error}");
        assert!(error.contains("fragment_id 8"), "{error}");
    }

    #[test]
    fn aligns_typed_root_fields_and_rejects_decimal_drift() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wire_i",
            DataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(7)])
                .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap();
        let aligned = align_fetch_chunks_to_output_columns(
            vec![chunk],
            &[PreparedOutputColumn {
                name: "col1".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
        )
        .unwrap();
        assert_eq!(aligned[0].batch.schema().field(0).name(), "col1");
        assert!(aligned[0].batch.schema().field(0).is_nullable());

        let decimal = Decimal128Array::from(vec![Some(100_i128)])
            .with_precision_and_scale(38, 2)
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wire_price",
            DataType::Decimal128(38, 2),
            false,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(decimal)]).unwrap();
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(8)])
                .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap();
        assert!(
            align_fetch_chunks_to_output_columns(
                vec![chunk],
                &[PreparedOutputColumn {
                    name: "price".to_string(),
                    data_type: DataType::Decimal128(20, 2),
                    nullable: false,
                }],
            )
            .expect_err("decimal precision drift must fail")
            .contains("type mismatch")
        );
    }

    #[test]
    fn patches_cte_multicast_from_sealed_root_output_slots() {
        let mut fragment = native_plan::PlanFragment {
            fragment_id: 1,
            output_columns: vec![novarocks_proto_models::common::OutputColumn {
                column_id: 10,
                name: "total".to_string(),
                r#type: None,
                nullable: true,
                is_internal: false,
            }],
            ..Default::default()
        };
        let source = placement(1, 1);
        let destination = placement(2, 1);
        patch_native_cte_multicast_sink(
            &mut fragment,
            1,
            3,
            &[(
                2,
                77,
                native_plan::DataPartition {
                    kind: native_plan::PartitionKind::Unpartitioned as i32,
                    exprs: Vec::new(),
                },
                vec![13],
                vec![ColumnId(13)],
            )],
            &source,
            &BTreeMap::from([(1, vec![source.clone()]), (2, vec![destination])]),
        )
        .expect("CTE patch");
        let Some(native_plan::data_sink::Kind::MultiCastDataStream(sink)) =
            fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("native multicast sink");
        };
        assert_eq!(sink.sinks[0].output_columns, vec![10]);
        assert_eq!(
            sink.destinations[0].destinations[0].endpoint,
            "10.0.0.2:9030"
        );
    }

    #[test]
    fn rejects_router_validation_drift_without_mutating_the_fragment() {
        let mut fragment = router_fragment();
        let before = fragment.clone();
        let edge = router_edge(2);
        let error = patch_native_change_stream_router_sink(
            &mut fragment,
            1,
            7,
            &[&edge],
            &placement(1, 1),
            &BTreeMap::new(),
        )
        .expect_err("missing target placement must fail before patching");
        assert!(
            error.contains("target fragment 2 has no placements"),
            "{error}"
        );
        assert_eq!(fragment, before, "router patch must be atomic");
    }
}
