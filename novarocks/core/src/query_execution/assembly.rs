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
use novarocks_spi::connector::{ConnectorSplit, ConnectorWriterHandle};

use crate::exec::chunk::Chunk;
use crate::novarocks_logging::debug;
use crate::protocol::native::encode::NativeFragmentBundle;
use crate::query_execution::preparation::{
    PreparedFragmentRole, PreparedFragmentSet, PreparedOutputColumn,
};
use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
use crate::sql::analysis::cte::CteId;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::{FragmentEdge, FragmentEdgeKind, FragmentId};

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

// Index each plain `Stream` producer fragment to its single outgoing stream
// edge. This is an infallible projection of the sealed edge set: the planner
// seal (`validate_source_edge_shape`) already rejects plain-stream fan-out and
// any plain/router mix, so at most one plain stream edge exists per source and
// the insert never overwrites. Re-adding a shape check here would duplicate a
// planner-owned decision (guarded by `planner_topology_contract`).
pub(crate) fn build_stream_edge_by_source<'a>(
    edges: &'a [FragmentEdge],
) -> BTreeMap<FragmentId, &'a FragmentEdge> {
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
pub(crate) fn group_router_edges_by_source<'a>(
    edges: &'a [FragmentEdge],
) -> BTreeMap<(FragmentId, i32), Vec<&'a FragmentEdge>> {
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
pub(crate) fn ensure_native_fragment_sink_supported(
    fragment_id: FragmentId,
    is_root: bool,
    is_terminal_write: bool,
    has_stream_edge: bool,
    has_router_edges: bool,
    has_cte_id: bool,
) -> Result<(), String> {
    if is_root || is_terminal_write || has_stream_edge || has_router_edges || has_cte_id {
        return Ok(());
    }

    let dynamic_sink = "dynamic fragment sink";
    Err(format!(
        "native submission cannot encode {dynamic_sink} for fragment {fragment_id}; \
         the native sink contract must carry dynamic destinations before this fragment can be submitted"
    ))
}

/// Install one placement-local provider-neutral writer handle planned by the
/// frontend control binding.
///
/// Native fragment templates are encoded before scheduling.  The writer
/// identity therefore cannot be valid until the placement has been frozen.
/// This is the only seam that installs the per-placement opaque handle; it
/// neither decodes its provider payload nor permits a fallback to an
/// unplanned handle.
pub(crate) fn patch_native_connector_write_sink(
    fragment: &mut crate::proto::plan::PlanFragment,
    fragment_id: FragmentId,
    fragment_instance_id: crate::common::types::UniqueId,
    backend_num: i32,
    handle: &ConnectorWriterHandle,
) -> Result<(), String> {
    let mut patched_fragment = fragment.clone();
    patch_native_connector_write_sink_in_place(
        &mut patched_fragment,
        fragment_id,
        fragment_instance_id,
        backend_num,
        handle,
    )?;
    *fragment = patched_fragment;
    Ok(())
}

fn patch_native_connector_write_sink_in_place(
    fragment: &mut crate::proto::plan::PlanFragment,
    fragment_id: FragmentId,
    fragment_instance_id: crate::common::types::UniqueId,
    backend_num: i32,
    handle: &ConnectorWriterHandle,
) -> Result<(), String> {
    let writer = handle.writer();
    if writer.fragment_id()
        != i32::try_from(fragment_id)
            .map_err(|_| format!("connector write fragment {fragment_id} exceeds i32 width"))?
        || writer.backend_num() != backend_num
        || writer.fragment_instance_id() != unique_id_bytes(fragment_instance_id)
        || writer.sink_ordinal() != 0
    {
        return Err(format!(
            "connector writer handle does not match scheduled placement: fragment={fragment_id} backend_num={backend_num} finst={fragment_instance_id:?}"
        ));
    }

    let sink = fragment
        .sink
        .as_mut()
        .ok_or_else(|| format!("terminal write fragment {fragment_id} has no native sink"))?;
    let template = match sink.kind.take() {
        Some(crate::proto::plan::data_sink::Kind::ConnectorWrite(template)) => template,
        Some(other) => {
            sink.kind = Some(other);
            return Err(format!(
                "terminal write fragment {fragment_id} requires CONNECTOR_WRITE template before connector write patch"
            ));
        }
        None => {
            return Err(format!(
                "terminal write fragment {fragment_id} has an empty native sink"
            ));
        }
    };
    if template.handle.is_some() {
        return Err(format!(
            "terminal connector write fragment {fragment_id} already has a writer handle"
        ));
    }
    let input = template.input.ok_or_else(|| {
        format!("terminal connector write fragment {fragment_id} is missing input binding")
    })?;
    let input = match input.kind {
        Some(crate::proto::plan::connector_write_input_binding::Kind::RootOutputByOrdinal(
            value,
        )) => crate::proto::plan::ConnectorWriteInputBinding {
            kind: Some(
                crate::proto::plan::connector_write_input_binding::Kind::RootOutputByOrdinal(value),
            ),
        },
        Some(crate::proto::plan::connector_write_input_binding::Kind::OutputOrdinals(values)) => {
            crate::proto::plan::ConnectorWriteInputBinding {
                kind: Some(
                    crate::proto::plan::connector_write_input_binding::Kind::OutputOrdinals(values),
                ),
            }
        }
        None => {
            return Err(format!(
                "terminal connector write fragment {fragment_id} has an empty input binding"
            ));
        }
    };
    sink.kind = Some(crate::proto::plan::data_sink::Kind::ConnectorWrite(
        crate::proto::plan::ConnectorWriteFragmentSink {
            handle: Some(crate::proto::plan::ConnectorWriterHandleEnvelope {
                contract_version: handle.version(),
                writer: Some(crate::proto::plan::ConnectorWriterIdentity {
                    operation_id: writer.operation_id().to_bytes().to_vec(),
                    cohort_id: writer.cohort_id().to_bytes().to_vec(),
                    execution_query_id: writer.execution_id().query_id().to_vec(),
                    execution_attempt_id: writer.execution_id().attempt_id(),
                    fragment_instance_id: Some(native_unique_id(writer.fragment_instance_id())),
                    fragment_id: writer.fragment_id(),
                    backend_num: writer.backend_num(),
                    sink_ordinal: writer.sink_ordinal(),
                    connector_instance_id: writer.binding_key().instance_id.as_str().to_string(),
                    connector_incarnation: writer.binding_key().incarnation.to_bytes().to_vec(),
                }),
                payload: handle.payload().to_vec(),
                payload_sha256: handle.payload_digest().to_vec(),
            }),
            input: Some(input),
        },
    ));
    Ok(())
}

fn unique_id_bytes(value: crate::common::types::UniqueId) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&value.high().to_be_bytes());
    bytes[8..].copy_from_slice(&value.low().to_be_bytes());
    bytes
}

fn native_unique_id(value: [u8; 16]) -> crate::proto::common::UniqueId {
    crate::proto::common::UniqueId {
        hi: i64::from_be_bytes(value[..8].try_into().expect("fixed writer identity prefix")),
        lo: i64::from_be_bytes(value[8..].try_into().expect("fixed writer identity suffix")),
    }
}

pub(crate) fn validate_fragment_output_kind(
    fragment_id: FragmentId,
    is_root: bool,
    is_terminal_write: bool,
    is_producer: bool,
    output_kind: PreparedFragmentRole,
) -> Result<(), String> {
    if is_root {
        return match output_kind {
            PreparedFragmentRole::Result
            | PreparedFragmentRole::Statistics
            | PreparedFragmentRole::TerminalWrite => Ok(()),
            PreparedFragmentRole::NonTerminal => Err(format!(
                "root fragment {fragment_id} must have Result or TerminalWrite output kind"
            )),
        };
    }
    if is_terminal_write {
        return (output_kind == PreparedFragmentRole::TerminalWrite)
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "terminal write fragment {fragment_id} must have TerminalWrite output kind, got {output_kind:?}"
                )
            });
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
    native_bundle: &NativeFragmentBundle,
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
    native_bundle: &NativeFragmentBundle,
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
/// interprets a split payload.
pub(crate) fn patch_native_connector_read_splits(
    fragment: &mut crate::proto::plan::PlanFragment,
    node_id: i32,
    splits: &[ConnectorSplit],
) -> Result<(), String> {
    let root = fragment.root.as_mut().ok_or_else(|| {
        format!(
            "native connector split patch requires a root node for fragment {}",
            fragment.fragment_id
        )
    })?;
    let mut matches = 0usize;
    patch_connector_splits_in_node(root, node_id, splits, &mut matches)?;
    match matches {
        1 => Ok(()),
        0 => Err(format!(
            "native connector split patch could not find ConnectorReadSource node {node_id} in fragment {}",
            fragment.fragment_id
        )),
        count => Err(format!(
            "native connector split patch found ConnectorReadSource node {node_id} {count} times in fragment {}",
            fragment.fragment_id
        )),
    }
}

fn patch_connector_splits_in_node(
    node: &mut crate::proto::plan::DistributedNode,
    node_id: i32,
    splits: &[ConnectorSplit],
    matches: &mut usize,
) -> Result<(), String> {
    if node.node_id == node_id {
        let source = node
            .payload
            .as_mut()
            .and_then(|payload| match payload {
                crate::proto::plan::distributed_node::Payload::Physical(physical) => {
                    physical.kind.as_mut()
                }
                crate::proto::plan::distributed_node::Payload::Exchange(_) => None,
            })
            .and_then(|kind| match kind {
                crate::proto::plan::plan_node::Kind::Scan(scan) => scan.table.as_mut(),
                _ => None,
            })
            .and_then(|table| table.source.as_mut())
            .and_then(|source| source.kind.as_mut());
        let Some(crate::proto::plan::scan_source::Kind::ConnectorRead(source)) = source else {
            return Err(format!(
                "native connector split patch node {node_id} is not a ConnectorReadSource"
            ));
        };
        source.splits = splits
            .iter()
            .map(|split| crate::proto::plan::ConnectorReadSplit {
                split_id: split.split_id().to_string(),
                split_payload: split.payload().to_vec(),
                estimated_bytes: split.estimated_bytes(),
            })
            .collect();
        *matches = matches.saturating_add(1);
    }
    for child in &mut node.children {
        patch_connector_splits_in_node(child, node_id, splits, matches)?;
    }
    Ok(())
}

pub(crate) fn patch_native_change_stream_router_sink(
    fragment: &mut crate::proto::plan::PlanFragment,
    fragment_id: FragmentId,
    router_group_id: i32,
    branch_edges: &[&FragmentEdge],
    placements: &BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
) -> Result<(), String> {
    let mut patched_fragment = fragment.clone();
    patch_native_change_stream_router_sink_in_place(
        &mut patched_fragment,
        fragment_id,
        router_group_id,
        branch_edges,
        placements,
    )?;
    *fragment = patched_fragment;
    Ok(())
}

fn patch_native_change_stream_router_sink_in_place(
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
        Some(crate::proto::plan::data_sink::Kind::ChangeStreamRouter(router)) => router,
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

    let mut edge_route_keys = BTreeSet::new();
    for edge in branch_edges {
        let FragmentEdgeKind::ChangeStreamRouter {
            router_group_id: edge_group_id,
            branch_id,
            branch_kind,
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
        if !edge_route_keys.insert((*branch_id, *branch_kind)) {
            return Err(format!(
                "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
                 has duplicate branch edge route key branch_id={branch_id} branch_kind={branch_kind:?}"
            ));
        }
    }

    let mut encoded_route_keys = BTreeSet::new();
    for route in &router.branches {
        let branch_kind = native_change_stream_branch_kind(route.branch_kind).map_err(|err| {
            format!(
                "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
                 branch_id={} has invalid encoded branch kind: {err}",
                route.branch_id
            )
        })?;
        if !encoded_route_keys.insert((route.branch_id, branch_kind)) {
            return Err(format!(
                "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
                 has duplicate encoded route key branch_id={} branch_kind={branch_kind:?}",
                route.branch_id
            ));
        }
    }

    if encoded_route_keys != edge_route_keys {
        return Err(format!(
            "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
             route key set mismatch: encoded={encoded_route_keys:?}, branch_edges={edge_route_keys:?}"
        ));
    }

    for edge in branch_edges {
        let FragmentEdgeKind::ChangeStreamRouter {
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
                        placement.finst_id,
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

pub(crate) fn patch_native_cte_multicast_sink(
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

fn native_stream_destination(
    src: &crate::runtime::endpoint::FragmentDestination,
) -> crate::proto::plan::StreamDestination {
    crate::proto::plan::StreamDestination {
        finst_id: Some(crate::proto::common::UniqueId {
            hi: src.finst_id().high(),
            lo: src.finst_id().low(),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::{Decimal128Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorWriteExecutionId, ConnectorWriteOperationId, ConnectorWriterHandle,
        ConnectorWriterIdentity,
    };

    use super::*;
    use crate::common::ids::SlotId;
    use crate::common::types::UniqueId;
    use crate::exec::chunk::ChunkSchema;
    use crate::proto::plan as native_plan;
    use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
    use crate::sql::common::ChangeStreamBranchKind;
    use crate::sql::planner::distributed::{DataPartition, FragmentStreamKind};

    fn placement(fragment_id: FragmentId, instance_lo: i64) -> FragmentInstancePlacement {
        FragmentInstancePlacement {
            fragment_id,
            instance_index: 0,
            finst_id: UniqueId::new(92_000, instance_lo),
            backend_idx: 0,
            endpoint: RuntimeEndpoint::new("10.0.0.2", 9030).unwrap(),
            scan_ranges: BTreeMap::new(),
            connector_splits: BTreeMap::new(),
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
                branch_id: 0,
                branch_kind: ChangeStreamBranchKind::DeleteDv,
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
                        change_op_output_ordinal: 0,
                        data_route_output_ordinal: None,
                        branches: vec![native_plan::ChangeStreamBranchRoute {
                            branch_id: 0,
                            branch_kind: native_plan::ChangeStreamBranchKind::DeleteDv as i32,
                            target_fragment_id: 0,
                            target_exchange_node_id: -1,
                            output_ordinals: vec![0],
                            output_partition_ordinals: Vec::new(),
                            output_partition: Some(native_plan::DataPartition {
                                kind: native_plan::PartitionKind::Unpartitioned as i32,
                                exprs: Vec::new(),
                            }),
                            destinations: None,
                        }],
                    },
                )),
            }),
            ..Default::default()
        }
    }

    fn connector_writer_handle(
        fragment_id: FragmentId,
        finst_id: UniqueId,
    ) -> ConnectorWriterHandle {
        let owner = ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg").expect("valid instance"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        };
        let operation_id = ConnectorWriteOperationId::new();
        let writer = ConnectorWriterIdentity::new(
            operation_id,
            novarocks_spi::connector::ConnectorWriteCohortId::primary(operation_id),
            ConnectorWriteExecutionId::new([3; 16], 1),
            unique_id_bytes(finst_id),
            i32::try_from(fragment_id).expect("test fragment fits i32"),
            0,
            0,
            owner.clone(),
        );
        ConnectorWriterHandle::try_new(owner, writer, 1, Bytes::from_static(b"opaque"))
            .expect("valid connector handle")
    }

    fn connector_writer_template_fragment(fragment_id: FragmentId) -> native_plan::PlanFragment {
        native_plan::PlanFragment {
            fragment_id,
            sink: Some(native_plan::DataSink {
                kind: Some(native_plan::data_sink::Kind::ConnectorWrite(
                    native_plan::ConnectorWriteFragmentSink {
                        handle: None,
                        input: Some(native_plan::ConnectorWriteInputBinding {
                            kind: Some(
                                native_plan::connector_write_input_binding::Kind::RootOutputByOrdinal(
                                    true,
                                ),
                            ),
                        }),
                    },
                )),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn validates_native_sink_and_output_roles() {
        ensure_native_fragment_sink_supported(7, false, false, true, false, false)
            .expect("stream sink is supported");
        ensure_native_fragment_sink_supported(8, false, false, false, true, false)
            .expect("router sink is supported");
        ensure_native_fragment_sink_supported(9, false, false, false, false, true)
            .expect("CTE multicast sink is supported");
        assert!(
            ensure_native_fragment_sink_supported(10, false, false, false, false, false)
                .expect_err("unowned dynamic sink must be rejected")
                .contains("dynamic fragment sink")
        );

        validate_fragment_output_kind(1, true, false, false, PreparedFragmentRole::Result)
            .expect("result root");
        validate_fragment_output_kind(1, true, false, false, PreparedFragmentRole::Statistics)
            .expect("statistics root");
        assert!(validate_fragment_output_kind(
            1,
            true,
            false,
            false,
            PreparedFragmentRole::NonTerminal,
        )
        .expect_err("root cannot be nonterminal")
        .contains("root fragment 1"));
        assert!(
            validate_fragment_output_kind(2, false, false, true, PreparedFragmentRole::Result,)
                .expect_err("producer must be nonterminal")
                .contains("producer fragment 2")
        );
    }

    #[test]
    fn patches_only_the_exact_placement_to_a_generic_connector_writer() {
        let finst_id = UniqueId::new(41, 77);
        let handle = connector_writer_handle(9, finst_id);
        let mut fragment = connector_writer_template_fragment(9);

        patch_native_connector_write_sink(&mut fragment, 9, finst_id, 0, &handle)
            .expect("exact writer placement patches");

        let Some(native_plan::data_sink::Kind::ConnectorWrite(sink)) =
            fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("generic connector write sink");
        };
        let writer = sink
            .handle
            .as_ref()
            .and_then(|handle| handle.writer.as_ref())
            .expect("writer");
        assert_eq!(writer.fragment_id, 9);
        assert_eq!(writer.backend_num, 0);
        assert_eq!(sink.handle.as_ref().expect("handle").payload, b"opaque");
        assert!(matches!(
            sink.input.as_ref().and_then(|input| input.kind.as_ref()),
            Some(native_plan::connector_write_input_binding::Kind::RootOutputByOrdinal(true))
        ));

        let mut wrong = connector_writer_template_fragment(9);
        let before = wrong.clone();
        let error =
            patch_native_connector_write_sink(&mut wrong, 9, UniqueId::new(41, 78), 0, &handle)
                .expect_err("wrong fragment instance must fail closed");
        assert!(
            error.contains("does not match scheduled placement"),
            "{error}"
        );
        assert_eq!(wrong, before, "failed patch must preserve legacy template");
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
            output_columns: vec![crate::proto::common::OutputColumn {
                column_id: 10,
                name: "total".to_string(),
                r#type: None,
                nullable: true,
                is_internal: false,
            }],
            ..Default::default()
        };
        let destination = FragmentDestination::new(
            UniqueId::new(98_000, 1),
            RuntimeEndpoint::new("10.0.0.20", 9010).unwrap(),
        );
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
                vec![ColumnId::new_for_test(13)],
            )],
            &BTreeMap::from([(2, vec![destination])]),
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
            "10.0.0.20:9010"
        );
    }

    #[test]
    fn rejects_router_validation_drift_without_mutating_the_fragment() {
        let mut fragment = router_fragment();
        let before = fragment.clone();
        let edge = router_edge(2);
        let error =
            patch_native_change_stream_router_sink(&mut fragment, 1, 7, &[&edge], &BTreeMap::new())
                .expect_err("missing target placement must fail before patching");
        assert!(
            error.contains("target fragment 2 has no placements"),
            "{error}"
        );
        assert_eq!(fragment, before, "router patch must be atomic");
    }
}
