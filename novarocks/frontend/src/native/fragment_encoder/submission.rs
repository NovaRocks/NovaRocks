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

//! Frontend-owned placement-local native submission mapping.
//!
//! Templates are sealed before this point, while placement, connector write
//! handles and per-instance sidecars become available only after Init,
//! ControlReady and connector-install acknowledgement.  This module consumes
//! those frozen facts without reacquiring planning, topology, or control
//! state, then seals the complete payload back into Core's neutral attachment.

use std::collections::{BTreeMap, BTreeSet};

use crate::query_execution::FragmentInstancePlacement;
use crate::query_execution::artifact::{
    NativeSubmissionAttachment, NativeSubmissionEncodingView, NativeSubmissionFragmentRole,
    ValidatedNativeSubmission, WriterRegistration, WriterRegistrationSet,
};
use crate::query_execution::assembly;
use crate::query_execution::write_plan::ConnectorWritePlanAttachment;
use novarocks_execution::runtime::endpoint::FragmentDestination;
use novarocks_spi::connector::ConnectorWriteCohortId;
use novarocks_sql::plan_read::{ColumnId, CteId, FragmentEdgeKind, FragmentId};
use novarocks_types::UniqueId;

use super::{encode_data_partition, encode_instance_params};

#[expect(
    clippy::type_complexity,
    reason = "The CTE exchange payload follows the frozen native fragment contract."
)]
pub(crate) fn encode_native_submission(
    view: &NativeSubmissionEncodingView<'_>,
) -> Result<NativeSubmissionAttachment, String> {
    let schedule = view.schedule();
    let root_fragment_id = schedule.root_fragment_id;
    let edges = view.edges();
    let stream_edge_by_source = assembly::build_stream_edge_by_source(edges);
    let router_edges_by_source: BTreeMap<FragmentId, (i32, Vec<_>)> =
        assembly::group_router_edges_by_source(edges)
            .into_iter()
            .map(|((source_fragment_id, router_group_id), branch_edges)| {
                (source_fragment_id, (router_group_id, branch_edges))
            })
            .collect();

    let mut cte_consumers: BTreeMap<
        CteId,
        Vec<(
            FragmentId,
            i32,
            novarocks_proto::plan::DataPartition,
            Vec<i32>,
            Vec<ColumnId>,
        )>,
    > = BTreeMap::new();
    for edge in edges {
        if let FragmentEdgeKind::CteMulticast {
            cte_id,
            receive_producer_column_ids,
        } = &edge.edge_kind
        {
            let native_partition = encode_data_partition(&edge.output_partition)?;
            cte_consumers.entry(*cte_id).or_default().push((
                edge.target_fragment_id,
                edge.target_exchange_node_id,
                native_partition,
                edge.output_slot_ids.clone(),
                receive_producer_column_ids.clone(),
            ));
        }
    }
    for fragment in view.fragments() {
        for (cte_id, exchange_node_id, receive_producer_column_ids) in fragment.cte_exchange_nodes()
        {
            let consumers = cte_consumers.entry(*cte_id).or_default();
            if !consumers.iter().any(|(fragment_id, node_id, _, _, _)| {
                *fragment_id == fragment.fragment_id() && *node_id == *exchange_node_id
            }) {
                consumers.push((
                    fragment.fragment_id(),
                    *exchange_node_id,
                    novarocks_proto::plan::DataPartition {
                        kind: novarocks_proto::plan::PartitionKind::Unpartitioned as i32,
                        exprs: Vec::new(),
                    },
                    Vec::new(),
                    receive_producer_column_ids.clone(),
                ));
            }
        }
    }

    let consumer_destinations = schedule
        .by_fragment
        .iter()
        .map(|(fragment_id, placements)| {
            let destinations = placements
                .iter()
                .map(|placement| {
                    FragmentDestination::new(placement.finst_id, placement.endpoint.clone())
                })
                .collect();
            (*fragment_id, destinations)
        })
        .collect::<BTreeMap<_, _>>();

    let mut native_by_fragment = view
        .native_fragments_in_id_order()
        .map(|(fragment_id, fragment)| (fragment_id, fragment.clone()))
        .collect::<BTreeMap<_, _>>();
    let connector_write_plans = view.connector_write_plans();
    let mut connector_attachment_by_fragment =
        BTreeMap::<FragmentId, &ConnectorWritePlanAttachment>::new();
    for attachment in connector_write_plans.values() {
        for writer in attachment.manifest().writers() {
            let fragment_id = u32::try_from(writer.fragment_id())
                .map_err(|_| "connector writer manifest contains a negative fragment ID")?;
            if connector_attachment_by_fragment
                .insert(fragment_id, attachment)
                .is_some_and(|previous| {
                    previous.manifest().cohort_id() != attachment.manifest().cohort_id()
                })
            {
                return Err(format!(
                    "connector write plans assign terminal fragment {fragment_id} to multiple cohorts"
                ));
            }
        }
    }

    let mut submissions_by_fragment = BTreeMap::new();
    let mut writer_registrations = Vec::new();
    let mut consumed_connector_writers = BTreeSet::new();
    let query_id = view.query_id();
    for (&fragment_id, placements) in &schedule.by_fragment {
        let facts = view
            .fragment(fragment_id)
            .ok_or_else(|| format!("prepared fragment {fragment_id} is missing"))?;
        let template = native_by_fragment
            .remove(&fragment_id)
            .ok_or_else(|| format!("native fragment template {fragment_id} is missing"))?;
        let is_root = fragment_id == root_fragment_id;
        let stream_edge = stream_edge_by_source.get(&fragment_id).copied();
        let router_edges = router_edges_by_source.get(&fragment_id);
        let is_writer = stream_edge.is_none()
            && router_edges.is_none()
            && facts.cte_id().is_none()
            && facts.role().is_terminal_write();
        let is_producer =
            stream_edge.is_some() || router_edges.is_some() || facts.cte_id().is_some();
        validate_fragment_output_kind(fragment_id, is_root, is_writer, is_producer, facts.role())?;
        assembly::ensure_native_fragment_sink_supported(
            fragment_id,
            is_root,
            is_writer,
            stream_edge.is_some(),
            router_edges.is_some(),
            facts.cte_id().is_some(),
        )?;
        let fragment_submissions = placements
            .iter()
            .map(|placement| {
                let connector_attachment = connector_attachment_by_fragment
                    .get(&fragment_id)
                    .copied();
                if is_writer && !connector_write_plans.is_empty() && connector_attachment.is_none() {
                    return Err(format!(
                        "connector write plans have no cohort attachment for terminal writer fragment {fragment_id}"
                    ));
                }
                if is_writer {
                    writer_registrations.push(WriterRegistration::new(
                        query_id,
                        view.execution_id(),
                        fragment_id,
                        placement.finst_id,
                        placement.instance_index as i32,
                        connector_attachment.map(|attachment| attachment.manifest().cohort_id()),
                    ));
                }
                let mut native_fragment = template.clone();
                if is_writer && let Some(attachment) = connector_attachment {
                    patch_connector_writer(
                        &mut native_fragment,
                        fragment_id,
                        placement,
                        attachment,
                        &mut consumed_connector_writers,
                    )?;
                }
                for (&node_id, splits) in &placement.connector_splits {
                    assembly::patch_native_connector_read_splits(
                        &mut native_fragment,
                        node_id,
                        splits,
                    )?;
                }
                if !is_root && !is_writer && stream_edge.is_none() {
                    if let Some((router_group_id, branch_edges)) = router_edges {
                        assembly::patch_native_change_stream_router_sink(
                            &mut native_fragment,
                            fragment_id,
                            *router_group_id,
                            branch_edges,
                            &schedule.by_fragment,
                        )?;
                    } else if let Some(cte_id) = facts.cte_id() {
                        let consumers = cte_consumers.get(&cte_id).cloned().unwrap_or_default();
                        assembly::patch_native_cte_multicast_sink(
                            &mut native_fragment,
                            fragment_id,
                            cte_id,
                            &consumers,
                            &consumer_destinations,
                        )?;
                    }
                }
                let backend_num = i32::try_from(placement.instance_index)
                    .map_err(|_| "native submission backend number exceeds i32 width")?;
                let instance_params = encode_instance_params(
                    &query_id,
                    placement,
                    view.query_options(),
                    backend_num,
                    is_root,
                )?;
                Ok(ValidatedNativeSubmission::new(
                    placement.backend_idx,
                    placement.finst_id,
                    view.execution_id(),
                    native_fragment,
                    instance_params,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        submissions_by_fragment.insert(fragment_id, fragment_submissions);
    }
    if !native_by_fragment.is_empty() {
        return Err(format!(
            "native templates remained after assembly: {:?}",
            native_by_fragment.keys().collect::<Vec<_>>()
        ));
    }

    let mut submissions = Vec::new();
    for &fragment_id in view.topological_fragment_order().iter().rev() {
        let mut fragment_submissions = submissions_by_fragment
            .remove(&fragment_id)
            .ok_or_else(|| format!("assembled fragment {fragment_id} is missing"))?;
        submissions.append(&mut fragment_submissions);
    }
    if !submissions_by_fragment.is_empty() {
        return Err("assembled submissions contain unknown fragments".to_string());
    }
    validate_connector_writer_coverage(connector_write_plans, &consumed_connector_writers)?;

    view.seal(
        submissions,
        WriterRegistrationSet::new(writer_registrations),
    )
    .map_err(|error| error.message().to_string())
}

fn validate_fragment_output_kind(
    fragment_id: FragmentId,
    is_root: bool,
    is_terminal_write: bool,
    is_producer: bool,
    role: NativeSubmissionFragmentRole,
) -> Result<(), String> {
    if is_root {
        return match role {
            NativeSubmissionFragmentRole::Result
            | NativeSubmissionFragmentRole::Statistics
            | NativeSubmissionFragmentRole::TerminalWrite => Ok(()),
            NativeSubmissionFragmentRole::NonTerminal => Err(format!(
                "root fragment {fragment_id} must have Result or TerminalWrite output kind"
            )),
        };
    }
    if is_terminal_write && role != NativeSubmissionFragmentRole::TerminalWrite {
        return Err(format!(
            "terminal write fragment {fragment_id} must have TerminalWrite output kind, got {role:?}"
        ));
    }
    if is_producer && role != NativeSubmissionFragmentRole::NonTerminal {
        return Err(format!(
            "producer fragment {fragment_id} must have NonTerminal output kind, got {role:?}"
        ));
    }
    Ok(())
}

fn patch_connector_writer(
    native_fragment: &mut novarocks_proto::plan::PlanFragment,
    fragment_id: FragmentId,
    placement: &FragmentInstancePlacement,
    attachment: &ConnectorWritePlanAttachment,
    consumed: &mut BTreeSet<novarocks_spi::connector::ConnectorWriterIdentity>,
) -> Result<(), String> {
    let backend_num = i32::try_from(placement.instance_index)
        .map_err(|_| "connector writer backend number exceeds i32 width")?;
    let writer_fragment_id =
        i32::try_from(fragment_id).map_err(|_| "connector writer fragment ID exceeds i32 width")?;
    let handle = attachment
        .plan()
        .handles()
        .iter()
        .find(|handle| {
            let writer = handle.writer();
            writer.fragment_id() == writer_fragment_id
                && writer.backend_num() == backend_num
                && writer.fragment_instance_id() == unique_id_bytes(placement.finst_id)
                && writer.sink_ordinal() == 0
        })
        .ok_or_else(|| format!(
            "connector write plan has no handle for terminal writer fragment={fragment_id} backend_num={backend_num} finst={:?}",
            placement.finst_id
        ))?;
    if !consumed.insert(handle.writer().clone()) {
        return Err(format!(
            "connector write plan reuses a writer handle for terminal writer fragment={fragment_id} backend_num={backend_num}"
        ));
    }
    assembly::patch_native_connector_write_sink(
        native_fragment,
        fragment_id,
        placement.finst_id,
        backend_num,
        handle,
    )
}

fn validate_connector_writer_coverage(
    plans: &BTreeMap<ConnectorWriteCohortId, ConnectorWritePlanAttachment>,
    consumed: &BTreeSet<novarocks_spi::connector::ConnectorWriterIdentity>,
) -> Result<(), String> {
    if plans.is_empty() {
        return Ok(());
    }
    let expected = plans
        .values()
        .flat_map(|attachment| attachment.manifest().writers().iter().cloned())
        .collect::<BTreeSet<_>>();
    if *consumed != expected {
        let missing = expected.difference(consumed).collect::<Vec<_>>();
        let unexpected = consumed.difference(&expected).collect::<Vec<_>>();
        return Err(format!(
            "connector write plan consumption does not exactly cover the frozen manifests: missing={missing:?} unexpected={unexpected:?}"
        ));
    }
    Ok(())
}

fn unique_id_bytes(value: UniqueId) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&value.high().to_be_bytes());
    bytes[8..].copy_from_slice(&value.low().to_be_bytes());
    bytes
}
