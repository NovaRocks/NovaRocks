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

use std::collections::BTreeMap;

use crate::query_execution::artifact::{
    NativeSubmissionAttachment, NativeSubmissionEncodingView, NativeSubmissionFragmentRole,
    ValidatedNativeSubmission,
};
use crate::query_execution::assembly;
use novarocks_plan_codec::encode_data_partition;
use novarocks_sql::plan_read::{ColumnId, CteId, FragmentEdgeKind, FragmentId};

use super::instance::encode_instance_params;

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
            novarocks_proto_models::plan::DataPartition,
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
                    novarocks_proto_models::plan::DataPartition {
                        kind: novarocks_proto_models::plan::PartitionKind::Unpartitioned as i32,
                        exprs: Vec::new(),
                    },
                    Vec::new(),
                    receive_producer_column_ids.clone(),
                ));
            }
        }
    }

    let mut native_by_fragment = view
        .native_fragments_in_id_order()
        .map(|(fragment_id, fragment)| (fragment_id, fragment.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut submissions_by_fragment = BTreeMap::new();
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
        let is_producer =
            stream_edge.is_some() || router_edges.is_some() || facts.cte_id().is_some();
        validate_fragment_output_kind(fragment_id, is_root, is_producer, facts.role())?;
        assembly::ensure_native_fragment_sink_supported(
            fragment_id,
            is_root,
            stream_edge.is_some(),
            router_edges.is_some(),
            facts.cte_id().is_some(),
        )?;
        let fragment_submissions = placements
            .iter()
            .map(|placement| {
                let mut native_fragment = template.clone();
                if !is_root && stream_edge.is_none() {
                    if let Some((router_group_id, branch_edges)) = router_edges {
                        assembly::patch_native_change_stream_router_sink(
                            &mut native_fragment,
                            fragment_id,
                            *router_group_id,
                            branch_edges,
                            placement,
                            &schedule.by_fragment,
                        )?;
                    } else if let Some(cte_id) = facts.cte_id() {
                        let consumers = cte_consumers.get(&cte_id).cloned().unwrap_or_default();
                        assembly::patch_native_cte_multicast_sink(
                            &mut native_fragment,
                            fragment_id,
                            cte_id,
                            &consumers,
                            placement,
                            &schedule.by_fragment,
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
    view.seal(submissions)
        .map_err(|error| error.message().to_string())
}

fn validate_fragment_output_kind(
    fragment_id: FragmentId,
    is_root: bool,
    is_producer: bool,
    role: NativeSubmissionFragmentRole,
) -> Result<(), String> {
    if is_root {
        return match role {
            NativeSubmissionFragmentRole::Result | NativeSubmissionFragmentRole::Statistics => {
                Ok(())
            }
            NativeSubmissionFragmentRole::NonTerminal => Err(format!(
                "root fragment {fragment_id} must have Result output kind"
            )),
        };
    }
    if is_producer && role != NativeSubmissionFragmentRole::NonTerminal {
        return Err(format!(
            "producer fragment {fragment_id} must have NonTerminal output kind, got {role:?}"
        ));
    }
    Ok(())
}
