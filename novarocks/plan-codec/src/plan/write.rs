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

use super::type_mapping::{encode_data_partition, encode_row_mutation_effect, usize_to_u64};
use super::{NativePlanEncodeContext, required_context_ref};
use novarocks_proto_models::plan;
use novarocks_sql::plan_read::{ChangeStreamRouterSink, ConnectorWriteInputBinding, FragmentId};

pub(super) fn encode_connector_write_input_binding(
    src: &ConnectorWriteInputBinding,
) -> plan::ConnectorWriteInputBinding {
    use plan::connector_write_input_binding::Kind;

    plan::ConnectorWriteInputBinding {
        kind: Some(match src {
            ConnectorWriteInputBinding::RootOutputByOrdinal => Kind::RootOutputByOrdinal(true),
            ConnectorWriteInputBinding::OutputOrdinals(ordinals) => {
                Kind::OutputOrdinals(plan::UInt64List {
                    values: ordinals.iter().map(|value| *value as u64).collect(),
                })
            }
        }),
    }
}

pub(super) fn encode_change_stream_router_sink(
    src: &ChangeStreamRouterSink,
    fragment_id: FragmentId,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::ChangeStreamRouterSink, String> {
    Ok(plan::ChangeStreamRouterSink {
        group_id: src.group_id(),
        effect_output_ordinal: usize_to_u64(src.effect_output_ordinal()),
        routes: src
            .routes()
            .map(|route| {
                Ok(plan::ChangeStreamBranchRoute {
                    target_fragment_id: route.target_fragment_id(),
                    target_exchange_node_id: route.target_exchange_node_id(),
                    output_partition_ordinals: route
                        .output_partition_ordinals()
                        .iter()
                        .map(|value| usize_to_u64(*value))
                        .collect(),
                    output_partition: Some(encode_finalized_router_branch_partition(
                        ctx,
                        fragment_id,
                        route.route_id(),
                    )?),
                    destinations: None,
                    route_id: route.route_id().to_bytes().to_vec(),
                    write_target_ordinal: route.write_target_ordinal().get(),
                    accepted_effects: route
                        .accepted_effects()
                        .iter()
                        .map(|effect| encode_row_mutation_effect(*effect))
                        .collect(),
                    input_ordinals: route
                        .input_ordinals()
                        .iter()
                        .map(|binding| usize_to_u64(binding.input_ordinal() as usize))
                        .collect(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
    })
}

/// Map a change-stream router branch's finalized partition from the sealed write
/// contract (CGO-9C Task 3). The planner already reconstructed the partition
/// expression from the branch's ordinals against the router fragment's output
/// columns at seal; the encoder maps the typed result 1:1.
fn encode_finalized_router_branch_partition(
    ctx: &NativePlanEncodeContext<'_>,
    fragment_id: FragmentId,
    route_id: novarocks_spi::connector::ConnectorWriteRouteId,
) -> Result<plan::DataPartition, String> {
    let partition = required_context_ref(ctx.write_contracts, || {
        format!("native change-stream router fragment {fragment_id} has no sealed write contract")
    })?
    .router_route_partition(fragment_id, route_id)
    .ok_or_else(|| {
        format!(
            "native row-mutation router fragment {fragment_id} route is missing from the sealed write contract"
        )
    })?;
    encode_data_partition(partition)
}
