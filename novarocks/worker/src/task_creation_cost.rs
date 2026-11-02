// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Actual retained creation geometry. Input getters execute only on host work;
//! the Core receives neutral costs and performs all admission decisions.
use novarocks_execution_contract::{CreateTask, TaskCreationInput};
use novarocks_worker_protocol::creation::CreationCost;

pub(crate) fn measure(
    request: &CreateTask,
    input: &TaskCreationInput,
    ingress_retained_bytes: usize,
) -> CreationCost {
    CreationCost {
        input_retained_bytes: input.retained_bytes() as u64,
        ingress_retained_bytes: ingress_retained_bytes as u64,
        structural_bytes: structural_bytes(request) as u64,
    }
}

pub(crate) fn structural_bytes(request: &CreateTask) -> usize {
    let mut bytes = std::mem::size_of::<CreateTask>()
        .saturating_add(std::mem::size_of_val(
            request.descriptor().split_plan_nodes(),
        ))
        .saturating_add(std::mem::size_of_val(request.initial_domains()));
    let topology = request.descriptor().topology();
    bytes = bytes
        .saturating_add(std::mem::size_of_val(topology.outbound()))
        .saturating_add(std::mem::size_of_val(topology.inbound()));
    for edge in topology.outbound() {
        bytes = bytes.saturating_add(std::mem::size_of_val(edge.destinations()));
    }
    for inbound in topology.inbound() {
        bytes = bytes.saturating_add(std::mem::size_of_val(inbound.sources()));
    }
    for domain in request.initial_domains() {
        let extra = match domain {
            novarocks_execution_contract::TaskDomainUpdate::SplitAssignment(intent) => {
                intent.payload().encoded_len()
            }
            novarocks_execution_contract::TaskDomainUpdate::TaskDynamicFilter {
                payload, ..
            } => payload.encoded_len(),
            novarocks_execution_contract::TaskDomainUpdate::OpenExchangeEdges { edges, .. } => {
                std::mem::size_of_val(edges.as_slice())
            }
            novarocks_execution_contract::TaskDomainUpdate::CloseExchangeDestination { .. } => {
                std::mem::size_of::<novarocks_execution_contract::TaskIdentity>()
                    + std::mem::size_of::<novarocks_execution_contract::ExchangeEdgeId>()
            }
        };
        bytes = bytes.saturating_add(extra);
    }
    bytes
}
