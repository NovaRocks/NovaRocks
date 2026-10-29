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
use novarocks_execution_contract::{
    CreateTask, OperationDomainFailure, SafeDetail, TaskCreationInput,
    failure::{FailureClass, FailureFact},
    operation::DomainFailureOutcome,
};
use novarocks_worker_protocol::creation::{AdmissionRefusal, CreationCost};

pub(crate) fn measure(
    request: &CreateTask,
    input: &TaskCreationInput,
    ingress_retained_bytes: usize,
) -> Result<CreationCost, AdmissionRefusal> {
    let inbound_bytes = inbound_record_bytes(request.descriptor()).ok_or_else(|| {
        AdmissionRefusal::Domain(
            DomainFailureOutcome::ResourceExhausted,
            OperationDomainFailure::new(
                FailureFact::new(
                    FailureClass::Resource,
                    crate::failure_reason::WorkerFailureReason::InboundRecordSize.reason(),
                ),
                SafeDetail::truncating("inbound normal-close record size overflowed"),
            ),
        )
    })?;
    Ok(CreationCost {
        input_retained_bytes: input.retained_bytes() as u64,
        ingress_retained_bytes: ingress_retained_bytes as u64,
        structural_bytes: structural_bytes(request) as u64,
        inbound_records: u64::from(inbound_bytes != 0),
        inbound_bytes: inbound_bytes as u64,
    })
}

/// The pure author receives the actual inline owner layout. The live permit
/// and its original reservation remain outside the protocol core.
pub(crate) fn inbound_record_bytes(
    descriptor: &novarocks_execution_contract::TaskDescriptor,
) -> Option<usize> {
    novarocks_worker_protocol::inbound_cost::record_bytes(
        descriptor,
        std::mem::size_of::<Option<crate::TaskInboundPermit>>(),
    )
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
