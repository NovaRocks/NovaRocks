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

pub mod artifact;
/// Carrier-neutral payload validation and patch helpers consumed by the
/// Frontend-owned native submission mapper.
pub mod assembly;
pub mod backend;
pub mod backend_command;
pub mod cancellation;
pub mod completion;
mod connector_binding;
pub(crate) mod connector_write_transaction;
pub mod contract;
pub mod control;
pub mod distributed_rewrite;
pub mod dml;
pub mod fragment_transport;
pub(crate) mod frozen_connector_read;
pub mod kernels;
pub mod lifecycle;
pub mod mv_assembly;
pub mod mv_native_write;
pub mod native_fragment;
pub(crate) mod outcome;
pub mod planning;
pub use completion::{
    PreparedDistributedQuery as PreparedQueryDistributedOperation, PreparedImmediateQuery,
    PreparedQueryCompletion, PreparedQueryOperation, StatementResult,
};
pub use outcome::{ConnectorWriteCompletion, ConnectorWriteStagingSummary, WriteExecutionOutcome};
/// Sealed preparation carriers consumed by the native Frontend encoder.
pub mod preparation;
pub use preparation::runtime_filter_view::{
    RuntimeFilterApplyPoint, RuntimeFilterArtifactCapability, RuntimeFilterBindingFacts,
    RuntimeFilterBindingFactsView, RuntimeFilterBindingFragmentFactsView,
    RuntimeFilterBindingRoleFacts, RuntimeFilterCompletionRequirement,
    RuntimeFilterConsumerActivation, RuntimeFilterConsumerTarget, RuntimeFilterContributionKind,
    RuntimeFilterCoverageFacts, RuntimeFilterDeploymentBindingFacts,
    RuntimeFilterDeploymentBindingRoleFacts, RuntimeFilterDeploymentFactsView,
    RuntimeFilterDeploymentLifecycleFacts, RuntimeFilterFragmentEdgeFacts,
    RuntimeFilterFrontierEdgeFacts, RuntimeFilterJoinProgressFacts,
    RuntimeFilterJoinProgressSkipReason, RuntimeFilterLateApplyGranularity,
    RuntimeFilterLogicalDomainFacts, RuntimeFilterNullOrder, RuntimeFilterNullSemantics,
    RuntimeFilterOrderKeyFacts, RuntimeFilterPolicyFacts, RuntimeFilterProducerTarget,
    RuntimeFilterReductionFacts, RuntimeFilterScanDomainTarget, RuntimeFilterSortDirection,
    RuntimeFilterValidatedPlacementFacts,
};
pub use schedule::FragmentInstancePlacement;
pub mod post_compile;
pub mod prepared_write;
pub(crate) mod profile;
pub mod read_session;
pub mod request_context;
pub(crate) mod row_mutation;
pub(crate) mod schedule;
pub mod service;
pub mod session;
pub mod statistics;
pub mod write;
pub mod write_operation;
pub mod write_plan;
pub(crate) mod write_transaction;

pub mod compiler;
#[cfg(test)]
mod tests;
