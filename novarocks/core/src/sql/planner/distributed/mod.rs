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

mod activation_decision;
#[cfg(test)]
pub(crate) use crate::sql::planner::runtime_filter::activation::{
    ActivationConstraint, ActivationFallback, RequiredLiveReason,
};
#[cfg(test)]
pub(crate) use activation_decision::DraftRuntimeFilterGraph;
pub(crate) mod boundary;
pub(crate) mod build;
#[cfg(feature = "runtime-filter-test-support")]
pub mod fragment;
#[cfg(not(feature = "runtime-filter-test-support"))]
mod fragment;
mod node;
pub(crate) mod output;
mod runtime_filter_progress;
mod seal;
pub(crate) mod topology;
mod validation;
pub(crate) mod write;

#[cfg(test)]
pub(crate) mod test_support;

pub(crate) use boundary::{BoundaryColumn, BoundaryContract, BoundaryKind, ExecutionColumnId};
pub use fragment::{DataPartition, FragmentEdge, FragmentEdgeKind, FragmentId, FragmentStreamKind};
pub(crate) use fragment::{DataSink, PartitionKind, PlanFragment};
pub(crate) use node::{
    DistributedNode, DistributedNodeKind, ExchangeFlavor, ExchangeReceiver,
    distributed_kind_from_physical, distributed_kind_to_physical,
};
pub(crate) use output::{
    FragmentEdgeOutputCatalog, NodeExecutionColumn, NodeOutputCatalog, WriteContractCatalog,
};
pub(crate) use runtime_filter_progress::{
    FrontierEdge, FrontierSkip, JoinBuildProgressCatalog, JoinBuildProgressProof,
    JoinBuildProgressSkip,
};
pub(crate) use seal::DistributedPlan;

#[cfg(test)]
mod tests {
    use std::path::Path;

    #[test]
    fn bridge2_owner_modules_are_split_into_files() {
        for module_file in [
            "distributed/fragment.rs",
            "distributed/node.rs",
            "distributed/build/fragment_cut.rs",
            "distributed/build/lowering.rs",
            "distributed/build/mod.rs",
            "distributed/build/runtime_filter_binding.rs",
        ] {
            let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/sql/planner")
                .join(module_file);
            assert!(path.is_file(), "{} should exist", path.display());
        }
    }
}
