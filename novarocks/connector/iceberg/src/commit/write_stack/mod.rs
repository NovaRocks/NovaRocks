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

//! Iceberg's implementation of the provider-neutral connector write stack.
//!
//! ```text
//! FE begin_write   -> IcebergWriteCommitSession + [WriteTargetOrdinal -> IcebergWriterHandle]
//! BE open_writer   -> one writer per driver
//!    writer.finish -> zero or more IcebergCommitFragment values, one per artifact
//! FE finish_write  -> exactly one Iceberg snapshot commit
//! ```
//!
//! Writers publish independent descriptions of staged artifacts and final
//! delete effects. Position mutations resolve applicable deletes against the
//! fixed read endpoint through the backend's query-leased storage resolver.
//!
//! The adapter that converts between these concrete values and the neutral
//! `ConnectorWriteCommitSession` / `ConnectorWriterHandle` /
//! `ConnectorCommitFragment` is `pub(crate)` and is never handed to a role
//! host. `codec` holds it privately too: it is the only module that turns a
//! central-IDL write carrier into one of these values, and it does so only
//! through the domain constructors above.

pub(crate) mod codec;
pub mod control;
pub(crate) mod copy_on_write;
pub mod domain;
pub(crate) mod equality_schema;
pub mod execution;
pub(crate) mod flavor;
pub mod old_delete;
pub mod planning;
pub(crate) mod program_recipe;
pub(crate) mod repartition;
pub(crate) mod runtime;

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

pub use control::{
    ICEBERG_WRITE_SESSION_EVIDENCE_VERSION, ICEBERG_WRITE_SESSION_MARKER_PROPERTY,
    ICEBERG_WRITE_SESSION_OPERATION_KIND, IcebergWriteSessionControl,
};
pub use domain::{
    IcebergArtifactMetrics, IcebergArtifactPartition, IcebergCommitArtifact, IcebergCommitFragment,
    IcebergContentRange, IcebergDataBranchRecipe, IcebergDataFileArtifact,
    IcebergDeletionVectorArtifact, IcebergEmptyWriteDecision, IcebergPositionDeleteFileArtifact,
    IcebergPositionRewriteTarget, IcebergSealedWriteTarget, IcebergWriteBranch,
    IcebergWriteCommitSession, IcebergWriteFlavor, IcebergWriteSessionId, IcebergWriteSessionState,
    IcebergWriteTableFacts, IcebergWriterHandle, IcebergWriterOutput,
};
pub use execution::{IcebergWriteStackExecution, IcebergWriteStackExecutionFactory};
pub use planning::{
    IcebergBranchSessionPlanInput, IcebergDataBranchPlan, IcebergDeleteBranchPlan,
    IcebergWriteBranchPlan, IcebergWriteSessionPlanInput, IcebergWriteTargetPlan,
    plan_branch_session, plan_write_session,
};
pub use program_recipe::IcebergWriteRecipeCompiler;
