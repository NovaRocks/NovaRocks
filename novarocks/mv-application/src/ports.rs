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

//! Narrow provider-operation ports owned by the MV product.

use std::fmt;

use crate::product::{
    MvCommand, MvCreatedTarget, MvOperationContext, MvProductError, MvProductErrorKind,
    MvRefreshAttemptIdentity, MvStagedTarget, MvTarget,
};
use crate::publication::MvRefreshPublicationFinalizationFacts;
use crate::readiness::{MvDropReadiness, MvProjectionDeleteGuard};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvProviderFailureKind {
    InvalidRequest,
    Unavailable,
    KnownUncommitted,
    CommitUnknown,
    KnownCommittedFinalizeFailed,
    TargetReplaced,
    Corruption,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvProviderFailure {
    kind: MvProviderFailureKind,
    message: String,
    compile_control: Option<novarocks_type_contract::CompileControlError>,
}

impl MvProviderFailure {
    pub fn new(kind: MvProviderFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            compile_control: None,
        }
    }

    pub fn with_compile_control(
        mut self,
        error: Option<novarocks_type_contract::CompileControlError>,
    ) -> Self {
        self.compile_control = error;
        self
    }
    pub const fn compile_control_error(
        &self,
    ) -> Option<novarocks_type_contract::CompileControlError> {
        self.compile_control
    }

    pub const fn kind(&self) -> MvProviderFailureKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// The product keeps the provider outcome class; in particular an unknown
    /// commit cannot become a normal retryable availability error.
    pub fn into_product_error(self) -> MvProductError {
        let kind = match self.kind {
            MvProviderFailureKind::InvalidRequest => MvProductErrorKind::InvalidRequest,
            MvProviderFailureKind::Unavailable => MvProductErrorKind::Unavailable,
            MvProviderFailureKind::KnownUncommitted => MvProductErrorKind::ProviderKnownUncommitted,
            MvProviderFailureKind::CommitUnknown => MvProductErrorKind::CommitUnknown,
            MvProviderFailureKind::KnownCommittedFinalizeFailed => {
                MvProductErrorKind::KnownCommittedFinalizeFailed
            }
            MvProviderFailureKind::TargetReplaced => MvProductErrorKind::TargetReplaced,
            MvProviderFailureKind::Corruption => MvProductErrorKind::Corruption,
        };
        MvProductError::new(kind, self.message).with_compile_control(self.compile_control)
    }
}

impl fmt::Display for MvProviderFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvProviderFailure {}

/// Provider effects required by the product-owned CREATE transition.
///
/// CREATE has exactly one atomic point: the staged target and its canonical
/// documents become visible together. The port therefore exposes a staging
/// phase with no catalog-visible effect, that single publish, its pre-publish
/// compensation, and the post-commit projection install. It deliberately
/// exposes neither a registry nor a different MV command, so a CREATE adapter
/// cannot become a general provider host.
pub trait MvCreateProviderPort: Send + Sync {
    /// Stage the target and freeze the provider's own field bindings.
    ///
    /// Nothing is visible in the catalog when this returns: a failure here
    /// leaves no effect, and a staged target that is never published is
    /// discarded by `abort_staged_target`.
    fn stage_target(
        &self,
        operation: MvOperationContext,
        command: &MvCommand,
    ) -> Result<MvStagedTarget, MvProviderFailure>;

    /// Publish the staged target together with its canonical definition,
    /// interpretation and configuration documents, in one commit.
    ///
    /// This is the atomic point. The target does not exist before it and is
    /// complete after it; there is no visible empty table in between.
    fn publish_staged_target(
        &self,
        operation: MvOperationContext,
        staged: &MvStagedTarget,
    ) -> Result<MvCreatedTarget, MvProviderFailure>;

    /// Discard a staged target that was proven not to have been published.
    ///
    /// This is not a compensating drop of a live table: it removes objects a
    /// never-visible stage left behind. An unknown publication is never
    /// aborted here.
    fn abort_staged_target(
        &self,
        operation: MvOperationContext,
        staged: &MvStagedTarget,
    ) -> Result<(), MvProviderFailure>;

    /// Install the Current projection from a fresh observation of the
    /// published target. Failure here is a finalization failure: the create
    /// is committed and must not be undone.
    fn install_created_projection(
        &self,
        operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> Result<(), MvProviderFailure>;
}

/// Provider effect required by the product-owned DROP transition.
pub trait MvDropProviderPort: Send + Sync {
    fn drop_target(
        &self,
        operation: MvOperationContext,
        target: &MvTarget,
    ) -> Result<(), MvProviderFailure>;
}

/// Product admission is expressed as a one-operation capability, never as a
/// workload controller or a process-global lookup.
pub trait MvWorkScopePort: Send + Sync {
    fn ensure_active(&self, operation: MvOperationContext) -> Result<(), MvProviderFailure>;
}

/// The product observes only the topology fact required for the current
/// operation.  It cannot own membership or construct role-local runtimes.
pub trait MvTopologyPort: Send + Sync {
    fn backend_count(&self) -> Result<usize, MvProviderFailure>;
}

/// Catalog-registration effect required by CREATE after product projection.
pub trait MvCreateCatalogRegistrationPort: Send + Sync {
    fn register_target(
        &self,
        operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> Result<(), MvProviderFailure>;
}

/// Catalog-registration effect required by DROP after exact projection deletion.
pub trait MvDropCatalogRegistrationPort: Send + Sync {
    fn unregister_target(
        &self,
        operation: MvOperationContext,
        target: &MvTarget,
    ) -> Result<(), MvProviderFailure>;
}

/// The product-owned durable side of DROP. An outer adapter may bridge sync
/// execution, but cannot reinterpret missing-target policy, dependency safety,
/// or the exact post-provider-delete CAS.
pub trait MvDropProjectionPort: Send + Sync {
    fn prepare_drop(
        &self,
        operation: MvOperationContext,
        target: &MvTarget,
        if_exists: bool,
    ) -> Result<MvDropReadiness, MvProviderFailure>;

    fn delete_after_provider_drop(
        &self,
        operation: MvOperationContext,
        guard: MvProjectionDeleteGuard,
    ) -> Result<(), MvProviderFailure>;
}

/// One consumed refresh execution capability. The adapter may retain only the
/// request-local query, connector, and prepared-work carriers required for
/// this operation; it cannot retain MV product lifecycle state.
pub trait MvRefreshExecutionPort: Send {
    fn execute_refresh(
        self: Box<Self>,
        target: &MvTarget,
        attempt: &MvRefreshAttemptIdentity,
    ) -> Result<Box<dyn MvRefreshKnownCommittedPort>, MvProviderFailure>;
}

/// A consumed continuation available only after the external publication is
/// known committed. It exposes product-owned proof and performs exactly one
/// outer projection effect when the product accepts that proof.
pub trait MvRefreshKnownCommittedPort: Send {
    fn finalization_facts(&self) -> &MvRefreshPublicationFinalizationFacts;

    fn project_known_committed(self: Box<Self>, target: &MvTarget)
    -> Result<(), MvProviderFailure>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_provider_commit_stays_unknown_to_the_product() {
        let error = MvProviderFailure::new(MvProviderFailureKind::CommitUnknown, "lost reply")
            .into_product_error();
        assert_eq!(error.kind(), MvProductErrorKind::CommitUnknown);
        assert_eq!(error.message(), "lost reply");
    }
    #[test]
    fn provider_control_cause_does_not_replace_commit_disposition() {
        use novarocks_type_contract::CompileControlError;
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for kind in [
                MvProviderFailureKind::CommitUnknown,
                MvProviderFailureKind::InvalidRequest,
            ] {
                let ordinary =
                    MvProviderFailure::new(kind, control.to_string()).into_product_error();
                let typed = MvProviderFailure::new(kind, control.to_string())
                    .with_compile_control(Some(control))
                    .into_product_error();
                assert_eq!(typed.kind(), ordinary.kind());
                assert_eq!(typed.compile_control_error(), Some(control));
                assert_eq!(ordinary.compile_control_error(), None);
            }
        }
    }
}
