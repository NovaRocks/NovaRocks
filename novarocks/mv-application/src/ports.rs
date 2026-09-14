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
    MvCommand, MvCreatedTarget, MvOperationContext, MvPreparedDefinition, MvProductError,
    MvProductErrorKind, MvTarget,
};
use crate::readiness::MvDropReadiness;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvProviderFailureKind {
    InvalidRequest,
    Unavailable,
    KnownUncommitted,
    CommitUnknown,
    TargetReplaced,
    Corruption,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvProviderFailure {
    kind: MvProviderFailureKind,
    message: String,
}

impl MvProviderFailure {
    pub fn new(kind: MvProviderFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
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
            MvProviderFailureKind::TargetReplaced => MvProductErrorKind::TargetReplaced,
            MvProviderFailureKind::Corruption => MvProductErrorKind::Corruption,
        };
        MvProductError::new(kind, self.message)
    }
}

impl fmt::Display for MvProviderFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvProviderFailure {}

/// Provider effects required by the product-owned CREATE transition. The port
/// deliberately exposes neither a registry nor a different MV command, so a
/// CREATE adapter cannot become a general provider host.
pub trait MvCreateProviderPort: Send + Sync {
    fn create_target(
        &self,
        operation: MvOperationContext,
        command: &MvCommand,
    ) -> Result<MvCreatedTarget, MvProviderFailure>;

    fn inspect_created_target(
        &self,
        operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> Result<MvPreparedDefinition, MvProviderFailure>;

    fn sync_target_descriptor(
        &self,
        operation: MvOperationContext,
        target: &MvCreatedTarget,
        definition: &MvPreparedDefinition,
    ) -> Result<(), MvProviderFailure>;

    fn project_created_target(
        &self,
        operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> Result<(), MvProviderFailure>;

    /// Compensate only the target created by this CREATE transition after
    /// inspection proves that no durable product definition can be finalized.
    fn cleanup_created_target(
        &self,
        operation: MvOperationContext,
        target: &MvTarget,
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

/// One execution capability for a refresh after the product has fixed its
/// target and operation identity.
pub trait MvQueryExecutionPort: Send + Sync {
    fn execute_refresh(
        &self,
        operation: MvOperationContext,
        target: &MvTarget,
        full: bool,
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
        target: &MvTarget,
    ) -> Result<(), MvProviderFailure>;
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
}
