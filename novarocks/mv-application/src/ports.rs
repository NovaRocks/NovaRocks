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
    MvCommand, MvCreatePublicationOutcome, MvCreatedTarget, MvOperationContext, MvProductError,
    MvProductErrorKind, MvRefreshAttemptIdentity, MvStagedTarget, MvTarget,
};
use crate::publication::MvRefreshPublicationFinalizationFacts;
use crate::readiness::MvDropReadiness;

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
    original_failure: Option<novarocks_failure_contract::SharedFailureCause>,
    compile_control: Option<novarocks_type_contract::CompileControlError>,
    unavailable_implementation: Option<novarocks_sql::analyze_error::AnalyzeError>,
}

impl MvProviderFailure {
    pub fn new(kind: MvProviderFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            original_failure: None,
            compile_control: None,
            unavailable_implementation: None,
        }
    }

    pub fn with_compile_control(
        mut self,
        error: Option<novarocks_type_contract::CompileControlError>,
    ) -> Self {
        self.compile_control = error;
        self
    }
    pub fn with_original_failure(
        mut self,
        cause: novarocks_failure_contract::SharedFailureCause,
    ) -> Self {
        self.original_failure = Some(cause);
        self
    }

    pub fn original_failure(&self) -> Option<&novarocks_failure_contract::SharedFailureCause> {
        self.original_failure.as_ref()
    }

    pub fn compile_control_error(&self) -> Option<novarocks_type_contract::CompileControlError> {
        self.compile_control.clone()
    }

    /// Retain only the typed missing-implementation cause, never diagnostic text.
    pub fn with_unavailable_implementation(
        mut self,
        error: Option<novarocks_sql::analyze_error::AnalyzeError>,
    ) -> Self {
        if matches!(
            self.kind,
            MvProviderFailureKind::CommitUnknown
                | MvProviderFailureKind::KnownCommittedFinalizeFailed
        ) {
            self.unavailable_implementation = None;
            return self;
        }
        self.unavailable_implementation = error.filter(|error| {
            error.kind()
                == novarocks_sql::analyze_error::AnalyzeErrorKind::UnavailableImplementation
        });
        self
    }

    pub fn unavailable_implementation(
        &self,
    ) -> Option<&novarocks_sql::analyze_error::AnalyzeError> {
        self.unavailable_implementation.as_ref()
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
        let mut failure = MvProductError::new(kind, self.message)
            .with_compile_control(self.compile_control)
            .with_unavailable_implementation(self.unavailable_implementation);
        if let Some(cause) = self.original_failure {
            failure = failure.with_original_failure(cause);
        }
        failure
    }
}

impl fmt::Display for MvProviderFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvProviderFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.original_failure.as_ref().map(|cause| cause as _)
    }
}

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

/// Borrowed progress of one original CREATE. Each poll may only advance its
/// named phase. The outer Source retains the raw result before dropping a
/// completed child Future, and retains every in-flight input through actual
/// exit. The product owns ordering and policy; this port owns no retry policy.
pub trait MvCreateProgressPort: Send {
    fn poll_stage_target(
        &mut self,
        cx: &mut std::task::Context<'_>,
        operation: MvOperationContext,
        command: &MvCommand,
    ) -> std::task::Poll<Result<MvStagedTarget, MvProviderFailure>>;
    fn poll_publish_staged_target(
        &mut self,
        cx: &mut std::task::Context<'_>,
        operation: MvOperationContext,
        staged: &MvStagedTarget,
    ) -> std::task::Poll<Result<MvCreatePublicationOutcome, MvProviderFailure>>;
    fn poll_abort_staged_target(
        &mut self,
        cx: &mut std::task::Context<'_>,
        operation: MvOperationContext,
        staged: &MvStagedTarget,
    ) -> std::task::Poll<Result<(), MvProviderFailure>>;
    fn poll_install_created_projection(
        &mut self,
        cx: &mut std::task::Context<'_>,
        operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> std::task::Poll<Result<(), MvProviderFailure>>;
    fn poll_register_target(
        &mut self,
        cx: &mut std::task::Context<'_>,
        operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> std::task::Poll<Result<(), MvProviderFailure>>;
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

/// DROP borrows the same original provider and exact projection-delete guard.
/// The first projection-delete poll may move `guard` into its stable Source;
/// a Pending result never permits dropping it or acquiring another guard.
/// As with CREATE, the Source receives raw results before child Future Drop.
pub trait MvDropProgressPort<G>: Send {
    fn poll_prepare_drop(
        &mut self,
        cx: &mut std::task::Context<'_>,
        operation: MvOperationContext,
        target: &MvTarget,
        if_exists: bool,
    ) -> std::task::Poll<Result<MvDropReadiness<G>, MvProviderFailure>>;
    fn poll_drop_target(
        &mut self,
        cx: &mut std::task::Context<'_>,
        operation: MvOperationContext,
        target: &MvTarget,
    ) -> std::task::Poll<Result<(), MvProviderFailure>>;
    fn poll_delete_after_provider_drop(
        &mut self,
        cx: &mut std::task::Context<'_>,
        operation: MvOperationContext,
        guard: &mut Option<G>,
    ) -> std::task::Poll<Result<(), MvProviderFailure>>;
    fn poll_unregister_target(
        &mut self,
        cx: &mut std::task::Context<'_>,
        operation: MvOperationContext,
        target: &MvTarget,
    ) -> std::task::Poll<Result<(), MvProviderFailure>>;
}

/// One request-local refresh capability borrowed by the product transition.
///
/// The outer original Source retains prepared/admitted inputs, the concrete
/// known-committed continuation, raw outcomes and actual-exit custody. A Ready
/// poll means its corresponding outcome is stable before foreign F Drop.
/// Polling runs outside Source locks; an unwind retains the same Source and is
/// never permission to dispatch another execution or projection.
pub trait MvRefreshProgressPort: Send {
    fn poll_execute_refresh(
        &mut self,
        cx: &mut std::task::Context<'_>,
        target: &MvTarget,
        attempt: &MvRefreshAttemptIdentity,
    ) -> std::task::Poll<()>;

    fn execution_outcome(
        &self,
    ) -> Option<Result<&MvRefreshPublicationFinalizationFacts, &MvProviderFailure>>;

    fn poll_project_known_committed(
        &mut self,
        cx: &mut std::task::Context<'_>,
        target: &MvTarget,
    ) -> std::task::Poll<()>;

    fn projection_outcome(&self) -> Option<&Result<(), MvProviderFailure>>;
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
                    .with_compile_control(Some(control.clone()))
                    .into_product_error();
                assert_eq!(typed.kind(), ordinary.kind());
                assert_eq!(typed.compile_control_error(), Some(control.clone()));
                assert_eq!(ordinary.compile_control_error(), None);
            }
        }
    }

    #[test]
    fn pure_admission_identity_does_not_override_provider_commit_disposition() {
        use novarocks_functions::{FunctionBindingError, FunctionOverloadId};
        use novarocks_sql::{analyze_error::AnalyzeError, compiler::SqlCompileError};
        let SqlCompileError::Analyze(analyze) =
            SqlCompileError::from(FunctionBindingError::UnavailableImplementation(
                FunctionOverloadId::try_new("fixture/mv/provider/missing-pure-v1").unwrap(),
            ))
        else {
            panic!("missing implementation must retain analysis identity");
        };
        for (provider_kind, product_kind) in [
            (
                MvProviderFailureKind::CommitUnknown,
                MvProductErrorKind::CommitUnknown,
            ),
            (
                MvProviderFailureKind::KnownCommittedFinalizeFailed,
                MvProductErrorKind::KnownCommittedFinalizeFailed,
            ),
        ] {
            let provider = MvProviderFailure::new(provider_kind, "publication outcome")
                .with_unavailable_implementation(Some(analyze.clone()));
            assert_eq!(provider.unavailable_implementation(), None);
            let product = provider
                .into_product_error()
                .with_unavailable_implementation(Some(analyze.clone()));
            assert_eq!(product.kind(), product_kind);
            assert_eq!(product.message(), "publication outcome");
            assert_eq!(product.unavailable_implementation(), None);
        }
        let diagnostic =
            MvProviderFailure::new(MvProviderFailureKind::InvalidRequest, "Unsupported")
                .with_unavailable_implementation(Some(AnalyzeError::internal(
                    "UnavailableImplementation",
                )))
                .into_product_error();
        assert_eq!(diagnostic.kind(), MvProductErrorKind::InvalidRequest);
        assert_eq!(diagnostic.message(), "Unsupported");
        assert_eq!(diagnostic.unavailable_implementation(), None);
    }
}
