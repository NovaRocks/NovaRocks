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

//! MV product commands and outcomes.
//!
//! SQL lowers into this vocabulary before it reaches the product.  In
//! particular, no parser AST, MySQL session, Frontend service, connector
//! registry, or role aggregate belongs in these values.

use std::fmt;

use novarocks_spi::connector::{
    ConnectorTableObjectId, ConnectorWriteOperationId, LakePublicationId,
};
use uuid::Uuid;

use crate::persistence::definition::CreateMvDefinitionRequest;
use crate::persistence::dependency::CreateMvDependencyRequest;
use crate::repository::InitialMvRefreshConfiguration;

/// Join refresh shape retained until query assembly admits exact connector
/// bindings for the corresponding write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvIncrementalJoinMode {
    AppendOnly,
    Coalesce,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvIncrementalWriteMode {
    FastAppend,
    RowDelta,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvIncrementalRewriteEvidence {
    None,
    Aggregate,
    JoinAggregate,
    BranchUnionAggregate,
}

/// An already-canonical MV target identity.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MvTarget {
    catalog: Option<String>,
    namespace: String,
    name: String,
}

impl MvTarget {
    pub fn try_new(
        catalog: Option<String>,
        namespace: String,
        name: String,
    ) -> Result<Self, MvProductError> {
        if catalog.as_deref().is_some_and(str::is_empty) || namespace.is_empty() || name.is_empty()
        {
            return Err(MvProductError::new(
                MvProductErrorKind::InvalidRequest,
                "MV target identity must contain non-empty canonical parts",
            ));
        }
        Ok(Self {
            catalog,
            namespace,
            name,
        })
    }

    /// This is for adapters whose SQL/catalog admission has already produced
    /// canonical identifiers.
    pub fn from_parts(catalog: Option<&str>, namespace: &str, name: &str) -> Self {
        Self::try_new(
            catalog.map(str::to_owned),
            namespace.to_owned(),
            name.to_owned(),
        )
        .expect("MV target adapter passed an empty canonical identifier")
    }

    pub fn catalog(&self) -> Option<&str> {
        self.catalog.as_deref()
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// One immutable product operation identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MvOperationContext {
    pub operation_id: Uuid,
}

/// Product-reserved identity for one lake-authoritative MV publication.
/// Provider write IDs and staging names are derived from this one immutable
/// value so an outer query adapter cannot split a publication across effects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshAttemptIdentity {
    pub publication_id: LakePublicationId,
}

impl MvRefreshAttemptIdentity {
    pub(crate) fn reserve() -> Self {
        Self {
            publication_id: LakePublicationId::new_v7(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        Ok(())
    }

    pub fn write_operation_id(&self) -> ConnectorWriteOperationId {
        self.publication_id.into()
    }

    pub fn staging_branch(&self) -> String {
        format!("__novarocks_mv_publication_{}", self.publication_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvCreateCommand {
    pub target: MvTarget,
    pub if_not_exists: bool,
    pub definition: CreateMvDefinitionRequest,
    pub refresh: InitialMvRefreshConfiguration,
    pub dependencies: Vec<CreateMvDependencyRequest>,
}

/// Product commands after SQL lowering and before provider work.
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "the public product command carries the frozen CREATE request inline; boxing changes the cross-crate command contract"
)]
pub enum MvCommand {
    Create(MvCreateCommand),
    Drop { target: MvTarget, if_exists: bool },
    Refresh { target: MvTarget, full: bool },
    Show { namespace: Option<String> },
}

/// One staged, catalog-invisible CREATE target.
///
/// The provider keeps the staged handle and its frozen field bindings; the
/// product holds only the identity it needs to order publish, abort and the
/// responsibility record. A staged target is not a table: it has no catalog
/// entry and no snapshot until it is published.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvStagedTarget {
    pub target: MvTarget,
    /// The provider operation this stage belongs to. Publish and abort must
    /// name the same one, so a lost response cannot be settled by a different
    /// attempt.
    pub staged_operation_id: Uuid,
}

/// A published CREATE target.
///
/// `object_id` is the provider's own opaque identity for the exact object the
/// publish created. It is not a display string and is never parsed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvCreatedTarget {
    pub target: MvTarget,
    pub object_id: ConnectorTableObjectId,
}

/// The Provider's exact CREATE publication verdict. A NoOp creates no target
/// identity and grants no authority to install a Current projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvCreatePublicationOutcome {
    Applied(MvCreatedTarget),
    NoOp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvProductResult {
    Acknowledged,
    Created(MvCreatedTarget),
    Dropped,
    Listed(Vec<MvTarget>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvProductErrorKind {
    InvalidRequest,
    Conflict,
    Unavailable,
    ProviderKnownUncommitted,
    CommitUnknown,
    TargetReplaced,
    Corruption,
    ShutdownCancelled,
    /// A provider-side target mutation is known committed, but a required
    /// product finalization step did not complete.
    KnownCommittedFinalizeFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvProductError {
    kind: MvProductErrorKind,
    message: String,
    original_failure: Option<novarocks_failure_contract::SharedFailureCause>,
    compile_control: Option<novarocks_type_contract::CompileControlError>,
    unavailable_implementation: Option<novarocks_sql::analyze_error::AnalyzeError>,
}

impl MvProductError {
    pub fn new(kind: MvProductErrorKind, message: impl Into<String>) -> Self {
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
            MvProductErrorKind::CommitUnknown | MvProductErrorKind::KnownCommittedFinalizeFailed
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

    pub const fn kind(&self) -> MvProductErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MvProductError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvProductError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.original_failure.as_ref().map(|cause| cause as _)
    }
}

/// Reasons authored by the MV product itself, within the metadata domain.
/// Original domain and compile-control causes keep their own facts instead.
pub mod failure_reason {
    use std::num::NonZeroU16;
    pub const INVALID_REQUEST: NonZeroU16 = NonZeroU16::new(920).unwrap();
    pub const CONFLICT: NonZeroU16 = NonZeroU16::new(921).unwrap();
    pub const UNAVAILABLE: NonZeroU16 = NonZeroU16::new(922).unwrap();
    pub const PROVIDER_KNOWN_UNCOMMITTED: NonZeroU16 = NonZeroU16::new(923).unwrap();
    pub const COMMIT_UNKNOWN: NonZeroU16 = NonZeroU16::new(924).unwrap();
    pub const TARGET_REPLACED: NonZeroU16 = NonZeroU16::new(925).unwrap();
    pub const CORRUPTION: NonZeroU16 = NonZeroU16::new(926).unwrap();
    pub const SHUTDOWN_CANCELLED: NonZeroU16 = NonZeroU16::new(927).unwrap();
    pub const KNOWN_COMMITTED_FINALIZE_FAILED: NonZeroU16 = NonZeroU16::new(928).unwrap();
}
impl novarocks_failure_contract::FailureProjection for MvProductError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass as C, FailureFact, FailureReason, domain};
        if let Some(original) = &self.original_failure {
            return original.failure_fact();
        }
        if let Some(control) = &self.compile_control {
            return control.failure_fact();
        }
        let (class, reason) = match self.kind {
            MvProductErrorKind::InvalidRequest => (C::Data, failure_reason::INVALID_REQUEST),
            MvProductErrorKind::Conflict => (C::Dependency, failure_reason::CONFLICT),
            MvProductErrorKind::Unavailable => (C::Dependency, failure_reason::UNAVAILABLE),
            MvProductErrorKind::ProviderKnownUncommitted => {
                (C::Dependency, failure_reason::PROVIDER_KNOWN_UNCOMMITTED)
            }
            MvProductErrorKind::CommitUnknown => (C::Dependency, failure_reason::COMMIT_UNKNOWN),
            MvProductErrorKind::TargetReplaced => (C::Dependency, failure_reason::TARGET_REPLACED),
            MvProductErrorKind::Corruption => (C::Data, failure_reason::CORRUPTION),
            MvProductErrorKind::ShutdownCancelled => {
                return FailureFact::stop_observation(FailureReason::new(
                    domain::METADATA,
                    failure_reason::SHUTDOWN_CANCELLED,
                ));
            }
            MvProductErrorKind::KnownCommittedFinalizeFailed => (
                C::Dependency,
                failure_reason::KNOWN_COMMITTED_FINALIZE_FAILED,
            ),
        };
        FailureFact::new(class, FailureReason::new(domain::METADATA, reason))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_rejects_noncanonical_empty_parts() {
        let error = MvTarget::try_new(
            Some(String::new()),
            "sales".to_string(),
            "daily".to_string(),
        )
        .expect_err("empty catalog is not a canonical target");
        assert_eq!(error.kind(), MvProductErrorKind::InvalidRequest);
    }

    #[test]
    fn product_target_is_a_parser_free_value() {
        let target = MvTarget::from_parts(Some("ice"), "sales", "daily");
        assert_eq!(target.catalog(), Some("ice"));
        assert_eq!(target.namespace(), "sales");
        assert_eq!(target.name(), "daily");
    }

    #[test]
    fn refresh_attempt_derives_all_provider_identity_from_one_publication() {
        let attempt = MvRefreshAttemptIdentity::reserve();
        attempt.validate().expect("complete publication identity");
        assert_eq!(
            attempt.write_operation_id().to_bytes(),
            attempt.publication_id.to_bytes()
        );
        assert_eq!(
            attempt.staging_branch(),
            format!("__novarocks_mv_publication_{}", attempt.publication_id)
        );
    }
}
