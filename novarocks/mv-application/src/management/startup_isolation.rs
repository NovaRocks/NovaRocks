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

//! The deployment's own statement that a previous incarnation is isolated.
//!
//! A restarted process closes management on every target its deployment owns
//! but a previous incarnation wrote, because an unanswered dispatch leaves no
//! trace in the documents. An operator can retire one such barrier per target
//! with a declaration, which does not scale to a deployment that restarts a
//! process holding dozens of them.
//!
//! What does scale is the deployment saying it once, outside this process,
//! about the writer rather than about each target. That statement is what this
//! module carries. It is deliberately not a lease, not an election and not a
//! liveness probe: it asserts that a named incarnation was isolated at a named
//! moment, and the remote-effect window still has to elapse from that moment
//! before anything reopens. Without a configured guarantee for the effect's
//! scope there is no window to elapse, and the deployment stays read-only --
//! which is the default, not a failure.
//!
//! Three bindings keep a statement from outliving its occasion: the deployment
//! it names, the incarnation of the process it was written for, and a nonce
//! the launch supplies. A file left behind by a previous start names a
//! previous nonce, so it cannot readmit anything here.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use novarocks_spi::connector::ConnectorTableIdentity;

use super::{
    DeploymentOwner, IsolationEvidence, ManagedMvTarget, ManagementTimestamp, ProcessIncarnation,
    ReadmissionError,
};

const MAX_NONCE_BYTES: usize = 128;

/// A per-launch token the orchestration supplies to both the process and the
/// declaration it wrote for that launch.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StartupNonce(Arc<str>);

impl StartupNonce {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, StartupIsolationError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(StartupIsolationError::EmptyNonce);
        }
        if value.len() > MAX_NONCE_BYTES
            || value
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        {
            return Err(StartupIsolationError::InvalidNonce);
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Which of this deployment's targets a statement speaks for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartupIsolationScope {
    /// Every target this deployment owns. The orchestration that stopped the
    /// previous process stopped it for all of them at once.
    Deployment,
    /// Only these exact targets. Anything outside the list stays read-only,
    /// because a statement about some targets is not a statement about the
    /// writer's other work.
    Targets(HashSet<ConnectorTableIdentity>),
}

impl StartupIsolationScope {
    fn covers(&self, table: &ConnectorTableIdentity) -> bool {
        match self {
            Self::Deployment => true,
            Self::Targets(targets) => targets.contains(table),
        }
    }
}

/// What an orchestration wrote, before it was checked against this process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupIsolationDeclaration {
    pub deployment: DeploymentOwner,
    /// The incarnation of the process this statement was written for. A
    /// declaration is written after the new process's identity is known, which
    /// is what stops it from being reused by the next one.
    pub for_incarnation: ProcessIncarnation,
    pub nonce: StartupNonce,
    /// The incarnations whose dispatches can no longer land.
    pub isolated_incarnations: BTreeSet<ProcessIncarnation>,
    pub scope: StartupIsolationScope,
    /// The deployment declares every other incarnation's old effects settled.
    pub all_other_incarnations_settled: bool,
    /// When the isolation was complete. It is the conservative T the remote
    /// effect window is measured from, so a statement that names a later
    /// moment than the truth delays readmission rather than rushing it.
    pub isolated_at: ManagementTimestamp,
    /// How the orchestration knows, for the person who later has to check it.
    pub source: String,
}

/// A declaration this process has accepted for this launch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupIsolationEvidence {
    deployment: DeploymentOwner,
    incarnation: ProcessIncarnation,
    all_other_incarnations_settled: bool,
    isolated_incarnations: BTreeSet<ProcessIncarnation>,
    scope: StartupIsolationScope,
    isolated_at: ManagementTimestamp,
    source: Arc<str>,
}

impl StartupIsolationEvidence {
    /// Check one declaration against the process it claims to be for.
    ///
    /// Every refusal leaves the deployment read-only, which is the same place
    /// no declaration at all leaves it. That is deliberate: the alternative to
    /// a statement this process cannot verify is not a weaker statement, it is
    /// no statement.
    pub fn try_accept(
        declaration: StartupIsolationDeclaration,
        local_owner: &DeploymentOwner,
        local_incarnation: &ProcessIncarnation,
        launch_nonce: &StartupNonce,
        now: ManagementTimestamp,
    ) -> Result<Self, StartupIsolationError> {
        if declaration.deployment != *local_owner {
            return Err(StartupIsolationError::ForeignDeployment);
        }
        if declaration.for_incarnation != *local_incarnation {
            return Err(StartupIsolationError::ForeignIncarnation);
        }
        if declaration.nonce != *launch_nonce {
            return Err(StartupIsolationError::StaleNonce);
        }
        if declaration.isolated_incarnations.is_empty()
            && !declaration.all_other_incarnations_settled
        {
            return Err(StartupIsolationError::NoIsolatedIncarnation);
        }
        if declaration
            .isolated_incarnations
            .contains(local_incarnation)
        {
            return Err(StartupIsolationError::SelfIsolation);
        }
        if let StartupIsolationScope::Targets(targets) = &declaration.scope
            && targets.is_empty()
        {
            return Err(StartupIsolationError::EmptyScope);
        }
        if declaration.isolated_at > now {
            return Err(StartupIsolationError::IsolationInTheFuture);
        }
        if declaration.source.trim().is_empty() {
            return Err(StartupIsolationError::MissingSource);
        }
        if declaration.all_other_incarnations_settled
            && declaration.scope != StartupIsolationScope::Deployment
        {
            return Err(StartupIsolationError::SettlementRequiresDeploymentScope);
        }
        Ok(Self {
            deployment: declaration.deployment,
            incarnation: declaration.for_incarnation,
            all_other_incarnations_settled: declaration.all_other_incarnations_settled,
            isolated_incarnations: declaration.isolated_incarnations,
            scope: declaration.scope,
            isolated_at: declaration.isolated_at,
            source: Arc::from(declaration.source.as_str()),
        })
    }

    pub(super) fn settled_for_launch(
        &self,
        owner: &DeploymentOwner,
        incarnation: &ProcessIncarnation,
    ) -> bool {
        self.all_other_incarnations_settled
            && owner == &self.deployment
            && incarnation == &self.incarnation
    }

    pub(super) fn covers_incarnations(&self, prior: &BTreeSet<ProcessIncarnation>) -> bool {
        prior.is_subset(&self.isolated_incarnations)
    }

    /// The isolation this statement establishes for one recovered target, or
    /// nothing when it does not speak for that target or that writer.
    ///
    /// A permit still has to come from the policy window: this only says when
    /// the old writer stopped being able to act, never that the window from
    /// that moment has elapsed.
    pub fn isolation_for(
        &self,
        target: &ManagedMvTarget,
        old_incarnation: &ProcessIncarnation,
    ) -> Option<Result<IsolationEvidence, ReadmissionError>> {
        if !self.isolated_incarnations.contains(old_incarnation)
            || !self.scope.covers(target.table())
        {
            return None;
        }
        Some(IsolationEvidence::try_new(
            target.clone(),
            old_incarnation.clone(),
            self.isolated_at,
            self.source.as_ref(),
        ))
    }

    pub const fn isolated_at(&self) -> ManagementTimestamp {
        self.isolated_at
    }

    pub fn source(&self) -> &str {
        &self.source
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupIsolationError {
    EmptyNonce,
    InvalidNonce,
    ForeignDeployment,
    ForeignIncarnation,
    StaleNonce,
    NoIsolatedIncarnation,
    SelfIsolation,
    EmptyScope,
    SettlementRequiresDeploymentScope,
    IsolationInTheFuture,
    MissingSource,
}

impl std::fmt::Display for StartupIsolationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::EmptyNonce => "startup nonce must not be empty",
            Self::InvalidNonce => "startup nonce is invalid or exceeds its limit",
            Self::ForeignDeployment => {
                "startup isolation evidence names another deployment; this process stays read-only"
            }
            Self::ForeignIncarnation => {
                "startup isolation evidence was written for another process incarnation; a \
                 statement left behind by an earlier start cannot readmit this one"
            }
            Self::StaleNonce => {
                "startup isolation evidence carries another launch's nonce; this process stays \
                 read-only"
            }
            Self::NoIsolatedIncarnation => {
                "startup isolation evidence names no isolated incarnation, so it states nothing"
            }
            Self::SelfIsolation => "startup isolation evidence names this very process as isolated",
            Self::SettlementRequiresDeploymentScope => "all prior effects require deployment scope",
            Self::EmptyScope => "startup isolation evidence lists no target in its scope",
            Self::IsolationInTheFuture => {
                "startup isolation evidence is dated after this process read it"
            }
            Self::MissingSource => "startup isolation evidence states no source",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for StartupIsolationError {}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, ConnectorTableObjectId,
    };

    fn owner() -> DeploymentOwner {
        DeploymentOwner::parse("deployment-a").expect("owner")
    }

    fn incarnation(value: &str) -> ProcessIncarnation {
        ProcessIncarnation::parse(value).expect("incarnation")
    }

    fn nonce(value: &str) -> StartupNonce {
        StartupNonce::parse(value).expect("nonce")
    }

    fn table(name: &str) -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("ice").expect("catalog"),
            namespace: Arc::from("db"),
            table: Arc::from(name),
        }
    }

    fn target(name: &str) -> ManagedMvTarget {
        ManagedMvTarget::try_new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("ice").expect("catalog"),
                CatalogVersion::from_bytes([1; 32]),
            ),
            table(name),
            ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"object-a"))
                .expect("object id"),
        )
        .expect("managed target")
    }

    fn declaration() -> StartupIsolationDeclaration {
        StartupIsolationDeclaration {
            deployment: owner(),
            for_incarnation: incarnation("new"),
            nonce: nonce("launch-1"),
            isolated_incarnations: [incarnation("old")].into_iter().collect(),
            scope: StartupIsolationScope::Deployment,
            all_other_incarnations_settled: false,
            isolated_at: ManagementTimestamp::from_unix_millis(1_000),
            source: "supervisor recorded pid 42 exited".to_string(),
        }
    }

    fn accept(
        declaration: StartupIsolationDeclaration,
    ) -> Result<StartupIsolationEvidence, StartupIsolationError> {
        StartupIsolationEvidence::try_accept(
            declaration,
            &owner(),
            &incarnation("new"),
            &nonce("launch-1"),
            ManagementTimestamp::from_unix_millis(2_000),
        )
    }

    #[test]
    fn a_statement_for_this_launch_covers_the_writer_it_names() {
        let evidence = accept(declaration()).expect("this launch's own statement");

        let isolation = evidence
            .isolation_for(&target("mv"), &incarnation("old"))
            .expect("the statement covers this writer")
            .expect("isolation evidence");
        assert_eq!(isolation.old_incarnation(), &incarnation("old"));
        assert_eq!(
            isolation.isolated_at(),
            ManagementTimestamp::from_unix_millis(1_000),
            "the declared moment is the conservative T the window runs from"
        );
    }

    #[test]
    fn a_statement_says_nothing_about_a_writer_it_does_not_name() {
        let evidence = accept(declaration()).expect("this launch's own statement");

        assert!(
            evidence
                .isolation_for(&target("mv"), &incarnation("another"))
                .is_none()
        );
    }

    #[test]
    fn a_listed_scope_leaves_every_other_target_read_only() {
        let mut declaration = declaration();
        declaration.scope = StartupIsolationScope::Targets([table("mv")].into_iter().collect());
        let evidence = accept(declaration).expect("a scoped statement is still a statement");

        assert!(
            evidence
                .isolation_for(&target("mv"), &incarnation("old"))
                .is_some()
        );
        assert!(
            evidence
                .isolation_for(&target("other"), &incarnation("old"))
                .is_none(),
            "a statement about some targets is not a statement about the rest"
        );
    }

    #[test]
    fn a_previous_launchs_file_cannot_readmit_this_one() {
        let mut stale = declaration();
        stale.nonce = nonce("launch-0");

        assert_eq!(accept(stale), Err(StartupIsolationError::StaleNonce));
    }

    #[test]
    fn a_statement_written_for_another_incarnation_is_refused() {
        let mut other = declaration();
        other.for_incarnation = incarnation("someone-else");

        assert_eq!(
            accept(other),
            Err(StartupIsolationError::ForeignIncarnation)
        );
    }

    #[test]
    fn a_statement_from_another_deployment_is_refused() {
        let mut foreign = declaration();
        foreign.deployment = DeploymentOwner::parse("deployment-b").expect("owner");

        assert_eq!(
            accept(foreign),
            Err(StartupIsolationError::ForeignDeployment)
        );
    }

    #[test]
    fn a_process_may_not_declare_itself_isolated() {
        let mut itself = declaration();
        itself.isolated_incarnations = [incarnation("new")].into_iter().collect();

        assert_eq!(accept(itself), Err(StartupIsolationError::SelfIsolation));
    }

    #[test]
    fn a_statement_dated_after_this_process_read_it_is_refused() {
        let mut ahead = declaration();
        ahead.isolated_at = ManagementTimestamp::from_unix_millis(9_000);

        assert_eq!(
            accept(ahead),
            Err(StartupIsolationError::IsolationInTheFuture)
        );
    }

    #[test]
    fn an_empty_statement_states_nothing() {
        let mut empty = declaration();
        empty.isolated_incarnations = BTreeSet::new();
        assert_eq!(
            accept(empty),
            Err(StartupIsolationError::NoIsolatedIncarnation)
        );

        let mut no_targets = declaration();
        no_targets.scope = StartupIsolationScope::Targets(HashSet::new());
        assert_eq!(accept(no_targets), Err(StartupIsolationError::EmptyScope));

        let mut no_source = declaration();
        no_source.source = "   ".to_string();
        assert_eq!(accept(no_source), Err(StartupIsolationError::MissingSource));
    }

    #[test]
    fn a_nonce_is_a_single_token() {
        assert_eq!(
            StartupNonce::parse(""),
            Err(StartupIsolationError::EmptyNonce)
        );
        assert_eq!(
            StartupNonce::parse("two tokens"),
            Err(StartupIsolationError::InvalidNonce)
        );
        assert_eq!(
            StartupNonce::parse("x".repeat(MAX_NONCE_BYTES + 1)),
            Err(StartupIsolationError::InvalidNonce)
        );
    }
}
