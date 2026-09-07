// Create-or-replace remains outside the production write path, while existing
// table publication and create both use this frontier. Every state transition
// is covered by focused tests.
#![allow(dead_code)]
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

//! The unified provider-private transaction.
//!
//! Design: ADR-0118 (docs/adr/ADR-0118-iceberg-provider-private-catalog-owner.md)
//!
//! All three constructors on [`super::NovaRocksCatalog`] return this one type.
//! It exists because the vendored `iceberg::Transaction::commit` cannot express
//! what the publication contract requires: it returns a plain error and retries
//! on `Error::retryable()`, so a caller cannot tell a rejected request from a
//! lost response, and a lost response gets resent.
//!
//! This type answers that question instead, and enforces three rules the
//! previous per-family state machines each restated in their own way:
//!
//! 1. **One dispatch.** [`Transaction::commit`] performs exactly one external
//!    request. It never loops, never reloads-and-resends, and never delegates
//!    to a layer that would.
//! 2. **Unknown closes mutation authority.** Once a request may have landed,
//!    `commit` and `abort` both refuse. The only remaining move is
//!    [`Transaction::adjudicate`], which is read-only.
//! 3. **Abort is pre-dispatch only.** Cleanup after a possible dispatch is how
//!    committed data gets deleted, so the type refuses rather than trusting
//!    each caller to remember.

use std::sync::Arc;

use async_trait::async_trait;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ExternalMutationEffect,
};

use super::error::{
    CatalogCommitEvidence, CatalogOutcome, classify_dispatched_error, proves_uncommitted,
    uncommitted_failure_kind,
};
use super::{CatalogCreateIntent, CatalogTableName};

/// The exact identity a publication attempt is bound to.
///
/// # On the identity type
///
/// The write and CTAS families carry a `LakePublicationId` end to end. The
/// catalog-mutation family does not: the frontend mints a fresh
/// `ConnectorMutationOperationId` per request and the neutral request carries
/// no publication id. Synthesising a `LakePublicationId` from those bytes here
/// would manufacture an identity the frontend never saw, which is precisely the
/// "second family UUID" the publication contract forbids.
///
/// So this holds whatever exact identity the caller actually owns, as opaque
/// bytes plus the label naming which authority minted it. One identity per
/// frontier is preserved; inventing one is not.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransactionIdentity {
    pub(crate) authority: &'static str,
    pub(crate) bytes: [u8; 16],
}

impl TransactionIdentity {
    pub(crate) fn new(authority: &'static str, bytes: [u8; 16]) -> Self {
        Self { authority, bytes }
    }

    pub(crate) fn hex(&self) -> String {
        self.bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// Proof that a specific publication is present in the catalog.
///
/// One flat shape, with the fields a publication may or may not produce left
/// optional. Different operations genuinely observe different things -- a
/// snapshot append lands on a snapshot id, a conditional metadata create lands
/// on a metadata file whose digest is re-read afterwards -- and this records
/// whichever of them the publisher saw.
///
/// It is deliberately not a variant per operation. A variant would put the
/// question "which kind of catalog published this?" back in front of every
/// caller, which is the thing this owner exists to remove; a caller reads the
/// fields it needs and ignores the rest.
///
/// There is deliberately no `Default`: the effect is the one field a proof can
/// never be vague about, so every proof states it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitProof {
    /// Snapshot the publication landed on, when the operation produces one.
    pub(crate) snapshot_id: Option<i64>,
    /// Table UUID observed at proof time.
    pub(crate) table_uuid: Option<Arc<str>>,
    /// Metadata file the target now resolves to, when the operation publishes
    /// one directly.
    pub(crate) metadata_location: Option<Arc<str>>,
    /// Digest of that metadata, read back after publication rather than taken
    /// from what was sent -- which is what makes it authoritative.
    pub(crate) metadata_digest: Option<Arc<str>>,
    /// Whether the catalog state changed or the request was already satisfied.
    pub(crate) effect: ExternalMutationEffect,
}

impl CommitProof {
    pub(crate) fn new(effect: ExternalMutationEffect) -> Self {
        Self {
            snapshot_id: None,
            table_uuid: None,
            metadata_location: None,
            metadata_digest: None,
            effect,
        }
    }

    pub(crate) fn applied(snapshot_id: Option<i64>) -> Self {
        Self {
            snapshot_id,
            ..Self::new(ExternalMutationEffect::Applied)
        }
    }

    pub(crate) fn no_op() -> Self {
        Self::new(ExternalMutationEffect::NoOp)
    }

    pub(crate) fn with_table_uuid(mut self, uuid: impl Into<Arc<str>>) -> Self {
        self.table_uuid = Some(uuid.into());
        self
    }

    pub(crate) fn with_metadata(
        mut self,
        location: impl Into<Arc<str>>,
        digest: impl Into<Arc<str>>,
    ) -> Self {
        self.metadata_location = Some(location.into());
        self.metadata_digest = Some(digest.into());
        self
    }
}

/// Performs the single external request behind one transaction, and can later
/// re-read the catalog to prove whether that request landed.
///
/// Each concrete catalog supplies its own implementation: a REST staged-create
/// commit, a Hadoop conditional metadata publication, or a plain
/// `update_table`. The transaction owns the contract; this owns the mechanism.
#[async_trait]
pub(crate) trait CatalogCommitDispatch: std::fmt::Debug + Send + Sync {
    /// Issue **exactly one** external request.
    ///
    /// Implementations must not retry, must not fall back to a second request,
    /// and must not consult `Error::retryable()`. Returning `Err` hands the
    /// dispatch question to [`super::error::proves_uncommitted`].
    async fn dispatch_once(
        &self,
        staged: Option<crate::iceberg::TableCommit>,
    ) -> Result<CommitProof, crate::iceberg::Error>;

    /// Re-read the catalog and report whether this exact publication is
    /// present.
    ///
    /// Read-only by contract. `Ok(None)` means "not observed", which is **not**
    /// proof of absence — a caller may never escalate it to "safe to delete".
    async fn adjudicate(&self) -> Result<Option<CommitProof>, ConnectorError>;

    /// Release caller-owned temporary state. Only ever invoked before dispatch.
    async fn abort_before_dispatch(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// What the transaction is publishing. Kept private to the catalog module so
/// callers above see one lifecycle rather than four.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionShape {
    Existing,
    Create(CatalogCreateIntent),
    CreateOrReplace,
}

/// Where the single frontier currently stands.
#[derive(Debug)]
enum TransactionState {
    /// Admitted. Nothing external has been attempted yet.
    Admitted,
    /// Terminal: the publication is proven present.
    Committed,
    /// Terminal: proven that nothing was published.
    Uncommitted,
    /// The request may have landed. Mutation authority is closed for good.
    Unknown(CatalogCommitEvidence),
}

/// Inputs for a transaction against an existing table.
#[derive(Clone, Debug)]
pub(crate) struct TransactionRequest {
    pub(crate) identity: TransactionIdentity,
    pub(crate) target: CatalogTableName,
    pub(crate) target_ref: Arc<str>,
    pub(crate) base_snapshot_id: Option<i64>,
    pub(crate) expected_table_uuid: Option<Arc<str>>,
    /// Snapshot-summary property that identifies this exact publication.
    ///
    /// Without one, a later adjudication cannot tell this attempt's snapshot
    /// from anyone else's, so it refuses to answer rather than guessing.
    pub(crate) marker: Option<(Arc<str>, Arc<str>)>,
}

/// Inputs for a transaction that creates a table.
///
/// Not `Clone`: the vendored `TableCreation` is not cloneable, and a creation
/// request is consumed by exactly one attempt anyway.
#[derive(Debug)]
pub(crate) struct CreateTableTransactionRequest {
    pub(crate) identity: TransactionIdentity,
    pub(crate) target: CatalogTableName,
    pub(crate) intent: CatalogCreateIntent,
    /// The full table definition. Creation has no caller-owned staging gap in
    /// which to supply this later, so it is part of admission: a definition the
    /// catalog cannot accept is refused before anything is attempted.
    pub(crate) creation: crate::iceberg::TableCreation,
    /// Explicit warehouse root. CTAS admission requires one; empty-table
    /// creation may fall back to the catalog's own table location.
    pub(crate) warehouse: Option<Arc<str>>,
}

/// What admission observed, for a caller that must freeze publication evidence
/// before anything is dispatched.
///
/// Flat and optional for the same reason [`CommitProof`] is: the facts differ by
/// operation, and a caller should read what it needs without asking which kind
/// of catalog admitted it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct AdmissionFacts {
    pub(crate) table_uuid: Option<Arc<str>>,
    pub(crate) metadata_location: Option<Arc<str>>,
    pub(crate) metadata_digest: Option<Arc<str>>,
}

/// One publication frontier, from admission to a terminal outcome.
#[derive(Debug)]
pub(crate) struct Transaction {
    identity: TransactionIdentity,
    target: CatalogTableName,
    shape: TransactionShape,
    evidence: CatalogCommitEvidence,
    dispatch: Arc<dyn CatalogCommitDispatch>,
    staged: Option<crate::iceberg::TableCommit>,
    admission: AdmissionFacts,
    state: TransactionState,
}

impl Transaction {
    pub(crate) fn new(
        identity: TransactionIdentity,
        target: CatalogTableName,
        shape: TransactionShape,
        evidence: CatalogCommitEvidence,
        dispatch: Arc<dyn CatalogCommitDispatch>,
    ) -> Self {
        Self {
            identity,
            target,
            shape,
            evidence,
            dispatch,
            staged: None,
            admission: AdmissionFacts::default(),
            state: TransactionState::Admitted,
        }
    }

    /// Record what admission observed. Set by the constructor that admitted
    /// this transaction, before the caller can dispatch anything.
    pub(crate) fn with_admission_facts(mut self, facts: AdmissionFacts) -> Self {
        self.admission = facts;
        self
    }

    /// What admission observed, for freezing evidence ahead of dispatch.
    pub(crate) fn admission_facts(&self) -> &AdmissionFacts {
        &self.admission
    }

    pub(crate) fn identity(&self) -> &TransactionIdentity {
        &self.identity
    }

    pub(crate) fn target(&self) -> &CatalogTableName {
        &self.target
    }

    pub(crate) fn shape(&self) -> TransactionShape {
        self.shape
    }

    /// True once no further mutation may be issued for this frontier.
    pub(crate) fn mutation_authority_closed(&self) -> bool {
        matches!(
            self.state,
            TransactionState::Unknown(_) | TransactionState::Committed
        )
    }

    /// Supply the updates this publication will apply.
    ///
    /// Refused once the frontier has moved: re-staging after a dispatch would
    /// mean publishing something other than what was admitted.
    pub(crate) fn stage(
        &mut self,
        staged: crate::iceberg::TableCommit,
    ) -> Result<(), ConnectorError> {
        if !matches!(self.state, TransactionState::Admitted) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "Iceberg publication {} has already left admission; staging is refused",
                    self.identity.hex()
                ),
            ));
        }
        self.staged = Some(staged);
        Ok(())
    }

    /// Publish, with exactly one external request.
    ///
    /// A second call is refused rather than re-dispatched: repeating a request
    /// whose first outcome is unknown is the specific mistake this owner
    /// exists to prevent.
    pub(crate) async fn commit(&mut self) -> CatalogOutcome<CommitProof> {
        match &self.state {
            TransactionState::Admitted => {}
            TransactionState::Committed => {
                return CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::InvalidRequest,
                    format!(
                        "Iceberg publication {} already committed; commit is not repeatable",
                        self.identity.hex()
                    ),
                );
            }
            TransactionState::Uncommitted => {
                return CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::InvalidRequest,
                    format!(
                        "Iceberg publication {} already failed; start a new attempt instead of \
                         reusing this transaction",
                        self.identity.hex()
                    ),
                );
            }
            TransactionState::Unknown(evidence) => {
                // Re-dispatching here is exactly how a lost response becomes a
                // duplicate publication. Report the standing unknown instead.
                return CatalogOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        format!(
                            "Iceberg publication {} outcome is unknown; mutation authority is \
                             closed for this attempt",
                            self.identity.hex()
                        ),
                    ),
                    evidence: evidence.clone(),
                };
            }
        }

        let staged = self.staged.take();
        match self.dispatch.dispatch_once(staged).await {
            Ok(proof) => {
                self.state = TransactionState::Committed;
                let effect = proof.effect;
                CatalogOutcome::committed(proof, effect)
            }
            Err(error) => {
                if proves_uncommitted(&error) {
                    self.state = TransactionState::Uncommitted;
                    return CatalogOutcome::KnownUncommitted {
                        failure: ConnectorMutationFailure::new(
                            uncommitted_failure_kind(&error),
                            error.to_string(),
                        ),
                    };
                }
                let evidence = self.evidence.clone();
                self.state = TransactionState::Unknown(evidence.clone());
                classify_dispatched_error(&error, || evidence)
            }
        }
    }

    /// Release caller-owned temporary state, before anything was dispatched.
    ///
    /// Refused once the outcome is unknown. Cleaning up after a possible
    /// dispatch is how a committed table loses its files.
    pub(crate) async fn abort(&mut self) -> Result<(), ConnectorError> {
        match &self.state {
            TransactionState::Admitted => {
                self.dispatch.abort_before_dispatch().await?;
                self.state = TransactionState::Uncommitted;
                Ok(())
            }
            TransactionState::Uncommitted => Ok(()),
            TransactionState::Committed => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "Iceberg publication {} is committed; abort would delete published state",
                    self.identity.hex()
                ),
            )),
            TransactionState::Unknown(_) => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "Iceberg publication {} outcome is unknown; abort is refused because the \
                     request may have been applied",
                    self.identity.hex()
                ),
            )),
        }
    }

    /// Read-only adjudication after an unknown outcome.
    ///
    /// Only exact positive evidence upgrades the verdict. Absence keeps the
    /// state unknown: a catalog that does not show the publication may simply
    /// not show it yet, and treating that as proof of absence is how orphaned
    /// data gets deleted while it is still live.
    pub(crate) async fn adjudicate(
        &mut self,
    ) -> Result<CatalogOutcome<CommitProof>, ConnectorError> {
        let TransactionState::Unknown(evidence) = &self.state else {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "Iceberg publication {} is not in an unknown state; adjudication applies only \
                     after a possibly-dispatched request",
                    self.identity.hex()
                ),
            ));
        };
        let evidence = evidence.clone();
        match self.dispatch.adjudicate().await? {
            Some(proof) => {
                self.state = TransactionState::Committed;
                let effect = proof.effect;
                Ok(CatalogOutcome::committed(proof, effect))
            }
            None => Ok(CatalogOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    format!(
                        "Iceberg publication {} was not observed in the catalog; absence is not \
                         proof that it did not commit",
                        self.identity.hex()
                    ),
                ),
                evidence,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::iceberg::{Error as IcebergError, ErrorKind};

    #[derive(Debug)]
    enum Behavior {
        Succeed,
        RejectDefinitely,
        LoseResponse,
    }

    #[derive(Debug)]
    struct FakeDispatch {
        behavior: Behavior,
        dispatches: AtomicUsize,
        adjudications: AtomicUsize,
        aborts: AtomicUsize,
        /// What a later read-only adjudication would observe.
        observable: Option<i64>,
    }

    impl FakeDispatch {
        fn new(behavior: Behavior) -> Arc<Self> {
            Arc::new(Self {
                behavior,
                dispatches: AtomicUsize::new(0),
                adjudications: AtomicUsize::new(0),
                aborts: AtomicUsize::new(0),
                observable: None,
            })
        }

        fn with_observable(behavior: Behavior, snapshot_id: i64) -> Arc<Self> {
            Arc::new(Self {
                behavior,
                dispatches: AtomicUsize::new(0),
                adjudications: AtomicUsize::new(0),
                aborts: AtomicUsize::new(0),
                observable: Some(snapshot_id),
            })
        }
    }

    #[async_trait]
    impl CatalogCommitDispatch for FakeDispatch {
        async fn dispatch_once(
            &self,
            _staged: Option<crate::iceberg::TableCommit>,
        ) -> Result<CommitProof, IcebergError> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                Behavior::Succeed => Ok(CommitProof::applied(Some(41))),
                Behavior::RejectDefinitely => Err(IcebergError::new(
                    ErrorKind::CatalogCommitConflicts,
                    "requirement failed",
                )),
                Behavior::LoseResponse => {
                    Err(IcebergError::new(ErrorKind::Unexpected, "connection reset"))
                }
            }
        }

        async fn adjudicate(&self) -> Result<Option<CommitProof>, ConnectorError> {
            self.adjudications.fetch_add(1, Ordering::SeqCst);
            Ok(self.observable.map(|id| CommitProof::applied(Some(id))))
        }

        async fn abort_before_dispatch(&self) -> Result<(), ConnectorError> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn transaction(dispatch: Arc<FakeDispatch>) -> Transaction {
        Transaction::new(
            TransactionIdentity::new("test", [7u8; 16]),
            CatalogTableName::new("db", "t"),
            TransactionShape::Existing,
            CatalogCommitEvidence::for_target("db.t").with_commit_uuid("commit-uuid"),
            dispatch,
        )
    }

    #[tokio::test]
    async fn success_commits_with_exactly_one_dispatch() {
        let dispatch = FakeDispatch::new(Behavior::Succeed);
        let mut tx = transaction(Arc::clone(&dispatch));
        let outcome = tx.commit().await;
        assert!(matches!(outcome, CatalogOutcome::KnownCommitted { .. }));
        assert_eq!(dispatch.dispatches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn definite_rejection_is_uncommitted_and_still_allows_cleanup() {
        let dispatch = FakeDispatch::new(Behavior::RejectDefinitely);
        let mut tx = transaction(Arc::clone(&dispatch));
        let outcome = tx.commit().await;
        assert!(outcome.permits_cleanup());
        assert!(matches!(outcome, CatalogOutcome::KnownUncommitted { .. }));
        assert_eq!(dispatch.dispatches.load(Ordering::SeqCst), 1);
        // Cleanup is legitimate here, so abort must succeed.
        tx.abort().await.expect("abort after definite rejection");
    }

    #[tokio::test]
    async fn definite_conflict_retry_requires_a_fresh_one_dispatch_frontier() {
        let first_dispatch = FakeDispatch::new(Behavior::RejectDefinitely);
        let mut first = transaction(Arc::clone(&first_dispatch));
        let first_outcome = first.commit().await;
        assert!(matches!(
            first_outcome,
            CatalogOutcome::KnownUncommitted { ref failure }
                if failure.kind() == ConnectorMutationFailureKind::Conflict
        ));
        assert_eq!(first_dispatch.dispatches.load(Ordering::SeqCst), 1);

        let second_dispatch = FakeDispatch::new(Behavior::Succeed);
        let mut fresh = transaction(Arc::clone(&second_dispatch));
        assert!(matches!(
            fresh.commit().await,
            CatalogOutcome::KnownCommitted { .. }
        ));
        assert_eq!(second_dispatch.dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(
            first_dispatch.dispatches.load(Ordering::SeqCst)
                + second_dispatch.dispatches.load(Ordering::SeqCst),
            2,
            "two attempts must mean two distinct one-dispatch frontiers"
        );
    }

    #[tokio::test]
    async fn lost_response_closes_mutation_authority() {
        let dispatch = FakeDispatch::new(Behavior::LoseResponse);
        let mut tx = transaction(Arc::clone(&dispatch));
        let outcome = tx.commit().await;
        assert!(matches!(outcome, CatalogOutcome::CommitUnknown { .. }));
        assert!(tx.mutation_authority_closed());

        // The three things that must not happen after an unknown outcome.
        let second = tx.commit().await;
        assert!(matches!(second, CatalogOutcome::CommitUnknown { .. }));
        assert_eq!(
            dispatch.dispatches.load(Ordering::SeqCst),
            1,
            "a second commit must not re-dispatch"
        );
        tx.abort().await.expect_err("abort must be refused");
        assert_eq!(
            dispatch.aborts.load(Ordering::SeqCst),
            0,
            "abort must not reach the dispatcher after an unknown outcome"
        );
    }

    #[tokio::test]
    async fn adjudication_upgrades_only_on_exact_positive_evidence() {
        // Absence keeps it unknown.
        let dispatch = FakeDispatch::new(Behavior::LoseResponse);
        let mut tx = transaction(Arc::clone(&dispatch));
        let _ = tx.commit().await;
        let verdict = tx.adjudicate().await.expect("adjudicate");
        assert!(matches!(verdict, CatalogOutcome::CommitUnknown { .. }));
        assert!(tx.mutation_authority_closed());

        // Presence upgrades it.
        let dispatch = FakeDispatch::with_observable(Behavior::LoseResponse, 99);
        let mut tx = transaction(Arc::clone(&dispatch));
        let _ = tx.commit().await;
        let verdict = tx.adjudicate().await.expect("adjudicate");
        let CatalogOutcome::KnownCommitted { receipt, .. } = verdict else {
            panic!("expected KnownCommitted");
        };
        assert_eq!(receipt.snapshot_id, Some(99));
        assert_eq!(
            dispatch.dispatches.load(Ordering::SeqCst),
            1,
            "adjudication must be read-only"
        );
    }

    #[tokio::test]
    async fn adjudication_is_refused_outside_the_unknown_state() {
        let dispatch = FakeDispatch::new(Behavior::Succeed);
        let mut tx = transaction(Arc::clone(&dispatch));
        tx.adjudicate()
            .await
            .expect_err("adjudication before any dispatch is a contract error");
        let _ = tx.commit().await;
        tx.adjudicate()
            .await
            .expect_err("adjudication after a proven commit is a contract error");
        assert_eq!(dispatch.adjudications.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn abort_before_dispatch_releases_caller_state_without_dispatching() {
        let dispatch = FakeDispatch::new(Behavior::Succeed);
        let mut tx = transaction(Arc::clone(&dispatch));
        tx.abort().await.expect("abort before dispatch");
        assert_eq!(dispatch.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(dispatch.dispatches.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn abort_after_commit_is_refused() {
        let dispatch = FakeDispatch::new(Behavior::Succeed);
        let mut tx = transaction(Arc::clone(&dispatch));
        let _ = tx.commit().await;
        tx.abort()
            .await
            .expect_err("abort must not delete published state");
        assert_eq!(dispatch.aborts.load(Ordering::SeqCst), 0);
    }
}
