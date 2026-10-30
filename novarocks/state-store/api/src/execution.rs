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

//! Borrowed execution contract for a provider's original blocking source.
//!
//! No executor, Root, permission, task, retry policy, or storage algorithm lives
//! here. Implementations must be installed in the existing role/Action owner
//! before borrowed API futures can request a phase. In particular, a Weak slot
//! alone is not custody and dropping a caller does not prove physical exit.

use std::{
    any::Any,
    sync::{Arc, Weak},
    task::{Context, Poll},
    time::Instant,
};

use crate::{InstanceScope, StateStoreError, StateStoreErrorKind, StateStoreProviderId};

/// Purpose identity, never execution authority. Opening is accepted only by
/// the selected Host's private opening port; it cannot authorize transactions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateStoreBlockingOrigin {
    Opening(StateStoreProviderId),
    Instance(InstanceScope),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateStoreBlockingPhase {
    Open,
    Close,
    BeginRead,
    BeginWrite,
    Get,
    Range,
    Put,
    Delete,
    Commit,
    Rollback,
    EvidenceRead,
    EvidenceRelease,
}

/// Original-owned serial request revision. A stale phase must never cause a
/// second commit or be treated as a new permission to run the original call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateStoreBlockingRequest {
    pub phase: StateStoreBlockingPhase,
    pub revision: std::num::NonZeroU64,
}

/// The private concrete provider type supplies its actual Any TypeId. Input,
/// connection, attempt, output, and evidence receiver remain stable fields of
/// this same original. Implementations execute only their existing algorithm.
pub trait StateStoreBlockingOriginal: Any + Send + Sync {
    fn origin(&self) -> StateStoreBlockingOrigin;
    fn requested(&self) -> Option<StateStoreBlockingRequest>;
    /// Pure same-source stop observation for an unpublished business phase.
    /// Settlement must remain possible after a method/borrower is abandoned.
    /// This is not health, dispatch authority, or proof of actual exit.
    fn publication_refusal(
        &self,
        request: StateStoreBlockingRequest,
    ) -> Option<StateStoreExecutionFailure>;
    /// Called only by the accepted physical worker for this exact request.
    /// Must reject a stale request inside the original without redispatch.
    fn run_original_phase(&self, request: StateStoreBlockingRequest);
    /// Original physical receiving is already stable. No I/O or queue publish
    /// occurs here; commit registers evidence and marks only this same attempt.
    fn before_publish(
        &self,
        request: StateStoreBlockingRequest,
    ) -> Result<(), StateStoreExecutionFailure>;
    /// Called once per same request, outside receiving/custody locks. The
    /// original retains a physical failure even if later evidence resolves it.
    fn receive_result(
        &self,
        request: StateStoreBlockingRequest,
        result: Result<(), StateStoreExecutionFailure>,
    );
    /// Only this receipt proof permits the original's next serial phase.
    /// An uncertain publication or failed destructor is never NeverPublished.
    fn receive_exit(
        &self,
        request: StateStoreBlockingRequest,
        exit: Result<StateStoreBlockingExit, StateStoreExecutionFailure>,
    );
    /// Idle is not complete. Transactions and unresolved attempts remain held.
    fn is_complete(&self) -> bool;
    fn local_failure(&self) -> Option<StateStoreExecutionFailure>;
    /// Abandons the transaction/undelivered opening result, unlike interrupting
    /// just one method. Existing owner progress performs rollback/settlement.
    fn request_close(&self);
    /// Nonblocking stop request. Transaction abandonment/rollback obligation
    /// is a separate original-owned fact, not implied by a method F dropping.
    fn request_interrupt(&self);
}

/// The originating author freezes its fact and raw cause together. Consumers
/// share that same identity; they do not infer classification from concrete
/// foreign types or diagnostics. Business results remain in Original.
pub type StateStoreExecutionFailure = novarocks_failure_contract::SharedFailureCause;

/// These are distinct proofs. NeverPublishedReturned requires normal return
/// from destruction of the same unpublished call and reservation; it is not a
/// worker-exit event and cannot resolve an uncertain publication as unstarted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateStoreBlockingExit {
    NeverPublishedReturned,
    ActualExited,
}

/// Borrower of a receiving record already strongly held by the original
/// role/Action, then moved unchanged into its residual. A slot must not strongly
/// point back to that owner or turn the provider original into a strong cycle.
/// No owner lock may span reserve, physical call, foreign poll/drop, or notify.
pub trait StateStoreBlockingSlot: Send {
    /// Advances only the same original request. Full waits on the original
    /// executor queue with the captured stop/deadline; it is not a new quota.
    /// Success means the physical result is stable in the original receiver,
    /// not that the worker's call/result/destructor has actually exited.
    fn poll_phase(
        &mut self,
        cx: &mut Context<'_>,
        request: StateStoreBlockingRequest,
    ) -> Poll<Result<(), StateStoreExecutionFailure>>;
    fn poll_exit(
        &mut self,
        cx: &mut Context<'_>,
        request: StateStoreBlockingRequest,
    ) -> Poll<Result<StateStoreBlockingExit, StateStoreExecutionFailure>>;
}

/// Concrete ports validate selected private TypeId, origin/instance, phase,
/// and captured original authority before mutation. Opening uses only its Host
/// process owner and ordinary executor. Statement/Job ports require their same
/// positive Root/source; an Opening port is never a missing-context fallback.
pub trait StateStoreExecutionPort: Send + Sync {
    /// Before returning a borrower in target, install the strong Original plus
    /// actual physical receiving record into existing stable owner custody.
    /// Refusal preserves target and original. This method performs no I/O or
    /// physical publication. Deadline is the captured original absolute value;
    /// cancellation comes from that same port's original owner, never a new F.
    fn bind_original_in(
        &self,
        original: &Arc<dyn StateStoreBlockingOriginal>,
        deadline: Instant,
        target: &mut Option<Box<dyn StateStoreBlockingSlot>>,
    ) -> Result<(), StateStoreExecutionFailure>;
    /// Wake the existing owner only, without dispatching or creating a task.
    /// Implementations retain a notification failure in original owner health.
    fn request_progress(&self);
}

/// A read request borrows the same installed execution owner. Each call
/// supplies one fixed transaction context; Get/Range never renew that context.
/// Existing absolute contexts keep their original deadline when used here.
pub trait StateStoreReadExecution: Send + Sync {
    fn read_transaction_context(
        &self,
    ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure>;
}

/// A request-local borrower for independent read transactions and write
/// operations. Implementations derive each context at its actual begin boundary;
/// they neither create a physical owner nor grant a missing write capability.
/// Host bootstrap readers implement only `StateStoreReadExecution`.
pub trait StateStoreExecutionRequests: StateStoreReadExecution {
    fn write_operation_context(
        &self,
    ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure>;
}

/// Weak borrower of an already-installed port, not a permission constructor.
/// The real role/Action must keep the port and its strong custody alive through
/// rollback/evidence/actual exit, including after this context or outer F drops.
#[derive(Clone)]
pub struct StateStoreExecutionContext {
    port: Weak<dyn StateStoreExecutionPort>,
    deadline: Instant,
}
impl StateStoreExecutionContext {
    pub fn new(port: &Arc<dyn StateStoreExecutionPort>, deadline: Instant) -> Self {
        Self {
            port: Arc::downgrade(port),
            deadline,
        }
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    /// Borrows the same owner with a deadline that can only become earlier.
    /// This neither extends business time nor supplies settlement authority.
    pub fn with_deadline(&self, deadline: Instant) -> Self {
        Self {
            port: self.port.clone(),
            deadline: self.deadline.min(deadline),
        }
    }
    /// Identity comparison of borrowers only, never a Root/source grant.
    pub fn shares_owner(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.port, &other.port)
    }
    pub fn request_progress(&self) {
        if let Some(port) = self.port.upgrade() {
            port.request_progress();
        }
    }
    pub fn bind_original_in(
        &self,
        original: &Arc<dyn StateStoreBlockingOriginal>,
        target: &mut Option<Box<dyn StateStoreBlockingSlot>>,
    ) -> Result<(), StateStoreExecutionFailure> {
        if target.is_some() {
            return Err(StateStoreError::new(
                StateStoreErrorKind::Internal,
                "state store original receiving slot is not empty",
            )
            .into_original_failure());
        }
        let port = self
            .port
            .upgrade()
            .ok_or_else(|| -> StateStoreExecutionFailure {
                StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "state store original execution owner is unavailable",
                )
                .into_original_failure()
            })?;
        port.bind_original_in(original, self.deadline, target)
    }
}

impl StateStoreReadExecution for StateStoreExecutionContext {
    fn read_transaction_context(
        &self,
    ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
        Ok(self.clone())
    }
}

impl StateStoreExecutionRequests for StateStoreExecutionContext {
    fn write_operation_context(
        &self,
    ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
        Ok(self.clone())
    }
}

/// Selected factory-authored private vocabulary. These facts confer no queue,
/// Root, provider access, or permission to accept a caller's arbitrary object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateStoreBlockingSourceVocabulary {
    provider: StateStoreProviderId,
    open: std::any::TypeId,
    transaction: std::any::TypeId,
    evidence: std::any::TypeId,
}
impl StateStoreBlockingSourceVocabulary {
    pub fn for_provider<
        O: StateStoreBlockingOriginal,
        T: StateStoreBlockingOriginal,
        E: StateStoreBlockingOriginal,
    >(
        provider: StateStoreProviderId,
    ) -> Self {
        Self {
            provider,
            open: std::any::TypeId::of::<O>(),
            transaction: std::any::TypeId::of::<T>(),
            evidence: std::any::TypeId::of::<E>(),
        }
    }
    pub fn provider(&self) -> StateStoreProviderId {
        self.provider
    }
    pub fn open_type(&self) -> std::any::TypeId {
        self.open
    }
    pub fn transaction_type(&self) -> std::any::TypeId {
        self.transaction
    }
    pub fn evidence_type(&self) -> std::any::TypeId {
        self.evidence
    }
}

/// A provider without the governed original contract explicitly refuses it.
/// It must not delegate a new *_in caller to legacy blocking/spawn methods.
#[derive(Debug)]
pub struct StateStoreOriginalUnsupported;
impl std::fmt::Display for StateStoreOriginalUnsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("state store provider does not support original execution")
    }
}
impl std::error::Error for StateStoreOriginalUnsupported {}

#[async_trait::async_trait]
pub trait OriginalReadTransaction: Send {
    async fn get_in(
        &mut self,
        key: &crate::Key,
    ) -> Result<Option<crate::StateRecord>, StateStoreExecutionFailure>;
    async fn range_in(
        &mut self,
        request: &crate::RangeRequest,
    ) -> Result<crate::RangePage, StateStoreExecutionFailure>;
    /// Borrows the same session; its original owns rollback even if F drops.
    async fn close_in(&mut self) -> Result<(), StateStoreExecutionFailure>;
}
#[async_trait::async_trait]
pub trait OriginalWriteTransaction: OriginalReadTransaction {
    fn attempt(&self) -> crate::AttemptId;
    /// Physical origin remains observable even if durable evidence resolved it.
    fn original_failure(&self) -> Option<StateStoreExecutionFailure>;
    async fn put_in(
        &mut self,
        key: crate::Key,
        value: crate::Value,
        precondition: crate::Precondition,
    ) -> Result<(), StateStoreExecutionFailure>;
    async fn delete_in(
        &mut self,
        key: crate::Key,
        precondition: crate::Precondition,
    ) -> Result<(), StateStoreExecutionFailure>;
    /// Retains the one attempt and operation result in its stable Original.
    /// Unknown does not authorize another commit.
    async fn commit_in(&mut self) -> Result<crate::CommitOutcome, StateStoreExecutionFailure>;
}

/// A borrowed opening source. The instance never becomes unique custody of a
/// disposable opening F: only a synchronous move into original role receiving
/// can consume the provider's already-stable result after actual exit.
pub trait StateStoreOriginalOpen: Send {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StateStoreExecutionFailure>>;
    fn take_instance_in(
        &mut self,
        target: &mut Option<Box<dyn crate::StateStoreProviderInstance>>,
    ) -> Result<(), StateStoreExecutionFailure>;
}

/// A borrowed close handle. Only the original Source's actual worker footers
/// prove closure; dropping this handle never releases the instance or its file.
pub trait StateStoreOriginalClose: Send {
    fn poll_closed(&mut self, cx: &mut Context<'_>)
    -> Poll<Result<(), StateStoreExecutionFailure>>;
}

impl novarocks_failure_contract::FailureProjection for StateStoreOriginalUnsupported {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Unsupported,
            FailureReason::new(domain::STATE_STORE, std::num::NonZeroU16::new(20).unwrap()),
        )
    }
}
impl StateStoreOriginalUnsupported {
    pub fn into_original_failure(self) -> StateStoreExecutionFailure {
        StateStoreExecutionFailure::from_static_detail(
            self,
            "state store provider does not support original execution",
        )
    }
}
