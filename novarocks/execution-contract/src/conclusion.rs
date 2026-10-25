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

//! One Task conclusion acceptance domain; stopping and physical exit are separate.

use crate::{TaskIdentity, TerminationDetail};
use std::{num::NonZeroU64, sync::Arc};

#[expect(
    clippy::large_enum_variant,
    reason = "This shared FE/BE result carries typed terminal detail inline; boxing would change the public contract and allocate when a failure is published."
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskConclusion {
    Succeeded,
    Terminated(TerminationDetail),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConclusionDecision {
    Accept,
    Keep,
    RefineDerived,
}

/// Called by the Task owner within its adjudication step before terminal
/// publication. An already published terminal retains its receipt; late facts
/// are diagnostics. No timestamp ordering,
/// lifecycle observer, or physical-stop escalation can replace this decision.
pub fn decide_conclusion(
    current: Option<&TaskConclusion>,
    proposed: &TaskConclusion,
) -> ConclusionDecision {
    match current {
        None => ConclusionDecision::Accept,
        Some(TaskConclusion::Terminated(existing))
            if existing.is_derived()
                && matches!(
                    proposed,
                    TaskConclusion::Terminated(TerminationDetail::Failed(_))
                ) =>
        {
            ConclusionDecision::RefineDerived
        }
        Some(_) => ConclusionDecision::Keep,
    }
}

/// A reporting owner calls this within the same adjudication step. Stop
/// observations keep every existing receipt, including a derived peer cause.
/// With no accepted cause the owner reports its own unexpected unowned-stop
/// failure, retaining the actual observation as its source. Class alone never
/// decides this: a genuine local Control timeout can be originating evidence.
pub fn decide_reported_failure(
    current: Option<&TaskConclusion>,
    proposed: &TaskConclusion,
    evidence: crate::failure::FailureEvidence,
) -> ConclusionDecision {
    assert!(
        matches!(
            proposed,
            TaskConclusion::Terminated(TerminationDetail::Failed(_))
        ),
        "reported failure requires a failure candidate"
    );
    if evidence == crate::failure::FailureEvidence::StopObservation && current.is_some() {
        ConclusionDecision::Keep
    } else {
        decide_conclusion(current, proposed)
    }
}

/// The immutable accepted fact. A refined derived cause gets a new generation;
/// it is never a second Task or a license to reopen completed work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedConclusion {
    identity: TaskIdentity,
    generation: NonZeroU64,
    conclusion: TaskConclusion,
}
impl AcceptedConclusion {
    pub const fn new(
        identity: TaskIdentity,
        generation: NonZeroU64,
        conclusion: TaskConclusion,
    ) -> Self {
        Self {
            identity,
            generation,
            conclusion,
        }
    }
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub const fn generation(&self) -> NonZeroU64 {
        self.generation
    }
    pub const fn conclusion(&self) -> &TaskConclusion {
        &self.conclusion
    }
}

/// One owned proposal. A failure always carries its actual local originating
/// cause; a projected status summary cannot stand in for that cause.
#[derive(Debug)]
pub enum TaskConclusionProposal<C> {
    Succeeded,
    Canceled(crate::CancelReason),
    Aborted(crate::AbortCause),
    Failed {
        cause: C,
        phase: crate::TaskFailurePhase,
    },
}

/// Immutable local receipt. Only `neutral` crosses a role boundary. Sharing
/// this value never clones a rich source or the bounded wire diagnostic.
#[derive(Debug)]
pub struct AcceptedTaskConclusion<C> {
    neutral: AcceptedConclusion,
    originating_failure: Option<C>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConclusionSourceError {
    MissingOriginatingFailure,
    UnexpectedOriginatingFailure,
    StopObservationIsNotOriginating,
}
impl std::fmt::Display for ConclusionSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::MissingOriginatingFailure => {
                "accepted Task failure requires its originating local cause"
            }
            Self::UnexpectedOriginatingFailure => {
                "Task success or control conclusion cannot carry an originating failure"
            }
            Self::StopObservationIsNotOriginating => {
                "a stop observation cannot be the originating accepted Task failure"
            }
        })
    }
}
impl std::error::Error for ConclusionSourceError {}
impl<C> AcceptedTaskConclusion<C> {
    pub fn new(
        neutral: AcceptedConclusion,
        originating_failure: Option<C>,
    ) -> Result<Self, ConclusionSourceError>
    where
        C: std::error::Error + crate::failure::FailureProjection + Send + Sync + 'static,
    {
        let failed = matches!(
            neutral.conclusion(),
            TaskConclusion::Terminated(TerminationDetail::Failed(_))
        );
        match (failed, originating_failure.is_some()) {
            (true, false) => return Err(ConclusionSourceError::MissingOriginatingFailure),
            (false, true) => return Err(ConclusionSourceError::UnexpectedOriginatingFailure),
            _ => {}
        }
        if originating_failure.as_ref().is_some_and(|source| {
            source.failure_fact().evidence() == crate::failure::FailureEvidence::StopObservation
        }) {
            return Err(ConclusionSourceError::StopObservationIsNotOriginating);
        }
        Ok(Self {
            neutral,
            originating_failure,
        })
    }
    pub const fn neutral(&self) -> &AcceptedConclusion {
        &self.neutral
    }
    pub const fn originating_failure(&self) -> Option<&C> {
        self.originating_failure.as_ref()
    }
}
/// A barrier continuation, called exactly once after every Task input ordered
/// before its registration has been adjudicated and published. `None` means
/// no conclusion was accepted before that point. The snapshot is not frozen:
/// later inputs may still change it through the shared refinement rule.
pub type SettledContinuation<C> =
    Box<dyn FnOnce(Option<Arc<AcceptedTaskConclusion<C>>>) + Send + 'static>;

/// Opaque, single-use authority issued with an admitted lifecycle role.
/// The issuer keeps its bounded storage and work responsibility until actual
/// callback/capture exit and every required completion consumer retires.
///
/// This is public so the application owner can implement it across crates.
/// An Execution consumer receives a permit; cloning a Task port grants no
/// authority to construct another admitted role.
pub trait SettlementCapability<C>: Send
where
    C: std::error::Error + crate::failure::FailureProjection + Send + Sync + 'static,
{
    /// Transfers the continuation into the role's existing prepaid custody.
    /// The implementation must not wait for the protocol owner to advance.
    fn register(self: Box<Self>, continuation: SettledContinuation<C>);
    /// Withdraws an unused registration authority. This is not evidence that
    /// the bound role, its captures or its other reports have actually exited.
    fn revoke_unused(self: Box<Self>);
}

/// Moving this value is the only way to register one settlement continuation.
/// It intentionally implements neither Clone nor a factory on a Task handle.
#[must_use = "An admitted role must consume or revoke its settlement authority"]
pub struct SettlementPermit<C>
where
    C: std::error::Error + crate::failure::FailureProjection + Send + Sync + 'static,
{
    identity: Option<TaskIdentity>,
    capability: Option<Box<dyn SettlementCapability<C>>>,
}
impl<C> SettlementPermit<C>
where
    C: std::error::Error + crate::failure::FailureProjection + Send + Sync + 'static,
{
    /// Called only by the role admission owner after reserving its complete
    /// storage/work vector. This factory creates no capacity on its own.
    pub fn from_admitted_role(
        identity: TaskIdentity,
        capability: Box<dyn SettlementCapability<C>>,
    ) -> Self {
        Self {
            identity: Some(identity),
            capability: Some(capability),
        }
    }
    /// Taskless execution can have an admitted local finisher without
    /// manufacturing a distributed Task identity or depending on Worker.
    pub fn from_admitted_local_role(capability: Box<dyn SettlementCapability<C>>) -> Self {
        Self {
            identity: None,
            capability: Some(capability),
        }
    }
    pub const fn identity(&self) -> Option<TaskIdentity> {
        self.identity
    }
    /// A successful handoff consumes the unique authority before invoking the
    /// issuer. An unwind cannot turn it back into an unused reservation.
    pub fn register(mut self, continuation: SettledContinuation<C>) {
        self.capability
            .take()
            .expect("one settlement authority")
            .register(continuation);
    }
}
impl<C> Drop for SettlementPermit<C>
where
    C: std::error::Error + crate::failure::FailureProjection + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if let Some(capability) = self.capability.take() {
            capability.revoke_unused();
        }
    }
}

/// Bound to one exact Task from Accepted through preparation and execution.
///
/// `submit` hands off an owned proposal without returning a verdict or waiting
/// for another thread to advance. Each handed-off proposal is adjudicated once,
/// eventually when its owner keeps running. All Task inputs share an order
/// consistent with happens-before, including physical exit and settlement.
/// A handoff may run short nonblocking observers inline; callers must not hold
/// locks needed by observers or rely on notification having run on return.
///
/// Failures are projected before handoff. The shared adjudication rules retain
/// the first cause, allowing only an originating failure to refine a live peer
/// placeholder. Success requires actual stop and completed output obligations;
/// terminal conclusions cannot be refined. Stop observations cannot refine an
/// existing conclusion; an unowned stop is an unexpected owner failure with
/// the actual observation as its source.
///
/// Publication installs the immutable snapshot before setting the monotonic
/// stop signal, then notifies outside owner locks and publication fences.
/// Handoff does not promise immediate visibility. Reads may lag proposals;
/// irreversible decisions read only inside an owner adjudication step or after
/// a settled continuation begins. Settlement is a barrier, not a result freeze.
/// Owner-driven actions complete termination and wake stopped work.
///
/// Rich sources belong to the owner; losing sources are destroyed outside its
/// locks and publication fences. Unwind before handoff means no transfer;
/// unwind after it cannot revoke transfer. A caller unable to distinguish these
/// retains its original work and reports unresolved local exit.
///
/// If an owner fails, published conclusions remain. Tasks without one first
/// publish an explicit owner failure, then stop. Unadjudicated proposals are
/// discarded and each registered continuation runs once with the published
/// snapshot, including during shutdown or after Task termination.
///
/// `C` is a local domain-projected cause, never a generated DTO or a String.
// Design: ADR-0193 (docs/adr/ADR-0193-task-conclusion-proposals-and-settled-points.md)
pub trait TaskConclusionPort<C>: Send + Sync
where
    C: std::error::Error + crate::failure::FailureProjection + Send + Sync + 'static,
{
    fn identity(&self) -> TaskIdentity;
    /// Transfers one proposal; its published effect may become visible later.
    fn submit(&self, proposed: TaskConclusionProposal<C>);
    fn accepted(&self) -> Option<Arc<AcceptedTaskConclusion<C>>>;
    /// Registers a nonblocking barrier continuation without waiting for it.
    fn on_settled(&self, continuation: SettledContinuation<C>);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AbortCause, CancelReason, SafeDetail, TaskFailure};
    fn failed() -> TaskConclusion {
        TaskConclusion::Terminated(TerminationDetail::Failed(TaskFailure::new(
            crate::failure::FailureFact::new(
                crate::failure::FailureClass::Data,
                crate::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(1).unwrap(),
                ),
            ),
            SafeDetail::truncating("originating failure"),
        )))
    }
    #[test]
    fn stop_observations_keep_existing_receipts_without_blocking_real_derived_refinement() {
        use crate::failure::FailureEvidence;
        let peer =
            TaskConclusion::Terminated(TerminationDetail::Aborted(AbortCause::PeerTaskFailed));
        let primary = failed();
        for current in [
            TaskConclusion::Succeeded,
            primary.clone(),
            peer.clone(),
            TaskConclusion::Terminated(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
        ] {
            assert_eq!(
                decide_reported_failure(Some(&current), &primary, FailureEvidence::StopObservation),
                ConclusionDecision::Keep
            );
        }
        assert_eq!(
            decide_reported_failure(Some(&peer), &primary, FailureEvidence::Originating),
            ConclusionDecision::RefineDerived
        );
        // The reporting owner authors an unexpected unowned-stop candidate
        // before accepting it; the original observation is still its source.
        assert_eq!(
            decide_reported_failure(None, &primary, FailureEvidence::StopObservation),
            ConclusionDecision::Accept
        );
    }

    #[test]
    fn accepted_cancellation_cannot_be_replaced_by_failure_abort_or_success() {
        let current = TaskConclusion::Terminated(TerminationDetail::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        for later in [
            failed(),
            TaskConclusion::Succeeded,
            TaskConclusion::Terminated(TerminationDetail::Aborted(AbortCause::LeaseExpired)),
        ] {
            assert_eq!(
                decide_conclusion(Some(&current), &later),
                ConclusionDecision::Keep
            );
        }
    }
    #[test]
    fn only_an_originating_failure_refines_the_derived_placeholder() {
        let derived =
            TaskConclusion::Terminated(TerminationDetail::Aborted(AbortCause::PeerTaskFailed));
        assert_eq!(
            decide_conclusion(Some(&derived), &failed()),
            ConclusionDecision::RefineDerived
        );
        assert_eq!(
            decide_conclusion(Some(&failed()), &derived),
            ConclusionDecision::Keep
        );
        assert_eq!(
            decide_conclusion(Some(&derived), &TaskConclusion::Succeeded),
            ConclusionDecision::Keep
        );
        assert_eq!(
            decide_conclusion(None, &failed()),
            ConclusionDecision::Accept
        );
    }
    #[test]
    fn successful_seal_keeps_late_cleanup_as_diagnostics() {
        assert_eq!(
            decide_conclusion(Some(&TaskConclusion::Succeeded), &failed()),
            ConclusionDecision::Keep
        );
    }
}

#[cfg(test)]
mod source_ownership_tests {
    use super::*;
    use crate::{SafeDetail, TaskFailure};
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Debug)]
    struct Origin(Arc<AtomicUsize>);
    impl std::fmt::Display for Origin {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("originating local failure")
        }
    }
    impl std::error::Error for Origin {}
    impl crate::failure::FailureProjection for Origin {
        fn failure_fact(&self) -> crate::failure::FailureFact {
            crate::failure::FailureFact::new(
                crate::failure::FailureClass::Data,
                crate::failure::FailureReason::new(
                    crate::failure::domain::EXECUTION,
                    std::num::NonZeroU16::new(65002).unwrap(),
                ),
            )
        }
    }
    impl Drop for Origin {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    fn receipt(conclusion: TaskConclusion) -> AcceptedConclusion {
        AcceptedConclusion::new(
            TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(3701, 3702), AttemptId::new(1).unwrap())
                    .unwrap(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            ),
            NonZeroU64::new(1).unwrap(),
            conclusion,
        )
    }
    fn failed() -> TaskConclusion {
        TaskConclusion::Terminated(TerminationDetail::Failed(TaskFailure::new(
            crate::failure::FailureFact::new(
                crate::failure::FailureClass::Data,
                crate::failure::FailureReason::new(
                    crate::failure::domain::EXECUTION,
                    std::num::NonZeroU16::new(65002).unwrap(),
                ),
            ),
            SafeDetail::truncating("originating local failure"),
        )))
    }
    struct CapabilityProbe {
        registered: Arc<AtomicUsize>,
        revoked: Arc<AtomicUsize>,
    }
    impl SettlementCapability<Origin> for CapabilityProbe {
        fn register(self: Box<Self>, continuation: SettledContinuation<Origin>) {
            self.registered.fetch_add(1, Ordering::AcqRel);
            continuation(None);
        }
        fn revoke_unused(self: Box<Self>) {
            self.revoked.fetch_add(1, Ordering::AcqRel);
        }
    }
    #[test]
    fn settlement_permit_handoff_consumes_authority_without_unused_revocation() {
        let registered = Arc::new(AtomicUsize::new(0));
        let revoked = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let permit = SettlementPermit::from_admitted_role(
            receipt(TaskConclusion::Succeeded).identity(),
            Box::new(CapabilityProbe {
                registered: registered.clone(),
                revoked: revoked.clone(),
            }),
        );
        let called = calls.clone();
        permit.register(Box::new(move |snapshot| {
            assert!(snapshot.is_none());
            called.fetch_add(1, Ordering::AcqRel);
        }));
        assert_eq!(registered.load(Ordering::Acquire), 1);
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(revoked.load(Ordering::Acquire), 0);
    }
    #[test]
    fn unused_settlement_permit_revokes_registration_authority_once() {
        let registered = Arc::new(AtomicUsize::new(0));
        let revoked = Arc::new(AtomicUsize::new(0));
        let permit = SettlementPermit::from_admitted_role(
            receipt(TaskConclusion::Succeeded).identity(),
            Box::new(CapabilityProbe {
                registered: registered.clone(),
                revoked: revoked.clone(),
            }),
        );
        drop(permit);
        assert_eq!(registered.load(Ordering::Acquire), 0);
        assert_eq!(revoked.load(Ordering::Acquire), 1);
    }
    #[test]
    fn local_settlement_authority_never_manufactures_a_task_identity() {
        let registered = Arc::new(AtomicUsize::new(0));
        let revoked = Arc::new(AtomicUsize::new(0));
        let permit = SettlementPermit::from_admitted_local_role(Box::new(CapabilityProbe {
            registered: registered.clone(),
            revoked: revoked.clone(),
        }));
        assert!(permit.identity().is_none());
        permit.register(Box::new(|snapshot| assert!(snapshot.is_none())));
        assert_eq!(registered.load(Ordering::Acquire), 1);
        assert_eq!(revoked.load(Ordering::Acquire), 0);
    }
    #[test]
    fn receipt_aliases_retain_one_nonclone_origin_until_the_last_reader_drops() {
        let drops = Arc::new(AtomicUsize::new(0));
        let original =
            AcceptedTaskConclusion::new(receipt(failed()), Some(Origin(Arc::clone(&drops))))
                .unwrap();
        let accepted = Arc::new(original);
        let alias = accepted.clone();
        assert!(std::ptr::eq(
            accepted.originating_failure().unwrap(),
            alias.originating_failure().unwrap()
        ));
        assert!(std::ptr::eq(accepted.neutral(), alias.neutral()));
        drop(accepted);
        assert_eq!(drops.load(Ordering::Acquire), 0);
        drop(alias);
        assert_eq!(drops.load(Ordering::Acquire), 1);
    }
    #[derive(Debug)]
    struct ObservedStop;
    impl std::fmt::Display for ObservedStop {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("observed stop")
        }
    }
    impl std::error::Error for ObservedStop {}
    impl crate::failure::FailureProjection for ObservedStop {
        fn failure_fact(&self) -> crate::failure::FailureFact {
            crate::failure::FailureFact::stop_observation(crate::failure::FailureReason::new(
                crate::failure::domain::EXECUTION,
                std::num::NonZeroU16::new(65003).unwrap(),
            ))
        }
    }
    #[test]
    fn an_immutable_failure_receipt_rejects_observations_as_primary_sources() {
        assert_eq!(
            AcceptedTaskConclusion::new(receipt(failed()), Some(ObservedStop)).unwrap_err(),
            ConclusionSourceError::StopObservationIsNotOriginating
        );
    }

    #[test]
    fn a_projected_failure_summary_cannot_replace_the_original_local_cause() {
        assert_eq!(
            AcceptedTaskConclusion::<Origin>::new(receipt(failed()), None).unwrap_err(),
            ConclusionSourceError::MissingOriginatingFailure
        );
        for control in [
            TaskConclusion::Succeeded,
            TaskConclusion::Terminated(TerminationDetail::Canceled(
                crate::CancelReason::UpstreamNoLongerNeeded,
            )),
            TaskConclusion::Terminated(TerminationDetail::Aborted(
                crate::AbortCause::PeerTaskFailed,
            )),
        ] {
            let drops = Arc::new(AtomicUsize::new(0));
            let error = AcceptedTaskConclusion::new(
                receipt(control.clone()),
                Some(Origin(Arc::clone(&drops))),
            )
            .unwrap_err();
            assert_eq!(error, ConclusionSourceError::UnexpectedOriginatingFailure);
            assert_eq!(drops.load(Ordering::Acquire), 1);
            assert!(AcceptedTaskConclusion::<Origin>::new(receipt(control), None).is_ok());
        }
    }
}
