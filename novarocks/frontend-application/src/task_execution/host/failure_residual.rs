// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original failure-work handoff and bounded local-custody verification.
//! Remote convergence and recovery decisions remain in the logical core.

use super::{
    auxiliary::AttemptRawFailurePublisher,
    failure_supervisor::FailureCleanupRequest,
    transport::{FailureAbortAttempt, FailureResidualPort},
};
use crate::control_runtime::membership::FrozenMember;
use novarocks_control_runtime::actor_turn::ActorRegistration;
use novarocks_coordination_contract::AttemptActivationIdentity;
use std::{
    fmt,
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU8, Ordering},
    },
    task::Waker,
};

pub(crate) struct OriginalFailureResidual {
    pub(crate) request: FailureCleanupRequest,
    pub(crate) attempts: Vec<FailureAbortAttempt>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResidualVerdict {
    LocallyProven,
    TypedRefusal,
    UnresolvedLocalCustody,
}
/// Same original raw objects, retained by the existing passive health observer.
/// ManuallyDrop prevents an observer's last alias from destroying opaque causes
/// on a poller. It allocates no Scope or Work and grants no exit authority.
/// Unproven original capabilities remain here; settled rich remainders still
/// require explicit controlled disposal before their Source can report exit.
#[repr(u8)]
enum ResidualDisposal {
    Retained,
    Issued,
    BodyDropped,
    Retired,
}
struct SelectedResidualFailures {
    // These exact causes remain in custody after selection; retention itself
    // is not another failure decision or an actual-exit receipt.
    _publication: Option<Arc<super::failure_publication::FailurePublicationError>>,
    _root_reply: Option<Arc<super::publication::PublicationError>>,
}
pub(crate) struct OriginalFailureResidualRecord {
    original: Mutex<Option<ManuallyDrop<OriginalFailureResidual>>>,
    verdict: OnceLock<ResidualVerdict>,
    selected: OnceLock<SelectedResidualFailures>,
    disposal: AtomicU8,
}
impl OriginalFailureResidualRecord {
    pub(crate) fn verdict(&self) -> Option<ResidualVerdict> {
        self.verdict.get().copied()
    }
    #[cfg(test)]
    pub(crate) fn original_publication_refusal(
        &self,
    ) -> Option<&Arc<super::failure_publication::FailurePublicationError>> {
        self.selected.get()?._publication.as_ref()
    }
    #[cfg(test)]
    pub(crate) fn original_root_reply_refusal(
        &self,
    ) -> Option<&Arc<super::publication::PublicationError>> {
        self.selected.get()?._root_reply.as_ref()
    }
    /// Called by the SAME credentialed blocking disposal body. A last observer
    /// alias never executes this opaque destructor, and panic never marks it done.
    pub(in crate::task_execution::host) fn dispose_on_executor(&self) {
        assert_eq!(
            self.disposal.load(Ordering::Acquire),
            ResidualDisposal::Issued as u8
        );
        let original = self
            .original
            .lock()
            .expect("original residual disposal")
            .take()
            .expect("same original residual is consumed once");
        drop(ManuallyDrop::into_inner(original));
        self.disposal
            .store(ResidualDisposal::BodyDropped as u8, Ordering::Release);
    }
    /// The original TransportReleases owner calls this only after its actual
    /// HostDisposal Join/report and original header reaping both succeeded.
    pub(in crate::task_execution::host) fn disposal_retired(&self) {
        assert!(
            self.disposal
                .compare_exchange(
                    ResidualDisposal::BodyDropped as u8,
                    ResidualDisposal::Retired as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok(),
            "same original residual disposal actually retired once"
        );
    }
    fn is_retired(&self) -> bool {
        self.disposal.load(Ordering::Acquire) == ResidualDisposal::Retired as u8
    }
    #[cfg(test)]
    pub(in crate::task_execution::host) fn disposal_body_returned(&self) -> bool {
        self.disposal.load(Ordering::Acquire) >= ResidualDisposal::BodyDropped as u8
    }
}
impl fmt::Debug for OriginalFailureResidualRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OriginalFailureResidualRecord")
            .field("verdict", &self.verdict())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for OriginalFailureResidualRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original attempt failure residual and raw causes retained")
    }
}
impl std::error::Error for OriginalFailureResidualRecord {}

enum Handoff {
    Awaiting,
    Ready(ManuallyDrop<OriginalFailureResidual>),
    Taken,
    Unused,
}
struct ResidualSlot {
    state: Mutex<Handoff>,
    wake: Waker,
}
struct ResidualSink(Weak<ResidualSlot>);
struct RefusedResidual {
    _original: ManuallyDrop<OriginalFailureResidual>,
}
impl FailureResidualPort for ResidualSink {
    fn finish(
        self: Box<Self>,
        request: FailureCleanupRequest,
        attempts: Vec<FailureAbortAttempt>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            let original = ManuallyDrop::new(OriginalFailureResidual { request, attempts });
            let Some(slot) = self.0.upgrade() else {
                std::panic::resume_unwind(Box::new(RefusedResidual {
                    _original: original,
                }));
            };
            let mut state = slot
                .state
                .lock()
                .expect("original failure residual handoff");
            if !matches!(*state, Handoff::Awaiting) {
                drop(state);
                std::panic::resume_unwind(Box::new(RefusedResidual {
                    _original: original,
                }));
            }
            *state = Handoff::Ready(original);
            drop(state);
            slot.wake.wake_by_ref();
        })
    }
}

/// Receiving state for the original supervisor's one actual residual. Explicit
/// progress checks its local custody; Drop never certifies resource or host exit.
#[must_use = "Retain the original residual until its actual supervisor and local custody settle"]
pub(crate) struct FailureResidualReceiveObligation {
    registration: ActorRegistration,
    activation: AttemptActivationIdentity,
    members: Arc<[FrozenMember]>,
    slot: Arc<ResidualSlot>,
    raw: AttemptRawFailurePublisher,
    record: Arc<OriginalFailureResidualRecord>,
    received: bool,
    published: bool,
    cursor: usize,
    typed_refusal: bool,
    unresolved: bool,
    unused: bool,
    refusal: Option<ResidualReceiveError>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResidualReceiveError {
    MissingOriginal,
    RawObserverUnavailable,
}
pub(crate) struct ResidualProgress {
    pub(crate) units: usize,
    pub(crate) more: bool,
    pub(crate) error: Option<ResidualReceiveError>,
}
impl FailureResidualReceiveObligation {
    pub(crate) fn new(
        registration: ActorRegistration,
        activation: AttemptActivationIdentity,
        members: Arc<[FrozenMember]>,
        wake: Waker,
        raw: AttemptRawFailurePublisher,
    ) -> (Self, Box<dyn FailureResidualPort>) {
        assert!(
            raw.matches(&registration),
            "same original infrastructure raw record"
        );
        assert!(
            members
                .iter()
                .all(|m| m.participation.query_execution_id() == activation.execution())
        );
        assert!(
            members
                .windows(2)
                .all(|p| p[0].participation < p[1].participation)
        );
        let slot = Arc::new(ResidualSlot {
            state: Mutex::new(Handoff::Awaiting),
            wake,
        });
        let sink = Box::new(ResidualSink(Arc::downgrade(&slot)));
        (
            Self {
                registration,
                activation,
                members,
                slot,
                raw,
                record: Arc::new(OriginalFailureResidualRecord {
                    original: Mutex::new(None),
                    verdict: OnceLock::new(),
                    selected: OnceLock::new(),
                    disposal: AtomicU8::new(ResidualDisposal::Retained as u8),
                }),
                received: false,
                published: false,
                cursor: 0,
                typed_refusal: false,
                unresolved: false,
                unused: false,
                refusal: None,
            },
            sink,
        )
    }
    pub(crate) fn matches(
        &self,
        registration: &ActorRegistration,
        activation: AttemptActivationIdentity,
    ) -> bool {
        self.registration.same_registration(registration) && self.activation == activation
    }
    pub(crate) fn record(&self) -> Option<&Arc<OriginalFailureResidualRecord>> {
        self.received.then_some(&self.record)
    }
    /// supervisor_exited is present only after the SAME supervisor's actual
    /// Join, result/report, input retirement and header reap. Its bool records
    /// whether that original AP actually failed, never a guessed terminal.
    pub(crate) fn progress(
        &mut self,
        budget: usize,
        supervisor_exited: Option<bool>,
    ) -> ResidualProgress {
        let mut out = ResidualProgress {
            units: 0,
            more: false,
            error: self.refusal,
        };
        if self.refusal.is_some() || self.unused || self.received && self.record.verdict().is_some()
        {
            return out;
        }
        if budget == 0 {
            out.more = true;
            return out;
        }
        if !self.received {
            out.units += 1;
            let mut state = self
                .slot
                .state
                .lock()
                .expect("original failure residual handoff");
            match &*state {
                Handoff::Awaiting if supervisor_exited == Some(false) => {
                    *state = Handoff::Unused;
                    self.unused = true;
                    return out;
                }
                Handoff::Awaiting if supervisor_exited == Some(true) => {
                    self.refusal = Some(ResidualReceiveError::MissingOriginal);
                    out.error = self.refusal;
                    return out;
                }
                Handoff::Awaiting => return out,
                Handoff::Ready(_) => {}
                Handoff::Taken | Handoff::Unused => unreachable!("same one-shot residual receiver"),
            }
            let Handoff::Ready(original) = std::mem::replace(&mut *state, Handoff::Taken) else {
                unreachable!()
            };
            // The raw payload moves once; no opaque destructor or notification
            // occurs while the handoff metadata mutex is held.
            drop(state);
            let mut destination = self
                .record
                .original
                .lock()
                .expect("original raw receiving record");
            assert!(destination.is_none(), "one original residual payload");
            *destination = Some(original);
            drop(destination);
            self.received = true;
        }
        let record = &self.record;
        if !self.published {
            if out.units == budget {
                out.more = true;
                return out;
            }
            out.units += 1;
            if self.raw.publish(record.clone()).is_err() {
                self.refusal = Some(ResidualReceiveError::RawObserverUnavailable);
                out.error = self.refusal;
                return out;
            }
            self.published = true;
            let held = record
                .original
                .lock()
                .expect("original residual local facts");
            let original = held.as_ref().expect("same retained raw original");
            assert!(
                record
                    .selected
                    .set(SelectedResidualFailures {
                        _publication: original.request.publication.as_ref().err().cloned(),
                        _root_reply: original
                            .request
                            .root_reply
                            .as_ref()
                            .and_then(|r| r.as_ref().err())
                            .cloned(),
                    })
                    .is_ok(),
                "one original typed residual selection"
            );
            self.typed_refusal = original.request.publication.is_err()
                || original
                    .request
                    .root_reply
                    .as_ref()
                    .is_some_and(Result::is_err)
                || original.request.unknown_participations.len() != original.attempts.len();
            // This is the original queued/tail disposal proof, not a business
            // rejection. Its Err never certifies that those local captures exited.
            self.unresolved = original.request.observation_drain.is_err();
        }
        let held = record
            .original
            .lock()
            .expect("original residual local facts");
        let original = held.as_ref().expect("same retained raw original");
        let exact = &original.request.unknown_participations;
        let count = exact.len().max(original.attempts.len());
        while self.cursor < count && out.units < budget {
            let index = self.cursor;
            self.cursor += 1;
            out.units += 1;
            let participation = exact.get(index).copied();
            let attempt = original.attempts.get(index);
            self.typed_refusal |= participation.is_none()
                || participation.is_some_and(|p| {
                    (index > 0 && exact[index - 1] >= p)
                        || self
                            .members
                            .binary_search_by_key(&p, |m| m.participation)
                            .is_err()
                })
                || attempt.is_none()
                || attempt.is_some_and(|a| Some(a.request.participation()) != participation);
            // A refusal or unknown remote verdict is not by itself evidence
            // that this original transport still has live local work.
            self.unresolved |= attempt
                .is_some_and(|a| a.result.as_ref().is_err_and(|error| error.exit_unproven()));
        }
        drop(held);
        if self.cursor == count && supervisor_exited.is_some() {
            if out.units == 0 {
                out.units = 1;
            }
            self.typed_refusal |= supervisor_exited == Some(false);
            let verdict = if self.unresolved {
                ResidualVerdict::UnresolvedLocalCustody
            } else if self.typed_refusal {
                ResidualVerdict::TypedRefusal
            } else {
                ResidualVerdict::LocallyProven
            };
            assert!(
                record.verdict.set(verdict).is_ok(),
                "one original local residual verdict"
            );
            if !self.raw.verdict_committed(record) {
                self.refusal = Some(ResidualReceiveError::RawObserverUnavailable);
                out.error = self.refusal;
            }
        } else {
            // Actual supervisor exit supplies the later wake. Do not spin on
            // an empty receiver or on a still-running original Join.
            out.more = self.cursor < count;
        }
        out
    }
    pub(crate) fn ready_for_disposal(&self) -> bool {
        self.unused
            || self.refusal.is_none()
                && self.published
                && self.received
                && matches!(
                    self.record.verdict(),
                    Some(ResidualVerdict::LocallyProven | ResidualVerdict::TypedRefusal)
                )
    }
    pub(crate) fn needs_disposal(&self) -> bool {
        !self.unused
            && self.ready_for_disposal()
            && self.record.disposal.load(Ordering::Acquire) == ResidualDisposal::Retained as u8
    }

    pub(in crate::task_execution::host) fn backing_bytes()
    -> Result<usize, super::observations::ObservationError> {
        use super::observations::backing;
        backing::sum([
            std::mem::size_of::<Self>(),
            AttemptRawFailurePublisher::backing_bytes()?,
            backing::arc::<ResidualSlot>()?.size(),
            backing::arc::<OriginalFailureResidualRecord>()?.size(),
            std::alloc::Layout::new::<ResidualSink>().size(),
        ])
    }

    pub(crate) fn take_disposal(&self) -> Option<Arc<OriginalFailureResidualRecord>> {
        if self.unused || !self.ready_for_disposal() {
            return None;
        }
        self.record
            .disposal
            .compare_exchange(
                ResidualDisposal::Retained as u8,
                ResidualDisposal::Issued as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .ok()
            .map(|_| self.record.clone())
    }
    pub(crate) fn is_retired(&self) -> bool {
        self.unused || self.received && self.record.is_retired()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_runtime::host::EffectSource;
    use crate::task_execution::host::{
        auxiliary::{AttemptAuxiliarySource, AttemptInfrastructureHealth},
        port::requests::test_support::{Fixture, activation},
        transport::SingleSendError,
    };
    use futures::FutureExt;
    use novarocks_control_runtime_core::{identity::Tick, mailbox::InputClass};
    use novarocks_coordination_contract::{IdSeed, OperationIds};
    use novarocks_execution_contract::{
        AbortCause, AbortParticipation, ParticipationRef, RuntimeEndpoint,
    };
    use novarocks_types::{BackendProcessId, FrontendProcessId};
    use std::{
        panic::AssertUnwindSafe,
        sync::atomic::{AtomicUsize, Ordering},
        task::Wake,
    };

    #[derive(Default)]
    struct WakeProbe(AtomicUsize);
    impl Wake for WakeProbe {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct RawProbe(Arc<AtomicUsize>);
    impl Drop for RawProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn request(drops: &Arc<AtomicUsize>) -> (FailureCleanupRequest, usize) {
        let original = Box::new(RawProbe(drops.clone()));
        let address = original.as_ref() as *const RawProbe as usize;
        (
            FailureCleanupRequest {
                core_panic: original,
                publication: Ok(()),
                root_reply: None,
                observation_drain: Ok(()),
                unknown_participations: Box::new([]),
            },
            address,
        )
    }
    fn auxiliary(f: &Fixture) -> (AttemptAuxiliarySource, AttemptInfrastructureHealth) {
        let input = f
            .runtime
            .reserve_admitted_input_for(
                &f.attempt.registration,
                f.attempt.registration.actor(),
                InputClass::Lifecycle,
                &f.attempt.work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap_or_else(|_| panic!("original infrastructure role"));
        AttemptAuxiliarySource::new(f.attempt.registration.clone(), input)
    }
    fn close_auxiliary(f: &Fixture, source: &mut AttemptAuxiliarySource) {
        source.close_external();
        source.transport_roles_drained();
        f.runtime
            .notify_host_actor(&f.attempt.registration)
            .unwrap();
        loop {
            let turn = f.runtime.claim_host_actor().expect("original host wake");
            if turn.matches_registration(&f.attempt.registration) {
                assert!(
                    source
                        .progress_retirement(&f.runtime, &turn, Tick(1), 1)
                        .retired
                );
                turn.release(false).unwrap();
                break;
            }
            turn.release(false).unwrap();
        }
    }
    fn dispose_record(record: &OriginalFailureResidualRecord) {
        let original = record.original.lock().unwrap().take().unwrap();
        // The test owns these known passive probes; destruction is explicit
        // and outside locks, after its original local roles retired.
        drop(ManuallyDrop::into_inner(original));
    }
    #[tokio::test]
    async fn raw_core_cause_keeps_exact_allocation_without_blocking_original_role_exit() {
        let f = Fixture::new();
        let (mut auxiliary, health) = auxiliary(&f);
        let wake = Arc::new(WakeProbe::default());
        let drops = Arc::new(AtomicUsize::new(0));
        let (mut receiver, sink) = FailureResidualReceiveObligation::new(
            f.attempt.registration.clone(),
            activation(),
            Arc::from([]),
            Waker::from(wake.clone()),
            auxiliary.raw_failure_publisher(),
        );
        let (original, address) = request(&drops);
        sink.finish(original, Vec::new()).await;
        assert_eq!(wake.0.load(Ordering::SeqCst), 1);
        assert!(receiver.matches(&f.attempt.registration, activation()));
        assert!(!receiver.matches(&f.logical.registration, activation()));
        let first = receiver.progress(1, None);
        assert_eq!(first.units, 1);
        assert!(first.more && health.raw_record().is_none());
        let second = receiver.progress(1, None);
        assert_eq!(second.units, 1);
        assert!(!second.more && !receiver.ready_for_disposal());
        let record = health.raw_record().unwrap();
        assert!(Arc::ptr_eq(receiver.record().unwrap(), &record));
        assert!(
            record.verdict().is_none(),
            "actual supervisor exit is independent"
        );
        {
            let held = record.original.lock().unwrap();
            let raw = held
                .as_ref()
                .unwrap()
                .request
                .core_panic
                .downcast_ref::<RawProbe>()
                .unwrap();
            assert_eq!(raw as *const RawProbe as usize, address);
        }

        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let mut observer = health.clone();
        let mut exit_failure = Box::pin(observer.exit_failed());
        assert!(futures::poll!(exit_failure.as_mut()).is_pending());
        assert!(receiver.progress(1, Some(true)).error.is_none());
        assert_eq!(record.verdict(), Some(ResidualVerdict::LocallyProven));
        assert!(
            futures::poll!(exit_failure.as_mut()).is_pending(),
            "a proven raw cause is not unresolved exit health"
        );
        drop(exit_failure);
        assert!(receiver.ready_for_disposal() && health.current_unresolved().is_none());
        assert!(
            !receiver.is_retired(),
            "verification is not actual rich disposal"
        );
        assert!(health.exit_failure().is_none());
        assert!(
            !auxiliary.result_admission_closed(),
            "raw cause is not unresolved transport custody"
        );
        close_auxiliary(&f, &mut auxiliary);
        dispose_record(&record);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop((receiver, auxiliary, health, record));
        f.finish();
    }
    #[tokio::test]
    async fn lost_weak_receiver_preserves_the_entire_original_in_failure_work_payload() {
        let f = Fixture::new();
        let (mut auxiliary, health) = auxiliary(&f);
        let drops = Arc::new(AtomicUsize::new(0));
        let (receiver, sink) = FailureResidualReceiveObligation::new(
            f.attempt.registration.clone(),
            activation(),
            Arc::from([]),
            Waker::noop().clone(),
            auxiliary.raw_failure_publisher(),
        );
        drop(receiver);
        let (original, address) = request(&drops);
        let refused = AssertUnwindSafe(sink.finish(original, Vec::new()))
            .catch_unwind()
            .await
            .unwrap_err();
        let refused = refused
            .downcast::<RefusedResidual>()
            .unwrap_or_else(|_| panic!("original residual refusal"));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(health.raw_record().is_none());
        let raw = refused
            ._original
            .request
            .core_panic
            .downcast_ref::<RawProbe>()
            .unwrap();
        assert_eq!(raw as *const RawProbe as usize, address);
        let RefusedResidual { _original } = *refused;
        drop(ManuallyDrop::into_inner(_original));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        close_auxiliary(&f, &mut auxiliary);
        drop((auxiliary, health));
        f.finish();
    }
    #[tokio::test]
    async fn bounded_same_residual_keeps_unknown_remote_verdict_and_typed_refusal_separate() {
        let f = Fixture::new();
        let (mut auxiliary, health) = auxiliary(&f);
        let drops = Arc::new(AtomicUsize::new(0));
        let mut members: Vec<_> = (1..=3)
            .map(|n| {
                let operation = OperationIds::new(IdSeed::new(n, 27)).issue();
                FrozenMember {
                    participation: ParticipationRef::new(
                        activation().execution(),
                        FrontendProcessId::try_from_bytes(
                            OperationIds::new(IdSeed::new(91, 27)).issue().to_bytes(),
                        )
                        .unwrap(),
                        BackendProcessId::try_from_bytes(operation.to_bytes()).unwrap(),
                    ),
                    endpoint: RuntimeEndpoint::new("127.0.0.1", 1234).unwrap(),
                }
            })
            .collect();
        members.sort_by_key(|m| m.participation);
        let (mut receiver, sink) = FailureResidualReceiveObligation::new(
            f.attempt.registration.clone(),
            activation(),
            members.clone().into(),
            Waker::noop().clone(),
            auxiliary.raw_failure_publisher(),
        );
        let (mut original, _) = request(&drops);
        original.unknown_participations = members.iter().map(|m| m.participation).collect();
        let publication_refusal = Arc::new(
            super::super::failure_publication::FailurePublicationError::Runtime(
                novarocks_control_runtime::actor_turn::ActorTurnError::Closed,
            ),
        );
        original.publication = Err(publication_refusal.clone());
        let attempts = members
            .iter()
            .enumerate()
            .map(|(n, member)| FailureAbortAttempt {
                request: AbortParticipation::new(
                    OperationIds::new(IdSeed::new(n as u128 + 100, 27)).issue(),
                    member.participation,
                    AbortCause::QueryFailed,
                ),
                result: Err(SingleSendError::WrongOwner),
            })
            .collect();
        sink.finish(original, attempts).await;
        assert_eq!(receiver.progress(1, None).units, 1); // Same payload received.
        assert_eq!(receiver.progress(1, None).units, 1); // Same raw record published.
        let record = health.raw_record().unwrap();
        for index in 1..=3 {
            let progress = receiver.progress(1, None);
            assert_eq!(progress.units, 1);
            assert_eq!(receiver.cursor, index);
            assert!(record.verdict().is_none() && !receiver.ready_for_disposal());
        }
        // The original infrastructure sender is closed, but the SAME raw
        // record still has a pending verdict. A closed watch must neither
        // spin nor swallow this later raw notification.
        close_auxiliary(&f, &mut auxiliary);
        drop(auxiliary);
        let mut observer = health.clone();
        let mut exit_failure = Box::pin(observer.exit_failed());
        assert!(futures::poll!(exit_failure.as_mut()).is_pending());
        assert!(receiver.progress(1, Some(true)).error.is_none());
        assert_eq!(record.verdict(), Some(ResidualVerdict::TypedRefusal));
        let std::task::Poll::Ready(notified_failure) = futures::poll!(exit_failure.as_mut()) else {
            panic!("original typed verdict wakes its existing observer");
        };
        assert!(std::ptr::eq(
            notified_failure
                .cause()
                .downcast_ref::<OriginalFailureResidualRecord>()
                .unwrap(),
            record.as_ref()
        ));
        drop(exit_failure);
        let failure = health.exit_failure().unwrap();
        assert!(std::ptr::eq(
            failure
                .cause()
                .downcast_ref::<OriginalFailureResidualRecord>()
                .unwrap(),
            record.as_ref(),
        ));
        assert!(receiver.ready_for_disposal() && health.current_unresolved().is_none());
        let held = record.original.lock().unwrap();
        let original = held.as_ref().unwrap();
        assert_eq!(original.attempts.len(), 3);
        assert!(
            original
                .attempts
                .iter()
                .all(|a| matches!(a.result, Err(SingleSendError::WrongOwner)))
        );
        assert_eq!(
            &*original.request.unknown_participations,
            &members.iter().map(|m| m.participation).collect::<Vec<_>>()
        );
        assert!(Arc::ptr_eq(
            original.request.publication.as_ref().unwrap_err(),
            &publication_refusal
        ));
        assert!(original.request.observation_drain.is_ok());
        drop(held);
        dispose_record(&record);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop((receiver, health, record));
        f.finish();
    }
    #[tokio::test]
    async fn missing_original_observation_disposal_proof_never_becomes_ready_for_disposal() {
        use super::super::observations::ObservationError;
        use novarocks_control_runtime::actor_turn::ActorTurnError;
        for refusal in [
            ObservationError::FailedTailUnresolved,
            ObservationError::Runtime(ActorTurnError::StandingInputBusy),
        ] {
            let f = Fixture::new();
            let (mut auxiliary, health) = auxiliary(&f);
            let drops = Arc::new(AtomicUsize::new(0));
            let (mut receiver, sink) = FailureResidualReceiveObligation::new(
                f.attempt.registration.clone(),
                activation(),
                Arc::from([]),
                Waker::noop().clone(),
                auxiliary.raw_failure_publisher(),
            );
            let (mut original, _) = request(&drops);
            original.observation_drain = Err(refusal);
            sink.finish(original, Vec::new()).await;
            let mut observer = health.clone();
            let mut exit_failure = Box::pin(observer.exit_failed());
            assert!(futures::poll!(exit_failure.as_mut()).is_pending());
            assert!(receiver.progress(2, Some(true)).error.is_none());
            let std::task::Poll::Ready(notified_failure) = futures::poll!(exit_failure.as_mut())
            else {
                panic!("unproven original custody wakes without inventing HostExit");
            };
            drop(exit_failure);
            let record = health.raw_record().unwrap();
            assert_eq!(
                record.verdict(),
                Some(ResidualVerdict::UnresolvedLocalCustody)
            );
            assert!(std::ptr::eq(
                notified_failure
                    .cause()
                    .downcast_ref::<OriginalFailureResidualRecord>()
                    .unwrap(),
                record.as_ref()
            ));
            assert!(!receiver.ready_for_disposal());
            assert!(receiver.take_disposal().is_none() && !receiver.is_retired());
            let failure = health.exit_failure().unwrap();
            assert!(std::ptr::eq(
                failure
                    .cause()
                    .downcast_ref::<OriginalFailureResidualRecord>()
                    .unwrap(),
                record.as_ref(),
            ));
            assert!(
                record
                    .original
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .request
                    .observation_drain
                    .is_err()
            );
            // This metadata probe supplied no actual observation tail. Reclaim
            // only its explicit probes/paid roles; no HostExit is asserted.
            close_auxiliary(&f, &mut auxiliary);
            dispose_record(&record);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            drop((receiver, auxiliary, health, record));
            f.finish();
        }
    }
    #[test]
    fn absent_residual_requires_original_supervisor_disposition() {
        let f = Fixture::new();
        let (mut auxiliary, health) = auxiliary(&f);
        let (mut normal, normal_sink) = FailureResidualReceiveObligation::new(
            f.attempt.registration.clone(),
            activation(),
            Arc::from([]),
            Waker::noop().clone(),
            auxiliary.raw_failure_publisher(),
        );
        assert!(normal.progress(1, None).error.is_none());
        assert!(!normal.ready_for_disposal() && !normal.needs_disposal());
        drop(normal_sink);
        assert!(
            !normal.ready_for_disposal(),
            "dropping a sink never proves its original unused work exited"
        );
        assert!(normal.progress(1, Some(false)).error.is_none());
        assert!(normal.ready_for_disposal() && !normal.needs_disposal());
        let (mut failed, sink) = FailureResidualReceiveObligation::new(
            f.attempt.registration.clone(),
            activation(),
            Arc::from([]),
            Waker::noop().clone(),
            auxiliary.raw_failure_publisher(),
        );
        drop(sink);
        let refused = failed.progress(1, Some(true));
        assert_eq!(refused.error, Some(ResidualReceiveError::MissingOriginal));
        assert!(!failed.ready_for_disposal() && health.raw_record().is_none());
        close_auxiliary(&f, &mut auxiliary);
        drop((normal, failed, auxiliary, health));
        f.finish();
    }
}
