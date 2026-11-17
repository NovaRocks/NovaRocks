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

//! Actual retirement of rich transport captures returned to the host.

use super::{AttemptCallSource, call_source::TransportRelease};
use crate::task_execution::host::{
    adapter::AttemptNotice,
    custody::{CustodyError, HostDisposal},
    observations::{ObservationError, backing, host_turn::OrderedObservationHost},
};
use novarocks_control_runtime::{obligation::WorkRetirementObserver, transaction::RuntimeError};
use novarocks_control_runtime_core::{capacity::CapacityError, scope::ScopeError};
use novarocks_workload_control::WorkOwner;
use std::{
    any::Any,
    collections::VecDeque,
    mem::ManuallyDrop,
    sync::{Arc, Weak},
    task::{Context, Poll, Waker},
};
use tokio::{runtime::Handle, task::JoinError};

type Original = Box<dyn Any + Send>;

/// Constructed only by the original AP Source after its real producer and
/// three original LP fact positions closed. The index is retained from the
/// same activation link, never used to issue Work or reconstruct an author.
/// Checked by the original metadata disposal body and its real Join/header.
/// This value has no Drop completion; issuance alone cannot return the author.
#[must_use = "Return the same original author through its actual metadata work"]
pub(super) struct OriginalInputAuthorReturnObligation {
    bank: Weak<crate::control_runtime::logical::LogicalInputBank>,
    index: usize,
    endpoints: Arc<crate::task_execution::host::port::AttemptPortEndpoints>,
}
impl OriginalInputAuthorReturnObligation {
    pub(super) fn after_close(
        bank: Weak<crate::control_runtime::logical::LogicalInputBank>,
        index: usize,
        endpoints: Arc<crate::task_execution::host::port::AttemptPortEndpoints>,
    ) -> Option<Self> {
        (endpoints.producer_exited()
            && endpoints.facts.is_closed()
            && endpoints.requests.is_empty())
        .then_some(Self {
            bank,
            index,
            endpoints,
        })
    }
}
async fn return_original_input_author(original: OriginalInputAuthorReturnObligation) {
    assert!(original.endpoints.producer_exited() && original.endpoints.facts.is_closed());
    let bank = original
        .bank
        .upgrade()
        .expect("same LP inventory outlives its original AP child");
    let mut loan = bank
        .loan()
        .await
        .expect("issued original AP input author is not retired");
    loan.ap_inputs_closed_after_producer_exit(original.index)
        .unwrap_or_else(|original| std::panic::panic_any(original));
}

pub(super) enum ReleaseCapture {
    NativeOriginals(
        Box<crate::query_execution::native_execution_adapter::AttemptSourceOriginalsObligation>,
    ),
    FailureResidual(
        Arc<crate::task_execution::host::failure_residual::OriginalFailureResidualRecord>,
    ),
    InputAuthor(OriginalInputAuthorReturnObligation),
    Original(Original),
    Transport(TransportRelease),
    Subscription(WorkOwner),
    Observations(OrderedObservationHost),
    Notice(Arc<AttemptNotice>),
}

impl ReleaseCapture {
    pub(super) fn dispose(self) {
        match self {
            Self::NativeOriginals(original) => original.dispose_on_executor(),
            Self::FailureResidual(original) => original.dispose_on_executor(),
            Self::InputAuthor(original) => std::panic::resume_unwind(Box::new(original)),
            Self::Original(original) => drop(original),
            Self::Transport(original) => original.dispose(),
            Self::Subscription(owner) => owner.complete_after_terminal_cancel_settled(),
            Self::Observations(owner) => owner.close_on_executor(),
            Self::Notice(notice) => crate::task_execution::host::auxiliary::report_notice(&notice),
        }
    }
}

#[derive(Debug)]
#[allow(
    dead_code,
    reason = "Release refusal retains the original admission or report error with disposal custody."
)]
pub(super) enum ReleaseError {
    Admission(RuntimeError),
    Retirement(CustodyError),
}

#[allow(
    dead_code,
    reason = "A failed release retains the typed cause and original launch, Join and panic evidence."
)]
pub(super) struct ReleaseFailure {
    pub(super) error: ReleaseError,
    pub(super) handoff: Option<Original>,
    pub(super) join: Option<JoinError>,
    pub(super) panic: Option<Original>,
}

enum Disposal {
    Blocking(HostDisposal<ReleaseCapture>),
    InputAuthor(HostDisposal<OriginalInputAuthorReturnObligation>),
}
impl Disposal {
    fn poll_complete(&self, cx: &mut Context<'_>) -> Poll<Result<(), CustodyError>> {
        match self {
            Self::Blocking(original) => original.poll_complete(cx),
            Self::InputAuthor(original) => original.poll_complete(cx),
        }
    }
    fn take_failure(&self) -> (Option<Original>, Option<JoinError>, Option<Original>) {
        match self {
            Self::Blocking(original) => original.take_failure(),
            Self::InputAuthor(original) => original.take_failure(),
        }
    }
}

struct Active {
    header: WorkRetirementObserver,
    original: Disposal,
    _observation_backing: Option<backing::ObservationBackingPin>,
    residual:
        Option<Arc<crate::task_execution::host::failure_residual::OriginalFailureResidualRecord>>,
}

/// One blocking job, one host batch (at most two objects), and the sole
/// refused root-control input. The host stops retiring more batches when its
/// preallocated batch is occupied.
pub(in crate::task_execution::host) struct TransportReleases {
    pending: VecDeque<ManuallyDrop<ReleaseCapture>>,
    refused_root: Option<ManuallyDrop<Original>>,
    active: Option<Active>,
    executor: Handle,
    wake: Waker,
    failed: bool,
}

impl TransportReleases {
    pub(super) fn new(executor: Handle, wake: Waker) -> Self {
        Self {
            pending: VecDeque::with_capacity(2),
            refused_root: None,
            active: None,
            executor,
            wake,
            failed: false,
        }
    }

    pub(super) fn accepts_batch(&self) -> bool {
        !self.failed && self.pending.is_empty()
    }

    pub(super) fn executor(&self) -> &Handle {
        &self.executor
    }

    pub(super) fn wake(&self) -> &Waker {
        &self.wake
    }

    pub(super) fn retain(&mut self, original: ReleaseCapture) {
        assert!(self.pending.len() < 2);
        self.pending.push_back(ManuallyDrop::new(original));
    }

    pub(super) fn retain_refused_root(&mut self, original: Original) {
        assert!(self.refused_root.is_none());
        self.refused_root = Some(ManuallyDrop::new(original));
    }

    pub(super) fn is_empty(&self) -> bool {
        !self.failed
            && self.active.is_none()
            && self.pending.is_empty()
            && self.refused_root.is_none()
    }

    pub(super) fn has_work(&self) -> bool {
        !self.failed && !self.is_empty()
    }

    pub(in crate::task_execution::host) fn backing_bytes() -> Result<usize, ObservationError> {
        let blocking = HostDisposal::<ReleaseCapture>::backing_layouts()
            .map_err(|_| ObservationError::Budget)?;
        let asynchronous =
            HostDisposal::<OriginalInputAuthorReturnObligation>::future_backing_layouts(
                return_original_input_author,
            )
            .map_err(|_| ObservationError::Budget)?;
        // There is still ONE sequential Active. Both the actual boxed inner
        // metadata future and its obligated carrier are priced; the original
        // inventory/endpoints already have their original Source allocation.
        let blocking = backing::sum(blocking.map(|layout| layout.size()))?;
        let asynchronous = backing::sum(asynchronous.map(|layout| layout.size()))?;
        backing::sum([
            std::mem::size_of::<Self>(),
            backing::array::<ManuallyDrop<ReleaseCapture>>(2)?,
            blocking.max(asynchronous),
        ])
    }

    pub(super) fn progress(&mut self, author: &AttemptCallSource) -> Result<bool, ReleaseFailure> {
        if self.failed {
            return Ok(false);
        }
        if let Some(active) = &self.active {
            match active
                .original
                .poll_complete(&mut Context::from_waker(&self.wake))
            {
                Poll::Pending => return Ok(false),
                Poll::Ready(Ok(())) => match author.reap_disposal(&active.header) {
                    Ok(true) => {
                        if let Some(original) = &active.residual {
                            original.disposal_retired();
                        }
                        self.active = None;
                        return Ok(true);
                    }
                    Ok(false) => return Ok(false),
                    Err(error) => return Err(self.admission_failure(error)),
                },
                Poll::Ready(Err(error)) => {
                    self.failed = true;
                    let (handoff, join, panic) = active.original.take_failure();
                    return Err(ReleaseFailure {
                        error: ReleaseError::Retirement(error),
                        handoff,
                        join,
                        panic,
                    });
                }
            }
        }
        if self.pending.is_empty() && self.refused_root.is_none() {
            return Ok(false);
        }
        let work = match author.reserve_disposal() {
            Ok(work) => work,
            Err(
                RuntimeError::Scope(ScopeError::GenerationMismatch)
                | RuntimeError::Capacity(CapacityError::Scope(ScopeError::GenerationMismatch)),
            ) => {
                // A concurrent owner failure advanced the same original
                // Scope. The next host visit derives Cleanup in that epoch.
                return Ok(true);
            }
            Err(error) => return Err(self.admission_failure(error)),
        };
        let original = self
            .pending
            .pop_front()
            .or_else(|| {
                self.refused_root.take().map(|root| {
                    ManuallyDrop::new(ReleaseCapture::Original(ManuallyDrop::into_inner(root)))
                })
            })
            .expect("original pending transport release");
        let original = ManuallyDrop::into_inner(original);
        let observation_backing = match &original {
            ReleaseCapture::Observations(original) => original.backing_pin(),
            _ => None,
        };
        let residual = match &original {
            ReleaseCapture::FailureResidual(original) => Some(original.clone()),
            _ => None,
        };
        let header = work.work().retirement_observer();
        let original = match original {
            ReleaseCapture::InputAuthor(original) => Disposal::InputAuthor(
                HostDisposal::start_future(
                    &self.executor,
                    work,
                    author.observer(),
                    original,
                    return_original_input_author,
                )
                .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused))),
            ),
            original => Disposal::Blocking(
                HostDisposal::start(
                    &self.executor,
                    work,
                    author.observer(),
                    original,
                    ReleaseCapture::dispose,
                )
                .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused))),
            ),
        };
        self.active = Some(Active {
            header,
            original,
            _observation_backing: observation_backing,
            residual,
        });
        Ok(true)
    }

    fn admission_failure(&mut self, error: RuntimeError) -> ReleaseFailure {
        self.failed = true;
        ReleaseFailure {
            error: ReleaseError::Admission(error),
            handoff: None,
            join: None,
            panic: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_execution::host::{
        custody::tests::WithdrawalInbox, transport::AttemptCallOwner,
    };
    use novarocks_control_runtime::transaction::RuntimeLedger;
    use novarocks_control_runtime_core::{
        capacity::{ReservationResources, ResourceVector},
        identity::{ConsumerId, Generation},
        owned_storage::OwnedSlotNeed,
        scope::{WorkAdmission, WorkKind},
    };
    use std::{sync::mpsc, thread, time::Duration};
    use tokio::sync::oneshot;

    struct OriginalCapture {
        entered: Option<oneshot::Sender<thread::ThreadId>>,
        finish: mpsc::Receiver<()>,
    }

    impl Drop for OriginalCapture {
        fn drop(&mut self) {
            self.entered
                .take()
                .unwrap()
                .send(thread::current().id())
                .unwrap();
            self.finish.recv_timeout(Duration::from_secs(3)).unwrap();
        }
    }

    #[tokio::test]
    async fn residual_rich_capture_waits_for_original_disposal_join_and_header() {
        use crate::{
            control_runtime::host::EffectSource,
            task_execution::host::{
                auxiliary::AttemptAuxiliarySource,
                failure_publication::FailurePublicationError,
                failure_residual::{FailureResidualReceiveObligation, ResidualVerdict},
                failure_supervisor::FailureCleanupRequest,
                port::requests::test_support::{EPOCH, Fixture, OBSERVER, activation},
            },
        };
        use novarocks_control_runtime::{
            actor_turn::ActorTurnError, causal_input::CausalInputObligation,
        };
        use novarocks_control_runtime_core::{identity::Tick, mailbox::InputClass};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct OriginalResidualCapture {
            _capture: OriginalCapture,
            dropped: Arc<AtomicUsize>,
        }
        impl Drop for OriginalResidualCapture {
            fn drop(&mut self) {
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
        for typed_refusal in [false, true] {
            let f = Fixture::new();
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
                .unwrap_or_else(|_| panic!("same original infrastructure role"));
            let (mut auxiliary, health) =
                AttemptAuxiliarySource::new(f.attempt.registration.clone(), input);
            let (mut receiver, sink) = FailureResidualReceiveObligation::new(
                f.attempt.registration.clone(),
                activation(),
                Arc::from([]),
                Waker::noop().clone(),
                auxiliary.raw_failure_publisher(),
            );
            let (entered, started) = oneshot::channel();
            let (release, finish) = mpsc::channel();
            let dropped = Arc::new(AtomicUsize::new(0));
            let refusal = Arc::new(FailurePublicationError::Runtime(ActorTurnError::Closed));
            sink.finish(
                FailureCleanupRequest {
                    core_panic: Box::new(OriginalResidualCapture {
                        _capture: OriginalCapture {
                            entered: Some(entered),
                            finish,
                        },
                        dropped: dropped.clone(),
                    }),
                    publication: if typed_refusal {
                        Err(refusal.clone())
                    } else {
                        Ok(())
                    },
                    root_reply: None,
                    observation_drain: Ok(()),
                    unknown_participations: Box::new([]),
                },
                Vec::new(),
            )
            .await;
            // Only receiver metadata is scripted here. The disposal below uses
            // actual Work, blocking execution, Join, reports and header reaping;
            // this test does not claim a real failure-supervisor/AP HostExit.
            assert!(receiver.progress(2, Some(true)).error.is_none());
            let record = health.raw_record().unwrap();
            assert_eq!(
                record.verdict(),
                Some(if typed_refusal {
                    ResidualVerdict::TypedRefusal
                } else {
                    ResidualVerdict::LocallyProven
                })
            );
            assert!(
                receiver.ready_for_disposal()
                    && receiver.needs_disposal()
                    && !receiver.is_retired()
            );
            let payload = receiver.take_disposal().unwrap();
            assert!(
                !receiver.needs_disposal(),
                "issued work waits for its original disposal wake"
            );
            assert!(Arc::ptr_eq(&payload, &record) && receiver.take_disposal().is_none());
            let result = CausalInputObligation::reserve_admitted(
                &f.runtime,
                &f.attempt.registration,
                f.logical.registration.actor(),
                InputClass::Lifecycle,
                &f.attempt.work,
            )
            .unwrap();
            let mut author = AttemptCallSource::new(
                &f.attempt.work,
                &f.attempt.payer,
                EPOCH,
                OBSERVER,
                AttemptCallOwner::Release,
                result,
            );
            let mut releases = TransportReleases::new(Handle::current(), Waker::noop().clone());
            releases.retain(ReleaseCapture::FailureResidual(payload));
            assert!(!receiver.is_retired() && !releases.is_empty());
            assert!(
                releases
                    .progress(&author)
                    .unwrap_or_else(|f| panic!("{:?}", f.error))
            );
            let executor = tokio::time::timeout(Duration::from_secs(3), started)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(executor, thread::current().id());
            assert!(!receiver.is_retired() && !releases.is_empty());
            assert_eq!(f.attempt.scope.snapshot().unwrap().work, 2);
            assert!(
                !releases
                    .progress(&author)
                    .unwrap_or_else(|f| panic!("{:?}", f.error))
            );
            release.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                while !record.disposal_body_returned() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(
                !receiver.is_retired() && !releases.is_empty(),
                "body return is not actual Join/header retirement"
            );
            assert_eq!(f.attempt.scope.snapshot().unwrap().work, 2);
            tokio::time::timeout(Duration::from_secs(3), async {
                while !releases
                    .progress(&author)
                    .unwrap_or_else(|f| panic!("{:?}", f.error))
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            assert!(receiver.is_retired() && !receiver.needs_disposal() && releases.is_empty());
            assert_eq!(f.attempt.scope.snapshot().unwrap().work, 1);
            assert_eq!(
                record.original_publication_refusal().is_some(),
                typed_refusal
            );
            if typed_refusal {
                assert!(Arc::ptr_eq(
                    record.original_publication_refusal().unwrap(),
                    &refusal
                ));
            }
            assert!(record.original_root_reply_refusal().is_none());
            assert_eq!(health.exit_failure().is_some(), typed_refusal);
            assert!(author.close_idle().unwrap());
            assert!(author.take_drained_owner().is_none() && author.is_drained());
            auxiliary.close_external();
            auxiliary.transport_roles_drained();
            f.runtime
                .notify_host_actor(&f.attempt.registration)
                .unwrap();
            loop {
                let turn = f.runtime.claim_host_actor().unwrap();
                if turn.matches_registration(&f.attempt.registration) {
                    assert!(
                        auxiliary
                            .progress_retirement(&f.runtime, &turn, Tick(1), 1)
                            .retired
                    );
                    turn.release(false).unwrap();
                    break;
                }
                turn.release(false).unwrap();
            }
            drop((receiver, releases, author, auxiliary, health, record));
            f.finish();
        }
    }

    #[tokio::test]
    async fn rich_releases_keep_the_original_work_until_destructors_exit() {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let (mut parent, mut reservation) = scope
            .reserve_next(
                generation,
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        parent.start().unwrap();
        let inbox = WithdrawalInbox::new(&scope);
        let mut author = AttemptCallSource::new(
            &parent,
            &reservation,
            generation,
            ConsumerId::new(1).unwrap(),
            AttemptCallOwner::Release,
            inbox.reserve_result(&parent),
        );
        let mut releases = TransportReleases::new(Handle::current(), Waker::noop().clone());
        let mut releases_held = Vec::new();
        for _ in 0..2 {
            let (entered, started) = oneshot::channel();
            let (release, finish) = mpsc::channel();
            releases.retain(ReleaseCapture::Original(Box::new(OriginalCapture {
                entered: Some(entered),
                finish,
            })));
            releases_held.push((started, release));
        }
        let poller = thread::current().id();
        for (n, (started, release)) in releases_held.into_iter().enumerate() {
            assert!(!releases.accepts_batch());
            assert!(
                releases
                    .progress(&author)
                    .unwrap_or_else(|failure| panic!("{:?}", failure.error))
            );
            let executor = tokio::time::timeout(Duration::from_secs(3), started)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(poller, executor);
            assert_eq!(scope.snapshot().unwrap().work, 2);
            assert!(
                !releases
                    .progress(&author)
                    .unwrap_or_else(|failure| panic!("{:?}", failure.error))
            );
            if n == 0 {
                scope.fail().unwrap();
            }
            release.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                while !releases
                    .progress(&author)
                    .unwrap_or_else(|failure| panic!("{:?}", failure.error))
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(scope.snapshot().unwrap().work, 1);
        }
        assert!(releases.is_empty());
        assert!(author.close_idle().unwrap());
        assert!(author.take_drained_owner().is_none() && author.is_drained());
        parent.body_exited().unwrap();
        parent.captures_exited().unwrap();
        reservation.finish().unwrap();
        scope.reap_ready(128).unwrap();
        inbox.finish();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    }
}
