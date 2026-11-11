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

use super::AttemptCallSource;
use crate::task_execution::host::{
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
    task::{Context, Poll, Waker},
};
use tokio::{runtime::Handle, task::JoinError};

type Original = Box<dyn Any + Send>;

pub(super) enum ReleaseCapture {
    Original(Original),
    Subscription(WorkOwner),
    Observations(OrderedObservationHost),
}

impl ReleaseCapture {
    pub(super) fn dispose(self) {
        match self {
            Self::Original(original) => drop(original),
            Self::Subscription(owner) => owner.complete_after_terminal_cancel_settled(),
            Self::Observations(owner) => owner.close_on_executor(),
        }
    }
}

#[derive(Debug)]
pub(super) enum ReleaseError {
    Admission(RuntimeError),
    Retirement(CustodyError),
}

pub(super) struct ReleaseFailure {
    pub(super) error: ReleaseError,
    pub(super) handoff: Option<Original>,
    pub(super) join: Option<JoinError>,
    pub(super) panic: Option<Original>,
}

struct Active {
    header: WorkRetirementObserver,
    original: HostDisposal<ReleaseCapture>,
    _observation_backing: Option<backing::ObservationBackingPin>,
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
        let [record, exit, panic, capture, carrier] =
            HostDisposal::<ReleaseCapture>::backing_layouts()
                .map_err(|_| ObservationError::Budget)?;
        backing::sum([
            std::mem::size_of::<Self>(),
            backing::array::<ManuallyDrop<ReleaseCapture>>(2)?,
            record.size(),
            exit.size(),
            panic.size(),
            capture.size(),
            carrier.size(),
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
        let header = work.work().retirement_observer();
        let original = HostDisposal::start(
            &self.executor,
            work,
            author.observer(),
            original,
            ReleaseCapture::dispose,
        )
        .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused)));
        self.active = Some(Active {
            header,
            original,
            _observation_backing: observation_backing,
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
