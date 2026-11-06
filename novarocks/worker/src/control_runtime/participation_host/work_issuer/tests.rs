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

//! Test composition uses the same registry, original obligations and retirement
//! as production. A pending child is never declared exited to clean a fixture.

use super::*;
use crate::{
    HostRejection, ParticipationHostInstance, ParticipationMaterializationControl,
    ReleasedParticipationEvidence, SharedFactsRequest,
};
use novarocks_control_runtime::{
    exit_work::{ExitWorkError, ObligatedFuture, run_future},
    history::HistoryCapture,
};
use novarocks_control_runtime_core::identity::Generation;
use novarocks_execution_contract::task_execution::operation::ParticipationDomainUpdate;
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};
use std::{
    future::Future,
    pin::Pin,
    sync::Weak,
    task::{Context, Poll, Waker},
    time::Duration,
};

/// A real, isolated Participation Lifetime owner for adapter and Worker tests.
/// `finish().await` must follow destruction of all granted instances/senders.
/// A timeout or an unresolved original is a fixture failure, never a fake exit.
pub struct ParticipationWorkTestOwner {
    registry: ParticipationWorkRegistry,
    lifetime: Option<ExitObligation>,
    lifetime_custody: ExitCustody,
    lifetime_report: ReportSlot,
    lifetime_retirement: WorkRetirementObserver,
    sender_parent: Option<(ExitObligation, InstanceWorkIssuer)>,
    finished: bool,
    #[cfg(test)]
    history: HistoryCapture,
}
impl Default for ParticipationWorkTestOwner {
    fn default() -> Self {
        Self::new()
    }
}
impl ParticipationWorkTestOwner {
    pub fn new() -> Self {
        Self::new_for(ParticipationRef::new(
            QueryExecutionId::new(QueryId::new(8761, 8762), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        ))
    }

    pub fn new_for(participation: ParticipationRef) -> Self {
        let history = HistoryCapture::new(256);
        let ledger = RuntimeLedger::with_history(Default::default(), history.clone());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let mut reserved = scope
            .reserve_next(
                generation,
                ReservationResources::new(
                    Default::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap();
        let lifetime_report = ReportSlot::reserve_next(
            &mut reserved,
            generation,
            1,
            [(OBSERVER, ConsumerKind::Observer)],
        )
        .unwrap();
        let (work, capacity) = reserved.bind(WorkKind::Cleanup).unwrap();
        let parent = Parent::from_work(&work).unwrap();
        let lifetime_retirement = work.retirement_observer();
        let (lifetime_custody, mut lifetime) = ExitCustody::from_admitted(adopt_reported_work(
            work,
            capacity,
            lifetime_report.clone(),
        ));
        lifetime.start().unwrap();
        let registry = ParticipationWorkRegistry(Arc::new(Inner {
            ledger,
            scope,
            participation,
            lifetime: parent,
            // Tests poll actual retirement directly; they do not invent an
            // Actor registration or substitute notification for real exit.
            wake: None,
            health: Arc::new(ExecutionHealth::new()),
            state: Mutex::new(State {
                entries: BTreeMap::new(),
                retained: None,
                scan_requested: false,
                scan_upper: None,
                scan_after: None,
            }),
        }));
        Self {
            registry,
            lifetime: Some(lifetime),
            lifetime_custody,
            lifetime_report,
            lifetime_retirement,
            sender_parent: None,
            finished: false,
            #[cfg(test)]
            history,
        }
    }

    pub fn instance(&self) -> ParticipationInstanceGrant {
        self.registry.issue_instance().expect("live test Lifetime")
    }

    /// Issues under an actual started instance held by this fixture until its
    /// original RF sender children have all retired from the real registry.
    pub fn runtime_filter_sender(&mut self) -> RuntimeFilterSenderWork {
        if self.sender_parent.is_none() {
            let (mut work, issuer) = self.instance().into_parts();
            work.start().expect("test sender instance starts once");
            self.sender_parent = Some((work, issuer));
        }
        self.sender_parent
            .as_ref()
            .expect("original test sender instance")
            .1
            .runtime_filter_sender()
            .expect("live test sender instance")
    }

    /// Makes bounded progress using actual reports, custody and original
    /// header observations. False means an original capture is still live.
    pub fn poll_finish(&mut self) -> Result<bool, ParticipationWorkError> {
        if self.finished {
            return Ok(true);
        }
        // This fixture has no Actor wake endpoint. Its explicit caller polls
        // actual originals; production instead requests sweeps on real events.
        self.registry.request_scan();
        self.registry.retire_ready(64)?;
        if let Some((_, issuer)) = &self.sender_parent {
            let children_live = self
                .registry
                .0
                .state
                .lock()
                .expect("test original registry lock")
                .entries
                .values()
                .any(|entry| entry.parent == issuer.parent.work.work);
            if !children_live {
                self.sender_parent
                    .take()
                    .expect("same original sender parent")
                    .0
                    .exited()?;
                self.registry.request_scan();
                self.registry.retire_ready(64)?;
            }
        }
        if !self
            .registry
            .0
            .state
            .lock()
            .expect("test original registry lock")
            .entries
            .is_empty()
        {
            return Ok(false);
        }
        if let Some(lifetime) = self.lifetime.take() {
            lifetime.exited()?;
            self.lifetime_report.consume(OBSERVER)?;
        }
        if (self.lifetime_custody.phase() != ExitCustodyPhase::Retired
            && !self.lifetime_custody.try_retire()?)
            || !self
                .registry
                .0
                .ledger
                .reap_observed_header(&self.lifetime_retirement)?
        {
            return Ok(false);
        }
        self.registry.0.scope.close_external()?;
        self.registry
            .0
            .ledger
            .remove_scope(self.registry.0.scope.id())?;
        self.finished = true;
        assert!(self.registry.0.ledger.is_empty());
        Ok(true)
    }

    pub async fn finish(mut self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.poll_finish().expect("original test work must resolve") {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("original test work did not actually exit");
        assert!(self.registry.0.ledger.is_empty());
    }
}

type TestInstanceFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
struct TestWorkNotify(ParticipationWorkWake);
impl Drop for TestWorkNotify {
    fn drop(&mut self) {
        self.0.notify();
    }
}
struct TestParticipationInstance {
    inner: Weak<dyn ParticipationHostInstance>,
    // Field order matters: destroy the actual owned inner capture and settle
    // its obligation before notifying, including an explicit Drop panic.
    _work: Mutex<ObligatedFuture<TestInstanceFuture>>,
    _wake: TestWorkNotify,
}
impl ParticipationInstanceGrant {
    pub fn bind_test_instance(
        self,
        inner: Arc<dyn ParticipationHostInstance>,
    ) -> Result<Arc<dyn ParticipationHostInstance>, ParticipationWorkError> {
        if self.participation() != inner.participation() {
            return Err(RuntimeError::Scope(
                novarocks_control_runtime_core::scope::ScopeError::InvalidTransition,
            )
            .into());
        }
        let panic = self
            .issuer
            .registry
            .0
            .state
            .lock()
            .expect("test original registry lock")
            .entries
            .get(&self.issuer.parent.work.work)
            .expect("same original granted instance")
            .panic
            .clone();
        let wake = TestWorkNotify(ParticipationWorkWake {
            registry: self.issuer.registry.clone(),
        });
        let weak = Arc::downgrade(&inner);
        let (work, _) = self.into_parts();
        let capture: TestInstanceFuture = Box::pin(async move {
            std::future::pending::<()>().await;
            drop(inner);
        });
        let mut work = run_future(work, capture, panic);
        match Pin::new(&mut work).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Pending => Ok(Arc::new(TestParticipationInstance {
                inner: weak,
                _work: Mutex::new(work),
                _wake: wake,
            })),
            Poll::Ready(Err(ExitWorkError::NotStarted(error) | ExitWorkError::Exit(error))) => {
                Err(error.into())
            }
            Poll::Ready(Ok(())) => unreachable!("test instance remains live until actual Drop"),
        }
    }
}
impl ParticipationHostInstance for TestParticipationInstance {
    fn participation(&self) -> ParticipationRef {
        self.inner
            .upgrade()
            .expect("live original test instance")
            .participation()
    }
    fn materialize(
        &self,
        request: SharedFactsRequest<'_>,
        control: &dyn ParticipationMaterializationControl,
    ) -> Result<(), HostRejection> {
        self.inner
            .upgrade()
            .expect("live original test instance")
            .materialize(request, control)
    }
    fn release(&self) -> ReleasedParticipationEvidence {
        self.inner
            .upgrade()
            .expect("live original test instance")
            .release()
    }
    fn advance_shared_domain(
        &self,
        domain: &ParticipationDomainUpdate,
    ) -> Result<(), HostRejection> {
        self.inner
            .upgrade()
            .expect("live original test instance")
            .advance_shared_domain(domain)
    }
}

#[cfg(test)]
mod evidence {
    use super::*;
    use novarocks_control_runtime::exit_work::ForwardedWorkPanic;
    use novarocks_control_runtime_core::history::HistoryFact;
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Condvar,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    #[tokio::test]
    async fn unused_original_grants_retire_without_consuming_an_unfilled_report() {
        let mut owner = ParticipationWorkTestOwner::new();
        let history = owner.history.clone();
        drop(owner.instance());
        drop(owner.runtime_filter_sender());
        owner.finish().await;
        let facts = history.drain(256);
        assert!(facts.complete);
        assert!(
            !facts
                .entries
                .iter()
                .any(|entry| matches!(entry.fact, HistoryFact::WorkUnresolved { .. }))
        );
        assert_eq!(
            facts
                .entries
                .iter()
                .filter(|entry| matches!(entry.fact, HistoryFact::WorkRetired { .. }))
                .count(),
            4
        );
        assert_eq!(
            facts
                .entries
                .iter()
                .filter(|entry| matches!(entry.fact, HistoryFact::ScopeRemoved { .. }))
                .count(),
            1
        );
    }

    struct MetadataInstance {
        participation: ParticipationRef,
        custody: ExitCustody,
        dropped: Arc<AtomicBool>,
        releases: Arc<AtomicUsize>,
    }
    impl Drop for MetadataInstance {
        fn drop(&mut self) {
            assert!(!self.custody.captures_exited());
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    impl ParticipationHostInstance for MetadataInstance {
        fn participation(&self) -> ParticipationRef {
            self.participation
        }
        fn materialize(
            &self,
            _: SharedFactsRequest<'_>,
            _: &dyn ParticipationMaterializationControl,
        ) -> Result<(), HostRejection> {
            unreachable!("metadata-only test instance")
        }
        fn release(&self) -> ReleasedParticipationEvidence {
            self.releases.fetch_add(1, Ordering::SeqCst);
            ReleasedParticipationEvidence::none()
        }
        fn advance_shared_domain(
            &self,
            _: &ParticipationDomainUpdate,
        ) -> Result<(), HostRejection> {
            unreachable!("metadata-only test instance")
        }
    }

    #[tokio::test]
    async fn test_instance_delegates_and_exits_only_after_its_original_capture_drops() {
        let mut owner = ParticipationWorkTestOwner::new();
        let grant = owner.instance();
        let custody = owner.registry.0.state.lock().unwrap().entries
            [&grant.issuer.parent.work.work]
            .custody
            .clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let releases = Arc::new(AtomicUsize::new(0));
        let instance = grant
            .bind_test_instance(Arc::new(MetadataInstance {
                participation: owner.registry.0.participation,
                custody: custody.clone(),
                dropped: dropped.clone(),
                releases: releases.clone(),
            }))
            .unwrap();
        assert_eq!(instance.participation(), owner.registry.0.participation);
        instance.release();
        assert_eq!(releases.load(Ordering::SeqCst), 1);
        assert_eq!(custody.phase(), ExitCustodyPhase::Started);
        let copy = instance.clone();
        drop(instance);
        assert!(!owner.poll_finish().unwrap());
        assert!(!dropped.load(Ordering::SeqCst));
        drop(copy);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(custody.phase(), ExitCustodyPhase::Exited);
        owner.finish().await;
        assert_eq!(custody.phase(), ExitCustodyPhase::Retired);
    }

    #[tokio::test]
    async fn original_sender_header_retires_before_its_instance_and_lifetime() {
        let mut owner = ParticipationWorkTestOwner::new();
        let history = owner.history.clone();
        let lifetime = owner.registry.0.lifetime.work.work;
        let grant = owner.instance();
        let instance = grant.issuer.parent.work.work;
        let (mut instance_work, issuer) = grant.into_parts();
        instance_work.start().unwrap();
        let sender = issuer.runtime_filter_sender().unwrap();
        let sender_id = *owner
            .registry
            .0
            .state
            .lock()
            .unwrap()
            .entries
            .last_key_value()
            .unwrap()
            .0;
        let inventory = owner.registry.0.scope.work_inventory().unwrap();
        let instance_entry = inventory
            .entries
            .iter()
            .find(|entry| entry.work == instance)
            .unwrap();
        let sender_entry = inventory
            .entries
            .iter()
            .find(|entry| entry.work == sender_id)
            .unwrap();
        assert_eq!(instance_entry.parent, Some(lifetime));
        assert_eq!(
            instance_entry.label,
            Some(work_labels::PARTICIPATION_INSTANCE)
        );
        assert_eq!(sender_entry.parent, Some(instance));
        assert_eq!(sender_entry.label, Some(work_labels::RUNTIME_FILTER_SENDER));
        assert_eq!(
            inventory.live_by_label[work_labels::PARTICIPATION_INSTANCE],
            1
        );
        assert_eq!(
            inventory.live_by_label[work_labels::RUNTIME_FILTER_SENDER],
            1
        );
        instance_work.exited().unwrap();
        assert!(!owner.poll_finish().unwrap());
        assert!(
            owner
                .registry
                .0
                .state
                .lock()
                .unwrap()
                .entries
                .contains_key(&instance)
        );
        // This child was truly unused: its own grant destructor revokes it.
        drop(sender);
        owner.finish().await;
        let facts = history.drain(256);
        assert!(facts.complete);
        let retired: Vec<_> = facts
            .entries
            .iter()
            .filter_map(|entry| match entry.fact {
                HistoryFact::WorkRetired { work, .. } => Some(work),
                _ => None,
            })
            .collect();
        assert_eq!(retired, [sender_id, instance, lifetime]);
    }

    #[tokio::test]
    async fn original_sender_panic_blocks_registry_progress_until_owner_takes_payload() {
        let mut owner = ParticipationWorkTestOwner::new();
        let sender = owner.runtime_filter_sender();
        let (work, panic, wake) = sender.into_parts();
        let mut future = run_future(
            work,
            async { panic!("original RF sender panic") },
            panic.clone(),
        );
        let marker = catch_unwind(AssertUnwindSafe(|| {
            Pin::new(&mut future).poll(&mut Context::from_waker(Waker::noop()))
        }))
        .unwrap_err();
        assert!(marker.is::<ForwardedWorkPanic>());
        drop(future);
        wake.notify();
        assert_eq!(owner.poll_finish(), Err(ParticipationWorkError::Panicked));
        assert_eq!(owner.registry.0.scope.snapshot().unwrap().unresolved, 0);
        assert!(panic.has_panic());
        assert_eq!(
            panic.take().unwrap().downcast_ref::<&str>(),
            Some(&"original RF sender panic")
        );
        assert!(!panic.has_panic());
        owner.finish().await;
    }

    #[test]
    fn explicit_sender_capture_panic_retains_the_original_unknown_in_inventory() {
        struct PanicOnDrop;
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("original RF capture Drop panic");
            }
        }
        let mut owner = ParticipationWorkTestOwner::new();
        let (work, panic, wake) = owner.runtime_filter_sender().into_parts();
        let capture = PanicOnDrop;
        let future = run_future(
            work,
            async move {
                let _capture = capture;
                std::future::pending::<()>().await;
            },
            panic.clone(),
        );
        let marker = catch_unwind(AssertUnwindSafe(|| drop(future))).unwrap_err();
        assert!(marker.is::<ForwardedWorkPanic>());
        wake.notify();
        assert_eq!(owner.poll_finish(), Err(ParticipationWorkError::Panicked));
        assert_eq!(
            panic.take().unwrap().downcast_ref::<&str>(),
            Some(&"original RF capture Drop panic")
        );
        assert_eq!(owner.poll_finish(), Err(ParticipationWorkError::Unresolved));
        // Newer live originals cannot hide this older Unknown behind a fixed
        // reverse prefix when the actual production turn budget is smaller.
        let newer: Vec<_> = (0..4).map(|_| owner.runtime_filter_sender()).collect();
        owner.registry.request_scan();
        assert_eq!(owner.registry.retire_ready(1), Ok(0));
        assert_eq!(
            owner.registry.retire_ready(1),
            Err(ParticipationWorkError::Unresolved)
        );
        drop(newer);
        let inventory = owner.registry.0.scope.work_inventory().unwrap();
        let sender = inventory
            .entries
            .iter()
            .find(|entry| entry.label == Some(work_labels::RUNTIME_FILTER_SENDER))
            .unwrap();
        assert!(sender.unresolved);
        assert!(!sender.body_exited);
        assert!(!sender.captures_exited);
        assert_eq!(
            inventory.unresolved_by_label[work_labels::RUNTIME_FILTER_SENDER],
            1
        );
        assert_eq!(owner.lifetime_custody.phase(), ExitCustodyPhase::Started);
        assert!(owner.registry.0.health.failure().is_some());
        // This intentional destructor defect cannot retire. Retain its actual
        // Lifetime owner as production must, rather than forging fixture exit
        // or silently dropping its still-required parent obligation.
        let _retained_original = std::mem::ManuallyDrop::new(owner);
    }

    #[tokio::test]
    async fn bounded_sweeps_reach_older_ready_work_and_quiesce_with_newer_live_work() {
        let mut owner = ParticipationWorkTestOwner::new();
        let older = owner.runtime_filter_sender();
        let older_id = *owner
            .registry
            .0
            .state
            .lock()
            .unwrap()
            .entries
            .last_key_value()
            .unwrap()
            .0;
        let mut newer: Vec<_> = (0..4).map(|_| owner.runtime_filter_sender()).collect();
        drop(older);
        // Visit the started instance, then the older ready child. Four newer
        // live children exceed this budget but cannot consume every turn.
        assert_eq!(owner.registry.retire_ready(1), Ok(0));
        let frozen_upper = owner.registry.0.state.lock().unwrap().scan_upper;
        newer.push(owner.runtime_filter_sender());
        assert_eq!(
            owner.registry.0.state.lock().unwrap().scan_upper,
            frozen_upper
        );
        assert_eq!(owner.registry.retire_ready(1), Ok(1));
        assert!(
            !owner
                .registry
                .0
                .state
                .lock()
                .unwrap()
                .entries
                .contains_key(&older_id)
        );
        let pending = || {
            let state = owner.registry.0.state.lock().unwrap();
            state.scan_requested || state.scan_upper.is_some()
        };
        let mut turns = 0;
        while pending() {
            assert!(
                turns < 16,
                "a live original cannot cause an endless self-wake sweep"
            );
            owner.registry.retire_ready(1).unwrap();
            turns += 1;
        }
        // No new event: repeated advance/teardown calls must not reopen a pass.
        for _ in 0..4 {
            assert_eq!(owner.registry.retire_ready(1), Ok(0));
            assert!(!pending());
        }
        assert_eq!(owner.registry.0.state.lock().unwrap().entries.len(), 6);
        drop(newer);
        owner.finish().await;
    }

    struct BlockingDrop {
        started: Option<tokio::sync::oneshot::Sender<()>>,
        released: Arc<(Mutex<bool>, Condvar)>,
        destroyed: Arc<AtomicBool>,
    }
    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            self.started.take().unwrap().send(()).unwrap();
            let (lock, changed) = &*self.released;
            let (released, _) = changed
                .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(3), |released| {
                    !*released
                })
                .unwrap();
            assert!(*released, "test must release the actual capture destructor");
            self.destroyed.store(true, Ordering::SeqCst);
        }
    }
    struct PendingSender {
        started: Option<tokio::sync::oneshot::Sender<()>>,
        _capture: BlockingDrop,
    }
    impl Future for PendingSender {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            if let Some(started) = self.started.take() {
                started.send(()).unwrap();
            }
            Poll::Pending
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborted_sender_cannot_finish_while_its_actual_capture_destructor_is_live() {
        let mut owner = ParticipationWorkTestOwner::new();
        let (work, panic, wake) = owner.runtime_filter_sender().into_parts();
        let custody = owner
            .registry
            .0
            .state
            .lock()
            .unwrap()
            .entries
            .last_key_value()
            .unwrap()
            .1
            .custody
            .clone();
        let (poll_tx, poll_rx) = tokio::sync::oneshot::channel();
        let (drop_tx, drop_rx) = tokio::sync::oneshot::channel();
        let released = Arc::new((Mutex::new(false), Condvar::new()));
        let destroyed = Arc::new(AtomicBool::new(false));
        let future = PendingSender {
            started: Some(poll_tx),
            _capture: BlockingDrop {
                started: Some(drop_tx),
                released: released.clone(),
                destroyed: destroyed.clone(),
            },
        };
        let task = tokio::spawn(async move {
            let _wake = TestWorkNotify(wake);
            run_future(work, future, panic).await
        });
        poll_rx.await.unwrap();
        assert!(!owner.poll_finish().unwrap());
        task.abort();
        drop_rx.await.unwrap();
        assert!(!destroyed.load(Ordering::SeqCst));
        assert!(!custody.captures_exited());
        assert!(!owner.poll_finish().unwrap());
        {
            let (lock, changed) = &*released;
            *lock.lock().unwrap() = true;
            changed.notify_all();
        }
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(destroyed.load(Ordering::SeqCst));
        assert_eq!(custody.phase(), ExitCustodyPhase::Exited);
        owner.finish().await;
        assert_eq!(custody.phase(), ExitCustodyPhase::Retired);
    }
}
