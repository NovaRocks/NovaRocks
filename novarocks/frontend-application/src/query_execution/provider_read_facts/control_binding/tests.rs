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

//! The original ordinary-read failure guard and actual corrected Source gate.
use super::*;
use crate::query_execution::native_execution_adapter::source_cohort::tests::{
    drive, finish, fixture,
};
use crate::task_execution::blocking_io::ConnectorBlockingIoResponsibility;
use novarocks_spi::connector::read_stack::ConnectorReadRequestControl;
use novarocks_spi::connector::{ConnectorPlanningContext, SharedDependency};
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;

struct OriginalReadFactory {
    selected: SharedDependency,
    visits: AtomicUsize,
}
impl ConnectorReadRequestControlFactory for OriginalReadFactory {
    fn visit_planning_dependencies(
        &self,
        visit: &mut dyn FnMut(&SharedDependency) -> Result<(), ConnectorError>,
    ) -> Result<(), ConnectorError> {
        self.visits.fetch_add(1, Ordering::SeqCst);
        visit(&self.selected)
    }
    fn for_planning(
        &self,
        _: &ConnectorPlanningContext,
    ) -> Result<ConnectorReadRequestControl, ConnectorError> {
        panic!(
            "no request control or physical preparation before original binding and child admission"
        )
    }
}
fn selected_factory() -> OriginalReadFactory {
    let selected = url::Url::parse("http://actual-read-catalog.example:8181/v1").unwrap();
    OriginalReadFactory {
        selected: SharedDependency::Network(
            novarocks_spi::connector::NetworkServiceOrigin::from_selected_url(&selected).unwrap(),
        ),
        visits: AtomicUsize::new(0),
    }
}

#[tokio::test]
async fn unbound_ordinary_read_parent_is_bound_by_its_original_provider_before_child() {
    let (control, mut driver, capacity) = fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    let window = capacity
        .try_acquire(&scope, ResultWindowClass::Client)
        .unwrap();
    let alias = window.retain_alias();
    // This is the exact unchanged parent-empty-D guard reached by the real
    // plain SELECT completion path before this fix, not a mocked error.
    let mut unbound = ConnectorBlockingIoResponsibility::request(&scope, &alias).unwrap();
    assert!(matches!(
        unbound.poll_ready(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(WorkError::Conflict))
    ));
    drop(unbound);
    let mut original = ProviderCreatorControlBinding::default();
    let factory = selected_factory();
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        match original.poll(&scope, &factory, &mut cx) {
            Poll::Pending => drive(&mut driver),
            Poll::Ready(result) => {
                result.unwrap();
                break;
            }
        }
    }
    assert_eq!(factory.visits.load(Ordering::SeqCst), 1);
    assert_eq!(
        control.snapshot().scopes.len(),
        1,
        "binding is not child/effect authority"
    );
    let mut child = ConnectorBlockingIoResponsibility::request(&scope, &alias).unwrap();
    let actual = loop {
        match child.poll_ready(&mut cx) {
            Poll::Pending => drive(&mut driver),
            Poll::Ready(result) => break result.unwrap(),
        }
    };
    let child_id = actual.scope().unwrap().id();
    assert_eq!(
        control
            .snapshot()
            .scopes
            .iter()
            .find(|s| s.id == child_id)
            .unwrap()
            .parent,
        Some(scope.id())
    );
    drop(actual);
    original.after_source_exit();
    drop((child, original, alias, window, capacity, scope));
    finish(control, root, driver);
}

#[test]
fn original_provider_binding_keeps_same_admission_with_other_builders_live() {
    let (control, mut driver, capacity) = fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    let blockers = [
        scope.begin_control_binding().unwrap(),
        scope.begin_control_binding().unwrap(),
    ];
    let factory = selected_factory();
    let mut original = ProviderCreatorControlBinding::default();
    let wake = std::sync::Arc::new(ChildWake::default());
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(original.poll(&scope, &factory, &mut cx).is_pending());
    assert!(original.wait.is_none() && original.proposal.is_none() && original.admission.is_some());
    assert_eq!(factory.visits.load(Ordering::SeqCst), 1);
    wake.0.store(0, Ordering::SeqCst);
    for _ in 0..16 {
        assert!(original.poll(&scope, &factory, &mut cx).is_pending());
    }
    assert_eq!(
        wake.0.load(Ordering::SeqCst),
        0,
        "a waiting reply is not a self event"
    );
    assert_eq!(factory.visits.load(Ordering::SeqCst), 1);
    assert_eq!(control.snapshot().scopes.len(), 1);
    let mut borrowed = Box::pin(std::future::poll_fn(|cx| {
        original.poll(&scope, &factory, cx)
    }));
    assert!(borrowed.as_mut().poll(&mut cx).is_pending());
    drop(borrowed);
    assert!(
        original.admission.is_some(),
        "same source owns the original request after borrowed F drop"
    );
    drive(&mut driver);
    loop {
        match original.poll(&scope, &factory, &mut cx) {
            Poll::Pending => drive(&mut driver),
            Poll::Ready(result) => {
                result.unwrap();
                break;
            }
        }
    }
    assert_eq!(factory.visits.load(Ordering::SeqCst), 1);
    assert_eq!(control.snapshot().scopes.len(), 1);
    original.after_source_exit();
    drop((original, blockers, capacity, scope));
    finish(control, root, driver);
}

#[derive(Default)]
struct ChildWake(AtomicUsize);
impl std::task::Wake for ChildWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn original_provider_child_waits_for_original_driver_without_reopening_source() {
    let (control, mut driver, capacity) = fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    let window = capacity
        .try_acquire(&scope, ResultWindowClass::Client)
        .unwrap();
    let alias = window.retain_alias();
    let factory = selected_factory();
    let mut binding = ProviderCreatorControlBinding::default();
    let wake = std::sync::Arc::new(ChildWake::default());
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    for _ in 0..100_000 {
        match binding.poll(&scope, &factory, &mut cx) {
            Poll::Pending => drive(&mut driver),
            Poll::Ready(result) => {
                result.unwrap();
                break;
            }
        }
    }
    assert!(binding.bound);
    drive(&mut driver);
    // These other original builders remain live throughout child admission.
    // They grant no child authority and impose no separate proposal quota.
    let blockers = [
        scope.begin_control_binding().unwrap(),
        scope.begin_control_binding().unwrap(),
    ];
    let mut child = ConnectorBlockingIoResponsibility::request(&scope, &alias).unwrap();
    wake.0.store(0, Ordering::SeqCst);
    for _ in 0..16 {
        assert!(child.poll_ready(&mut cx).is_pending());
    }
    assert_eq!(
        wake.0.load(Ordering::SeqCst),
        0,
        "a waiting reply is not a self event"
    );
    assert_eq!(factory.visits.load(Ordering::SeqCst), 1);
    assert_eq!(control.snapshot().scopes.len(), 1);
    let mut borrowed = Box::pin(child.ready());
    assert!(borrowed.as_mut().poll(&mut cx).is_pending());
    drop(borrowed);
    // Dropping a borrowed observer does not drive or replace the original
    // request. Only the SAME driver can publish its admission result.
    assert!(child.poll_ready(&mut cx).is_pending());
    assert_eq!(control.snapshot().scopes.len(), 1);
    drive(&mut driver);
    let mut result = None;
    for _ in 0..100_000 {
        match child.poll_ready(&mut cx) {
            Poll::Pending => drive(&mut driver),
            Poll::Ready(ready) => {
                result = Some(ready.unwrap());
                break;
            }
        }
    }
    let actual = result.expect("same original child receives its positive receipt");
    let actual_scope = actual.scope().unwrap();
    let child_window = actual.window().unwrap();
    assert!(child_window.is_for_scope(&actual_scope));
    assert_eq!(factory.visits.load(Ordering::SeqCst), 1);
    let snapshot = control.snapshot();
    assert_eq!(snapshot.scopes.len(), 2);
    assert_eq!(
        snapshot
            .scopes
            .iter()
            .find(|s| s.id == actual_scope.id())
            .unwrap()
            .parent,
        Some(scope.id())
    );
    // The original responsibility/window still controls actual lifetime after
    // Ready; removing the observer does not synthesize a joined exit.
    let backing = actual.retain_backing();
    drop((actual, child, child_window));
    drive(&mut driver);
    assert!(
        control
            .snapshot()
            .scopes
            .iter()
            .any(|s| s.id == actual_scope.id())
    );
    drop(backing);
    drive(&mut driver);
    assert!(
        !control
            .snapshot()
            .scopes
            .iter()
            .any(|s| s.id == actual_scope.id())
    );
    binding.after_source_exit();
    drop((
        binding,
        blockers,
        actual_scope,
        alias,
        window,
        capacity,
        scope,
    ));
    finish(control, root, driver);
}

#[test]
fn original_provider_child_waiting_reply_keeps_exact_parent_cancellation() {
    let (control, mut driver, capacity) = fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    crate::query_execution::native_execution_adapter::source_cohort::tests::bind(
        &scope,
        &mut driver,
    );
    drive(&mut driver);
    let window = capacity
        .try_acquire(&scope, ResultWindowClass::Client)
        .unwrap();
    let alias = window.retain_alias();
    let blockers = [
        scope.begin_control_binding().unwrap(),
        scope.begin_control_binding().unwrap(),
    ];
    let mut child = ConnectorBlockingIoResponsibility::request(&scope, &alias).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(child.poll_ready(&mut cx).is_pending());
    root.owner
        .cancel(novarocks_workload_control::CancellationReason::Requested);
    assert!(matches!(
        scope.check(),
        Err(WorkError::Cancelled(
            novarocks_workload_control::CancellationReason::Requested
        ))
    ));
    // The existing request is Waiting, not an old proposal-slot wait. Drive
    // its original owner to deliver the exact parent cancellation; no child
    // may be installed even while the other original builders remain live.
    drive(&mut driver);
    assert!(matches!(
        child.poll_ready(&mut cx),
        Poll::Ready(Err(WorkError::Cancelled(
            novarocks_workload_control::CancellationReason::Requested
        )))
    ));
    assert_eq!(control.snapshot().scopes.len(), 1);
    drop((child, blockers, alias, window, capacity, scope));
    finish(control, root, driver);
}

#[test]
fn factory_refusal_keeps_original_connector_source_and_fact_in_the_same_binding() {
    use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
    struct RefusingFactory {
        error: ConnectorError,
        visits: AtomicUsize,
    }
    impl ConnectorReadRequestControlFactory for RefusingFactory {
        fn visit_planning_dependencies(
            &self,
            _: &mut dyn FnMut(&SharedDependency) -> Result<(), ConnectorError>,
        ) -> Result<(), ConnectorError> {
            self.visits.fetch_add(1, Ordering::SeqCst);
            Err(self.error.clone())
        }
        fn for_planning(
            &self,
            _: &ConnectorPlanningContext,
        ) -> Result<ConnectorReadRequestControl, ConnectorError> {
            panic!("a refused original factory cannot dispatch a read")
        }
    }
    let (control, mut driver, capacity) = fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    // Deliberately distinct outer kind: only the original producer fact may
    // choose Control, never a facade label or the resulting diagnostic.
    let error = ConnectorError::from_domain(
        ConnectorErrorKind::ResourceExhausted,
        WorkError::Cancelled(novarocks_workload_control::CancellationReason::Requested),
    );
    let cause = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<SharedFailureCause>())
        .unwrap()
        .clone();
    let factory = RefusingFactory {
        error,
        visits: AtomicUsize::new(0),
    };
    let mut binding = ProviderCreatorControlBinding::default();
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..2 {
        let refusal = match binding.poll(&scope, &factory, &mut cx) {
            Poll::Ready(Err(error)) => error,
            _ => panic!("the actual selected factory refused before child admission"),
        };
        assert_eq!(refusal.fact(), Some(cause.failure_fact()));
        assert_eq!(refusal.fact().unwrap().class(), FailureClass::Control);
        let same = refusal
            .into_original_cause()
            .expect("actual producer failure");
        assert_eq!(same, cause);
    }
    assert_eq!(
        factory.visits.load(Ordering::SeqCst),
        1,
        "no replay after the source cached its refusal"
    );
    assert_eq!(control.snapshot().scopes.len(), 1);
    binding.after_source_exit();
    drop((binding, scope, factory, capacity));
    drive(&mut driver);
    finish(control, root, driver);
}
