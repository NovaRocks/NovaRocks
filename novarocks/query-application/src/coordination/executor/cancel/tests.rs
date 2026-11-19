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

use super::*;
use crate::api::logical_runtime::LogicalHostFact;
use novarocks_logical_execution_protocol::LogicalSourceId;
use novarocks_workload_control::{
    ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

struct Source(Arc<Mutex<Vec<LogicalHostFact>>>);
impl LogicalFactSource for Source {
    fn id(&self) -> LogicalSourceId {
        LogicalSourceId::new(NonZeroU64::new(1).unwrap())
    }
    fn send(
        &mut self,
        fact: LogicalHostFact,
    ) -> LogicalFuture<'_, Result<(), LogicalSourceClosed>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(fact);
            Ok(())
        })
    }
}
fn control() -> WorkloadControl {
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 128,
            control_bytes: 16,
            per_scope_bytes: 112,
        },
    )
    .unwrap();
    control.mark_ready().unwrap();
    control
}

#[tokio::test]
async fn stop_wakes_original_observer_without_cancelling_work() {
    let control = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let view = root.owner.scope().cancellation().unwrap();
    let observer = CancelObserver::new(view.clone());
    let facts = Arc::new(Mutex::new(Vec::new()));
    let mut source = Source(facts.clone());
    let mut observed = observer.observe(&mut source);
    assert!(futures::poll!(&mut observed).is_pending());
    observer.stop();
    assert_eq!(observed.await, Ok(()));
    assert_eq!(view.reason(), None);
    assert!(facts.lock().unwrap().is_empty());
    root.owner.complete();
    root.business.release();
}

#[tokio::test]
async fn sealed_success_still_needs_observer_stop_for_actual_exit() {
    let control = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let view = root.owner.scope().cancellation().unwrap();
    root.owner.success_sealer().seal().unwrap();
    let observer = CancelObserver::new(view.clone());
    let mut source = Source(Arc::new(Mutex::new(Vec::new())));
    let mut observed = observer.observe(&mut source);
    assert!(futures::poll!(&mut observed).is_pending());
    observer.stop();
    assert_eq!(observed.await, Ok(()));
    assert_eq!(view.reason(), None);
    root.owner.complete();
    root.business.release();
}

#[tokio::test]
async fn original_first_winner_is_forwarded_once() {
    let control = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let observer = CancelObserver::new(root.owner.scope().cancellation().unwrap());
    root.owner.cancel(CancellationReason::ClientCancelRequested);
    root.owner.cancel(CancellationReason::ServerShutdown);
    let facts = Arc::new(Mutex::new(Vec::new()));
    let mut source = Source(facts.clone());
    assert_eq!(observer.observe(&mut source).await, Ok(()));
    assert_eq!(
        observer.observe(&mut source).await,
        Err(LogicalSourceClosed)
    );
    assert!(matches!(
        facts.lock().unwrap().as_slice(),
        [LogicalHostFact::Cancelled(
            LogicalCancellationReason::ClientCancelRequested
        )]
    ));
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
}

#[test]
fn cancellation_projection_preserves_all_reason_categories() {
    use CancellationReason as R;
    use LogicalCancellationReason as L;
    for (original, expected) in [
        (R::Requested, L::Requested),
        (
            R::ExplicitKill {
                requester_session: 7,
            },
            L::ExplicitKill,
        ),
        (
            R::ExplicitKillConnection {
                requester_session: 9,
            },
            L::ExplicitKillConnection,
        ),
        (R::ClientCancelRequested, L::ClientCancelRequested),
        (R::ClientDisconnected, L::ClientDisconnected),
        (R::DeadlineExceeded, L::DeadlineExceeded),
        (
            R::FrontendDrainDeadlineExceeded,
            L::FrontendDrainDeadlineExceeded,
        ),
        (R::ServerShutdown, L::ServerShutdown),
        (R::OwnerDropped, L::OwnerDropped),
    ] {
        assert_eq!(project_reason(original), expected);
    }
}

#[tokio::test]
async fn stop_does_not_forge_delivery_for_selected_pending_cancellation() {
    struct ClosingSource(Arc<std::sync::atomic::AtomicBool>);
    impl LogicalFactSource for ClosingSource {
        fn id(&self) -> LogicalSourceId {
            LogicalSourceId::new(NonZeroU64::new(1).unwrap())
        }
        fn send(
            &mut self,
            _fact: LogicalHostFact,
        ) -> LogicalFuture<'_, Result<(), LogicalSourceClosed>> {
            Box::pin(std::future::poll_fn(|_| {
                if self.0.load(Ordering::Acquire) {
                    std::task::Poll::Ready(Err(LogicalSourceClosed))
                } else {
                    std::task::Poll::Pending
                }
            }))
        }
    }
    let control = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let observer = CancelObserver::new(root.owner.scope().cancellation().unwrap());
    root.owner.cancel(CancellationReason::Requested);
    let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut source = ClosingSource(closed.clone());
    let mut observed = observer.observe(&mut source);
    assert!(futures::poll!(&mut observed).is_pending());
    observer.stop();
    assert!(futures::poll!(&mut observed).is_pending());
    // Only the original carrier source close settles its selected send.
    closed.store(true, Ordering::Release);
    assert_eq!(observed.await, Err(LogicalSourceClosed));
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
}
