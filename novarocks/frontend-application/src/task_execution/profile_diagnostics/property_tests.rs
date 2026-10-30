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

//! Original final-info Future custody and accepted terminal-frontier regressions.

use futures::future::BoxFuture;
use std::collections::{BTreeSet, VecDeque};
use std::future::poll_fn;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use super::TaskFinalInfoCollector;
use crate::native::fragment_transport::{
    DynamicFilterRead, DynamicFilterReadError, FinalTaskInfoRead, TaskResultTransport,
};
use crate::task_execution::intent::{AckPayload, OperationAcknowledgement, OperationIntent};
use crate::task_execution::status_intake::TestIntakeWake;
use novarocks_execution_contract::{
    DomainVersion, OperationKind, OperationOutcome, QueryContextReceipt, QueryContextState,
    ReleaseOutcome, TaskIdentity,
};

#[derive(Default)]
struct FinalReadProbe {
    started: Mutex<Vec<TaskIdentity>>,
    completed: Mutex<Vec<TaskIdentity>>,
    dropped: Mutex<Vec<TaskIdentity>>,
}
struct FinalReadDrop {
    identity: TaskIdentity,
    probe: Arc<FinalReadProbe>,
}
impl Drop for FinalReadDrop {
    fn drop(&mut self) {
        self.probe.dropped.lock().unwrap().push(self.identity);
    }
}

struct GatedFinalInfo {
    release: Mutex<VecDeque<tokio::sync::oneshot::Receiver<()>>>,
    probe: Arc<FinalReadProbe>,
}
impl TaskResultTransport for GatedFinalInfo {
    fn final_task_info(
        &self,
        identity: TaskIdentity,
    ) -> BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
        // Taking the one actual receiver detects a duplicate read even if
        // a previous borrowed wait was cancelled and rebuilt.
        let release = self
            .release
            .lock()
            .unwrap()
            .pop_front()
            .expect("one receiver for each original read");
        self.probe.started.lock().unwrap().push(identity);
        let actual_drop = FinalReadDrop {
            identity,
            probe: Arc::clone(&self.probe),
        };
        Box::pin(async move {
            let _actual_drop = actual_drop;
            release
                .await
                .map_err(|_| "the fixture release gate closed".to_owned())?;
            self.probe.completed.lock().unwrap().push(identity);
            // Missing optional diagnostics must not be fabricated as a
            // complete final-info value for this or any unread Task.
            Ok(FinalTaskInfoRead::Unavailable(OperationOutcome::Gone))
        })
    }
    fn dynamic_filters(
        &self,
        _: TaskIdentity,
        _: Option<DomainVersion>,
    ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
        panic!("a final-info collector must not fetch dynamic filters")
    }
}

#[tokio::test]
async fn pending_final_info_survives_borrowed_wait_cancel_and_stop_starting_until_same_future_drops()
 {
    let (mut round, acks, release) =
        crate::task_execution::tests::manifest_normal_release_ack_regression_fixture();
    let terminal_identities = round
        .execution()
        .graph()
        .stages()
        .flat_map(|stage| {
            round
                .execution()
                .stage(stage.stage_id())
                .unwrap()
                .tasks()
                .map(|(_, task)| {
                    assert!(
                        task.is_terminal(),
                        "the original fixture already observed terminal Tasks"
                    );
                    task.identity()
                })
        })
        .collect::<Vec<_>>();
    assert!(
        terminal_identities.len() > 1,
        "the original fixture must leave an unread sibling"
    );
    assert!(
        !round.attempt_drained(),
        "the original Release ACK has not arrived"
    );
    let original_cause = round.failure_cause().cloned();
    let (release_read, read_gate) = tokio::sync::oneshot::channel();
    let probe = Arc::new(FinalReadProbe::default());
    let transport: Arc<dyn TaskResultTransport> = Arc::new(GatedFinalInfo {
        release: Mutex::new(VecDeque::from([read_gate])),
        probe: Arc::clone(&probe),
    });
    // Poll the actual collector directly on the original TaskRound fixture.
    // No coordinator runtime bridge or replacement source is constructed.
    let mut collector =
        TaskFinalInfoCollector::new(transport, true, &round, Arc::new(TestIntakeWake::default()));
    let mut wait = Box::pin(poll_fn(|cx| {
        collector.poll_observe(&round, cx);
        if collector.is_closed() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }));
    // This is an actual Pending poll of the same original oneshot read.
    // No elapsed timeout is used as a stand-in for a cleanup receipt.
    assert!(futures::poll!(wait.as_mut()).is_pending());
    drop(wait);
    let started = probe.started.lock().unwrap().clone();
    assert_eq!(started.len(), 1);
    assert!(terminal_identities.contains(&started[0]));
    assert!(probe.completed.lock().unwrap().is_empty());
    assert!(
        probe.dropped.lock().unwrap().is_empty(),
        "dropping a borrowing wait must retain the read"
    );
    assert!(!collector.is_closed());
    assert!(!collector.all_terminal_observed());

    collector.stop_starting();
    let mut stopped_wait = Box::pin(poll_fn(|cx| {
        collector.poll_observe(&round, cx);
        if collector.is_closed() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }));
    assert!(futures::poll!(stopped_wait.as_mut()).is_pending());
    drop(stopped_wait);
    assert_eq!(*probe.started.lock().unwrap(), started);
    assert!(
        probe.dropped.lock().unwrap().is_empty(),
        "stop_starting must not drop an already issued read"
    );
    assert!(!collector.is_closed());

    release_read.send(()).unwrap();
    poll_fn(|cx| {
        collector.poll_observe(&round, cx);
        if collector.is_closed() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    // Further real turns cannot issue a new read after the stop cut.
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..3 {
        collector.poll_observe(&round, &mut cx);
    }
    let completed = probe.completed.lock().unwrap().clone();
    let dropped = probe.dropped.lock().unwrap().clone();
    let remaining_started = probe.started.lock().unwrap().clone();
    let closed = collector.is_closed();
    let partial_not_complete = !collector.all_terminal_observed();
    let no_fabricated_info = collector.collected.is_empty();
    let original_cause_preserved = round.failure_cause() == original_cause.as_ref();

    // Settle the same fixture's ORIGINAL Release, rather than dropping its
    // TaskRound and treating that as remote convergence.
    let OperationIntent::ReleaseQueryContext(request) = &release else {
        unreachable!("the original fixture selected its Release by kind")
    };
    acks.publish(OperationAcknowledgement::new(
        release.operation_id(),
        OperationKind::ReleaseQueryContext,
        OperationOutcome::Accepted,
        AckPayload::Release {
            receipt: QueryContextReceipt::new(
                request.context(),
                QueryContextState::TerminalRetained,
            ),
            outcome: ReleaseOutcome::Released,
            runtime_filter: None,
        },
    ));
    round.turn().expect("the original Release ACK settles");
    assert!(round.attempt_drained());
    assert_eq!(
        remaining_started, started,
        "no unread sibling was newly dispatched"
    );
    assert_eq!(
        completed, started,
        "the originally issued read actually completed"
    );
    assert_eq!(
        dropped, started,
        "that same original future actually dropped once"
    );
    assert!(closed);
    assert!(
        partial_not_complete,
        "one optional missing result cannot prove every Task was observed"
    );
    assert!(no_fabricated_info);
    assert!(original_cause_preserved);
}

#[tokio::test]
async fn terminal_frontier_reads_each_original_task_once_and_wakes_each_ready_successor() {
    use std::future::Future;
    let (mut round, acks, release) =
        crate::task_execution::tests::manifest_normal_release_ack_regression_fixture();
    let frozen = round
        .execution()
        .graph()
        .tasks()
        .map(|task| task.identity())
        .collect::<BTreeSet<_>>();
    // Diagnostic requests follow the original accepted terminal frontier.
    // Their order is independent of planner ordinals, but coverage is exact.
    let identities = round.execution().accepted_terminal_tasks().to_vec();
    assert_eq!(identities.iter().copied().collect::<BTreeSet<_>>(), frozen);
    assert!(identities.len() > 1);
    assert!(identities.iter().all(|identity| {
        round
            .execution()
            .task(identity.task_id())
            .unwrap()
            .is_terminal()
    }));
    let mut releases = VecDeque::new();
    let mut gates = VecDeque::new();
    for _ in &identities {
        let (release, gate) = tokio::sync::oneshot::channel();
        releases.push_back(release);
        gates.push_back(gate);
    }
    let probe = Arc::new(FinalReadProbe::default());
    let transport: Arc<dyn TaskResultTransport> = Arc::new(GatedFinalInfo {
        release: Mutex::new(gates),
        probe: Arc::clone(&probe),
    });
    let wake = Arc::new(TestIntakeWake::default());
    let mut collector =
        TaskFinalInfoCollector::new(Arc::clone(&transport), true, &round, wake.clone());
    let mut cx = Context::from_waker(Waker::noop());
    for (index, identity) in identities.iter().enumerate() {
        collector.poll_observe(&round, &mut cx);
        assert_eq!(*probe.started.lock().unwrap(), identities[..=index]);
        assert_eq!(*probe.completed.lock().unwrap(), identities[..index]);
        assert_eq!(collector.unread.len(), identities.len() - index - 1);
        assert!(!collector.is_closed());
        assert!(!collector.all_terminal_observed());
        // This one actual read stays pending across repeated turns and
        // cannot start the next read or consume another receiver.
        collector.poll_observe(&round, &mut cx);
        assert_eq!(probe.started.lock().unwrap().last(), Some(identity));
        assert_eq!(probe.started.lock().unwrap().len(), index + 1);
        let notified = wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        releases.pop_front().unwrap().send(()).unwrap();
        collector.poll_observe(&round, &mut cx);
        let successor_woken =
            poll_fn(|cx| Poll::Ready(notified.as_mut().poll(cx).is_ready())).await;
        assert_eq!(successor_woken, index + 1 < identities.len());
        assert_eq!(*probe.completed.lock().unwrap(), identities[..=index]);
        assert_eq!(*probe.dropped.lock().unwrap(), identities[..=index]);
        assert!(collector.is_closed());
    }
    assert!(collector.all_terminal_observed());
    assert!(
        collector.collected.is_empty(),
        "Unavailable is never fabricated as Available"
    );
    collector.poll_observe(&round, &mut cx);
    assert_eq!(*probe.started.lock().unwrap(), identities);
    let mut disabled = TaskFinalInfoCollector::new(transport, false, &round, wake);
    disabled.poll_observe(&round, &mut cx);
    assert_eq!(
        disabled.unread.len(),
        0,
        "non-profile attempts allocate no task frontier"
    );
    assert!(!disabled.all_terminal_observed());
    assert_eq!(*probe.started.lock().unwrap(), identities);
    let OperationIntent::ReleaseQueryContext(request) = &release else {
        unreachable!()
    };
    acks.publish(OperationAcknowledgement::new(
        release.operation_id(),
        OperationKind::ReleaseQueryContext,
        OperationOutcome::Accepted,
        AckPayload::Release {
            receipt: QueryContextReceipt::new(
                request.context(),
                QueryContextState::TerminalRetained,
            ),
            outcome: ReleaseOutcome::Released,
            runtime_filter: None,
        },
    ));
    round.turn().expect("the same original Release ACK settles");
    assert!(round.attempt_drained());
}
