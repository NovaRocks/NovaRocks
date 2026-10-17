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

//! One declared ACK-loss test owns one finite, exact remote input pause.
//! This never holds terminal publication or reopens a closed input queue.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use novarocks_execution::connector::TaskAttemptSplitQueues;
use novarocks_execution::connector::scan_queue::TestSplitConsumptionGate;
use novarocks_execution_contract::{TaskDomainUpdate, TaskIdentity, TaskOperationId, UpdateTask};
use novarocks_failpoint::scope::{ControlCancellation, ProcessBinding};
use novarocks_spi::connector::ConnectorStopView;
use novarocks_worker::TestTaskUpdateConsumptionReservation;
use tokio::sync::Notify;

use crate::test_controls::{NativeFaultClaim, NativeTestControls};

type NodeRange = (i32, u64, u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Reserved,
    Armed,
    Released,
}

struct State {
    phase: Phase,
    duplicates: BTreeMap<NodeRange, bool>,
}

pub(super) struct TaskUpdateReplayGate {
    identity: TaskIdentity,
    operation: TaskOperationId,
    state: Mutex<State>,
    inputs: Vec<Arc<TestSplitConsumptionGate>>,
    control: ControlCancellation,
    task_stop: ConnectorStopView,
    claim: Mutex<Option<NativeFaultClaim>>,
    released: Arc<Notify>,
    monitor: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl TaskUpdateReplayGate {
    pub(super) fn reserve<S: novarocks_execution::connector::ScheduledSplitFacts>(
        request: &UpdateTask,
        queues: &TaskAttemptSplitQueues<S>,
        control: ControlCancellation,
        task_stop: ConnectorStopView,
    ) -> Result<Arc<Self>, String> {
        if control.process() != ProcessBinding::Backend(request.identity().backend_process_id())
            || !control.is_active()
            || task_stop.is_stopped()
        {
            return Err("replay gate has no live control for the actual task process".into());
        }
        let mut duplicates = BTreeMap::new();
        let mut inputs: Vec<Arc<TestSplitConsumptionGate>> = Vec::new();
        for domain in request.domains() {
            let TaskDomainUpdate::SplitAssignment(intent) = domain else {
                continue;
            };
            let Some((first, last)) = intent.offer().range().filter(|_| intent.no_more_splits())
            else {
                continue;
            };
            let node = intent.node().get();
            if duplicates
                .insert((node, first.get(), last.get()), false)
                .is_some()
            {
                for gate in &inputs {
                    gate.release();
                }
                return Err("replay gate repeats a terminal input range".into());
            }
            match queues.queue(node).reserve_test_consumption_gate() {
                Ok(gate) => inputs.push(gate),
                Err(error) => {
                    for gate in &inputs {
                        gate.release();
                    }
                    return Err(error);
                }
            }
        }
        if inputs.is_empty() {
            return Err("replay gate requires an actual nonempty terminal split assignment".into());
        }
        Ok(Arc::new(Self {
            identity: request.identity(),
            operation: request.envelope().operation_id(),
            state: Mutex::new(State {
                phase: Phase::Reserved,
                duplicates,
            }),
            inputs,
            control,
            task_stop,
            claim: Mutex::new(None),
            released: Arc::new(Notify::new()),
            monitor: Mutex::new(None),
        }))
    }

    pub(super) fn is_held(&self) -> bool {
        self.state.lock().expect("replay gate state lock").phase != Phase::Released
            && self.inputs.iter().all(|input| input.is_held())
    }

    fn activate(self: &Arc<Self>, claim: NativeFaultClaim) -> Result<(), String> {
        if claim.execution_id != self.identity.query_execution_id()
            || claim.process_id != self.identity.backend_process_id()
            || !claim.requires_replay_barrier()
            || !self.control.is_active()
            || self.task_stop.is_stopped()
            || claim.action_ended()?
        {
            return Err(
                "replay gate activation does not match a live accepted ACK-drop claim".into(),
            );
        }
        {
            let mut state = self.state.lock().expect("replay gate state lock");
            if state.phase != Phase::Reserved || !self.inputs.iter().all(|input| input.is_held()) {
                return Err("reserved replay input ended before actual ACK-drop activation".into());
            }
            *self.claim.lock().expect("replay gate claim lock") = Some(claim);
            state.phase = Phase::Armed;
            // A concurrent exact duplicate may reach the real queue between
            // acceptance and activation. It is evidence, never a second arm.
            if state.duplicates.values().all(|observed| *observed) {
                drop(state);
                self.release();
                return Ok(());
            }
        }
        Ok(())
    }

    fn watch(self: &Arc<Self>, runtime: &tokio::runtime::Handle) {
        let weak = Arc::downgrade(self);
        let control = self.control.clone();
        let released = Arc::clone(&self.released);
        let mut task_stopped = self.task_stop.stopped();
        // The owned task uses the original control deadline/EOF. The cadence
        // only checks action retirement; it grants no additional lifetime.
        let monitor = runtime.spawn(async move {
            let mut retirement = tokio::time::interval(Duration::from_millis(20));
            loop {
                let Some(gate) = weak.upgrade() else {
                    break;
                };
                if !gate.is_held() {
                    gate.release();
                    break;
                }
                drop(gate);
                tokio::select! {
                    _ = control.cancelled() => {
                        if let Some(gate) = weak.upgrade() { gate.release(); }
                        break;
                    }
                    _ = &mut task_stopped => {
                        if let Some(gate) = weak.upgrade() { gate.release(); }
                        break;
                    }
                    _ = released.notified() => { break; }
                    _ = retirement.tick() => {
                        let ended = weak.upgrade().is_none_or(|gate| {
                            gate.claim.lock().expect("replay gate claim lock").as_ref()
                                .is_some_and(|claim| claim.action_ended().unwrap_or(true))
                        });
                        if ended {
                            if let Some(gate) = weak.upgrade() { gate.release(); }
                            break;
                        }
                    }
                }
            }
        });
        *self.monitor.lock().expect("replay gate monitor lock") = Some(monitor);
    }

    pub(super) fn observe_duplicate(
        &self,
        identity: TaskIdentity,
        operation: TaskOperationId,
        node: i32,
        first: u64,
        last: u64,
    ) {
        if identity != self.identity || operation != self.operation {
            return;
        }
        let mut state = self.state.lock().expect("replay gate state lock");
        if state.phase == Phase::Released {
            return;
        }
        let Some(observed) = state.duplicates.get_mut(&(node, first, last)) else {
            return;
        };
        *observed = true;
        if state.phase == Phase::Armed && state.duplicates.values().all(|observed| *observed) {
            drop(state);
            self.release();
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("replay gate state lock");
        if state.phase == Phase::Released {
            return;
        }
        state.phase = Phase::Released;
        drop(state);
        for input in &self.inputs {
            input.release();
        }
        self.released.notify_one();
    }
}

impl Drop for TaskUpdateReplayGate {
    fn drop(&mut self) {
        for input in &self.inputs {
            input.release();
        }
        if let Some(monitor) = self
            .monitor
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            monitor.abort();
        }
    }
}

pub(super) struct ReplayReservation {
    gate: Arc<TaskUpdateReplayGate>,
    controls: Arc<NativeTestControls>,
    activated: bool,
}
impl ReplayReservation {
    pub(super) fn new(
        gate: Arc<TaskUpdateReplayGate>,
        controls: Arc<NativeTestControls>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        // The original deadline also bounds the pre-acceptance reservation.
        gate.watch(&runtime);
        Self {
            gate,
            controls,
            activated: false,
        }
    }
}
impl TestTaskUpdateConsumptionReservation for ReplayReservation {
    fn activate(&mut self) -> Result<(), String> {
        let claim = self
            .controls
            .claimed_task_update_replay(self.gate.identity)?;
        self.gate.activate(claim)?;
        Ok(())
    }
    fn acknowledgement_dropped(&mut self) {
        self.activated = true;
    }
}
impl Drop for ReplayReservation {
    fn drop(&mut self) {
        // Every refused/nonaccepted/unknown local outcome rolls back. Only
        // the actual accepted ACK-drop activation transfers custody to task.
        if !self.activated {
            self.gate.release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_controls::tests::{
        Controlled, bind_fixture, controlled, controlled_replay, end_fixture, task,
    };
    use novarocks_execution::connector::{
        ScheduledSplitFacts, SplitPoll, SplitQueueConfig, SplitQueueRegistry, TaskAttemptKey,
    };
    use novarocks_execution_contract::{
        CodecOwnedContent, ContentFingerprint, PlanNodeId, SplitAssignmentIntent, SplitOffer,
        SplitSequence,
    };
    use novarocks_failpoint::QueryLifecycleFaultKind;
    use novarocks_spi::connector::ConnectorStopOwner;
    use novarocks_types::UniqueId;

    #[derive(Debug)]
    struct Content;
    impl CodecOwnedContent for Content {
        fn fingerprint(&self) -> ContentFingerprint {
            ContentFingerprint::from_bytes([1; 16])
        }
        fn encoded_len(&self) -> usize {
            1
        }
    }
    #[derive(Debug)]
    struct Split(u64);
    impl ScheduledSplitFacts for Split {
        fn sequence_id(&self) -> u64 {
            self.0
        }
        fn plan_node_id(&self) -> i32 {
            0
        }
        fn retained_size_in_bytes(&self) -> u64 {
            1
        }
    }
    fn request(fixture: &Controlled) -> UpdateTask {
        UpdateTask::try_new(
            TaskOperationId::new_v7(),
            task(fixture, 2),
            vec![TaskDomainUpdate::SplitAssignment(
                SplitAssignmentIntent::new(
                    PlanNodeId::new(0).unwrap(),
                    SplitOffer::batch(
                        SplitSequence::new(1).unwrap(),
                        SplitSequence::new(3).unwrap(),
                        true,
                    )
                    .unwrap(),
                    Arc::new(Content),
                ),
            )],
        )
        .unwrap()
    }
    fn queues(fixture: &Controlled) -> Arc<TaskAttemptSplitQueues<Split>> {
        SplitQueueRegistry::new().open_attempt(
            TaskAttemptKey::new(fixture.execution, UniqueId::new(1, 2)),
            SplitQueueConfig::default(),
        )
    }
    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }
    async fn await_release(gate: &TaskUpdateReplayGate) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while gate.is_held() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            while gate
                .monitor
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|monitor| !monitor.is_finished())
            {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("the original finite control must release the held input and its owned monitor");
    }
    fn drop_ack(fixture: &Controlled, request: &UpdateTask, reservation: &mut ReplayReservation) {
        let error = crate::task_protocol_fault::task_update_terminal_ack_dropped(
            &fixture.controls,
            request.identity(),
            novarocks_execution_contract::OperationOutcome::Accepted,
            true,
            || reservation.activate(),
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        reservation.acknowledgement_dropped();
    }

    #[test]
    fn claimed_ack_drop_keeps_real_input_until_only_its_exact_live_duplicate() {
        let fixture = controlled_replay(10_000);
        bind_fixture(&fixture, QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop);
        let request = request(&fixture);
        let target = fixture
            .controls
            .reserve_task_update_replay_target(&request)
            .unwrap()
            .unwrap();
        let queues = queues(&fixture);
        let queue = queues.queue(0);
        let gate = TaskUpdateReplayGate::reserve(
            &request,
            &queues,
            target.cancellation(),
            ConnectorStopOwner::new().view(),
        )
        .unwrap();
        let runtime = runtime();
        let mut reservation = ReplayReservation::new(
            Arc::clone(&gate),
            Arc::clone(&fixture.controls),
            runtime.handle().clone(),
        );
        queue
            .offer_splits(0, vec![Split(1), Split(2), Split(3)], true)
            .unwrap();
        drop_ack(&fixture, &request, &mut reservation);
        drop(reservation);
        drop(target);
        runtime.block_on(async {
            tokio::time::sleep(Duration::from_millis(40)).await;
        });
        // Ordinary claim consumption/ACK-drop is not action retirement.
        assert!(gate.is_held());
        assert!(matches!(queue.poll(), SplitPoll::Blocked));
        assert!(
            fixture
                .controls
                .reserve_task_update_replay_target(&request)
                .unwrap()
                .is_none(),
            "retry must not reserve a second gate"
        );
        gate.observe_duplicate(request.identity(), TaskOperationId::new_v7(), 0, 1, 3);
        gate.observe_duplicate(
            task(&fixture, 3),
            request.envelope().operation_id(),
            0,
            1,
            3,
        );
        gate.observe_duplicate(
            request.identity(),
            request.envelope().operation_id(),
            0,
            1,
            2,
        );
        assert!(gate.is_held());
        let duplicate = queue
            .offer_splits(0, vec![Split(1), Split(2), Split(3)], true)
            .unwrap();
        assert_eq!(duplicate.duplicate_sequences, vec![1, 2, 3]);
        assert!(duplicate.enqueued.is_empty());
        gate.observe_duplicate(
            request.identity(),
            request.envelope().operation_id(),
            0,
            1,
            3,
        );
        runtime.block_on(await_release(&gate));
        for expected in 1..=3 {
            let SplitPoll::Ready(split) = queue.poll() else {
                panic!("original split is consumed once");
            };
            assert_eq!(split.0, expected);
        }
        assert!(matches!(queue.poll(), SplitPoll::Exhausted));
        assert_eq!(queue.stats().duplicate_splits_skipped, 3);
    }

    #[test]
    fn a_refused_update_rolls_back_before_claiming_the_single_fault() {
        let fixture = controlled_replay(10_000);
        bind_fixture(&fixture, QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop);
        let request = request(&fixture);
        let target = fixture
            .controls
            .reserve_task_update_replay_target(&request)
            .unwrap()
            .unwrap();
        let queues = queues(&fixture);
        let gate = TaskUpdateReplayGate::reserve(
            &request,
            &queues,
            target.cancellation(),
            ConnectorStopOwner::new().view(),
        )
        .unwrap();
        let runtime = runtime();
        let mut reservation = ReplayReservation::new(
            Arc::clone(&gate),
            Arc::clone(&fixture.controls),
            runtime.handle().clone(),
        );
        crate::task_protocol_fault::task_update_terminal_ack_dropped(
            &fixture.controls,
            request.identity(),
            novarocks_execution_contract::OperationOutcome::InvalidStateOrRequest,
            true,
            || reservation.activate(),
        )
        .unwrap();
        drop(reservation);
        drop(target);
        assert!(!gate.is_held());
        assert!(
            fixture
                .controls
                .reserve_task_update_replay_target(&request)
                .unwrap()
                .is_some(),
            "refusal did not consume the once grant or its pending reservation"
        );
        assert!(
            fixture
                .controls
                .claim(
                    QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop,
                    fixture.execution,
                    fixture.process,
                    false
                )
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn a_duplicate_between_acceptance_and_activation_is_not_lost_or_a_second_claim() {
        let fixture = controlled_replay(10_000);
        bind_fixture(&fixture, QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop);
        let request = request(&fixture);
        let target = fixture
            .controls
            .reserve_task_update_replay_target(&request)
            .unwrap()
            .unwrap();
        let queues = queues(&fixture);
        let gate = TaskUpdateReplayGate::reserve(
            &request,
            &queues,
            target.cancellation(),
            ConnectorStopOwner::new().view(),
        )
        .unwrap();
        let queue = queues.queue(0);
        queue
            .offer_splits(0, vec![Split(1), Split(2), Split(3)], true)
            .unwrap();
        let repeated = queue
            .offer_splits(0, vec![Split(1), Split(2), Split(3)], true)
            .unwrap();
        assert_eq!(repeated.duplicate_sequences.len(), 3);
        let concurrent = Arc::clone(&gate);
        let identity = request.identity();
        let operation = request.envelope().operation_id();
        std::thread::spawn(move || concurrent.observe_duplicate(identity, operation, 0, 1, 3))
            .join()
            .unwrap();
        assert!(
            gate.is_held(),
            "a reserved witness cannot release before accepted activation"
        );
        let runtime = runtime();
        let mut reservation = ReplayReservation::new(
            Arc::clone(&gate),
            Arc::clone(&fixture.controls),
            runtime.handle().clone(),
        );
        drop_ack(&fixture, &request, &mut reservation);
        assert!(!gate.is_held());
        assert!(matches!(queue.poll(), SplitPoll::Ready(_)));
    }

    #[test]
    fn actual_action_end_session_eof_and_original_control_deadline_release_input() {
        for ending in [
            "action-end",
            "session-eof",
            "original-deadline",
            "task-close",
            "task-stop",
        ] {
            let mut fixture = controlled_replay(if ending == "original-deadline" {
                500
            } else {
                10_000
            });
            let binding =
                bind_fixture(&fixture, QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop);
            let request = request(&fixture);
            let target = fixture
                .controls
                .reserve_task_update_replay_target(&request)
                .unwrap()
                .unwrap();
            let queues = queues(&fixture);
            let task_stop = ConnectorStopOwner::new();
            let gate = TaskUpdateReplayGate::reserve(
                &request,
                &queues,
                target.cancellation(),
                task_stop.view(),
            )
            .unwrap();
            let runtime = runtime();
            let mut reservation = ReplayReservation::new(
                Arc::clone(&gate),
                Arc::clone(&fixture.controls),
                runtime.handle().clone(),
            );
            queues
                .queue(0)
                .offer_splits(0, vec![Split(1), Split(2), Split(3)], true)
                .unwrap();
            drop_ack(&fixture, &request, &mut reservation);
            match ending {
                "action-end" => {
                    end_fixture(
                        &fixture,
                        QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop,
                        &binding,
                    );
                }
                "session-eof" => {
                    fixture.runner.end_and_join().unwrap();
                }
                "original-deadline" => {}
                "task-close" => {
                    queues.queue(0).close();
                }
                "task-stop" => {
                    task_stop.request_stop();
                }
                _ => unreachable!(),
            }
            runtime.block_on(await_release(&gate));
            assert!(!gate.is_held());
            assert_eq!(queues.queue(0).stats().duplicate_splits_skipped, 0);
            if ending == "task-close" {
                assert!(matches!(queues.queue(0).poll(), SplitPoll::Exhausted));
            } else {
                assert!(
                    matches!(queues.queue(0).poll(), SplitPoll::Ready(_)),
                    "release does not reopen or replace input"
                );
            }
        }
    }

    #[test]
    fn the_original_deadline_also_bounds_a_reservation_not_yet_accepted() {
        let fixture = controlled_replay(500);
        bind_fixture(&fixture, QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop);
        let request = request(&fixture);
        let target = fixture
            .controls
            .reserve_task_update_replay_target(&request)
            .unwrap()
            .unwrap();
        let queues = queues(&fixture);
        let gate = TaskUpdateReplayGate::reserve(
            &request,
            &queues,
            target.cancellation(),
            ConnectorStopOwner::new().view(),
        )
        .unwrap();
        let runtime = runtime();
        let _reservation = ReplayReservation::new(
            Arc::clone(&gate),
            Arc::clone(&fixture.controls),
            runtime.handle().clone(),
        );
        runtime.block_on(await_release(&gate));
        assert!(
            fixture
                .controls
                .claimed_task_update_replay(request.identity())
                .is_err(),
            "no accepted update, so no once claim exists"
        );
    }

    #[test]
    fn failure_to_publish_the_real_fault_rolls_back_an_activated_reservation() {
        let fixture = controlled_replay(10_000);
        let binding = bind_fixture(&fixture, QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop);
        let request = request(&fixture);
        let target = fixture
            .controls
            .reserve_task_update_replay_target(&request)
            .unwrap()
            .unwrap();
        let queues = queues(&fixture);
        let gate = TaskUpdateReplayGate::reserve(
            &request,
            &queues,
            target.cancellation(),
            ConnectorStopOwner::new().view(),
        )
        .unwrap();
        let runtime = runtime();
        let mut reservation = ReplayReservation::new(
            Arc::clone(&gate),
            Arc::clone(&fixture.controls),
            runtime.handle().clone(),
        );
        let error = crate::task_protocol_fault::task_update_terminal_ack_dropped(
            &fixture.controls,
            request.identity(),
            novarocks_execution_contract::OperationOutcome::Accepted,
            true,
            || {
                reservation.activate()?;
                end_fixture(
                    &fixture,
                    QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop,
                    &binding,
                );
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        drop(reservation);
        assert!(
            !gate.is_held(),
            "a failed boundary never transfers pending-input custody"
        );
        runtime.block_on(await_release(&gate));
    }

    #[test]
    fn an_ordinary_ack_drop_has_no_consumption_authority() {
        let fixture = controlled("task-update-terminal-ack-drop");
        bind_fixture(&fixture, QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop);
        assert!(
            fixture
                .controls
                .reserve_task_update_replay_target(&request(&fixture))
                .unwrap()
                .is_none()
        );
        let request = request(&fixture);
        let activations = std::cell::Cell::new(0);
        let fault = || {
            crate::task_protocol_fault::task_update_terminal_ack_dropped(
                &fixture.controls,
                request.identity(),
                novarocks_execution_contract::OperationOutcome::Accepted,
                true,
                || {
                    // This is the ingress's unreserved ordinary-fault branch.
                    let claim = fixture
                        .controls
                        .claimed_task_update_replay(request.identity())?;
                    assert!(!claim.requires_replay_barrier());
                    activations.set(activations.get() + 1);
                    Ok(())
                },
            )
        };
        assert_eq!(fault().unwrap_err().code(), tonic::Code::DeadlineExceeded);
        fault().unwrap();
        assert_eq!(activations.get(), 1, "ordinary ACK-drop still claims once");
    }
}
