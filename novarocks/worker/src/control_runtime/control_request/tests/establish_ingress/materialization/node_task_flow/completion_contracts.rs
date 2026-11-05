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

//! The original Task reporter/exit notice at the real Node completion owner.

use super::*;
use crate::task_completion::{
    CompletionReservationError, TaskCompletionReservation, TaskCompletionSupervisor,
};
use novarocks_execution_contract::failure::{FailureClass, FailureProjection};

#[derive(Clone, Copy)]
enum Contract {
    CaptureExit,
    ForeignIdentity,
}
pub(super) struct CompletionFixture {
    contract: Contract,
    disposal: Arc<PreparationGate>,
    body_entries: AtomicUsize,
    capture_exits: AtomicUsize,
    foreign_refusals: AtomicUsize,
}
impl CompletionFixture {
    fn new(contract: Contract) -> Self {
        Self {
            contract,
            disposal: Arc::new(PreparationGate::default()),
            body_entries: AtomicUsize::new(0),
            capture_exits: AtomicUsize::new(0),
            foreign_refusals: AtomicUsize::new(0),
        }
    }
    pub(super) fn reserve(
        &self,
        supervisor: &TaskCompletionSupervisor,
        descriptor: &TaskDescriptor,
        reporter: TaskStatusReporter,
        exit: crate::CompletionExitNotice,
    ) -> TaskCompletionReservation {
        let task = descriptor.identity();
        let (reporter, exit) = if matches!(self.contract, Contract::ForeignIdentity) {
            let foreign = TaskIdentity::new(
                task.query_execution_id(),
                task.stage_id(),
                TaskId::new(999).unwrap(),
                task.backend_process_id(),
            );
            let refusal = supervisor
                .reserve_task_slot(foreign, descriptor.task_key(), reporter, exit)
                .err()
                .expect("foreign Task identity must refuse the original reporter and exit");
            let (error, reporter, exit) = refusal.into_task_parts();
            assert!(
                matches!(&error, CompletionReservationError::StatusIdentity { reserved, reporter }
                if *reserved == foreign && *reporter == task)
            );
            assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
            assert_eq!(reporter.identity(), task);
            assert_eq!(exit.identity(), task);
            self.foreign_refusals.fetch_add(1, Ordering::SeqCst);
            (reporter, exit)
        } else {
            (reporter, exit)
        };
        // Capacity is one. Retrying these exact returned originals must work:
        // the refusal cannot leave a foreign slot or consume the exit notice.
        supervisor
            .reserve_task_slot(task, descriptor.task_key(), reporter, exit)
            .unwrap()
    }
    pub(super) fn capture(self: &Arc<Self>) -> Option<CompletionCapture> {
        matches!(self.contract, Contract::CaptureExit).then(|| CompletionCapture(self.clone()))
    }
    pub(super) fn expected_state(&self) -> TaskState {
        match self.contract {
            Contract::CaptureExit => TaskState::Canceled,
            Contract::ForeignIdentity => TaskState::Finished,
        }
    }
    pub(super) fn observe_before_exit(
        &self,
        scene: &Scenario<TaskResources>,
        participation: ParticipationRef,
        task: TaskIdentity,
        resources: &TaskResources,
    ) -> Result<(), Failure> {
        if matches!(self.contract, Contract::ForeignIdentity) {
            if !wait_until(|| self.foreign_refusals.load(Ordering::SeqCst) == 1) {
                return Err(Box::new(
                    "the actual foreign completion admission was not exercised",
                ));
            }
            return Ok(());
        }
        let _release = ReleasePreparationGate(self.disposal.clone());
        if !wait_until(|| {
            self.disposal.entered() && scene.ledger.used(Dimension::Requests.id()) == 0
        }) {
            return Err(Box::new(
                "the original completion capture destructor did not enter",
            ));
        }
        let reporter = resources
            .host
            .observed
            .reporter
            .lock()
            .unwrap()
            .clone()
            .expect("the original runtime supplied its reporter");
        let before = reporter.convergence().unwrap();
        assert!(
            !before.actual_stopped() && !before.resources_converged(),
            "logical publication cannot cover the live original capture destructor"
        );
        assert_eq!(self.body_entries.load(Ordering::SeqCst), 1);
        assert_eq!(self.capture_exits.load(Ordering::SeqCst), 0);
        // The real kernel already reported completion, but the independent
        // exit notice is still held by its action capture. Cancel must reach
        // this same Core and remain its accepted conclusion after real exit.
        let mut cancel = scene
            .node
            .submit_task_command(
                participation,
                operation_number(26),
                MonotonicInstant::from_origin(Duration::from_secs(100)),
                ParticipationCommand::Cancel {
                    task,
                    reason: CancelReason::UpstreamNoLongerNeeded,
                },
            )
            .map_err(|original| Box::new(original) as Failure)?;
        let Some(delivery) = wait_delivery(&mut cancel) else {
            return Err(Box::new((
                "actual completion-capture Cancel timeout",
                cancel,
            )));
        };
        assert!(
            matches!(delivery.body(), ParticipationReplyBody::Task { outcome: OperationOutcome::Accepted, status }
            if status.identity() == task)
        );
        drop(delivery);
        drop(cancel);
        let accepted = reporter.current().unwrap();
        assert_eq!(accepted.identity(), task);
        assert_eq!(accepted.state(), TaskState::Canceling);
        let during = reporter.convergence().unwrap();
        assert!(!during.actual_stopped() && !during.resources_converged());
        self.disposal.release();
        if !wait_until(|| {
            reporter.convergence().is_ok_and(|value| {
                value.actual_stopped() && value.resources_converged() && value.conclusion_stable()
            })
        }) {
            return Err(Box::new(
                "the original completion exit did not reach Core after real capture destruction",
            ));
        }
        let after = reporter.current().unwrap();
        assert_eq!(after.identity(), accepted.identity());
        assert_eq!(
            after.termination(),
            accepted.termination(),
            "completion exit never replaces the accepted Task conclusion"
        );
        assert_eq!(after.state(), TaskState::Canceled);
        assert_eq!(self.capture_exits.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
pub(super) struct CompletionCapture(Arc<CompletionFixture>);
impl CompletionCapture {
    pub(super) fn observe_body(&self) {
        self.0.body_entries.fetch_add(1, Ordering::SeqCst);
    }
}
impl Drop for CompletionCapture {
    fn drop(&mut self) {
        self.0.disposal.wait();
        self.0.capture_exits.fetch_add(1, Ordering::SeqCst);
    }
}
fn start(
    root: &mut ProcessControlOwner,
    contract: Contract,
) -> Result<(TaskIoPorts, TaskResources), Failure> {
    let (ports, resources) = start_tasks(root)?;
    let previous = resources
        .host
        .completion_contract
        .lock()
        .unwrap()
        .replace(Arc::new(CompletionFixture::new(contract)));
    assert!(previous.is_none());
    Ok((ports, resources))
}
#[test]
fn native_exit_evidence_follows_the_actual_action_capture_destruction() {
    run_node_lifecycle_with_task(
        1,
        NodeClose::Abort,
        compiled_task_geometry(),
        |root| start(root, Contract::CaptureExit),
        exercise_unread_root,
        shutdown,
    );
}
#[test]
fn an_exit_reporter_for_another_task_is_refused_before_admission() {
    run_node_lifecycle_with_task(
        1,
        NodeClose::Release,
        compiled_task_geometry(),
        |root| start(root, Contract::ForeignIdentity),
        exercise,
        shutdown,
    );
}
