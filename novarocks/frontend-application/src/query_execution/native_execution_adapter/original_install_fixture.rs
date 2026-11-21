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

//! Passive metadata and pause for the SAME production Native installation.
//! No Ready source, M author, Work, Scope, Source or HostRegistry is retained.

use super::*;
use crate::fixtures::native_recipe::ReadCloseGate;
use crate::query_execution::lifecycle_plan::AttemptCredentialStorage;
use crate::task_execution::host::transport::{
    OriginalSendHandoff, OriginalSendHandoffFixture, OriginalSendTarget,
};
use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_execution_contract::TaskIdentity;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OriginalEstablishFixtureRefusal {
    AlreadyReleased,
    ForeignParticipation,
    AlreadyConfigured,
}

#[derive(Default)]
pub(crate) struct OriginalInstallProbe {
    isolation_finished: Mutex<Vec<novarocks_types::QueryExecutionId>>,
    rows: Mutex<Vec<Arc<OriginalInstallRow>>>,
    qualifications: Mutex<Vec<OriginalQualificationObservation>>,
    candidates: Mutex<Vec<Arc<OriginalCandidateObservation>>>,
}
pub(crate) struct OriginalInstallRow {
    pub(crate) activation: AttemptActivationIdentity,
    pub(crate) root: TaskIdentity,
    pub(crate) tasks: Vec<TaskIdentity>,
    pub(crate) participations: Vec<ParticipationRef>,
    pub(crate) credentials: Weak<AttemptCredentialStorage>,
    pub(crate) read_observation:
        Arc<crate::native::task_result_transport::OriginalRootReadObservation>,
    released: AtomicBool,
    changed: tokio::sync::Notify,
    pub(crate) admissions: Vec<(
        ParticipationRef,
        novarocks_execution_contract::ParticipationAdmissionTicketReceipt,
    )>,
    pub(crate) closed: Vec<ParticipationRef>,
    actual_exit: Mutex<Option<watch::Receiver<bool>>>,
    actual_actor: Mutex<Option<novarocks_control_runtime_core::identity::ActorId>>,
    original_disposal_gate: Mutex<Option<Weak<ReadCloseGate>>>,
    establish_target: Mutex<Option<ParticipationRef>>,
    establish_handoff: Mutex<Weak<OriginalSendHandoff>>,
}
impl OriginalInstallProbe {
    pub(crate) fn row(&self, index: usize) -> Option<Arc<OriginalInstallRow>> {
        self.rows.lock().unwrap().get(index).cloned()
    }
    pub(super) fn capture(
        &self,
        activation: AttemptActivationIdentity,
        graph: &crate::task_execution::graph::TaskGraph,
        credentials: Weak<AttemptCredentialStorage>,
        original_root_port: Weak<NativeBoundedRootReadPort>,
        admissions: &[ReplacementWorkerAdmissionEvidence],
        closed: &BTreeSet<ParticipationRef>,
    ) -> Arc<OriginalInstallRow> {
        assert_eq!(graph.execution_id(), activation.execution());
        let mut rows = self.rows.lock().unwrap();
        if let Some(row) = rows.iter().find(|row| row.activation == activation) {
            assert_eq!(row.root, graph.root_identity());
            assert_eq!(
                row.tasks,
                graph
                    .tasks()
                    .map(|task| task.identity())
                    .collect::<Vec<_>>()
            );
            assert!(Weak::ptr_eq(&row.credentials, &credentials));
            return row.clone();
        }
        let read_observation =
            Arc::new(crate::native::task_result_transport::OriginalRootReadObservation::default());
        original_root_port
            .upgrade()
            .expect("same original Root port")
            .observe_original_read_for_fixture(Arc::downgrade(&read_observation));
        let row = Arc::new(OriginalInstallRow {
            activation,
            root: graph.root_identity(),
            tasks: graph.tasks().map(|task| task.identity()).collect(),
            participations: graph.participations().copied().collect(),
            credentials,
            read_observation,
            released: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
            admissions: admissions
                .iter()
                .map(|row| (row.participation(), row.receipt()))
                .collect(),
            closed: closed.iter().copied().collect(),
            actual_exit: Mutex::new(None),
            actual_actor: Mutex::new(None),
            original_disposal_gate: Mutex::new(None),
            establish_target: Mutex::new(None),
            establish_handoff: Mutex::new(Weak::new()),
        });
        rows.push(row.clone());
        row
    }
}
impl OriginalInstallRow {
    /// Configure only the SAME preinstall row's real participation.
    pub(crate) fn hold_original_establish(
        &self,
        target: ParticipationRef,
    ) -> Result<(), OriginalEstablishFixtureRefusal> {
        let mut configured = self.establish_target.lock().unwrap();
        if self.released.load(Ordering::Acquire) {
            return Err(OriginalEstablishFixtureRefusal::AlreadyReleased);
        }
        if target.query_execution_id() != self.activation.execution()
            || !self.participations.contains(&target)
        {
            return Err(OriginalEstablishFixtureRefusal::ForeignParticipation);
        }
        if configured.is_some() {
            return Err(OriginalEstablishFixtureRefusal::AlreadyConfigured);
        }
        *configured = Some(target);
        Ok(())
    }

    pub(crate) fn original_establish_handoff(&self) -> Option<Arc<OriginalSendHandoff>> {
        self.establish_handoff.lock().unwrap().upgrade()
    }

    pub(super) fn original_send_handoff_fixture(&self) -> Option<OriginalSendHandoffFixture<'_>> {
        self.establish_target
            .lock()
            .unwrap()
            .map(|participation| OriginalSendHandoffFixture {
                target: OriginalSendTarget::Establish(participation),
                observation: &self.establish_handoff,
            })
    }

    pub(crate) fn hold_original_disposal(&self, original: &Arc<ReadCloseGate>) {
        assert!(!self.released.load(Ordering::Acquire));
        assert!(
            self.original_disposal_gate
                .lock()
                .unwrap()
                .replace(Arc::downgrade(original))
                .is_none()
        );
    }
    pub(super) fn original_disposal_gate(&self) -> Option<Weak<ReadCloseGate>> {
        self.original_disposal_gate.lock().unwrap().clone()
    }
    pub(crate) fn release(&self) {
        {
            // Serialize the preinstall test configuration against release.
            let _configuration = self.establish_target.lock().unwrap();
            self.released.store(true, Ordering::Release);
        }
        self.changed.notify_waiters();
    }
    pub(super) async fn wait_for_release(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
    pub(super) fn bind_actual_exit(
        &self,
        original: watch::Receiver<bool>,
        actor: novarocks_control_runtime_core::identity::ActorId,
    ) {
        assert!(self.actual_actor.lock().unwrap().replace(actor).is_none());
        assert!(self.actual_exit.lock().unwrap().replace(original).is_none());
    }
    pub(crate) fn actual_actor(&self) -> Option<novarocks_control_runtime_core::identity::ActorId> {
        *self.actual_actor.lock().unwrap()
    }
    pub(crate) fn actual_exit_observed(&self) -> bool {
        self.actual_exit
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|original| *original.borrow())
    }
}

#[derive(Clone)]
pub(crate) struct OriginalQualificationObservation {
    pub(crate) identity: ReplacementQualificationIdentity,
    pub(crate) operation: TaskOperationId,
    pub(crate) failed: Vec<ParticipationRef>,
    pub(crate) replacement: Vec<ParticipationRef>,
    pub(crate) issued_at: novarocks_coordination_contract::MonotonicInstant,
    pub(crate) conservative_expiry: novarocks_coordination_contract::MonotonicInstant,
}
impl OriginalInstallProbe {
    pub(super) fn observe_qualification(&self, request: &ReplacementQualificationRequest) {
        self.qualifications
            .lock()
            .unwrap()
            .push(OriginalQualificationObservation {
                identity: request.identity(),
                operation: request.operation_id(),
                failed: request.failed_participations().to_vec(),
                replacement: request.replacement_participations().to_vec(),
                issued_at: request.issued_at(),
                conservative_expiry: request.conservative_expiry(),
            });
    }
    pub(crate) fn qualifications(&self) -> Vec<OriginalQualificationObservation> {
        self.qualifications.lock().unwrap().clone()
    }
}

/// The SAME already-bound dormant manifest, observed before actual qualification
/// IO. This pause does not authorize a ticket, activation or Source install.
pub(crate) struct OriginalCandidateObservation {
    pub(crate) execution: novarocks_types::QueryExecutionId,
    pub(crate) root: TaskIdentity,
    pub(crate) tasks: Vec<TaskIdentity>,
    pub(crate) participations: Vec<ParticipationRef>,
    released: AtomicBool,
    wake: futures::task::AtomicWaker,
}
impl OriginalCandidateObservation {
    pub(crate) fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.wake.wake();
    }
    pub(super) fn poll_released(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        self.wake.register(cx.waker());
        if self.released.load(Ordering::Acquire) {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    }
}
impl OriginalInstallProbe {
    pub(crate) fn candidate(&self, index: usize) -> Option<Arc<OriginalCandidateObservation>> {
        self.candidates.lock().unwrap().get(index).cloned()
    }
    pub(super) fn capture_candidate(
        &self,
        manifest: &TaskManifestBinding,
    ) -> Arc<OriginalCandidateObservation> {
        let mut candidates = self.candidates.lock().unwrap();
        if let Some(original) = candidates
            .iter()
            .find(|row| row.execution == manifest.execution())
        {
            assert_eq!(original.root, manifest.root());
            return original.clone();
        }
        let row = Arc::new(OriginalCandidateObservation {
            execution: manifest.execution(),
            root: manifest.root(),
            tasks: manifest
                .tasks()
                .iter()
                .map(|task| task.identity())
                .collect(),
            participations: manifest
                .participations()
                .iter()
                .map(|row| row.participation())
                .collect(),
            released: AtomicBool::new(false),
            wake: futures::task::AtomicWaker::new(),
        });
        candidates.push(row.clone());
        row
    }
}

impl OriginalInstallProbe {
    // Observation only AFTER the SAME production owner's actual finish path.
    // No fake replacement owner is introduced to count a callback.
    pub(super) fn observe_isolation_finish(&self, execution: novarocks_types::QueryExecutionId) {
        self.isolation_finished.lock().unwrap().push(execution);
    }
    pub(crate) fn isolation_finished(&self) -> Vec<novarocks_types::QueryExecutionId> {
        self.isolation_finished.lock().unwrap().clone()
    }
}
