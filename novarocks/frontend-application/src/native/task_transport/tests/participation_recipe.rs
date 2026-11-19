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

//! Finite RPC facts for one original non-Root participation.
//! No Root identity, output kind, result segment, End or ACK exists here.
//! These external facts are not BE execution or FE local-exit evidence.
use super::*;
use novarocks_execution_contract::{
    LeaseReceipt, ParticipationAdmissionTicketReceipt, ReleaseOutcome,
};
use novarocks_task_codec::operation::{DecodedOperation as D, DecodedUpdateParticipation as U};
use std::collections::BTreeMap;

#[derive(Default)]
struct TaskSlot {
    descriptor: Option<three_task_recipe::ScriptedExchangeUpdates>,
    status: Option<TaskStatus>,
    stopped_revision: Option<u64>,
    final_info: Option<SingleRootFinalInfo>,
}
pub(super) struct ParticipationOnly {
    participation: ParticipationRef,
    tasks: BTreeMap<TaskIdentity, TaskSlot>,
    revision: u64,
    ticket: Option<ParticipationAdmissionTicketReceipt>,
    established: bool,
    lease: Option<LeaseReceipt>,
    quiesce: Option<QuiesceParticipationReceipt>,
    quiesce_revision: Option<u64>,
    abort_cause: Option<AbortCause>,
    released: bool,
    release_revision: Option<u64>,
}
impl ParticipationOnly {
    fn new(participation: ParticipationRef, declared: &[TaskIdentity]) -> Result<Self, Status> {
        if !matches!(declared.len(), 1 | 2)
            || declared
                .iter()
                .any(|task| task.verify_participation(participation).is_err())
        {
            return Err(Status::failed_precondition(
                "participation-only roster differs from its exact participation",
            ));
        }
        let tasks = declared
            .iter()
            .copied()
            .map(|task| (task, TaskSlot::default()))
            .collect::<BTreeMap<_, _>>();
        if tasks.len() != declared.len() {
            return Err(Status::failed_precondition(
                "participation-only roster repeats a Task",
            ));
        }
        Ok(Self {
            participation,
            tasks,
            revision: 0,
            ticket: None,
            established: false,
            lease: None,
            quiesce: None,
            quiesce_revision: None,
            abort_cause: None,
            released: false,
            release_revision: None,
        })
    }
    fn next_revision(&mut self) -> u64 {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("finite original peer revisions");
        self.revision
    }
    fn require_participation(&self, participation: ParticipationRef) -> Result<(), Status> {
        if self.participation != participation {
            return Err(Status::failed_precondition(
                "participation-only identity mismatch",
            ));
        }
        Ok(())
    }
    fn frame(
        &self,
        fact: CoveredStatusStreamFact,
        revision: Option<u64>,
    ) -> proto::TaskStatusStreamEvent {
        encode_covered_status_event(&CoveredStatusStreamEvent {
            fact,
            source_revision: revision,
        })
        .expect("finite original participation fact encodes")
    }
    fn current_state(&self) -> ParticipationState {
        if self.released {
            ParticipationState::TerminalRetained
        } else if self.abort_cause.is_some() {
            ParticipationState::Aborting
        } else if self.quiesce.is_some() {
            ParticipationState::Quiescing
        } else {
            ParticipationState::Active
        }
    }
    pub(super) fn statuses(&self) -> Vec<TaskStatus> {
        self.tasks
            .values()
            .filter_map(|slot| slot.status.clone())
            .collect()
    }
    pub(super) fn final_info_reads(&self) -> Vec<TaskIdentity> {
        self.tasks
            .iter()
            .filter_map(|(task, slot)| {
                slot.final_info
                    .as_ref()
                    .is_some_and(|info| info.requested)
                    .then_some(*task)
            })
            .collect()
    }
    pub(super) fn released(&self, expected: ParticipationRef) -> bool {
        self.participation == expected && self.released
    }
    pub(super) fn declare_final_info(
        &mut self,
        task: TaskIdentity,
        operators: Vec<novarocks_execution_contract::OperatorStatistics>,
        truncated: bool,
    ) -> Result<(), Status> {
        if operators.len() > novarocks_execution_contract::status::FINAL_TASK_INFO_MAX_OPERATORS {
            return Err(Status::resource_exhausted(
                "original final-info payload exceeds its bound",
            ));
        }
        let slot = self.tasks.get_mut(&task).ok_or_else(|| {
            Status::failed_precondition("foreign participation-only final-info Task")
        })?;
        if slot.status.is_some() || slot.final_info.is_some() {
            return Err(Status::failed_precondition(
                "original final-info payload is already fixed",
            ));
        }
        slot.final_info = Some(SingleRootFinalInfo {
            operators,
            truncated,
            requested: false,
        });
        Ok(())
    }
    pub(super) fn final_info(
        &mut self,
        request: proto::GetFinalTaskInfoRequest,
    ) -> Result<Response<proto::GetFinalTaskInfoResponse>, Status> {
        let request = codec::decode_get_final_task_info(
            &request,
            TaskOperationId::new_v7(),
            FieldPath::root("participation_only_final_info"),
        )
        .map_err(|_| {
            Status::invalid_argument("participation-only final-info request did not decode")
        })?;
        let task = request.identity();
        let slot = self
            .tasks
            .get_mut(&task)
            .ok_or_else(|| Status::failed_precondition("foreign final-info Task"))?;
        let status = slot
            .status
            .clone()
            .filter(|_| slot.stopped_revision.is_some())
            .ok_or_else(|| {
                Status::failed_precondition(
                    "final info requires the original created terminal Task",
                )
            })?;
        let original = slot.final_info.as_mut().ok_or_else(|| {
            Status::failed_precondition("original final-info payload is not declared")
        })?;
        if original.requested {
            return Err(Status::failed_precondition(
                "original final-info read already consumed",
            ));
        }
        let info = novarocks_execution_contract::FinalTaskInfo::try_new(
            task,
            status,
            original.operators.clone(),
            original.truncated,
        )
        .map_err(|_| Status::failed_precondition("original final-info contract refused"))?;
        original.requested = true;
        Ok(Response::new(proto::GetFinalTaskInfoResponse {
            result: Some(proto::get_final_task_info_response::Result::Info(
                novarocks_task_codec::status::encode_final_task_info(&info),
            )),
        }))
    }
    fn create(
        &mut self,
        descriptor: &novarocks_execution_contract::TaskDescriptor,
    ) -> Result<(TaskStatus, Vec<proto::TaskStatusStreamEvent>), Status> {
        let task = descriptor.identity();
        let slot = self.tasks.get(&task).ok_or_else(|| {
            Status::failed_precondition("Create names an undeclared participation-only Task")
        })?;
        if let Some(original) = &slot.descriptor {
            if !original.matches(descriptor) {
                return Err(Status::failed_precondition(
                    "Create replay changed its original descriptor",
                ));
            }
            return Ok((slot.status.clone().unwrap(), Vec::new()));
        }
        if !self.established
            || self.quiesce.is_some()
            || self.abort_cause.is_some()
            || self.released
        {
            return Err(Status::failed_precondition(
                "participation-only Create is not admitted",
            ));
        }
        let running = TaskStatus::try_new(
            task,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::default(),
        )
        .unwrap();
        let finished = TaskStatus::try_new(
            task,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap();
        let running_revision = self.next_revision();
        let finished_revision = self.next_revision();
        let stopped_revision = self.next_revision();
        let slot = self.tasks.get_mut(&task).unwrap();
        slot.descriptor = Some(three_task_recipe::ScriptedExchangeUpdates::new(
            descriptor.clone(),
        ));
        slot.status = Some(finished.clone());
        slot.stopped_revision = Some(stopped_revision);
        Ok((
            TaskStatus::created(task).with_installed(),
            vec![
                self.frame(
                    CoveredStatusStreamFact::Status(running),
                    Some(running_revision),
                ),
                self.frame(
                    CoveredStatusStreamFact::Status(finished),
                    Some(finished_revision),
                ),
                self.frame(
                    CoveredStatusStreamFact::TaskConvergence(
                        TaskConvergenceReceipt::actual_stopped(task, TaskConvergenceVersion::FIRST),
                    ),
                    Some(stopped_revision),
                ),
            ],
        ))
    }
    fn apply(
        &mut self,
        request: &proto::ApplyTaskOperationsRequest,
        held: &[tokio::sync::mpsc::Sender<Result<proto::TaskStatusStreamEvent, Status>>],
    ) -> Result<Response<proto::ApplyTaskOperationsResponse>, Status> {
        let decoded = request
            .operations
            .iter()
            .map(|wire| {
                codec::decode_operation(wire, FieldPath::root("participation_only")).map_err(|_| {
                    Status::invalid_argument("participation-only operation did not decode")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut receipts = Vec::with_capacity(request.operations.len());
        for (wire, operation) in request.operations.iter().zip(decoded) {
            let mut outcome = OperationOutcome::Accepted;
            let mut frames = Vec::new();
            let ack = match operation {
                D::AcquireParticipationAdmissionTicket(acquire) => {
                    self.require_participation(acquire.participation())?;
                    if self.released {
                        return Err(Status::failed_precondition(
                            "Acquire follows the original participation retirement",
                        ));
                    }
                    if let Some(original) = self.ticket {
                        if original.valid_for() != acquire.valid_for() {
                            return Err(Status::failed_precondition(
                                "ticket replay changed original validity",
                            ));
                        }
                        outcome = OperationOutcome::Idempotent;
                    } else {
                        self.ticket = Some(ParticipationAdmissionTicketReceipt::new(
                            AdmissionTicketId::try_from_bytes([0x58; 16]).unwrap(),
                            self.participation,
                            acquire.valid_for(),
                        ));
                    }
                    Some(
                        proto::task_operation_verdict::Ack::ParticipationAdmissionTicket(
                            codec::encode_participation_admission_ticket_ack(self.ticket.unwrap()),
                        ),
                    )
                }
                D::UpdateParticipation(U::Establish(establish)) => {
                    self.require_participation(establish.participation())?;
                    let ticket = self.ticket.ok_or_else(|| {
                        Status::failed_precondition("Establish has no original ticket")
                    })?;
                    if establish.admission_ticket_id() != ticket.ticket_id()
                        || self.quiesce.is_some()
                        || self.abort_cause.is_some()
                        || self.released
                    {
                        return Err(Status::failed_precondition(
                            "Establish differs from the original admitted ticket",
                        ));
                    }
                    if self.established {
                        outcome = OperationOutcome::Idempotent;
                    }
                    self.established = true;
                    self.lease = Some(LeaseReceipt::new(
                        LeaseSequence::INITIAL,
                        establish.initial_lease_valid_for(),
                        establish.initial_lease_valid_for().get(),
                    ));
                    Some(proto::task_operation_verdict::Ack::Participation(
                        encode_participation_ack(
                            &ParticipationReceipt::new(
                                self.participation,
                                ParticipationState::Active,
                            )
                            .with_lease(self.lease.unwrap()),
                            None,
                        )
                        .unwrap(),
                    ))
                }
                D::UpdateParticipation(U::RenewLease {
                    participation,
                    sequence,
                    valid_for,
                    ..
                }) => {
                    self.require_participation(participation)?;
                    if !self.established
                        || self.released
                        || self.lease.is_some_and(|old| old.sequence() > sequence)
                    {
                        return Err(Status::failed_precondition(
                            "Renew does not name the original active lease",
                        ));
                    }
                    self.lease = Some(LeaseReceipt::new(sequence, valid_for, valid_for.get()));
                    Some(proto::task_operation_verdict::Ack::Participation(
                        encode_participation_ack(
                            &ParticipationReceipt::new(self.participation, self.current_state())
                                .with_lease(self.lease.unwrap()),
                            self.abort_cause,
                        )
                        .unwrap(),
                    ))
                }
                D::CreateTask(create) => {
                    self.require_participation(create.request().participation())?;
                    let (status, published) = self.create(create.request().descriptor())?;
                    frames = published;
                    Some(TaskWirePeer::create_ack(
                        create.request().identity(),
                        status,
                    ))
                }
                D::UpdateTask(update) => {
                    let request = update.request();
                    if !self.established
                        || self.released
                        || ((self.quiesce.is_some() || self.abort_cause.is_some())
                            && request.domains().iter().any(|domain| {
                                matches!(domain, TaskDomainUpdate::OpenExchangeEdges { .. })
                            }))
                    {
                        return Err(Status::failed_precondition(
                            "Update follows its original participation fence",
                        ));
                    }
                    let original = self
                        .tasks
                        .get_mut(&request.identity())
                        .and_then(|slot| slot.descriptor.as_mut())
                        .ok_or_else(|| {
                            Status::failed_precondition("Update precedes the same original Create")
                        })?;
                    let (receipt, applied) = original.apply(request)?;
                    if !applied {
                        outcome = OperationOutcome::Idempotent;
                    }
                    Some(proto::task_operation_verdict::Ack::UpdateTask(
                        codec::encode_update_task_ack(&receipt).ok_or_else(|| {
                            Status::internal("original participation update receipt did not encode")
                        })?,
                    ))
                }
                D::QuiesceParticipation(quiesce) => {
                    self.require_participation(quiesce.participation())?;
                    if self.quiesce.is_none() {
                        let tasks = self
                            .tasks
                            .iter()
                            .filter_map(|(task, slot)| slot.status.as_ref().map(|_| *task))
                            .collect();
                        self.quiesce = Some(QuiesceParticipationReceipt::new(
                            self.participation,
                            1,
                            tasks,
                            if self.abort_cause.is_some() {
                                ParticipationState::Aborting
                            } else {
                                ParticipationState::Quiescing
                            },
                        ));
                        self.quiesce_revision = Some(self.next_revision());
                        frames.push(self.frame(
                            CoveredStatusStreamFact::Quiesce(
                                self.quiesce.as_ref().unwrap().clone(),
                            ),
                            self.quiesce_revision,
                        ));
                    } else {
                        outcome = OperationOutcome::Idempotent;
                    }
                    Some(proto::task_operation_verdict::Ack::QuiesceParticipation(
                        codec::encode_quiesce_ack(self.quiesce.as_ref().unwrap()).unwrap(),
                    ))
                }
                D::ReleaseParticipation(release) => {
                    self.require_participation(release.participation())?;
                    if self.quiesce.is_none() && self.abort_cause.is_none() {
                        return Err(Status::failed_precondition(
                            "Release precedes original fence",
                        ));
                    }
                    if !self.released {
                        self.released = true;
                        self.release_revision = Some(self.next_revision());
                        frames.push(self.frame(
                            CoveredStatusStreamFact::ParticipationConvergence(convergence_receipt(
                                self.participation,
                            )),
                            self.release_revision,
                        ));
                    } else {
                        outcome = OperationOutcome::Idempotent;
                    }
                    let mut receipt = codec::encode_release_ack(
                        self.participation,
                        ReleaseOutcome::Released,
                        ParticipationState::TerminalRetained,
                        None,
                    )
                    .unwrap();
                    receipt.termination_cause =
                        self.abort_cause.map(codec::encode_abort_cause_field);
                    Some(proto::task_operation_verdict::Ack::ReleaseParticipation(
                        receipt,
                    ))
                }
                D::AbortParticipation(abort) => {
                    self.require_participation(abort.participation())?;
                    if self.released {
                        outcome = OperationOutcome::Idempotent;
                    } else if self.abort_cause.is_none() {
                        self.abort_cause = Some(abort.cause());
                    }
                    let state = if self.released
                        || self
                            .tasks
                            .values()
                            .all(|slot| slot.stopped_revision.is_some())
                    {
                        ParticipationState::TerminalRetained
                    } else {
                        ParticipationState::Aborting
                    };
                    Some(proto::task_operation_verdict::Ack::Participation(
                        encode_participation_ack(
                            &ParticipationReceipt::new(self.participation, state),
                            self.abort_cause,
                        )
                        .unwrap(),
                    ))
                }
                D::CancelTask(cancel) => {
                    let slot = self.tasks.get(&cancel.identity()).ok_or_else(|| {
                        Status::failed_precondition(
                            "Cancel names a foreign participation-only Task",
                        )
                    })?;
                    outcome = if slot.stopped_revision.is_some() {
                        OperationOutcome::Idempotent
                    } else {
                        OperationOutcome::NotReady
                    };
                    None
                }
                _ => {
                    return Err(Status::failed_precondition(
                        "operation is outside the finite participation-only recipe",
                    ));
                }
            };
            for frame in frames {
                TaskWirePeer::queue_single_root_frame(held, frame)?;
            }
            receipts.push(proto::TaskOperationReceipt {
                operation_id: wire
                    .envelope
                    .as_ref()
                    .and_then(|envelope| envelope.operation_id.clone()),
                result: Some(proto::task_operation_receipt::Result::Verdict(
                    proto::TaskOperationVerdict {
                        outcome: encode_operation_outcome(outcome),
                        safe_detail: String::new(),
                        safe_field_path: None,
                        ack,
                    },
                )),
            });
        }
        Ok(Response::new(proto::ApplyTaskOperationsResponse {
            receipts,
        }))
    }
    fn subscribe(
        &self,
        request: &proto::SubscribeTaskStatusRequest,
        held: &mut Vec<tokio::sync::mpsc::Sender<Result<proto::TaskStatusStreamEvent, Status>>>,
    ) -> Result<Response<StatusStream>, Status> {
        let decoded = decode_covered_subscribe_task_status(
            request,
            FieldPath::root("participation_only_subscription"),
        )
        .map_err(|_| Status::invalid_argument("participation-only subscription did not decode"))?;
        self.require_participation(decoded.participation)?;
        if held.iter().any(|sender| !sender.is_closed())
            || decoded
                .required_identities
                .iter()
                .any(|task| !self.tasks.contains_key(task))
            || decoded
                .status_cursors
                .iter()
                .any(|cursor| !self.tasks.contains_key(&cursor.identity()))
            || decoded
                .task_convergence_cursors
                .iter()
                .any(|cursor| !self.tasks.contains_key(&cursor.identity()))
        {
            return Err(Status::failed_precondition(
                "subscription has foreign Tasks or a live predecessor",
            ));
        }
        for cursor in &decoded.status_cursors {
            if cursor.current_version().is_some_and(|version| {
                self.tasks[&cursor.identity()]
                    .status
                    .as_ref()
                    .is_none_or(|status| version > status.version())
            }) {
                return Err(Status::failed_precondition(
                    "status cursor exceeds its original fact",
                ));
            }
        }
        for cursor in &decoded.task_convergence_cursors {
            if cursor.current_version().is_some_and(|version| {
                self.tasks[&cursor.identity()].stopped_revision.is_none()
                    || version > TaskConvergenceVersion::FIRST
            }) {
                return Err(Status::failed_precondition(
                    "Task convergence cursor exceeds its original fact",
                ));
            }
        }
        if decoded
            .participation_cursor
            .and_then(|cursor| cursor.current_version())
            .is_some_and(|version| {
                !self.released || version > ParticipationConvergenceVersion::FIRST
            })
            || decoded
                .quiesce_cursor
                .and_then(|cursor| cursor.fence_version)
                .is_some_and(|version| self.quiesce.is_none() || version.get() > 1)
        {
            return Err(Status::failed_precondition(
                "participation cursor exceeds its original fact",
            ));
        }
        held.clear();
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        for (task, slot) in &self.tasks {
            if let Some(status) = &slot.status {
                let fact = if decoded.status_cursors.iter().any(|cursor| {
                    cursor.identity() == *task && cursor.current_version() == Some(status.version())
                }) {
                    CoveredStatusStreamFact::StatusUnchanged(*task)
                } else {
                    CoveredStatusStreamFact::Status(status.clone())
                };
                sender.try_send(Ok(self.frame(fact, None))).map_err(|_| {
                    Status::resource_exhausted("finite participation snapshot did not fit")
                })?;
                if slot.stopped_revision.is_some() {
                    let fact = if decoded.task_convergence_cursors.iter().any(|cursor| {
                        cursor.identity() == *task
                            && cursor.current_version() == Some(TaskConvergenceVersion::FIRST)
                    }) {
                        CoveredStatusStreamFact::TaskConvergenceUnchanged(*task)
                    } else {
                        CoveredStatusStreamFact::TaskConvergence(
                            TaskConvergenceReceipt::actual_stopped(
                                *task,
                                TaskConvergenceVersion::FIRST,
                            ),
                        )
                    };
                    sender.try_send(Ok(self.frame(fact, None))).map_err(|_| {
                        Status::resource_exhausted("finite participation snapshot did not fit")
                    })?;
                }
            } else if decoded.required_identities.contains(task)
                || decoded
                    .status_cursors
                    .iter()
                    .any(|cursor| cursor.identity() == *task)
            {
                sender
                    .try_send(Ok(self.frame(CoveredStatusStreamFact::Unknown(*task), None)))
                    .map_err(|_| {
                        Status::resource_exhausted("finite participation snapshot did not fit")
                    })?;
            }
        }
        if let Some(receipt) = &self.quiesce {
            sender
                .try_send(Ok(
                    self.frame(CoveredStatusStreamFact::Quiesce(receipt.clone()), None)
                ))
                .map_err(|_| {
                    Status::resource_exhausted("finite participation fence snapshot did not fit")
                })?;
        }
        if self.released
            && decoded
                .participation_cursor
                .and_then(|cursor| cursor.current_version())
                != Some(ParticipationConvergenceVersion::FIRST)
        {
            sender
                .try_send(Ok(self.frame(
                    CoveredStatusStreamFact::ParticipationConvergence(convergence_receipt(
                        self.participation,
                    )),
                    None,
                )))
                .map_err(|_| {
                    Status::resource_exhausted("finite participation closure snapshot did not fit")
                })?;
        }
        sender
            .try_send(Ok(self.frame(
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: decoded.generation.get(),
                    initial_cut: self.revision,
                }),
                None,
            )))
            .map_err(|_| Status::resource_exhausted("finite participation catch-up did not fit"))?;
        if !self.released {
            held.push(sender);
        }
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}
impl Loopback {
    pub(crate) fn enable_original_participation_lifecycle(
        &self,
        descriptor: &BackendProcessDescriptor,
        participation: ParticipationRef,
        tasks: &[TaskIdentity],
    ) -> Result<(), Status> {
        if descriptor.endpoint() != &self.endpoint
            || descriptor.control_endpoint() != &self.control_endpoint
            || descriptor.process_id() != participation.backend_process_id()
            || !matches!(tasks.len(), 1 | 2)
            || tasks
                .iter()
                .any(|task| task.verify_participation(participation).is_err())
        {
            return Err(Status::failed_precondition(
                "participation-only roster differs from its original peer",
            ));
        }
        let original = ParticipationOnly::new(participation, tasks)?;
        let mut state = self.peer.state.lock().expect("peer state");
        if state.single_root.is_some()
            || state.participation_only.is_some()
            || !state.applied.is_empty()
            || !state.subscribed.is_empty()
            || !state.apply_answers.is_empty()
            || !state.subscribe_answers.is_empty()
            || !state.held.is_empty()
        {
            return Err(Status::failed_precondition(
                "configure exact participation before its original runtime starts",
            ));
        }
        state.participation_only = Some(original);
        Ok(())
    }
    pub(crate) fn original_participation_release_observed(
        &self,
        expected: ParticipationRef,
    ) -> bool {
        self.peer
            .state
            .lock()
            .expect("peer state")
            .participation_only
            .as_ref()
            .is_some_and(|original| original.released(expected))
    }
}
impl TaskWirePeer {
    pub(super) fn apply_participation_only(
        &self,
        request: proto::ApplyTaskOperationsRequest,
    ) -> Result<Response<proto::ApplyTaskOperationsResponse>, Status> {
        let mut state = self.state.lock().expect("peer state");
        state.applied.push(request.clone());
        let PeerState {
            participation_only,
            held,
            ..
        } = &mut *state;
        let response = participation_only
            .as_mut()
            .expect("exact original participation recipe")
            .apply(&request, held)?;
        if participation_only.as_ref().unwrap().released {
            held.clear();
        }
        Ok(response)
    }
    pub(super) fn subscribe_participation_only(
        &self,
        request: proto::SubscribeTaskStatusRequest,
    ) -> Result<Response<StatusStream>, Status> {
        let mut state = self.state.lock().expect("peer state");
        state.subscribed.push(request.clone());
        let PeerState {
            participation_only,
            held,
            ..
        } = &mut *state;
        participation_only
            .as_ref()
            .expect("exact original participation recipe")
            .subscribe(&request, held)
    }
}

#[tokio::test]
async fn participation_only_peer_has_no_root_result_even_for_its_declared_task() {
    let backend = BackendProcessId::new_v7();
    let part = participation(backend);
    let declared = identity(1, backend);
    let peer = TaskWirePeer::default();
    peer.state.lock().unwrap().participation_only =
        Some(ParticipationOnly::new(part, &[declared]).unwrap());
    let refused = peer
        .fetch_root_result(Request::new(proto::FetchRootResultRequest::default()))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    let state = peer.state.lock().unwrap();
    assert!(state.single_root.is_none());
    assert!(state.applied.is_empty() && state.subscribed.is_empty());
    assert!(
        state
            .participation_only
            .as_ref()
            .unwrap()
            .statuses()
            .is_empty()
    );
}

#[test]
fn participation_only_roster_rejects_foreign_and_duplicate_tasks_before_any_rpc() {
    let backend = BackendProcessId::new_v7();
    let part = participation(backend);
    let declared = identity(1, backend);
    let foreign = identity(1, BackendProcessId::new_v7());
    assert_eq!(
        ParticipationOnly::new(part, &[foreign])
            .err()
            .unwrap()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        ParticipationOnly::new(part, &[declared, declared])
            .err()
            .unwrap()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let original = ParticipationOnly::new(part, &[declared]).unwrap();
    assert_eq!(original.participation, part);
    assert!(original.statuses().is_empty() && !original.established && original.ticket.is_none());
}
