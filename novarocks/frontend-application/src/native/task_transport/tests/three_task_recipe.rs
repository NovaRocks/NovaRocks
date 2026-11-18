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

//! Two or three declared protocol Tasks over the SAME finite peer/socket/participation.
//! No Worker execution, exchange delivery or writer runtime is claimed here.
use super::*;

#[derive(Default)]
pub(super) struct OtherTaskSlot {
    deferred: bool,
    created: Option<OtherTaskRevisions>,
}
struct OtherTaskRevisions {
    terminal: Option<OtherTaskTerminal>,
}
struct OtherTaskTerminal {
    status: TaskStatus,
    preceding: Option<(TaskStatus, u64)>,
    status_revision: u64,
    stopped: u64,
}
fn running(task: TaskIdentity) -> TaskStatus {
    TaskStatus::try_new(
        task,
        TaskStatusVersion::new(2).unwrap(),
        TaskState::Running,
        None,
        TaskOutputFacts::default(),
    )
    .unwrap()
}
fn finished(task: TaskIdentity) -> TaskStatus {
    TaskStatus::try_new(
        task,
        TaskStatusVersion::new(3).unwrap(),
        TaskState::Finished,
        None,
        TaskOutputFacts::new(true),
    )
    .unwrap()
}
impl SingleRootLifecycle {
    pub(super) fn declares_task(&self, task: TaskIdentity) -> bool {
        task == self.root || self.other_tasks.contains_key(&task)
    }
    pub(super) fn other_task_created(&self, task: TaskIdentity) -> bool {
        self.other_tasks
            .get(&task)
            .is_some_and(|row| row.created.is_some())
    }
    pub(super) fn other_task_terminal(&self, task: TaskIdentity) -> bool {
        self.other_tasks
            .get(&task)
            .and_then(|row| row.created.as_ref())
            .is_some_and(|row| row.terminal.is_some())
    }
    pub(super) fn created_tasks(&self) -> Vec<TaskIdentity> {
        let mut tasks = self
            .other_tasks
            .iter()
            .filter_map(|(task, row)| row.created.as_ref().map(|_| *task))
            .collect::<Vec<_>>();
        if self.created {
            tasks.push(self.root);
        }
        tasks.sort_unstable();
        tasks
    }
    fn other_terminal_frames(&self, task: TaskIdentity) -> Vec<proto::TaskStatusStreamEvent> {
        let Some(terminal) = self
            .other_tasks
            .get(&task)
            .and_then(|row| row.created.as_ref())
            .and_then(|row| row.terminal.as_ref())
        else {
            return Vec::new();
        };
        let mut frames = Vec::new();
        if let Some((status, revision)) = &terminal.preceding {
            frames.push(self.frame(
                CoveredStatusStreamFact::Status(status.clone()),
                Some(*revision),
            ));
        }
        frames.push(self.frame(
            CoveredStatusStreamFact::Status(terminal.status.clone()),
            Some(terminal.status_revision),
        ));
        frames.push(self.frame(
            CoveredStatusStreamFact::TaskConvergence(TaskConvergenceReceipt::actual_stopped(
                task,
                TaskConvergenceVersion::FIRST,
            )),
            Some(terminal.stopped),
        ));
        frames
    }
    fn set_other_terminal(
        &mut self,
        task: TaskIdentity,
        status: TaskStatus,
        preceding: Option<TaskStatus>,
    ) -> Result<Vec<proto::TaskStatusStreamEvent>, Status> {
        let row = self
            .other_tasks
            .get(&task)
            .and_then(|row| row.created.as_ref())
            .ok_or_else(|| Status::failed_precondition("extra Task has not been created"))?;
        if row.terminal.is_some() {
            return Err(Status::failed_precondition(
                "extra Task already has its original terminal",
            ));
        }
        let preceding = preceding.map(|status| (status, self.next_revision()));
        let terminal = OtherTaskTerminal {
            status,
            preceding,
            status_revision: self.next_revision(),
            stopped: self.next_revision(),
        };
        self.other_tasks
            .get_mut(&task)
            .unwrap()
            .created
            .as_mut()
            .unwrap()
            .terminal = Some(terminal);
        Ok(self.other_terminal_frames(task))
    }
    pub(super) fn create_other_task(
        &mut self,
        task: TaskIdentity,
    ) -> Result<(TaskStatus, Vec<proto::TaskStatusStreamEvent>), Status> {
        let row = self
            .other_tasks
            .get(&task)
            .ok_or_else(|| Status::failed_precondition("undeclared extra Task"))?;
        if row.created.is_some() {
            return Ok((self.static_status(task).unwrap(), Vec::new()));
        }
        let deferred = row.deferred;
        let revision = self.next_revision();
        self.other_tasks.get_mut(&task).unwrap().created =
            Some(OtherTaskRevisions { terminal: None });
        let mut frames = vec![self.frame(
            CoveredStatusStreamFact::Status(running(task)),
            Some(revision),
        )];
        if !deferred {
            frames.extend(self.set_other_terminal(task, finished(task), None)?);
        }
        Ok((TaskStatus::created(task).with_installed(), frames))
    }
    pub(super) fn cancel_other_task(
        &mut self,
        task: TaskIdentity,
        reason: novarocks_execution_contract::CancelReason,
    ) -> Result<Vec<proto::TaskStatusStreamEvent>, Status> {
        self.other_tasks
            .get(&task)
            .ok_or_else(|| Status::failed_precondition("same original leaf"))?;
        let detail = novarocks_execution_contract::TerminationDetail::Canceled(reason);
        let preceding = TaskStatus::try_new(
            task,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Canceling,
            Some(detail.clone()),
            TaskOutputFacts::default(),
        )
        .unwrap();
        let status = TaskStatus::try_new(
            task,
            TaskStatusVersion::new(4).unwrap(),
            TaskState::Canceled,
            Some(detail),
            TaskOutputFacts::default(),
        )
        .unwrap();
        self.set_other_terminal(task, status, Some(preceding))
    }
    pub(super) fn static_status(&self, task: TaskIdentity) -> Option<TaskStatus> {
        if task == self.root {
            self.created.then(|| self.current_status())
        } else {
            self.other_tasks
                .get(&task)
                .and_then(|row| row.created.as_ref())
                .map(|row| {
                    row.terminal
                        .as_ref()
                        .map(|terminal| terminal.status.clone())
                        .unwrap_or_else(|| running(task))
                })
        }
    }
    pub(super) fn static_stopped(&self, task: TaskIdentity) -> bool {
        if task == self.root {
            self.task_revision.is_some()
        } else {
            self.other_task_terminal(task)
        }
    }
}
impl Loopback {
    /// Passive statuses authored by this SAME peer's actual Create/stream path.
    /// No declared-but-uncreated Task is made observable by this snapshot.
    pub(crate) fn original_created_task_statuses(&self) -> Vec<TaskStatus> {
        let state = self.peer.state.lock().expect("peer state");
        if let Some(original) = state.participation_only.as_ref() {
            return original.statuses();
        }
        let Some(script) = state.single_root.as_ref() else {
            return Vec::new();
        };
        script
            .created_tasks()
            .into_iter()
            .map(|task| {
                script
                    .static_status(task)
                    .expect("same actual created Task")
            })
            .collect()
    }

    /// Called after the SAME root-result declaration and before actual Create.
    /// All identities come from the observed actual manifest, never guessed.
    pub(crate) fn enable_three_task_roster(&self, tasks: [TaskIdentity; 3]) -> Result<(), Status> {
        let mut state = self.peer.state.lock().expect("peer state");
        let script = state
            .single_root
            .as_mut()
            .ok_or_else(|| Status::failed_precondition("declare original root first"))?;
        let identities = tasks.into_iter().collect::<std::collections::BTreeSet<_>>();
        if identities.len() != 3
            || !identities.contains(&script.root)
            || script.created
            || !script.other_tasks.is_empty()
            || script.released
            || script.quiesce.is_some()
            || identities.iter().any(|task| {
                task.query_execution_id() != script.root.query_execution_id()
                    || task.backend_process_id() != script.root.backend_process_id()
            })
        {
            return Err(Status::failed_precondition(
                "three-task roster differs from its original root or lifecycle",
            ));
        }
        script.other_tasks = identities
            .into_iter()
            .filter(|task| *task != script.root)
            .map(|task| (task, OtherTaskSlot::default()))
            .collect();
        Ok(())
    }
}
impl Loopback {
    /// Declare only SAME actual manifest identities. This finite peer input
    /// grants no graph/permission/Ready/Source authority.
    pub(crate) fn enable_original_task_roster(&self, tasks: &[TaskIdentity]) -> Result<(), Status> {
        let mut state = self.peer.state.lock().expect("peer state");
        let script = state
            .single_root
            .as_mut()
            .ok_or_else(|| Status::failed_precondition("declare original Root first"))?;
        let identities = tasks
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if identities.is_empty()
            || identities.len() != tasks.len()
            || !identities.contains(&script.root)
            || script.created
            || !script.other_tasks.is_empty()
            || script.released
            || script.quiesce.is_some()
            || identities.iter().any(|task| {
                task.query_execution_id() != script.root.query_execution_id()
                    || task.backend_process_id() != script.root.backend_process_id()
            })
        {
            return Err(Status::failed_precondition(
                "original roster differs from its Root or lifecycle",
            ));
        }
        script.other_tasks = identities
            .into_iter()
            .filter(|task| *task != script.root)
            .map(|task| (task, OtherTaskSlot::default()))
            .collect();
        Ok(())
    }

    /// Called before original Create. Default three-task recipes stay unchanged.
    pub(crate) fn defer_three_task_terminals(&self) -> Result<(), Status> {
        let mut state = self.peer.state.lock().expect("peer state");
        let script = state
            .single_root
            .as_mut()
            .ok_or_else(|| Status::failed_precondition("root is not declared"))?;
        if script.other_tasks.len() != 2
            || script.created
            || script.other_tasks.values().any(|row| row.created.is_some())
        {
            return Err(Status::failed_precondition(
                "defer precedes every original Create",
            ));
        }
        for row in script.other_tasks.values_mut() {
            row.deferred = true;
        }
        Ok(())
    }
    /// Original peer facts, through the SAME covered status stream/codec.
    pub(crate) fn publish_three_task_terminal(
        &self,
        task: TaskIdentity,
        failure: Option<novarocks_execution_contract::TaskFailure>,
    ) -> Result<(), Status> {
        let mut state = self.peer.state.lock().expect("peer state");
        let PeerState {
            single_root, held, ..
        } = &mut *state;
        let script = single_root
            .as_mut()
            .ok_or_else(|| Status::failed_precondition("root is not declared"))?;
        if script.quiesce.is_some() || script.released || script.abort_cause.is_some() {
            return Err(Status::failed_precondition(
                "original three-task owner is closing",
            ));
        }
        let (status, preceding) = match failure {
            None => (finished(task), None),
            Some(original) => {
                let detail = novarocks_execution_contract::TerminationDetail::Failed(original);
                let preceding = TaskStatus::try_new(
                    task,
                    TaskStatusVersion::new(3).unwrap(),
                    TaskState::Failing,
                    Some(detail.clone()),
                    TaskOutputFacts::default(),
                )
                .unwrap();
                let status = TaskStatus::try_new(
                    task,
                    TaskStatusVersion::new(4).unwrap(),
                    TaskState::Failed,
                    Some(detail),
                    TaskOutputFacts::default(),
                )
                .unwrap();
                (status, Some(preceding))
            }
        };
        for frame in script.set_other_terminal(task, status, preceding)? {
            TaskWirePeer::queue_single_root_frame(held, frame)?;
        }
        Ok(())
    }
    /// Two existing RPC futures are gated independently; release itself offers
    /// no Root bytes, consumes no DATA and never supplies an End receipt.
    pub(crate) fn defer_encoded_root_result(&self) -> Result<(), Status> {
        let mut state = self.peer.state.lock().expect("peer state");
        let script = state
            .single_root
            .as_mut()
            .ok_or_else(|| Status::failed_precondition("root is not declared"))?;
        if script.created
            || !matches!(script.result, SingleRootResult::EncodedData { .. })
            || script.root_data_gate.is_some()
            || script.root_end_gate.is_some()
        {
            return Err(Status::failed_precondition(
                "defer original encoded root before Create",
            ));
        }
        script.root_data_gate = Some(Arc::new(SingleRootIoGate::new()));
        script.root_end_gate = Some(Arc::new(SingleRootIoGate::new()));
        Ok(())
    }
    pub(crate) fn release_encoded_root_data(&self) {
        let gate = self
            .peer
            .state
            .lock()
            .expect("peer state")
            .single_root
            .as_ref()
            .and_then(|script| script.root_data_gate.clone())
            .expect("same original DATA gate");
        gate.release();
    }
    pub(crate) fn release_encoded_root_end(&self) {
        let gate = self
            .peer
            .state
            .lock()
            .expect("peer state")
            .single_root
            .as_ref()
            .and_then(|script| script.root_end_gate.clone())
            .expect("same original End gate");
        gate.release();
    }
}
impl Loopback {
    /// Hold the SAME first ReadRootResult future of the existing empty Count
    /// root. No replacement root, offered End, ACK or Ready is constructed.
    pub(crate) fn defer_empty_root_end(&self) -> Result<(), Status> {
        let mut state = self.peer.state.lock().expect("peer state");
        let script = state
            .single_root
            .as_mut()
            .ok_or_else(|| Status::failed_precondition("root is not declared"))?;
        if script.created
            || !matches!(script.result, SingleRootResult::EndOnly)
            || script.root_data_gate.is_some()
            || script.root_end_gate.is_some()
        {
            return Err(Status::failed_precondition(
                "defer original empty End before Create",
            ));
        }
        script.root_data_gate = Some(Arc::new(SingleRootIoGate::new()));
        Ok(())
    }
    pub(crate) fn single_root_first_result_waiting(&self) -> bool {
        self.peer
            .state
            .lock()
            .expect("peer state")
            .single_root
            .as_ref()
            .and_then(|script| script.root_data_gate.as_ref())
            .is_some_and(|gate| gate.waiting())
    }
    pub(crate) fn release_single_root_first_result(&self) {
        let gate = self
            .peer
            .state
            .lock()
            .expect("peer state")
            .single_root
            .as_ref()
            .and_then(|script| script.root_data_gate.clone())
            .expect("same original first-result gate");
        gate.release();
    }
}
impl TaskWirePeer {
    pub(super) fn subscribe_three_task_root(
        &self,
        request: proto::SubscribeTaskStatusRequest,
        close_after_initial: bool,
    ) -> Result<Response<StatusStream>, Status> {
        let decoded = decode_covered_subscribe_task_status(
            &request,
            FieldPath::root("three_task_subscription"),
        )
        .map_err(|_| Status::invalid_argument("three-task subscription did not decode"))?;
        let mut state = self.state.lock().expect("peer state");
        state.subscribed.push(request);
        let PeerState {
            single_root, held, ..
        } = &mut *state;
        let script = single_root.as_mut().expect("same declared three-task root");
        script.bind_participation(decoded.participation)?;
        // The supplied original roster may contain Root plus one or two
        // declared peers. Larger rosters have no capacity in this finite peer.
        if !matches!(script.other_tasks.len(), 1 | 2)
            || held.iter().any(|sender| !sender.is_closed())
            || decoded
                .required_identities
                .iter()
                .any(|task| !script.declares_task(*task))
            || decoded
                .status_cursors
                .iter()
                .any(|cursor| !script.declares_task(cursor.identity()))
            || decoded
                .task_convergence_cursors
                .iter()
                .any(|cursor| !script.declares_task(cursor.identity()))
        {
            return Err(Status::failed_precondition(
                "three-task subscription has a foreign member or live predecessor",
            ));
        }
        for cursor in &decoded.status_cursors {
            if cursor.current_version().is_some_and(|version| {
                script
                    .static_status(cursor.identity())
                    .is_none_or(|current| version > current.version())
            }) {
                return Err(Status::failed_precondition(
                    "three-task status cursor is ahead of its retained fact",
                ));
            }
        }
        for cursor in &decoded.task_convergence_cursors {
            if cursor.current_version().is_some_and(|version| {
                !script.static_stopped(cursor.identity()) || version > TaskConvergenceVersion::FIRST
            }) {
                return Err(Status::failed_precondition(
                    "three-task convergence cursor is ahead of its retained fact",
                ));
            }
        }
        let participation_version = decoded
            .participation_cursor
            .and_then(|cursor| cursor.current_version());
        if participation_version.is_some_and(|version| {
            !script.released || version > ParticipationConvergenceVersion::FIRST
        }) || decoded
            .quiesce_cursor
            .and_then(|cursor| cursor.fence_version)
            .is_some_and(|version| script.quiesce.is_none() || version.get() > 1)
        {
            return Err(Status::failed_precondition(
                "three-task owner cursor is ahead of its retained fact",
            ));
        }
        // Worst deferred prefix: 3 Unknown + CatchUp, original Root's 3
        // frames, two legal Running/Canceling/Canceled/Stopped prefixes,
        // then exact Quiesce and Release. This is a finite peer fixture
        // envelope, not a new FE input/source/work capacity.
        let capacity = if script.other_tasks.values().any(|row| row.deferred) {
            4 + 3 + 2 * 4 + 2
        } else {
            16
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
        held.clear();
        let mut tasks = script.other_tasks.keys().copied().collect::<Vec<_>>();
        tasks.push(script.root);
        tasks.sort_unstable();
        for task in tasks {
            let status_version = decoded
                .status_cursors
                .iter()
                .find(|cursor| cursor.identity() == task)
                .and_then(|cursor| cursor.current_version());
            if let Some(status) = script.static_status(task) {
                let fact = if status_version == Some(status.version()) {
                    CoveredStatusStreamFact::StatusUnchanged(task)
                } else {
                    CoveredStatusStreamFact::Status(status)
                };
                sender
                    .try_send(Ok(script.frame(fact, None)))
                    .expect("finite status snapshot fits");
                if script.static_stopped(task) {
                    let unchanged = decoded
                        .task_convergence_cursors
                        .iter()
                        .find(|cursor| cursor.identity() == task)
                        .and_then(|cursor| cursor.current_version())
                        == Some(TaskConvergenceVersion::FIRST);
                    let fact = if unchanged {
                        CoveredStatusStreamFact::TaskConvergenceUnchanged(task)
                    } else {
                        CoveredStatusStreamFact::TaskConvergence(
                            TaskConvergenceReceipt::actual_stopped(
                                task,
                                TaskConvergenceVersion::FIRST,
                            ),
                        )
                    };
                    sender
                        .try_send(Ok(script.frame(fact, None)))
                        .expect("finite stop snapshot fits");
                }
            } else if decoded.required_identities.contains(&task)
                || decoded
                    .status_cursors
                    .iter()
                    .any(|cursor| cursor.identity() == task)
            {
                sender
                    .try_send(Ok(
                        script.frame(CoveredStatusStreamFact::Unknown(task), None)
                    ))
                    .expect("finite absence snapshot fits");
            }
        }
        if script.quiesce.is_some() {
            sender
                .try_send(Ok(script.frame(
                    CoveredStatusStreamFact::Quiesce(script.quiesce_receipt().clone()),
                    None,
                )))
                .expect("finite fence snapshot fits");
        }
        if script.released && participation_version != Some(ParticipationConvergenceVersion::FIRST)
        {
            sender
                .try_send(Ok(script.frame(
                    CoveredStatusStreamFact::ParticipationConvergence(convergence_receipt(
                        decoded.participation,
                    )),
                    None,
                )))
                .expect("finite release snapshot fits");
        }
        sender
            .try_send(Ok(script.frame(
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: decoded.generation.get(),
                    initial_cut: script.revision,
                }),
                None,
            )))
            .expect("finite catch-up snapshot fits");
        if !script.released && !close_after_initial {
            held.push(sender);
        }
        drop(state);
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

// Finite wire-peer state only. These are the SAME decoded Create descriptors;
// no Worker execution, exchange delivery or resource grant is claimed here.
#[derive(Clone)]
pub(super) struct ScriptedExchangeUpdates {
    descriptor: novarocks_execution_contract::TaskDescriptor,
    opened: std::collections::BTreeMap<
        novarocks_execution_contract::EdgeOpenVersion,
        std::collections::BTreeSet<novarocks_execution_contract::ExchangeEdgeId>,
    >,
    closed: std::collections::BTreeMap<
        novarocks_execution_contract::DomainVersion,
        (novarocks_execution_contract::ExchangeEdgeId, TaskIdentity),
    >,
}
impl ScriptedExchangeUpdates {
    pub(super) fn new(descriptor: novarocks_execution_contract::TaskDescriptor) -> Self {
        Self {
            descriptor,
            opened: std::collections::BTreeMap::new(),
            closed: std::collections::BTreeMap::new(),
        }
    }
    pub(super) fn matches(
        &self,
        descriptor: &novarocks_execution_contract::TaskDescriptor,
    ) -> bool {
        &self.descriptor == descriptor
    }
    pub(super) fn apply(
        &mut self,
        request: &UpdateTask,
    ) -> Result<(novarocks_execution_contract::UpdateTaskReceipt, bool), Status> {
        use novarocks_execution_contract::{DomainProgression as P, TaskDomainReceipt as R};
        if request.identity() != self.descriptor.identity() {
            return Err(Status::failed_precondition(
                "exchange update names a foreign Task",
            ));
        }
        // A malformed later domain cannot partially mutate this actual peer.
        let mut pending = self.clone();
        let mut receipts = Vec::with_capacity(request.domains().len());
        let mut applied = false;
        for domain in request.domains() {
            let receipt = match domain {
                TaskDomainUpdate::OpenExchangeEdges { version, edges } => {
                    let requested = edges
                        .iter()
                        .copied()
                        .collect::<std::collections::BTreeSet<_>>();
                    if requested.is_empty()
                        || requested.len() != edges.len()
                        || requested
                            .iter()
                            .any(|edge| pending.descriptor.topology().edge(*edge).is_none())
                    {
                        return Err(Status::failed_precondition(
                            "exchange open differs from the original Create descriptor",
                        ));
                    }
                    let progression = if let Some(original) = pending.opened.get(version) {
                        if original != &requested {
                            return Err(Status::failed_precondition(
                                "exchange open replay changed its original content",
                            ));
                        }
                        P::Idempotent
                    } else if pending
                        .opened
                        .keys()
                        .next_back()
                        .is_some_and(|latest| version < latest)
                    {
                        P::Older
                    } else {
                        if pending
                            .opened
                            .values()
                            .any(|original| !original.is_disjoint(&requested))
                        {
                            return Err(Status::failed_precondition(
                                "exchange open repeated an already opened member under a new version",
                            ));
                        }
                        pending.opened.insert(*version, requested);
                        P::Apply
                    };
                    applied |= progression == P::Apply;
                    R::OpenExchangeEdges {
                        accepted_version: *pending
                            .opened
                            .keys()
                            .next_back()
                            .expect("an accepted open retains its version"),
                        opened: edges
                            .iter()
                            .copied()
                            .filter(|edge| {
                                pending.opened.values().any(|opened| opened.contains(edge))
                            })
                            .collect(),
                        progression,
                    }
                }
                TaskDomainUpdate::CloseExchangeDestination {
                    version,
                    edge,
                    destination,
                } => {
                    if !pending
                        .descriptor
                        .topology()
                        .edge(*edge)
                        .is_some_and(|original| {
                            original
                                .destinations()
                                .iter()
                                .any(|member| member.task() == *destination)
                        })
                    {
                        return Err(Status::failed_precondition(
                            "exchange close differs from the original Create descriptor",
                        ));
                    }
                    let member = (*edge, *destination);
                    let progression = if let Some(original) = pending.closed.get(version) {
                        if *original != member {
                            return Err(Status::failed_precondition(
                                "exchange close replay changed its original member",
                            ));
                        }
                        P::Idempotent
                    } else if pending
                        .closed
                        .keys()
                        .next_back()
                        .is_some_and(|latest| version < latest)
                    {
                        P::Older
                    } else {
                        if pending.closed.values().any(|original| *original == member) {
                            return Err(Status::failed_precondition(
                                "exchange destination was already closed under another version",
                            ));
                        }
                        pending.closed.insert(*version, member);
                        P::Apply
                    };
                    applied |= progression == P::Apply;
                    R::CloseExchangeDestination {
                        accepted_version: *pending
                            .closed
                            .keys()
                            .next_back()
                            .expect("an accepted close retains its version"),
                        progression,
                    }
                }
                _ => {
                    return Err(Status::failed_precondition(
                        "domain is outside the finite exchange-update script",
                    ));
                }
            };
            receipts.push(receipt);
        }
        *self = pending;
        Ok((
            novarocks_execution_contract::UpdateTaskReceipt::new(request.identity(), receipts),
            applied,
        ))
    }
}
impl SingleRootLifecycle {
    pub(super) fn bind_exchange_descriptor(
        &mut self,
        descriptor: &novarocks_execution_contract::TaskDescriptor,
    ) -> Result<(), Status> {
        if let Some(original) = self.exchange_updates.get(&descriptor.identity()) {
            if !original.matches(descriptor) {
                return Err(Status::failed_precondition(
                    "Create replay changed its original descriptor",
                ));
            }
        } else {
            self.exchange_updates.insert(
                descriptor.identity(),
                ScriptedExchangeUpdates::new(descriptor.clone()),
            );
        }
        Ok(())
    }
    pub(super) fn update_exchange_domains(
        &mut self,
        request: &UpdateTask,
    ) -> Result<(novarocks_execution_contract::UpdateTaskReceipt, bool), Status> {
        if !self.declares_task(request.identity()) || !self.established || self.released {
            return Err(Status::failed_precondition(
                "exchange update has no original admitted Task",
            ));
        }
        if (self.quiesce.is_some() || self.abort_cause.is_some())
            && request
                .domains()
                .iter()
                .any(|domain| matches!(domain, TaskDomainUpdate::OpenExchangeEdges { .. }))
        {
            return Err(Status::failed_precondition(
                "exchange open follows the original admission fence",
            ));
        }
        self.exchange_updates
            .get_mut(&request.identity())
            .ok_or_else(|| Status::failed_precondition("exchange update precedes original Create"))?
            .apply(request)
    }
}

#[test]
fn finite_exchange_update_uses_the_same_created_descriptor_and_idempotent_receipt() {
    use novarocks_execution_contract::{
        DomainProgression as P, EdgeOpenVersion, TaskDomainReceipt as R,
    };
    let descriptor = crate::task_execution::creation::tests::original_creation_seed()
        .descriptor()
        .unwrap();
    let edges = descriptor.topology().edge_ids().collect::<Vec<_>>();
    assert!(!edges.is_empty());
    let mut actual = ScriptedExchangeUpdates::new(descriptor.clone());
    let request = UpdateTask::try_new(
        TaskOperationId::new_v7(),
        descriptor.identity(),
        vec![TaskDomainUpdate::OpenExchangeEdges {
            version: EdgeOpenVersion::FIRST,
            edges: edges.clone(),
        }],
    )
    .unwrap();
    let (receipt, applied) = actual.apply(&request).unwrap();
    assert!(applied);
    assert_eq!(receipt.identity(), descriptor.identity());
    assert_eq!(
        receipt.domains(),
        &[R::OpenExchangeEdges {
            accepted_version: EdgeOpenVersion::FIRST,
            opened: edges.clone(),
            progression: P::Apply
        }]
    );
    let (replay, applied) = actual.apply(&request).unwrap();
    assert!(!applied);
    assert_eq!(
        replay.domains(),
        &[R::OpenExchangeEdges {
            accepted_version: EdgeOpenVersion::FIRST,
            opened: edges,
            progression: P::Idempotent
        }]
    );
}

#[test]
fn finite_exchange_update_rejects_foreign_members_without_partially_opening_a_domain() {
    use novarocks_execution_contract::{
        DomainProgression as P, EdgeOpenVersion, ExchangeEdgeId, TaskDomainReceipt as R,
    };
    let descriptor = crate::task_execution::creation::tests::original_creation_seed()
        .descriptor()
        .unwrap();
    let edges = descriptor.topology().edge_ids().collect::<Vec<_>>();
    assert!(!edges.is_empty());
    let foreign = ExchangeEdgeId::new(u32::MAX).unwrap();
    assert!(descriptor.topology().edge(foreign).is_none());
    let mut actual = ScriptedExchangeUpdates::new(descriptor.clone());
    let bad = UpdateTask::try_new(
        TaskOperationId::new_v7(),
        descriptor.identity(),
        vec![
            TaskDomainUpdate::OpenExchangeEdges {
                version: EdgeOpenVersion::FIRST,
                edges: edges.clone(),
            },
            TaskDomainUpdate::CloseExchangeDestination {
                version: DomainVersion::FIRST,
                edge: foreign,
                destination: descriptor.identity(),
            },
        ],
    )
    .unwrap();
    assert_eq!(
        actual.apply(&bad).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    let valid = UpdateTask::try_new(
        TaskOperationId::new_v7(),
        descriptor.identity(),
        vec![TaskDomainUpdate::OpenExchangeEdges {
            version: EdgeOpenVersion::FIRST,
            edges: edges.clone(),
        }],
    )
    .unwrap();
    let (receipt, applied) = actual.apply(&valid).unwrap();
    assert!(
        applied,
        "the rejected multi-domain request did not open its first domain"
    );
    assert_eq!(
        receipt.domains(),
        &[R::OpenExchangeEdges {
            accepted_version: EdgeOpenVersion::FIRST,
            opened: edges,
            progression: P::Apply
        }]
    );
    let domain = TaskDomainUpdate::CloseExchangeDestination {
        version: DomainVersion::FIRST,
        edge: foreign,
        destination: descriptor.identity(),
    };
    let bad = UpdateTask::try_new(
        TaskOperationId::new_v7(),
        descriptor.identity(),
        vec![domain],
    )
    .unwrap();
    assert!(actual.apply(&bad).is_err());
    let original = descriptor.topology().outbound().first().unwrap();
    let member = original.destinations().first().unwrap().task();
    let close = UpdateTask::try_new(
        TaskOperationId::new_v7(),
        descriptor.identity(),
        vec![TaskDomainUpdate::CloseExchangeDestination {
            version: DomainVersion::FIRST,
            edge: original.edge_id(),
            destination: member,
        }],
    )
    .unwrap();
    let (receipt, applied) = actual.apply(&close).unwrap();
    assert!(applied);
    assert_eq!(
        receipt.domains(),
        &[R::CloseExchangeDestination {
            accepted_version: DomainVersion::FIRST,
            progression: P::Apply
        }]
    );
    let (replay, applied) = actual.apply(&close).unwrap();
    assert!(!applied);
    assert_eq!(
        replay.domains(),
        &[R::CloseExchangeDestination {
            accepted_version: DomainVersion::FIRST,
            progression: P::Idempotent
        }]
    );
}
