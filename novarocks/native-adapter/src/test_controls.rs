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

//! Finite Native fault grants owned by one actual role launch.

use novarocks_execution_contract::task_execution::identity::{QueryContextRef, TaskIdentity};
use novarocks_failpoint::scope::{ActionClaim, ControlCancellation, GrantHandle, ProcessBinding};
use novarocks_failpoint::target_binding::{
    RoleProcessReceipt, StatementExecutionBinding, StatementInputNomination, StatementTaskBinding,
    StatementWorkNomination,
};
use novarocks_failpoint::{QueryLifecycleFaultKind, QueryLifecycleFaultScope};
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct FaultGrant {
    handle: GrantHandle,
    kind: QueryLifecycleFaultKind,
    bind: bool,
    replay_barrier: bool,
    remote_writer_sender: bool,
    target_session_id: String,
    target_process_receipt: Option<PathBuf>,
    root: PathBuf,
    backend_index: usize,
    statement_run_token: String,
    ended: Arc<AtomicBool>,
}
#[derive(Debug)]
struct Claimed {
    replay_barrier: bool,
    scope: QueryLifecycleFaultScope,
    kind: QueryLifecycleFaultKind,
    grant_id: String,
    binding: StatementExecutionBinding,
    selected_task: Option<(u32, u32)>,
    ended: Arc<AtomicBool>,
    root: PathBuf,
    permission: Mutex<ActionClaim>,
    hit: Mutex<Option<Result<(), String>>>,
}
#[derive(Clone, Debug)]
pub struct NativeFaultClaim {
    inner: Arc<Claimed>,
}
impl std::ops::Deref for NativeFaultClaim {
    type Target = QueryLifecycleFaultScope;
    fn deref(&self) -> &Self::Target {
        &self.inner.scope
    }
}
impl NativeFaultClaim {
    pub(crate) fn requires_replay_barrier(&self) -> bool {
        self.inner.replay_barrier
    }
    pub(crate) fn action_ended(&self) -> Result<bool, String> {
        let cancellation = self.cancellation();
        if grant_retired(
            &self.inner.root,
            self.inner.kind,
            self.inner.binding.backend_index,
            &self.inner.binding.statement_run_token,
            &self.inner.binding.target_session_id,
            cancellation.launch_id(),
            self.inner.scope.process_id,
            &self.inner.ended,
        )? {
            return Ok(true);
        }
        action_ended(
            &self.inner.root,
            self.inner.kind,
            &self.inner.binding,
            &self.inner.ended,
        )
    }
    pub fn root(&self) -> &Path {
        &self.inner.root
    }
    pub fn cancellation(&self) -> ControlCancellation {
        self.inner
            .permission
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cancellation()
    }
    pub fn record_hit(&self) -> Result<(), String> {
        let mut state = self
            .inner
            .hit
            .lock()
            .map_err(|_| "Native action hit state lock failed")?;
        if let Some(result) = state.as_ref() {
            return result.clone();
        }
        let result = match self.action_ended() {
            Ok(true) => Err("Native action grant ended before a new hit".into()),
            Ok(false) => self.publish_hit(),
            Err(error) => Err(error),
        };
        *state = Some(result.clone());
        result
    }
    fn publish_hit(&self) -> Result<(), String> {
        let mut permission = self
            .inner
            .permission
            .lock()
            .map_err(|_| "Native fault claim lock failed")?;
        permission.record_hit().map_err(|error| error.to_string())?;
        // This is output from the existing action point, never authorization
        // for a later attempt. The receiver records it only after Accepted.
        let cancellation = permission.cancellation();
        let binding = StatementExecutionBinding::new(
            cancellation.session_id().to_owned(),
            self.inner.scope.token.clone(),
            self.inner.scope.backend_index,
            self.inner.scope.process_id,
            self.inner.scope.execution_id,
        )
        .map_err(|error| error.to_string())?;
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "event": "hit",
            "domain": "native",
            "action": self.inner.kind.as_str(),
            "grant_id": self.inner.grant_id,
            "launch_id": cancellation.launch_id(),
            "binding": binding,
        }))
        .map_err(|error| error.to_string())?;
        if bytes.len() > 4096 {
            return Err("Native action hit receipt exceeds byte bound".into());
        }
        let path = fault_binding_path(
            &self.inner.root,
            self.inner.kind,
            self.inner.scope.backend_index,
            &self.inner.scope.token,
        )
        .with_extension("hit.json");
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        // The runner can poll while the action executes. Publish a complete
        // immutable record atomically; a partial JSON file is never a hit.
        let staging = path.with_extension("pending");
        let mut file = options
            .open(&staging)
            .map_err(|error| format!("publish Native action hit: {error}"))?;
        std::io::Write::write_all(&mut file, &bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| format!("retain Native action hit: {error}"))?;
        std::fs::hard_link(&staging, &path)
            .map_err(|error| format!("publish completed Native action hit: {error}"))?;
        std::fs::remove_file(staging)
            .map_err(|error| format!("remove Native action hit staging file: {error}"))
    }
}

#[derive(Debug)]
struct HoldGrant {
    handle: GrantHandle,
    action: String,
    token: String,
    backend_index: usize,
    execution: QueryExecutionId,
    frontend: Option<FrontendProcessId>,
    task: Option<(u32, u32)>,
}
#[derive(Debug)]
pub struct NativeHoldClaim {
    pub token: String,
    pub backend_index: usize,
    pub process_id: BackendProcessId,
    permission: ActionClaim,
}
impl NativeHoldClaim {
    pub fn cancellation(&self) -> ControlCancellation {
        self.permission.cancellation()
    }
    pub fn record_hit(&mut self) -> Result<(), String> {
        self.permission
            .record_hit()
            .map_err(|error| error.to_string())
    }
}

pub struct NativeReplayTargetReservation {
    controls: Arc<NativeTestControls>,
    identity: TaskIdentity,
    operation: novarocks_execution_contract::TaskOperationId,
    cancellation: ControlCancellation,
}
impl NativeReplayTargetReservation {
    pub fn cancellation(&self) -> ControlCancellation {
        self.cancellation.clone()
    }
}
impl Drop for NativeReplayTargetReservation {
    fn drop(&mut self) {
        let mut pending = self
            .controls
            .replay_reservations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let execution = self.identity.query_execution_id();
        if pending.get(&execution) == Some(&(self.identity, self.operation)) {
            pending.remove(&execution);
        }
    }
}

#[derive(Debug, Default)]
struct NominationPin {
    input: Option<StatementInputNomination>,
    statement: Option<StatementWorkNomination>,
    selected_writer_task: Option<TaskIdentity>,
}

/// Composition constructs this once and injects the same role-local handle
/// into only the Native domain owners. There is no process-global accessor.
#[derive(Debug)]
pub struct NativeTestControls {
    faults: Vec<FaultGrant>,
    holds: Vec<HoldGrant>,
    held_once: Mutex<HashSet<usize>>,
    published_once: Mutex<HashSet<usize>>,
    nominations: Mutex<Vec<NominationPin>>,
    claimed: Mutex<HashMap<(QueryLifecycleFaultKind, QueryExecutionId), NativeFaultClaim>>,
    replay_reservations: Mutex<
        HashMap<QueryExecutionId, (TaskIdentity, novarocks_execution_contract::TaskOperationId)>,
    >,
    restart: Mutex<
        HashMap<
            novarocks_execution_contract::task_execution::identity::QueryContextRef,
            NativeFaultClaim,
        >,
    >,
}
impl NativeTestControls {
    /// Placement reads the exact target process receipt, never a live index or ambient arm file.
    pub fn preferred_backend_process(
        &self,
        nomination: &StatementWorkNomination,
        execution: QueryExecutionId,
    ) -> Result<Option<BackendProcessId>, String> {
        self.validate_frozen_nomination(nomination)?;
        let mut selected = None;
        for grant in self.faults.iter().filter(|grant| {
            grant.bind
                && grant.kind == QueryLifecycleFaultKind::RestartAfterEstablishContext
                && grant.statement_run_token == nomination.statement_run_token
                && grant.handle.cancellation().is_active()
        }) {
            let binding = fault_binding_path(
                &grant.root,
                grant.kind,
                grant.backend_index,
                &grant.statement_run_token,
            );
            match StatementExecutionBinding::read_private(&binding) {
                Ok(bound) if bound.execution().map_err(|error| error.to_string())? != execution => {
                    continue;
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(format!("read exact Native placement binding: {error}")),
            }
            let receipt = RoleProcessReceipt::read_private(
                grant
                    .target_process_receipt
                    .as_ref()
                    .ok_or("FE placement lacks its target process receipt")?,
            )
            .map_err(|error| error.to_string())?;
            if receipt.session_id != grant.target_session_id {
                return Err("FE placement receipt belongs to another target session".into());
            }
            let process = receipt
                .backend_process()
                .map_err(|error| error.to_string())?;
            if grant_retired(
                &grant.root,
                grant.kind,
                grant.backend_index,
                &grant.statement_run_token,
                &receipt.session_id,
                &receipt.launch_id,
                process,
                &grant.ended,
            )? {
                continue;
            }
            if selected.is_some_and(|previous| previous != process) {
                return Err("Native placement grants name more than one process".into());
            }
            selected = Some(process);
        }
        Ok(selected)
    }

    pub fn from_grants(handles: Vec<GrantHandle>) -> Result<Self, String> {
        let owner = handles.first().map(|handle| {
            let cancellation = handle.cancellation();
            (
                cancellation.process(),
                cancellation.session_id().to_owned(),
                cancellation.launch_id().to_owned(),
            )
        });
        for handle in &handles {
            if handle.domain() != "native" {
                return Err("Native controls require only Native domain grants".into());
            }
            let cancellation = handle.cancellation();
            if owner.as_ref().is_some_and(|owner| {
                owner.0 != cancellation.process()
                    || owner.1 != cancellation.session_id()
                    || owner.2 != cancellation.launch_id()
            }) {
                return Err("Native controls cannot combine distinct role launches".into());
            }
        }
        let mut faults = Vec::new();
        let mut holds = Vec::new();
        for handle in handles {
            let parameters = handle
                .parameters()
                .as_object()
                .ok_or("Native fault parameters must be an object")?;
            let action = parameters
                .get("action")
                .and_then(|value| value.as_str())
                .ok_or("Native fault action is missing")?;
            if matches!(
                action,
                "ingress-hold"
                    | "registry-hold"
                    | "preparation-hold"
                    | "prepared-failure-exit-hold"
            ) {
                let preparation =
                    matches!(action, "preparation-hold" | "prepared-failure-exit-hold");
                let keys = if preparation {
                    vec![
                        "action",
                        "token",
                        "backend_index",
                        "query_id_hi",
                        "query_id_lo",
                        "attempt_id",
                        "stage_id",
                        "task_id",
                    ]
                } else {
                    vec![
                        "action",
                        "token",
                        "backend_index",
                        "query_id_hi",
                        "query_id_lo",
                        "attempt_id",
                        "frontend_process_id",
                    ]
                };
                if parameters.len() != keys.len()
                    || keys.iter().any(|key| !parameters.contains_key(*key))
                {
                    return Err("Native direct hold target fields are invalid".into());
                }
                if !matches!(handle.cancellation().process(), ProcessBinding::Backend(_)) {
                    return Err("Native direct hold requires backend scope".into());
                }
                let token = parameters["token"]
                    .as_str()
                    .ok_or("Native hold token is invalid")?
                    .to_owned();
                novarocks_failpoint::native_registry_hold_socket_path(&token)?;
                let backend_index = parameters["backend_index"]
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or("Native hold backend index is invalid")?;
                let query = QueryId::new(
                    parameters["query_id_hi"]
                        .as_i64()
                        .ok_or("Native hold query high is invalid")?,
                    parameters["query_id_lo"]
                        .as_i64()
                        .ok_or("Native hold query low is invalid")?,
                );
                let attempt = AttemptId::new(
                    parameters["attempt_id"]
                        .as_u64()
                        .ok_or("Native hold attempt is invalid")?,
                )
                .map_err(|error| error.to_string())?;
                let execution =
                    QueryExecutionId::new(query, attempt).map_err(|error| error.to_string())?;
                let frontend = if preparation {
                    None
                } else {
                    Some(
                        parameters["frontend_process_id"]
                            .as_str()
                            .ok_or("Native hold frontend identity is invalid")?
                            .parse::<FrontendProcessId>()
                            .map_err(|error| error.to_string())?,
                    )
                };
                let task = if preparation {
                    let stage = parameters["stage_id"]
                        .as_u64()
                        .and_then(|value| u32::try_from(value).ok())
                        .filter(|value| *value > 0)
                        .ok_or("Native hold stage is invalid")?;
                    let task = parameters["task_id"]
                        .as_u64()
                        .and_then(|value| u32::try_from(value).ok())
                        .filter(|value| *value > 0)
                        .ok_or("Native hold task is invalid")?;
                    Some((stage, task))
                } else {
                    None
                };
                if holds.iter().any(|grant: &HoldGrant| {
                    grant.action == action && grant.execution == execution && grant.task == task
                }) {
                    return Err("duplicate Native direct hold target".into());
                }
                let action = action.to_owned();
                holds.push(HoldGrant {
                    handle,
                    action,
                    token,
                    backend_index,
                    execution,
                    frontend,
                    task,
                });
                continue;
            }
            let bind = action.starts_with("bind-");
            let replay_barrier = match parameters.get("hold_split_consumption_until_replay") {
                None => false,
                Some(serde_json::Value::Bool(true)) if matches!(action, "task-update-terminal-ack-drop" | "bind-task-update-terminal-ack-drop") => true,
                Some(_) => return Err("split consumption refinement requires the terminal ACK-drop fault and literal true".into()),
            };
            let remote_writer_sender = match parameters.get("task_selector") {
                None => false,
                Some(serde_json::Value::String(value))
                    if value == "remote-writer-sender"
                        && matches!(
                            action,
                            "task-execution-failure"
                                | "bind-task-execution-failure"
                                | "create-task-before-worker-hold"
                                | "bind-create-task-before-worker-hold"
                        ) =>
                {
                    true
                }
                Some(_) => return Err(
                    "remote writer selection requires its exact Task failure or Create hold action"
                        .into(),
                ),
            };
            if parameters.len()
                != (if bind { 6 } else { 5 })
                    + usize::from(replay_barrier)
                    + usize::from(remote_writer_sender)
            {
                return Err("Native fault grant target parameters are invalid".into());
            }
            let target_process_receipt = if bind {
                let receipt = PathBuf::from(
                    parameters
                        .get("target_process_receipt")
                        .and_then(|value| value.as_str())
                        .ok_or("Native bind grant requires exact target process receipt")?,
                );
                if !receipt.is_absolute() {
                    return Err("Native target process receipt must be absolute".into());
                }
                Some(receipt)
            } else {
                None
            };
            if bind != matches!(handle.cancellation().process(), ProcessBinding::Frontend(_)) {
                return Err("Native fault action does not match its actual role".into());
            }
            let target_session_id = parameters
                .get("target_session_id")
                .and_then(|value| value.as_str())
                .filter(|value| {
                    !value.is_empty()
                        && value.len() <= 128
                        && value.bytes().all(|byte| {
                            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                        })
                })
                .ok_or("Native fault target session identity is invalid")?
                .to_owned();
            if !bind && target_session_id != handle.cancellation().session_id() {
                return Err("Native fault belongs to another role session".into());
            }
            let kind =
                QueryLifecycleFaultKind::parse(action.strip_prefix("bind-").unwrap_or(action))
                    .filter(|kind| {
                        !matches!(
                            kind,
                            QueryLifecycleFaultKind::TerminalAckDrop
                                | QueryLifecycleFaultKind::TerminalSnapshotConflict
                        )
                    })
                    .ok_or("Native fault action is unsupported")?;
            let root = PathBuf::from(
                parameters
                    .get("root")
                    .and_then(|value| value.as_str())
                    .ok_or("Native fault root is missing")?,
            );
            if !root.is_absolute() || !root.is_dir() {
                return Err("Native fault root must be an existing absolute directory".into());
            }
            let backend_index = parameters
                .get("backend_index")
                .and_then(|value| value.as_u64())
                .and_then(|value| usize::try_from(value).ok())
                .ok_or("Native fault backend index is invalid")?;
            let statement_run_token = parameters
                .get("statement_run_token")
                .and_then(|value| value.as_str())
                .ok_or("Native fault statement run token is missing")?
                .to_owned();
            novarocks_failpoint::native_registry_hold_socket_path(&statement_run_token)?;
            if faults.iter().any(|grant: &FaultGrant| {
                grant.kind == kind
                    && grant.backend_index == backend_index
                    && grant.statement_run_token == statement_run_token
            }) {
                return Err("duplicate Native fault action target".into());
            }
            faults.push(FaultGrant {
                handle,
                kind,
                bind,
                replay_barrier,
                remote_writer_sender,
                target_session_id,
                target_process_receipt,
                root,
                backend_index,
                statement_run_token,
                ended: Arc::new(AtomicBool::new(false)),
            });
        }
        let nominations = (0..faults.len())
            .map(|_| NominationPin::default())
            .collect();
        Ok(Self {
            faults,
            holds,
            held_once: Mutex::new(HashSet::new()),
            published_once: Mutex::new(HashSet::new()),
            nominations: Mutex::new(nominations),
            claimed: Mutex::new(HashMap::new()),
            replay_reservations: Mutex::new(HashMap::new()),
            restart: Mutex::new(HashMap::new()),
        })
    }
    /// Pin the original request input in the existing declaration-bounded slots.
    /// Observer drain/rearm does not clear this pin or spend action permission.
    pub fn pin_statement_input(&self, input: &StatementInputNomination) -> Result<(), String> {
        input.validate().map_err(|error| error.to_string())?;
        let frontend = input
            .frontend_process()
            .map_err(|error| error.to_string())?;
        let mut frozen = self
            .nominations
            .lock()
            .map_err(|_| "Native first nomination lock failed")?;
        let matching = self
            .faults
            .iter()
            .enumerate()
            .filter(|(_, grant)| {
                grant.bind && grant.statement_run_token == input.statement_run_token
            })
            .collect::<Vec<_>>();
        for (index, grant) in &matching {
            Self::validate_input_owner(grant, input, frontend)?;
            if frozen[*index]
                .input
                .as_ref()
                .is_some_and(|original| original != input)
            {
                return Err("Native original statement input replacement refused".into());
            }
        }
        for (index, _) in matching {
            frozen[index].input.get_or_insert_with(|| input.clone());
        }
        Ok(())
    }

    fn validate_input_owner(
        grant: &FaultGrant,
        input: &StatementInputNomination,
        frontend: FrontendProcessId,
    ) -> Result<(), String> {
        let cancellation = grant.handle.cancellation();
        if !cancellation.is_active() {
            return Err("Native first nomination session is closed".into());
        }
        if cancellation.process() != ProcessBinding::Frontend(frontend)
            || cancellation.session_id() != input.target_session_id
            || cancellation.launch_id() != input.target_launch_id
        {
            return Err("Native nomination belongs to another frontend role session".into());
        }
        Ok(())
    }

    /// Freeze the actual generation only after the real request extends the
    /// immutable arm input. This performs no I/O or action claim.
    pub fn nominate_statement(
        &self,
        input: &StatementInputNomination,
        nomination: &StatementWorkNomination,
    ) -> Result<(), String> {
        input
            .validate_statement(nomination)
            .map_err(|error| error.to_string())?;
        let frontend = input
            .frontend_process()
            .map_err(|error| error.to_string())?;
        let mut frozen = self
            .nominations
            .lock()
            .map_err(|_| "Native first nomination lock failed")?;
        let matching = self
            .faults
            .iter()
            .enumerate()
            .filter(|(_, grant)| {
                grant.bind && grant.statement_run_token == input.statement_run_token
            })
            .collect::<Vec<_>>();
        for (index, grant) in &matching {
            Self::validate_input_owner(grant, input, frontend)?;
            match frozen[*index].input.as_ref() {
                Some(original) if original == input => {}
                Some(_) => return Err("Native original statement input replacement refused".into()),
                None => {
                    return Err("Native statement input was not pinned before nomination".into());
                }
            }
            if frozen[*index]
                .statement
                .as_ref()
                .is_some_and(|original| original != nomination)
            {
                return Err("Native first statement nomination replacement refused".into());
            }
        }
        for (index, _) in matching {
            frozen[index]
                .statement
                .get_or_insert_with(|| nomination.clone());
        }
        Ok(())
    }
    fn validate_frozen_nomination(
        &self,
        nomination: &StatementWorkNomination,
    ) -> Result<(), String> {
        nomination.validate().map_err(|error| error.to_string())?;
        let frontend = nomination
            .frontend_process()
            .map_err(|error| error.to_string())?;
        let frozen = self
            .nominations
            .lock()
            .map_err(|_| "Native first nomination lock failed")?;
        for (index, grant) in self.faults.iter().enumerate().filter(|(_, grant)| {
            grant.bind && grant.statement_run_token == nomination.statement_run_token
        }) {
            let cancellation = grant.handle.cancellation();
            if cancellation.process() != ProcessBinding::Frontend(frontend)
                || cancellation.session_id() != nomination.target_session_id
            {
                return Err("Native nomination belongs to another frontend role session".into());
            }
            match frozen[index].statement.as_ref() {
                Some(original) if original == nomination => {}
                Some(_) => {
                    return Err(
                        "Native execution does not match the frozen first statement nomination"
                            .into(),
                    );
                }
                None => return Err("Native statement was not nominated before execution".into()),
            }
        }
        Ok(())
    }

    /// Called only after Query Control matched the previously nominated
    /// connection/SQL/epoch/generation. This publishes no permission for BE.
    pub fn publish_nominated_binding(
        &self,
        nomination: &StatementWorkNomination,
        process: BackendProcessId,
        execution: QueryExecutionId,
    ) -> Result<usize, String> {
        self.publish_binding(nomination, process, execution, None)
    }

    pub fn needs_remote_writer_selection(
        &self,
        nomination: &StatementWorkNomination,
    ) -> Result<bool, String> {
        self.validate_frozen_nomination(nomination)?;
        let pins = self
            .nominations
            .lock()
            .map_err(|_| "Native first nomination lock failed")?;
        Ok(self.faults.iter().enumerate().any(|(index, grant)| {
            grant.bind
                && grant.remote_writer_sender
                && grant.statement_run_token == nomination.statement_run_token
                && grant.handle.cancellation().is_active()
                && pins[index].selected_writer_task.is_none()
        }))
    }

    pub fn publish_nominated_writer_task(
        &self,
        nomination: &StatementWorkNomination,
        task: TaskIdentity,
        remote_destination: BackendProcessId,
    ) -> Result<usize, String> {
        self.publish_binding(
            nomination,
            task.backend_process_id(),
            task.query_execution_id(),
            Some((task, remote_destination)),
        )
    }

    fn publish_binding(
        &self,
        nomination: &StatementWorkNomination,
        process: BackendProcessId,
        execution: QueryExecutionId,
        selected: Option<(TaskIdentity, BackendProcessId)>,
    ) -> Result<usize, String> {
        self.validate_frozen_nomination(nomination)?;
        let mut published = 0;
        let mut published_once = self
            .published_once
            .lock()
            .map_err(|_| "Native nomination state lock failed")?;
        for (index, grant) in self.faults.iter().enumerate().filter(|(_, grant)| {
            grant.bind
                && grant.remote_writer_sender == selected.is_some()
                && grant.statement_run_token == nomination.statement_run_token
                && grant.handle.cancellation().is_active()
        }) {
            // A spent grant is not permission for a replan or replacement.
            // It cannot republish the old selected statement onto a new attempt.
            if published_once.contains(&index) {
                continue;
            }
            let receipt = RoleProcessReceipt::read_private(
                grant
                    .target_process_receipt
                    .as_ref()
                    .ok_or("Native bind grant has no exact process receipt")?,
            )
            .map_err(|error| format!("read exact Native target process receipt: {error}"))?;
            if receipt.session_id != grant.target_session_id {
                return Err("Native target receipt belongs to another session".into());
            }
            let target = receipt.backend_process().map_err(|error| {
                format!("Native target receipt is not an authenticated backend: {error}")
            })?;
            if target != process {
                continue;
            }
            if grant_retired(
                &grant.root,
                grant.kind,
                grant.backend_index,
                &grant.statement_run_token,
                &receipt.session_id,
                &receipt.launch_id,
                process,
                &grant.ended,
            )? {
                continue;
            }
            if let Some((task, _)) = selected {
                // Refine the existing finite nomination pins once for this
                // statement, without spending unselected candidate grants.
                let mut pins = self
                    .nominations
                    .lock()
                    .map_err(|_| "Native first nomination lock failed")?;
                let same_selection = |candidate: &FaultGrant| {
                    candidate.bind
                        && candidate.remote_writer_sender
                        && candidate.statement_run_token == nomination.statement_run_token
                };
                if self
                    .faults
                    .iter()
                    .enumerate()
                    .filter(|(_, candidate)| same_selection(candidate))
                    .any(|(index, _)| {
                        pins[index]
                            .selected_writer_task
                            .is_some_and(|old| old != task)
                    })
                {
                    return Ok(published);
                }
                for (index, _) in self
                    .faults
                    .iter()
                    .enumerate()
                    .filter(|(_, candidate)| same_selection(candidate))
                {
                    pins[index].selected_writer_task = Some(task);
                }
            }
            let mut permission = grant
                .handle
                .claim_once(grant.handle.cancellation().process())
                .map_err(|error| error.to_string())?;
            let binding = StatementExecutionBinding::new(
                grant.target_session_id.clone(),
                grant.statement_run_token.clone(),
                grant.backend_index,
                process,
                execution,
            )
            .map_err(|error| error.to_string())?;
            let path = fault_binding_path(
                &grant.root,
                grant.kind,
                grant.backend_index,
                &grant.statement_run_token,
            );
            if let Some((task, destination)) = selected {
                StatementTaskBinding::new(
                    binding.clone(),
                    task.stage_id(),
                    task.task_id(),
                    destination,
                )
                .map_err(|error| error.to_string())?
                .write_private(&path.with_extension("task.json"))
                .map_err(|error| format!("publish exact Native Task selection: {error}"))?;
            }
            // The existing execution binding becomes visible only after its
            // complete narrowing record, so BE cannot consume a wider grant.
            binding
                .write_private(&path)
                .map_err(|error| format!("publish nominated Native fault: {error}"))?;
            permission.record_hit().map_err(|error| error.to_string())?;
            published_once.insert(index);
            published += 1;
        }
        Ok(published)
    }
    /// Matches the declared gate without claiming an ACK-drop before acceptance.
    pub fn reserve_task_update_replay_target(
        self: &Arc<Self>,
        request: &novarocks_execution_contract::UpdateTask,
    ) -> Result<Option<NativeReplayTargetReservation>, String> {
        let identity = request.identity();
        let execution = identity.query_execution_id();
        let process = identity.backend_process_id();
        let kind = QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop;
        // A replay uses the original gate. It never reserves or claims another.
        let claimed = self
            .claimed
            .lock()
            .map_err(|_| "Native fault claim lock failed")?;
        if claimed.contains_key(&(kind, execution)) {
            return Ok(None);
        }
        for grant in self
            .faults
            .iter()
            .filter(|grant| grant.kind == kind && grant.replay_barrier && !grant.bind)
        {
            if !grant.handle.cancellation().is_active()
                || grant.handle.cancellation().process() != ProcessBinding::Backend(process)
                || grant_retired(
                    &grant.root,
                    kind,
                    grant.backend_index,
                    &grant.statement_run_token,
                    &grant.target_session_id,
                    grant.handle.cancellation().launch_id(),
                    process,
                    &grant.ended,
                )?
            {
                continue;
            }
            let binding = match StatementExecutionBinding::read_private(&fault_binding_path(
                &grant.root,
                kind,
                grant.backend_index,
                &grant.statement_run_token,
            )) {
                Ok(binding) => binding,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(format!("read nominated replay gate: {error}")),
            };
            if binding.target_session_id != grant.target_session_id
                || binding.statement_run_token != grant.statement_run_token
                || binding.backend_index != grant.backend_index
                || binding.execution().map_err(|error| error.to_string())? != execution
                || binding.process().map_err(|error| error.to_string())? != process
                || action_ended(&grant.root, kind, &binding, &grant.ended)?
            {
                continue;
            }
            // Nonterminal updates cannot own the one terminal replay barrier.
            if !request.domains().iter().any(|domain| {
                matches!(domain,
                novarocks_execution_contract::TaskDomainUpdate::SplitAssignment(intent)
                    if intent.no_more_splits() && intent.offer().range().is_some())
            }) {
                continue;
            }
            let mut reservations = self
                .replay_reservations
                .lock()
                .map_err(|_| "Native replay reservation lock failed")?;
            if reservations.contains_key(&execution) {
                return Err("the selected replay gate already has an in-flight reservation".into());
            }
            reservations.insert(execution, (identity, request.envelope().operation_id()));
            return Ok(Some(NativeReplayTargetReservation {
                controls: Arc::clone(self),
                identity,
                operation: request.envelope().operation_id(),
                cancellation: grant.handle.cancellation(),
            }));
        }
        Ok(None)
    }

    pub(crate) fn claimed_task_update_replay(
        &self,
        identity: TaskIdentity,
    ) -> Result<NativeFaultClaim, String> {
        self.claimed
            .lock()
            .map_err(|_| "Native fault claim lock failed")?
            .get(&(
                QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop,
                identity.query_execution_id(),
            ))
            .filter(|claim| claim.process_id == identity.backend_process_id())
            .cloned()
            .ok_or_else(|| "replay gate cannot activate before the actual ACK-drop claim".into())
    }

    pub fn claim(
        &self,
        kind: QueryLifecycleFaultKind,
        execution: QueryExecutionId,
        process: BackendProcessId,
        persistent: bool,
    ) -> Result<Option<NativeFaultClaim>, String> {
        self.claim_inner(kind, execution, process, persistent, false, None)
    }

    /// Reserve the real once grant before the second Root read can offer Data.
    /// This permits only a finite hold, never an execution failure or a hit.
    /// Activation must still be probed before the original Task submission.
    pub(crate) fn claim_dormant_after_visible_prefix(
        &self,
        execution: QueryExecutionId,
        process: BackendProcessId,
    ) -> Result<Option<NativeFaultClaim>, String> {
        self.claim_inner(
            QueryLifecycleFaultKind::TaskExecutionFailureAfterVisiblePrefix,
            execution,
            process,
            true,
            true,
            None,
        )
    }

    /// Only this owner's original reservation can observe its exact enable.
    /// A missing, ended or cancelled activation is not permission to fail.
    pub(crate) fn after_visible_prefix_enabled(
        &self,
        claim: &NativeFaultClaim,
    ) -> Result<bool, String> {
        if claim.inner.kind != QueryLifecycleFaultKind::TaskExecutionFailureAfterVisiblePrefix {
            return Err("visible-prefix activation requires its own Native action".into());
        }
        let same_owner = self
            .claimed
            .lock()
            .map_err(|_| "Native fault claim lock failed")?
            .get(&(claim.inner.kind, claim.execution_id))
            .is_some_and(|owned| Arc::ptr_eq(&owned.inner, &claim.inner));
        if !same_owner {
            return Err(
                "visible-prefix activation requires the original Native claim owner".into(),
            );
        }
        if !claim.cancellation().is_active() || claim.action_ended()? {
            return Ok(false);
        }
        visible_prefix_activation(&claim.inner.root, &claim.inner.binding)
    }

    pub(crate) fn claim_task(
        &self,
        kind: QueryLifecycleFaultKind,
        identity: TaskIdentity,
        persistent: bool,
    ) -> Result<Option<NativeFaultClaim>, String> {
        self.claim_inner(
            kind,
            identity.query_execution_id(),
            identity.backend_process_id(),
            persistent,
            false,
            Some(identity),
        )
    }

    fn claim_inner(
        &self,
        kind: QueryLifecycleFaultKind,
        execution: QueryExecutionId,
        process: BackendProcessId,
        persistent: bool,
        allow_dormant_visible_prefix: bool,
        task: Option<TaskIdentity>,
    ) -> Result<Option<NativeFaultClaim>, String> {
        if allow_dormant_visible_prefix
            && kind != QueryLifecycleFaultKind::TaskExecutionFailureAfterVisiblePrefix
        {
            return Err("dormant Native claim is restricted to the visible-prefix action".into());
        }
        let mut claimed = self
            .claimed
            .lock()
            .map_err(|_| "Native fault claim lock failed")?;
        if let Some(scope) = claimed.get(&(kind, execution)) {
            if !persistent
                || scope.inner.selected_task.is_some_and(|selected| {
                    task.map(|task| (task.stage_id().get(), task.task_id().get())) != Some(selected)
                })
                || scope.process_id != process
                || !scope.cancellation().is_active()
                || scope.action_ended()?
            {
                return Ok(None);
            }
            // A prior dormant reservation must not make ordinary persistent
            // claims bypass the exact activation requirement.
            if kind == QueryLifecycleFaultKind::TaskExecutionFailureAfterVisiblePrefix
                && !allow_dormant_visible_prefix
                && !visible_prefix_activation(&scope.inner.root, &scope.inner.binding)?
            {
                return Ok(None);
            }
            return Ok(Some(scope.clone()));
        }
        for grant in self.faults.iter().filter(|grant| grant.kind == kind) {
            if !grant.handle.cancellation().is_active()
                || grant.handle.cancellation().process() != ProcessBinding::Backend(process)
            {
                continue;
            }
            if !grant.bind
                && grant_retired(
                    &grant.root,
                    kind,
                    grant.backend_index,
                    &grant.statement_run_token,
                    &grant.target_session_id,
                    grant.handle.cancellation().launch_id(),
                    process,
                    &grant.ended,
                )?
            {
                continue;
            }
            let binding = match StatementExecutionBinding::read_private(&fault_binding_path(
                &grant.root,
                kind,
                grant.backend_index,
                &grant.statement_run_token,
            )) {
                Ok(binding) => binding,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(format!("read nominated Native fault: {error}")),
            };
            if grant.bind
                || binding.target_session_id != grant.target_session_id
                || binding.statement_run_token != grant.statement_run_token
                || binding.backend_index != grant.backend_index
                || binding.execution().map_err(|error| error.to_string())? != execution
                || binding.process().map_err(|error| error.to_string())? != process
            {
                continue;
            }
            if action_ended(&grant.root, kind, &binding, &grant.ended)? {
                continue;
            }
            if kind == QueryLifecycleFaultKind::TaskExecutionFailureAfterVisiblePrefix
                && !allow_dormant_visible_prefix
                && !visible_prefix_activation(&grant.root, &binding)?
            {
                continue;
            }
            let selected_task = if grant.remote_writer_sender {
                let Some(task) = task else {
                    continue;
                };
                let selected = match StatementTaskBinding::read_private(
                    &fault_binding_path(
                        &grant.root,
                        kind,
                        grant.backend_index,
                        &grant.statement_run_token,
                    )
                    .with_extension("task.json"),
                ) {
                    Ok(selected) => selected,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(format!("read exact Native Task selection: {error}")),
                };
                if selected.statement != binding
                    || selected.stage_id != task.stage_id().get()
                    || selected.task_id != task.task_id().get()
                {
                    continue;
                }
                Some((selected.stage_id, selected.task_id))
            } else {
                None
            };
            let scope = QueryLifecycleFaultScope {
                token: binding.statement_run_token.clone(),
                execution_id: execution,
                backend_index: grant.backend_index,
                process_id: process,
            };
            // Token transport only provides the selected target. Permission is
            // exclusively the exact role-bound once grant, claimed afterwards.
            let permission = grant
                .handle
                .claim_once(ProcessBinding::Backend(process))
                .map_err(|error| error.to_string())?;
            let scope = NativeFaultClaim {
                inner: Arc::new(Claimed {
                    replay_barrier: grant.replay_barrier,
                    scope,
                    kind,
                    grant_id: grant.handle.grant_id().to_owned(),
                    binding,
                    selected_task,
                    ended: Arc::clone(&grant.ended),
                    root: grant.root.clone(),
                    permission: Mutex::new(permission),
                    hit: Mutex::new(None),
                }),
            };
            claimed.insert((kind, execution), scope.clone());
            return Ok(Some(scope));
        }
        Ok(None)
    }
    pub fn claim_receiver(
        &self,
        kind: QueryLifecycleFaultKind,
        execution: QueryExecutionId,
    ) -> Result<Option<NativeFaultClaim>, String> {
        let Some(process) = self
            .faults
            .iter()
            .find(|grant| grant.kind == kind)
            .and_then(|grant| match grant.handle.cancellation().process() {
                ProcessBinding::Backend(process) => Some(process),
                ProcessBinding::Frontend(_) => None,
            })
        else {
            return Ok(None);
        };
        self.claim(kind, execution, process, false)
    }
    pub fn claim_context_hold(
        &self,
        action: &str,
        context: QueryContextRef,
        requested_token: Option<&str>,
    ) -> Result<Option<NativeHoldClaim>, String> {
        let mut held_once = self
            .held_once
            .lock()
            .map_err(|_| "Native hold claim lock failed")?;
        for (index, grant) in self.holds.iter().enumerate() {
            if held_once.contains(&index)
                || grant.action != action
                || grant.execution != context.query_execution_id()
                || grant.frontend != Some(context.frontend_process_id())
                || requested_token.is_some_and(|token| token != grant.token)
                || grant.handle.cancellation().process()
                    != ProcessBinding::Backend(context.backend_process_id())
                || !grant.handle.cancellation().is_active()
            {
                continue;
            }
            let permission = grant
                .handle
                .claim_once(ProcessBinding::Backend(context.backend_process_id()))
                .map_err(|error| error.to_string())?;
            held_once.insert(index);
            return Ok(Some(NativeHoldClaim {
                token: grant.token.clone(),
                backend_index: grant.backend_index,
                process_id: context.backend_process_id(),
                permission,
            }));
        }
        Ok(None)
    }
    pub fn claim_preparation_hold(
        &self,
        identity: TaskIdentity,
    ) -> Result<Option<NativeHoldClaim>, String> {
        self.claim_task_hold("preparation-hold", identity)
    }
    pub(crate) fn claim_prepared_failure_exit_hold(
        &self,
        identity: TaskIdentity,
    ) -> Result<Option<NativeHoldClaim>, String> {
        self.claim_task_hold("prepared-failure-exit-hold", identity)
    }
    fn claim_task_hold(
        &self,
        action: &str,
        identity: TaskIdentity,
    ) -> Result<Option<NativeHoldClaim>, String> {
        let mut held_once = self
            .held_once
            .lock()
            .map_err(|_| "Native hold claim lock failed")?;
        for (index, grant) in self.holds.iter().enumerate() {
            if held_once.contains(&index)
                || grant.action != action
                || grant.execution != identity.query_execution_id()
                || grant.task != Some((identity.stage_id().get(), identity.task_id().get()))
                || grant.handle.cancellation().process()
                    != ProcessBinding::Backend(identity.backend_process_id())
                || !grant.handle.cancellation().is_active()
            {
                continue;
            }
            let permission = grant
                .handle
                .claim_once(ProcessBinding::Backend(identity.backend_process_id()))
                .map_err(|error| error.to_string())?;
            held_once.insert(index);
            return Ok(Some(NativeHoldClaim {
                token: grant.token.clone(),
                backend_index: grant.backend_index,
                process_id: identity.backend_process_id(),
                permission,
            }));
        }
        Ok(None)
    }
    pub(crate) fn arm_restart(
        &self,
        context: novarocks_execution_contract::task_execution::identity::QueryContextRef,
        claim: NativeFaultClaim,
    ) {
        self.restart
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(context, claim);
    }
    pub(crate) fn restart_claim(
        &self,
        context: novarocks_execution_contract::task_execution::identity::QueryContextRef,
    ) -> Option<NativeFaultClaim> {
        self.restart
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&context)
            .filter(|claim| claim.cancellation().is_active())
            .cloned()
    }
    pub(crate) fn disarm_restart(
        &self,
        context: novarocks_execution_contract::task_execution::identity::QueryContextRef,
    ) {
        self.restart
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&context);
    }
}

/// The unique role/action/token target is the finite grant's key. This record
/// retires that permission without inventing an execution or proving Work exit.
fn grant_retired(
    root: &Path,
    kind: QueryLifecycleFaultKind,
    index: usize,
    token: &str,
    session: &str,
    launch: &str,
    process: BackendProcessId,
    ended: &AtomicBool,
) -> Result<bool, String> {
    use std::io::Read;
    if ended.load(Ordering::Acquire) {
        return Ok(true);
    }
    let path = fault_binding_path(root, kind, index, token).with_extension("retired.json");
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("open Native grant retirement: {error}")),
    };
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file() || metadata.len() > 4096 {
        return Err("Native grant retirement must be a bounded regular file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("Native grant retirement must be private".into());
        }
    }
    let mut bytes = Vec::new();
    file.take(4097)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() > 4096 {
        return Err("Native grant retirement exceeds byte bound".into());
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("decode Native grant retirement: {error}"))?;
    let keys = [
        "schema_version",
        "event",
        "domain",
        "action",
        "target_session_id",
        "target_launch_id",
        "statement_run_token",
        "backend_index",
        "backend_process_id",
    ];
    let object = value
        .as_object()
        .ok_or("Native grant retirement must be an object")?;
    if object.len() != keys.len()
        || object.keys().any(|key| !keys.contains(&key.as_str()))
        || value["schema_version"] != 1
        || value["event"] != "grant-retired"
        || value["domain"] != "native"
        || value["action"] != kind.as_str()
        || value["target_session_id"] != session
        || value["target_launch_id"] != launch
        || value["statement_run_token"] != token
        || value["backend_index"] != index
        || value["backend_process_id"] != process.to_string()
    {
        return Err("Native grant retirement names another finite role/action/token target".into());
    }
    ended.store(true, Ordering::Release);
    Ok(true)
}

fn action_ended(
    root: &Path,
    kind: QueryLifecycleFaultKind,
    binding: &StatementExecutionBinding,
    ended: &AtomicBool,
) -> Result<bool, String> {
    if ended.load(Ordering::Acquire) {
        return Ok(true);
    }
    let path = fault_binding_path(
        root,
        kind,
        binding.backend_index,
        &binding.statement_run_token,
    )
    .with_extension("end.json");
    let record = match StatementExecutionBinding::read_private(&path) {
        Ok(record) => record,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("read exact Native action end: {error}")),
    };
    if record != *binding {
        return Err("Native action end names another session/process/statement/execution".into());
    }
    // Ending a finite action is irreversible even if its transport disappears.
    // It releases injection only; the normal Worker still decides its request.
    ended.store(true, Ordering::Release);
    Ok(true)
}

fn visible_prefix_activation(
    root: &Path,
    binding: &StatementExecutionBinding,
) -> Result<bool, String> {
    let path = fault_binding_path(
        root,
        QueryLifecycleFaultKind::TaskExecutionFailureAfterVisiblePrefix,
        binding.backend_index,
        &binding.statement_run_token,
    )
    .with_extension("enable.json");
    let enabled = match StatementExecutionBinding::read_private(&path) {
        Ok(enabled) => enabled,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("read deferred Native action activation: {error}")),
    };
    if enabled != *binding {
        return Err("deferred Native action activation names a foreign execution".into());
    }
    Ok(true)
}

pub fn fault_binding_path(
    root: &Path,
    kind: QueryLifecycleFaultKind,
    backend_index: usize,
    run_token: &str,
) -> PathBuf {
    root.join(format!(
        "be-{backend_index}.native-fault-{}.{}.execution.json",
        kind.as_str(),
        run_token
    ))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use novarocks_failpoint::liveness::{PreparedRoleSession, RoleSessionOwner, RunnerSession};
    use novarocks_failpoint::session::{DomainGrant, SessionDeclaration, SessionRole};
    use std::time::Duration;

    fn statement_input(
        nomination: &StatementWorkNomination,
        launch_id: &str,
        sql: &str,
    ) -> StatementInputNomination {
        use sha2::{Digest, Sha256};
        StatementInputNomination {
            target_session_id: nomination.target_session_id.clone(),
            target_launch_id: launch_id.into(),
            statement_run_token: nomination.statement_run_token.clone(),
            frontend_process_id: nomination.frontend_process_id.clone(),
            connection_id: nomination.connection_id,
            session_key: nomination.session_key,
            sql_sha256: Sha256::digest(sql.as_bytes()).into(),
        }
    }

    pub(crate) struct Controlled {
        pub directory: tempfile::TempDir,
        pub runner: RunnerSession,
        pub role: RoleSessionOwner,
        pub controls: Arc<NativeTestControls>,
        pub process: BackendProcessId,
        pub frontend: FrontendProcessId,
        pub execution: QueryExecutionId,
        pub token: String,
    }
    pub(crate) fn controlled(action: &str) -> Controlled {
        controlled_with_follow_up(action, false)
    }
    fn controlled_with_follow_up(action: &str, follow_up: bool) -> Controlled {
        controlled_options(action, follow_up, false, 10_000, false)
    }
    pub(crate) fn controlled_replay(lifetime_millis: u64) -> Controlled {
        controlled_options(
            "task-update-terminal-ack-drop",
            false,
            true,
            lifetime_millis,
            false,
        )
    }
    fn controlled_options(
        action: &str,
        follow_up: bool,
        replay: bool,
        lifetime_millis: u64,
        remote_writer_sender: bool,
    ) -> Controlled {
        let directory = tempfile::Builder::new()
            .prefix("g2-native-")
            .tempdir_in("/tmp")
            .unwrap();
        let process = BackendProcessId::new_v7();
        let frontend = FrontendProcessId::new_v7();
        let token = format!("g2-{process}");
        let execution =
            QueryExecutionId::new(QueryId::new(71, 73), AttemptId::new(1).unwrap()).unwrap();
        let mut parameters = if matches!(action, "ingress-hold" | "registry-hold") {
            serde_json::json!({"action":action,"token":token,"backend_index":0,"query_id_hi":71,"query_id_lo":73,"attempt_id":1,"frontend_process_id":frontend.to_string()})
        } else if matches!(action, "preparation-hold" | "prepared-failure-exit-hold") {
            serde_json::json!({"action":action,"token":token,"backend_index":0,"query_id_hi":71,"query_id_lo":73,"attempt_id":1,"stage_id":1,"task_id":2})
        } else {
            serde_json::json!({"action":action,"root":directory.path(),"backend_index":0,"statement_run_token":token,"target_session_id":"native-tests"})
        };
        if remote_writer_sender {
            parameters["task_selector"] = serde_json::json!("remote-writer-sender");
        }
        if replay {
            parameters["hold_split_consumption_until_replay"] = serde_json::json!(true);
        }
        let mut domain_grants = vec![DomainGrant {
            domain: "native".into(),
            grant_id: "one-action".into(),
            parameters: parameters.clone(),
        }];
        if remote_writer_sender {
            let mut hold = parameters.clone();
            hold["action"] = serde_json::json!("create-task-before-worker-hold");
            domain_grants.push(DomainGrant {
                domain: "native".into(),
                grant_id: "same-writer-hold".into(),
                parameters: hold,
            });
        }
        if follow_up {
            assert!(!matches!(
                action,
                "ingress-hold"
                    | "registry-hold"
                    | "preparation-hold"
                    | "prepared-failure-exit-hold"
            ));
            let mut second = parameters;
            second["statement_run_token"] = serde_json::json!(format!("{token}-next"));
            domain_grants.push(DomainGrant {
                domain: "native".into(),
                grant_id: "second-action".into(),
                parameters: second,
            });
        }
        let declaration = SessionDeclaration {
            schema_version: 1,
            session_id: "native-tests".into(),
            launch_id: process.to_string(),
            role: SessionRole::Be,
            lifetime_millis,
            liveness_socket: directory.path().join("live.sock"),
            handshake_secret: "01234567890123456789012345678901".into(),
            domain_grants,
        };
        let path = directory.path().join("declaration.json");
        declaration.write_private(&path).unwrap();
        let runner = RunnerSession::start(declaration).unwrap();
        let role = PreparedRoleSession::read_and_claim(&path, SessionRole::Be)
            .unwrap()
            .bind(ProcessBinding::Backend(process))
            .unwrap();
        let controls =
            Arc::new(NativeTestControls::from_grants(role.scope().grants_for("native")).unwrap());
        Controlled {
            directory,
            runner,
            role,
            controls,
            process,
            frontend,
            execution,
            token,
        }
    }
    impl Drop for Controlled {
        fn drop(&mut self) {
            let _ = self.runner.end_and_join();
            let _ = self.role.close_and_join();
        }
    }
    pub(crate) fn context(fixture: &Controlled) -> QueryContextRef {
        QueryContextRef::new(fixture.execution, fixture.frontend, fixture.process)
    }
    pub(crate) fn task(fixture: &Controlled, task: u32) -> TaskIdentity {
        TaskIdentity::new(
            fixture.execution,
            novarocks_types::StageId::new(1).unwrap(),
            novarocks_types::TaskId::new(task).unwrap(),
            fixture.process,
        )
    }
    pub(crate) fn bind_fixture(
        fixture: &Controlled,
        kind: QueryLifecycleFaultKind,
    ) -> StatementExecutionBinding {
        let binding = StatementExecutionBinding::new(
            "native-tests".into(),
            fixture.token.clone(),
            0,
            fixture.process,
            fixture.execution,
        )
        .unwrap();
        binding
            .write_private(&fault_binding_path(
                fixture.directory.path(),
                kind,
                0,
                &fixture.token,
            ))
            .unwrap();
        binding
    }
    pub(crate) fn end_fixture(
        fixture: &Controlled,
        kind: QueryLifecycleFaultKind,
        binding: &StatementExecutionBinding,
    ) -> PathBuf {
        let path = fault_binding_path(fixture.directory.path(), kind, 0, &fixture.token)
            .with_extension("end.json");
        let staging = path.with_extension("pending");
        binding.write_private(&staging).unwrap();
        std::fs::hard_link(&staging, &path).unwrap();
        std::fs::remove_file(staging).unwrap();
        path
    }
    fn retirement_value(
        fixture: &Controlled,
        kind: QueryLifecycleFaultKind,
        index: usize,
        token: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "schema_version": 1,
            "event": "grant-retired",
            "domain": "native",
            "action": kind.as_str(),
            "target_session_id": fixture.runner.declaration().session_id,
            "target_launch_id": fixture.runner.declaration().launch_id,
            "statement_run_token": token,
            "backend_index": index,
            "backend_process_id": fixture.process.to_string(),
        })
    }
    fn write_private_test_bytes(path: &Path, bytes: &[u8]) {
        use std::io::Write;
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    }
    fn retire_fixture(fixture: &Controlled, kind: QueryLifecycleFaultKind) -> PathBuf {
        let path = fault_binding_path(fixture.directory.path(), kind, 0, &fixture.token)
            .with_extension("retired.json");
        write_private_test_bytes(
            &path,
            &serde_json::to_vec(&retirement_value(fixture, kind, 0, &fixture.token)).unwrap(),
        );
        path
    }
    #[test]
    fn unbound_retirement_never_invents_execution_and_rejects_a_late_binding() {
        let kind = QueryLifecycleFaultKind::CreateTaskAckDrop;
        let fixture = controlled(kind.as_str());
        let binding_path = fault_binding_path(fixture.directory.path(), kind, 0, &fixture.token);
        let retired = retire_fixture(&fixture, kind);
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&retired).unwrap()).unwrap();
        assert_eq!(value.as_object().unwrap().len(), 9);
        assert!(value.get("query_id_hi").is_none());
        assert!(value.get("binding").is_none());
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .unwrap()
                .is_none()
        );
        assert!(!binding_path.exists());
        assert!(!binding_path.with_extension("hit.json").exists());
        assert!(fixture.role.scope().observations().is_empty());

        // A late real target refinement is data; it cannot revive permission.
        bind_fixture(&fixture, kind);
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .unwrap()
                .is_none()
        );
        std::fs::remove_file(&retired).unwrap();
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .unwrap()
                .is_none()
        );
        assert!(fixture.role.scope().observations().is_empty());
        assert!(!binding_path.with_extension("hit.json").exists());
        assert!(fixture.role.scope().is_active());
    }
    #[test]
    fn retiring_unbound_statement_keeps_another_predeclared_token_available() {
        let kind = QueryLifecycleFaultKind::CreateTaskAckDrop;
        let fixture = controlled_with_follow_up(kind.as_str(), true);
        retire_fixture(&fixture, kind);
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .unwrap()
                .is_none()
        );
        let next_execution =
            QueryExecutionId::new(QueryId::new(71, 74), AttemptId::new(1).unwrap()).unwrap();
        let next_token = format!("{}-next", fixture.token);
        let next_path = fault_binding_path(fixture.directory.path(), kind, 0, &next_token);
        StatementExecutionBinding::new(
            "native-tests".into(),
            next_token.clone(),
            0,
            fixture.process,
            next_execution,
        )
        .unwrap()
        .write_private(&next_path)
        .unwrap();
        let claim = fixture
            .controls
            .claim(kind, next_execution, fixture.process, true)
            .unwrap()
            .unwrap();
        assert_eq!(claim.token, next_token);
        assert_eq!(claim.execution_id, next_execution);
        assert!(!claim.action_ended().unwrap());
        claim.record_hit().unwrap();
        assert!(next_path.with_extension("hit.json").exists());
        assert!(!next_path.with_extension("retired.json").exists());
        assert!(fixture.role.scope().is_active());
        let events = fixture.role.scope().observations();
        assert!(events.iter().all(|event| event.grant_id == "second-action"));
        assert_eq!(
            events.iter().filter(|event| event.event == "hit").count(),
            1
        );
    }
    #[test]
    fn retiring_a_cached_claim_prevents_its_first_hit_irreversibly() {
        let kind = QueryLifecycleFaultKind::CreateTaskAckDrop;
        let fixture = controlled(kind.as_str());
        bind_fixture(&fixture, kind);
        let claim = fixture
            .controls
            .claim(kind, fixture.execution, fixture.process, true)
            .unwrap()
            .unwrap();
        let retired = retire_fixture(&fixture, kind);
        let error = claim.record_hit().unwrap_err();
        assert!(error.contains("ended before a new hit"));
        assert!(claim.action_ended().unwrap());
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .unwrap()
                .is_none()
        );
        std::fs::remove_file(retired).unwrap();
        assert_eq!(claim.record_hit().unwrap_err(), error);
        assert_eq!(claim.clone().record_hit().unwrap_err(), error);
        assert!(
            fixture
                .role
                .scope()
                .observations()
                .iter()
                .all(|event| event.event != "hit")
        );
        assert!(
            !fault_binding_path(fixture.directory.path(), kind, 0, &fixture.token)
                .with_extension("hit.json")
                .exists()
        );
        assert!(fixture.role.scope().is_active());
    }
    #[test]
    fn retired_frozen_nomination_cannot_publish_prefer_or_spend_bind_permission() {
        let kind = QueryLifecycleFaultKind::RestartAfterEstablishContext;
        let fixture = controlled(kind.as_str());
        let declaration = SessionDeclaration {
            schema_version: 1,
            session_id: "retirement-fe".into(),
            launch_id: fixture.frontend.to_string(),
            role: SessionRole::Fe,
            lifetime_millis: 10_000,
            liveness_socket: fixture.directory.path().join("retire-fe.sock"),
            handshake_secret: "01234567890123456789012345678901".into(),
            domain_grants: vec![DomainGrant {
                domain: "native".into(),
                grant_id: "exact-bind".into(),
                parameters: serde_json::json!({"action":format!("bind-{}", kind.as_str()),"root":fixture.directory.path(),"backend_index":0,"statement_run_token":fixture.token,"target_session_id":"native-tests","target_process_receipt":fixture.runner.process_receipt_path()}),
            }],
        };
        let path = fixture.directory.path().join("retire-fe.json");
        declaration.write_private(&path).unwrap();
        let mut runner = RunnerSession::start(declaration).unwrap();
        let mut role = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe)
            .unwrap()
            .bind(ProcessBinding::Frontend(fixture.frontend))
            .unwrap();
        let grants = role.scope().grants_for("native");
        let controls = NativeTestControls::from_grants(grants.clone()).unwrap();
        let nomination = StatementWorkNomination {
            target_session_id: "retirement-fe".into(),
            statement_run_token: fixture.token.clone(),
            frontend_process_id: fixture.frontend.to_string(),
            connection_id: 7,
            session_key: 1,
            statement_generation: 1,
        };
        let input = statement_input(&nomination, role.scope().launch_id(), "SELECT 1");
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &nomination).unwrap();
        assert_eq!(
            controls
                .preferred_backend_process(&nomination, fixture.execution)
                .unwrap(),
            Some(fixture.process)
        );
        let retired = retire_fixture(&fixture, kind);
        assert_eq!(
            controls
                .publish_nominated_binding(&nomination, fixture.process, fixture.execution)
                .unwrap(),
            0
        );
        assert_eq!(
            controls
                .preferred_backend_process(&nomination, fixture.execution)
                .unwrap(),
            None
        );
        std::fs::remove_file(retired).unwrap();
        let input = statement_input(&nomination, role.scope().launch_id(), "SELECT 1");
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &nomination).unwrap();
        assert_eq!(
            controls
                .publish_nominated_binding(&nomination, fixture.process, fixture.execution)
                .unwrap(),
            0
        );
        assert_eq!(
            controls
                .preferred_backend_process(&nomination, fixture.execution)
                .unwrap(),
            None
        );
        let changed = StatementWorkNomination {
            statement_generation: 2,
            ..nomination.clone()
        };
        assert!(
            controls
                .nominate_statement(
                    &statement_input(&changed, role.scope().launch_id(), "SELECT 1"),
                    &changed
                )
                .is_err()
        );
        assert!(
            controls
                .publish_nominated_binding(&changed, fixture.process, fixture.execution)
                .is_err()
        );
        assert!(controls.published_once.lock().unwrap().is_empty());
        assert!(role.scope().observations().is_empty());
        let target = fault_binding_path(fixture.directory.path(), kind, 0, &fixture.token);
        assert!(!target.exists());
        assert!(!target.with_extension("hit.json").exists());
        // The common once permission is still unspent. This direct mechanism
        // probe does not run the Native action, which remains retired.
        grants[0]
            .claim_once(ProcessBinding::Frontend(fixture.frontend))
            .unwrap();
        assert!(role.scope().is_active());
        runner.end_and_join().unwrap();
        role.close_and_join().unwrap();
    }
    #[test]
    fn foreign_retirement_records_are_rejected_without_spending_permission() {
        let kind = QueryLifecycleFaultKind::CreateTaskAckDrop;
        for (field, wrong) in [
            ("target_session_id", serde_json::json!("foreign-session")),
            ("target_launch_id", serde_json::json!("foreign-launch")),
            (
                "backend_process_id",
                serde_json::json!(BackendProcessId::new_v7().to_string()),
            ),
            ("statement_run_token", serde_json::json!("foreign-token")),
            ("action", serde_json::json!("lease-renewal-ack-drop")),
            ("backend_index", serde_json::json!(1)),
            ("schema_version", serde_json::json!(2)),
            ("event", serde_json::json!("hit")),
            ("domain", serde_json::json!("native-observation")),
            ("extra", serde_json::json!("not-allowed")),
        ] {
            let fixture = controlled(kind.as_str());
            let mut value = retirement_value(&fixture, kind, 0, &fixture.token);
            value[field] = wrong;
            let path = fault_binding_path(fixture.directory.path(), kind, 0, &fixture.token)
                .with_extension("retired.json");
            write_private_test_bytes(&path, &serde_json::to_vec(&value).unwrap());
            assert!(
                fixture
                    .controls
                    .claim(kind, fixture.execution, fixture.process, false)
                    .is_err(),
                "foreign field {field} accepted"
            );
            assert!(fixture.role.scope().observations().is_empty());
            std::fs::remove_file(path).unwrap();
            bind_fixture(&fixture, kind);
            let claim = fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, false)
                .unwrap()
                .unwrap();
            claim.record_hit().unwrap();
            assert_eq!(
                fixture
                    .role
                    .scope()
                    .observations()
                    .iter()
                    .filter(|event| event.event == "hit")
                    .count(),
                1
            );
        }
    }
    #[test]
    #[cfg(unix)]
    fn unsafe_retirement_files_reject_without_claim_and_identical_replay_stays_retired() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let kind = QueryLifecycleFaultKind::CreateTaskAckDrop;
        for invalid in ["public", "symlink", "oversized", "partial"] {
            let fixture = controlled(kind.as_str());
            let path = fault_binding_path(fixture.directory.path(), kind, 0, &fixture.token)
                .with_extension("retired.json");
            let value =
                serde_json::to_vec(&retirement_value(&fixture, kind, 0, &fixture.token)).unwrap();
            match invalid {
                "public" => {
                    write_private_test_bytes(&path, &value);
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
                        .unwrap();
                }
                "symlink" => {
                    let target = fixture.directory.path().join("retirement-target.json");
                    write_private_test_bytes(&target, &value);
                    symlink(target, &path).unwrap();
                }
                "oversized" => write_private_test_bytes(&path, &[b' '; 4097]),
                "partial" => write_private_test_bytes(&path, b"{"),
                _ => unreachable!(),
            }
            assert!(
                fixture
                    .controls
                    .claim(kind, fixture.execution, fixture.process, false)
                    .is_err(),
                "{invalid} retirement accepted"
            );
            assert!(fixture.role.scope().observations().is_empty());
            std::fs::remove_file(path).unwrap();
            bind_fixture(&fixture, kind);
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, false)
                .unwrap()
                .unwrap()
                .record_hit()
                .unwrap();
        }
        let fixture = controlled(kind.as_str());
        let path = retire_fixture(&fixture, kind);
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .unwrap()
                .is_none()
        );
        std::fs::remove_file(&path).unwrap();
        write_private_test_bytes(
            &path,
            &serde_json::to_vec(&retirement_value(&fixture, kind, 0, &fixture.token)).unwrap(),
        );
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .unwrap()
                .is_none()
        );
        assert!(fixture.role.scope().observations().is_empty());
        assert!(fixture.role.scope().is_active());
    }
    #[test]
    fn failed_private_hit_publication_remains_failed_after_io_condition_is_removed() {
        let kind = QueryLifecycleFaultKind::CreateTaskAckDrop;
        let fixture = controlled(kind.as_str());
        bind_fixture(&fixture, kind);
        let claim = fixture
            .controls
            .claim(kind, fixture.execution, fixture.process, false)
            .unwrap()
            .unwrap();
        let hit = fault_binding_path(fixture.directory.path(), kind, 0, &fixture.token)
            .with_extension("hit.json");
        let staging = hit.with_extension("pending");
        // Fail the atomic publication after the complete private staging write.
        std::fs::create_dir(&hit).unwrap();
        let first = claim.record_hit().unwrap_err();
        assert!(first.contains("publish completed Native action hit"));
        assert!(hit.is_dir());
        assert!(staging.is_file());
        std::fs::remove_dir(&hit).unwrap();
        std::fs::remove_file(&staging).unwrap();
        // The same retained claim and its clone must return the original error,
        // even though a new publication would now be able to succeed.
        assert_eq!(claim.record_hit().unwrap_err(), first);
        assert_eq!(claim.clone().record_hit().unwrap_err(), first);
        assert!(!hit.exists());
        assert!(!staging.exists());
        assert!(fixture.role.scope().is_active());
        assert_eq!(
            fixture
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit" && event.grant_id == "one-action")
                .count(),
            1
        );
    }

    #[test]
    fn ending_one_action_keeps_a_different_predeclared_statement_available() {
        let kind = QueryLifecycleFaultKind::CreateTaskBeforeWorkerHold;
        let fixture = controlled_with_follow_up(kind.as_str(), true);
        let first_binding = bind_fixture(&fixture, kind);
        let next_token = format!("{}-next", fixture.token);
        let next_execution =
            QueryExecutionId::new(QueryId::new(71, 74), AttemptId::new(1).unwrap()).unwrap();
        let next_binding = StatementExecutionBinding::new(
            "native-tests".into(),
            next_token.clone(),
            0,
            fixture.process,
            next_execution,
        )
        .unwrap();
        let next_path = fault_binding_path(fixture.directory.path(), kind, 0, &next_token);
        next_binding.write_private(&next_path).unwrap();
        let original_next = std::fs::read(&next_path).unwrap();
        let first = fixture
            .controls
            .claim(kind, fixture.execution, fixture.process, true)
            .unwrap()
            .unwrap();
        first.record_hit().unwrap();
        let end = end_fixture(&fixture, kind, &first_binding);
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .unwrap()
                .is_none()
        );
        assert!(first.action_ended().unwrap());
        assert!(fixture.role.scope().is_active());
        assert!(
            fixture
                .role
                .scope()
                .observations()
                .iter()
                .all(|event| event.grant_id != "second-action")
        );
        assert_eq!(std::fs::read(&next_path).unwrap(), original_next);
        let second = fixture
            .controls
            .claim(kind, next_execution, fixture.process, false)
            .unwrap()
            .unwrap();
        assert_eq!(second.token, next_token);
        assert_eq!(second.execution_id, next_execution);
        assert_eq!(second.process_id, fixture.process);
        assert!(!second.action_ended().unwrap());
        second.record_hit().unwrap();
        assert!(fixture.role.scope().is_active());
        assert!(end.exists());
        assert!(!next_path.with_extension("end.json").exists());
        assert_eq!(
            fixture
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit" && event.grant_id == "second-action")
                .count(),
            1
        );
        assert!(
            fixture
                .controls
                .claim(kind, next_execution, fixture.process, false)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn exact_end_is_irreversible_for_fresh_and_cached_claims_without_ending_role() {
        let kind = QueryLifecycleFaultKind::CreateTaskBeforeWorkerHold;
        for previously_claimed in [false, true] {
            let fixture = controlled(kind.as_str());
            let binding = bind_fixture(&fixture, kind);
            let claim = previously_claimed.then(|| {
                fixture
                    .controls
                    .claim(kind, fixture.execution, fixture.process, true)
                    .unwrap()
                    .unwrap()
            });
            let path = end_fixture(&fixture, kind, &binding);
            assert!(
                fixture
                    .controls
                    .claim(kind, fixture.execution, fixture.process, true)
                    .unwrap()
                    .is_none()
            );
            std::fs::remove_file(path).unwrap();
            assert!(
                fixture
                    .controls
                    .claim(kind, fixture.execution, fixture.process, true)
                    .unwrap()
                    .is_none()
            );
            if let Some(claim) = claim {
                assert!(claim.action_ended().unwrap());
            }
            assert!(fixture.role.scope().is_active());
            assert!(
                fixture
                    .role
                    .scope()
                    .observations()
                    .iter()
                    .all(|event| event.event != "hit")
            );
        }
    }
    #[test]
    fn foreign_end_cannot_consume_the_exact_action_permission() {
        let kind = QueryLifecycleFaultKind::CreateTaskBeforeWorkerHold;
        let fixture = controlled(kind.as_str());
        let binding = bind_fixture(&fixture, kind);
        let mut foreign = binding.clone();
        foreign.attempt_id += 1;
        let path = end_fixture(&fixture, kind, &foreign);
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .unwrap_err()
                .contains("another session/process/statement/execution")
        );
        assert!(fixture.role.scope().observations().is_empty());
        std::fs::remove_file(path).unwrap();
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .unwrap()
                .is_some()
        );
    }
    #[test]
    fn scoped_native_fault_matches_exact_statement_execution_process_and_claims_once() {
        let fixture = controlled("create-task-ack-drop");
        let kind = QueryLifecycleFaultKind::CreateTaskAckDrop;
        StatementExecutionBinding::new(
            "native-tests".into(),
            fixture.token.clone(),
            0,
            fixture.process,
            fixture.execution,
        )
        .unwrap()
        .write_private(&fault_binding_path(
            fixture.directory.path(),
            kind,
            0,
            &fixture.token,
        ))
        .unwrap();
        let other =
            QueryExecutionId::new(QueryId::new(71, 73), AttemptId::new(2).unwrap()).unwrap();
        assert!(
            fixture
                .controls
                .claim(kind, other, fixture.process, false)
                .unwrap()
                .is_none()
        );
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, BackendProcessId::new_v7(), false)
                .unwrap()
                .is_none()
        );
        assert!(fixture.role.scope().observations().is_empty());
        let claim = fixture
            .controls
            .claim(kind, fixture.execution, fixture.process, false)
            .unwrap()
            .unwrap();
        let binding_path = fault_binding_path(fixture.directory.path(), kind, 0, &fixture.token);
        let hit_path = binding_path.with_extension("hit.json");
        assert!(!hit_path.exists());
        claim.record_hit().unwrap();
        let receipt: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&hit_path).unwrap()).unwrap();
        assert_eq!(receipt["event"], "hit");
        assert_eq!(receipt["action"], kind.as_str());
        assert_eq!(
            receipt["binding"],
            serde_json::from_slice::<serde_json::Value>(&std::fs::read(&binding_path).unwrap())
                .unwrap()
        );
        let original = std::fs::read(&hit_path).unwrap();
        claim.record_hit().unwrap();
        assert_eq!(std::fs::read(hit_path).unwrap(), original);
        assert!(!binding_path.with_extension("hit.pending").exists());
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, false)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            fixture
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count(),
            1
        );
    }
    #[test]
    fn fault_transport_from_another_session_or_statement_does_not_consume_permission() {
        for (session, wrong_token) in [("old-session", false), ("native-tests", true)] {
            let fixture = controlled("create-task-ack-drop");
            let kind = QueryLifecycleFaultKind::CreateTaskAckDrop;
            StatementExecutionBinding::new(
                session.into(),
                if wrong_token {
                    "another-statement".into()
                } else {
                    fixture.token.clone()
                },
                0,
                fixture.process,
                fixture.execution,
            )
            .unwrap()
            .write_private(&fault_binding_path(
                fixture.directory.path(),
                kind,
                0,
                &fixture.token,
            ))
            .unwrap();
            assert!(
                fixture
                    .controls
                    .claim(kind, fixture.execution, fixture.process, false)
                    .unwrap()
                    .is_none()
            );
            assert!(fixture.role.scope().observations().is_empty());
        }
    }
    #[test]
    fn frozen_remote_writer_selection_narrows_both_actual_claims_and_keeps_root_unspent() {
        let fixture = controlled_options("task-execution-failure", false, false, 10_000, true);
        let kinds = [
            QueryLifecycleFaultKind::TaskExecutionFailure,
            QueryLifecycleFaultKind::CreateTaskBeforeWorkerHold,
        ];
        let grants = kinds.iter().enumerate().map(|(i, kind)| DomainGrant {
            domain: "native".into(), grant_id: format!("bind-{i}"),
            parameters: serde_json::json!({"action":format!("bind-{}",kind.as_str()),
                "root":fixture.directory.path(),"backend_index":0,"statement_run_token":fixture.token,
                "target_session_id":"native-tests","target_process_receipt":fixture.runner.process_receipt_path(),
                "task_selector":"remote-writer-sender"}),
        }).collect();
        let declaration = SessionDeclaration {
            schema_version: 1,
            session_id: "frontend-selected-writer".into(),
            launch_id: fixture.frontend.to_string(),
            role: SessionRole::Fe,
            lifetime_millis: 10_000,
            liveness_socket: fixture.directory.path().join("selected-frontend.sock"),
            handshake_secret: "01234567890123456789012345678901".into(),
            domain_grants: grants,
        };
        let path = fixture.directory.path().join("selected-frontend.json");
        declaration.write_private(&path).unwrap();
        let mut runner = RunnerSession::start(declaration).unwrap();
        let mut role = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe)
            .unwrap()
            .bind(ProcessBinding::Frontend(fixture.frontend))
            .unwrap();
        let controls = NativeTestControls::from_grants(role.scope().grants_for("native")).unwrap();
        let nomination = StatementWorkNomination {
            target_session_id: "frontend-selected-writer".into(),
            statement_run_token: fixture.token.clone(),
            frontend_process_id: fixture.frontend.to_string(),
            connection_id: 7,
            session_key: 1,
            statement_generation: 1,
        };
        let input = statement_input(
            &nomination,
            role.scope().launch_id(),
            "INSERT INTO t SELECT * FROM s",
        );
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &nomination).unwrap();
        let selected = task(&fixture, 2);
        let root_on_same_backend = task(&fixture, 1);
        assert!(controls.needs_remote_writer_selection(&nomination).unwrap());
        assert_eq!(
            controls
                .publish_nominated_binding(&nomination, fixture.process, fixture.execution)
                .unwrap(),
            0
        );
        for kind in kinds {
            assert!(
                fixture
                    .controls
                    .claim_task(kind, selected, false)
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(
            controls
                .publish_nominated_writer_task(
                    &nomination,
                    TaskIdentity::new(
                        fixture.execution,
                        selected.stage_id(),
                        selected.task_id(),
                        BackendProcessId::new_v7()
                    ),
                    BackendProcessId::new_v7()
                )
                .unwrap(),
            0,
            "an unselected candidate process consumes no bind grant"
        );
        assert!(role.scope().observations().is_empty());
        let destination = BackendProcessId::new_v7();
        assert_eq!(
            controls
                .publish_nominated_writer_task(&nomination, selected, destination)
                .unwrap(),
            2
        );
        assert!(!controls.needs_remote_writer_selection(&nomination).unwrap());
        // A later attempt cannot redirect the same finite statement selection.
        let pins = || {
            controls
                .nominations
                .lock()
                .unwrap()
                .iter()
                .map(|pin| {
                    (
                        pin.input.clone(),
                        pin.statement.clone(),
                        pin.selected_writer_task,
                    )
                })
                .collect::<Vec<_>>()
        };
        let bindings = || {
            kinds
                .iter()
                .flat_map(|kind| {
                    let path =
                        fault_binding_path(fixture.directory.path(), *kind, 0, &fixture.token);
                    [path.clone(), path.with_extension("task.json")]
                })
                .map(|path| std::fs::read(path).unwrap())
                .collect::<Vec<_>>()
        };
        let hits = || {
            [
                role.scope()
                    .observations()
                    .iter()
                    .filter(|event| event.event == "hit")
                    .count(),
                fixture
                    .role
                    .scope()
                    .observations()
                    .iter()
                    .filter(|event| event.event == "hit")
                    .count(),
            ]
        };
        let original_pins = pins();
        let original_bindings = bindings();
        let original_hits = hits();
        let later_task = TaskIdentity::new(
            QueryExecutionId::new(fixture.execution.query_id(), AttemptId::new(2).unwrap())
                .unwrap(),
            selected.stage_id(),
            novarocks_types::TaskId::new(31).unwrap(),
            fixture.process,
        );
        assert_eq!(
            controls
                .publish_nominated_writer_task(&nomination, later_task, destination)
                .unwrap(),
            0
        );
        assert_eq!(pins(), original_pins);
        assert_eq!(bindings(), original_bindings);
        assert_eq!(hits(), original_hits);
        assert!(
            controls
                .nominations
                .lock()
                .unwrap()
                .iter()
                .all(|pin| pin.selected_writer_task == Some(selected))
        );

        for kind in kinds {
            let path = fault_binding_path(fixture.directory.path(), kind, 0, &fixture.token);
            let narrowing =
                StatementTaskBinding::read_private(&path.with_extension("task.json")).unwrap();
            assert_eq!(narrowing.stage_id, selected.stage_id().get());
            assert_eq!(narrowing.task_id, selected.task_id().get());
            assert_eq!(
                narrowing.statement,
                StatementExecutionBinding::read_private(&path).unwrap()
            );
            assert!(
                fixture
                    .controls
                    .claim(kind, fixture.execution, fixture.process, true)
                    .unwrap()
                    .is_none(),
                "an execution-only probe cannot bypass Task narrowing"
            );
            for foreign in [
                root_on_same_backend,
                task(&fixture, 3),
                TaskIdentity::new(
                    fixture.execution,
                    novarocks_types::StageId::new(2).unwrap(),
                    selected.task_id(),
                    fixture.process,
                ),
                TaskIdentity::new(
                    QueryExecutionId::new(fixture.execution.query_id(), AttemptId::new(2).unwrap())
                        .unwrap(),
                    selected.stage_id(),
                    selected.task_id(),
                    fixture.process,
                ),
                TaskIdentity::new(
                    fixture.execution,
                    selected.stage_id(),
                    selected.task_id(),
                    BackendProcessId::new_v7(),
                ),
            ] {
                assert!(
                    fixture
                        .controls
                        .claim_task(kind, foreign, true)
                        .unwrap()
                        .is_none()
                );
            }
            assert!(fixture.role.scope().observations().is_empty());
        }
        for kind in kinds {
            let persistent = kind == QueryLifecycleFaultKind::CreateTaskBeforeWorkerHold;
            let claim = fixture
                .controls
                .claim_task(kind, selected, persistent)
                .unwrap()
                .unwrap();
            claim.record_hit().unwrap();
            assert!(
                fixture
                    .controls
                    .claim_task(kind, root_on_same_backend, true)
                    .unwrap()
                    .is_none(),
                "a cached persistent claim remains exact to the selected Task"
            );
            assert_eq!(
                fixture
                    .controls
                    .claim_task(kind, selected, persistent)
                    .unwrap()
                    .is_some(),
                persistent
            );
            let binding = StatementExecutionBinding::read_private(&fault_binding_path(
                fixture.directory.path(),
                kind,
                0,
                &fixture.token,
            ))
            .unwrap();
            end_fixture(&fixture, kind, &binding);
            assert!(
                fixture
                    .controls
                    .claim_task(kind, selected, true)
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(
            fixture
                .role
                .scope()
                .observations()
                .iter()
                .filter(|e| e.event == "hit")
                .count(),
            2
        );
        assert_eq!(
            controls
                .publish_nominated_writer_task(&nomination, selected, destination)
                .unwrap(),
            0
        );
        let mut foreign = nomination.clone();
        foreign.statement_generation += 1;
        assert!(
            controls
                .publish_nominated_writer_task(&foreign, selected, destination)
                .is_err()
        );
        role.close_and_join().unwrap();
        runner.end_and_join().unwrap();
    }

    #[test]
    fn nomination_uses_authenticated_receipt_and_own_label_and_never_redirects_a_spent_grant() {
        let fixture = controlled("create-task-ack-drop");
        let parameters = serde_json::json!({"action":"bind-create-task-ack-drop","root":fixture.directory.path(),"backend_index":23,"statement_run_token":fixture.token,"target_session_id":"native-tests","target_process_receipt":fixture.runner.process_receipt_path()});
        let declaration = SessionDeclaration {
            schema_version: 1,
            session_id: "frontend-bind".into(),
            launch_id: fixture.frontend.to_string(),
            role: SessionRole::Fe,
            lifetime_millis: 10_000,
            liveness_socket: fixture.directory.path().join("frontend.sock"),
            handshake_secret: "01234567890123456789012345678901".into(),
            domain_grants: vec![
                DomainGrant {
                    domain: "native".into(),
                    grant_id: "bind".into(),
                    parameters: parameters.clone(),
                },
                DomainGrant {
                    domain: "wrong-domain".into(),
                    grant_id: "not-native".into(),
                    parameters,
                },
            ],
        };
        let path = fixture.directory.path().join("frontend.json");
        declaration.write_private(&path).unwrap();
        let mut runner = RunnerSession::start(declaration).unwrap();
        let mut role = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe)
            .unwrap()
            .bind(ProcessBinding::Frontend(fixture.frontend))
            .unwrap();
        assert!(NativeTestControls::from_grants(role.scope().grants_for("wrong-domain")).is_err());
        let controls = NativeTestControls::from_grants(role.scope().grants_for("native")).unwrap();
        let nomination = StatementWorkNomination {
            target_session_id: "frontend-bind".into(),
            statement_run_token: fixture.token.clone(),
            frontend_process_id: fixture.frontend.to_string(),
            connection_id: 7,
            session_key: 1,
            statement_generation: 1,
        };
        let input = statement_input(&nomination, role.scope().launch_id(), "SELECT 1");
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &nomination).unwrap();
        let other = StatementWorkNomination {
            statement_run_token: "other-statement".into(),
            ..nomination.clone()
        };
        assert_eq!(
            controls
                .publish_nominated_binding(&other, fixture.process, fixture.execution)
                .unwrap(),
            0
        );
        assert_eq!(
            controls
                .publish_nominated_binding(
                    &nomination,
                    BackendProcessId::new_v7(),
                    fixture.execution
                )
                .unwrap(),
            0
        );
        assert!(role.scope().observations().is_empty());
        assert_eq!(
            controls
                .publish_nominated_binding(&nomination, fixture.process, fixture.execution)
                .unwrap(),
            1
        );
        let target = fault_binding_path(
            fixture.directory.path(),
            QueryLifecycleFaultKind::CreateTaskAckDrop,
            23,
            &fixture.token,
        );
        let original = StatementExecutionBinding::read_private(&target).unwrap();
        assert_eq!(original.backend_index, 23);
        assert_eq!(original.process().unwrap(), fixture.process);
        let replan =
            QueryExecutionId::new(fixture.execution.query_id(), AttemptId::new(2).unwrap())
                .unwrap();
        assert_eq!(
            controls
                .publish_nominated_binding(&nomination, fixture.process, replan)
                .unwrap(),
            0
        );
        assert_eq!(
            StatementExecutionBinding::read_private(&target)
                .unwrap()
                .execution()
                .unwrap(),
            fixture.execution
        );
        assert_eq!(
            role.scope()
                .observations()
                .iter()
                .filter(|observation| observation.event == "hit")
                .count(),
            1
        );
        runner.end_and_join().unwrap();
        role.close_and_join().unwrap();
    }
    #[test]
    fn first_nomination_pin_survives_no_publication_and_rejects_rearmed_generation() {
        let directory = tempfile::Builder::new()
            .prefix("g2-first-pin-")
            .tempdir_in("/tmp")
            .unwrap();
        let frontend = FrontendProcessId::new_v7();
        let token = "first-selection";
        let declaration=SessionDeclaration{schema_version:1,session_id:"first-fe".into(),launch_id:frontend.to_string(),role:SessionRole::Fe,lifetime_millis:10_000,liveness_socket:directory.path().join("fe.sock"),handshake_secret:"01234567890123456789012345678901".into(),domain_grants:["bind-create-task-ack-drop","bind-lease-renewal-ack-drop"].into_iter().enumerate().map(|(index,action)|DomainGrant{domain:"native".into(),grant_id:format!("bind-{index}"),parameters:serde_json::json!({"action":action,"root":directory.path(),"backend_index":23,"statement_run_token":token,"target_session_id":"future-be","target_process_receipt":directory.path().join("not-started.process.json")})}).collect()};
        let path = directory.path().join("fe.json");
        declaration.write_private(&path).unwrap();
        let mut runner = RunnerSession::start(declaration).unwrap();
        let mut role = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe)
            .unwrap()
            .bind(ProcessBinding::Frontend(frontend))
            .unwrap();
        let controls = NativeTestControls::from_grants(role.scope().grants_for("native")).unwrap();
        let a = StatementWorkNomination {
            target_session_id: "first-fe".into(),
            statement_run_token: token.into(),
            frontend_process_id: frontend.to_string(),
            connection_id: 19,
            session_key: 2,
            statement_generation: 3,
        };
        let backend = BackendProcessId::new_v7();
        let execution =
            QueryExecutionId::new(QueryId::new(91, 93), AttemptId::new(1).unwrap()).unwrap();
        assert!(
            controls
                .publish_nominated_binding(&a, backend, execution)
                .unwrap_err()
                .contains("not nominated")
        );
        // The receipt does not exist. Pure first selection must neither read
        // it nor consume permission; A is pinned before any work/publication.
        let input = statement_input(&a, role.scope().launch_id(), "SELECT 1");
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &a).unwrap();
        let input = statement_input(&a, role.scope().launch_id(), "SELECT 1");
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &a).unwrap();
        assert!(role.scope().observations().is_empty());
        let mut b = a.clone();
        b.statement_generation += 1;
        for _ in 0..2 {
            assert!(
                controls
                    .nominate_statement(
                        &statement_input(&b, role.scope().launch_id(), "SELECT 1"),
                        &b
                    )
                    .is_err()
            );
            assert!(
                controls
                    .publish_nominated_binding(&b, backend, execution)
                    .unwrap_err()
                    .contains("frozen")
            );
        }
        for field in 0..4 {
            let mut changed = a.clone();
            match field {
                0 => changed.connection_id += 1,
                1 => changed.session_key += 1,
                2 => changed.frontend_process_id = FrontendProcessId::new_v7().to_string(),
                _ => changed.target_session_id = "another-fe-session".into(),
            }
            assert!(
                controls
                    .nominate_statement(
                        &statement_input(&changed, role.scope().launch_id(), "SELECT 1"),
                        &changed
                    )
                    .is_err()
            );
            assert!(
                controls
                    .publish_nominated_binding(&changed, backend, execution)
                    .is_err()
            );
        }
        let input = statement_input(&a, role.scope().launch_id(), "SELECT 1");
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &a).unwrap();
        assert!(
            role.scope().observations().is_empty(),
            "pinning and refused rearm are not control actions"
        );
        assert!(
            !fault_binding_path(
                directory.path(),
                QueryLifecycleFaultKind::CreateTaskAckDrop,
                23,
                token
            )
            .exists()
        );
        runner.end_and_join().unwrap();
        role.close_and_join().unwrap();
    }
    #[test]
    fn missing_target_receipt_fails_before_a_binding_permission_is_consumed() {
        let fixture = controlled("create-task-ack-drop");
        let declaration = SessionDeclaration {
            schema_version: 1,
            session_id: "missing-receipt-fe".into(),
            launch_id: fixture.frontend.to_string(),
            role: SessionRole::Fe,
            lifetime_millis: 10_000,
            liveness_socket: fixture.directory.path().join("missing-fe.sock"),
            handshake_secret: "01234567890123456789012345678901".into(),
            domain_grants: vec![DomainGrant {
                domain: "native".into(),
                grant_id: "must-read-exact".into(),
                parameters: serde_json::json!({"action":"bind-create-task-ack-drop","root":fixture.directory.path(),"backend_index":41,"statement_run_token":fixture.token,"target_session_id":"native-tests","target_process_receipt":fixture.directory.path().join("missing-receipt.json")}),
            }],
        };
        let path = fixture.directory.path().join("missing-fe.json");
        declaration.write_private(&path).unwrap();
        let mut runner = RunnerSession::start(declaration).unwrap();
        let mut role = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe)
            .unwrap()
            .bind(ProcessBinding::Frontend(fixture.frontend))
            .unwrap();
        let controls = NativeTestControls::from_grants(role.scope().grants_for("native")).unwrap();
        let nomination = StatementWorkNomination {
            target_session_id: "missing-receipt-fe".into(),
            statement_run_token: fixture.token.clone(),
            frontend_process_id: fixture.frontend.to_string(),
            connection_id: 7,
            session_key: 1,
            statement_generation: 1,
        };
        let input = statement_input(&nomination, role.scope().launch_id(), "SELECT 1");
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &nomination).unwrap();
        assert!(
            controls
                .publish_nominated_binding(&nomination, fixture.process, fixture.execution)
                .is_err()
        );
        assert!(role.scope().observations().is_empty());
        assert!(
            !fault_binding_path(
                fixture.directory.path(),
                QueryLifecycleFaultKind::CreateTaskAckDrop,
                41,
                &fixture.token
            )
            .exists()
        );
        runner.end_and_join().unwrap();
        role.close_and_join().unwrap();
    }
    #[test]
    fn native_controls_cannot_combine_two_actual_backend_launches() {
        let first = controlled("create-task-ack-drop");
        let second = controlled("create-task-ack-drop");
        let mut grants = first.role.scope().grants_for("native");
        grants.extend(second.role.scope().grants_for("native"));
        assert!(NativeTestControls::from_grants(grants).is_err());
        assert!(first.role.scope().observations().is_empty());
        assert!(second.role.scope().observations().is_empty());
    }
    #[test]
    fn preparation_target_cannot_redirect_to_another_task_or_process() {
        let fixture = controlled("preparation-hold");
        assert!(
            fixture
                .controls
                .claim_preparation_hold(task(&fixture, 3))
                .unwrap()
                .is_none()
        );
        let exact = task(&fixture, 2);
        let other = TaskIdentity::new(
            fixture.execution,
            exact.stage_id(),
            exact.task_id(),
            BackendProcessId::new_v7(),
        );
        assert!(
            fixture
                .controls
                .claim_preparation_hold(other)
                .unwrap()
                .is_none()
        );
        assert!(fixture.role.scope().observations().is_empty());
        assert!(
            fixture
                .controls
                .claim_preparation_hold(exact)
                .unwrap()
                .is_some()
        );
    }
    #[test]
    fn after_visible_prefix_action_is_dormant_until_its_exact_activation_and_end_is_final() {
        let kind = QueryLifecycleFaultKind::TaskExecutionFailureAfterVisiblePrefix;
        let fixture = controlled(kind.as_str());
        let binding = bind_fixture(&fixture, kind);
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, false)
                .unwrap()
                .is_none()
        );
        binding
            .write_private(
                &fault_binding_path(
                    fixture.directory.path(),
                    kind,
                    0,
                    &binding.statement_run_token,
                )
                .with_extension("enable.json"),
            )
            .unwrap();
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .unwrap()
                .is_some()
        );
        end_fixture(&fixture, kind, &binding);
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn visible_prefix_dormant_reservation_uses_one_real_grant_without_activating_or_recording_hit()
    {
        let kind = QueryLifecycleFaultKind::TaskExecutionFailureAfterVisiblePrefix;
        let fixture = controlled(kind.as_str());
        assert!(
            fixture
                .controls
                .claim_dormant_after_visible_prefix(fixture.execution, fixture.process)
                .unwrap()
                .is_none()
        );
        let binding = bind_fixture(&fixture, kind);
        let foreign_execution =
            QueryExecutionId::new(QueryId::new(71, 73), AttemptId::new(2).unwrap()).unwrap();
        assert!(
            fixture
                .controls
                .claim_dormant_after_visible_prefix(foreign_execution, fixture.process)
                .unwrap()
                .is_none()
        );
        assert!(
            fixture
                .controls
                .claim_dormant_after_visible_prefix(fixture.execution, BackendProcessId::new_v7())
                .unwrap()
                .is_none()
        );
        let claim = fixture
            .controls
            .claim_dormant_after_visible_prefix(fixture.execution, fixture.process)
            .unwrap()
            .unwrap();
        let same = fixture
            .controls
            .claim_dormant_after_visible_prefix(fixture.execution, fixture.process)
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&claim.inner, &same.inner));
        assert!(
            !fixture
                .controls
                .after_visible_prefix_enabled(&claim)
                .unwrap()
        );
        for persistent in [false, true] {
            assert!(
                fixture
                    .controls
                    .claim(kind, fixture.execution, fixture.process, persistent)
                    .unwrap()
                    .is_none()
            );
        }
        let path = fault_binding_path(fixture.directory.path(), kind, 0, &fixture.token);
        assert!(!path.with_extension("hit.json").exists());
        assert!(
            !fixture
                .role
                .scope()
                .observations()
                .iter()
                .any(|event| event.event == "hit")
        );
        binding
            .write_private(&path.with_extension("enable.json"))
            .unwrap();
        assert!(
            fixture
                .controls
                .after_visible_prefix_enabled(&claim)
                .unwrap()
        );
        let enabled = fixture
            .controls
            .claim(kind, fixture.execution, fixture.process, true)
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&claim.inner, &enabled.inner));
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, false)
                .unwrap()
                .is_none()
        );
        // Activation is still not an action hit or a Task acceptance receipt.
        assert!(!path.with_extension("hit.json").exists());
        claim.record_hit().unwrap();
        let bytes = std::fs::read(path.with_extension("hit.json")).unwrap();
        enabled.record_hit().unwrap();
        assert_eq!(
            std::fs::read(path.with_extension("hit.json")).unwrap(),
            bytes
        );
        assert_eq!(
            fixture
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count(),
            1
        );
    }

    #[test]
    fn visible_prefix_dormant_activation_rejects_foreign_binding_and_foreign_claim_owner() {
        let kind = QueryLifecycleFaultKind::TaskExecutionFailureAfterVisiblePrefix;
        let fixture = controlled(kind.as_str());
        let mut binding = bind_fixture(&fixture, kind);
        let claim = fixture
            .controls
            .claim_dormant_after_visible_prefix(fixture.execution, fixture.process)
            .unwrap()
            .unwrap();
        let other = controlled(kind.as_str());
        assert!(other.controls.after_visible_prefix_enabled(&claim).is_err());
        binding.attempt_id += 1;
        let path = fault_binding_path(fixture.directory.path(), kind, 0, &fixture.token);
        binding
            .write_private(&path.with_extension("enable.json"))
            .unwrap();
        assert!(
            fixture
                .controls
                .after_visible_prefix_enabled(&claim)
                .is_err()
        );
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .is_err()
        );
        assert!(!path.with_extension("hit.json").exists());
        assert!(
            !fixture
                .role
                .scope()
                .observations()
                .iter()
                .any(|event| event.event == "hit")
        );
    }

    #[test]
    fn visible_prefix_dormant_action_end_is_sticky_even_if_the_enable_and_end_files_change() {
        let kind = QueryLifecycleFaultKind::TaskExecutionFailureAfterVisiblePrefix;
        let fixture = controlled(kind.as_str());
        let binding = bind_fixture(&fixture, kind);
        let claim = fixture
            .controls
            .claim_dormant_after_visible_prefix(fixture.execution, fixture.process)
            .unwrap()
            .unwrap();
        let ended = end_fixture(&fixture, kind, &binding);
        assert!(
            !fixture
                .controls
                .after_visible_prefix_enabled(&claim)
                .unwrap()
        );
        std::fs::remove_file(ended).unwrap();
        let path = fault_binding_path(fixture.directory.path(), kind, 0, &fixture.token);
        binding
            .write_private(&path.with_extension("enable.json"))
            .unwrap();
        assert!(
            !fixture
                .controls
                .after_visible_prefix_enabled(&claim)
                .unwrap()
        );
        assert!(
            fixture
                .controls
                .claim_dormant_after_visible_prefix(fixture.execution, fixture.process)
                .unwrap()
                .is_none()
        );
        assert!(
            fixture
                .controls
                .claim(kind, fixture.execution, fixture.process, true)
                .unwrap()
                .is_none()
        );
        assert!(!path.with_extension("hit.json").exists());
    }

    #[test]
    fn visible_prefix_dormant_role_close_disables_the_original_reservation_without_hit() {
        let kind = QueryLifecycleFaultKind::TaskExecutionFailureAfterVisiblePrefix;
        let mut fixture = controlled(kind.as_str());
        let binding = bind_fixture(&fixture, kind);
        let claim = fixture
            .controls
            .claim_dormant_after_visible_prefix(fixture.execution, fixture.process)
            .unwrap()
            .unwrap();
        fixture.runner.end_and_join().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while fixture.role.scope().is_active() && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(!fixture.role.scope().is_active());
        assert!(
            !fixture
                .controls
                .after_visible_prefix_enabled(&claim)
                .unwrap()
        );
        assert!(
            fixture
                .controls
                .claim_dormant_after_visible_prefix(fixture.execution, fixture.process)
                .unwrap()
                .is_none()
        );
        let path = fault_binding_path(
            fixture.directory.path(),
            kind,
            0,
            &binding.statement_run_token,
        );
        assert!(!path.with_extension("hit.json").exists());
    }

    #[test]
    fn role_end_disables_unclaimed_native_action() {
        let mut fixture = controlled("registry-hold");
        fixture.runner.end_and_join().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while fixture.role.scope().is_active() && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(
            fixture
                .controls
                .claim_context_hold("registry-hold", context(&fixture), None)
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn restart_placement_is_bound_to_the_first_attempt_and_closes_with_its_session() {
        let directory = tempfile::tempdir().unwrap();
        let frontend = FrontendProcessId::new_v7();
        let backend = BackendProcessId::new_v7();
        let receipt = directory.path().join("selected.process.json");
        RoleProcessReceipt {
            schema_version: 1,
            session_id: "selected-be".into(),
            launch_id: "be-launch".into(),
            role: SessionRole::Be,
            process_id: backend.to_string(),
        }
        .write_private(&receipt)
        .unwrap();
        let declaration = SessionDeclaration {
            schema_version: 1,
            session_id: "placement-fe".into(),
            launch_id: frontend.to_string(),
            role: SessionRole::Fe,
            lifetime_millis: 10_000,
            liveness_socket: directory.path().join("placement.sock"),
            handshake_secret: "01234567890123456789012345678901".into(),
            domain_grants: vec![DomainGrant {
                domain: "native".into(),
                grant_id: "restart-placement".into(),
                parameters: serde_json::json!({"action":"bind-restart-after-establish-context", "root":directory.path(), "backend_index":23, "statement_run_token":"selected-work", "target_session_id":"selected-be", "target_process_receipt":receipt}),
            }],
        };
        let path = directory.path().join("placement.json");
        declaration.write_private(&path).unwrap();
        let mut runner = RunnerSession::start(declaration).unwrap();
        let mut role = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe)
            .unwrap()
            .bind(ProcessBinding::Frontend(frontend))
            .unwrap();
        let controls = NativeTestControls::from_grants(role.scope().grants_for("native")).unwrap();
        let nomination = StatementWorkNomination {
            target_session_id: "placement-fe".into(),
            statement_run_token: "selected-work".into(),
            frontend_process_id: frontend.to_string(),
            connection_id: 19,
            session_key: 2,
            statement_generation: 3,
        };
        let first =
            QueryExecutionId::new(QueryId::new(91, 93), AttemptId::new(1).unwrap()).unwrap();
        let successor =
            QueryExecutionId::new(first.query_id(), AttemptId::new(2).unwrap()).unwrap();
        assert!(
            controls
                .preferred_backend_process(&nomination, first)
                .is_err()
        );
        let input = statement_input(&nomination, role.scope().launch_id(), "SELECT 1");
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &nomination).unwrap();
        assert_eq!(
            controls
                .preferred_backend_process(&nomination, first)
                .unwrap(),
            Some(backend)
        );
        assert!(
            role.scope().observations().is_empty(),
            "selection alone does not claim or hit a binding permission"
        );
        controls
            .publish_nominated_binding(&nomination, backend, first)
            .unwrap();
        let publication_observations = role.scope().observations();
        assert_eq!(
            publication_observations
                .iter()
                .filter(|event| event.event == "hit")
                .count(),
            1,
            "publishing the exact binding reaches the FE binding action"
        );
        assert_eq!(
            controls
                .preferred_backend_process(&nomination, first)
                .unwrap(),
            Some(backend)
        );
        assert_eq!(
            controls
                .preferred_backend_process(&nomination, successor)
                .unwrap(),
            None,
            "spent restart permission cannot constrain a successor to the old process"
        );
        assert_eq!(
            role.scope().observations().len(),
            publication_observations.len(),
            "repeated placement selection does not consume another permission"
        );
        runner.end_and_join().unwrap();
        role.close_and_join().unwrap();
        assert_eq!(
            controls
                .preferred_backend_process(&nomination, first)
                .unwrap(),
            None
        );
    }
    #[test]
    fn unbound_input_pin_refuses_client_sql_rearm_but_allows_a_fresh_declared_slot() {
        let directory = tempfile::tempdir().unwrap();
        let frontend = FrontendProcessId::new_v7();
        let declaration = SessionDeclaration {
            schema_version: 1,
            session_id: "input-pin-fe".into(),
            launch_id: frontend.to_string(),
            role: SessionRole::Fe,
            lifetime_millis: 10_000,
            liveness_socket: directory.path().join("input.sock"),
            handshake_secret: "01234567890123456789012345678901".into(),
            domain_grants: [
                ("bind-create-task-ack-drop", "original-input"),
                ("bind-lease-renewal-ack-drop", "original-input"),
                ("bind-restart-after-establish-context", "fresh-input"),
            ].into_iter().enumerate().map(|(index, (action, token))| DomainGrant {
                domain: "native".into(),
                grant_id: format!("input-{index}"),
                parameters: serde_json::json!({"action":action,"root":directory.path(),
                    "backend_index":index,"statement_run_token":token,"target_session_id":"future-be",
                    "target_process_receipt":directory.path().join("not-started.process.json")}),
            }).collect(),
        };
        let path = directory.path().join("input-fe.json");
        declaration.write_private(&path).unwrap();
        let mut runner = RunnerSession::start(declaration).unwrap();
        let mut role = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe)
            .unwrap()
            .bind(ProcessBinding::Frontend(frontend))
            .unwrap();
        let controls = NativeTestControls::from_grants(role.scope().grants_for("native")).unwrap();
        let nomination = StatementWorkNomination {
            target_session_id: role.scope().session_id().into(),
            statement_run_token: "original-input".into(),
            frontend_process_id: frontend.to_string(),
            connection_id: 7,
            session_key: 1,
            statement_generation: 1,
        };
        let input = statement_input(&nomination, role.scope().launch_id(), "SELECT 1");
        assert!(
            controls
                .nominate_statement(&input, &nomination)
                .unwrap_err()
                .contains("not pinned")
        );
        let unknown = StatementInputNomination {
            statement_run_token: "unlisted-input".into(),
            ..input.clone()
        };
        controls.pin_statement_input(&unknown).unwrap();
        assert!(
            controls
                .nominations
                .lock()
                .unwrap()
                .iter()
                .all(|pin| pin.input.is_none())
        );
        controls.pin_statement_input(&input).unwrap();
        controls.pin_statement_input(&input).unwrap();
        // No actual statement or target receipt has been bound. Draining an
        // observer cannot erase these independent finite input pins.
        assert!(
            controls
                .nominations
                .lock()
                .unwrap()
                .iter()
                .all(|pin| pin.statement.is_none())
        );
        let replacement = StatementWorkNomination {
            session_key: 2,
            ..nomination.clone()
        };
        let replacement_input = statement_input(&replacement, role.scope().launch_id(), "SELECT 1");
        let different_sql = statement_input(&nomination, role.scope().launch_id(), "SELECT 2");
        for changed in [
            replacement_input.clone(),
            different_sql.clone(),
            StatementInputNomination {
                connection_id: 8,
                ..input.clone()
            },
            StatementInputNomination {
                target_launch_id: "another-launch".into(),
                ..input.clone()
            },
            StatementInputNomination {
                target_session_id: "another-session".into(),
                ..input.clone()
            },
            StatementInputNomination {
                frontend_process_id: FrontendProcessId::new_v7().to_string(),
                ..input.clone()
            },
        ] {
            assert!(controls.pin_statement_input(&changed).is_err());
        }
        assert!(
            controls
                .nominate_statement(&replacement_input, &replacement)
                .is_err()
        );
        assert!(
            controls
                .nominate_statement(&different_sql, &nomination)
                .is_err()
        );
        assert!(controls.nominate_statement(&input, &replacement).is_err());
        let fresh = StatementWorkNomination {
            statement_run_token: "fresh-input".into(),
            ..replacement.clone()
        };
        let fresh_input = statement_input(&fresh, role.scope().launch_id(), "SELECT 2");
        controls.pin_statement_input(&fresh_input).unwrap();
        controls.nominate_statement(&fresh_input, &fresh).unwrap();
        assert!(controls.nominations.lock().unwrap()[0].statement.is_none());
        assert!(controls.nominations.lock().unwrap()[1].statement.is_none());
        assert!(role.scope().observations().is_empty());
        assert!(controls.published_once.lock().unwrap().is_empty());
        assert!(controls.claimed.lock().unwrap().is_empty());
        assert!(!directory.path().join("not-started.process.json").exists());
        runner.end_and_join().unwrap();
        role.close_and_join().unwrap();
        assert!(
            controls
                .pin_statement_input(&input)
                .unwrap_err()
                .contains("closed")
        );
        assert!(
            controls
                .nominate_statement(&input, &nomination)
                .unwrap_err()
                .contains("closed")
        );
    }
}
