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

//! Shared test vocabulary and bounded role-session protocol.
//! Mechanical declarations and runner clients do not grant product control.
//! The role receiver and finite action scopes exist only with `test-controls`;
//! domain owners retain exact target matching and product lifecycle ownership.

/// Capabilities compiled by this owner; activation still requires a role session.
pub const fn compiled_test_capabilities() -> (&'static str, &'static [&'static str]) {
    (
        env!("CARGO_PKG_NAME"),
        &[
            #[cfg(novarocks_dev_controls)]
            "test-controls",
        ],
    )
}

#[cfg(all(unix, feature = "typed"))]
pub mod liveness;
#[cfg(novarocks_dev_controls)]
pub mod scope;
pub mod session;
#[cfg(feature = "typed")]
pub mod target_binding;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub const QUERY_LIFECYCLE_FAULT_DIR_ENV: &str = "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR";
pub const CLEANUP_FAULT_DIR_ENV: &str = "NOVAROCKS_SQL_TEST_CLEANUP_FAULT_DIR";
/// Runner-owned, debug-only MV publication barrier. Its name describes the
/// durable MV boundary rather than a distributed query-lifecycle phase.
pub const MV_KNOWN_COMMITTED_BEFORE_PROJECTOR_CAS_TRIGGER: &str =
    "mv-known-committed-before-projector-cas.trigger";
pub const MV_KNOWN_COMMITTED_BEFORE_PROJECTOR_CAS_MARKER: &str =
    "mv-known-committed-before-projector-cas.marker";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum QueryLifecycleFaultKind {
    TerminalAckDrop,
    TerminalSnapshotConflict,
    RuntimeFilterContributionAckDrop,
    /// Drops an accepted Runtime Filter contribution response and asks the
    /// receiver to hold only until that exact retry reaches its ordinary
    /// dedupe owner. This runner-only fault is distinct from the generic
    /// ACK-drop probe because cancellation scenarios do not promise a retry.
    RuntimeFilterContributionAckDropRendezvous,
    RuntimeFilterFeedbackContractDigestCorrupt,
    RuntimeFilterFeedbackUnavailable,
    TaskUpdateTerminalAckDrop,
    /// Drops the acknowledgement of one `EstablishQueryContext` after the
    /// backend has installed the context. The context exists and the frontend
    /// does not know it, which is the unknown-outcome case the protocol's
    /// idempotent replay exists to survive.
    EstablishContextAckDrop,
    /// Drops the acknowledgement of one `CreateTask` after the task is
    /// admitted and running. It is the successor of the retired protocol's
    /// separate Stage and Start acknowledgements, because `CreateTask` is the
    /// task protocol's single admission point rather than two phases.
    CreateTaskAckDrop,
    /// Holds an exact ordinary Create before Worker admission for the runner.
    CreateTaskBeforeWorkerHold,
    /// Drops one `SubscribeTaskStatus` stream after the subscription is
    /// established. Cursors are unconsumed, so the observation is recovered by
    /// resubscribing rather than by re-running anything.
    TaskStatusSubscriptionDrop,
    /// Drops the acknowledgement of one applied lease renewal. It has no
    /// counterpart in the retired lifecycle: the query execution lease is a
    /// domain the task protocol introduced.
    LeaseRenewalAckDrop,
    /// Stops answering lease renewals for the rest of the attempt, rather
    /// than dropping one acknowledgement. The lease then genuinely expires and
    /// the backend must stand its tasks down -- the failure path the retired
    /// protocol's heartbeat-loss case asserted, which a single dropped
    /// acknowledgement cannot reach because the frontend simply resends it.
    LeaseRenewalStop,
    /// Fails one task's local execution after the task started.
    ///
    /// It is deliberately not an acknowledgement drop: the subject is a task
    /// that was admitted, published RUNNING, and then failed on its own, which
    /// is the only way to observe the attempt-wide stand-down a single
    /// participant's failure must cause. It never fabricates a success and
    /// never skips the create it follows.
    TaskExecutionFailure,
    /// Fails only a prepared Root Task after RUNNING and before data polling.
    RootTaskExecutionFailureBeforeData,
    /// Holds the second Root read until the exact runner activation after
    /// a complete visible client prefix, then fails the original live Task.
    /// Client observation grants no ACK or replacement Task authority.
    TaskExecutionFailureAfterVisiblePrefix,
    /// Holds one applied `EstablishQueryContext` open so the harness can
    /// replace that exact backend process.
    ///
    /// The successor of the retired `RestartAfterInitAck`: the establish is the task
    /// protocol's first per-backend admission point, so it is where a process
    /// replacement can still be observed against a context the backend really
    /// installed. It fabricates nothing -- it publishes a token-scoped marker
    /// and waits to be killed.
    ///
    /// Its arming is matched rather than consumed. The frontend's own operation
    /// wait is shorter than the hold, so it replays the establish while the
    /// rendezvous is still waiting, and answering that replay would tell the
    /// frontend this backend is ready moments before the harness replaces it.
    RestartAfterEstablishContext,
    /// Answers one admitted `CreateTask` with the protocol's first-creation
    /// refusal, `InvalidStateOrRequest`.
    ///
    /// The successor of the retired `StageConflictAfterApply`, and the same shape: the
    /// operation really applied -- the task is admitted and running on this
    /// backend -- and only the answer says otherwise. That is what makes the
    /// resulting failure a statement about the frontend's fence rather than
    /// about the backend's state, because a frontend that retried or ignored a
    /// refused create would find a working task and the query would succeed.
    ///
    /// A create replay is decided by task identity and never answers with a
    /// content conflict, so the verdict this fault forges is the one a real
    /// first creation can still receive when its body is refused.
    CreateTaskRejectedAfterApply,
    /// Makes one admitted `CreateTask` acknowledgement name a different task
    /// than the request it answers.
    ///
    /// The successor of the retired `StartDigestCorrupt`. The digest that fault corrupted
    /// has no counterpart: the task protocol has no second operation that
    /// commits a previously staged plan, and a descriptor's fingerprint is
    /// derived by the receiver from the bytes it just read, so no request field
    /// can disagree with a plan the receiver already holds. What survives is
    /// the identity half of the same fence -- `TaskIdentity` is indivisible and
    /// an answer that names another task is refused rather than adopted -- so
    /// that is the value this fault misstates.
    CreateTaskReceiptForeignTask,
    /// Makes one delivered task status event name a foreign backend process.
    ///
    /// The successor of the retired `ObservationForeignParticipant`, which swapped the
    /// `ParticipantAttemptRef` of a fragment observation on the retired control
    /// stream. The surviving observation channel is `SubscribeTaskStatus`, and
    /// the identity it carries is the event's own `TaskIdentity`, so the
    /// forgeable fact is that identity's backend process.
    TaskStatusForeignProcess,
    /// Makes one terminal logical feedback publication claim it belongs to
    /// another attempt of the same query.
    ///
    /// It replaces the retired `RuntimeFilterFeedbackForeignParticipant`,
    /// which swapped the publisher's `ParticipantAttemptRef` on the control
    /// stream. The task carrier names no publisher on the wire -- the
    /// frontend derives it from the `TaskIdentity` it fetched from -- so that
    /// forgery has no expression on the surviving carrier. The attempt the
    /// publication claims is the part a backend can still misstate, and the
    /// frontend fences it ahead of the pruning winner.
    RuntimeFilterFeedbackForeignAttempt,
    /// Fails one connector writer at commit-fragment egress. It only fails a
    /// writer that already staged its artifacts; it never substitutes a
    /// fabricated carrier, so it cannot become a production fallback.
    ConnectorWriteWriterFailure,
    /// Fails the single root aggregation at commit-fragment carrier
    /// validation. Like the writer fault it only rejects, never repairs.
    ConnectorWriteRootFailure,
    /// Refuses the first real write Root Data before offering it to the FE.
    ConnectorWriteRootReadBeforeDataFailure,
    /// Refuses the original write Root read after its first Data was consumed,
    /// before offering any second Data or End. It does not fail a Task.
    ConnectorWriteRootReadAfterPrefixFailure,
    /// Rejects an ordinary partial aggregate update inside `TableWriter`.
    ConnectorWritePartialUpdateFailure,
    /// Rejects ordinary partial aggregate finalization inside `TableWriter`.
    ConnectorWritePartialFinalizeFailure,
    /// Rejects an ordinary final aggregate merge inside `TableFinish`.
    ConnectorWriteFinalMergeFailure,
    /// Rejects ordinary final aggregate finalization inside `TableFinish`.
    ConnectorWriteFinalFinalizeFailure,
    /// Holds the first append reached by one writer until the owning attempt
    /// is cancelled. This is a debug-only rendezvous used to prove that a
    /// query deadline aborts an already-open provider writer.
    ConnectorWriteAppendHold,
}

impl QueryLifecycleFaultKind {
    pub const ALL: [Self; 30] = [
        Self::TerminalAckDrop,
        Self::TerminalSnapshotConflict,
        Self::RuntimeFilterContributionAckDrop,
        Self::RuntimeFilterContributionAckDropRendezvous,
        Self::RuntimeFilterFeedbackContractDigestCorrupt,
        Self::RuntimeFilterFeedbackUnavailable,
        Self::TaskUpdateTerminalAckDrop,
        Self::EstablishContextAckDrop,
        Self::CreateTaskAckDrop,
        Self::CreateTaskBeforeWorkerHold,
        Self::TaskStatusSubscriptionDrop,
        Self::LeaseRenewalAckDrop,
        Self::LeaseRenewalStop,
        Self::TaskExecutionFailure,
        Self::RootTaskExecutionFailureBeforeData,
        Self::TaskExecutionFailureAfterVisiblePrefix,
        Self::RestartAfterEstablishContext,
        Self::CreateTaskRejectedAfterApply,
        Self::CreateTaskReceiptForeignTask,
        Self::TaskStatusForeignProcess,
        Self::RuntimeFilterFeedbackForeignAttempt,
        Self::ConnectorWriteWriterFailure,
        Self::ConnectorWriteRootFailure,
        Self::ConnectorWriteRootReadBeforeDataFailure,
        Self::ConnectorWriteRootReadAfterPrefixFailure,
        Self::ConnectorWritePartialUpdateFailure,
        Self::ConnectorWritePartialFinalizeFailure,
        Self::ConnectorWriteFinalMergeFailure,
        Self::ConnectorWriteFinalFinalizeFailure,
        Self::ConnectorWriteAppendHold,
    ];

    pub const fn file_stem(self) -> &'static str {
        match self {
            Self::TerminalAckDrop => "terminal-ack-drop",
            Self::TerminalSnapshotConflict => "terminal-snapshot-conflict",
            Self::RuntimeFilterContributionAckDrop => "runtime-filter-contribution-ack-drop",
            Self::RuntimeFilterContributionAckDropRendezvous => {
                "runtime-filter-contribution-ack-drop-rendezvous"
            }
            Self::RuntimeFilterFeedbackContractDigestCorrupt => {
                "runtime-filter-feedback-contract-digest-corrupt"
            }
            Self::RuntimeFilterFeedbackUnavailable => "runtime-filter-feedback-unavailable",
            Self::TaskUpdateTerminalAckDrop => "task-update-terminal-ack-drop",
            Self::EstablishContextAckDrop => "establish-context-ack-drop",
            Self::CreateTaskAckDrop => "create-task-ack-drop",
            Self::CreateTaskBeforeWorkerHold => "create-task-before-worker-hold",
            Self::TaskStatusSubscriptionDrop => "task-status-subscription-drop",
            Self::LeaseRenewalAckDrop => "lease-renewal-ack-drop",
            Self::LeaseRenewalStop => "lease-renewal-stop",
            Self::TaskExecutionFailure => "task-execution-failure",
            Self::RootTaskExecutionFailureBeforeData => "root-task-execution-failure-before-data",
            Self::TaskExecutionFailureAfterVisiblePrefix => {
                "task-execution-failure-after-visible-prefix"
            }
            Self::RestartAfterEstablishContext => "restart-after-establish-context",
            Self::CreateTaskRejectedAfterApply => "create-task-rejected-after-apply",
            Self::CreateTaskReceiptForeignTask => "create-task-receipt-foreign-task",
            Self::TaskStatusForeignProcess => "task-status-foreign-process",
            Self::RuntimeFilterFeedbackForeignAttempt => "runtime-filter-feedback-foreign-attempt",
            Self::ConnectorWriteWriterFailure => "connector-write-writer-failure",
            Self::ConnectorWriteRootFailure => "connector-write-root-failure",
            Self::ConnectorWriteRootReadBeforeDataFailure => {
                "connector-write-root-read-before-data-failure"
            }
            Self::ConnectorWriteRootReadAfterPrefixFailure => {
                "connector-write-root-read-after-prefix-failure"
            }
            Self::ConnectorWritePartialUpdateFailure => "connector-write-partial-update-failure",
            Self::ConnectorWritePartialFinalizeFailure => {
                "connector-write-partial-finalize-failure"
            }
            Self::ConnectorWriteFinalMergeFailure => "connector-write-final-merge-failure",
            Self::ConnectorWriteFinalFinalizeFailure => "connector-write-final-finalize-failure",
            Self::ConnectorWriteAppendHold => "connector-write-append-hold",
        }
    }

    pub const fn as_str(self) -> &'static str {
        self.file_stem()
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.file_stem() == value)
    }
}

/// The faults a test harness may arm by name.
///
/// It is deliberately an allowlist rather than the whole declared set: a
/// harness gets the closeout faults it needs, never a generic lifecycle hook.
/// Both the SQL runner's directive vocabulary and the cluster harness's
/// arm-by-kind path read this list, so a fault that belongs to one belongs to
/// both.
pub const RUNNER_RFO_KINDS: [QueryLifecycleFaultKind; 28] = [
    QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop,
    QueryLifecycleFaultKind::RuntimeFilterContributionAckDropRendezvous,
    QueryLifecycleFaultKind::RuntimeFilterFeedbackContractDigestCorrupt,
    QueryLifecycleFaultKind::RuntimeFilterFeedbackUnavailable,
    QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop,
    // The task protocol's four core-operation faults. Each drops an
    // acknowledgement the backend already earned; none of them skips the
    // operation, because a fault that never applied anything would exercise
    // no replay path at all.
    QueryLifecycleFaultKind::EstablishContextAckDrop,
    QueryLifecycleFaultKind::CreateTaskAckDrop,
    QueryLifecycleFaultKind::TaskStatusSubscriptionDrop,
    QueryLifecycleFaultKind::LeaseRenewalAckDrop,
    QueryLifecycleFaultKind::LeaseRenewalStop,
    // The task protocol's two non-acknowledgement faults. One fails a task
    // that really started; the other holds an applied establish open so the
    // harness can replace that exact backend process. Neither can be written
    // as an acknowledgement drop, because in both cases the answer arriving
    // is not what the case is about.
    QueryLifecycleFaultKind::TaskExecutionFailure,
    QueryLifecycleFaultKind::RootTaskExecutionFailureBeforeData,
    QueryLifecycleFaultKind::TaskExecutionFailureAfterVisiblePrefix,
    QueryLifecycleFaultKind::RestartAfterEstablishContext,
    // The exact ordinary Create remains outside Worker admission until the
    // runner releases it, proving unknown ownership during root completion.
    QueryLifecycleFaultKind::CreateTaskBeforeWorkerHold,
    // The task protocol's three identity-fencing faults, successors of the
    // retired protocol's `stage-conflict-after-apply`, `start-digest-corrupt`
    // and `observation-foreign-participant`. Each misstates one fact on the
    // wire after the operation it follows has genuinely applied: the create's
    // verdict, the create acknowledgement's task, and a status event's backend
    // process. None of them skips an operation, and none fabricates a success.
    QueryLifecycleFaultKind::CreateTaskRejectedAfterApply,
    QueryLifecycleFaultKind::CreateTaskReceiptForeignTask,
    QueryLifecycleFaultKind::TaskStatusForeignProcess,
    QueryLifecycleFaultKind::RuntimeFilterFeedbackForeignAttempt,
    // The write data plane faults only ever reject at exact execution
    // boundaries; none substitutes a carrier or aggregate value.
    QueryLifecycleFaultKind::ConnectorWriteWriterFailure,
    QueryLifecycleFaultKind::ConnectorWriteRootFailure,
    QueryLifecycleFaultKind::ConnectorWriteRootReadBeforeDataFailure,
    QueryLifecycleFaultKind::ConnectorWriteRootReadAfterPrefixFailure,
    QueryLifecycleFaultKind::ConnectorWritePartialUpdateFailure,
    QueryLifecycleFaultKind::ConnectorWritePartialFinalizeFailure,
    QueryLifecycleFaultKind::ConnectorWriteFinalMergeFailure,
    QueryLifecycleFaultKind::ConnectorWriteFinalFinalizeFailure,
    QueryLifecycleFaultKind::ConnectorWriteAppendHold,
];

pub fn parse_runner_rfo_kind(value: &str) -> Option<QueryLifecycleFaultKind> {
    QueryLifecycleFaultKind::parse(value).filter(|kind| RUNNER_RFO_KINDS.contains(kind))
}

pub fn runner_rfo_kind_names() -> impl Iterator<Item = &'static str> {
    RUNNER_RFO_KINDS
        .into_iter()
        .map(QueryLifecycleFaultKind::file_stem)
}

pub fn mv_known_committed_before_projector_cas_trigger_path(root: &Path) -> PathBuf {
    root.join(MV_KNOWN_COMMITTED_BEFORE_PROJECTOR_CAS_TRIGGER)
}

pub fn mv_known_committed_before_projector_cas_marker_path(root: &Path) -> PathBuf {
    root.join(MV_KNOWN_COMMITTED_BEFORE_PROJECTOR_CAS_MARKER)
}

pub fn arm_path(root: &Path, backend_index: usize, kind: QueryLifecycleFaultKind) -> PathBuf {
    root.join(format!("be-{backend_index}.{}.arm", kind.file_stem()))
}

pub fn trigger_path(root: &Path, backend_index: usize, kind: QueryLifecycleFaultKind) -> PathBuf {
    root.join(format!("be-{backend_index}.{}.trigger", kind.file_stem()))
}

/// Token-scoped socket for the runner's exact pre-Worker Create hold.
pub fn create_before_worker_rendezvous_socket_path(token: &str) -> Result<PathBuf, String> {
    let path = restart_after_establish_rendezvous_socket_path(token)?;
    Ok(path.with_file_name(format!("nr-cbw-{token}.sock")))
}

/// Returns the short, token-scoped Unix socket name used by the debug-only
/// establish-before-restart rendezvous.
///
/// The lifecycle-fault root is intentionally not used as the socket parent:
/// worktree paths can exceed the platform Unix-socket pathname limit. The
/// runner owns binding and unlinking this path, while the BE merely connects
/// with the already validated lifecycle-fault token.
pub fn restart_after_establish_rendezvous_socket_path(token: &str) -> Result<PathBuf, String> {
    if token.is_empty()
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err("restart-after-establish rendezvous token is invalid".to_string());
    }
    // `temp_dir()` may itself be a long per-user macOS sandbox path. A Unix
    // socket is addressed by its literal pathname, so use the conventional
    // short shared temporary parent; the validated, runner-minted token keeps
    // each rendezvous name unique.
    let path = PathBuf::from("/tmp").join(format!("nr-rfo-{token}.sock"));
    if path.to_string_lossy().len() > 100 {
        return Err(format!(
            "restart-after-establish rendezvous socket path is too long: {}",
            path.display()
        ));
    }
    Ok(path)
}

/// Short runner-owned debug rendezvous for one Native ordinary handler hold.
/// The token is unique per request; the backend reports its process identity
/// over the connected socket before the runner releases that exact closure.
pub fn native_ingress_hold_socket_path(token: &str) -> Result<PathBuf, String> {
    if token.is_empty()
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err("native ingress hold token is invalid".to_string());
    }
    let path = PathBuf::from("/tmp").join(format!("nr-ni-{token}.sock"));
    if path.to_string_lossy().len() > 100 {
        return Err(format!(
            "native ingress hold socket path is too long: {}",
            path.display()
        ));
    }
    Ok(path)
}

/// Runner-owned rendezvous for holding the real Worker registry mutex.
/// Kept separate from the ordinary closure hold so their markers cannot mix.
pub fn native_registry_hold_socket_path(token: &str) -> Result<PathBuf, String> {
    if token.is_empty()
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err("native registry hold token is invalid".to_string());
    }
    let path = PathBuf::from("/tmp").join(format!("nr-nir-{token}.sock"));
    if path.to_string_lossy().len() > 100 {
        return Err(format!(
            "native registry hold socket path is too long: {}",
            path.display()
        ));
    }
    Ok(path)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum CleanupFaultKind {
    DeleteFailed,
    DropDeleteResponse,
    ReceiptWriteFailed,
    CheckpointFailed,
    KillFeAfterDelete,
}

impl CleanupFaultKind {
    pub const ALL: [Self; 5] = [
        Self::DeleteFailed,
        Self::DropDeleteResponse,
        Self::ReceiptWriteFailed,
        Self::CheckpointFailed,
        Self::KillFeAfterDelete,
    ];

    pub const fn directive_name(self) -> &'static str {
        match self {
            Self::DeleteFailed => "delete_failed",
            Self::DropDeleteResponse => "drop_delete_response",
            Self::ReceiptWriteFailed => "receipt_write_failed",
            Self::CheckpointFailed => "checkpoint_failed",
            Self::KillFeAfterDelete => "kill_fe_after_delete",
        }
    }

    pub const fn file_stem(self) -> &'static str {
        match self {
            Self::DeleteFailed => "delete-failed",
            Self::DropDeleteResponse => "drop-delete-response",
            Self::ReceiptWriteFailed => "receipt-write-failed",
            Self::CheckpointFailed => "checkpoint-failed",
            Self::KillFeAfterDelete => "kill-fe-after-delete",
        }
    }
}

pub fn parse_cleanup_fault_directive(value: &str) -> Option<CleanupFaultKind> {
    CleanupFaultKind::ALL
        .into_iter()
        .find(|kind| kind.directive_name() == value)
}

pub fn cleanup_fault_directive_names() -> impl Iterator<Item = &'static str> {
    CleanupFaultKind::ALL
        .into_iter()
        .map(CleanupFaultKind::directive_name)
}

pub fn cleanup_trigger_path(root: &Path, kind: CleanupFaultKind) -> PathBuf {
    root.join(format!("{}.trigger", kind.file_stem()))
}

pub fn claim_cleanup_fault(root: &Path, kind: CleanupFaultKind) -> Result<bool, String> {
    let trigger = cleanup_trigger_path(root, kind);
    let sequence = NEXT_CLAIM.fetch_add(1, Ordering::Relaxed);
    let claimed = root.join(format!(
        ".{}.claimed-{}-{}",
        kind.file_stem(),
        std::process::id(),
        sequence
    ));
    match fs::rename(&trigger, &claimed) {
        Ok(()) => {
            fs::remove_file(&claimed).map_err(|error| {
                format!(
                    "remove claimed cleanup fault {}: {error}",
                    claimed.display()
                )
            })?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!(
            "claim cleanup fault {}: {error}",
            trigger.display()
        )),
    }
}

static NEXT_CLAIM: AtomicU64 = AtomicU64::new(1);

#[cfg(feature = "typed")]
mod typed {
    use super::*;
    use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId};
    use std::collections::BTreeMap;
    use std::io::Write;

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct QueryLifecycleFaultScope {
        pub token: String,
        pub execution_id: QueryExecutionId,
        pub backend_index: usize,
        pub process_id: BackendProcessId,
    }

    pub fn bind_armed_fault(
        root: &Path,
        kind: QueryLifecycleFaultKind,
        execution_id: QueryExecutionId,
        backend_index: usize,
        process_id: BackendProcessId,
    ) -> Result<Option<QueryLifecycleFaultScope>, String> {
        let arm = arm_path(root, backend_index, kind);
        // Consume eligibility before publishing: a BE may claim the trigger
        // immediately, while another FE binder is still visiting this arm.
        // A unique rename gives exactly one binder ownership of the token.
        let claimed = arm.with_extension(format!(
            "arm-claimed-{}-{}",
            std::process::id(),
            NEXT_CLAIM.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::rename(&arm, &claimed) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("claim {}: {error}", arm.display())),
        }
        let result = (|| {
            let contents = fs::read_to_string(&claimed)
                .map_err(|error| format!("read {}: {error}", claimed.display()))?;
            let fields = parse_fields(&contents)?;
            let token = required_token(&fields)?;
            let armed_backend_index = required_usize(&fields, "backend_index")?;
            if armed_backend_index != backend_index {
                return Err(format!(
                    "fault arm backend_index {armed_backend_index} does not match path backend {backend_index}"
                ));
            }
            let scope = QueryLifecycleFaultScope {
                token,
                execution_id,
                backend_index,
                process_id,
            };
            let trigger = trigger_path(root, backend_index, kind);
            publish_new(&trigger, serialize_scope(&scope).as_bytes())?;
            Ok(scope)
        })();
        match result {
            Ok(scope) => {
                fs::remove_file(&claimed)
                    .map_err(|error| format!("consume {}: {error}", claimed.display()))?;
                Ok(Some(scope))
            }
            Err(error) => {
                // Restore a refused arm for diagnosis/retry without replacing
                // a newer harness arm. If restoration fails, retain the claim
                // and report its exact path instead of losing the token.
                restore_claimed_file(&claimed, &arm)
                    .map_err(|restore_error| format!("{error}; {restore_error}"))?;
                Err(error)
            }
        }
    }

    /// Matches an armed fault without consuming it.
    ///
    /// A one-shot claim answers "perturb this one operation". Some faults have
    /// to answer "keep perturbing every one of these until the attempt ends" --
    /// a lease that genuinely expires needs every renewal refused, not one.
    /// Consuming the trigger would make the second renewal succeed and the
    /// lease survive, which is the opposite of what such a case asserts.
    pub fn match_persistent_fault(
        root: &Path,
        kind: QueryLifecycleFaultKind,
        execution_id: QueryExecutionId,
        backend_index: usize,
        process_id: BackendProcessId,
    ) -> Result<Option<QueryLifecycleFaultScope>, String> {
        let trigger = trigger_path(root, backend_index, kind);
        let contents = match fs::read_to_string(&trigger) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("read {}: {error}", trigger.display())),
        };
        let scope = parse_scope(&contents)?;
        if scope.execution_id != execution_id
            || scope.backend_index != backend_index
            || scope.process_id != process_id
        {
            return Ok(None);
        }
        Ok(Some(scope))
    }

    pub fn claim_matching_fault(
        root: &Path,
        kind: QueryLifecycleFaultKind,
        execution_id: QueryExecutionId,
        backend_index: usize,
        process_id: BackendProcessId,
    ) -> Result<Option<QueryLifecycleFaultScope>, String> {
        let trigger = trigger_path(root, backend_index, kind);
        let contents = match fs::read_to_string(&trigger) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("read {}: {error}", trigger.display())),
        };
        let scope = parse_scope(&contents)?;
        if scope.execution_id != execution_id
            || scope.backend_index != backend_index
            || scope.process_id != process_id
        {
            return Ok(None);
        }
        claim_observed_scope(&trigger, scope)
    }

    /// Claims a runner fault by its authoritative process identity. The file
    /// name keeps the runner's local backend index solely as an arm location;
    /// a BE never treats that ordinal as membership identity.
    pub fn claim_matching_fault_for_process(
        root: &Path,
        kind: QueryLifecycleFaultKind,
        execution_id: QueryExecutionId,
        process_id: BackendProcessId,
    ) -> Result<Option<QueryLifecycleFaultScope>, String> {
        let suffix = format!(".{}.trigger", kind.file_stem());
        for entry in
            fs::read_dir(root).map_err(|error| format!("read {}: {error}", root.display()))?
        {
            let entry = entry.map_err(|error| format!("read {} entry: {error}", root.display()))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("be-") || !name.ends_with(&suffix) {
                continue;
            }
            let path = entry.path();
            let contents = match fs::read_to_string(&path) {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(format!("read {}: {error}", path.display())),
            };
            let scope = parse_scope(&contents)?;
            if scope.execution_id != execution_id || scope.process_id != process_id {
                continue;
            }
            if let Some(scope) = claim_observed_scope(&path, scope)? {
                return Ok(Some(scope));
            }
        }
        Ok(None)
    }

    /// Claims an arm-bound token from the first receiver that has accepted a
    /// matching runtime-filter Contribution. The arm's backend identity
    /// remains part of the stored scope for provenance, but is intentionally
    /// not an eligibility condition: remote materialization can accept the
    /// contribution on a different BE than the producer placement.
    pub fn claim_matching_receiver_agnostic_fault(
        root: &Path,
        kind: QueryLifecycleFaultKind,
        execution_id: QueryExecutionId,
    ) -> Result<Option<QueryLifecycleFaultScope>, String> {
        let suffix = format!(".{}.trigger", kind.file_stem());
        let entries =
            fs::read_dir(root).map_err(|error| format!("read {}: {error}", root.display()))?;
        for entry in entries {
            let entry = entry.map_err(|error| format!("read {} entry: {error}", root.display()))?;
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("be-") || !name.ends_with(&suffix) {
                continue;
            }
            let contents = match fs::read_to_string(&path) {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(format!("read {}: {error}", path.display())),
            };
            let scope = parse_scope(&contents)?;
            if scope.execution_id != execution_id {
                continue;
            }
            if let Some(scope) = claim_observed_scope(&path, scope)? {
                return Ok(Some(scope));
            }
        }
        Ok(None)
    }

    /// Reading selects an eligible scope; renaming, rather than unlinking,
    /// grants one claimant ownership. Revalidate after the rename so a trigger
    /// replaced between selection and claim cannot perturb another attempt.
    fn claim_observed_scope(
        trigger: &Path,
        observed: QueryLifecycleFaultScope,
    ) -> Result<Option<QueryLifecycleFaultScope>, String> {
        let claimed = trigger.with_extension(format!(
            "trigger-claimed-{}-{}",
            std::process::id(),
            NEXT_CLAIM.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::rename(trigger, &claimed) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("claim {}: {error}", trigger.display())),
        }
        let result = (|| {
            let contents = fs::read_to_string(&claimed)
                .map_err(|error| format!("read {}: {error}", claimed.display()))?;
            let scope = parse_scope(&contents)?;
            if scope != observed {
                return Err(format!(
                    "fault scope changed while claiming {}",
                    trigger.display()
                ));
            }
            Ok(scope)
        })();
        match result {
            Ok(scope) => {
                fs::remove_file(&claimed)
                    .map_err(|error| format!("consume {}: {error}", claimed.display()))?;
                Ok(Some(scope))
            }
            Err(error) => {
                restore_claimed_file(&claimed, trigger)
                    .map_err(|restore_error| format!("{error}; {restore_error}"))?;
                Err(error)
            }
        }
    }

    fn restore_claimed_file(claimed: &Path, original: &Path) -> Result<(), String> {
        // A hard link never replaces a newer arm/trigger at the original path.
        fs::hard_link(claimed, original).map_err(|error| {
            format!(
                "restore {}: {error}; retained claim {}",
                original.display(),
                claimed.display()
            )
        })?;
        fs::remove_file(claimed).map_err(|error| format!("remove {}: {error}", claimed.display()))
    }

    pub fn observe_matching_fault(
        root: &Path,
        kind: QueryLifecycleFaultKind,
        execution_id: QueryExecutionId,
        backend_index: usize,
        process_id: BackendProcessId,
    ) -> Result<Option<QueryLifecycleFaultScope>, String> {
        let trigger = trigger_path(root, backend_index, kind);
        let contents = match fs::read_to_string(&trigger) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("read {}: {error}", trigger.display())),
        };
        let scope = parse_scope(&contents)?;
        if scope.execution_id != execution_id
            || scope.backend_index != backend_index
            || scope.process_id != process_id
        {
            return Ok(None);
        }
        Ok(Some(scope))
    }

    #[cfg(test)]
    #[test]
    fn a_replaced_trigger_is_restored_instead_of_firing_on_the_old_attempt() {
        let root = super::tests::unique_temp_root("replaced-trigger");
        fs::create_dir_all(&root).unwrap();
        let kind = QueryLifecycleFaultKind::ConnectorWriteAppendHold;
        let process = BackendProcessId::new_v7();
        let execution = QueryExecutionId::new(
            novarocks_types::QueryId::new(7, 9),
            AttemptId::new(1).unwrap(),
        )
        .unwrap();
        fs::write(
            arm_path(&root, 0, kind),
            "token=original\nbackend_index=0\n",
        )
        .unwrap();
        let observed = bind_armed_fault(&root, kind, execution, 0, process)
            .unwrap()
            .unwrap();
        let replacement = QueryLifecycleFaultScope {
            token: "replacement".to_string(),
            execution_id: QueryExecutionId::new(execution.query_id(), AttemptId::new(2).unwrap())
                .unwrap(),
            ..observed.clone()
        };
        let trigger = trigger_path(&root, 0, kind);
        fs::write(&trigger, serialize_scope(&replacement)).unwrap();
        assert!(claim_observed_scope(&trigger, observed).is_err());
        assert_eq!(
            parse_scope(&fs::read_to_string(&trigger).unwrap()).unwrap(),
            replacement
        );
        assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
        fs::remove_dir_all(root).unwrap();
    }

    fn serialize_scope(scope: &QueryLifecycleFaultScope) -> String {
        format!(
            "token={}\nexecution_hi={}\nexecution_lo={}\nattempt={}\nbackend_index={}\nprocess_id={}\n",
            scope.token,
            scope.execution_id.query_id().high(),
            scope.execution_id.query_id().low(),
            scope.execution_id.attempt_id().get(),
            scope.backend_index,
            scope.process_id
        )
    }
    fn parse_scope(contents: &str) -> Result<QueryLifecycleFaultScope, String> {
        let fields = parse_fields(contents)?;
        let query_id = novarocks_types::QueryId::new(
            required_i64(&fields, "execution_hi")?,
            required_i64(&fields, "execution_lo")?,
        );
        let attempt =
            AttemptId::new(required_u64(&fields, "attempt")?).map_err(|error| error.to_string())?;
        let execution_id =
            QueryExecutionId::new(query_id, attempt).map_err(|error| error.to_string())?;
        Ok(QueryLifecycleFaultScope {
            token: required_token(&fields)?,
            execution_id,
            backend_index: required_usize(&fields, "backend_index")?,
            process_id: fields
                .get("process_id")
                .ok_or_else(|| "missing fault field \"process_id\"".to_string())?
                .parse::<BackendProcessId>()
                .map_err(|error| format!("invalid fault process_id: {error}"))?,
        })
    }
    fn parse_fields(contents: &str) -> Result<BTreeMap<&str, &str>, String> {
        let mut fields = BTreeMap::new();
        for line in contents.lines() {
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| format!("malformed fault field {line:?}"))?;
            if key.is_empty() || value.is_empty() || fields.insert(key, value).is_some() {
                return Err(format!("invalid or duplicate fault field {key:?}"));
            }
        }
        Ok(fields)
    }
    fn required_token(fields: &BTreeMap<&str, &str>) -> Result<String, String> {
        let token = fields
            .get("token")
            .ok_or_else(|| "fault scope missing token".to_string())?;
        if !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err("fault scope token is invalid".to_string());
        }
        Ok((*token).to_string())
    }
    fn required_u64(fields: &BTreeMap<&str, &str>, key: &str) -> Result<u64, String> {
        fields
            .get(key)
            .ok_or_else(|| format!("fault scope missing {key}"))?
            .parse()
            .map_err(|error| format!("invalid fault scope {key}: {error}"))
    }
    fn required_i64(fields: &BTreeMap<&str, &str>, key: &str) -> Result<i64, String> {
        fields
            .get(key)
            .ok_or_else(|| format!("fault scope missing {key}"))?
            .parse()
            .map_err(|error| format!("invalid fault scope {key}: {error}"))
    }
    fn required_usize(fields: &BTreeMap<&str, &str>, key: &str) -> Result<usize, String> {
        fields
            .get(key)
            .ok_or_else(|| format!("fault scope missing {key}"))?
            .parse()
            .map_err(|error| format!("invalid fault scope {key}: {error}"))
    }
    fn publish_new(path: &Path, contents: &[u8]) -> Result<(), String> {
        static NEXT_STAGING: AtomicU64 = AtomicU64::new(1);
        let staging = path.with_extension(format!(
            "binding-{}-{}",
            std::process::id(),
            NEXT_STAGING.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)
            .map_err(|error| format!("create {}: {error}", staging.display()))?;
        file.write_all(contents)
            .map_err(|error| format!("write {}: {error}", staging.display()))?;
        drop(file);
        if let Err(error) = fs::hard_link(&staging, path) {
            let _ = fs::remove_file(&staging);
            return Err(format!("publish {}: {error}", path.display()));
        }
        let _ = fs::remove_file(staging);
        Ok(())
    }
}

#[cfg(feature = "typed")]
pub use typed::*;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn create_before_worker_socket_has_a_distinct_validated_token_scope() {
        assert_eq!(
            create_before_worker_rendezvous_socket_path("abc-123").unwrap(),
            PathBuf::from("/tmp/nr-cbw-abc-123.sock"),
        );
        for invalid in ["", "../escape", "space token"] {
            assert!(create_before_worker_rendezvous_socket_path(invalid).is_err());
        }
        assert!(
            QueryLifecycleFaultKind::parse("create-task-before-worker-hold")
                .is_some_and(|kind| RUNNER_RFO_KINDS.contains(&kind))
        );
    }

    #[test]
    fn native_ingress_hold_socket_accepts_only_short_safe_tokens() {
        assert_eq!(
            native_ingress_hold_socket_path("abc-123").unwrap(),
            PathBuf::from("/tmp/nr-ni-abc-123.sock")
        );
        for token in ["", "../elsewhere", "with space", "other.sock", "ü"] {
            assert!(native_ingress_hold_socket_path(token).is_err());
        }
        assert!(native_ingress_hold_socket_path(&"a".repeat(100)).is_err());
    }
    #[test]
    fn native_registry_hold_socket_accepts_only_short_safe_tokens() {
        assert_eq!(
            native_registry_hold_socket_path("abc-123").unwrap(),
            PathBuf::from("/tmp/nr-nir-abc-123.sock")
        );
        for token in ["", "../elsewhere", "with space", "other.sock", "ü"] {
            assert!(native_registry_hold_socket_path(token).is_err());
        }
        assert!(native_registry_hold_socket_path(&"a".repeat(100)).is_err());
    }
    #[test]
    fn every_lifecycle_kind_round_trips_its_stable_file_stem() {
        let mut stems = std::collections::BTreeSet::new();
        for kind in QueryLifecycleFaultKind::ALL {
            assert!(stems.insert(kind.file_stem()), "duplicate fault file stem");
            assert_eq!(QueryLifecycleFaultKind::parse(kind.file_stem()), Some(kind));
        }
    }
    #[test]
    fn runner_parser_rejects_non_rfo_kinds() {
        assert_eq!(
            parse_runner_rfo_kind("create-task-before-worker-hold"),
            Some(QueryLifecycleFaultKind::CreateTaskBeforeWorkerHold)
        );
        assert_eq!(
            parse_runner_rfo_kind("runtime-filter-contribution-ack-drop"),
            Some(QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop)
        );
        assert_eq!(
            parse_runner_rfo_kind("runtime-filter-contribution-ack-drop-rendezvous"),
            Some(QueryLifecycleFaultKind::RuntimeFilterContributionAckDropRendezvous)
        );
        assert_eq!(
            parse_runner_rfo_kind("task-update-terminal-ack-drop"),
            Some(QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop)
        );
        assert_eq!(
            parse_runner_rfo_kind("establish-context-ack-drop"),
            Some(QueryLifecycleFaultKind::EstablishContextAckDrop)
        );
        assert_eq!(
            parse_runner_rfo_kind("create-task-ack-drop"),
            Some(QueryLifecycleFaultKind::CreateTaskAckDrop)
        );
        assert_eq!(
            parse_runner_rfo_kind("task-status-subscription-drop"),
            Some(QueryLifecycleFaultKind::TaskStatusSubscriptionDrop)
        );
        assert_eq!(
            parse_runner_rfo_kind("lease-renewal-ack-drop"),
            Some(QueryLifecycleFaultKind::LeaseRenewalAckDrop)
        );
        assert_eq!(
            parse_runner_rfo_kind("task-execution-failure"),
            Some(QueryLifecycleFaultKind::TaskExecutionFailure)
        );
        assert_eq!(
            parse_runner_rfo_kind("restart-after-establish-context"),
            Some(QueryLifecycleFaultKind::RestartAfterEstablishContext)
        );
        assert_eq!(
            parse_runner_rfo_kind("create-task-rejected-after-apply"),
            Some(QueryLifecycleFaultKind::CreateTaskRejectedAfterApply)
        );
        assert_eq!(
            parse_runner_rfo_kind("create-task-receipt-foreign-task"),
            Some(QueryLifecycleFaultKind::CreateTaskReceiptForeignTask)
        );
        assert_eq!(
            parse_runner_rfo_kind("task-status-foreign-process"),
            Some(QueryLifecycleFaultKind::TaskStatusForeignProcess)
        );
        assert_eq!(
            parse_runner_rfo_kind("connector-write-writer-failure"),
            Some(QueryLifecycleFaultKind::ConnectorWriteWriterFailure)
        );
        assert_eq!(
            parse_runner_rfo_kind("connector-write-root-failure"),
            Some(QueryLifecycleFaultKind::ConnectorWriteRootFailure)
        );
        assert_eq!(
            parse_runner_rfo_kind("connector-write-partial-update-failure"),
            Some(QueryLifecycleFaultKind::ConnectorWritePartialUpdateFailure)
        );
        assert_eq!(
            parse_runner_rfo_kind("connector-write-partial-finalize-failure"),
            Some(QueryLifecycleFaultKind::ConnectorWritePartialFinalizeFailure)
        );
        assert_eq!(
            parse_runner_rfo_kind("connector-write-final-merge-failure"),
            Some(QueryLifecycleFaultKind::ConnectorWriteFinalMergeFailure)
        );
        assert_eq!(
            parse_runner_rfo_kind("connector-write-final-finalize-failure"),
            Some(QueryLifecycleFaultKind::ConnectorWriteFinalFinalizeFailure)
        );
        assert_eq!(
            parse_runner_rfo_kind("connector-write-append-hold"),
            Some(QueryLifecycleFaultKind::ConnectorWriteAppendHold)
        );
        // A generic lifecycle hook stays out of reach of a harness.
        assert_eq!(parse_runner_rfo_kind("terminal-ack-drop"), None);
    }
    #[test]
    fn cleanup_directive_and_file_stem_are_explicitly_distinct() {
        let root = Path::new("fault-root");
        let kind = parse_cleanup_fault_directive("drop_delete_response").expect("directive");
        assert_eq!(kind.file_stem(), "drop-delete-response");
        assert_eq!(
            cleanup_trigger_path(root, kind),
            root.join("drop-delete-response.trigger")
        );
        assert!(parse_cleanup_fault_directive("drop-delete-response").is_none());
    }

    #[test]
    fn cleanup_token_is_claimed_once_at_its_explicit_path() {
        let root = unique_temp_root("cleanup");
        std::fs::create_dir_all(&root).expect("create root");
        let kind = CleanupFaultKind::DropDeleteResponse;
        std::fs::write(cleanup_trigger_path(&root, kind), "token=test\n").expect("write token");
        assert!(claim_cleanup_fault(&root, kind).expect("first claim"));
        assert!(!claim_cleanup_fault(&root, kind).expect("second claim"));
        std::fs::remove_dir_all(root).expect("remove root");
    }

    pub(super) fn unique_temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "novarocks-failpoint-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ))
    }

    #[cfg(feature = "typed")]
    #[test]
    fn typed_scope_keeps_the_existing_token_text_and_identity_contract() {
        use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId};

        let root = unique_temp_root("scope");
        std::fs::create_dir_all(&root).expect("create root");
        let kind = QueryLifecycleFaultKind::TerminalAckDrop;
        std::fs::write(arm_path(&root, 1, kind), "token=abc-123\nbackend_index=1\n")
            .expect("write arm");
        let execution_id = QueryExecutionId::new(
            novarocks_types::QueryId::new(7, 9),
            AttemptId::new(1).expect("attempt"),
        )
        .expect("execution id");
        let process_id = "018f3d8a-2b4c-7d6e-8f90-123456789abc"
            .parse::<BackendProcessId>()
            .expect("UUIDv7 process id");
        let scope = bind_armed_fault(&root, kind, execution_id, 1, process_id)
            .expect("bind")
            .expect("armed");
        assert_eq!(scope.token, "abc-123");
        assert_eq!(
            std::fs::read_to_string(trigger_path(&root, 1, kind)).expect("trigger"),
            "token=abc-123\nexecution_hi=7\nexecution_lo=9\nattempt=1\nbackend_index=1\nprocess_id=018f3d8a-2b4c-7d6e-8f90-123456789abc\n"
        );
        assert!(
            claim_matching_fault(&root, kind, execution_id, 1, process_id)
                .expect("claim")
                .is_some()
        );
        std::fs::remove_dir_all(root).expect("remove root");
    }

    #[cfg(feature = "typed")]
    #[test]
    fn concurrent_binders_cannot_republish_a_consumed_token() {
        use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId};
        use std::sync::{Arc, Barrier};

        let root = unique_temp_root("concurrent-bind");
        std::fs::create_dir_all(&root).unwrap();
        let kind = QueryLifecycleFaultKind::ConnectorWriteAppendHold;
        let process = BackendProcessId::new_v7();
        for round in 0..32 {
            let execution =
                QueryExecutionId::new(QueryId::new(7, round), AttemptId::new(1).unwrap()).unwrap();
            std::fs::write(arm_path(&root, 0, kind), "token=once\nbackend_index=0\n").unwrap();
            let barrier = Arc::new(Barrier::new(12));
            let outcomes = std::thread::scope(|threads| {
                let workers = (0..12)
                    .map(|_| {
                        let root = &root;
                        let barrier = Arc::clone(&barrier);
                        threads.spawn(move || {
                            barrier.wait();
                            let bound =
                                bind_armed_fault(root, kind, execution, 0, process).unwrap();
                            // Consume promptly while other binders can still be in flight.
                            let claimed =
                                claim_matching_fault(root, kind, execution, 0, process).unwrap();
                            (bound.is_some(), claimed.is_some())
                        })
                    })
                    .collect::<Vec<_>>();
                workers
                    .into_iter()
                    .map(|worker| worker.join().unwrap())
                    .collect::<Vec<_>>()
            });
            assert_eq!(outcomes.iter().filter(|(bound, _)| *bound).count(), 1);
            assert_eq!(outcomes.iter().filter(|(_, claimed)| *claimed).count(), 1);
            assert!(!arm_path(&root, 0, kind).exists());
            assert!(!trigger_path(&root, 0, kind).exists());
        }
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(feature = "typed")]
    #[test]
    fn failed_binding_restores_the_arm_without_overwriting_a_trigger() {
        use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId};

        let root = unique_temp_root("refused-bind");
        std::fs::create_dir_all(&root).unwrap();
        let kind = QueryLifecycleFaultKind::ConnectorWriteAppendHold;
        let execution =
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).unwrap()).unwrap();
        let process = BackendProcessId::new_v7();
        let arm = arm_path(&root, 0, kind);
        let trigger = trigger_path(&root, 0, kind);
        std::fs::write(&arm, "token=next\nbackend_index=0\n").unwrap();
        std::fs::write(&trigger, "existing trigger").unwrap();
        assert!(bind_armed_fault(&root, kind, execution, 0, process).is_err());
        assert_eq!(
            std::fs::read_to_string(&trigger).unwrap(),
            "existing trigger"
        );
        assert_eq!(
            std::fs::read_to_string(&arm).unwrap(),
            "token=next\nbackend_index=0\n"
        );
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 2);
        std::fs::remove_file(&trigger).unwrap();
        let scope = bind_armed_fault(&root, kind, execution, 0, process)
            .unwrap()
            .unwrap();
        assert_eq!(scope.token, "next");
        assert!(!arm.exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(feature = "typed")]
    #[test]
    fn receiver_agnostic_contribution_fault_claims_once_for_the_exact_execution() {
        use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId};

        let root = unique_temp_root("receiver-agnostic");
        std::fs::create_dir_all(&root).expect("create root");
        let kind = QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop;
        std::fs::write(
            arm_path(&root, 1, kind),
            "token=ack-drop\nbackend_index=1\n",
        )
        .expect("write arm");
        let execution_id = QueryExecutionId::new(
            novarocks_types::QueryId::new(7, 9),
            AttemptId::new(1).expect("attempt"),
        )
        .expect("execution id");
        let other_execution_id = QueryExecutionId::new(
            novarocks_types::QueryId::new(7, 10),
            AttemptId::new(1).expect("attempt"),
        )
        .expect("other execution id");
        let process_id = "018f3d8a-2b4c-7d6e-8f90-123456789abc"
            .parse::<BackendProcessId>()
            .expect("UUIDv7 process id");
        bind_armed_fault(&root, kind, execution_id, 1, process_id)
            .expect("bind")
            .expect("armed");

        assert!(
            claim_matching_receiver_agnostic_fault(&root, kind, other_execution_id)
                .expect("different query cannot claim")
                .is_none()
        );
        let claimed = claim_matching_receiver_agnostic_fault(&root, kind, execution_id)
            .expect("matching accepted receiver can claim")
            .expect("matching execution claims");
        assert_eq!(claimed.backend_index, 1);
        assert_eq!(claimed.process_id, process_id);
        assert!(
            claim_matching_receiver_agnostic_fault(&root, kind, execution_id)
                .expect("second receiver observes consumed token")
                .is_none()
        );
        std::fs::remove_dir_all(root).expect("remove root");
    }
}

#[cfg(all(test, unix, novarocks_dev_controls))]
mod session_tests;

// Refuse flags that would make capability reporting disagree with compiled seams.
#[cfg(any(
    all(novarocks_dev_controls, not(debug_assertions)),
    all(not(novarocks_dev_controls), debug_assertions)
))]
compile_error!("novarocks_dev_controls must match debug_assertions");
