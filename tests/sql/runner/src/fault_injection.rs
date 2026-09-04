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

use crate::cluster::ServerHandle;
use crate::types::QueryMeta;
use anyhow::{Context, Result, bail};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, sleep};
use std::time::{Duration, Instant};

#[cfg(not(test))]
const POST_FRAGMENT_START_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const POST_FRAGMENT_START_TIMEOUT: Duration = Duration::from_secs(1);
const POST_FRAGMENT_START_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// The armed kind name of the task protocol's establish rendezvous.
///
/// It is armed through the generic lifecycle-fault hook, which takes the kind
/// by name, so the runner and the backend agree on this one string.
const RESTART_AFTER_ESTABLISH_CONTEXT: &str = "restart-after-establish-context";
/// The token-scoped marker that rendezvous publishes.
const TASK_ESTABLISH_CONTEXT_OBSERVED: &str = "NOVAROCKS_TASK_ESTABLISH_CONTEXT_OBSERVED";
/// The retired lifecycle protocol's restart rendezvous marker.
const QUERY_INIT_ACK_OBSERVED: &str = "NOVAROCKS_QUERY_INIT_ACK_OBSERVED";

/// What a replaced backend process must not be found doing.
///
/// A restart proof is only as good as the markers it looks for: a fresh
/// process that re-published the old attempt's admission evidence would have
/// restored state it must never restore, and a marker family that names its
/// process identity differently has to be read with that family's own field
/// name. Both are protocol facts, so both are named per protocol rather than
/// assumed.
#[derive(Copy, Clone)]
struct RestartNonRestoreContract {
    kind: &'static str,
    forbidden: &'static [&'static str],
    family_prefix: &'static str,
    identity_field: &'static str,
}

/// The retired Init/Stage/Start protocol's contract.
const LIFECYCLE_RESTART_CONTRACT: RestartNonRestoreContract = RestartNonRestoreContract {
    kind: "be-restart",
    forbidden: &[
        "NOVAROCKS_QUERY_CONTROL_READY",
        "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED",
        "NOVAROCKS_QUERY_INIT_APPLIED",
    ],
    family_prefix: "NOVAROCKS_QUERY_",
    identity_field: "process_id",
};

/// The task protocol's contract.
///
/// The two forbidden markers are the protocol's only two admission points: a
/// context this process established, and a task it admitted. Its marker family
/// names the backend process in `backend=` rather than `process_id=`, because
/// a task-protocol identity is a component of the operation's identity rather
/// than a separate attribution field.
const TASK_RESTART_CONTRACT: RestartNonRestoreContract = RestartNonRestoreContract {
    kind: "be-restart-after-establish-context",
    forbidden: &[
        "NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED",
        "NOVAROCKS_TASK_CREATE_APPLIED",
    ],
    family_prefix: "NOVAROCKS_TASK_",
    identity_field: "backend",
};

const QUERY_RUNNING: u8 = 0;
const FAULT_CLAIMED: u8 = 1;
const QUERY_DONE: u8 = 2;

struct ActiveQueryFaultState {
    state: AtomicU8,
}

impl ActiveQueryFaultState {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(QUERY_RUNNING),
        }
    }

    fn claim_fault(&self) -> bool {
        self.state
            .compare_exchange(
                QUERY_RUNNING,
                FAULT_CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn mark_query_done(&self) -> bool {
        self.state.swap(QUERY_DONE, Ordering::AcqRel) != QUERY_DONE
    }

    fn query_is_done(&self) -> bool {
        self.state.load(Ordering::Acquire) == QUERY_DONE
    }
}

pub(crate) type SharedServerHandle = Arc<Mutex<Box<dyn ServerHandle>>>;

pub(crate) struct FragmentFailureStepGuard {
    target: Option<(SharedServerHandle, usize)>,
}

pub(crate) fn fragment_failure_step_guard(
    meta: &QueryMeta,
    server: Arc<Mutex<Box<dyn ServerHandle>>>,
) -> FragmentFailureStepGuard {
    FragmentFailureStepGuard {
        target: meta
            .fail_fragment_after_start_be_index
            .map(|index| (server, index)),
    }
}

impl Drop for FragmentFailureStepGuard {
    fn drop(&mut self) {
        let Some((server, index)) = self.target.take() else {
            return;
        };
        let mut server = match server.lock() {
            Ok(server) => server,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Err(error) = server.disarm_fragment_executor_failure(index) {
            eprintln!(
                "failed to disarm fragment executor failure for BE[{index}] after SQL step: {error:#}"
            );
        }
    }
}

pub(crate) struct QueryLifecycleFaultStepGuard {
    server: Option<Arc<Mutex<Box<dyn ServerHandle>>>>,
}

pub(crate) struct CleanupFaultStepGuard {
    server: Option<Arc<Mutex<Box<dyn ServerHandle>>>>,
}

pub(crate) fn cleanup_fault_step_guard(
    meta: &QueryMeta,
    server: Arc<Mutex<Box<dyn ServerHandle>>>,
) -> CleanupFaultStepGuard {
    CleanupFaultStepGuard {
        server: meta.cleanup_fault.is_some().then_some(server),
    }
}

impl Drop for CleanupFaultStepGuard {
    fn drop(&mut self) {
        let Some(server) = self.server.take() else {
            return;
        };
        let mut server = match server.lock() {
            Ok(server) => server,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Err(error) = server.clear_cleanup_faults() {
            eprintln!("failed to clear connector cleanup fault triggers after SQL step: {error:#}");
        }
    }
}

pub(crate) fn query_lifecycle_fault_step_guard(
    meta: &QueryMeta,
    server: Arc<Mutex<Box<dyn ServerHandle>>>,
) -> QueryLifecycleFaultStepGuard {
    let armed = meta.drop_next_init_ack_be_index.is_some()
        || meta.stop_query_control_heartbeat_be_index.is_some()
        || meta.kill_fe_after_control_ready_count.is_some()
        || meta.kill_fe_after_mv_known_committed_before_projector_cas
        || meta.restart_be_after_init_ack_index.is_some()
        || meta.restart_be_after_establish_context_index.is_some()
        || meta.kill_query_after_control_ready_count.is_some()
        || meta.kill_query_after_be_log_contains.is_some()
        || meta.kill_fe_after_be_log_contains.is_some()
        || meta.kill_be_after_be_log_contains.is_some()
        || meta.fail_stage_prepare_ordinal.is_some()
        || meta.drop_next_stage_ack_be_index.is_some()
        || meta.drop_next_start_ack_be_index.is_some()
        || meta.suppress_start_ack_be_index.is_some()
        || meta.drop_next_terminal_ack_be_index.is_some()
        || meta.drop_terminal_snapshot_stream_be_index.is_some()
        || meta.terminal_snapshot_conflict_be_index.is_some()
        || !configured_query_lifecycle_faults(meta).is_empty()
        || meta.kill_query_at_lifecycle_phase.is_some()
        || meta.kill_fe_at_lifecycle_phase.is_some()
        || meta.kill_be_at_lifecycle_phase.is_some()
        || meta
            .stop_query_control_heartbeat_after_stage_be_index
            .is_some()
        || meta.hold_start_until_early_ingress
        || meta.query_control_fragment_backend_limit.is_some();
    QueryLifecycleFaultStepGuard {
        server: armed.then_some(server),
    }
}

impl Drop for QueryLifecycleFaultStepGuard {
    fn drop(&mut self) {
        let Some(server) = self.server.take() else {
            return;
        };
        let mut server = match server.lock() {
            Ok(server) => server,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Err(error) = server.clear_query_lifecycle_faults() {
            eprintln!("failed to clear query lifecycle fault triggers after SQL step: {error:#}");
        }
    }
}

pub(crate) fn has_fault(meta: &QueryMeta) -> bool {
    meta.cleanup_fault.is_some()
        || meta.kill_be_index.is_some()
        || meta.kill_be_after_fragment_start.is_some()
        || meta.fail_fragment_after_start_be_index.is_some()
        || meta.network_partition_be.is_some()
        || meta.heartbeat_delay_ms.is_some()
        || meta.restart_be_delay_ms.is_some()
        || meta.drop_next_init_ack_be_index.is_some()
        || meta.stop_query_control_heartbeat_be_index.is_some()
        || meta.kill_fe_after_control_ready_count.is_some()
        || meta.kill_fe_after_mv_known_committed_before_projector_cas
        || meta.restart_be_after_init_ack_index.is_some()
        || meta.restart_be_after_establish_context_index.is_some()
        || meta.kill_query_after_control_ready_count.is_some()
        || meta.kill_query_after_be_log_contains.is_some()
        || meta.kill_fe_after_be_log_contains.is_some()
        || meta.kill_be_after_be_log_contains.is_some()
        || meta.fail_stage_prepare_ordinal.is_some()
        || meta.drop_next_stage_ack_be_index.is_some()
        || meta.drop_next_start_ack_be_index.is_some()
        || meta.suppress_start_ack_be_index.is_some()
        || meta.drop_next_terminal_ack_be_index.is_some()
        || meta.drop_terminal_snapshot_stream_be_index.is_some()
        || meta.terminal_snapshot_conflict_be_index.is_some()
        || !configured_query_lifecycle_faults(meta).is_empty()
        || meta.kill_query_at_lifecycle_phase.is_some()
        || meta.kill_fe_at_lifecycle_phase.is_some()
        || meta.kill_be_at_lifecycle_phase.is_some()
        || meta
            .stop_query_control_heartbeat_after_stage_be_index
            .is_some()
        || meta.hold_start_until_early_ingress
        || meta.query_control_fragment_backend_limit.is_some()
}

fn configured_query_lifecycle_faults(
    meta: &QueryMeta,
) -> Vec<crate::types::QueryLifecycleFaultDirective> {
    if meta.query_lifecycle_faults.is_empty() {
        meta.query_lifecycle_fault.into_iter().collect()
    } else {
        meta.query_lifecycle_faults.clone()
    }
}

/// A frontend crash may leave bounded terminal delivery records after the FE
/// process is restarted. They are not execution resources and must therefore
/// be checked against their published limits rather than a pre-fault zero.
pub(crate) fn permits_terminal_retention(meta: &QueryMeta) -> bool {
    meta.kill_fe_after_control_ready_count.is_some()
        || meta.kill_fe_after_mv_known_committed_before_projector_cas
        || meta.kill_fe_at_lifecycle_phase.is_some()
        || meta.kill_fe_after_be_log_contains.is_some()
}

pub(crate) fn apply_pre_query(meta: &QueryMeta, server: &mut dyn ServerHandle) -> Result<()> {
    if let Some(kind) = &meta.cleanup_fault {
        server.arm_cleanup_fault(kind)?;
    }
    let fragment_fault_count = [
        meta.kill_be_index.is_some(),
        meta.kill_be_after_fragment_start.is_some(),
        meta.kill_be_after_be_log_contains.is_some(),
        meta.fail_fragment_after_start_be_index.is_some(),
    ]
    .into_iter()
    .filter(|configured| *configured)
    .count();
    if fragment_fault_count > 1 {
        bail!(
            "a SQL step may configure at most one fragment fault directive: kill_be_index, kill_be_after_fragment_start, kill_be_after_be_log_contains, or fail_fragment_after_start_be_index"
        );
    }

    let lifecycle_fault_count = [
        meta.drop_next_init_ack_be_index.is_some(),
        meta.stop_query_control_heartbeat_be_index.is_some(),
        meta.kill_fe_after_control_ready_count.is_some(),
        meta.kill_fe_after_mv_known_committed_before_projector_cas,
        meta.restart_be_after_init_ack_index.is_some(),
        meta.restart_be_after_establish_context_index.is_some(),
        meta.kill_query_after_control_ready_count.is_some(),
        meta.kill_query_after_be_log_contains.is_some(),
        meta.kill_fe_after_be_log_contains.is_some(),
        meta.fail_stage_prepare_ordinal.is_some(),
        meta.drop_next_stage_ack_be_index.is_some(),
        meta.drop_next_start_ack_be_index.is_some(),
        meta.suppress_start_ack_be_index.is_some(),
        meta.drop_next_terminal_ack_be_index.is_some(),
        meta.drop_terminal_snapshot_stream_be_index.is_some(),
        meta.terminal_snapshot_conflict_be_index.is_some(),
        !configured_query_lifecycle_faults(meta).is_empty(),
        meta.kill_query_at_lifecycle_phase.is_some(),
        meta.kill_fe_at_lifecycle_phase.is_some(),
        meta.kill_be_at_lifecycle_phase.is_some(),
        meta.stop_query_control_heartbeat_after_stage_be_index
            .is_some(),
    ]
    .into_iter()
    .filter(|configured| *configured)
    .count();
    let start_ack_and_terminal_ack_compound = meta.suppress_start_ack_be_index.is_some()
        && meta.drop_next_terminal_ack_be_index.is_some()
        && lifecycle_fault_count == 2;
    // A terminal-retained BE kill is intentionally composable with one or
    // more owner-local RFO arms. The arms hold one participant's terminal
    // delivery open; the FE phase barrier then proves another participant is
    // already finalizing before the process death is released.
    let terminal_kill_and_rfo_compound = meta.kill_be_at_lifecycle_phase.is_some()
        && !configured_query_lifecycle_faults(meta).is_empty()
        && lifecycle_fault_count == 2;
    if lifecycle_fault_count > 1
        && !start_ack_and_terminal_ack_compound
        && !terminal_kill_and_rfo_compound
    {
        bail!(
            "a SQL step may configure at most one query lifecycle fault directive; hold_start_until_early_ingress is schedule-shaping, persistent StartAck suppression may be combined with one TerminalAck drop, and a terminal-retained BE kill may be combined with RFO terminal delivery arms"
        );
    }

    if let Some(index) = meta.network_partition_be {
        bail!(
            "network_partition_be is unsupported by the SQL test runner in Task 7.1 (index={index})"
        );
    }

    if meta.restart_be_delay_ms.is_some() && meta.kill_be_index.is_none() {
        bail!("restart_be_delay_ms requires kill_be_index so the runner knows which BE to restart");
    }

    if has_fault(meta) && !server.supports_fault_injection() {
        bail!(
            "fault injection directives require a mutable cross-process server handle; current server mode does not support fault injection"
        );
    }

    let be_count = server.be_count();
    for (name, index) in [
        (
            "drop_next_init_ack_be_index",
            meta.drop_next_init_ack_be_index,
        ),
        (
            "stop_query_control_heartbeat_be_index",
            meta.stop_query_control_heartbeat_be_index,
        ),
        (
            "restart_be_after_init_ack_index",
            meta.restart_be_after_init_ack_index,
        ),
        (
            "restart_be_after_establish_context_index",
            meta.restart_be_after_establish_context_index,
        ),
        (
            "drop_next_stage_ack_be_index",
            meta.drop_next_stage_ack_be_index,
        ),
        (
            "drop_next_start_ack_be_index",
            meta.drop_next_start_ack_be_index,
        ),
        (
            "suppress_start_ack_be_index",
            meta.suppress_start_ack_be_index,
        ),
        (
            "drop_next_terminal_ack_be_index",
            meta.drop_next_terminal_ack_be_index,
        ),
        (
            "drop_terminal_snapshot_stream_be_index",
            meta.drop_terminal_snapshot_stream_be_index,
        ),
        (
            "terminal_snapshot_conflict_be_index",
            meta.terminal_snapshot_conflict_be_index,
        ),
        (
            "stop_query_control_heartbeat_after_stage_be_index",
            meta.stop_query_control_heartbeat_after_stage_be_index,
        ),
    ] {
        if let Some(index) = index
            && index >= be_count
        {
            bail!("{name} {index} is out of bounds for {be_count} BE(s)");
        }
    }
    if let Some(count) = meta.kill_fe_after_control_ready_count
        && !(1..=be_count).contains(&count)
    {
        bail!("kill_fe_after_control_ready_count must be between 1 and {be_count}, got {count}");
    }
    if let Some(count) = meta.kill_query_after_control_ready_count
        && !(1..=be_count).contains(&count)
    {
        bail!("kill_query_after_control_ready_count must be between 1 and {be_count}, got {count}");
    }
    if let Some(ordinal) = meta.fail_stage_prepare_ordinal
        && ordinal == 0
    {
        bail!("fail_stage_prepare_ordinal must be at least 1, got {ordinal}");
    }
    if let Some(limit) = meta.query_control_fragment_backend_limit
        && !(1..=be_count).contains(&limit)
    {
        bail!("query_control_fragment_backend_limit must be between 1 and {be_count}, got {limit}");
    }
    for fault in configured_query_lifecycle_faults(meta) {
        if fault.be_index >= be_count {
            bail!(
                "query_lifecycle_fault {} BE index {} is out of bounds for {be_count} BE(s)",
                fault.kind.as_str(),
                fault.be_index
            );
        }
    }
    if let Some(fault) = &meta.kill_be_after_be_log_contains
        && fault.be_index >= be_count
    {
        bail!(
            "kill_be_after_be_log_contains BE index {} is out of bounds for {be_count} BE(s)",
            fault.be_index
        );
    }
    if let Some(fault) = meta.kill_be_at_lifecycle_phase
        && fault.be_index >= be_count
    {
        bail!(
            "kill_be_at_lifecycle_phase BE index {} is out of bounds for {be_count} BE(s)",
            fault.be_index
        );
    }

    if let Some(index) = meta.drop_next_init_ack_be_index {
        server.arm_init_ack_drop(index)?;
    }
    if let Some(index) = meta.stop_query_control_heartbeat_be_index {
        server.arm_query_control_heartbeat_stop(index)?;
    }
    if let Some(count) = meta.kill_fe_after_control_ready_count {
        server.arm_fe_crash_after_control_ready(count)?;
    }
    if meta.kill_fe_after_mv_known_committed_before_projector_cas {
        server.arm_mv_known_committed_before_projector_cas()?;
    }
    if let Some(index) = meta.restart_be_after_init_ack_index {
        server.arm_be_restart_after_init_ack(index)?;
    }
    if let Some(index) = meta.restart_be_after_establish_context_index {
        // Armed through the generic lifecycle-fault hook rather than through a
        // dedicated harness method: the arm file, its token and its cleanup
        // are already the same for every kind, and a second bespoke method
        // would only duplicate them under a new name.
        server.arm_query_lifecycle_fault(index, RESTART_AFTER_ESTABLISH_CONTEXT)?;
    }
    if let Some(ordinal) = meta.fail_stage_prepare_ordinal {
        server.arm_stage_prepare_failure(ordinal)?;
    }
    if let Some(index) = meta.drop_next_stage_ack_be_index {
        server.arm_stage_ack_drop(index)?;
    }
    if let Some(index) = meta.drop_next_start_ack_be_index {
        server.arm_start_ack_drop(index)?;
    }
    if let Some(index) = meta.suppress_start_ack_be_index {
        server.arm_start_ack_suppress(index)?;
    }
    if let Some(index) = meta.drop_next_terminal_ack_be_index {
        server.arm_terminal_ack_drop(index)?;
    }
    if let Some(index) = meta.drop_terminal_snapshot_stream_be_index {
        server.arm_terminal_snapshot_stream_drop(index)?;
    }
    if let Some(index) = meta.terminal_snapshot_conflict_be_index {
        server.arm_terminal_snapshot_conflict(index)?;
    }
    for fault in configured_query_lifecycle_faults(meta) {
        server.arm_query_lifecycle_fault(fault.be_index, fault.kind.as_str())?;
    }
    if let Some(phase) = meta.kill_query_at_lifecycle_phase {
        server.arm_kill_query_at_lifecycle_phase(phase)?;
    }
    if let Some(phase) = meta.kill_fe_at_lifecycle_phase {
        server.arm_fe_crash_at_lifecycle_phase(phase)?;
    }
    if let Some(fault) = meta.kill_be_at_lifecycle_phase {
        server.arm_be_kill_at_lifecycle_phase(fault.phase)?;
    }
    if let Some(index) = meta.stop_query_control_heartbeat_after_stage_be_index {
        server.arm_query_control_heartbeat_stop_after_stage(index)?;
    }
    if meta.hold_start_until_early_ingress {
        server.arm_hold_start_until_early_ingress()?;
    }
    if let Some(limit) = meta.query_control_fragment_backend_limit {
        server.arm_query_control_fragment_backend_limit(limit)?;
    }

    if let Some(index) = meta.fail_fragment_after_start_be_index {
        server.arm_fragment_executor_failure(index)?;
    }

    if let Some(index) = meta.kill_be_index {
        server.kill_be(index)?;
        if let Some(delay_ms) = meta.restart_be_delay_ms {
            sleep(Duration::from_millis(delay_ms));
            server.restart_be(index)?;
        }
    }

    if let Some(delay_ms) = meta.heartbeat_delay_ms {
        sleep(Duration::from_millis(delay_ms));
    }

    Ok(())
}

pub(crate) fn execute_with_post_fragment_start_fault<T, F>(
    meta: &QueryMeta,
    server: &Arc<Mutex<Box<dyn ServerHandle>>>,
    query_connection_id: Option<u32>,
    shared_deadline: Option<Instant>,
    execute_query: F,
) -> Result<T>
where
    F: FnOnce() -> T,
{
    #[derive(Clone)]
    enum PostQueryFault {
        KillBackend(usize),
        ReleaseFragmentFailure(usize),
        KillFrontendAfterControlReady(usize),
        KillFrontendAfterMvKnownCommittedBeforeProjectorCas,
        RestartBackendAfterInitAck(usize),
        RestartBackendAfterEstablishContext(usize),
        KillFrontendAfterBeLogContains {
            pattern: String,
        },
        KillBackendAfterBeLogContains {
            index: usize,
            pattern: String,
        },
        KillQueryAfterControlReady {
            ready_count: usize,
            connection_id: u32,
        },
        KillQueryAfterBeLogContains {
            pattern: String,
            connection_id: u32,
        },
        KillQueryAtLifecyclePhase {
            phase: crate::types::QueryLifecyclePhase,
            connection_id: u32,
        },
        KillFrontendAtLifecyclePhase(crate::types::QueryLifecyclePhase),
        KillBackendAtLifecyclePhase {
            index: usize,
            phase: crate::types::QueryLifecyclePhase,
        },
    }

    enum FaultBaseline {
        ScheduledFragments(Vec<(usize, u64)>),
        FrontendStage {
            marker_count: u64,
        },
        FrontendReady {
            ready_count: u64,
            coordinator_lost: Vec<u64>,
        },
        BackendInit {
            index: usize,
            token: String,
            process_id: novarocks_types::BackendProcessId,
            /// The token-scoped rendezvous marker of this fault's protocol.
            marker: &'static str,
        },
        FrontendPhase {
            phase: crate::types::QueryLifecyclePhase,
            fe_crash: bool,
            marker_count: u64,
        },
        MvKnownCommittedBeforeProjectorCas {
            marker_count: u64,
        },
        BeLogPattern {
            pattern: String,
            counts: Vec<usize>,
        },
        /// A pattern that must appear on one named backend.
        ///
        /// A kill aimed at one process has to wait for that process to reach
        /// the named point. Accepting the line from any backend would let the
        /// kill land on a backend the attempt had not reached yet, which
        /// proves nothing about losing a running participant.
        BeLogPatternOnBackend {
            index: usize,
            pattern: String,
            count: usize,
        },
    }

    let faults = [
        meta.kill_be_after_fragment_start
            .map(PostQueryFault::KillBackend),
        meta.fail_fragment_after_start_be_index
            .map(PostQueryFault::ReleaseFragmentFailure),
        meta.kill_fe_after_control_ready_count
            .map(PostQueryFault::KillFrontendAfterControlReady),
        meta.kill_fe_after_mv_known_committed_before_projector_cas
            .then_some(PostQueryFault::KillFrontendAfterMvKnownCommittedBeforeProjectorCas),
        meta.restart_be_after_init_ack_index
            .map(PostQueryFault::RestartBackendAfterInitAck),
        meta.restart_be_after_establish_context_index
            .map(PostQueryFault::RestartBackendAfterEstablishContext),
        meta.kill_fe_after_be_log_contains
            .as_deref()
            .map(|pattern| PostQueryFault::KillFrontendAfterBeLogContains {
                pattern: pattern.to_string(),
            }),
        meta.kill_be_after_be_log_contains.as_ref().map(|fault| {
            PostQueryFault::KillBackendAfterBeLogContains {
                index: fault.be_index,
                pattern: fault.pattern.clone(),
            }
        }),
        meta.kill_query_after_control_ready_count
            .map(|ready_count| {
                query_connection_id
                    .map(|connection_id| PostQueryFault::KillQueryAfterControlReady {
                        ready_count,
                        connection_id,
                    })
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "kill_query_after_control_ready_count requires the target query connection id"
                        )
                    })
            })
            .transpose()?,
        meta.kill_query_after_be_log_contains.as_deref().map(|pattern| {
            query_connection_id
                .map(|connection_id| PostQueryFault::KillQueryAfterBeLogContains {
                    pattern: pattern.to_string(),
                    connection_id,
                })
                .ok_or_else(|| anyhow::anyhow!(
                    "kill_query_after_be_log_contains requires the target query connection id"
                ))
        }).transpose()?,
        meta.kill_query_at_lifecycle_phase
            .map(|phase| {
                query_connection_id
                    .map(|connection_id| PostQueryFault::KillQueryAtLifecyclePhase {
                        phase,
                        connection_id,
                    })
                    .ok_or_else(|| anyhow::anyhow!(
                        "kill_query_at_lifecycle_phase requires the target query connection id"
                    ))
            })
            .transpose()?,
        meta.kill_fe_at_lifecycle_phase
            .map(PostQueryFault::KillFrontendAtLifecyclePhase),
        meta.kill_be_at_lifecycle_phase.map(|fault| {
            PostQueryFault::KillBackendAtLifecyclePhase {
                index: fault.be_index,
                phase: fault.phase,
            }
        }),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    let [fault] = faults.as_slice() else {
        if faults.is_empty() {
            return Ok(execute_query());
        }
        bail!("a SQL step may configure at most one post-query lifecycle fault");
    };
    let fault = fault.clone();
    let baseline = {
        let server = server
            .lock()
            .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
        if !server.supports_fault_injection() {
            bail!("post-query faults require a mutable cross-process server handle");
        }
        match fault.clone() {
            PostQueryFault::KillBackend(index) => {
                if index >= server.be_count() {
                    bail!(
                        "post-query fault index {index} is out of bounds for {} BE(s)",
                        server.be_count()
                    );
                }
                FaultBaseline::ScheduledFragments(vec![(
                    index,
                    server.scheduled_fragment_count(index)?,
                )])
            }
            PostQueryFault::ReleaseFragmentFailure(index) => {
                if index >= server.be_count() {
                    bail!(
                        "post-query fault index {index} is out of bounds for {} BE(s)",
                        server.be_count()
                    );
                }
                FaultBaseline::FrontendStage {
                    marker_count: server.fe_log_count("NOVAROCKS_QUERY_STAGE_BARRIER")? as u64,
                }
            }
            PostQueryFault::KillFrontendAfterControlReady(_) => FaultBaseline::FrontendReady {
                ready_count: server.fe_log_count("NOVAROCKS_QUERY_CONTROL_READY")? as u64,
                coordinator_lost: (0..server.be_count())
                    .map(|index| {
                        server.be_log_count(index, "NOVAROCKS_QUERY_CONTROL_COORDINATOR_LOST")
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .map(|count| count as u64)
                    .collect(),
            },
            PostQueryFault::KillQueryAfterControlReady { .. } => FaultBaseline::FrontendReady {
                ready_count: server.fe_log_count("NOVAROCKS_QUERY_CONTROL_READY")? as u64,
                coordinator_lost: Vec::new(),
            },
            PostQueryFault::KillBackendAfterBeLogContains { index, .. }
                if index >= server.be_count() =>
            {
                bail!(
                    "post-query fault index {index} is out of bounds for {} BE(s)",
                    server.be_count()
                );
            }
            PostQueryFault::KillBackendAfterBeLogContains { index, pattern } => {
                FaultBaseline::BeLogPatternOnBackend {
                    count: server.be_log_count(index, &pattern)?,
                    index,
                    pattern,
                }
            }
            PostQueryFault::KillQueryAfterBeLogContains { pattern, .. }
            | PostQueryFault::KillFrontendAfterBeLogContains { pattern } => {
                let counts = (0..server.be_count())
                    .map(|index| server.be_log_count(index, &pattern))
                    .collect::<Result<Vec<_>>>()?;
                FaultBaseline::BeLogPattern { pattern, counts }
            }
            PostQueryFault::RestartBackendAfterInitAck(index) => {
                if index >= server.be_count() {
                    bail!(
                        "post-query fault index {index} is out of bounds for {} BE(s)",
                        server.be_count()
                    );
                }
                FaultBaseline::BackendInit {
                    index,
                    token: server
                        .armed_query_lifecycle_fault_token(index, "restart-after-init-ack")?
                        .context("restart-after-InitAck fault has no armed token")?,
                    // The harness already parses it: a process identity crosses
                    // this boundary as a type, not as text to re-parse.
                    process_id: server.backend_process_id(index)?,
                    marker: QUERY_INIT_ACK_OBSERVED,
                }
            }
            PostQueryFault::RestartBackendAfterEstablishContext(index) => {
                if index >= server.be_count() {
                    bail!(
                        "post-query fault index {index} is out of bounds for {} BE(s)",
                        server.be_count()
                    );
                }
                FaultBaseline::BackendInit {
                    index,
                    token: server
                        .armed_query_lifecycle_fault_token(index, RESTART_AFTER_ESTABLISH_CONTEXT)?
                        .context("restart-after-EstablishQueryContext fault has no armed token")?,
                    process_id: server.backend_process_id(index)?,
                    marker: TASK_ESTABLISH_CONTEXT_OBSERVED,
                }
            }
            PostQueryFault::KillQueryAtLifecyclePhase { phase, .. }
            | PostQueryFault::KillFrontendAtLifecyclePhase(phase)
            | PostQueryFault::KillBackendAtLifecyclePhase { phase, .. } => {
                FaultBaseline::FrontendPhase {
                    phase,
                    fe_crash: matches!(fault, PostQueryFault::KillFrontendAtLifecyclePhase(_)),
                    marker_count: lifecycle_phase_marker_count(
                        &server.fe_log_contents()?,
                        phase,
                        matches!(fault, PostQueryFault::KillFrontendAtLifecyclePhase(_)),
                    )? as u64,
                }
            }
            PostQueryFault::KillFrontendAfterMvKnownCommittedBeforeProjectorCas => {
                FaultBaseline::MvKnownCommittedBeforeProjectorCas {
                    marker_count: server.fe_log_count("NOVAROCKS_MV_PROJECTOR_PHASE")? as u64,
                }
            }
        }
    };
    let fault_state = Arc::new(ActiveQueryFaultState::new());
    let worker_server = Arc::clone(server);
    let worker_fault_state = Arc::clone(&fault_state);
    let worker = thread::spawn(move || -> Result<()> {
        let deadline =
            shared_deadline.unwrap_or_else(|| Instant::now() + POST_FRAGMENT_START_TIMEOUT);
        let mut deadline_cancel_sent = false;
        loop {
            if Instant::now() >= deadline {
                let server = worker_server
                    .lock()
                    .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
                let fe = server.fe_log_contents().unwrap_or_default();
                let bes = (0..server.be_count())
                    .map(|index| server.be_log_contents(index).unwrap_or_default())
                    .collect::<Vec<_>>();
                bail!(
                    "timed out waiting for post-query fault marker; fe_tail={:?}; be_tails={:?}",
                    log_tail(&fe),
                    bes.iter().map(|log| log_tail(log)).collect::<Vec<_>>()
                );
            }
            maybe_cancel_query_near_deadline(
                &worker_server,
                &worker_fault_state,
                query_connection_id,
                deadline,
                &mut deadline_cancel_sent,
            )?;
            let ready = {
                let server = worker_server
                    .lock()
                    .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
                match &baseline {
                    FaultBaseline::ScheduledFragments(baselines) => {
                        let mut all_fresh = true;
                        for &(index, baseline) in baselines {
                            let current = server.scheduled_fragment_count(index)?;
                            if current < baseline {
                                bail!(
                                    "BE[{index}] fragment-start marker count decreased from {baseline} to {current}"
                                );
                            }
                            all_fresh &= current > baseline;
                        }
                        all_fresh
                    }
                    FaultBaseline::FrontendStage { marker_count } => {
                        server.fe_log_count("NOVAROCKS_QUERY_STAGE_BARRIER")?
                            > *marker_count as usize
                    }
                    FaultBaseline::FrontendReady { ready_count, .. } => {
                        let target = match &fault {
                            PostQueryFault::KillFrontendAfterControlReady(target) => *target,
                            PostQueryFault::KillQueryAfterControlReady { ready_count, .. } => {
                                *ready_count
                            }
                            _ => unreachable!(
                                "frontend baseline pairs with ControlReady-driven fault"
                            ),
                        };
                        server.fe_log_count("NOVAROCKS_QUERY_CONTROL_READY")?
                            >= (*ready_count as usize).saturating_add(target)
                    }
                    FaultBaseline::BackendInit {
                        index,
                        token,
                        marker,
                        ..
                    } => server.be_log_contents(*index)?.lines().any(|line| {
                        line.contains(marker) && line.contains(&format!("token={token}"))
                    }),
                    FaultBaseline::FrontendPhase {
                        phase,
                        fe_crash,
                        marker_count,
                    } => {
                        lifecycle_phase_marker_count(&server.fe_log_contents()?, *phase, *fe_crash)?
                            > *marker_count as usize
                    }
                    FaultBaseline::MvKnownCommittedBeforeProjectorCas { marker_count } => {
                        server.fe_log_count("NOVAROCKS_MV_PROJECTOR_PHASE")?
                            > *marker_count as usize
                    }
                    FaultBaseline::BeLogPatternOnBackend {
                        index,
                        pattern,
                        count,
                    } => server.be_log_count(*index, pattern)? > *count,
                    FaultBaseline::BeLogPattern { pattern, counts } => (0..server.be_count())
                        .zip(counts)
                        .map(|(index, baseline)| {
                            server
                                .be_log_count(index, pattern)
                                .map(|current| current > *baseline)
                        })
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .any(|ready| ready),
                }
            };
            if ready {
                if !worker_fault_state.claim_fault() {
                    bail!(
                        "query completed before the post-query fault could claim its marker barrier"
                    );
                }
                let mut server = worker_server
                    .lock()
                    .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
                let mut evidence_execution = match (&baseline, &fault) {
                    (
                        FaultBaseline::FrontendReady { ready_count, .. },
                        PostQueryFault::KillFrontendAfterControlReady(_)
                        | PostQueryFault::KillQueryAfterControlReady { .. },
                    ) => fresh_fe_control_ready_execution(
                        &server.fe_log_contents()?,
                        *ready_count as usize,
                    )?,
                    (
                        FaultBaseline::FrontendPhase {
                            phase,
                            fe_crash,
                            marker_count,
                        },
                        PostQueryFault::KillQueryAtLifecyclePhase { .. }
                        | PostQueryFault::KillFrontendAtLifecyclePhase(_)
                        | PostQueryFault::KillBackendAtLifecyclePhase { .. },
                    ) => fresh_lifecycle_phase_execution(
                        &server.fe_log_contents()?,
                        *marker_count as usize,
                        *phase,
                        *fe_crash,
                    )?,
                    _ => None,
                };
                let action_result = (|| -> Result<()> {
                    match fault.clone() {
                        PostQueryFault::KillBackend(index) => server.kill_be(index)?,
                        PostQueryFault::ReleaseFragmentFailure(index) => {
                            server.release_fragment_executor_failure(index)?
                        }
                        PostQueryFault::RestartBackendAfterInitAck(index)
                        | PostQueryFault::RestartBackendAfterEstablishContext(index) => {
                            let FaultBaseline::BackendInit {
                                token,
                                process_id,
                                marker,
                                ..
                            } = &baseline
                            else {
                                unreachable!("BE restart fault has BackendInit baseline")
                            };
                            let contract = if matches!(
                                fault,
                                PostQueryFault::RestartBackendAfterEstablishContext(_)
                            ) {
                                TASK_RESTART_CONTRACT
                            } else {
                                LIFECYCLE_RESTART_CONTRACT
                            };
                            evidence_execution = Some(restart_backend_and_prove_no_restore(
                                &mut **server,
                                index,
                                token,
                                *process_id,
                                marker,
                                contract,
                                deadline,
                            )?);
                        }
                        PostQueryFault::KillQueryAfterControlReady { connection_id, .. } => {
                            server.kill_query_until(connection_id, deadline)?
                        }
                        PostQueryFault::KillQueryAfterBeLogContains { connection_id, .. } => {
                            server.kill_query_until(connection_id, deadline)?
                        }
                        PostQueryFault::KillQueryAtLifecyclePhase {
                            phase,
                            connection_id,
                        } => {
                            server.kill_query_until(connection_id, deadline)?;
                            server.release_query_lifecycle_phase_fault(phase, false)?;
                        }
                        PostQueryFault::KillFrontendAtLifecyclePhase(phase) => {
                            server.kill_fe()?;
                            server.release_query_lifecycle_phase_fault(phase, true)?;
                            server.restart_fe_until(deadline)?;
                        }
                        PostQueryFault::KillBackendAfterBeLogContains { index, .. } => {
                            server.kill_be(index)?
                        }
                        PostQueryFault::KillFrontendAfterBeLogContains { .. } => {
                            // The coordinator dies and nothing replaces it, so
                            // every backend has to reach its own conclusion
                            // from the query execution lease running out. The
                            // step's own BE-log directives are what assert
                            // that; this action only removes the coordinator
                            // and puts a frontend back for the next step.
                            server.kill_fe()?;
                            server.clear_query_lifecycle_faults()?;
                            server.restart_fe_until(deadline)?;
                        }
                        PostQueryFault::KillFrontendAfterMvKnownCommittedBeforeProjectorCas => {
                            println!(
                                "MV known-committed projector barrier PASS: lake package observed, projector CAS not entered"
                            );
                            server.kill_fe()?;
                            server.clear_query_lifecycle_faults()?;
                            server.restart_fe_until(deadline)?;
                        }
                        PostQueryFault::KillBackendAtLifecyclePhase { index, phase } => {
                            server.kill_be(index)?;
                            server.release_be_kill_at_lifecycle_phase(phase)?;
                            // This fault proves convergence against the
                            // generation that died; restore the runner-owned
                            // backend before the next SQL step so a later
                            // case observes a healthy 1FE+3BE topology.
                            server.restart_be_until(index, deadline)?;
                        }
                        PostQueryFault::KillFrontendAfterControlReady(_) => {
                            server.kill_fe()?;
                            let FaultBaseline::FrontendReady {
                                coordinator_lost, ..
                            } = &baseline
                            else {
                                unreachable!("FE crash fault has frontend baseline");
                            };
                            loop {
                                if Instant::now() >= deadline {
                                    let fe = server.fe_log_contents().unwrap_or_default();
                                    let bes = (0..server.be_count())
                                        .map(|index| {
                                            server.be_log_contents(index).unwrap_or_default()
                                        })
                                        .collect::<Vec<_>>();
                                    bail!(
                                        "timed out waiting for coordinator-lost marker on every BE after FE crash; fe_tail={:?}; be_tails={:?}",
                                        log_tail(&fe),
                                        bes.iter().map(|log| log_tail(log)).collect::<Vec<_>>()
                                    );
                                }
                                let lost_executions = coordinator_lost
                                    .iter()
                                    .enumerate()
                                    .map(|(index, baseline)| {
                                        let log = server.be_log_contents(index)?;
                                        let execution = log
                                            .lines()
                                            .filter(|line| {
                                                line.contains(
                                                    "NOVAROCKS_QUERY_CONTROL_COORDINATOR_LOST",
                                                )
                                            })
                                            .skip(*baseline as usize)
                                            .find_map(|line| marker_field(line, "execution_id"));
                                        Ok(execution)
                                    })
                                    .collect::<Result<Vec<_>>>()?;
                                let all_lost = lost_executions.iter().all(Option::is_some);
                                let same_execution = all_lost
                                    && lost_executions.iter().flatten().all(|execution| {
                                        Some(execution) == evidence_execution.as_ref()
                                    });
                                if same_execution {
                                    break;
                                }
                                sleep(POST_FRAGMENT_START_POLL_INTERVAL);
                            }
                            server.clear_query_lifecycle_faults()?;
                            server.restart_fe_until(deadline)?;
                        }
                    }
                    Ok(())
                })();
                if let Err(error) = action_result {
                    let fe = server.fe_log_contents().unwrap_or_default();
                    let bes = (0..server.be_count())
                        .map(|index| server.be_log_contents(index).unwrap_or_default())
                        .collect::<Vec<_>>();
                    bail!(
                        "post-query fault action failed within 30s deadline: {error:#}; fe_tail={:?}; be_tails={:?}",
                        log_tail(&fe),
                        bes.iter().map(|log| log_tail(log)).collect::<Vec<_>>()
                    );
                }
                if matches!(
                    &fault,
                    PostQueryFault::KillFrontendAfterControlReady(_)
                        | PostQueryFault::KillQueryAfterControlReady { .. }
                        | PostQueryFault::KillQueryAfterBeLogContains { .. }
                        | PostQueryFault::KillFrontendAfterBeLogContains { .. }
                        | PostQueryFault::KillFrontendAtLifecyclePhase(
                            crate::types::QueryLifecyclePhase::TerminalRetained
                        )
                ) {
                    deadline_cancel_sent = true;
                }
                drop(server);
                loop {
                    if Instant::now() >= deadline {
                        let server = worker_server
                            .lock()
                            .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
                        let fe = server.fe_log_contents().unwrap_or_default();
                        let bes = (0..server.be_count())
                            .map(|index| server.be_log_contents(index).unwrap_or_default())
                            .collect::<Vec<_>>();
                        bail!(
                            "post-query lifecycle deadline expired before query completion and required terminal cleanup evidence: query_done={} execution_id={:?}; fe_tail={:?}; be_tails={:?}",
                            worker_fault_state.query_is_done(),
                            evidence_execution,
                            log_tail(&fe),
                            bes.iter().map(|log| log_tail(log)).collect::<Vec<_>>()
                        );
                    }
                    maybe_cancel_query_near_deadline(
                        &worker_server,
                        &worker_fault_state,
                        query_connection_id,
                        deadline,
                        &mut deadline_cancel_sent,
                    )?;
                    let evidence_ready = match &fault {
                        PostQueryFault::KillFrontendAfterControlReady(_)
                        | PostQueryFault::KillQueryAfterControlReady { .. }
                        | PostQueryFault::KillFrontendAtLifecyclePhase(
                            crate::types::QueryLifecyclePhase::TerminalRetained,
                        ) => {
                            let execution = evidence_execution
                                .as_deref()
                                .context("post-query lifecycle fault has no execution anchor")?;
                            let server = worker_server
                                .lock()
                                .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
                            terminal_cleanup_on_all_backends(
                                server.as_ref(),
                                execution,
                                !matches!(
                                    &fault,
                                    PostQueryFault::KillFrontendAtLifecyclePhase(
                                        crate::types::QueryLifecyclePhase::TerminalRetained,
                                    )
                                ),
                            )?
                        }
                        _ => true,
                    };
                    if worker_fault_state.query_is_done() && evidence_ready {
                        return Ok(());
                    }
                    sleep(POST_FRAGMENT_START_POLL_INTERVAL);
                }
            }
            if worker_fault_state.query_is_done() {
                bail!("query completed before the post-query marker barrier was reached");
            }
            sleep(POST_FRAGMENT_START_POLL_INTERVAL);
        }
    });

    let query_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(execute_query));
    fault_state.mark_query_done();
    let worker_result = worker
        .join()
        .map_err(|_| anyhow::anyhow!("post-fragment-start fault worker panicked"));
    match query_result {
        Ok(query_result) => {
            worker_result??;
            Ok(query_result)
        }
        Err(panic) => {
            if let Ok(Err(error)) = worker_result {
                eprintln!(
                    "post-fragment-start fault worker stopped while query panicked: {error:#}"
                );
            }
            std::panic::resume_unwind(panic)
        }
    }
}

fn maybe_cancel_query_near_deadline(
    server: &Arc<Mutex<Box<dyn ServerHandle>>>,
    fault_state: &ActiveQueryFaultState,
    query_connection_id: Option<u32>,
    deadline: Instant,
    cancel_sent: &mut bool,
) -> Result<()> {
    #[cfg(not(test))]
    const DEADLINE_CANCEL_RESERVE: Duration = Duration::from_secs(1);
    #[cfg(test)]
    const DEADLINE_CANCEL_RESERVE: Duration = Duration::from_millis(100);
    if *cancel_sent
        || fault_state.query_is_done()
        || deadline.saturating_duration_since(Instant::now()) > DEADLINE_CANCEL_RESERVE
    {
        return Ok(());
    }
    let Some(connection_id) = query_connection_id else {
        return Ok(());
    };
    let mut server = server
        .lock()
        .map_err(|_| anyhow::anyhow!("server handle mutex is poisoned"))?;
    if let Err(error) = server.kill_query_until(connection_id, deadline) {
        let benign_completion_race = error.to_string().contains("has no active query")
            || error.to_string().contains("ER_NO_SUCH_THREAD")
            || error.to_string().contains("ERROR 1094");
        if !benign_completion_race {
            let fe = server.fe_log_contents().unwrap_or_default();
            let bes = (0..server.be_count())
                .map(|index| server.be_log_contents(index).unwrap_or_default())
                .collect::<Vec<_>>();
            bail!(
                "cancel target query connection {connection_id} before shared fault deadline failed: {error:#}; fe_tail={:?}; be_tails={:?}",
                log_tail(&fe),
                bes.iter().map(|log| log_tail(log)).collect::<Vec<_>>()
            );
        }
    }
    *cancel_sent = true;
    Ok(())
}

fn fresh_fe_control_ready_execution(log: &str, baseline: usize) -> Result<Option<String>> {
    let executions = log
        .lines()
        .filter(|line| line.contains("NOVAROCKS_QUERY_CONTROL_READY"))
        .skip(baseline)
        .filter_map(|line| marker_field(line, "execution_id"))
        .collect::<Vec<_>>();
    let Some(first) = executions.first() else {
        return Ok(None);
    };
    if executions.iter().any(|execution| execution != first) {
        bail!("fresh FE ControlReady markers span multiple executions: {executions:?}");
    }
    Ok(Some(first.clone()))
}

fn lifecycle_phase_marker_count(
    log: &str,
    phase: crate::types::QueryLifecyclePhase,
    fe_crash: bool,
) -> Result<usize> {
    let action = if fe_crash { "kill_fe" } else { "kill_query" };
    let markers = log
        .lines()
        .filter(|line| line.contains("NOVAROCKS_QUERY_LIFECYCLE_PHASE"))
        .filter(|line| {
            marker_field(line, "phase").as_deref() == Some(phase.as_str())
                && marker_field(line, "action").as_deref() == Some(action)
        })
        .collect::<Vec<_>>();
    if markers
        .iter()
        .any(|line| marker_field(line, "token").is_none())
    {
        bail!("lifecycle phase marker has no token: {markers:?}");
    }
    Ok(markers.len())
}

fn fresh_lifecycle_phase_execution(
    log: &str,
    baseline: usize,
    phase: crate::types::QueryLifecyclePhase,
    fe_crash: bool,
) -> Result<Option<String>> {
    let action = if fe_crash { "kill_fe" } else { "kill_query" };
    let executions = log
        .lines()
        .filter(|line| line.contains("NOVAROCKS_QUERY_LIFECYCLE_PHASE"))
        .filter(|line| {
            marker_field(line, "phase").as_deref() == Some(phase.as_str())
                && marker_field(line, "action").as_deref() == Some(action)
        })
        .skip(baseline)
        .filter_map(|line| marker_field(line, "execution_id"))
        .collect::<Vec<_>>();
    let Some(first) = executions.first() else {
        return Ok(None);
    };
    if executions.iter().any(|execution| execution != first) {
        bail!("fresh lifecycle phase markers span multiple executions: {executions:?}");
    }
    Ok(Some(first.clone()))
}

fn terminal_cleanup_on_all_backends(
    server: &dyn ServerHandle,
    execution_id: &str,
    require_terminated_marker: bool,
) -> Result<bool> {
    if server.be_count() != 3 {
        bail!(
            "post-query lifecycle terminal cleanup evidence requires exactly 3 BEs, found {}",
            server.be_count()
        );
    }
    for index in 0..3 {
        let log = server.be_log_contents(index)?;
        let terminated = log.lines().any(|line| {
            line.contains("NOVAROCKS_QUERY_LIFECYCLE_TERMINATED")
                && marker_field(line, "execution_id").as_deref() == Some(execution_id)
        });
        let cleaned = log.lines().any(|line| {
            line.contains("NOVAROCKS_QUERY_LIFECYCLE_CLEANUP")
                && marker_field(line, "execution_id").as_deref() == Some(execution_id)
                && marker_field(line, "active").as_deref() == Some("false")
                && marker_field(line, "tombstone").as_deref() == Some("true")
        });
        if (require_terminated_marker && !terminated) || !cleaned {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Replaces one backend process at its rendezvous and proves the fresh
/// process did not resume the attempt the old one was holding.
///
/// The rendezvous marker is read for both the execution identity and the
/// process identity the backend itself reported, and that identity is checked
/// against the membership owner's before anything is killed: a proof built on
/// a marker from a different process would prove nothing about this one.
/// Returns the old execution id, which the caller keeps as its evidence
/// anchor.
fn restart_backend_and_prove_no_restore(
    server: &mut dyn ServerHandle,
    index: usize,
    token: &str,
    process_id: novarocks_types::BackendProcessId,
    marker: &'static str,
    contract: RestartNonRestoreContract,
    deadline: Instant,
) -> Result<String> {
    let old_log = server.be_log_contents(index)?;
    let rendezvous = old_log
        .lines()
        .rev()
        .find(|line| line.contains(marker) && line.contains(&format!("token={token}")))
        .with_context(|| format!("{marker} for token={token} is missing from BE[{index}]"))?;
    let old_execution = marker_field(rendezvous, "execution_id")
        .context("restart marker is missing execution_id")?;
    let observed_process_id = marker_field(rendezvous, "process_id")
        .context("restart marker is missing process_id")?
        .parse::<novarocks_types::BackendProcessId>()
        .context("restart marker has invalid process_id")?;
    if observed_process_id != process_id {
        bail!(
            "restart marker process identity differs from SHOW BACKENDS: expected={process_id} observed={observed_process_id}"
        );
    }
    server.restart_be_until(index, deadline)?;
    let new_process_id = server.backend_process_id(index)?;
    if new_process_id == process_id {
        bail!(
            "BE[{index}] restart did not replace process identity: old={process_id} new={new_process_id}"
        );
    }
    let new_log = server.be_current_log_contents(index)?;
    validate_restarted_process_has_no_attempt_evidence(
        &new_log,
        &old_execution,
        process_id,
        new_process_id,
        contract,
    )?;
    println!(
        "query lifecycle BE restart proof PASS: kind={} backend_index={index} old_process_id={process_id} new_process_id={new_process_id} token={token} old_execution={old_execution} no_old_execution_restored=true",
        contract.kind
    );
    Ok(old_execution)
}

fn validate_restarted_process_has_no_attempt_evidence(
    new_log: &str,
    old_execution: &str,
    old_process_id: novarocks_types::BackendProcessId,
    new_process_id: novarocks_types::BackendProcessId,
    contract: RestartNonRestoreContract,
) -> Result<()> {
    if old_process_id == new_process_id {
        bail!(
            "restarted BE retained process identity {old_process_id} instead of receiving a new BackendProcessId"
        );
    }
    for forbidden in contract.forbidden {
        if new_log.lines().any(|line| {
            line.contains(forbidden)
                && marker_field(line, "execution_id").as_deref() == Some(old_execution)
        }) {
            bail!(
                "BE process_id={new_process_id} restored old execution {old_execution}: found {forbidden}"
            );
        }
    }
    if new_log.lines().any(|line| {
        line.contains(contract.family_prefix)
            && marker_field(line, contract.identity_field).as_deref()
                == Some(old_process_id.to_string().as_str())
    }) {
        bail!("restarted BE emitted lifecycle evidence with retired process_id={old_process_id}");
    }
    Ok(())
}

fn log_tail(log: &str) -> String {
    log.lines()
        .rev()
        .take(40)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n")
}

fn marker_field(line: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    line.split_ascii_whitespace()
        .find_map(|field| field.strip_prefix(&prefix).map(ToOwned::to_owned))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Condvar, Mutex};

    #[derive(Default)]
    struct RecordingServerHandle {
        events: Vec<String>,
    }

    impl ServerHandle for RecordingServerHandle {
        fn target_host(&self) -> Option<&str> {
            None
        }

        fn target_port(&self) -> Option<u16> {
            None
        }

        fn supports_fault_injection(&self) -> bool {
            true
        }

        fn be_count(&self) -> usize {
            3
        }

        fn kill_be(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("kill:{index}"));
            Ok(())
        }

        fn restart_be(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("restart:{index}"));
            Ok(())
        }

        fn arm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-failure:{index}"));
            Ok(())
        }

        fn disarm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("disarm-failure:{index}"));
            Ok(())
        }

        fn arm_init_ack_drop(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-init-ack-drop:{index}"));
            Ok(())
        }

        fn arm_query_control_heartbeat_stop(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-heartbeat-stop:{index}"));
            Ok(())
        }

        fn arm_fe_crash_after_control_ready(&mut self, count: usize) -> Result<()> {
            self.events.push(format!("arm-fe-crash:{count}"));
            Ok(())
        }

        fn arm_be_restart_after_init_ack(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-be-restart:{index}"));
            Ok(())
        }

        fn arm_stage_prepare_failure(&mut self, ordinal: usize) -> Result<()> {
            self.events
                .push(format!("arm-stage-prepare-failure:{ordinal}"));
            Ok(())
        }

        fn arm_stage_ack_drop(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-stage-ack-drop:{index}"));
            Ok(())
        }

        fn arm_start_ack_drop(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-start-ack-drop:{index}"));
            Ok(())
        }

        fn arm_start_ack_suppress(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-start-ack-suppress:{index}"));
            Ok(())
        }

        fn arm_terminal_ack_drop(&mut self, index: usize) -> Result<()> {
            self.events.push(format!("arm-terminal-ack-drop:{index}"));
            Ok(())
        }

        fn arm_query_lifecycle_fault(&mut self, index: usize, kind: &'static str) -> Result<()> {
            self.events.push(format!("arm-rfo-8r2:{kind}:{index}"));
            Ok(())
        }

        fn arm_kill_query_at_lifecycle_phase(
            &mut self,
            phase: crate::types::QueryLifecyclePhase,
        ) -> Result<()> {
            self.events
                .push(format!("arm-kill-query-phase:{}", phase.as_str()));
            Ok(())
        }

        fn arm_fe_crash_at_lifecycle_phase(
            &mut self,
            phase: crate::types::QueryLifecyclePhase,
        ) -> Result<()> {
            self.events
                .push(format!("arm-fe-crash-phase:{}", phase.as_str()));
            Ok(())
        }

        fn arm_be_kill_at_lifecycle_phase(
            &mut self,
            phase: crate::types::QueryLifecyclePhase,
        ) -> Result<()> {
            self.events
                .push(format!("arm-be-kill-phase:{}", phase.as_str()));
            Ok(())
        }

        fn arm_query_control_heartbeat_stop_after_stage(&mut self, index: usize) -> Result<()> {
            self.events
                .push(format!("arm-heartbeat-stop-after-stage:{index}"));
            Ok(())
        }

        fn arm_hold_start_until_early_ingress(&mut self) -> Result<()> {
            self.events
                .push("arm-hold-start-until-early-ingress".to_string());
            Ok(())
        }

        fn arm_query_control_fragment_backend_limit(&mut self, limit: usize) -> Result<()> {
            self.events.push(format!("arm-fragment-limit:{limit}"));
            Ok(())
        }
    }

    #[test]
    fn has_fault_detects_any_fault_directive() {
        assert!(!has_fault(&QueryMeta::default()));
        assert!(has_fault(&QueryMeta {
            heartbeat_delay_ms: Some(0),
            ..QueryMeta::default()
        }));
    }

    #[test]
    fn restart_delay_without_kill_is_rejected() {
        let meta = QueryMeta {
            restart_be_delay_ms: Some(0),
            ..QueryMeta::default()
        };
        let mut server = RecordingServerHandle::default();

        let err = apply_pre_query(&meta, &mut server).expect_err("restart without kill");

        assert!(
            err.to_string()
                .contains("restart_be_delay_ms requires kill_be_index"),
            "unexpected error: {err}"
        );
        assert!(server.events.is_empty());
    }

    #[test]
    fn unsupported_server_mode_rejects_fault_directives() {
        struct UnsupportedServerHandle;

        impl ServerHandle for UnsupportedServerHandle {
            fn target_host(&self) -> Option<&str> {
                None
            }

            fn target_port(&self) -> Option<u16> {
                None
            }
        }

        let meta = QueryMeta {
            heartbeat_delay_ms: Some(0),
            ..QueryMeta::default()
        };
        let mut server = UnsupportedServerHandle;

        let err = apply_pre_query(&meta, &mut server).expect_err("unsupported server mode");

        assert!(
            err.to_string()
                .contains("require a mutable cross-process server handle"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn lifecycle_fault_directives_require_cross_process_mode() {
        struct UnsupportedServerHandle;

        impl ServerHandle for UnsupportedServerHandle {
            fn target_host(&self) -> Option<&str> {
                None
            }

            fn target_port(&self) -> Option<u16> {
                None
            }
        }

        for meta in [
            QueryMeta {
                drop_next_init_ack_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                stop_query_control_heartbeat_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_fe_after_control_ready_count: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                restart_be_after_init_ack_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_query_after_control_ready_count: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                query_control_fragment_backend_limit: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                fail_stage_prepare_ordinal: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                drop_next_stage_ack_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                drop_next_start_ack_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                suppress_start_ack_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_query_at_lifecycle_phase: Some(crate::types::QueryLifecyclePhase::Staged),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_fe_at_lifecycle_phase: Some(crate::types::QueryLifecyclePhase::Staged),
                ..QueryMeta::default()
            },
            QueryMeta {
                stop_query_control_heartbeat_after_stage_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                hold_start_until_early_ingress: true,
                ..QueryMeta::default()
            },
        ] {
            let mut server = UnsupportedServerHandle;
            let error = apply_pre_query(&meta, &mut server)
                .expect_err("lifecycle faults must reject non-cross-process mode");
            assert!(
                error
                    .to_string()
                    .contains("require a mutable cross-process server handle"),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn lifecycle_fault_directives_reject_mutually_exclusive_faults_before_mutation() {
        let mut server = RecordingServerHandle::default();
        let meta = QueryMeta {
            drop_next_init_ack_be_index: Some(0),
            stop_query_control_heartbeat_be_index: Some(1),
            ..QueryMeta::default()
        };

        let error = apply_pre_query(&meta, &mut server)
            .expect_err("one step may arm only one lifecycle failure");

        assert!(
            error
                .to_string()
                .contains("at most one query lifecycle fault directive"),
            "unexpected error: {error}"
        );
        assert!(server.events.is_empty());
    }

    #[test]
    fn lifecycle_start_hold_may_combine_with_one_primary_fault() {
        let mut server = RecordingServerHandle::default();
        let meta = QueryMeta {
            drop_next_start_ack_be_index: Some(1),
            hold_start_until_early_ingress: true,
            ..QueryMeta::default()
        };

        apply_pre_query(&meta, &mut server).expect("schedule-shaping hold may compose");

        assert_eq!(
            server.events,
            vec![
                "arm-start-ack-drop:1".to_string(),
                "arm-hold-start-until-early-ingress".to_string(),
            ]
        );
    }

    #[test]
    fn lifecycle_start_ack_suppression_may_compose_with_terminal_ack_drop() {
        let mut server = RecordingServerHandle::default();
        let meta = QueryMeta {
            suppress_start_ack_be_index: Some(2),
            drop_next_terminal_ack_be_index: Some(1),
            query_control_fragment_backend_limit: Some(2),
            ..QueryMeta::default()
        };

        apply_pre_query(&meta, &mut server)
            .expect("QLC composite Start/Terminal ACK fault may compose");

        assert_eq!(
            server.events,
            vec![
                "arm-start-ack-suppress:2".to_string(),
                "arm-terminal-ack-drop:1".to_string(),
                "arm-fragment-limit:2".to_string(),
            ]
        );
    }

    #[test]
    fn lifecycle_fault_directives_validate_counts_and_backend_indices() {
        struct ThreeBackendServer;

        impl ServerHandle for ThreeBackendServer {
            fn target_host(&self) -> Option<&str> {
                None
            }

            fn target_port(&self) -> Option<u16> {
                None
            }

            fn supports_fault_injection(&self) -> bool {
                true
            }

            fn be_count(&self) -> usize {
                3
            }
        }

        for (meta, expected) in [
            (
                QueryMeta {
                    drop_next_init_ack_be_index: Some(3),
                    ..QueryMeta::default()
                },
                "drop_next_init_ack_be_index 3 is out of bounds for 3 BE(s)",
            ),
            (
                QueryMeta {
                    stop_query_control_heartbeat_be_index: Some(4),
                    ..QueryMeta::default()
                },
                "stop_query_control_heartbeat_be_index 4 is out of bounds for 3 BE(s)",
            ),
            (
                QueryMeta {
                    restart_be_after_init_ack_index: Some(5),
                    ..QueryMeta::default()
                },
                "restart_be_after_init_ack_index 5 is out of bounds for 3 BE(s)",
            ),
            (
                QueryMeta {
                    kill_fe_after_control_ready_count: Some(0),
                    ..QueryMeta::default()
                },
                "kill_fe_after_control_ready_count must be between 1 and 3",
            ),
            (
                QueryMeta {
                    kill_query_after_control_ready_count: Some(4),
                    ..QueryMeta::default()
                },
                "kill_query_after_control_ready_count must be between 1 and 3",
            ),
            (
                QueryMeta {
                    query_control_fragment_backend_limit: Some(0),
                    ..QueryMeta::default()
                },
                "query_control_fragment_backend_limit must be between 1 and 3",
            ),
            (
                QueryMeta {
                    query_control_fragment_backend_limit: Some(4),
                    ..QueryMeta::default()
                },
                "query_control_fragment_backend_limit must be between 1 and 3",
            ),
            (
                QueryMeta {
                    fail_stage_prepare_ordinal: Some(0),
                    ..QueryMeta::default()
                },
                "fail_stage_prepare_ordinal must be at least 1",
            ),
            (
                QueryMeta {
                    drop_next_stage_ack_be_index: Some(3),
                    ..QueryMeta::default()
                },
                "drop_next_stage_ack_be_index 3 is out of bounds for 3 BE(s)",
            ),
            (
                QueryMeta {
                    drop_next_start_ack_be_index: Some(3),
                    ..QueryMeta::default()
                },
                "drop_next_start_ack_be_index 3 is out of bounds for 3 BE(s)",
            ),
            (
                QueryMeta {
                    suppress_start_ack_be_index: Some(3),
                    ..QueryMeta::default()
                },
                "suppress_start_ack_be_index 3 is out of bounds for 3 BE(s)",
            ),
            (
                QueryMeta {
                    stop_query_control_heartbeat_after_stage_be_index: Some(3),
                    ..QueryMeta::default()
                },
                "stop_query_control_heartbeat_after_stage_be_index 3 is out of bounds for 3 BE(s)",
            ),
        ] {
            let mut server = ThreeBackendServer;
            let error = apply_pre_query(&meta, &mut server)
                .expect_err("invalid lifecycle fault target must fail");
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error:#}"
            );
        }
    }

    #[test]
    fn lifecycle_fault_directives_arm_tokenized_cluster_hooks() {
        for (meta, expected_event) in [
            (
                QueryMeta {
                    drop_next_init_ack_be_index: Some(1),
                    ..QueryMeta::default()
                },
                "arm-init-ack-drop:1",
            ),
            (
                QueryMeta {
                    stop_query_control_heartbeat_be_index: Some(2),
                    ..QueryMeta::default()
                },
                "arm-heartbeat-stop:2",
            ),
            (
                QueryMeta {
                    kill_fe_after_control_ready_count: Some(3),
                    ..QueryMeta::default()
                },
                "arm-fe-crash:3",
            ),
            (
                QueryMeta {
                    restart_be_after_init_ack_index: Some(0),
                    ..QueryMeta::default()
                },
                "arm-be-restart:0",
            ),
            (
                QueryMeta {
                    query_control_fragment_backend_limit: Some(2),
                    ..QueryMeta::default()
                },
                "arm-fragment-limit:2",
            ),
            (
                QueryMeta {
                    fail_stage_prepare_ordinal: Some(2),
                    ..QueryMeta::default()
                },
                "arm-stage-prepare-failure:2",
            ),
            (
                QueryMeta {
                    drop_next_stage_ack_be_index: Some(1),
                    ..QueryMeta::default()
                },
                "arm-stage-ack-drop:1",
            ),
            (
                QueryMeta {
                    drop_next_start_ack_be_index: Some(1),
                    ..QueryMeta::default()
                },
                "arm-start-ack-drop:1",
            ),
            (
                QueryMeta {
                    suppress_start_ack_be_index: Some(1),
                    ..QueryMeta::default()
                },
                "arm-start-ack-suppress:1",
            ),
            (
                QueryMeta {
                    drop_next_terminal_ack_be_index: Some(1),
                    ..QueryMeta::default()
                },
                "arm-terminal-ack-drop:1",
            ),
            (
                QueryMeta {
                    kill_query_at_lifecycle_phase: Some(
                        crate::types::QueryLifecyclePhase::Starting,
                    ),
                    ..QueryMeta::default()
                },
                "arm-kill-query-phase:starting",
            ),
            (
                QueryMeta {
                    kill_fe_at_lifecycle_phase: Some(crate::types::QueryLifecyclePhase::Staged),
                    ..QueryMeta::default()
                },
                "arm-fe-crash-phase:staged",
            ),
            (
                QueryMeta {
                    kill_be_at_lifecycle_phase: Some(
                        crate::types::KillBeAtLifecyclePhaseDirective {
                            be_index: 2,
                            phase: crate::types::QueryLifecyclePhase::TerminalRetained,
                        },
                    ),
                    ..QueryMeta::default()
                },
                "arm-be-kill-phase:terminal-retained",
            ),
            (
                QueryMeta {
                    stop_query_control_heartbeat_after_stage_be_index: Some(1),
                    ..QueryMeta::default()
                },
                "arm-heartbeat-stop-after-stage:1",
            ),
            (
                QueryMeta {
                    hold_start_until_early_ingress: true,
                    ..QueryMeta::default()
                },
                "arm-hold-start-until-early-ingress",
            ),
        ] {
            let mut server = RecordingServerHandle::default();
            apply_pre_query(&meta, &mut server).expect("arm lifecycle hook");
            assert_eq!(server.events, vec![expected_event]);
        }
    }

    #[test]
    fn rfo_8r2_fault_directives_arm_every_configured_hook() {
        let mut server = RecordingServerHandle::default();
        let meta = QueryMeta {
            query_lifecycle_fault: Some(crate::types::QueryLifecycleFaultDirective {
                kind: crate::types::QueryLifecycleFaultKind::TerminalP1EncodeFailure,
                be_index: 1,
            }),
            query_lifecycle_faults: vec![
                crate::types::QueryLifecycleFaultDirective {
                    kind: crate::types::QueryLifecycleFaultKind::TerminalP1EncodeFailure,
                    be_index: 1,
                },
                crate::types::QueryLifecycleFaultDirective {
                    kind: crate::types::QueryLifecycleFaultKind::TerminalAttestationStreamDrop,
                    be_index: 1,
                },
            ],
            ..QueryMeta::default()
        };

        apply_pre_query(&meta, &mut server).expect("arm all RFO-8R2 faults");

        assert_eq!(
            server.events,
            vec![
                "arm-rfo-8r2:terminal-p1-encode-failure:1",
                "arm-rfo-8r2:terminal-attestation-stream-drop:1",
            ]
        );
    }

    #[test]
    fn network_partition_is_explicitly_unsupported() {
        let meta = QueryMeta {
            network_partition_be: Some(1),
            ..QueryMeta::default()
        };
        let mut server = RecordingServerHandle::default();

        let err = apply_pre_query(&meta, &mut server).expect_err("unsupported partition");

        assert!(
            err.to_string()
                .contains("network_partition_be is unsupported"),
            "unexpected error: {err}"
        );
        assert!(server.events.is_empty());
    }

    #[test]
    fn kill_and_restart_target_same_be() {
        let meta = QueryMeta {
            kill_be_index: Some(2),
            restart_be_delay_ms: Some(0),
            ..QueryMeta::default()
        };
        let mut server = RecordingServerHandle::default();

        apply_pre_query(&meta, &mut server).expect("apply fault");

        assert_eq!(server.events, vec!["kill:2", "restart:2"]);
    }

    #[test]
    fn fragment_executor_failure_is_armed_before_query_but_fires_after_start() {
        let meta = QueryMeta {
            fail_fragment_after_start_be_index: Some(2),
            ..QueryMeta::default()
        };
        let mut server = RecordingServerHandle::default();

        apply_pre_query(&meta, &mut server).expect("arm fragment executor failure");

        assert_eq!(server.events, vec!["arm-failure:2"]);
    }

    #[test]
    fn multiple_fragment_faults_are_rejected_before_mutating_the_cluster() {
        for meta in [
            QueryMeta {
                kill_be_index: Some(0),
                kill_be_after_fragment_start: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_be_after_fragment_start: Some(1),
                fail_fragment_after_start_be_index: Some(2),
                ..QueryMeta::default()
            },
        ] {
            let mut server = RecordingServerHandle::default();
            let error = apply_pre_query(&meta, &mut server)
                .expect_err("a step must select exactly one fragment fault");
            assert!(
                error
                    .to_string()
                    .contains("at most one fragment fault directive"),
                "{error}"
            );
            assert!(server.events.is_empty());
        }
    }

    #[test]
    fn completed_query_wins_before_fault_claim() {
        let state = ActiveQueryFaultState::new();

        assert!(state.mark_query_done());
        assert!(
            !state.claim_fault(),
            "a fault worker must not claim permission to kill after query completion"
        );
    }

    struct SharedCleanupServerHandle {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl ServerHandle for SharedCleanupServerHandle {
        fn target_host(&self) -> Option<&str> {
            None
        }

        fn target_port(&self) -> Option<u16> {
            None
        }

        fn disarm_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
            self.events
                .lock()
                .expect("cleanup events")
                .push(format!("disarm-failure:{index}"));
            Ok(())
        }
    }

    #[test]
    fn fragment_failure_step_guard_disarms_during_unwind() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let server: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(SharedCleanupServerHandle {
                events: Arc::clone(&events),
            })));
        let meta = QueryMeta {
            fail_fragment_after_start_be_index: Some(2),
            ..QueryMeta::default()
        };

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = fragment_failure_step_guard(&meta, Arc::clone(&server));
            panic!("simulated step panic");
        }));

        assert!(panic.is_err());
        assert_eq!(
            *events.lock().expect("cleanup events"),
            vec!["disarm-failure:2"]
        );
    }

    struct ActiveQueryServerHandle {
        state: Arc<(Mutex<ActiveQueryState>, Condvar)>,
    }

    #[derive(Default)]
    struct ActiveQueryState {
        events: Vec<&'static str>,
        fragment_started: bool,
        killed: bool,
    }

    impl ServerHandle for ActiveQueryServerHandle {
        fn target_host(&self) -> Option<&str> {
            None
        }

        fn target_port(&self) -> Option<u16> {
            None
        }

        fn supports_fault_injection(&self) -> bool {
            true
        }

        fn be_count(&self) -> usize {
            2
        }

        fn scheduled_fragment_count(&self, index: usize) -> Result<u64> {
            assert_eq!(index, 1);
            let (lock, _) = self.state.as_ref();
            let mut state = lock.lock().expect("active query state");
            let event = if state.fragment_started {
                "scheduled:fresh"
            } else {
                "scheduled:baseline"
            };
            state.events.push(event);
            Ok(u64::from(state.fragment_started))
        }

        fn kill_be(&mut self, index: usize) -> Result<()> {
            assert_eq!(index, 1);
            let (lock, wake) = self.state.as_ref();
            let mut state = lock.lock().expect("active query state");
            state.events.push("kill");
            state.killed = true;
            wake.notify_all();
            Ok(())
        }
    }

    #[test]
    fn active_query_kill_waits_for_fresh_scheduled_fragment_count() {
        let state = Arc::new((Mutex::new(ActiveQueryState::default()), Condvar::new()));
        let server_handle: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(ActiveQueryServerHandle {
                state: Arc::clone(&state),
            })));
        let meta = QueryMeta {
            kill_be_after_fragment_start: Some(1),
            ..QueryMeta::default()
        };

        let result =
            execute_with_post_fragment_start_fault(&meta, &server_handle, None, None, || {
                let (lock, wake) = state.as_ref();
                let mut query = lock.lock().expect("active query state");
                query.events.push("query:start");
                query.fragment_started = true;
                wake.notify_all();
                query = wake
                    .wait_while(query, |state| !state.killed)
                    .expect("wait for runner kill");
                query.events.push("query:end");
                42
            })
            .expect("active-query fault execution");

        assert_eq!(result, 42);
        assert_eq!(
            state.0.lock().expect("active query state").events,
            vec![
                "scheduled:baseline",
                "query:start",
                "scheduled:fresh",
                "kill",
                "query:end",
            ]
        );
    }

    struct AllBackendsReleaseServerHandle {
        state: Arc<(Mutex<AllBackendsReleaseState>, Condvar)>,
    }

    #[derive(Default)]
    struct AllBackendsReleaseState {
        baseline_reads: Vec<usize>,
        fresh_reads: Vec<usize>,
        query_started: bool,
        released_index: Option<usize>,
        events: Vec<&'static str>,
    }

    impl ServerHandle for AllBackendsReleaseServerHandle {
        fn target_host(&self) -> Option<&str> {
            None
        }

        fn target_port(&self) -> Option<u16> {
            None
        }

        fn supports_fault_injection(&self) -> bool {
            true
        }

        fn be_count(&self) -> usize {
            3
        }

        fn scheduled_fragment_count(&self, index: usize) -> Result<u64> {
            let (lock, _) = self.state.as_ref();
            let mut state = lock.lock().expect("all-backend release state");
            if state.query_started {
                state.fresh_reads.push(index);
                Ok(11)
            } else {
                state.baseline_reads.push(index);
                Ok(10)
            }
        }

        fn fe_log_count(&self, marker: &str) -> Result<usize> {
            assert_eq!(marker, "NOVAROCKS_QUERY_STAGE_BARRIER");
            let (lock, _) = self.state.as_ref();
            let state = lock.lock().expect("all-backend release state");
            Ok(usize::from(state.query_started))
        }

        fn release_fragment_executor_failure(&mut self, index: usize) -> Result<()> {
            let (lock, wake) = self.state.as_ref();
            let mut state = lock.lock().expect("all-backend release state");
            state.released_index = Some(index);
            state.events.push("release");
            wake.notify_all();
            Ok(())
        }
    }

    #[test]
    fn fragment_failure_release_waits_for_the_stage_barrier() {
        let state = Arc::new((
            Mutex::new(AllBackendsReleaseState::default()),
            Condvar::new(),
        ));
        let server_handle: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(AllBackendsReleaseServerHandle {
                state: Arc::clone(&state),
            })));
        let meta = QueryMeta {
            fail_fragment_after_start_be_index: Some(1),
            ..QueryMeta::default()
        };

        let result =
            execute_with_post_fragment_start_fault(&meta, &server_handle, None, None, || {
                let (lock, wake) = state.as_ref();
                let mut query = lock.lock().expect("all-backend release state");
                query.events.push("query:start");
                query.query_started = true;
                wake.notify_all();
                query = wake
                    .wait_while(query, |state| state.released_index.is_none())
                    .expect("wait for runner release");
                query.events.push("query:end");
                42
            })
            .expect("active-query fragment failure release");

        assert_eq!(result, 42);
        let state = state.0.lock().expect("all-backend release state");
        assert!(state.baseline_reads.is_empty());
        assert!(state.fresh_reads.is_empty());
        assert_eq!(state.released_index, Some(1));
        assert_eq!(state.events, vec!["query:start", "release", "query:end"]);
    }

    #[test]
    fn query_panic_joins_fault_worker_without_a_late_kill() {
        let state = Arc::new((Mutex::new(ActiveQueryState::default()), Condvar::new()));
        let server_handle: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(ActiveQueryServerHandle {
                state: Arc::clone(&state),
            })));
        let meta = QueryMeta {
            kill_be_after_fragment_start: Some(1),
            ..QueryMeta::default()
        };

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = execute_with_post_fragment_start_fault(
                &meta,
                &server_handle,
                None,
                None,
                || -> () {
                    panic!("simulated query panic");
                },
            );
        }));

        assert!(panic.is_err());
        std::thread::sleep(POST_FRAGMENT_START_POLL_INTERVAL * 2);
        let state = state.0.lock().expect("active query state");
        assert!(
            !state.killed,
            "the joined fault worker must not kill a BE after query unwind"
        );
        assert!(
            !state.events.contains(&"kill"),
            "the fault worker must be quiescent before panic resumes"
        );
    }

    struct KillQueryServerHandle {
        state: Arc<(Mutex<KillQueryState>, Condvar)>,
    }

    #[derive(Default)]
    struct KillQueryState {
        control_ready_count: usize,
        killed_connection_id: Option<u32>,
    }

    impl ServerHandle for KillQueryServerHandle {
        fn target_host(&self) -> Option<&str> {
            None
        }

        fn target_port(&self) -> Option<u16> {
            None
        }

        fn supports_fault_injection(&self) -> bool {
            true
        }

        fn be_count(&self) -> usize {
            3
        }

        fn fe_log_count(&self, needle: &str) -> Result<usize> {
            assert_eq!(needle, "NOVAROCKS_QUERY_CONTROL_READY");
            Ok(self
                .state
                .0
                .lock()
                .expect("kill-query state")
                .control_ready_count)
        }

        fn fe_log_contents(&self) -> Result<String> {
            let count = self
                .state
                .0
                .lock()
                .expect("kill-query state")
                .control_ready_count;
            Ok((0..count)
                .map(|_| "NOVAROCKS_QUERY_CONTROL_READY execution_id=10:20:1 process_id=018f3d8a-2b4c-7d6e-8f90-123456789abd\n")
                .collect())
        }

        fn be_log_contents(&self, index: usize) -> Result<String> {
            let killed = self
                .state
                .0
                .lock()
                .expect("kill-query state")
                .killed_connection_id
                .is_some();
            Ok(if killed {
                format!(
                    "NOVAROCKS_QUERY_LIFECYCLE_TERMINATED execution_id=10:20:1 process_id=018f3d8a-2b4c-7d6e-8f90-123456789ab{index:x} reason=CoordinatorAbort\nNOVAROCKS_QUERY_LIFECYCLE_CLEANUP execution_id=10:20:1 process_id=018f3d8a-2b4c-7d6e-8f90-123456789ab{index:x} active=false tombstone=true reason=CoordinatorAbort\n"
                )
            } else {
                String::new()
            })
        }

        fn kill_query(&mut self, connection_id: u32) -> Result<()> {
            let (lock, wake) = self.state.as_ref();
            let mut state = lock.lock().expect("kill-query state");
            state.killed_connection_id = Some(connection_id);
            wake.notify_all();
            Ok(())
        }
    }

    #[test]
    fn kill_query_waits_for_control_ready_and_uses_separate_connection_id() {
        let state = Arc::new((Mutex::new(KillQueryState::default()), Condvar::new()));
        let server_handle: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(KillQueryServerHandle {
                state: Arc::clone(&state),
            })));
        let meta = QueryMeta {
            kill_query_after_control_ready_count: Some(3),
            ..QueryMeta::default()
        };

        let result =
            execute_with_post_fragment_start_fault(&meta, &server_handle, Some(41), None, || {
                let (lock, wake) = state.as_ref();
                let mut query = lock.lock().expect("kill-query state");
                query.control_ready_count = 3;
                wake.notify_all();
                query = wake
                    .wait_while(query, |state| state.killed_connection_id.is_none())
                    .expect("wait for KILL QUERY");
                query.killed_connection_id
            })
            .expect("KILL QUERY orchestration");

        assert_eq!(result, Some(41));
    }

    #[test]
    fn expired_shared_deadline_never_claims_a_post_query_fault() {
        let state = Arc::new((Mutex::new(KillQueryState::default()), Condvar::new()));
        let server_handle: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(KillQueryServerHandle {
                state: Arc::clone(&state),
            })));
        let meta = QueryMeta {
            kill_query_after_control_ready_count: Some(3),
            ..QueryMeta::default()
        };

        let error = execute_with_post_fragment_start_fault(
            &meta,
            &server_handle,
            Some(41),
            Some(Instant::now()),
            || None::<u32>,
        )
        .expect_err("expired deadline must fail before claiming the fault");

        assert!(error.to_string().contains("timed out"));
        assert_eq!(
            state
                .0
                .lock()
                .expect("kill-query state")
                .killed_connection_id,
            None
        );
    }

    #[test]
    fn deadline_cancel_accepts_no_active_query_after_concurrent_completion() {
        struct CompletionRaceHandle;

        impl ServerHandle for CompletionRaceHandle {
            fn target_host(&self) -> Option<&str> {
                None
            }

            fn target_port(&self) -> Option<u16> {
                None
            }

            fn supports_fault_injection(&self) -> bool {
                true
            }

            fn kill_query(&mut self, _connection_id: u32) -> Result<()> {
                bail!("ERROR 1094 (HY000): connection has no active query")
            }
        }

        let fault_state = Arc::new(ActiveQueryFaultState::new());
        let server: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(CompletionRaceHandle)));
        let mut cancel_sent = false;

        maybe_cancel_query_near_deadline(
            &server,
            fault_state.as_ref(),
            Some(41),
            Instant::now() + Duration::from_millis(50),
            &mut cancel_sent,
        )
        .expect("concurrent query completion makes no-active-query benign");

        assert!(cancel_sent);
        assert!(
            !fault_state.query_is_done(),
            "the benign decision must not depend on the client setting query_done first"
        );
    }

    fn restart_process_id(seed: &str) -> novarocks_types::BackendProcessId {
        seed.parse().expect("fixture UUIDv7 backend process id")
    }

    #[test]
    fn restart_nonrestore_proof_accepts_a_distinct_process_without_old_execution() {
        let old = restart_process_id("018f3d8a-2b4c-7d6e-8f90-123456789abc");
        let new = restart_process_id("018f3d8a-2b4c-7d6f-8f90-123456789abc");
        for contract in [LIFECYCLE_RESTART_CONTRACT, TASK_RESTART_CONTRACT] {
            validate_restarted_process_has_no_attempt_evidence("", "10:20:1", old, new, contract)
                .expect("a distinct fresh process with no old execution is valid");
        }
    }

    #[test]
    fn restart_nonrestore_proof_rejects_same_process_identity() {
        let process_id = restart_process_id("018f3d8a-2b4c-7d6e-8f90-123456789abc");
        for contract in [LIFECYCLE_RESTART_CONTRACT, TASK_RESTART_CONTRACT] {
            let error = validate_restarted_process_has_no_attempt_evidence(
                "", "10:20:1", process_id, process_id, contract,
            )
            .expect_err("restart must install a new BackendProcessId");
            assert!(error.to_string().contains("retained process identity"));
        }
    }

    #[test]
    fn restart_nonrestore_proof_rejects_old_execution_control_or_fragment_state() {
        let old = restart_process_id("018f3d8a-2b4c-7d6e-8f90-123456789abc");
        let new = restart_process_id("018f3d8a-2b4c-7d6f-8f90-123456789abc");
        for marker in [
            "NOVAROCKS_QUERY_CONTROL_READY",
            "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED",
            "NOVAROCKS_QUERY_INIT_APPLIED",
        ] {
            let log = format!("{marker} execution_id=10:20:1 process_id={new}\n");
            let error = validate_restarted_process_has_no_attempt_evidence(
                &log,
                "10:20:1",
                old,
                new,
                LIFECYCLE_RESTART_CONTRACT,
            )
            .expect_err("old execution state must fail");
            assert!(error.to_string().contains(marker));
        }
    }

    /// The task protocol names its own two admission points, and its markers
    /// attribute a backend process in `backend=` rather than `process_id=`.
    /// Reading the retired field name here would make every task-protocol
    /// restart proof pass for the wrong reason.
    #[test]
    fn task_restart_nonrestore_proof_rejects_old_execution_admission_state() {
        let old = restart_process_id("018f3d8a-2b4c-7d6e-8f90-123456789abc");
        let new = restart_process_id("018f3d8a-2b4c-7d6f-8f90-123456789abc");
        for marker in [
            "NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED",
            "NOVAROCKS_TASK_CREATE_APPLIED",
        ] {
            let log = format!("{marker} execution_id=10:20:1 frontend=fe backend={new}\n");
            let error = validate_restarted_process_has_no_attempt_evidence(
                &log,
                "10:20:1",
                old,
                new,
                TASK_RESTART_CONTRACT,
            )
            .expect_err("old execution admission state must fail");
            assert!(error.to_string().contains(marker));
        }
    }

    #[test]
    fn restart_nonrestore_proof_rejects_retired_process_identity() {
        let old = restart_process_id("018f3d8a-2b4c-7d6e-8f90-123456789abc");
        let new = restart_process_id("018f3d8a-2b4c-7d6f-8f90-123456789abc");
        let log = format!("NOVAROCKS_QUERY_CONTROL_READY execution_id=other process_id={old}\n");
        let error = validate_restarted_process_has_no_attempt_evidence(
            &log,
            "10:20:1",
            old,
            new,
            LIFECYCLE_RESTART_CONTRACT,
        )
        .expect_err("new process must not emit retired identity");
        assert!(error.to_string().contains("retired process_id"));
    }

    #[test]
    fn task_restart_nonrestore_proof_rejects_retired_process_identity() {
        let old = restart_process_id("018f3d8a-2b4c-7d6e-8f90-123456789abc");
        let new = restart_process_id("018f3d8a-2b4c-7d6f-8f90-123456789abc");
        let log = format!(
            "NOVAROCKS_TASK_CREATE_APPLIED execution_id=other stage=1 task=2 backend={old}\n"
        );
        let error = validate_restarted_process_has_no_attempt_evidence(
            &log,
            "10:20:1",
            old,
            new,
            TASK_RESTART_CONTRACT,
        )
        .expect_err("new process must not emit retired identity");
        assert!(error.to_string().contains("retired process_id"));
    }

    #[test]
    fn task_restart_directive_arms_the_generic_lifecycle_hook() {
        let mut server = RecordingServerHandle::default();
        let meta = QueryMeta {
            restart_be_after_establish_context_index: Some(1),
            ..QueryMeta::default()
        };

        apply_pre_query(&meta, &mut server).expect("arm establish-context restart");

        assert_eq!(
            server.events,
            vec!["arm-rfo-8r2:restart-after-establish-context:1"]
        );
    }

    #[test]
    fn task_restart_directive_validates_its_backend_index() {
        let mut server = RecordingServerHandle::default();
        let meta = QueryMeta {
            restart_be_after_establish_context_index: Some(3),
            ..QueryMeta::default()
        };

        let error = apply_pre_query(&meta, &mut server)
            .expect_err("an out-of-range backend index must fail before arming");

        assert!(
            error.to_string().contains(
                "restart_be_after_establish_context_index 3 is out of bounds for 3 BE(s)"
            ),
            "unexpected error: {error}"
        );
        assert!(server.events.is_empty());
    }

    #[test]
    fn fe_kill_after_be_log_and_task_restart_are_mutually_exclusive() {
        let mut server = RecordingServerHandle::default();
        let meta = QueryMeta {
            restart_be_after_establish_context_index: Some(1),
            kill_fe_after_be_log_contains: Some("NOVAROCKS_TASK_CREATE_APPLIED".to_string()),
            ..QueryMeta::default()
        };

        let error = apply_pre_query(&meta, &mut server)
            .expect_err("one step may arm only one lifecycle failure");

        assert!(
            error
                .to_string()
                .contains("at most one query lifecycle fault directive"),
            "unexpected error: {error}"
        );
        assert!(server.events.is_empty());
    }

    /// An FE crash may leave bounded terminal delivery records behind, so a
    /// step that kills the frontend has to be allowed to have them. Without
    /// this the runner would compare them against a pre-fault zero and fail a
    /// case for retention the crash itself caused.
    #[test]
    fn killing_the_frontend_from_a_be_marker_permits_terminal_retention() {
        assert!(permits_terminal_retention(&QueryMeta {
            kill_fe_after_be_log_contains: Some("NOVAROCKS_TASK_TERMINAL_RETAINED".to_string()),
            ..QueryMeta::default()
        }));
        assert!(!permits_terminal_retention(&QueryMeta::default()));
    }
}

/// Parse the harness's textual backend process identity into the typed form the
/// failpoint scopes carry.
///
/// The harness reports the identity as it appears in `SHOW BACKENDS`, while the
/// fault scopes hold a validated `BackendProcessId`; converting here keeps the
/// comparison and the evidence on the typed value instead of on formatting.
fn parse_backend_process_id(value: &str) -> Result<novarocks_types::BackendProcessId> {
    value
        .parse::<novarocks_types::BackendProcessId>()
        .map_err(|error| anyhow::anyhow!("backend process identity {value} is invalid: {error}"))
}
