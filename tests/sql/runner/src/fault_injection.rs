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
/// The only action the frontend phase barrier still publishes. The barrier
/// waits for a runner-owned kill of the target query; there is no longer a
/// coordinator-crash variant of the same marker.
const LIFECYCLE_PHASE_ACTION: &str = "kill_query";
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
    let armed = meta.kill_fe_after_mv_known_committed_before_projector_cas
        || meta.restart_be_after_establish_context_index.is_some()
        || meta.kill_query_after_be_log_contains.is_some()
        || meta.kill_fe_after_be_log_contains.is_some()
        || meta.kill_be_after_be_log_contains.is_some()
        || meta.terminal_snapshot_conflict_be_index.is_some()
        || !configured_query_lifecycle_faults(meta).is_empty()
        || meta.kill_query_at_lifecycle_phase.is_some()
        || meta.kill_be_at_lifecycle_phase.is_some()
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
        || meta.heartbeat_delay_ms.is_some()
        || meta.restart_be_delay_ms.is_some()
        || meta.kill_fe_after_mv_known_committed_before_projector_cas
        || meta.restart_be_after_establish_context_index.is_some()
        || meta.kill_query_after_be_log_contains.is_some()
        || meta.kill_fe_after_be_log_contains.is_some()
        || meta.kill_be_after_be_log_contains.is_some()
        || meta.terminal_snapshot_conflict_be_index.is_some()
        || !configured_query_lifecycle_faults(meta).is_empty()
        || meta.kill_query_at_lifecycle_phase.is_some()
        || meta.kill_be_at_lifecycle_phase.is_some()
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

pub(crate) fn apply_pre_query(meta: &QueryMeta, server: &mut dyn ServerHandle) -> Result<()> {
    if let Some(kind) = &meta.cleanup_fault {
        server.arm_cleanup_fault(kind)?;
    }
    let fragment_fault_count = [
        meta.kill_be_index.is_some(),
        meta.kill_be_after_be_log_contains.is_some(),
    ]
    .into_iter()
    .filter(|configured| *configured)
    .count();
    if fragment_fault_count > 1 {
        bail!(
            "a SQL step may configure at most one fragment fault directive: kill_be_index or kill_be_after_be_log_contains"
        );
    }

    let lifecycle_fault_count = [
        meta.kill_fe_after_mv_known_committed_before_projector_cas,
        meta.restart_be_after_establish_context_index.is_some(),
        meta.kill_query_after_be_log_contains.is_some(),
        meta.kill_fe_after_be_log_contains.is_some(),
        meta.terminal_snapshot_conflict_be_index.is_some(),
        !configured_query_lifecycle_faults(meta).is_empty(),
        meta.kill_query_at_lifecycle_phase.is_some(),
        meta.kill_be_at_lifecycle_phase.is_some(),
    ]
    .into_iter()
    .filter(|configured| *configured)
    .count();
    // A terminal-retained BE kill is intentionally composable with one or
    // more owner-local RFO arms. The arms hold one participant's terminal
    // delivery open; the FE phase barrier then proves another participant is
    // already finalizing before the process death is released.
    let terminal_kill_and_rfo_compound = meta.kill_be_at_lifecycle_phase.is_some()
        && !configured_query_lifecycle_faults(meta).is_empty()
        && lifecycle_fault_count == 2;
    if lifecycle_fault_count > 1 && !terminal_kill_and_rfo_compound {
        bail!(
            "a SQL step may configure at most one query lifecycle fault directive; a terminal-retained BE kill may be combined with RFO terminal delivery arms"
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
            "restart_be_after_establish_context_index",
            meta.restart_be_after_establish_context_index,
        ),
        (
            "terminal_snapshot_conflict_be_index",
            meta.terminal_snapshot_conflict_be_index,
        ),
    ] {
        if let Some(index) = index
            && index >= be_count
        {
            bail!("{name} {index} is out of bounds for {be_count} BE(s)");
        }
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

    if meta.kill_fe_after_mv_known_committed_before_projector_cas {
        server.arm_mv_known_committed_before_projector_cas()?;
    }
    if let Some(index) = meta.restart_be_after_establish_context_index {
        // Armed through the generic lifecycle-fault hook rather than through a
        // dedicated harness method: the arm file, its token and its cleanup
        // are already the same for every kind, and a second bespoke method
        // would only duplicate them under a new name.
        server.arm_query_lifecycle_fault(index, RESTART_AFTER_ESTABLISH_CONTEXT)?;
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
    if let Some(fault) = meta.kill_be_at_lifecycle_phase {
        server.arm_be_kill_at_lifecycle_phase(fault.phase)?;
    }
    if let Some(limit) = meta.query_control_fragment_backend_limit {
        server.arm_query_control_fragment_backend_limit(limit)?;
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
        KillFrontendAfterMvKnownCommittedBeforeProjectorCas,
        RestartBackendAfterEstablishContext(usize),
        KillFrontendAfterBeLogContains {
            pattern: String,
        },
        KillBackendAfterBeLogContains {
            index: usize,
            pattern: String,
        },
        KillQueryAfterBeLogContains {
            pattern: String,
            connection_id: u32,
        },
        KillQueryAtLifecyclePhase {
            phase: crate::types::QueryLifecyclePhase,
            connection_id: u32,
        },
        KillBackendAtLifecyclePhase {
            index: usize,
            phase: crate::types::QueryLifecyclePhase,
        },
    }

    enum FaultBaseline {
        BackendInit {
            index: usize,
            token: String,
            process_id: novarocks_types::BackendProcessId,
            /// The token-scoped rendezvous marker of this fault's protocol.
            marker: &'static str,
        },
        FrontendPhase {
            phase: crate::types::QueryLifecyclePhase,
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
        meta.kill_fe_after_mv_known_committed_before_projector_cas
            .then_some(PostQueryFault::KillFrontendAfterMvKnownCommittedBeforeProjectorCas),
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
        meta.kill_query_after_be_log_contains
            .as_deref()
            .map(|pattern| {
                query_connection_id
                .map(|connection_id| PostQueryFault::KillQueryAfterBeLogContains {
                    pattern: pattern.to_string(),
                    connection_id,
                })
                .ok_or_else(|| anyhow::anyhow!(
                    "kill_query_after_be_log_contains requires the target query connection id"
                ))
            })
            .transpose()?,
        meta.kill_query_at_lifecycle_phase
            .map(|phase| {
                query_connection_id
                    .map(|connection_id| PostQueryFault::KillQueryAtLifecyclePhase {
                        phase,
                        connection_id,
                    })
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "kill_query_at_lifecycle_phase requires the target query connection id"
                        )
                    })
            })
            .transpose()?,
        meta.kill_be_at_lifecycle_phase
            .map(|fault| PostQueryFault::KillBackendAtLifecyclePhase {
                index: fault.be_index,
                phase: fault.phase,
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
                    // The harness already parses it: a process identity crosses
                    // this boundary as a type, not as text to re-parse.
                    process_id: server.backend_process_id(index)?,
                    marker: TASK_ESTABLISH_CONTEXT_OBSERVED,
                }
            }
            PostQueryFault::KillQueryAtLifecyclePhase { phase, .. }
            | PostQueryFault::KillBackendAtLifecyclePhase { phase, .. } => {
                FaultBaseline::FrontendPhase {
                    phase,
                    marker_count: lifecycle_phase_marker_count(&server.fe_log_contents()?, phase)?
                        as u64,
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
                        marker_count,
                    } => {
                        lifecycle_phase_marker_count(&server.fe_log_contents()?, *phase)?
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
                        FaultBaseline::FrontendPhase {
                            phase,
                            marker_count,
                        },
                        PostQueryFault::KillQueryAtLifecyclePhase { .. }
                        | PostQueryFault::KillBackendAtLifecyclePhase { .. },
                    ) => fresh_lifecycle_phase_execution(
                        &server.fe_log_contents()?,
                        *marker_count as usize,
                        *phase,
                    )?,
                    _ => None,
                };
                let action_result = (|| -> Result<()> {
                    match fault.clone() {
                        PostQueryFault::RestartBackendAfterEstablishContext(index) => {
                            let FaultBaseline::BackendInit {
                                token,
                                process_id,
                                marker,
                                ..
                            } = &baseline
                            else {
                                unreachable!("BE restart fault has BackendInit baseline")
                            };
                            evidence_execution = Some(restart_backend_and_prove_no_restore(
                                &mut **server,
                                index,
                                token,
                                *process_id,
                                marker,
                                TASK_RESTART_CONTRACT,
                                deadline,
                            )?);
                        }
                        PostQueryFault::KillQueryAfterBeLogContains { connection_id, .. } => {
                            server.kill_query_until(connection_id, deadline)?
                        }
                        PostQueryFault::KillQueryAtLifecyclePhase {
                            phase,
                            connection_id,
                        } => {
                            server.kill_query_until(connection_id, deadline)?;
                            server.release_query_lifecycle_phase_fault(phase)?;
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
                    PostQueryFault::KillQueryAfterBeLogContains { .. }
                        | PostQueryFault::KillFrontendAfterBeLogContains { .. }
                ) {
                    deadline_cancel_sent = true;
                }
                drop(server);
                loop {
                    if worker_fault_state.query_is_done() {
                        return Ok(());
                    }
                    if Instant::now() >= deadline {
                        if worker_fault_state.query_is_done() {
                            return Ok(());
                        }
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

fn lifecycle_phase_marker_count(
    log: &str,
    phase: crate::types::QueryLifecyclePhase,
) -> Result<usize> {
    let markers = log
        .lines()
        .filter(|line| line.contains("NOVAROCKS_QUERY_LIFECYCLE_PHASE"))
        .filter(|line| {
            marker_field(line, "phase").as_deref() == Some(phase.as_str())
                && marker_field(line, "action").as_deref() == Some(LIFECYCLE_PHASE_ACTION)
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
) -> Result<Option<String>> {
    let executions = log
        .lines()
        .filter(|line| line.contains("NOVAROCKS_QUERY_LIFECYCLE_PHASE"))
        .filter(|line| {
            marker_field(line, "phase").as_deref() == Some(phase.as_str())
                && marker_field(line, "action").as_deref() == Some(LIFECYCLE_PHASE_ACTION)
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

        fn arm_be_kill_at_lifecycle_phase(
            &mut self,
            phase: crate::types::QueryLifecyclePhase,
        ) -> Result<()> {
            self.events
                .push(format!("arm-be-kill-phase:{}", phase.as_str()));
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
                query_control_fragment_backend_limit: Some(1),
                ..QueryMeta::default()
            },
            QueryMeta {
                restart_be_after_establish_context_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                terminal_snapshot_conflict_be_index: Some(0),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_query_at_lifecycle_phase: Some(crate::types::QueryLifecyclePhase::Staged),
                ..QueryMeta::default()
            },
            QueryMeta {
                kill_be_at_lifecycle_phase: Some(crate::types::KillBeAtLifecyclePhaseDirective {
                    be_index: 0,
                    phase: crate::types::QueryLifecyclePhase::TerminalRetained,
                }),
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
            terminal_snapshot_conflict_be_index: Some(0),
            kill_query_at_lifecycle_phase: Some(crate::types::QueryLifecyclePhase::Staged),
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
                    terminal_snapshot_conflict_be_index: Some(3),
                    ..QueryMeta::default()
                },
                "terminal_snapshot_conflict_be_index 3 is out of bounds for 3 BE(s)",
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
                    query_control_fragment_backend_limit: Some(2),
                    ..QueryMeta::default()
                },
                "arm-fragment-limit:2",
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
                kind: crate::types::QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop,
                be_index: 1,
            }),
            query_lifecycle_faults: vec![
                crate::types::QueryLifecycleFaultDirective {
                    kind: crate::types::QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop,
                    be_index: 1,
                },
                crate::types::QueryLifecycleFaultDirective {
                    kind: crate::types::QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop,
                    be_index: 1,
                },
            ],
            ..QueryMeta::default()
        };

        apply_pre_query(&meta, &mut server).expect("arm all RFO-8R2 faults");

        assert_eq!(
            server.events,
            vec![
                "arm-rfo-8r2:runtime-filter-contribution-ack-drop:1",
                "arm-rfo-8r2:task-update-terminal-ack-drop:1",
            ]
        );
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
    fn multiple_fragment_faults_are_rejected_before_mutating_the_cluster() {
        let meta = QueryMeta {
            kill_be_index: Some(0),
            kill_be_after_be_log_contains: Some(crate::types::KillBeAfterBeLogDirective {
                be_index: 1,
                pattern: "NOVAROCKS_TASK_CREATE_APPLIED".to_string(),
            }),
            ..QueryMeta::default()
        };
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

    #[test]
    fn completed_query_wins_before_fault_claim() {
        let state = ActiveQueryFaultState::new();

        assert!(state.mark_query_done());
        assert!(
            !state.claim_fault(),
            "a fault worker must not claim permission to kill after query completion"
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

        fn be_log_count(&self, index: usize, needle: &str) -> Result<usize> {
            assert_eq!(index, 1);
            assert_eq!(needle, "NOVAROCKS_TASK_CREATE_APPLIED");
            let (lock, _) = self.state.as_ref();
            let mut state = lock.lock().expect("active query state");
            let event = if state.fragment_started {
                "marker:fresh"
            } else {
                "marker:baseline"
            };
            state.events.push(event);
            Ok(usize::from(state.fragment_started))
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

    fn kill_be_after_marker_meta() -> QueryMeta {
        QueryMeta {
            kill_be_after_be_log_contains: Some(crate::types::KillBeAfterBeLogDirective {
                be_index: 1,
                pattern: "NOVAROCKS_TASK_CREATE_APPLIED".to_string(),
            }),
            ..QueryMeta::default()
        }
    }

    #[test]
    fn active_query_kill_waits_for_a_fresh_backend_log_marker() {
        let state = Arc::new((Mutex::new(ActiveQueryState::default()), Condvar::new()));
        let server_handle: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(ActiveQueryServerHandle {
                state: Arc::clone(&state),
            })));
        let meta = kill_be_after_marker_meta();

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
        let state = state.0.lock().expect("active query state");
        let events = &state.events;
        let query_start = events
            .iter()
            .position(|event| *event == "query:start")
            .expect("the query started");
        assert!(
            events[..query_start]
                .iter()
                .all(|event| *event == "marker:baseline"),
            "polling before the query starts may only observe the baseline: {events:?}"
        );
        assert!(!events[..query_start].is_empty());
        assert_eq!(
            &events[query_start..],
            &["query:start", "marker:fresh", "kill", "query:end"]
        );
    }

    #[test]
    fn query_panic_joins_fault_worker_without_a_late_kill() {
        let state = Arc::new((Mutex::new(ActiveQueryState::default()), Condvar::new()));
        let server_handle: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(ActiveQueryServerHandle {
                state: Arc::clone(&state),
            })));
        let meta = kill_be_after_marker_meta();

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

    /// The BE-log marker a runner-owned KILL QUERY waits for. It is one
    /// protocol-neutral pattern rather than a phase, so the fake only has to
    /// answer "has this line appeared yet".
    const KILL_QUERY_MARKER: &str = "NOVAROCKS_TASK_CREATE_APPLIED";

    struct KillQueryServerHandle {
        state: Arc<(Mutex<KillQueryState>, Condvar)>,
    }

    #[derive(Default)]
    struct KillQueryState {
        marker_emitted: bool,
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

        fn be_log_count(&self, _index: usize, needle: &str) -> Result<usize> {
            assert_eq!(needle, KILL_QUERY_MARKER);
            Ok(usize::from(
                self.state
                    .0
                    .lock()
                    .expect("kill-query state")
                    .marker_emitted,
            ))
        }

        fn be_log_contents(&self, _index: usize) -> Result<String> {
            Ok(String::new())
        }

        fn kill_query(&mut self, connection_id: u32) -> Result<()> {
            let (lock, wake) = self.state.as_ref();
            let mut state = lock.lock().expect("kill-query state");
            state.killed_connection_id = Some(connection_id);
            wake.notify_all();
            Ok(())
        }
    }

    fn kill_query_after_marker_meta() -> QueryMeta {
        QueryMeta {
            kill_query_after_be_log_contains: Some(KILL_QUERY_MARKER.to_string()),
            ..QueryMeta::default()
        }
    }

    #[test]
    fn kill_query_waits_for_a_fresh_marker_and_uses_a_separate_connection_id() {
        let state = Arc::new((Mutex::new(KillQueryState::default()), Condvar::new()));
        let server_handle: Arc<Mutex<Box<dyn ServerHandle>>> =
            Arc::new(Mutex::new(Box::new(KillQueryServerHandle {
                state: Arc::clone(&state),
            })));
        let meta = kill_query_after_marker_meta();

        let result =
            execute_with_post_fragment_start_fault(&meta, &server_handle, Some(41), None, || {
                let (lock, wake) = state.as_ref();
                let mut query = lock.lock().expect("kill-query state");
                query.marker_emitted = true;
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
        let meta = kill_query_after_marker_meta();

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
        validate_restarted_process_has_no_attempt_evidence(
            "",
            "10:20:1",
            old,
            new,
            TASK_RESTART_CONTRACT,
        )
        .expect("a distinct fresh process with no old execution is valid");
    }

    #[test]
    fn restart_nonrestore_proof_rejects_same_process_identity() {
        let process_id = restart_process_id("018f3d8a-2b4c-7d6e-8f90-123456789abc");
        let error = validate_restarted_process_has_no_attempt_evidence(
            "",
            "10:20:1",
            process_id,
            process_id,
            TASK_RESTART_CONTRACT,
        )
        .expect_err("restart must install a new BackendProcessId");
        assert!(error.to_string().contains("retained process identity"));
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
}
