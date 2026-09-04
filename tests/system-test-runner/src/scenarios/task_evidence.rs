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

//! Task-protocol evidence for "one distributed query really ran end to end".
//!
//! Several scenarios are not about the query lifecycle at all -- the Native
//! trust profiles are about JWT and TLS, and the process-attribution baseline
//! is about identity minting -- but each needed one way to confirm that a
//! query it issued was genuinely carried across the FE/BE boundary. Before the
//! task cutover they all confirmed it the same way: the frontend's terminal
//! convergence snapshot listed a per-participant terminal outcome, and each
//! backend logged `NOVAROCKS_QUERY_INIT_APPLIED`.
//!
//! Neither fact exists any more. The task protocol mints no
//! `ParticipantTerminalOutcome` at all -- its per-domain receipts and
//! termination latch replace the retired terminal-evidence funnel (ADR-0134),
//! and `publish_task_round_convergence` in
//! `novarocks/frontend/src/coordinator/execution.rs` deliberately publishes an
//! empty outcome list rather than inventing proofs this protocol never made.
//! `NOVAROCKS_QUERY_INIT_APPLIED` is emitted only by the retired
//! `QueryLifecycleRegistry::log_init`, which no production query reaches.
//!
//! So the confirmation is rebuilt here out of what the task protocol does
//! own, and it is deliberately one helper rather than one per scenario: the
//! three facts below have to agree with each other to mean anything, and
//! three copies of that reasoning would drift.

use std::collections::BTreeSet;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::{
    QueryLifecycleStructuredSnapshot, RuntimeFilterTerminalRollup, ServerHandle,
};

use crate::scenario::ScenarioContext;

/// A backend applied a frontend's request to establish this attempt's query
/// context. This is the successor of the retired `NOVAROCKS_QUERY_INIT_APPLIED`:
/// the point at which a backend admits an attempt's identity and agrees to hold
/// resources for it.
pub const CONTEXT_ESTABLISH_APPLIED: &str = "NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED";

/// A backend moved its context out of `Active` on the frontend's release, which
/// is the operation that carries its sealed observation back. This is the
/// successor of the retired per-participant terminal proof.
pub const RELEASE_APPLIED: &str = "NOVAROCKS_TASK_RELEASE_APPLIED";

/// Backend markers are written by an asynchronous log pump, so a marker the
/// frontend has already accounted for can be a few milliseconds behind the
/// snapshot that proves it happened.
const MARKER_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Which backends' own logs carry `marker` for exactly this execution.
///
/// The execution identity is matched with a trailing space because every
/// context-scoped marker prints further `key=value` words after it: without the
/// separator, attempt 1 of a query would also match attempt 11 of the same
/// query.
///
/// Matched per line so the marker name and the identity have to come from the
/// same emission rather than from two unrelated lines in the same log.
pub fn backends_with_marker(
    context: &mut ScenarioContext,
    marker: &str,
    execution_id: &str,
) -> Result<BTreeSet<usize>> {
    let token = format!("execution_id={execution_id} ");
    let mut backends = BTreeSet::new();
    for index in 0..context.handle().be_count() {
        let log = context
            .handle()
            .be_log_contents(index)
            .with_context(|| format!("read BE[{index}] log for task-protocol evidence"))?;
        if log
            .lines()
            .any(|line| line.contains(marker) && line.contains(&token))
        {
            backends.insert(index);
        }
    }
    Ok(backends)
}

/// Confirms that the query behind `snapshot` was carried across the FE/BE
/// boundary and completed there, and returns the backends that took part.
///
/// Three facts, each of which is unfalsifiable alone:
///
/// * The frontend's rollup is `Available`. On the task path that is not a
///   statement about runtime filters -- `publish_task_round_convergence`
///   chooses `Available` exactly when every query context the attempt placed
///   answered its release, and `Unavailable(TerminalOutcomesIncomplete)`
///   otherwise. It is therefore the frontend's own successor of "every
///   participant produced a terminal outcome", and it holds no count.
/// * At least one backend admitted this exact execution identity, proving the
///   work crossed the process boundary instead of being answered locally.
/// * Every backend that admitted the attempt also applied its release, so the
///   frontend's claim above is corroborated backend-side rather than taken on
///   trust.
///
/// # Why no expected backend count
///
/// A query context exists only where the scheduler placed a task, so the
/// number of participating backends is a property of the plan and of the
/// splits, never of the cluster size. Measured on a real 1FE+3BE run, 285 of
/// 1289 executions established exactly one context and every one of those had
/// no split assignment at all -- which is precisely the shape of the constant
/// queries these scenarios issue. An assertion pinned at "all three backends"
/// would therefore be asserting the scheduler's current choice. Scenarios that
/// genuinely need per-backend coverage must obtain it from something
/// placement-independent, such as the frontend's live backend registry.
pub fn assert_query_completed_across_boundary(
    context: &mut ScenarioContext,
    snapshot: &QueryLifecycleStructuredSnapshot,
    subject: &str,
) -> Result<BTreeSet<usize>> {
    let execution_id = snapshot
        .execution_id
        .clone()
        .with_context(|| format!("{subject} terminal snapshot carries no execution identity"))?;

    // The published attribution has to be the identity's own, not a second
    // reading of frontend state that could disagree with it. This is what the
    // retired per-backend `query_process_namespace=` assertion proved, kept as
    // a direct field comparison now that the namespace and the local sequence
    // are the high and low halves of the query id the backends are told.
    let attributed = format!(
        "{}:{}:{}",
        snapshot.process_namespace as i64, snapshot.local_sequence, snapshot.attempt_id
    );
    ensure!(
        execution_id == attributed,
        "{subject} terminal snapshot execution id {execution_id} does not match its own \
         published attribution {attributed}"
    );

    ensure!(
        matches!(
            snapshot.runtime_filter,
            RuntimeFilterTerminalRollup::Available { .. }
        ),
        "{subject} attempt {execution_id} did not report every placed query context as \
         released: {:?}",
        snapshot.runtime_filter
    );

    loop {
        let established = backends_with_marker(context, CONTEXT_ESTABLISH_APPLIED, &execution_id)?;
        let released = backends_with_marker(context, RELEASE_APPLIED, &execution_id)?;
        if !established.is_empty() && released == established {
            return Ok(established);
        }
        // Only the pump can still be behind: the frontend publishes its
        // convergence evidence after the drain that collected every release
        // acknowledgement, so both markers were printed before this snapshot
        // became readable. A divergence that outlasts the deadline is real,
        // and the message names which side is short.
        let remaining = context.remaining(&format!(
            "{subject} attempt {execution_id} awaiting task-protocol evidence \
             (established={established:?}, released={released:?})"
        ))?;
        thread::sleep(remaining.min(MARKER_POLL_INTERVAL));
    }
}
