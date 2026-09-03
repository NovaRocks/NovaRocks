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

//! The coordinator's per-round split-assignment worker.
//!
//! A scan blocks until splits arrive, and the statement thread blocks fetching
//! results, so the pump cannot share either. It runs on its own thread for the
//! life of one execution round, and the guard below closes it on every exit
//! path — success, failure, cancellation, or timeout — because a round that
//! left its sources open would leak a connector's enumeration state past the
//! attempt that owned it.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_spi::connector::read_stack::SplitSourceProfile;

use crate::query_execution::artifact::ValidatedFragmentSchedule;
use crate::query_execution::split_assignment::{
    AssignmentTarget, RoundSplitAssignment, RoundSplitAssignmentStop, RoundSplitSource,
    SplitAssignmentDriverError, TaskUpdateRetryPolicy, TaskUpdateTransport,
    emit_split_source_close_marker,
};
use novarocks_sql::plan_read::FragmentId;

/// How many splits one task may hold before the driver stops pulling for it.
const DEFAULT_MAX_QUEUED_SPLITS_PER_TASK: u64 = 4096;

/// The admitted tasks that will read each typed scan node.
///
/// Derived from the schedule rather than from placed splits: with runtime
/// assignment every instance of a fragment that owns the scan node is a
/// legitimate destination, and deriving from placement would silently exclude
/// an instance that happened to receive nothing at planning time.
pub(crate) fn assignment_targets(
    schedule: &ValidatedFragmentSchedule,
    scan_nodes: &[(FragmentId, i32)],
) -> BTreeMap<i32, Vec<AssignmentTarget>> {
    let placements_by_fragment = schedule.fragment_placements();
    let mut targets: BTreeMap<i32, Vec<AssignmentTarget>> = BTreeMap::new();
    for &(fragment_id, plan_node_id) in scan_nodes {
        let Some(placements) = placements_by_fragment.get(&fragment_id) else {
            continue;
        };
        let entry = targets.entry(plan_node_id).or_default();
        for placement in placements {
            entry.push(AssignmentTarget {
                backend_idx: placement.backend_idx,
                fragment_instance_id: placement.finst_id,
            });
        }
    }
    targets
}

/// Every backend this round may deliver a task update to.
pub(crate) fn assignment_endpoints(
    schedule: &ValidatedFragmentSchedule,
) -> Vec<(usize, RuntimeEndpoint)> {
    let mut endpoints: BTreeMap<usize, RuntimeEndpoint> = BTreeMap::new();
    for placements in schedule.fragment_placements().values() {
        for placement in placements {
            endpoints
                .entry(placement.backend_idx)
                .or_insert_with(|| placement.endpoint.clone());
        }
    }
    endpoints.into_iter().collect()
}

/// Everything one round needs to start assigning splits, built before the
/// prepared artifacts are consumed by staging.
///
/// It already owns open connector split sources, so it closes them if the
/// round never starts — staging or Start can still fail between here and
/// there, and a source dropped without closing leaves the connector holding
/// whatever the enumeration opened.
pub(crate) struct RoundSplitAssignmentPlan {
    targets: BTreeMap<i32, Vec<AssignmentTarget>>,
    sources: Vec<RoundSplitSource>,
    retry_policy: TaskUpdateRetryPolicy,
    initial_dynamic_filter_wait_cap: std::time::Duration,
    /// Every backend this round may address, frozen with the schedule.
    ///
    /// Carried on the plan because the schedule is consumed before either path
    /// builds its transport, and a transport built from a later snapshot could
    /// address a process this attempt never scheduled.
    endpoints: Vec<(usize, RuntimeEndpoint)>,
}

impl RoundSplitAssignmentPlan {
    /// Everything but the delivery transport.
    ///
    /// The transport arrives at [`SplitAssignmentRoundGuard::start`] because
    /// the two are frozen at different moments: sources must be open before
    /// the attempt's credential leases are sealed, while the task substrate's
    /// delivery bridge cannot exist until the task graph does -- and the graph
    /// is built from the encoder output, which comes later.
    pub(crate) fn new(
        targets: BTreeMap<i32, Vec<AssignmentTarget>>,
        sources: Vec<RoundSplitSource>,
        retry_policy: TaskUpdateRetryPolicy,
        initial_dynamic_filter_wait_cap: std::time::Duration,
        endpoints: Vec<(usize, RuntimeEndpoint)>,
    ) -> Self {
        Self {
            targets,
            sources,
            retry_policy,
            initial_dynamic_filter_wait_cap,
            endpoints,
        }
    }

    /// The scan nodes this plan opened a source for.
    pub(crate) fn plan_node_ids(&self) -> impl Iterator<Item = i32> + '_ {
        self.sources.iter().map(|source| source.plan_node_id)
    }

    /// Every backend this round may address.
    pub(crate) fn endpoints(&self) -> &[(usize, RuntimeEndpoint)] {
        &self.endpoints
    }
}

impl Drop for RoundSplitAssignmentPlan {
    fn drop(&mut self) {
        for source in &mut self.sources {
            if let Err(error) = source.source.close() {
                tracing::warn!(
                    plan_node_id = source.plan_node_id,
                    error = %error,
                    "closing an unstarted split source failed"
                );
            }
            // The same evidence a started round emits: a source opened and
            // closed without ever assigning is still a source that closed.
            emit_split_source_close_marker(source.plan_node_id);
        }
    }
}

/// A running round's split-assignment worker.
///
/// Dropping it stops the pump and closes every source, so no exit path has to
/// remember to.
pub(crate) struct SplitAssignmentRoundGuard {
    stop: RoundSplitAssignmentStop,
    worker: Option<std::thread::JoinHandle<Result<SplitSourceProfile, SplitAssignmentDriverError>>>,
}

impl SplitAssignmentRoundGuard {
    /// Start pumping. Returns `None` when this round has no typed scan, so a
    /// query that reads nothing through a connector starts no thread.
    pub(crate) fn start(
        execution_id: QueryExecutionId,
        mut plan: RoundSplitAssignmentPlan,
        transport: Arc<dyn TaskUpdateTransport>,
    ) -> Option<Self> {
        // Taken, not borrowed: the sources move into the round, so the plan's
        // own drop must not close what the round now owns.
        let sources = std::mem::take(&mut plan.sources);
        let tasks = std::mem::take(&mut plan.targets);
        if sources.is_empty() {
            return None;
        }
        let mut assignment = RoundSplitAssignment::new(
            execution_id,
            transport,
            tasks,
            DEFAULT_MAX_QUEUED_SPLITS_PER_TASK,
            sources,
            plan.retry_policy,
            plan.initial_dynamic_filter_wait_cap,
        );
        let stop = assignment.stop_handle();
        let worker = std::thread::Builder::new()
            .name(format!(
                "split-assign-{:x}-{:x}-{}",
                execution_id.query_id().high(),
                execution_id.query_id().low(),
                execution_id.attempt_id().get()
            ))
            .spawn(move || {
                let result = assignment.pump_to_completion();
                // The round owns its sources; closing here rather than only on
                // drop means a failed pump releases them immediately instead of
                // at an unpredictable later moment.
                assignment.close();
                result
            })
            .ok()?;
        Some(Self {
            stop,
            worker: Some(worker),
        })
    }

    /// Whether the worker has stopped.
    ///
    /// The task substrate's delivery bridge is settled by the round runner on
    /// the statement thread, so a worker still waiting for an acknowledgement
    /// can only be released by another turn. Joining it before it has stopped
    /// would stop the very loop that releases it, so the caller asks first and
    /// keeps turning until this is true.
    pub(crate) fn is_finished(&self) -> bool {
        self.worker
            .as_ref()
            .is_none_or(std::thread::JoinHandle::is_finished)
    }

    /// Wait for delivery to finish, returning the worker result.
    ///
    /// The root fetch may complete before an unknown-outcome TaskUpdate has
    /// received its retry acknowledgement. A normal successful finish must
    /// therefore not stop the worker: doing so would discard an accepted but
    /// unconfirmed immutable assignment. Cancellation and unwinding still use
    /// `Drop`, which signals stop before joining.
    pub(crate) fn finish(mut self) -> Result<SplitSourceProfile, SplitAssignmentDriverError> {
        match self.worker.take() {
            Some(worker) => worker.join().unwrap_or(Ok(SplitSourceProfile::default())),
            None => Ok(SplitSourceProfile::default()),
        }
    }
}

impl Drop for SplitAssignmentRoundGuard {
    fn drop(&mut self) {
        self.stop.stop();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use novarocks_types::{AttemptId, QueryId};

    use super::*;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(9, 9), AttemptId::new(1).expect("attempt"))
            .expect("execution id")
    }

    #[test]
    fn a_round_with_no_typed_scan_starts_no_worker() {
        struct NeverCalled;
        impl TaskUpdateTransport for NeverCalled {
            fn send(
                &self,
                _execution_id: QueryExecutionId,
                _target: &AssignmentTarget,
                _request: &crate::query_execution::connector_domain::TaskUpdateRequest,
                _timeout: std::time::Duration,
                _stop: &crate::query_execution::split_assignment::SplitAssignmentStop,
            ) -> Result<
                crate::query_execution::split_assignment::TaskUpdateOutcome,
                crate::query_execution::split_assignment::TaskUpdateTransportError,
            > {
                panic!("a round with no source must never send");
            }
        }
        assert!(
            SplitAssignmentRoundGuard::start(
                execution_id(),
                RoundSplitAssignmentPlan::new(
                    BTreeMap::new(),
                    Vec::new(),
                    TaskUpdateRetryPolicy::default(),
                    std::time::Duration::ZERO,
                    Vec::new(),
                ),
                Arc::new(NeverCalled),
            )
            .is_none()
        );
    }

    #[test]
    fn finish_does_not_cancel_an_in_flight_delivery() {
        let stop = RoundSplitAssignmentStop::default();
        let observed_stop = Arc::new(AtomicBool::new(true));
        let worker_stop = stop.clone();
        let worker_observed_stop = Arc::clone(&observed_stop);
        let guard = SplitAssignmentRoundGuard {
            stop,
            worker: Some(std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(10));
                worker_observed_stop.store(worker_stop.is_stopped(), Ordering::SeqCst);
                Ok(SplitSourceProfile::default())
            })),
        };

        guard.finish().expect("finish waits for delivery");

        assert!(
            !observed_stop.load(Ordering::SeqCst),
            "normal finish must not interrupt an in-flight task update retry"
        );
    }
}
