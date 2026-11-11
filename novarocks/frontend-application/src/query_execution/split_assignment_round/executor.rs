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

//! Async entry over the original split I/O machine. Connector enumeration,
//! retry waits and close remain its original algorithms; Task protocol policy
//! is absent. The common Source retains this record until the real job exits.

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

#[derive(Debug)]
struct ExecutorWake(Arc<Notify>);
impl StatusIntakeWake for ExecutorWake {
    fn wake(&self) {
        self.0.notify_one();
    }
}

/// The original Source prepays distinct failure and profile inputs before the
/// runner starts. The failure author retains this exact first typed cause in
/// its original rich record before publishing ExecutorFact::PumpFailed; this
/// is protocol visibility, not PumpFinished, profile availability or actual
/// job/capture exit. Profile publication retains its original consumer too.
/// Both synchronous handoffs use that SAME admitted Source and its original
/// paid inputs, without a new queue, Work, capacity grant or runtime fallback.
/// On refusal the Source retains the original publication debt/cause; the
/// runner records the handoff error and stops, without resending/spinning.
/// P10a must supply this real author; P10d provides no production fallback.
pub(crate) trait OriginalSplitPublisher: Send + Sync {
    fn publish_original_profile(
        &self,
        observation: SplitAssignmentProfileRead,
    ) -> Result<(), TaskExecutionError>;
    fn publish_original_failure(
        &self,
        failure: SplitAssignmentDriverError,
    ) -> Result<(), TaskExecutionError>;
}

/// A dormant attempt has no Core split recipient. Its original close still
/// returns the typed result to the Cleanup owner.
struct UnstartedSplitClose;
impl OriginalSplitPublisher for UnstartedSplitClose {
    fn publish_original_profile(
        &self,
        _: SplitAssignmentProfileRead,
    ) -> Result<(), TaskExecutionError> {
        Ok(())
    }

    fn publish_original_failure(
        &self,
        _: SplitAssignmentDriverError,
    ) -> Result<(), TaskExecutionError> {
        Ok(())
    }
}

pub(crate) struct SplitAssignmentExecutor {
    pump: Mutex<Option<SplitAssignmentPump>>,
    stop: RoundSplitAssignmentStop,
    changed: Arc<Notify>,
    stop_requested: AtomicBool,
    started: AtomicBool,
    failure: Arc<Mutex<Option<SplitAssignmentDriverError>>>,
    outcome: Arc<Mutex<Option<Result<SplitSourceProfile, SplitAssignmentDriverError>>>>,
    primary: Mutex<Option<TaskExecutionError>>,
    unresolved: Arc<Mutex<SplitUnresolved>>,
}
impl SplitAssignmentExecutor {
    /// The caller has already admitted the original Source and job. This takes
    /// the same opened sources once, exactly as the old guard's installation.
    pub(crate) fn prepare(
        execution_id: QueryExecutionId,
        mut plan: RoundSplitAssignmentPlan,
        transport: Arc<dyn TaskUpdateTransport>,
        data_runtime: FrontendDataRuntime,
    ) -> Option<Arc<Self>> {
        let sources = std::mem::take(&mut plan.sources);
        let tasks = std::mem::take(&mut plan.targets);
        if sources.is_empty() {
            return None;
        }
        let assignment = RoundSplitAssignment::new(
            execution_id,
            transport,
            tasks,
            DEFAULT_MAX_QUEUED_SPLITS_PER_TASK,
            sources,
            plan.retry_policy,
            plan.initial_dynamic_filter_wait_cap,
        );
        let stop = assignment.stop_handle();
        let failure = Arc::new(Mutex::new(None));
        let outcome = Arc::new(Mutex::new(None));
        let unresolved = Arc::new(Mutex::new(SplitUnresolved::default()));
        let changed = Arc::new(Notify::new());
        let wake: Arc<dyn StatusIntakeWake> = Arc::new(ExecutorWake(changed.clone()));
        let pump = SplitAssignmentPump {
            assignment: Some(assignment),
            stop: stop.clone(),
            data_runtime,
            wake: wake.clone(),
            enumeration: None,
            enumeration_result: None,
            enumeration_in_flight: false,
            close_job: None,
            closing_reason: None,
            source_waker: std::task::Waker::from(Arc::new(PumpWake(wake))),
            timer: None,
            retry_at: None,
            timer_at: None,
            failure: failure.clone(),
            outcome: outcome.clone(),
            closing: false,
            close_joined: false,
            cleanup_failure: Arc::new(Mutex::new(None)),
            cleanup_failure_reported: false,
            unresolved: unresolved.clone(),
            unresolved_reported: false,
        };
        Some(Arc::new(Self {
            pump: Mutex::new(Some(pump)),
            stop,
            changed,
            stop_requested: AtomicBool::new(false),
            started: AtomicBool::new(false),
            failure,
            outcome,
            primary: Mutex::new(None),
            unresolved,
        }))
    }
    /// Bind this exact runner wake to its original delivery transport before
    /// handing the runner job to an executor. A Source wake alone cannot wake
    /// a physical future parked on a driver delivery.
    pub(crate) fn wake_handle(&self) -> Arc<dyn StatusIntakeWake> {
        Arc::new(ExecutorWake(self.changed.clone()))
    }
    pub(crate) fn request_stop(&self) {
        self.stop_requested.store(true, Ordering::Release);
        self.changed.notify_one();
    }
    pub(crate) fn has_started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    pub(crate) async fn close_unstarted(self: Arc<Self>) -> Result<(), TaskExecutionError> {
        if self.has_started() {
            return Err(TaskExecutionError::Schedule(
                "split cleanup cannot replace an already started runner".into(),
            ));
        }
        self.request_stop();
        self.run_admitted(&UnstartedSplitClose).await.map(|_| ())
    }

    pub(crate) fn profile_source(&self) -> SplitAssignmentProfileSource {
        SplitAssignmentProfileSource {
            outcome: Arc::downgrade(&self.outcome),
        }
    }
    pub(crate) fn failure_source(&self) -> SplitAssignmentFailureSource {
        SplitAssignmentFailureSource {
            failure: self.failure.clone(),
        }
    }
    /// This getter reports the original health source without giving the core a
    /// false normal finish. The Source keeps the same runner/custody unresolved.
    pub(crate) fn unresolved(&self) -> Option<PumpCloseUnresolved> {
        let close_joined = self
            .pump
            .lock()
            .expect("original split executor record")
            .as_ref()
            .is_none_or(|pump| pump.close_joined);
        let faults = self
            .unresolved
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let cause: Arc<dyn std::error::Error + Send + Sync> =
            if let Some(cause) = faults.enumeration.as_ref().or(faults.close.as_ref()) {
                cause.clone()
            } else if !close_joined {
                faults.provider_close.as_ref()?.clone()
            } else {
                return None;
            };
        Some(PumpCloseUnresolved {
            owner: "split_assignment",
            cause,
        })
    }
    /// Run only under the original ExitObligation wrapper. Dropping a borrowed
    /// waiter does not destroy this Source-owned physical machine or its jobs.
    pub(crate) async fn run_admitted(
        self: Arc<Self>,
        publisher: &dyn OriginalSplitPublisher,
    ) -> Result<SplitSourceProfile, TaskExecutionError> {
        assert!(
            !self.started.swap(true, Ordering::AcqRel),
            "one original split runner"
        );
        let mut published_profile = None;
        let mut attempted_failure = false;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (moved, completed) = {
                let mut original = self.pump.lock().expect("original split executor record");
                let pump = original
                    .as_mut()
                    .expect("original split runner has not exited");
                // Serialize stop with the driver's own delivery turn. A
                // concurrent stop cannot turn a legitimate in-flight update
                // into the driver's TaskUpdateTransportError::Closed path.
                if self.stop_requested.load(Ordering::Acquire) {
                    self.stop.stop();
                }
                let moved = match pump.advance() {
                    Ok(moved) => moved,
                    Err(error) => {
                        let mut primary =
                            self.primary.lock().expect("original split executor error");
                        if primary.is_none() {
                            *primary = Some(error);
                        }
                        self.stop.stop();
                        1
                    }
                };
                let completed = pump.close_joined
                    && pump
                        .outcome
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .is_some()
                    && pump
                        .enumeration
                        .as_ref()
                        .is_none_or(|job| !job.input_or_value_retained());
                (moved, completed)
            };
            // start_close records the first cause immediately, while the
            // original close/enumeration may still be live. Read that SAME
            // author now; outcome/profile is intentionally not its substitute.
            if !attempted_failure {
                let failure = self
                    .failure
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone();
                if let Some(failure) = failure {
                    // Mark the sole handoff attempt BEFORE foreign publication.
                    // A refusal remains primary+stop custody, never a retry loop.
                    attempted_failure = true;
                    if let Err(error) = publisher.publish_original_failure(failure) {
                        let mut primary =
                            self.primary.lock().expect("original split executor error");
                        if primary.is_none() {
                            *primary = Some(error);
                        }
                        self.stop.stop();
                    }
                }
            }
            // The original outcome can become Failed while its close/enumeration
            // still owns captures. Publish that immutable diagnostic fact now,
            // independently of the credentialed runner's eventual physical exit.
            let profile = self
                .outcome
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
                .map(|result| result.as_ref().copied().map_err(|_| ()));
            if let Some(profile) = profile
                && published_profile != Some(profile)
            {
                published_profile = Some(profile);
                let observation = match profile {
                    Ok(profile) => SplitAssignmentProfileRead::Available(profile),
                    Err(()) => SplitAssignmentProfileRead::Failed,
                };
                if let Err(error) = publisher.publish_original_profile(observation) {
                    let mut primary = self.primary.lock().expect("original split executor error");
                    if primary.is_none() {
                        *primary = Some(error);
                    }
                    self.stop.stop();
                }
            }
            if completed {
                // Destroy the exact machine and captures in this admitted job,
                // before its credentialed wrapper can publish physical exit.
                let original = self
                    .pump
                    .lock()
                    .expect("original split executor record")
                    .take();
                drop(original);
                if let Some(error) = self
                    .primary
                    .lock()
                    .expect("original split executor error")
                    .take()
                {
                    return Err(error);
                }
                return self
                    .outcome
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .as_ref()
                    .expect("real closed split profile")
                    .clone()
                    .map_err(TaskExecutionError::SplitAssignment);
            }
            if moved != 0 {
                tokio::task::yield_now().await;
            } else {
                notified.await;
            }
        }
    }
}

#[cfg(test)]
#[path = "executor_tests.rs"]
mod tests;
