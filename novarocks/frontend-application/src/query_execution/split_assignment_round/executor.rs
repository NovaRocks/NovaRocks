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

/// The original Source reserves this one profile-result position before the
/// runner starts. Implementations register Available in their immutable rich
/// records before filling ExecutorFact::SplitProfile on that same paid Source.
/// This synchronous handoff must not allocate a queue, submit Work, await the
/// core, or use PumpFinished/actual exit as a substitute for profile visibility.
/// The Source retains the rich record through its final publication consumer.
/// Production composition must supply this port; there is no silent fallback.
pub(crate) trait OriginalSplitProfilePublisher: Send + Sync {
    fn publish_original_profile(
        &self,
        observation: SplitAssignmentProfileRead,
    ) -> Result<(), TaskExecutionError>;
}

pub(crate) struct SplitAssignmentExecutor {
    pump: Mutex<Option<SplitAssignmentPump>>,
    stop: RoundSplitAssignmentStop,
    changed: Arc<Notify>,
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
        self.stop.stop();
        self.changed.notify_one();
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
        profile_publisher: &dyn OriginalSplitProfilePublisher,
    ) -> Result<SplitSourceProfile, TaskExecutionError> {
        assert!(
            !self.started.swap(true, Ordering::AcqRel),
            "one original split runner"
        );
        let mut published_profile = None;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (moved, completed) = {
                let mut original = self.pump.lock().expect("original split executor record");
                let pump = original
                    .as_mut()
                    .expect("original split runner has not exited");
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
                if let Err(error) = profile_publisher.publish_original_profile(observation) {
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
