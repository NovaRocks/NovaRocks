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

//! The original FE deadline service also drives the one joint progress owner.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use novarocks_workload_control::DeadlineExpiryHandle;
use novarocks_workload_control::joint_admission::{JointProgress, JointProgressDriver};
use tokio::runtime::Handle;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::{Instant, Sleep};

/// Explicit process policy; this type supplies no admission or readiness right.
#[derive(Clone, Copy)]
pub(super) struct FrontendWorkloadProgressPolicy {
    recheck_interval: Duration,
    first_recheck: Instant,
}
impl FrontendWorkloadProgressPolicy {
    pub(super) fn validate(recheck_interval: Duration) -> Result<Self, &'static str> {
        if recheck_interval.is_zero() {
            return Err("joint backing recheck interval must be nonzero");
        }
        let first_recheck = Instant::now()
            .checked_add(recheck_interval)
            .ok_or("joint backing recheck interval exceeds the clock range")?;
        Ok(Self {
            recheck_interval,
            first_recheck,
        })
    }
}

/// A bounded error marker. The original JoinError stays in its supervisor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FrontendWorkloadProgressFailed;

/// This is custody of the one existing driver, not a second progress author.
/// The mutex protects only an Option move; callbacks never run under it.
struct DriverCustody {
    driver: Mutex<Option<Box<JointProgressDriver>>>,
}
impl DriverCustody {
    fn with_driver<R>(&self, operation: impl FnOnce(&mut JointProgressDriver) -> R) -> R {
        let driver = {
            self.driver
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        }
        .expect("the unique deadline worker owns the original driver turn");
        let mut lease = DriverLease {
            custody: self,
            driver: Some(driver),
        };
        let result = operation(lease.driver.as_deref_mut().expect("original driver lease"));
        drop(lease);
        result
    }
}
struct DriverLease<'a> {
    custody: &'a DriverCustody,
    driver: Option<Box<JointProgressDriver>>,
}
impl Drop for DriverLease<'_> {
    fn drop(&mut self) {
        let driver = self
            .driver
            .take()
            .expect("return the same original driver once");
        let previous = {
            self.custody
                .driver
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .replace(driver)
        };
        assert!(
            previous.is_none(),
            "original joint driver custody was replaced"
        );
    }
}

/// One existing FE service and its original join. Neither the service nor a
/// cancellable shutdown await owns the only copy of the actual driver.
pub(super) struct FrontendWorkloadDeadlineSupervisor {
    worker: Option<JoinHandle<()>>,
    worker_failure: Option<JoinError>,
    driver: Option<Arc<DriverCustody>>,
    stop: Option<tokio::sync::watch::Sender<bool>>,
    shutdown_requested: bool,
    unexpected_exit: bool,
    shutdown_complete: bool,
}
impl FrontendWorkloadDeadlineSupervisor {
    pub(super) fn start(
        runtime: &Handle,
        deadlines: DeadlineExpiryHandle,
        driver: JointProgressDriver,
        policy: FrontendWorkloadProgressPolicy,
    ) -> Self {
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let driver = Arc::new(DriverCustody {
            driver: Mutex::new(Some(Box::new(driver))),
        });
        // Establish stable original custody before spawn can call foreign code.
        let mut owner = Self {
            worker: None,
            worker_failure: None,
            driver: Some(Arc::clone(&driver)),
            stop: Some(stop),
            shutdown_requested: false,
            unexpected_exit: false,
            shutdown_complete: false,
        };
        owner.worker =
            Some(runtime.spawn(run_original_progress(deadlines, driver, stopped, policy)));
        owner
    }

    /// Pure observation of the SAME original join, suitable for the existing
    /// Host listener-health loop. It neither joins nor disposes the raw cause.
    pub(super) fn has_exited(&self) -> bool {
        self.worker_failure.is_some()
            || self.unexpected_exit
            || self.worker.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Borrow the original opaque error. A formatted diagnostic is not its
    /// disposition and must not allow the aggregate to report clean shutdown.
    pub(super) fn original_failure(&self) -> Option<&JoinError> {
        self.worker_failure.as_ref()
    }

    /// The existing aggregate health poller also polls this original join.
    /// Ready(Ok) before requested shutdown is an unexpected service stop;
    /// Ready(Err) retains the exact original JoinError for original_failure().
    pub(super) fn poll_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), FrontendWorkloadProgressFailed>> {
        if self.worker_failure.is_some() || self.unexpected_exit {
            return Poll::Ready(Err(FrontendWorkloadProgressFailed));
        }
        let Some(worker) = self.worker.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        let outcome = match Pin::new(worker).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(outcome) => outcome,
        };
        // Stabilize the original cause/status before releasing the completed
        // join and its runtime backing. The returned marker owns no payload.
        let result = match outcome {
            Ok(()) if self.shutdown_requested => Ok(()),
            Ok(()) => {
                self.unexpected_exit = true;
                Err(FrontendWorkloadProgressFailed)
            }
            Err(error) => {
                self.worker_failure = Some(error);
                Err(FrontendWorkloadProgressFailed)
            }
        };
        drop(self.worker.take());
        Poll::Ready(result)
    }

    /// Call only after the SAME aggregate proved its WC actually drained.
    /// Canceling this await keeps the original join and driver in Self.
    /// A failed worker is never restarted and its unwound driver is not repolled.
    pub(super) async fn shutdown_after_workload_drained(
        &mut self,
    ) -> Result<(), FrontendWorkloadProgressFailed> {
        if self.worker_failure.is_some() || self.unexpected_exit {
            return Err(FrontendWorkloadProgressFailed);
        }
        if self.shutdown_complete {
            return Ok(());
        }
        // A previously stopped worker is not made normal by a late shutdown.
        if !self.shutdown_requested && self.worker.as_ref().is_some_and(JoinHandle::is_finished) {
            let _ = poll_fn(|cx| self.poll_exit(cx)).await;
            return Err(FrontendWorkloadProgressFailed);
        }
        self.shutdown_requested = true;
        self.stop
            .as_ref()
            .expect("original stop owner")
            .send_replace(true);
        // poll_exit only takes the original handle after actual join. A
        // canceled borrowed await cannot mark shutdown complete or take it.
        poll_fn(|cx| self.poll_exit(cx)).await?;
        self.shutdown_complete = true;
        // Keep the actual driver here until this original supervisor exits.
        Ok(())
    }

    pub(super) fn abort_for_process_exit(&self) {
        self.stop
            .as_ref()
            .expect("original stop owner")
            .send_replace(true);
        if let Some(worker) = &self.worker {
            worker.abort();
        }
        // An abort request is not actual exit. Root Drop retains this custody.
    }
}
impl Drop for FrontendWorkloadDeadlineSupervisor {
    fn drop(&mut self) {
        if !self.shutdown_complete {
            // Illegal owner loss, not normal shutdown. Preserve the ORIGINAL
            // finite driver/turn/join/cause to process exit before any foreign
            // callback or destructor; do not abort-and-detach or close requests.
            std::mem::forget(self.worker.take());
            std::mem::forget(self.driver.take());
            std::mem::forget(self.worker_failure.take());
            std::mem::forget(self.stop.take());
        }
    }
}

async fn run_original_progress(
    deadlines: DeadlineExpiryHandle,
    driver: Arc<DriverCustody>,
    mut stopped: tokio::sync::watch::Receiver<bool>,
    policy: FrontendWorkloadProgressPolicy,
) {
    // One owned timer frame for this original process service. Reset
    // the same frame; do not create per-Work or per-turn timers.
    let mut timer: std::pin::Pin<Box<Sleep>> =
        Box::pin(tokio::time::sleep_until(policy.first_recheck));
    let mut next_recheck = policy.first_recheck;
    loop {
        if *stopped.borrow() {
            return;
        }
        deadlines.expire_deadlines();
        let now = Instant::now();
        if now >= next_recheck {
            driver.with_driver(JointProgressDriver::recheck_retired_backing);
            next_recheck = now
                .checked_add(policy.recheck_interval)
                .expect("joint backing recheck exceeded the clock range");
        }
        let progress = driver.with_driver(JointProgressDriver::step);
        match progress {
            JointProgress::More => {
                // Check deadlines, stop and the absolute recheck time
                // again after yielding; hot work cannot defer that tick.
                tokio::task::yield_now().await;
            }
            JointProgress::Idle(marker) => {
                let wake_at = deadlines
                    .next_deadline()
                    .map_or(next_recheck, |deadline| deadline.min(next_recheck));
                timer.as_mut().reset(wake_at);
                tokio::select! {
                    changed = stopped.changed() => {
                        if changed.is_err() || *stopped.borrow() { return; }
                    }
                    _ = deadlines.wait_state_change(marker) => {}
                    _ = timer.as_mut() => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recheck_policy_requires_a_nonzero_owned_service_interval() {
        assert!(FrontendWorkloadProgressPolicy::validate(Duration::ZERO).is_err());
        assert!(FrontendWorkloadProgressPolicy::validate(Duration::from_millis(25)).is_ok());
    }
}
