// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One observer of the original scope latch. Stopping observation never
//! requests query cancellation or supplies an actual-exit certificate.

use crate::api::logical_runtime::{
    LogicalFactSource, LogicalFuture, LogicalHostFact, LogicalSourceClosed,
};
use novarocks_logical_execution_protocol::cancel::LogicalCancellationReason;
use novarocks_workload_control::{CancellationReason, CancellationView};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

pub struct CancelObserver {
    original: CancellationView,
    started: AtomicBool,
    stopped: AtomicBool,
    changed: Notify,
}

impl CancelObserver {
    pub fn new(original: CancellationView) -> Self {
        Self {
            original,
            started: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            changed: Notify::new(),
        }
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    pub fn observe<'a>(
        &'a self,
        source: &'a mut dyn LogicalFactSource,
    ) -> LogicalFuture<'a, Result<(), LogicalSourceClosed>> {
        Box::pin(async move {
            if self.started.swap(true, Ordering::AcqRel) {
                return Err(LogicalSourceClosed);
            }
            let stopped = self.changed.notified();
            tokio::pin!(stopped);
            stopped.as_mut().enable();
            if self.stopped.load(Ordering::Acquire) {
                return Ok(());
            }
            let reason = tokio::select! {
                biased;
                _ = &mut stopped => return Ok(()),
                reason = self.original.cancelled() => reason,
            };
            // Once selected, the original standing source retains delivery.
            // Closing the carrier source settles this wait during retirement.
            source
                .send(LogicalHostFact::Cancelled(project_reason(reason)))
                .await
        })
    }
}

fn project_reason(reason: CancellationReason) -> LogicalCancellationReason {
    use CancellationReason as R;
    match reason {
        R::DeadlineExceeded => LogicalCancellationReason::DeadlineExceeded,
        R::FrontendDrainDeadlineExceeded => {
            LogicalCancellationReason::FrontendDrainDeadlineExceeded
        }
        R::ServerShutdown => LogicalCancellationReason::ServerShutdown,
        R::OwnerDropped => LogicalCancellationReason::OwnerDropped,
        R::Requested => LogicalCancellationReason::Requested,
        R::ClientCancelRequested => LogicalCancellationReason::ClientCancelRequested,
        R::ClientDisconnected => LogicalCancellationReason::ClientDisconnected,
        R::ExplicitKill { .. } => LogicalCancellationReason::ExplicitKill,
        R::ExplicitKillConnection { .. } => LogicalCancellationReason::ExplicitKillConnection,
    }
}

#[cfg(test)]
#[path = "cancel/tests.rs"]
mod tests;
