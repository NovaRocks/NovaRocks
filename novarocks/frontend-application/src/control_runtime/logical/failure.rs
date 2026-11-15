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

//! The exact infrastructure cause and the original prepaid failure marker.
mod driver;
mod originals;
pub(super) use originals::LogicalFailureOriginals;
mod progress;
use super::work::{FailureReporter, RawFailure};
use crate::control_runtime::actor::{FrontendActor, FrontendInput};
pub(super) use driver::LogicalFailureDriver;
use novarocks_control_runtime::actor_turn::{
    ActorFailureProof, ActorTurnError, StandingInputObligation,
};
use novarocks_coordination_contract::{
    ExecutionFailureInput, ExecutionFailureSite, MonotonicInstant, QueryExecutionError,
};
use novarocks_execution_contract::{
    SafeDetail,
    failure::{FailureClass, FailureFact, FailureReason, domain},
};
pub(super) use progress::LogicalFailureProgressObligation;
use std::{
    fmt,
    mem::ManuallyDrop,
    num::NonZeroU16,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::Waker,
    time::Duration,
};

#[derive(Clone, Copy, Debug)]
pub(super) struct LogicalFailureCut {
    pub(super) cut: MonotonicInstant,
    pub(super) remote_deadline: MonotonicInstant,
}
impl LogicalFailureCut {
    pub(super) fn from_original(proof: &ActorFailureProof, budget: Duration) -> Self {
        let cut = MonotonicInstant::from_origin(Duration::from_nanos(proof.failed_at().0));
        Self {
            cut,
            remote_deadline: cut.saturating_add(budget),
        }
    }
}

pub(super) struct LogicalInfrastructureFailure {
    originals: Mutex<Vec<ManuallyDrop<RawFailure>>>,
    error: Mutex<Option<QueryExecutionError>>,
    marker: Mutex<Option<StandingInputObligation<FrontendActor>>>,
    published: AtomicBool,
    wake: Waker,
}
impl fmt::Debug for LogicalInfrastructureFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogicalInfrastructureFailure")
            .field("published", &self.published.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}
impl fmt::Display for LogicalInfrastructureFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("logical execution retained original infrastructure failure custody")
    }
}
impl std::error::Error for LogicalInfrastructureFailure {}
impl LogicalInfrastructureFailure {
    pub(super) fn new(marker: StandingInputObligation<FrontendActor>, wake: Waker) -> Arc<Self> {
        Arc::new(Self {
            originals: Mutex::new(Vec::new()),
            error: Mutex::new(None),
            marker: Mutex::new(Some(marker)),
            published: AtomicBool::new(false),
            wake,
        })
    }
    pub(super) fn reporter(self: &Arc<Self>) -> FailureReporter {
        let original = self.clone();
        Arc::new(move |failure| original.retain(failure))
    }
    pub(super) fn retain(&self, original: RawFailure) {
        self.originals
            .lock()
            .unwrap()
            .push(ManuallyDrop::new(original));
        self.publish();
    }
    pub(super) fn retain_typed(&self, error: QueryExecutionError) {
        let mut selected = self.error.lock().unwrap();
        if selected.is_none() {
            *selected = Some(error);
        }
        drop(selected);
        self.publish();
    }
    fn publish(&self) {
        if !self.published.swap(true, Ordering::AcqRel) {
            let marker = self
                .marker
                .lock()
                .unwrap()
                .take()
                .expect("original logical failure marker");
            if let Err((error, original)) = marker.fill(FrontendInput::LogicalInfrastructureFailed)
            {
                self.originals
                    .lock()
                    .unwrap()
                    .push(ManuallyDrop::new(Box::new((error, original))));
            }
            *self.marker.lock().unwrap() = Some(marker);
        }
        self.wake.wake_by_ref();
    }
    /// A fault on the SAME paid actor-turn source, compiled only into tests.
    /// The normal runtime catches the actual Rust panic and mints its proof.
    #[cfg(test)]
    pub(in crate::control_runtime::logical) fn inject_core_turn_panic(&self) {
        let marker = self
            .marker
            .lock()
            .unwrap()
            .take()
            .expect("same original marker");
        let published = marker.fill(FrontendInput::Test(u64::MAX));
        *self.marker.lock().unwrap() = Some(marker);
        self.wake.wake_by_ref();
        published
            .unwrap_or_else(|(error, _)| panic!("original test fault source refused: {error:?}"));
    }
    pub(super) fn failed(&self) -> bool {
        self.published.load(Ordering::Acquire)
    }
    /// Core failure already fenced the original actor. Keep that health level
    /// without trying to fill another marker into the failed destination.
    pub(super) fn observe_owner_failed(&self) {
        self.published.store(true, Ordering::Release);
        self.wake.wake_by_ref();
    }
    pub(super) fn error(&self) -> QueryExecutionError {
        self.error
            .lock()
            .unwrap()
            .clone()
            .filter(|original| {
                original
                    .execution_failure()
                    .is_some_and(|input| input.fact().class() == FailureClass::Invariant)
            })
            .unwrap_or_else(|| {
                QueryExecutionError::from_execution(ExecutionFailureInput::new(
                    FailureFact::new(
                        FailureClass::Invariant,
                        FailureReason::new(domain::QUERY, NonZeroU16::new(84).unwrap()),
                    ),
                    SafeDetail::truncating("logical execution infrastructure failed"),
                    ExecutionFailureSite::Coordinator,
                ))
            })
    }
    /// Called only after every producer/callback has really exited. The marker
    /// is passive; it cannot dispose any retained foreign panic or capture.
    pub(super) fn close_marker_after_exit(&self, failed: bool) -> Result<bool, ActorTurnError> {
        let Some(mut marker) = self.marker.lock().unwrap().take() else {
            return Ok(false);
        };
        let progress = (|| {
            if !marker.retire_ready_after_exit()? {
                if failed && let Some(original) = marker.take_failed_role_payload()? {
                    original.dispose()?;
                }
                if !marker.retire_ready_after_exit()? {
                    return Ok(false);
                }
            }
            marker.close_after_exit()?;
            Ok(true)
        })();
        if !matches!(progress, Ok(true)) {
            *self.marker.lock().unwrap() = Some(marker);
        }
        progress
    }
}
