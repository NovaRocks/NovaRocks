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

//! Stable physical execution custody in the original adapter's active map.

use super::*;
use novarocks_failure_contract::{FailureFact, FailureProjection};
use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

type PhysicalFuture = Pin<
    Box<dyn Future<Output = Result<ConnectorMetadataMaintenanceOutcome, ConnectorError>> + Send>,
>;
type Active = Mutex<HashMap<ConnectorMutationOperationId, Arc<MaintenanceExecution>>>;
type Terminal = Mutex<HashMap<ConnectorMutationOperationId, TerminalRecord>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Waiting,
    Polling,
    TerminalSaved,
    Closed,
    Poisoned,
}

struct ExecutionState {
    future: Option<PhysicalFuture>,
    phase: Phase,
    result: Option<Result<ConnectorMetadataMaintenanceOutcome, ConnectorError>>,
    failure: Option<ConnectorError>,
    retiring_terminal: Option<TerminalRecord>,
    operation: Option<IcebergCommitOperation>,
    journal: Option<PublicationJournal<MaintenancePublication>>,
}

pub(super) struct MaintenanceExecution {
    pub(super) operation: ConnectorMutationOperationId,
    plan_digest: [u8; 32],
    active: Weak<Active>,
    terminal: Arc<Terminal>,
    state: Mutex<ExecutionState>,
}

/// The raw panic stays in this one original execution row. Formatting never
/// formats or disposes the original payload.
#[derive(Clone)]
struct MaintenancePanic {
    _payload: Arc<Mutex<Option<Box<dyn Any + Send>>>>,
}
impl std::fmt::Debug for MaintenancePanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MaintenancePanic")
    }
}
impl std::fmt::Display for MaintenancePanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Iceberg maintenance physical future did not prove actual exit")
    }
}
impl std::error::Error for MaintenancePanic {}
impl FailureProjection for MaintenancePanic {
    fn failure_fact(&self) -> FailureFact {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            "maintenance physical future panicked",
        )
        .failure_fact()
    }
}

impl MaintenanceExecution {
    pub(super) fn new(
        operation: ConnectorMutationOperationId,
        plan_digest: [u8; 32],
        active: Weak<Active>,
        terminal: Arc<Terminal>,
    ) -> Self {
        Self {
            operation,
            plan_digest,
            active,
            terminal,
            state: Mutex::new(ExecutionState {
                future: None,
                phase: Phase::Waiting,
                result: None,
                failure: None,
                retiring_terminal: None,
                operation: None,
                journal: None,
            }),
        }
    }
    pub(super) fn install(&self, future: PhysicalFuture) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.future.is_none() && state.phase == Phase::Waiting);
        state.future = Some(future);
    }
    pub(super) fn retain_operation(
        &self,
        operation: IcebergCommitOperation,
        journal: PublicationJournal<MaintenancePublication>,
    ) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.operation.is_none() && state.journal.is_none());
        state.operation = Some(operation);
        state.journal = Some(journal);
    }
    pub(super) fn retain_journal(&self, journal: PublicationJournal<MaintenancePublication>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.journal.is_none());
        state.journal = Some(journal);
    }
    fn poison(&self, panic: Box<dyn Any + Send>, future: Option<PhysicalFuture>) -> ConnectorError {
        let error = ConnectorError::from_domain(
            ConnectorErrorKind::Internal,
            MaintenancePanic {
                _payload: Arc::new(Mutex::new(Some(panic))),
            },
        );
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A poll-unwound future is retained, never polled again. A Drop-unwound
        // future has already consumed its Box; the original row/journal stays.
        state.future = future;
        state.phase = Phase::Poisoned;
        state.failure = Some(error.clone());
        error
    }
}

impl ConnectorMetadataMaintenanceExecutionSource for MaintenanceExecution {
    fn cleanup_responsibility(
        &self,
    ) -> novarocks_spi::connector::ConnectorMetadataCleanupResponsibility {
        use novarocks_spi::connector::ConnectorMetadataCleanupResponsibility as R;
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase != Phase::Closed || state.failure.is_some() {
            return R::Retained;
        }
        // Borrow the exact already-retained outcome. The same row still owns
        // its original operation/journal and full cleanup report, not a copy
        // hidden in a diagnostic string or the unbounded replay cache.
        match &state.result {
            Some(Ok(
                ExternalMutationOutcome::KnownCommitted {
                    cleanup: ExternalMutationFinalization::Complete,
                    ..
                }
                | ExternalMutationOutcome::KnownUncommitted {
                    cleanup: ExternalMutationFinalization::Complete,
                    ..
                },
            )) => R::Disposed,
            _ => R::Retained,
        }
    }
    fn poll_result(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ConnectorMetadataMaintenanceOutcome, ConnectorError>> {
        let mut future = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(error) = &state.failure {
                return Poll::Ready(Err(error.clone()));
            }
            if state.phase == Phase::Closed {
                return Poll::Ready(
                    state
                        .result
                        .as_ref()
                        .expect("closed execution has a result")
                        .clone(),
                );
            }
            // The public execution handle is move-only and has one poller.
            // A reentrant poll cannot steal or replace the original future.
            if state.phase == Phase::Polling || state.phase == Phase::TerminalSaved {
                return Poll::Pending;
            }
            state.phase = Phase::Polling;
            state
                .future
                .take()
                .expect("active execution retains its physical future")
        };
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Err(panic) => Poll::Ready(Err(self.poison(panic, Some(future)))),
            Ok(Poll::Pending) => {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.future = Some(future);
                state.phase = Phase::Waiting;
                Poll::Pending
            }
            Ok(Poll::Ready(result)) => {
                // Preserve the original terminal outcome before foreign future
                // destruction. Neither a cancelled waiter nor Drop panic can
                // erase the original publication facts or permit redispatch.
                {
                    let mut state = self
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.result = Some(result.clone());
                    state.phase = Phase::TerminalSaved;
                }
                let retired_terminal = if let Ok(outcome) = &result {
                    self.terminal
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(
                            self.operation,
                            TerminalRecord {
                                plan_digest: self.plan_digest,
                                outcome: outcome.clone(),
                            },
                        )
                } else {
                    None
                };
                {
                    let mut state = self
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.retiring_terminal = retired_terminal;
                }
                // Destructors, polling and notifications never run under the
                // active/terminal/row mutex. A failed Drop does not retire row.
                if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(future))) {
                    return Poll::Ready(Err(self.poison(panic, None)));
                }
                let retired_terminal = {
                    self.state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .retiring_terminal
                        .take()
                };
                if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(retired_terminal))) {
                    return Poll::Ready(Err(self.poison(panic, None)));
                }
                {
                    let mut state = self
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.phase = Phase::Closed;
                }
                let retired_row = self.active.upgrade().and_then(|active| {
                    active
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&self.operation)
                });
                drop(retired_row);
                Poll::Ready(result)
            }
        }
    }
    fn retained_result(
        &self,
    ) -> Option<Result<ConnectorMetadataMaintenanceOutcome, ConnectorError>> {
        let (result, operation, journal, failed) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                state.result.clone(),
                state.operation.clone(),
                state.journal.clone(),
                state.failure.is_some(),
            )
        };
        if result.is_some() || !failed {
            return result;
        }
        // This reads the ORIGINAL journal, outside the row lock. It performs no
        // I/O and never turns failed frame exit into permission to dispatch.
        journal.map(|journal| {
            Ok(public_outcome(match operation {
                Some(operation) => journal.bridge_failure(
                    &operation,
                    "Maintenance execution frame did not prove actual exit".into(),
                ),
                None => journal.catalog_entry_outcome(
                    journal
                        .current_entry()
                        .expect("replay retains its original entry"),
                ),
            }))
        })
    }
}

#[cfg(test)]
#[path = "execution/tests.rs"]
mod tests;

/// Exact first-party fixed carrier payloads. The F pointee is quoted by the
/// same parent factory; variable journal/plan increments are separate.
pub(super) fn fixed_backing_bytes() -> usize {
    std::mem::size_of::<MaintenanceExecution>()
        + std::mem::size_of::<MaintenancePanic>()
        + 2 * std::mem::size_of::<[usize; 2]>()
        + std::mem::size_of::<Mutex<Option<Box<dyn Any + Send>>>>()
        + if cfg!(target_os = "macos") {
            2 * 128
        } else {
            0
        }
}
