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

//! Actual unused Root RPC originals, before any command capture. Requests1
//! remains in each original payment through report/header retirement and
//! refund. The FIFO contains only addresses of these same retained holders.

use super::{ControlRequestAdmissionRefusal, ControlRequestError, ProcessControlOwner};
use crate::control_runtime::control_request::ControlRequestPartial;
use novarocks_control_runtime::{lease::WorkRetirementObserver, transaction::RuntimeLedger};
use novarocks_control_runtime_core::identity::WorkId;
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Copy)]
enum Phase {
    Input,
    RevokeReport,
    WorkBody,
    WorkCaptures,
    ReportBody,
    ReportCaptures,
    RetireReport,
    ReapWork,
    Payment,
    Escrow,
    Done,
    Unresolved,
}
struct OriginalPartial {
    original: ControlRequestPartial,
    retirement: WorkRetirementObserver,
    phase: Phase,
}
impl OriginalPartial {
    fn new(original: ControlRequestPartial) -> Self {
        Self {
            retirement: original.work.retirement_observer(),
            original,
            phase: Phase::Input,
        }
    }
    /// Every phase is a fact on these actual never-captured originals. A
    /// refused revoke cannot turn a filled/started inventory into unused.
    fn advance(&mut self, ledger: &RuntimeLedger) -> Result<PartialStep, ControlRequestError> {
        let mut step = PartialStep::default();
        let result = (|| {
            loop {
                self.phase = match self.phase {
                    Phase::Input => {
                        if let Some(permit) = self.original.permit.take() {
                            permit
                                .revoke_unused()
                                .map_err(ControlRequestError::Driver)?;
                        }
                        Phase::RevokeReport
                    }
                    Phase::RevokeReport => {
                        if let Some(report) = &self.original.report {
                            report
                                .revoke_unused()
                                .map_err(ControlRequestError::Runtime)?;
                        }
                        Phase::WorkBody
                    }
                    Phase::WorkBody => {
                        self.original
                            .work
                            .body_exited()
                            .map_err(ControlRequestError::Runtime)?;
                        Phase::WorkCaptures
                    }
                    Phase::WorkCaptures => {
                        self.original
                            .work
                            .captures_exited()
                            .map_err(ControlRequestError::Runtime)?;
                        Phase::ReportBody
                    }
                    Phase::ReportBody => {
                        if let Some(report) = &self.original.report {
                            report.body_exited().map_err(ControlRequestError::Runtime)?;
                        }
                        Phase::ReportCaptures
                    }
                    Phase::ReportCaptures => {
                        if let Some(report) = &self.original.report {
                            report
                                .captures_exited()
                                .map_err(ControlRequestError::Runtime)?;
                        }
                        Phase::RetireReport
                    }
                    Phase::RetireReport => {
                        if let Some(report) = &self.original.report
                            && !report.try_retire().map_err(ControlRequestError::Runtime)?
                        {
                            return Ok(step);
                        }
                        // Never query a ReportSlot after its actual removal.
                        self.original.report = None;
                        Phase::ReapWork
                    }
                    Phase::ReapWork => {
                        if !self.retirement.is_retired() {
                            step.progressed |= ledger
                                .reap_scope_ready(self.original.work.scope().id(), 1)
                                .map_err(ControlRequestError::Runtime)?
                                != 0;
                            if !self.retirement.is_retired() {
                                return Ok(step);
                            }
                        }
                        Phase::Payment
                    }
                    Phase::Payment => {
                        if !self.original.capacity.is_released() {
                            let remaining = self
                                .original
                                .capacity
                                .resources()
                                .map_err(ControlRequestError::Runtime)?;
                            self.original
                                .capacity
                                .release_quota(&remaining.quota)
                                .and_then(|()| self.original.capacity.finish())
                                .map_err(ControlRequestError::Runtime)?;
                        }
                        Phase::Escrow
                    }
                    Phase::Escrow => {
                        if let Some(escrow) = &mut self.original.route_escrow
                            && !escrow.is_released()
                        {
                            let remaining =
                                escrow.resources().map_err(ControlRequestError::Runtime)?;
                            escrow
                                .release_quota(&remaining.quota)
                                .and_then(|()| escrow.finish())
                                .map_err(ControlRequestError::Runtime)?;
                        }
                        Phase::Done
                    }
                    Phase::Done => {
                        step.retired = true;
                        return Ok(step);
                    }
                    Phase::Unresolved => return Err(ControlRequestError::SubmissionUnresolved),
                };
                step.progressed = true;
            }
        })();
        if result.is_err() {
            // The runtime still holds the exact failed input/report record.
            // Retain its original Work/payments; no later retry signs exit.
            self.phase = Phase::Unresolved;
        }
        result
    }
}
#[derive(Default)]
struct PartialStep {
    progressed: bool,
    retired: bool,
}
#[derive(Default)]
pub(super) struct RootRequestPartials {
    owners: BTreeMap<WorkId, OriginalPartial>,
    ready: VecDeque<WorkId>,
}
impl RootRequestPartials {
    pub(super) fn is_empty(&self) -> bool {
        self.owners.is_empty()
    }
    fn insert(&mut self, original: ControlRequestPartial) {
        let work = original.work.id();
        if self.owners.contains_key(&work) {
            // Never replace/drop an actual original on a duplicate identity.
            std::mem::forget(original);
            std::process::abort();
        }
        self.owners.insert(work, OriginalPartial::new(original));
        self.ready.push_back(work);
    }
    fn advance(
        &mut self,
        ledger: &RuntimeLedger,
        budget: usize,
    ) -> Result<RootRequestPartialProgress, ControlRequestError> {
        let mut progress = RootRequestPartialProgress::default();
        // Visit each actual original at most once per turn. Requeue is one
        // bounded address, not a tombstone or another admission obligation.
        let visits = budget.min(self.ready.len());
        for _ in 0..visits {
            let work = self.ready.pop_front().expect("original partial address");
            let step = self
                .owners
                .get_mut(&work)
                .expect("original partial holder")
                .advance(ledger);
            match step {
                Ok(step) => {
                    progress.visited += 1;
                    if step.retired {
                        self.owners.remove(&work);
                        progress.retired += 1;
                    } else {
                        self.ready.push_back(work);
                    }
                    progress.runnable |= step.progressed;
                }
                Err(error) => {
                    self.ready.push_front(work);
                    return Err(error);
                }
            }
        }
        Ok(progress)
    }
}
#[derive(Debug, Default)]
pub(crate) struct RootRequestPartialProgress {
    pub visited: usize,
    pub retired: usize,
    /// Only a real phase edge or actual reaper progress requests another turn.
    pub runnable: bool,
}
impl ProcessControlOwner {
    /// The public refusal now carries metadata only. This Root keeps every
    /// paid pre-capture original, including failed admission after a split.
    pub(super) fn retain_request_partial(
        &mut self,
        mut refusal: ControlRequestAdmissionRefusal,
    ) -> ControlRequestAdmissionRefusal {
        if let Some(original) = refusal.partial.take() {
            if !original.work.scope().same_scope(&self.resources.scope) {
                std::mem::forget((original, refusal));
                std::process::abort();
            }
            self.resources.request_partials.insert(original);
            // A same-registration notify cannot ordinarily fail while this
            // actual Root still owns the partial's live Scope/Work.
            if self
                .runtime()
                .notify_host_actor(self.registration())
                .is_err()
            {
                std::process::abort();
            }
        }
        refusal
    }
    pub(crate) fn advance_request_partials(
        &mut self,
        budget: usize,
    ) -> Result<RootRequestPartialProgress, ControlRequestError> {
        let ledger = self.ledger().clone();
        self.resources.request_partials.advance(&ledger, budget)
    }
}
