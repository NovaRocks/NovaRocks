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

//! A non-cloneable carrier for one actually bound Work/capacity/report triple.

use crate::{
    completion::ReportSlot,
    lease::{CapacityBundle, WorkLease},
};

/// Rejection of mismatched original ownership, not an admission or exit fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReportedWorkBindingError {
    DifferentLedger,
    WrongScope,
    UnknownScope,
    UnknownWork,
    UnboundWork,
    InactiveCapacity,
    UnknownCapacity,
    UnboundCapacity,
    CapacityWorkMismatch,
    UnknownReport,
    ReportProducerMismatch,
}
impl std::fmt::Display for ReportedWorkBindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "reported Work binding failure: {self:?}")
    }
}
impl std::error::Error for ReportedWorkBindingError {}
/// Every refusal returns all three original owners without changing them.
#[must_use = "A refused binding still owns its Work, capacity and report"]
pub struct ReportedWorkRefusal {
    pub error: ReportedWorkBindingError,
    pub work: WorkLease,
    pub capacity: CapacityBundle,
    pub report: ReportSlot,
}
impl std::fmt::Debug for ReportedWorkRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReportedWorkRefusal")
            .field("error", &self.error)
            .field("scope", &self.work.scope().id())
            .field("work", &self.work.id())
            .field("reservation", &self.capacity.id())
            .field("report", &self.report.id())
            .finish()
    }
}
/// Only a validated carrier can enter either exit or continuation custody.
/// Fields are private; extracting them requires another validation before a
/// custodian can accept them again. This object signs no new responsibility.
#[must_use = "The original reported Work must reach its owner or be returned intact"]
pub struct ReportedWork {
    work: WorkLease,
    capacity: CapacityBundle,
    report: ReportSlot,
}
impl std::fmt::Debug for ReportedWork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReportedWork")
            .field("scope", &self.work.scope().id())
            .field("work", &self.work.id())
            .field("reservation", &self.capacity.id())
            .field("report", &self.report.id())
            .finish()
    }
}
impl ReportedWork {
    /// One original ledger transaction checks identities and actual bindings.
    /// Current scope generation/health or Work execution phase do not cancel
    /// previously admitted ownership. No self pin, authority or credit is
    /// created on refusal, and no foreign callback or destructor runs here.
    pub fn try_new(
        work: WorkLease,
        capacity: CapacityBundle,
        report: ReportSlot,
    ) -> Result<Self, ReportedWorkRefusal> {
        if let Err(error) = capacity.validate_reported_work_binding(&work, &report) {
            return Err(ReportedWorkRefusal {
                error,
                work,
                capacity,
                report,
            });
        }
        Ok(Self {
            work,
            capacity,
            report,
        })
    }
    pub fn work(&self) -> &WorkLease {
        &self.work
    }
    pub fn report(&self) -> &ReportSlot {
        &self.report
    }
    pub fn into_parts(self) -> (WorkLease, CapacityBundle, ReportSlot) {
        (self.work, self.capacity, self.report)
    }
}
