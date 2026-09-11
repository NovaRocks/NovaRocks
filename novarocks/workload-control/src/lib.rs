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

//! Process-local work admission, responsibility and allocation accounting.
//!
//! A scope identifies responsibility; it is neither an execution permit nor a
//! memory reservation. Product owners retain their state machines and locks.
//! Resource guarantees cover allocations represented by this authority's
//! reservations and charges, not process RSS or memory on another process.
//!
//! Production entry points must validate a scope and acquire the applicable
//! stage. A scope cannot be manufactured by callers:
//! ```compile_fail
//! use novarocks_workload_control::WorkScope;
//! let scope = WorkScope::default();
//! ```
//! ```compile_fail
//! use novarocks_workload_control::WorkScope;
//! let scope = WorkScope {};
//! ```

mod admission;
mod cancellation;
mod observation;
mod queue;
mod resource;
mod scope;

pub use admission::{Stage, StageAdmission, StagePermit, StageRequest};
pub use cancellation::{CancellationReason, CancellationView};
pub use observation::{
    ControlIntent, ControlIntents, ControlPermit, Obligation, ObligationKey, ObligationKind,
    ObligationSnapshot, OwnerState, ScopeSnapshot, UsageObservation, WorkloadSnapshot,
};
pub use resource::{
    AllocationCharge, LocalResourceAuthority, Reservation, ResourceClass, ResourceConfig,
    ResourceSnapshot, ResultCredit, ResultCreditReservationError, ResultCreditSnapshot,
    ResultCreditStage,
};
pub use scope::{
    BusinessPermit, RootWork, ServingState, WorkCancellationRequester, WorkClass, WorkId,
    WorkOwner, WorkRequest, WorkScope, WorkloadConfig, WorkloadControl,
};

/// Admission failures never imply cancellation, physical stop, or release.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkError {
    InvalidConfig(&'static str),
    Closed,
    NotReady,
    Released,
    ForeignAuthority,
    Cancelled(CancellationReason),
    Capacity(&'static str),
    /// Only this capacity wait expired; the logical work remains active.
    CapacityWaitTimeout,
    AlreadyAdmitted,
    AlreadyWaitingForResource(ResourceClass),
    AlreadyWaitingForResultFetch,
    AlreadyWaitingForResultDecode,
    Conflict,
    OwnerStillPresent,
    ArithmeticOverflow,
    InvalidResultCreditTransition {
        from: ResultCreditStage,
        requested: ResultCreditStage,
    },
}

impl std::fmt::Display for WorkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfig(field) => write!(f, "Invalid workload configuration: {field}"),
            Self::Closed => f.write_str("New work admission is closed"),
            Self::NotReady => f.write_str("Work admission is not ready"),
            Self::Released => f.write_str("Work responsibility is already released"),
            Self::ForeignAuthority => f.write_str("Scope belongs to a different local authority"),
            Self::Cancelled(reason) => write!(f, "Work is cancelled: {reason:?}"),
            Self::Capacity(resource) => write!(f, "Work capacity exhausted: {resource}"),
            Self::CapacityWaitTimeout => f.write_str("Capacity wait deadline exceeded"),
            Self::AlreadyAdmitted => f.write_str("Work already holds or awaits this stage"),
            Self::AlreadyWaitingForResource(class) => {
                write!(f, "Work already has a {class:?} resource capacity wait")
            }
            Self::AlreadyWaitingForResultFetch => {
                f.write_str("Work already has a result fetch capacity wait")
            }
            Self::AlreadyWaitingForResultDecode => {
                f.write_str("Work already has a result decode capacity wait")
            }
            Self::Conflict => f.write_str("Work identity has conflicting facts"),
            Self::OwnerStillPresent => f.write_str("Work still has an active owner"),
            Self::ArithmeticOverflow => f.write_str("Work accounting overflow"),
            Self::InvalidResultCreditTransition { from, requested } => {
                write!(
                    f,
                    "Invalid result-credit transition from {from:?} to {requested:?}"
                )
            }
        }
    }
}

impl std::error::Error for WorkError {}
