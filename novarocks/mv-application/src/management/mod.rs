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

//! Pure MV management admission and readmission policy.
//!
//! The module neither performs provider I/O nor persists an attempt ledger.
//! It consumes exact connector observations and produces bounded, target-local
//! admission decisions for the application owner.

mod admission;
mod audit;
mod continuation;
mod effects;
mod observation;
mod ownership;
mod readmission;
mod startup_isolation;

pub use admission::{
    AutomaticMaintenanceEffect, ManagementAdmissionError, ManagementDependencySet,
    ManagementEntrance, ManagementEntranceLease, ManagementEntranceTicket, ManagementRequest,
    MvCurrentManagementAdmission, MvDropManagementObservation, MvManagementPhase,
    SlotChangeConvergence,
};
pub use audit::{
    MAX_MANAGEMENT_AUDIT_FIELD_BYTES, ManagementAuditAction, ManagementAuditOutcome,
    ManagementAuditRecord, ManagementAuditSink,
};
pub use continuation::{
    ManagementContinuationService, MvManagementStatus, MvResumeDeclaration,
    MvUnsettledEffectStatus, RemoteEffectPolicy, StartupContinuation,
};
pub use effects::{
    CreateIntentResponsibility, EffectDisposition, EffectIdentity, EffectPath,
    EffectResponsibility, EffectScope, EffectTerminalFact, UnsettledEffect,
};
pub use observation::{
    FreshCreateIntentObservation, FreshManagementObservation, ManagementContinuation,
    ManagementObservationError, ManagementObservationFailure, ManagementObservationPhase,
    ManagementObservationRequestId, ManagementObservationState, PendingCreateIntentObservation,
    PendingManagementObservation, RegistrationRequirement,
};
pub use ownership::{
    CreateIntent, DeploymentOwner, ManagedMvTarget, ManagementOwnershipError, ProcessIncarnation,
};
pub use startup_isolation::{
    StartupIsolationDeclaration, StartupIsolationError, StartupIsolationEvidence,
    StartupIsolationScope, StartupNonce,
};

pub use readmission::{
    ActualCompletionEvidence, GarbageCollectionSafetyPolicy, IsolationEvidence, ManagementClock,
    ManagementTimestamp, ManualReadmissionDeclaration, ReadmissionChallenge, ReadmissionError,
    ReadmissionEvaluator, ReadmissionMode, ReadmissionPermit, RemoteEffectGuaranteeBasis,
    RemoteEffectLifetimeGuarantee, SystemManagementClock, VirtualManagementClock,
};

#[cfg(test)]
mod tests;
