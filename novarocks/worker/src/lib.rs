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

//! Worker-owned policy for native task execution.
//!
//! The execution contract supplies immutable cross-process facts. This crate
//! decides how one worker admits those facts, advances local lifecycle state,
//! fences exchange input, and accounts for its process-local time. It owns no
//! transport model and has no dependency on a query application or frontend.

mod admission;
mod domain;
mod ingress;
mod lease;
mod lifecycle;
mod operation;

pub use admission::{
    AdmissionTicketAcquisitionRejection, AdmissionTicketAuthority, AdmissionTicketConfig,
    AdmissionTicketGrant, AdmissionTicketProgression, AdmissionTicketRedemption,
    AdmissionTicketRedemptionRejection, AdmissionTicketState, MAX_ADMISSION_RESERVATIONS,
    MAX_ADMISSION_TICKET_VALID_FOR,
};
pub use domain::{
    DomainPolicyRejection, InitialDomainKey, QueryContextDomains, TaskDomains,
    commit_task_domain_updates, initial_domain_keys, plan_task_domain_updates,
    task_domain_reaches_execution, validate_task_domain_membership,
};
pub use ingress::{IngressRejection, authorize_inbound_frame};
pub use lease::{InstalledLease, LeaseBounds, LeaseProgression, MonotonicInstant, RequestHorizon};
pub use lifecycle::{
    ContextOperationKind, ContextTransition, LatchOutcome, OperationAdmission, QueryContextEvent,
    RootDrainAction, TaskTransition, TerminationLatch, classify_context_transition,
    classify_operation_admission, classify_root_drain, classify_task_transition,
};
pub use operation::OperationWaitCaps;
