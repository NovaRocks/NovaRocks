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

//! Core-authored cuts published atomically through the original host records.

pub(crate) mod protocol_cursor;

use novarocks_coordination_contract::{AttemptComputationCut, SelectedTaskTermination};
use novarocks_execution_contract::{OperationOutcome, TaskIdentity, TaskStatus};
use novarocks_result_contract::PerKind;
use std::{collections::BTreeSet, sync::Arc};

/// Frozen memberships from the original plan, separate from changing Task facts.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct ProtocolMembership {
    pub writers: BTreeSet<TaskIdentity>,
    pub summary_sources: PerKind<BTreeSet<TaskIdentity>>,
}

#[derive(Clone, Debug)]
pub struct ProtocolTaskChange {
    pub identity: TaskIdentity,
    pub status: Option<TaskStatus>,
    pub first_writer_witness: Option<TaskStatus>,
}

/// The host validates every identity, then updates the original publisher once
/// under its original lock. Owner and publication revisions remain distinct.
/// The original activation/root/PlanSeal binding stays in that canonical record.
#[derive(Clone, Debug)]
pub struct ProtocolPublication {
    pub owner_revision: u64,
    /// The first publication binds the same writer and per-kind summary obligations.
    pub initial_membership: Option<Arc<ProtocolMembership>>,
    /// Move the charged assembly without a final shrink/copy on the poller.
    pub changes: Vec<ProtocolTaskChange>,
    pub selected_failure: Option<SelectedTaskTermination>,
    pub root_success_sealed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticClosure {
    FrontierSettled,
    ObservationDeadline,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticMissing {
    Unavailable(OperationOutcome),
    ReadFailed,
    TerminalMismatch,
    ReadPending,
    TaskNotTerminal,
    NotRead,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticTaskObservation {
    Available(crate::input::FinalTaskInfoRef),
    Missing(DiagnosticMissing),
}

/// Exact split outcome selected from the original Source's immutable record.
/// Absent is positive empty data; SourceGone is missing data. Neither success
/// nor failure proves that the original job/captures have actually exited.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SplitDiagnosticObservation {
    Absent,
    Pending,
    Available(crate::start::PayloadRef),
    Failed,
    SourceGone,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterDiagnostic {
    pub contributions: Vec<(novarocks_types::BackendProcessId, crate::start::PayloadRef)>,
    pub complete: bool,
}

/// The complete Source selection from the actual diagnostic boundary. Hosts
/// resolve these same original refs; they must never read later live Sources.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticSources {
    pub selected_at: novarocks_coordination_contract::MonotonicInstant,
    pub runtime_filters: RuntimeFilterDiagnostic,
    pub split: SplitDiagnosticObservation,
}

/// Freeze at the original diagnostic boundary, independently of business ready
/// and actual job exit. Materialization may finish later but uses only this
/// immutable selection. Vec keeps final transfer constant-time after charged
/// per-Task assembly; converting a large accumulated Vec to Box may copy it.
#[derive(Clone, Debug)]
pub struct DiagnosticPublication {
    pub ready: AttemptComputationCut,
    pub closure: DiagnosticClosure,
    pub sources: DiagnosticSources,
    pub tasks: Vec<(TaskIdentity, DiagnosticTaskObservation)>,
}
