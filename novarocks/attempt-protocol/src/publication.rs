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

use novarocks_coordination_contract::{AttemptComputationCut, SelectedTaskTermination};
use novarocks_execution_contract::{FinalTaskInfo, OperationOutcome, TaskIdentity, TaskStatus};

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
    pub changes: Box<[ProtocolTaskChange]>,
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

#[derive(Clone, Debug)]
pub enum DiagnosticTaskObservation {
    Available(Box<FinalTaskInfo>),
    Missing(DiagnosticMissing),
}

/// Freeze once at this computation cut. The executor samples the original RF
/// and split-profile sources at the same publication boundary. A pending RPC
/// remains owned and polled after this snapshot, without changing it.
#[derive(Clone, Debug)]
pub struct DiagnosticPublication {
    pub ready: AttemptComputationCut,
    pub closure: DiagnosticClosure,
    pub tasks: Box<[(TaskIdentity, DiagnosticTaskObservation)]>,
}
