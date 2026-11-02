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

//! Neutral facts retained after the original execution custody retires.
use crate::lease::MonotonicInstant;

/// Neutral final facts for one Task whose original execution custody exited.
/// It owns no live input, reservation, queue, source object or callback.
#[derive(Debug)]
pub struct DecidedTask {
    pub(crate) custody_payload: crate::creation::PayloadId,
    pub(crate) custody_epoch: novarocks_control_runtime_core::identity::Generation,
    pub(crate) status: novarocks_execution_contract::TaskStatus,
    pub(crate) output: novarocks_execution_contract::TaskOutputFacts,
    pub(crate) initial_domains:
        std::sync::Arc<Vec<novarocks_execution_contract::TaskDomainReceipt>>,
    pub(crate) final_info: Option<std::sync::Arc<novarocks_execution_contract::FinalTaskInfo>>,
    pub(crate) diagnostics: crate::proposal_summary::Diagnostics,
    pub(crate) convergence: crate::convergence::TaskConvergenceSnapshot,
    pub(crate) source: Option<crate::proposal_summary::SourceId>,
}
impl DecidedTask {
    pub(crate) fn compose_status(
        &self,
    ) -> Result<novarocks_execution_contract::TaskStatus, crate::context::CoreError> {
        crate::task::compose_status(
            self.status.identity(),
            self.status.version().get(),
            self.status.state(),
            self.status.termination().cloned(),
            self.output,
            self.status.installed(),
            self.status.resources(),
            self.status.writer(),
            self.status.dynamic_filters(),
        )
    }
    pub fn status(&self) -> &novarocks_execution_contract::TaskStatus {
        &self.status
    }
    pub fn final_info(
        &self,
    ) -> Option<&std::sync::Arc<novarocks_execution_contract::FinalTaskInfo>> {
        self.final_info.as_ref()
    }
    pub fn diagnostics(&self) -> &crate::proposal_summary::Diagnostics {
        &self.diagnostics
    }
    pub const fn convergence(&self) -> crate::convergence::TaskConvergenceSnapshot {
        self.convergence
    }
    /// A neutral provenance identity, never access to a retained rich source.
    pub const fn source(&self) -> Option<crate::proposal_summary::SourceId> {
        self.source
    }
}

/// Frozen inline cold-Core inventory. Indirect Arc backing, pending RPC
/// collections, tree nodes and allocator overhead are explicitly unmeasured.
/// This observation grants neither Work nor a retention/physical-exit proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetainedContextInventory {
    pub decided_task_records: usize,
    pub metadata_bytes_estimate: Option<usize>,
}

/// The immutable original execution-retirement fact fixes the whole request
/// horizon. Sharing this plain value never admits Work or starts another TTL.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetentionOrigin {
    retired_at: MonotonicInstant,
    expires_at: MonotonicInstant,
}
impl RetentionOrigin {
    pub fn new(retired_at: MonotonicInstant, horizon: crate::lease::RequestHorizon) -> Self {
        Self {
            retired_at,
            expires_at: horizon.retention_deadline(retired_at),
        }
    }
    pub const fn retired_at(&self) -> MonotonicInstant {
        self.retired_at
    }
    pub const fn expires_at(&self) -> MonotonicInstant {
        self.expires_at
    }
    pub fn has_expired(&self, now: MonotonicInstant) -> bool {
        now.has_reached(self.expires_at)
    }
}

/// Provenance of the original Context retirement, independent of its time.
/// These plain identities grant no Work, failure, input or publication right.
/// Worker constructs OwnerFailure only from the actual ActorFailureProof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextRetirementIdentity {
    Protocol {
        close_epoch: novarocks_control_runtime_core::identity::Generation,
    },
    OwnerFailure {
        actor: novarocks_control_runtime_core::identity::ActorId,
        initial_epoch: novarocks_control_runtime_core::identity::Generation,
        failed_epoch: novarocks_control_runtime_core::identity::Generation,
    },
}
impl ContextRetirementIdentity {
    /// This checks neutral chronology only; it does not authenticate a runtime.
    pub fn is_valid(self) -> bool {
        match self {
            Self::Protocol { .. } => true,
            Self::OwnerFailure {
                initial_epoch,
                failed_epoch,
                ..
            } => failed_epoch.get() > initial_epoch.get(),
        }
    }
}
