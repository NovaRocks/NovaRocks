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

//! Joint resource admission under the original workload authority.
//!
//! Limits and their projections are declarations, not new grants. A resource
//! quantity has one global charge; class, lane and dependency coordinates are
//! constraints on that same quantity. Physical holders retain their original
//! capability until actual exit, independently of a logical terminal result.

mod driver;
pub(crate) mod ledger;

pub use driver::{JointProgress, JointProgressDriver};
pub use ledger::{
    AsyncActionAdmission, AsyncActionGrant, AsyncActionSource, BlockingActionAdmission,
    BlockingActionSource, ControlBindingProposal, InternalComputationAlias,
    InternalComputationGrant, InternalCpuActionAdmission, InternalCpuActionSource,
    InternalCpuFactoryBinding, InternalManifestBinding, InternalResultClosedFactoryDescriptor,
    JointAdjustmentReceipt, JointAdmission, JointAdmissionFailure, JointAdmissionHandle,
    JointClaim, JointClaimView, JointGrantedBatch, JointGrantedClaims, JointLocalGrant,
    JointLocalOffer, JointProposal, JointProposalSlotSignal, JointProposalSlotWait, JointTryBegin,
    NativeAsyncActionAdmission, NativeAsyncActionAuthority, NativeAsyncActionGrant,
    NativeAsyncActionQuote, NativeAsyncActionSource, NativeDispatchFrozen, NativeDispatchSource,
    NativeQueuedInputAdmission, NativeQueuedInputGrant, NativeQueuedInputQuote,
    NativeQueuedInputReady, NativeReplyGrant, RetainedPayloadAlias, RetainedPayloadGrant,
    RootDomainBinding, RootDomainInitialAdmission, RootDomainInitialProposal,
    RootDomainInitialSource, RootPayloadAdmission, RootPayloadInitialAdmission,
    RootPayloadInitialSource, RootPayloadProposal, RootPayloadReady, RootPayloadSource,
    RootStageAdmission, RootStageProposal, RootStageReady, RootStageSource, SourceCohort,
    UnreceivedProductionAdmission, UnreceivedProductionGrant, UnreceivedProductionOwner,
    UnreceivedProductionQuote, UnreceivedProductionReady,
};
#[cfg(test)]
pub use ledger::{JointClaimChange, JointDependency};

use crate::{WorkClass, WorkError};
use std::num::NonZeroUsize;

/// Independently constrained resource coordinates. Byte values describe owned
/// backing or an explicit reserved envelope, never allocator metadata or RSS.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum JointResource {
    ScopeRecords,
    ClaimRecords,
    ReplyRecords,
    QueueRecords,
    MetadataBytes,
    PayloadBytes,
    UnreceivedProductions,
    Publications,
    Residuals,
    Computations,
    AsyncIo,
    BlockingJobs,
    InternalCpuJobs,
    PhysicalQueueRecords,
    PhysicalQueueBytes,
}
const RESOURCE_COUNT: usize = 15;

// Factory provenance remains distinct from ordinary responsibility facts.
// This tag never removes a resource coordinate or changes a physical offer.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
enum SourceKind {
    #[default]
    Plain,
    InternalCpu,
}
impl SourceKind {
    fn is_empty(self) -> bool {
        self == Self::Plain
    }
}

/// A request or a checked profile. Constructing a vector grants no capacity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JointResources([u64; RESOURCE_COUNT]);
impl JointResources {
    pub const ZERO: Self = Self([0; RESOURCE_COUNT]);

    pub const fn with(mut self, resource: JointResource, amount: u64) -> Self {
        self.0[resource as usize] = amount;
        self
    }

    pub const fn get(&self, resource: JointResource) -> u64 {
        self.0[resource as usize]
    }

    fn checked_add(self, other: Self) -> Result<Self, WorkError> {
        let mut result = Self::ZERO;
        for index in 0..RESOURCE_COUNT {
            result.0[index] = self.0[index]
                .checked_add(other.0[index])
                .ok_or(WorkError::ArithmeticOverflow)?;
        }
        Ok(result)
    }

    fn checked_sub(self, other: Self) -> Result<Self, WorkError> {
        let mut result = Self::ZERO;
        for index in 0..RESOURCE_COUNT {
            result.0[index] = self.0[index]
                .checked_sub(other.0[index])
                .ok_or(WorkError::Conflict)?;
        }
        Ok(result)
    }
}

/// Custody remains separately bounded while foreground and settlement actions
/// obtain their stage resources. A lane never changes an originating WorkClass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum JointLane {
    Custody,
    Foreground,
    Settlement,
}

fn class_index(class: WorkClass) -> usize {
    match class {
        WorkClass::Query => 0,
        WorkClass::MaterializedView => 1,
        WorkClass::Statistics => 2,
        WorkClass::TableMaintenance => 3,
        WorkClass::Management => 4,
    }
}

/// Scheduling of the unique responsibility transaction service. This contains
/// no resource quota, dependency fanout, class/lane partition or proof budget.
#[derive(Clone, Copy, Debug)]
pub struct ResponsibilityConfig {
    pub work_per_turn: NonZeroUsize,
}

#[cfg(test)]
mod tests;
