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

//! Composition of actual process quotas and per-object storage profiles.
//! Context, ticket and retained metadata follow their original objects; they
//! do not contribute a process count threshold or prepaid storage dimension.

use super::{
    context_host::{CONTEXT_BOOTSTRAP_STORAGE, CONTEXT_REQUEST_STORAGE},
    control_request::{CONTROL_REQUEST_STORAGE, ESTABLISH_REQUEST_STORAGE},
};
use crate::{
    control_admission::{AdmissionStorage, Dimension},
    control_runtime_policy::{ControlRuntimePolicy, PolicyError, StorageNeed},
};
use novarocks_control_runtime_core::capacity::ResourceVector;
use std::num::NonZeroUsize;

/// T remains the wire bound on cumulative Task identities. A/P/bytes and the
/// original ingress R describe actual resources; Context records have no cap.
#[derive(Clone, Copy, Debug)]
pub struct NodeGeometryInputs {
    pub tasks_per_context: NonZeroUsize,
    pub active_tasks: NonZeroUsize,
    pub preparing_tasks: NonZeroUsize,
    pub preparing_bytes: NonZeroUsize,
    pub policy: ControlRuntimePolicy,
    /// Supplied by the actual Native result geometry.
    pub native_root_data_holders: NonZeroUsize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeGeometryError {
    Overflow,
    Policy(PolicyError),
    PolicyMismatch,
    RootHolderMismatch,
    InvalidBootstrapManifest,
}
impl From<PolicyError> for NodeGeometryError {
    fn from(error: PolicyError) -> Self {
        Self::Policy(error)
    }
}
impl std::fmt::Display for NodeGeometryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overflow => f.write_str("Node geometry overflowed"),
            Self::Policy(error) => write!(f, "invalid Node storage policy: {error}"),
            Self::PolicyMismatch => {
                f.write_str("Node policy differs from its actual ingress manifest")
            }
            Self::RootHolderMismatch => {
                f.write_str("Node Root data positions differ from Native geometry")
            }
            Self::InvalidBootstrapManifest => {
                f.write_str("Node bootstrap differs from its original owner manifest")
            }
        }
    }
}
impl std::error::Error for NodeGeometryError {}

#[derive(Clone, Debug)]
pub struct NodeGeometry {
    pub(crate) limits: ResourceVector,
    pub(crate) admission: AdmissionStorage,
    pub(crate) request_per_position: StorageNeed,
    pub(crate) request_positions: NonZeroUsize,
    pub(crate) inputs: NodeGeometryInputs,
}

fn units(value: usize) -> Result<u64, NodeGeometryError> {
    u64::try_from(value).map_err(|_| NodeGeometryError::Overflow)
}
fn envelope(a: StorageNeed, b: StorageNeed) -> StorageNeed {
    StorageNeed {
        inputs: a.inputs.max(b.inputs),
        batches: a.batches.max(b.batches),
        effects: a.effects.max(b.effects),
        cuts: a.cuts.max(b.cuts),
        replies: a.replies.max(b.replies),
        completions: a.completions.max(b.completions),
        captures: a.captures.max(b.captures),
        work: a.work.max(b.work),
        requests: a.requests.max(b.requests),
    }
}

impl NodeGeometry {
    pub fn request_positions(&self) -> NonZeroUsize {
        self.request_positions
    }

    pub fn try_new(inputs: NodeGeometryInputs) -> Result<Self, NodeGeometryError> {
        let policy = inputs.policy;
        let actual = ControlRuntimePolicy::try_new_with_request_sources(
            policy.ingress,
            policy.root_request_positions,
            policy.observation_request_positions,
        )?;
        if actual != policy {
            return Err(NodeGeometryError::PolicyMismatch);
        }
        if policy.ingress.root_read_holders != inputs.native_root_data_holders.get() {
            return Err(NodeGeometryError::RootHolderMismatch);
        }
        let request_positions =
            NonZeroUsize::new(policy.request_roles).ok_or(NodeGeometryError::PolicyMismatch)?;
        // These are the largest mutually exclusive branches of one actual R,
        // not a process-wide prepaid metadata pool.
        let request_per_position = [
            CONTROL_REQUEST_STORAGE,
            ESTABLISH_REQUEST_STORAGE,
            CONTEXT_REQUEST_STORAGE,
            StorageNeed::domain_request()?,
        ]
        .into_iter()
        .reduce(envelope)
        .expect("nonempty actual request manifest");
        let limits = ResourceVector::new([
            (
                Dimension::ActiveTasks.id(),
                units(inputs.active_tasks.get())?,
            ),
            (
                Dimension::PreparingTasks.id(),
                units(inputs.preparing_tasks.get())?,
            ),
            (
                Dimension::PreparingBytes.id(),
                units(inputs.preparing_bytes.get())?,
            ),
            (Dimension::Requests.id(), units(request_positions.get())?),
        ])
        .map_err(|_| NodeGeometryError::Overflow)?;
        let admission = AdmissionStorage {
            loading: policy.loading.resources()?,
            accepted: policy.accepted.resources()?,
            context: CONTEXT_BOOTSTRAP_STORAGE.resources()?,
        };
        Ok(Self {
            limits,
            admission,
            request_per_position,
            request_positions,
            inputs,
        })
    }
}
