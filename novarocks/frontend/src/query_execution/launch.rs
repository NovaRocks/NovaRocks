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

//! Frontend-owned Stage/Start orchestration values.
//!
//! The participant-local Stage and Start contracts belong to
//! `novarocks_protocol::lifecycle`. These values retain the frozen ownership
//! binding, exact-batch assembly, and the two-barrier launch port. They are
//! deliberately separate from the lifecycle wire value family.

use std::collections::BTreeSet;

use novarocks_protocol::lifecycle::{
    ContractError, ContractErrorCode, ParticipantManifestDigest, ParticipantRole, QueryExecutionId,
    QueryStageRequest, QueryStartRequest, StageDigest, StageDigestVersion, StageFragment,
};
use novarocks_types::UniqueId;

use crate::query_execution::contract::DistributedQueryError;

use crate::query_execution::lifecycle_plan::QueryLifecycleTarget;

pub const DEFAULT_STAGE_MAX_FRAGMENTS: usize =
    novarocks_protocol::lifecycle::stage::DEFAULT_STAGE_MAX_FRAGMENTS;

/// Frozen Stage ownership for one Init participant. It is captured before the
/// Init plan is consumed and never re-reads live backend topology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageParticipantBinding {
    target: QueryLifecycleTarget,
    init_digest: ParticipantManifestDigest,
    roles: BTreeSet<ParticipantRole>,
    expected_fragment_instance_ids: BTreeSet<UniqueId>,
}

impl StageParticipantBinding {
    pub fn new(
        target: QueryLifecycleTarget,
        init_digest: ParticipantManifestDigest,
        roles: impl IntoIterator<Item = ParticipantRole>,
        expected_fragment_instance_ids: impl IntoIterator<Item = UniqueId>,
    ) -> Result<Self, ContractError> {
        let roles = roles.into_iter().collect::<BTreeSet<_>>();
        if roles.is_empty() {
            return Err(ContractError::new(
                ContractErrorCode::InvalidValue,
                "stage participant must have at least one role",
            ));
        }
        Ok(Self {
            target,
            init_digest,
            roles,
            expected_fragment_instance_ids: expected_fragment_instance_ids.into_iter().collect(),
        })
    }

    pub const fn target(&self) -> QueryLifecycleTarget {
        self.target
    }

    pub const fn init_digest(&self) -> ParticipantManifestDigest {
        self.init_digest
    }

    pub fn roles(&self) -> &BTreeSet<ParticipantRole> {
        &self.roles
    }

    pub fn expected_fragment_instance_ids(&self) -> &BTreeSet<UniqueId> {
        &self.expected_fragment_instance_ids
    }

    pub fn is_fragment_executor(&self) -> bool {
        self.roles.contains(&ParticipantRole::FragmentExecutor)
    }
}

/// One complete, exact participant-local Protocol Stage request and its frozen
/// Core owner.
#[derive(Clone, Debug, PartialEq)]
pub struct StageBatch {
    binding: StageParticipantBinding,
    request: QueryStageRequest,
}

impl StageBatch {
    pub fn new(
        execution_id: QueryExecutionId,
        binding: StageParticipantBinding,
        fragments: Vec<StageFragment>,
    ) -> Result<Self, ContractError> {
        if fragments.len() > DEFAULT_STAGE_MAX_FRAGMENTS {
            return Err(ContractError::new(
                ContractErrorCode::Capacity,
                format!(
                    "stage batch contains {} fragments; limit is {DEFAULT_STAGE_MAX_FRAGMENTS}",
                    fragments.len()
                ),
            ));
        }
        let actual = fragments
            .iter()
            .map(StageFragment::fragment_instance_id)
            .collect::<BTreeSet<_>>();
        if actual != *binding.expected_fragment_instance_ids() {
            return Err(ContractError::new(
                ContractErrorCode::InvalidValue,
                format!(
                    "stage batch exact fragment set differs for backend {}: expected {:?}, actual {:?}",
                    binding.target().backend_idx(),
                    binding.expected_fragment_instance_ids(),
                    actual
                ),
            ));
        }
        let digest = StageDigest::compute_v1(execution_id, binding.init_digest(), &fragments)?;
        let request = QueryStageRequest::new(
            execution_id,
            binding.init_digest(),
            StageDigestVersion::V1,
            digest,
            fragments,
        )?;
        Ok(Self { binding, request })
    }

    pub const fn binding(&self) -> &StageParticipantBinding {
        &self.binding
    }

    pub const fn request(&self) -> &QueryStageRequest {
        &self.request
    }

    pub fn start_request(&self) -> QueryStartRequest {
        QueryStartRequest::new(
            self.request.execution_id(),
            self.request.digest_version(),
            self.request.digest(),
        )
        .expect("validated Stage request contains a valid Start fence")
    }
}

/// FE-owned two-barrier launch port. Implementations must not issue a Start
/// request until `stage_all` has succeeded for every supplied batch.
pub trait QueryLaunchBarrier: Send + Sync + 'static {
    fn stage_all(&self, batches: &[StageBatch]) -> Result<(), DistributedQueryError>;

    fn start_all(&self, batches: &[StageBatch]) -> Result<(), DistributedQueryError>;
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use novarocks_protocol::common;
    use novarocks_protocol::lifecycle::AttemptId;
    use novarocks_protocol::{novarocks, plan};
    use novarocks_types::QueryId;

    use super::*;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(7, 8),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    fn binding(expected: impl IntoIterator<Item = UniqueId>) -> StageParticipantBinding {
        StageParticipantBinding::new(
            QueryLifecycleTarget::new(
                4,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 19040),
                9,
            ),
            ParticipantManifestDigest::new([3; 32]),
            [ParticipantRole::FragmentExecutor],
            expected,
        )
        .expect("valid Stage participant binding")
    }

    fn fragment(lo: i64) -> StageFragment {
        StageFragment::new(
            plan::PlanFragment::default(),
            novarocks::InstanceParams {
                fragment_instance_id: Some(common::UniqueId { hi: 1, lo }),
                ..Default::default()
            },
        )
        .expect("valid Stage fragment")
    }

    #[test]
    fn batch_keeps_protocol_stage_and_start_carriers() {
        let first = fragment(9);
        let second = fragment(3);
        let batch = StageBatch::new(
            execution_id(),
            binding([first.fragment_instance_id(), second.fragment_instance_id()]),
            vec![first, second],
        )
        .expect("valid exact Stage batch");

        assert_eq!(
            batch.request().fragments()[0].fragment_instance_id().low(),
            3
        );
        assert_eq!(batch.request().as_proto().fragments.len(), 2);
        assert_eq!(
            batch.start_request().execution_id(),
            batch.request().execution_id()
        );
    }

    #[test]
    fn batch_rejects_a_fragment_set_outside_its_frozen_binding() {
        let error = StageBatch::new(
            execution_id(),
            binding([UniqueId::new(1, 9)]),
            vec![fragment(3)],
        )
        .expect_err("unbound fragment must not reach Protocol Stage");
        assert_eq!(error.code(), ContractErrorCode::InvalidValue);
    }
}
