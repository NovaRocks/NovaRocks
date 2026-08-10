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

use std::collections::BTreeSet;
use std::time::Duration;

use crate::common::types::UniqueId;
use crate::query_execution::backend::{CoordinatorReportEndpoint, LiveBackendTarget};
use novarocks_execution::runtime::query_options::QueryOptions;

use super::contract::{QueryLifecycleError, QueryLifecycleErrorCode};
use super::digest::digest_v1;
use super::identity::QueryExecutionId;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ParticipantRole {
    FragmentExecutor,
    RuntimeFilterService,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryControlEndpoint {
    host: String,
    port: u16,
}

impl QueryControlEndpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, QueryLifecycleError> {
        let host = host.into().trim().to_string();
        if host.is_empty() {
            return Err(QueryLifecycleError::invalid_manifest(
                "query control endpoint host must not be empty",
            ));
        }
        if port == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "query control endpoint port must be nonzero",
            ));
        }
        Ok(Self { host, port })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub const fn port(&self) -> u16 {
        self.port
    }
}

impl TryFrom<CoordinatorReportEndpoint> for QueryControlEndpoint {
    type Error = QueryLifecycleError;

    fn try_from(value: CoordinatorReportEndpoint) -> Result<Self, Self::Error> {
        let endpoint = value.into_runtime_endpoint();
        let port = u16::try_from(endpoint.port()).map_err(|_| {
            QueryLifecycleError::invalid_manifest("report endpoint port is outside u16 range")
        })?;
        Self::new(endpoint.host(), port)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParticipantBackendIdentity {
    backend_id: u64,
    endpoint: QueryControlEndpoint,
    start_epoch: u64,
}

impl ParticipantBackendIdentity {
    pub fn new(
        backend_id: u64,
        endpoint: QueryControlEndpoint,
        start_epoch: u64,
    ) -> Result<Self, QueryLifecycleError> {
        if start_epoch == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "backend start epoch must be nonzero",
            ));
        }
        Ok(Self {
            backend_id,
            endpoint,
            start_epoch,
        })
    }

    pub fn from_live_backend(target: LiveBackendTarget) -> Result<Self, QueryLifecycleError> {
        let backend_id = u64::try_from(target.backend_idx()).map_err(|_| {
            QueryLifecycleError::invalid_manifest("backend index is outside u64 range")
        })?;
        let endpoint = QueryControlEndpoint::new(
            target.endpoint().ip().to_string(),
            target.endpoint().port(),
        )?;
        Self::new(backend_id, endpoint, target.start_epoch())
    }

    pub const fn backend_id(&self) -> u64 {
        self.backend_id
    }

    pub const fn endpoint(&self) -> &QueryControlEndpoint {
        &self.endpoint
    }

    pub const fn start_epoch(&self) -> u64 {
        self.start_epoch
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ParticipantQueryOptions {
    native: QueryOptions,
}

impl ParticipantQueryOptions {
    pub const fn new(native: QueryOptions) -> Self {
        Self { native }
    }

    pub(crate) const fn native(&self) -> &QueryOptions {
        &self.native
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ExchangeRouteManifest {
    source_fragment_instance_id: UniqueId,
    destination_fragment_instance_id: UniqueId,
    destination_node_id: i32,
    sender_ordinal: u32,
    sender_count: u32,
}

impl ExchangeRouteManifest {
    pub fn new(
        source_fragment_instance_id: UniqueId,
        destination_fragment_instance_id: UniqueId,
        destination_node_id: i32,
        sender_ordinal: u32,
        sender_count: u32,
    ) -> Result<Self, QueryLifecycleError> {
        if is_missing_unique_id(source_fragment_instance_id)
            || is_missing_unique_id(destination_fragment_instance_id)
        {
            return Err(QueryLifecycleError::invalid_manifest(
                "exchange route fragment instance ids must be nonzero",
            ));
        }
        if destination_node_id < 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "exchange route destination node id must be nonnegative",
            ));
        }
        if sender_count == 0 || sender_ordinal >= sender_count {
            return Err(QueryLifecycleError::invalid_manifest(
                "exchange route sender ordinal must be less than nonzero sender count",
            ));
        }
        Ok(Self {
            source_fragment_instance_id,
            destination_fragment_instance_id,
            destination_node_id,
            sender_ordinal,
            sender_count,
        })
    }

    pub const fn source_fragment_instance_id(&self) -> UniqueId {
        self.source_fragment_instance_id
    }

    pub const fn destination_fragment_instance_id(&self) -> UniqueId {
        self.destination_fragment_instance_id
    }

    pub const fn destination_node_id(&self) -> i32 {
        self.destination_node_id
    }

    pub const fn sender_ordinal(&self) -> u32 {
        self.sender_ordinal
    }

    pub const fn sender_count(&self) -> u32 {
        self.sender_count
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeFilterContribution {
    wire: novarocks_protocol::novarocks::RuntimeFilterContribution,
}

impl RuntimeFilterContribution {
    /// Core carries a ready Protocol DTO without interpreting runtime-filter
    /// lifecycle, install, routing, epoch, or digest semantics. The Backend is
    /// the only owner that decodes and validates those facts.
    pub fn from_wire(
        wire: novarocks_protocol::novarocks::RuntimeFilterContribution,
    ) -> Result<Self, QueryLifecycleError> {
        if wire.participant_id == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "runtime filter participant id must be nonzero",
            ));
        }
        if wire.contribution_digest.len() != 32 {
            return Err(QueryLifecycleError::invalid_manifest(
                "runtime filter contribution digest must be 32 bytes",
            ));
        }
        Ok(Self { wire })
    }

    pub const fn participant_id(&self) -> u32 {
        self.wire.participant_id
    }

    pub fn wire(&self) -> &novarocks_protocol::novarocks::RuntimeFilterContribution {
        &self.wire
    }

    pub fn digest(&self) -> &[u8] {
        &self.wire.contribution_digest
    }

    /// Construct an opaque owner-level contract fixture. It deliberately has no
    /// typed install: Core only verifies carrier shape.
    pub fn empty_for_contract_test(
        execution_id: QueryExecutionId,
        participant_id: u32,
    ) -> Result<Self, QueryLifecycleError> {
        let _ = execution_id;
        Self::from_wire(novarocks_protocol::novarocks::RuntimeFilterContribution {
            participant_id,
            lifecycle: None,
            install: None,
            contribution_digest: vec![0; 32],
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParticipantManifestDigest([u8; 32]);

impl ParticipantManifestDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn try_from_slice(bytes: &[u8]) -> Result<Self, QueryLifecycleError> {
        let bytes: [u8; 32] = bytes.try_into().map_err(|_| {
            QueryLifecycleError::invalid_manifest("participant manifest digest must be 32 bytes")
        })?;
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ParticipantManifest {
    execution_id: QueryExecutionId,
    backend: ParticipantBackendIdentity,
    roles: BTreeSet<ParticipantRole>,
    expected_fragment_instance_ids: BTreeSet<UniqueId>,
    query_options: ParticipantQueryOptions,
    query_deadline_unix_ms: u64,
    exchange_routes: Vec<ExchangeRouteManifest>,
    runtime_filter: Option<RuntimeFilterContribution>,
    pre_start_timeout: Duration,
    report_endpoint: QueryControlEndpoint,
}

impl ParticipantManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        execution_id: QueryExecutionId,
        backend: ParticipantBackendIdentity,
        roles: impl IntoIterator<Item = ParticipantRole>,
        expected_fragment_instance_ids: impl IntoIterator<Item = UniqueId>,
        query_options: ParticipantQueryOptions,
        query_deadline_unix_ms: u64,
        exchange_routes: impl IntoIterator<Item = ExchangeRouteManifest>,
        runtime_filter: Option<RuntimeFilterContribution>,
        pre_start_timeout: Duration,
        report_endpoint: QueryControlEndpoint,
    ) -> Result<Self, QueryLifecycleError> {
        let roles = collect_unique(roles, "participant role")?;
        if roles.is_empty() {
            return Err(QueryLifecycleError::invalid_manifest(
                "participant roles must not be empty",
            ));
        }
        let expected_fragment_instance_ids =
            collect_unique(expected_fragment_instance_ids, "fragment instance id")?;
        if expected_fragment_instance_ids
            .iter()
            .any(|id| is_missing_unique_id(*id))
        {
            return Err(QueryLifecycleError::invalid_manifest(
                "expected fragment instance ids must be nonzero",
            ));
        }
        if !roles.contains(&ParticipantRole::FragmentExecutor)
            && !expected_fragment_instance_ids.is_empty()
        {
            return Err(QueryLifecycleError::invalid_manifest(
                "service-only participant must not declare fragment instances",
            ));
        }
        if runtime_filter.is_some() != roles.contains(&ParticipantRole::RuntimeFilterService) {
            return Err(QueryLifecycleError::invalid_manifest(
                "runtime filter contribution and participant role must be present together",
            ));
        }
        if query_deadline_unix_ms == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "query deadline must be nonzero",
            ));
        }
        if pre_start_timeout.is_zero() {
            return Err(QueryLifecycleError::invalid_manifest(
                "pre-start timeout must be nonzero",
            ));
        }
        if u64::try_from(pre_start_timeout.as_millis()).is_err() {
            return Err(QueryLifecycleError::invalid_manifest(
                "pre-start timeout must fit in u64 milliseconds",
            ));
        }
        let mut exchange_routes = exchange_routes.into_iter().collect::<Vec<_>>();
        exchange_routes.sort();
        if exchange_routes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(QueryLifecycleError::invalid_manifest(
                "duplicate exchange route",
            ));
        }
        Ok(Self {
            execution_id,
            backend,
            roles,
            expected_fragment_instance_ids,
            query_options,
            query_deadline_unix_ms,
            exchange_routes,
            runtime_filter,
            pre_start_timeout,
            report_endpoint,
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn backend(&self) -> &ParticipantBackendIdentity {
        &self.backend
    }

    pub const fn roles(&self) -> &BTreeSet<ParticipantRole> {
        &self.roles
    }

    pub const fn expected_fragment_instance_ids(&self) -> &BTreeSet<UniqueId> {
        &self.expected_fragment_instance_ids
    }

    pub const fn runtime_filter(&self) -> Option<&RuntimeFilterContribution> {
        self.runtime_filter.as_ref()
    }

    pub const fn query_options(&self) -> &ParticipantQueryOptions {
        &self.query_options
    }

    pub const fn query_deadline_unix_ms(&self) -> u64 {
        self.query_deadline_unix_ms
    }

    pub fn exchange_routes(&self) -> &[ExchangeRouteManifest] {
        &self.exchange_routes
    }

    pub const fn pre_start_timeout(&self) -> Duration {
        self.pre_start_timeout
    }

    pub const fn report_endpoint(&self) -> &QueryControlEndpoint {
        &self.report_endpoint
    }

    pub fn digest(&self) -> ParticipantManifestDigest {
        digest_v1(self)
    }

    #[cfg(test)]
    pub(crate) fn with_execution_id(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<Self, QueryLifecycleError> {
        let mut next = self.clone();
        next.execution_id = execution_id;
        Ok(next)
    }
}

fn collect_unique<T>(
    values: impl IntoIterator<Item = T>,
    identity: &'static str,
) -> Result<BTreeSet<T>, QueryLifecycleError>
where
    T: Ord,
{
    let mut set = BTreeSet::new();
    for value in values {
        if !set.insert(value) {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                format!("duplicate {identity}"),
            ));
        }
    }
    Ok(set)
}

const fn is_missing_unique_id(id: UniqueId) -> bool {
    id.high() == 0 && id.low() == 0
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        ParticipantBackendIdentity, ParticipantManifest, ParticipantQueryOptions, ParticipantRole,
        QueryControlEndpoint, RuntimeFilterContribution,
    };
    use crate::common::types::UniqueId;
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::contract::QueryLifecycleErrorCode;
    use crate::query_execution::lifecycle::identity::{AttemptId, QueryExecutionId};
    use novarocks_execution::runtime::query_options::QueryOptions;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(5, 6),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    fn endpoint(port: u16) -> QueryControlEndpoint {
        QueryControlEndpoint::new("127.0.0.1", port).expect("valid endpoint")
    }

    fn backend() -> ParticipantBackendIdentity {
        ParticipantBackendIdentity::new(3, endpoint(9030), 11).expect("valid backend")
    }

    #[test]
    fn participant_manifest_validation_rejects_service_only_fragments() {
        let error = ParticipantManifest::new(
            execution_id(),
            backend(),
            [ParticipantRole::RuntimeFilterService],
            [UniqueId::new(7, 9)],
            ParticipantQueryOptions::new(QueryOptions::default()),
            1_000,
            [],
            None,
            Duration::from_secs(30),
            endpoint(9031),
        )
        .expect_err("service-only participant must reject fragments");

        assert_eq!(error.code(), QueryLifecycleErrorCode::InvalidManifest);
    }

    #[test]
    fn participant_manifest_validation_rejects_duplicate_fragments() {
        let duplicate = UniqueId::new(7, 9);
        let error = ParticipantManifest::new(
            execution_id(),
            backend(),
            [ParticipantRole::FragmentExecutor],
            [duplicate, duplicate],
            ParticipantQueryOptions::new(QueryOptions::default()),
            1_000,
            [],
            None,
            Duration::from_secs(30),
            endpoint(9031),
        )
        .expect_err("duplicate fragment ids must be rejected");

        assert_eq!(error.code(), QueryLifecycleErrorCode::InvalidManifest);
    }

    #[test]
    fn participant_manifest_execution_id_change_preserves_opaque_runtime_filter_wire() {
        let contribution = RuntimeFilterContribution::empty_for_contract_test(execution_id(), 3)
            .expect("valid opaque contribution");
        let expected_wire = contribution.wire().clone();
        let manifest = ParticipantManifest::new(
            execution_id(),
            backend(),
            [ParticipantRole::RuntimeFilterService],
            [],
            ParticipantQueryOptions::new(QueryOptions::default()),
            1_000,
            [],
            Some(contribution),
            Duration::from_secs(30),
            endpoint(9031),
        )
        .expect("valid manifest");
        let next_execution_id = QueryExecutionId::new(
            manifest.execution_id().query_id(),
            AttemptId::new(2).expect("nonzero attempt"),
        )
        .expect("valid execution id");

        let changed = manifest
            .with_execution_id(next_execution_id)
            .expect("Core does not reinterpret opaque runtime-filter wire facts");

        assert_eq!(changed.execution_id(), next_execution_id);
        assert_eq!(
            changed
                .runtime_filter()
                .expect("runtime-filter contribution")
                .wire(),
            &expected_wire
        );
    }
}
