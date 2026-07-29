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

use std::fmt;

use novarocks::protocol::starrocks::decode::{
    LakeMetaStorageFacts, LakeMetaStorageRequest, StarRocksExternalDependency,
    StarRocksResolvedDependencies, StarRocksResolvedDependencyValue,
};
use novarocks::thrift::types::TNetworkAddress;

use crate::fragment::admission::PrelaunchCancellationToken;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DependencyResolutionError {
    QueryProfileTransport { dependency_id: u64, source: String },
    LakeMetaStorage { dependency_id: u64, source: String },
    Cancelled { dependency_id: u64 },
}

impl fmt::Display for DependencyResolutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueryProfileTransport {
                dependency_id,
                source,
            } => write!(
                f,
                "query-profile dependency {dependency_id} failed: {source}"
            ),
            Self::LakeMetaStorage {
                dependency_id,
                source,
            } => write!(
                f,
                "lake-meta storage dependency {dependency_id} failed: {source}"
            ),
            Self::Cancelled { dependency_id } => {
                write!(f, "dependency {dependency_id} cancelled during preparation")
            }
        }
    }
}

impl std::error::Error for DependencyResolutionError {}

pub(crate) fn resolve_dependencies(
    requirements: &[StarRocksExternalDependency],
    token: &PrelaunchCancellationToken,
) -> Result<StarRocksResolvedDependencies, DependencyResolutionError> {
    resolve_dependencies_with(
        requirements,
        token,
        |endpoint, query_id| {
            let address = TNetworkAddress::new(endpoint.host().to_string(), endpoint.port());
            novarocks::service::fe_report::fetch_query_profile(&address, query_id)
        },
        |request| novarocks::connector::starrocks::resolve_lake_meta_storage(request),
    )
}

pub(crate) fn resolve_dependencies_with<QueryProfileResolver, LakeMetaResolver>(
    requirements: &[StarRocksExternalDependency],
    token: &PrelaunchCancellationToken,
    mut resolve_query_profile: QueryProfileResolver,
    mut resolve_lake_meta: LakeMetaResolver,
) -> Result<StarRocksResolvedDependencies, DependencyResolutionError>
where
    QueryProfileResolver:
        FnMut(&novarocks::runtime::endpoint::RuntimeEndpoint, &str) -> Result<String, String>,
    LakeMetaResolver: FnMut(&LakeMetaStorageRequest) -> Result<LakeMetaStorageFacts, String>,
{
    let mut resolved = StarRocksResolvedDependencies::default();
    for requirement in requirements {
        let dependency_id = requirement.id();
        token.check(dependency_id)?;
        let value = match requirement {
            StarRocksExternalDependency::QueryProfile { query_id, .. } => {
                let endpoint = token.frontend_endpoint().ok_or_else(|| {
                    DependencyResolutionError::QueryProfileTransport {
                        dependency_id,
                        source: "frontend endpoint is missing".to_string(),
                    }
                })?;
                let profile = resolve_query_profile(endpoint, query_id).map_err(|source| {
                    DependencyResolutionError::QueryProfileTransport {
                        dependency_id,
                        source,
                    }
                })?;
                StarRocksResolvedDependencyValue::QueryProfile(profile)
            }
            StarRocksExternalDependency::LakeMetaStorage { request, .. } => {
                let facts = resolve_lake_meta(request).map_err(|source| {
                    DependencyResolutionError::LakeMetaStorage {
                        dependency_id,
                        source,
                    }
                })?;
                StarRocksResolvedDependencyValue::LakeMetaStorage(facts)
            }
        };
        token.check(dependency_id)?;
        resolved.insert(dependency_id, value);
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use novarocks::common::types::UniqueId;
    use novarocks::protocol::starrocks::decode::{
        LakeMetaStorageFacts, LakeMetaStorageRequest, StarRocksExternalDependency,
        StarRocksResolvedDependencyValue,
    };
    use novarocks::runtime::endpoint::RuntimeEndpoint;
    use novarocks::runtime::query_context::QueryId;

    use super::{DependencyResolutionError, resolve_dependencies_with};
    use crate::fragment::admission::PrelaunchRegistry;

    fn guarded_token(
        finst_id: UniqueId,
    ) -> (
        Arc<PrelaunchRegistry>,
        super::PrelaunchCancellationToken,
        crate::fragment::admission::PrelaunchGuard,
    ) {
        let registry = Arc::new(PrelaunchRegistry::default());
        let mut guard = registry
            .install(QueryId::new(91, 92), 1, [finst_id])
            .expect("install");
        guard.set_frontend_endpoint(Some(
            RuntimeEndpoint::new("fe.test", 9020).expect("endpoint"),
        ));
        let token = guard.cancellation_token();
        (registry, token, guard)
    }

    fn query_profile_dependency(id: u64) -> StarRocksExternalDependency {
        StarRocksExternalDependency::QueryProfile {
            id,
            query_id: "query-1".to_string(),
        }
    }

    fn lake_meta_dependency(id: u64) -> StarRocksExternalDependency {
        StarRocksExternalDependency::LakeMetaStorage {
            id,
            request: LakeMetaStorageRequest::new(
                QueryId::new(93, 94),
                "catalog".to_string(),
                "db".to_string(),
                "table".to_string(),
                1,
                2,
                3,
                Vec::new(),
                Vec::new(),
            ),
        }
    }

    #[test]
    fn resolves_query_profile_and_lake_meta_dependencies() {
        let finst_id = UniqueId { hi: 101, lo: 102 };
        let (_registry, token, _guard) = guarded_token(finst_id);
        let result = resolve_dependencies_with(
            &[query_profile_dependency(7), lake_meta_dependency(8)],
            &token,
            |endpoint, query_id| {
                assert_eq!(endpoint.host(), "fe.test");
                assert_eq!(endpoint.port(), 9020);
                assert_eq!(query_id, "query-1");
                Ok("profile-json".to_string())
            },
            |_| Ok(LakeMetaStorageFacts::new(17, BTreeMap::new())),
        )
        .expect("resolve");
        assert!(matches!(
            result.get(7),
            Some(StarRocksResolvedDependencyValue::QueryProfile(profile)) if profile == "profile-json"
        ));
        assert!(matches!(
            result.get(8),
            Some(StarRocksResolvedDependencyValue::LakeMetaStorage(facts)) if facts.total_rows() == 17
        ));
    }

    #[test]
    fn classifies_provider_failures() {
        let (_registry, token, _guard) = guarded_token(UniqueId { hi: 103, lo: 104 });
        let transport = resolve_dependencies_with(
            &[query_profile_dependency(9)],
            &token,
            |_, _| Err("network unavailable".to_string()),
            |_| unreachable!(),
        )
        .expect_err("transport failure");
        assert!(matches!(
            transport,
            DependencyResolutionError::QueryProfileTransport { dependency_id: 9, ref source }
                if source == "network unavailable"
        ));
        let storage = resolve_dependencies_with(
            &[lake_meta_dependency(10)],
            &token,
            |_, _| unreachable!(),
            |_| Err("object store unavailable".to_string()),
        )
        .expect_err("storage failure");
        assert!(matches!(
            storage,
            DependencyResolutionError::LakeMetaStorage { dependency_id: 10, ref source }
                if source == "object store unavailable"
        ));
    }

    #[test]
    fn cancellation_before_and_after_wait_prevents_resolution() {
        let finst_id = UniqueId { hi: 105, lo: 106 };
        let (registry, token, _guard) = guarded_token(finst_id);
        assert!(registry.cancel(finst_id));
        let calls = AtomicUsize::new(0);
        let before = resolve_dependencies_with(
            &[query_profile_dependency(11)],
            &token,
            |_, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok("unreachable".to_string())
            },
            |_| unreachable!(),
        )
        .expect_err("cancelled");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            before,
            DependencyResolutionError::Cancelled { dependency_id: 11 }
        );

        let finst_id = UniqueId { hi: 107, lo: 108 };
        let (registry, token, _guard) = guarded_token(finst_id);
        let after = resolve_dependencies_with(
            &[query_profile_dependency(12)],
            &token,
            |_, _| {
                assert!(registry.cancel(finst_id));
                Ok("late profile".to_string())
            },
            |_| unreachable!(),
        )
        .expect_err("cancelled after wait");
        assert_eq!(
            after,
            DependencyResolutionError::Cancelled { dependency_id: 12 }
        );
    }
}
