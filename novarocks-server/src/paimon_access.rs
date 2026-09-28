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

//! Server-owned Paimon filesystem authorization composition.

use std::sync::Arc;
use std::time::Instant;

use novarocks_connector_paimon::io::{PaimonFsAuthorizedListing, PaimonHostFileIo};
use novarocks_connector_paimon::role_binding::PaimonRoleFileIoFactory;
use novarocks_fs::{
    FileCancellation, FileError, FileRangeScope, FileRangeService, FsAccessResources, FsScheme,
    ObjectStoreAccessContext, ObjectStoreCredentialProviderIdentity, ObjectStoreSecretMaterial,
    object_store_endpoint_config_from_aws_s3_catalog_property_pairs,
};
use novarocks_spi::connector::{
    CatalogCredentialMode, CatalogCredentialPurpose, CatalogNonSecretProperty, CatalogProperties,
    CatalogStorageAccessDomainInput, ConnectorError, ConnectorErrorKind, ConnectorProviderId,
    ConnectorRequestContext,
};

use crate::catalog_credential_registry::CatalogCredentialRegistry;
use novarocks_types::ClusterRole;

/// Immutable role-local resources used to bind one admitted Paimon request.
/// Construction performs no catalog or object-store I/O.
#[derive(Clone)]
pub(crate) struct ServerPaimonRoleFileIoFactory {
    resources: FsAccessResources,
    credentials: CatalogCredentialRegistry,
    purpose: CatalogCredentialPurpose,
    /// The shared scan I/O a BE attempt reads through; a coordinator has none.
    range_service: Option<Arc<FileRangeService>>,
}

impl ServerPaimonRoleFileIoFactory {
    pub(crate) fn new(
        resources: FsAccessResources,
        credentials: CatalogCredentialRegistry,
    ) -> Self {
        let purpose = match credentials.role() {
            ClusterRole::Fe => CatalogCredentialPurpose::ObjectStoreMetadata,
            ClusterRole::Be => CatalogCredentialPurpose::ObjectStoreData,
        };
        Self {
            resources,
            credentials,
            purpose,
            range_service: None,
        }
    }

    /// Reads of every bound attempt go through `range_service`, under the
    /// execution source their request was bound to.
    pub(crate) fn with_range_service(mut self, range_service: Arc<FileRangeService>) -> Self {
        self.range_service = Some(range_service);
        self
    }
}

impl PaimonRoleFileIoFactory for ServerPaimonRoleFileIoFactory {
    fn bind_file_io(
        &self,
        properties: &CatalogProperties,
        warehouse: &str,
        request: &ConnectorRequestContext,
    ) -> Result<PaimonHostFileIo, ConnectorError> {
        check_request_active(request)?;
        if properties.provider_id().as_str() != novarocks_connector_paimon::PROVIDER_ID {
            return Err(invalid(
                "Paimon filesystem binding received another provider identity",
            ));
        }
        if properties
            .execution_properties()
            .iter()
            .find(|property| property.key() == "warehouse")
            .map(|property| property.value())
            != Some(warehouse)
        {
            return Err(invalid(
                "Paimon filesystem binding warehouse differs from the catalog definition",
            ));
        }

        let parsed_warehouse = self
            .resources
            .access_resolver()
            .parse_location(warehouse)
            .map_err(map_file_error)?;
        if parsed_warehouse.scheme() != FsScheme::ObjectStore {
            return Err(unsupported(
                "PAI-1 Paimon filesystem catalogs require an object-store warehouse",
            ));
        }

        let object_store_binding = properties
            .credential_bindings()
            .iter()
            .find(|binding| binding.purpose() == self.purpose)
            .ok_or_else(|| {
                invalid(
                    "Paimon object-store warehouse requires the exact role-local credential binding",
                )
            })?;
        let credential_reference = match object_store_binding.mode() {
            CatalogCredentialMode::Static(reference) => reference,
            CatalogCredentialMode::Vended => {
                return Err(unsupported(
                    "PAI-1 Paimon filesystem catalogs do not support vended credentials",
                ));
            }
        };

        let execution_properties = properties
            .execution_properties()
            .iter()
            .map(|property| (property.key().to_string(), property.value().to_string()))
            .collect::<Vec<_>>();
        let endpoint_config =
            object_store_endpoint_config_from_aws_s3_catalog_property_pairs(&execution_properties)
                .map_err(invalid)?
                .ok_or_else(|| {
                    invalid("Paimon object-store credential binding is missing endpoint config")
                })?;
        let non_secret_properties = properties
            .execution_properties()
            .iter()
            .map(|property| CatalogNonSecretProperty::try_new(property.key(), property.value()))
            .collect::<Result<Vec<_>, _>>()?;
        let provider_id = ConnectorProviderId::parse(novarocks_connector_paimon::PROVIDER_ID)
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
            })?;
        let access_domain = CatalogStorageAccessDomainInput::try_new(
            provider_id,
            properties.handle().catalog_name().clone(),
            properties.config_format_version(),
            non_secret_properties,
            object_store_binding.clone(),
            vec![],
        )?
        .derive_access_domain();

        let material = self
            .credentials
            .resolve(self.purpose, credential_reference)
            .and_then(|material| material.as_s3())
            .ok_or_else(|| {
                invalid("role-local registry has no exact S3 object-store credential binding")
            })?;
        let object_store_access = ObjectStoreAccessContext::new(
            endpoint_config,
            ObjectStoreCredentialProviderIdentity::Static(credential_reference.clone()),
            ObjectStoreSecretMaterial {
                access_key_id: material.access_key_id().clone(),
                access_key_secret: material.access_key_secret().clone(),
                session_token: material.session_token().cloned(),
            },
            self.resources.object_store_provider_pool(),
        );
        let access = self
            .resources
            .access_resolver()
            .resolve_location(access_domain, warehouse, Some(object_store_access))
            .map_err(map_file_error)?;

        let file_io = PaimonHostFileIo::try_new(
            access,
            warehouse,
            FileCancellation::from_connector_request(request),
            Arc::new(PaimonFsAuthorizedListing),
        )
        .map_err(map_file_error)?;
        let Some(range_service) = &self.range_service else {
            return Ok(file_io);
        };
        // A BE attempt reads through the shared range service only for the one
        // execution source its request was bound to.
        let (scope, operations) = request
            .range_scope()
            .zip(request.source_operations())
            .ok_or_else(|| invalid("BE Paimon read requires an exact execution source"))?;
        let (query_high, query_low, attempt, fragment_high, fragment_low, node_id) = scope.parts();
        let scope = FileRangeScope::try_new(
            query_high,
            query_low,
            attempt,
            fragment_high,
            fragment_low,
            node_id,
        )
        .map_err(map_file_error)?;
        Ok(file_io.with_range_binding(range_service.bind(scope, operations.clone())))
    }
}

fn check_request_active(request: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if request.is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "Paimon filesystem binding request was cancelled",
        ));
    }
    if Instant::now() >= request.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Paimon filesystem binding request deadline elapsed",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

fn map_file_error(error: FileError) -> ConnectorError {
    ConnectorError::from(error)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use novarocks_fs::{
        FsAccessResolver, ObjectStoreProviderPool, ObjectStoreProviderPoolOptions,
        TokioFileIoRuntime, TokioFileTaskSpawner,
    };
    use novarocks_secret::SecretValue;
    use novarocks_spi::connector::read_stack::ConnectorSourceOperations;
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
        CatalogProperties, CatalogProperty, CatalogVersion, ConnectorInstanceId,
        ConnectorProviderId, ConnectorRangeScope, ConnectorRequestContext, CredentialConsumerRole,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        StaticCredentialReference,
    };
    use novarocks_types::ClusterRole;

    use super::*;
    use crate::catalog_credential_registry::{
        CatalogCredentialMaterial, CatalogCredentialRegistryEntry, S3CredentialMaterial,
    };

    fn request() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request")
    }

    fn properties(
        reference: &StaticCredentialReference,
        purpose: CatalogCredentialPurpose,
        role: CredentialConsumerRole,
    ) -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("paimon_fixture").expect("instance"),
                CatalogVersion::from_bytes([7; 32]),
            ),
            ConnectorProviderId::parse("paimon").expect("provider"),
            1,
            vec![
                CatalogProperty::new("paimon.catalog.type", "filesystem").expect("property"),
                CatalogProperty::new("warehouse", "s3://warehouse/paimon").expect("property"),
                CatalogProperty::new("aws.s3.endpoint", "http://127.0.0.1:9000").expect("property"),
                CatalogProperty::new("aws.s3.region", "us-east-1").expect("property"),
                CatalogProperty::new("aws.s3.enable_path_style_access", "true").expect("property"),
            ],
            vec![
                CatalogCredentialBinding::try_new(
                    purpose,
                    role,
                    CatalogCredentialMode::Static(reference.clone()),
                )
                .expect("binding"),
            ],
        )
        .expect("catalog properties")
    }

    fn factory(
        runtime: &tokio::runtime::Runtime,
        reference: &StaticCredentialReference,
        cluster_role: ClusterRole,
        purpose: CatalogCredentialPurpose,
    ) -> ServerPaimonRoleFileIoFactory {
        let material = CatalogCredentialMaterial::S3(
            S3CredentialMaterial::new(SecretValue::new("access"), SecretValue::new("secret"), None)
                .expect("material"),
        );
        let registry = CatalogCredentialRegistry::try_new(
            cluster_role,
            vec![
                CatalogCredentialRegistryEntry::try_new(purpose, reference.clone(), material)
                    .expect("entry"),
            ],
        )
        .expect("registry");
        let resources = FsAccessResources::new(
            Arc::new(
                ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default())
                    .expect("pool"),
            ),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        ServerPaimonRoleFileIoFactory::new(resources, registry)
    }

    #[test]
    fn binds_exact_static_credential_without_remote_io() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let reference = StaticCredentialReference::try_new("minio", "v1").expect("reference");
        let factory = factory(
            &runtime,
            &reference,
            ClusterRole::Be,
            CatalogCredentialPurpose::ObjectStoreData,
        );

        factory
            .bind_file_io(
                &properties(
                    &reference,
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::Backend,
                ),
                "s3://warehouse/paimon",
                &request(),
            )
            .expect("pure Paimon file binding");
    }

    #[test]
    fn a_backend_attempt_reads_through_the_scan_io_of_its_execution_source() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let reference = StaticCredentialReference::try_new("minio", "v1").expect("reference");
        let range_service = FileRangeService::new(
            std::num::NonZeroUsize::new(2).unwrap(),
            std::num::NonZeroUsize::new(1).unwrap(),
            std::num::NonZeroUsize::new(4).unwrap(),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
            runtime.handle().clone(),
        );
        let factory = factory(
            &runtime,
            &reference,
            ClusterRole::Be,
            CatalogCredentialPurpose::ObjectStoreData,
        )
        .with_range_service(range_service);
        let properties = properties(
            &reference,
            CatalogCredentialPurpose::ObjectStoreData,
            CredentialConsumerRole::Backend,
        );

        let error = factory
            .bind_file_io(&properties, "s3://warehouse/paimon", &request())
            .expect_err("a backend attempt must name its execution source");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);

        let operations = ConnectorSourceOperations::new();
        let scoped = request().with_execution_source(
            ConnectorRangeScope::try_new(1, 2, 3, 4, 5, 6).expect("scope"),
            operations.clone(),
        );
        let file_io = factory
            .bind_file_io(&properties, "s3://warehouse/paimon", &scoped)
            .expect("scoped Paimon file binding");
        // The bound IO carries this request's own scope and source operations:
        // sealing the request's registry is visible through it.
        operations.seal();
        let bound = format!("{file_io:?}");
        assert!(
            bound.contains(
                "FileRangeBinding { scope: FileRangeScope { query: (1, 2, 3), source: (4, 5, 6) }"
            ),
            "{bound}"
        );
        assert!(bound.contains("sealed: true"), "{bound}");
    }

    #[test]
    fn frontend_binds_only_the_metadata_credential() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let reference = StaticCredentialReference::try_new("minio", "v1").expect("reference");
        let factory = factory(
            &runtime,
            &reference,
            ClusterRole::Fe,
            CatalogCredentialPurpose::ObjectStoreMetadata,
        );

        factory
            .bind_file_io(
                &properties(
                    &reference,
                    CatalogCredentialPurpose::ObjectStoreMetadata,
                    CredentialConsumerRole::Frontend,
                ),
                "s3://warehouse/paimon",
                &request(),
            )
            .expect("frontend Paimon metadata binding");
        let error = factory
            .bind_file_io(
                &properties(
                    &reference,
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::Backend,
                ),
                "s3://warehouse/paimon",
                &request(),
            )
            .expect_err("frontend must not fall back to a data credential");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn rejects_a_missing_exact_credential() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let configured = StaticCredentialReference::try_new("minio", "v1").expect("reference");
        let requested = StaticCredentialReference::try_new("minio", "v2").expect("reference");
        let factory = factory(
            &runtime,
            &configured,
            ClusterRole::Be,
            CatalogCredentialPurpose::ObjectStoreData,
        );

        let error = factory
            .bind_file_io(
                &properties(
                    &requested,
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::Backend,
                ),
                "s3://warehouse/paimon",
                &request(),
            )
            .expect_err("unknown credential generation must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }
}
