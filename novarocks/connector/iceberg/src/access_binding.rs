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

//! Provider-owned process-local filesystem access binding.
//!
//! The binding carries only startup-composed credentials, access resolution,
//! and file-I/O runtime services. It is intentionally independent of Core's
//! execution operators and SQL/application lifecycle.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use novarocks_fs::{
    FileError, FileIdentity, FileIoRuntime, FileReadContext, FileTaskSpawner, FsAccessHandle,
    FsAccessResolver, FsAccessResources, FsScheme, ObjectStoreAccessContext,
    ObjectStoreCredentialProviderIdentity, ObjectStoreEndpointConfig, ObjectStoreSecretMaterial,
};
use novarocks_spi::connector::{
    CatalogCredentialMode, CatalogCredentialPurpose, CatalogNonSecretProperty, CatalogProperties,
    CatalogStorageAccessDomainInput, CatalogUncredentialedStorageKind, ConnectorError,
    ConnectorErrorKind, ConnectorProviderId, ConnectorRequestContext, StaticCredentialReference,
    StorageAccessDomainId, StorageAccessRequest,
};

/// Role-local resolver for one exact static object-store credential reference.
///
/// The composition root owns the immutable registry. This connector only
/// receives a sealed resolver and never reads configuration or discovers
/// another role's credentials.
pub trait IcebergStaticCredentialResolver: Send + Sync {
    fn resolve_object_store_static(
        &self,
        reference: &StaticCredentialReference,
    ) -> Result<ObjectStoreSecretMaterial, ConnectorError>;
}

#[derive(Clone)]
enum IcebergStorageAccess {
    StaticObjectStore {
        access_domain: StorageAccessDomainId,
        endpoint_config: ObjectStoreEndpointConfig,
        credential_reference: StaticCredentialReference,
    },
    VendedObjectStore {
        owner: novarocks_spi::connector::CatalogHandle,
        endpoint_config: ObjectStoreEndpointConfig,
    },
    Uncredentialed {
        provider_id: ConnectorProviderId,
        catalog_name: novarocks_spi::connector::ConnectorInstanceId,
        config_format_version: u32,
        non_secret_properties: Vec<CatalogNonSecretProperty>,
    },
}

#[derive(Clone)]
pub struct IcebergReadBinding {
    resources: FsAccessResources,
    credential_resolver: Option<Arc<dyn IcebergStaticCredentialResolver>>,
    storage_access: Option<IcebergStorageAccess>,
    request_context: Option<ConnectorRequestContext>,
}

/// Provider-local credentials selected for one Iceberg object-store location.
/// This is process-local construction state, never a connector handle or
/// durable catalog property.
#[derive(Clone, Debug)]
pub struct IcebergObjectStoreBinding {
    bucket: String,
    config: novarocks_fs::ObjectStoreConfig,
}

impl IcebergObjectStoreBinding {
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn config(&self) -> &novarocks_fs::ObjectStoreConfig {
        &self.config
    }
}

impl std::fmt::Debug for IcebergReadBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergReadBinding")
            .field(
                "storage_access",
                &self.storage_access.as_ref().map(|_| "<bound>"),
            )
            .finish_non_exhaustive()
    }
}

impl IcebergReadBinding {
    /// Builds an Iceberg filesystem binding from resources supplied by the
    /// process composition root.
    pub fn from_resources(resources: FsAccessResources) -> Self {
        Self {
            resources,
            credential_resolver: None,
            storage_access: None,
            request_context: None,
        }
    }

    /// Build a role-local template that can be rebound only to an admitted
    /// immutable catalog definition.
    pub fn with_static_credential_resolver(
        resources: FsAccessResources,
        credential_resolver: Arc<dyn IcebergStaticCredentialResolver>,
    ) -> Self {
        Self {
            resources,
            credential_resolver: Some(credential_resolver),
            storage_access: None,
            request_context: None,
        }
    }

    /// Bind a catalog definition to its exact, role-local static credential
    /// resolver. The catalog carrier contains only non-secret facts. Vended
    /// credentials are intentionally rejected until their query-attempt lease
    /// protocol exists.
    pub fn from_catalog_properties(
        resources: FsAccessResources,
        credential_resolver: Arc<dyn IcebergStaticCredentialResolver>,
        properties: &CatalogProperties,
    ) -> Result<Self, ConnectorError> {
        if properties.provider_id().as_str() != "iceberg" {
            return Err(invalid(
                "Iceberg access binding received another provider kind",
            ));
        }
        let provider_id = ConnectorProviderId::parse("iceberg").map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
        })?;
        let non_secret_properties = properties
            .execution_properties()
            .iter()
            .map(|property| CatalogNonSecretProperty::try_new(property.key(), property.value()))
            .collect::<Result<Vec<_>, _>>()?;
        let object_store_binding = properties
            .credential_bindings()
            .iter()
            .find(|binding| binding.purpose() == CatalogCredentialPurpose::ObjectStoreData);

        let endpoint_config =
            crate::catalog_config::object_store_endpoint_config_from_catalog_properties(
                &properties
                    .execution_properties()
                    .iter()
                    .map(|property| (property.key().to_string(), property.value().to_string()))
                    .collect::<Vec<_>>(),
            )
            .map_err(invalid)?;

        let storage_access = match object_store_binding {
            Some(binding) => {
                let endpoint_config = endpoint_config.ok_or_else(|| {
                    invalid("Iceberg object-store binding missing endpoint config")
                })?;
                match binding.mode() {
                    CatalogCredentialMode::Static(reference) => {
                        let domain_input = CatalogStorageAccessDomainInput::try_new(
                            provider_id,
                            properties.handle().catalog_name().clone(),
                            properties.config_format_version(),
                            non_secret_properties,
                            binding.clone(),
                            vec![],
                        )?;
                        IcebergStorageAccess::StaticObjectStore {
                            access_domain: domain_input.derive_access_domain(),
                            endpoint_config,
                            credential_reference: reference.clone(),
                        }
                    }
                    CatalogCredentialMode::Vended => IcebergStorageAccess::VendedObjectStore {
                        owner: properties.handle().clone(),
                        endpoint_config,
                    },
                }
            }
            None => {
                if endpoint_config.is_some() {
                    return Err(invalid(
                        "Iceberg object-store endpoint requires an exact object-store credential binding",
                    ));
                }
                IcebergStorageAccess::Uncredentialed {
                    provider_id,
                    catalog_name: properties.handle().catalog_name().clone(),
                    config_format_version: properties.config_format_version(),
                    non_secret_properties,
                }
            }
        };
        Ok(Self {
            resources,
            credential_resolver: Some(credential_resolver),
            storage_access: Some(storage_access),
            request_context: None,
        })
    }

    /// Rebind this role-local template to one immutable catalog definition.
    /// Templates without a credential resolver are deliberately unusable for
    /// catalog I/O: production composition must supply an exact resolver.
    pub fn bind_catalog(&self, properties: &CatalogProperties) -> Result<Self, ConnectorError> {
        let resolver = self.credential_resolver.clone().ok_or_else(|| {
            invalid("Iceberg catalog access binding has no role-local credential resolver")
        })?;
        Self::from_catalog_properties(self.resources.clone(), resolver, properties)
    }

    /// Explicit convenience constructor for composition roots that do not
    /// retain a reusable [`FsAccessResources`] bundle.
    pub fn new(
        object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
        access_resolver: FsAccessResolver,
        file_runtime: Arc<dyn FileIoRuntime>,
        file_task_spawner: Arc<dyn FileTaskSpawner>,
    ) -> Self {
        let pool = Arc::new(
            novarocks_fs::ObjectStoreProviderPool::new(
                novarocks_fs::ObjectStoreProviderPoolOptions::default(),
            )
            .expect("build test object-store provider pool"),
        );
        let resources =
            FsAccessResources::new(pool, access_resolver, file_runtime, file_task_spawner);
        let access_domain = StorageAccessDomainId::from_bytes([0x54; 32]);
        let storage_access = match object_store_config.as_ref() {
            Some(config) => IcebergStorageAccess::StaticObjectStore {
                access_domain,
                endpoint_config: config.endpoint_config(),
                credential_reference: StaticCredentialReference::try_new(
                    "iceberg-test-object-store",
                    "test",
                )
                .expect("build test static credential reference"),
            },
            None => IcebergStorageAccess::Uncredentialed {
                provider_id: ConnectorProviderId::parse("iceberg")
                    .expect("static Iceberg provider id"),
                catalog_name: novarocks_spi::connector::ConnectorInstanceId::try_from_canonical(
                    "iceberg-test",
                )
                .expect("build test catalog name"),
                config_format_version: 1,
                non_secret_properties: vec![],
            },
        };
        let resolver = Arc::new(TestCredentialResolver {
            object_store_config,
        });
        Self {
            resources,
            credential_resolver: Some(resolver),
            storage_access: Some(storage_access),
            request_context: None,
        }
    }

    /// Bind this provider template to one admitted request. This local view is
    /// carried only by an active reader or writer, never by a table or handle.
    pub fn for_request(&self, request_context: ConnectorRequestContext) -> Self {
        Self {
            resources: self.resources.clone(),
            credential_resolver: self.credential_resolver.clone(),
            storage_access: self.storage_access.clone(),
            request_context: Some(request_context),
        }
    }

    /// Whether object-store access is intentionally unavailable until this
    /// binding is rebound to an admitted request. Callers use this only to
    /// defer optional startup capability probes; actual I/O must still call
    /// [`Self::resolve_access`] and therefore remains fail-closed.
    pub(crate) fn requires_request_storage_resolver(&self) -> bool {
        matches!(
            self.storage_access,
            Some(IcebergStorageAccess::VendedObjectStore { .. })
        )
    }

    /// Resolve the startup-composed object-store credentials for an Iceberg
    /// output location. Local/HDFS paths intentionally return no object-store
    /// binding; object-store paths must name a bucket and have explicit BE
    /// credentials.
    pub fn object_store_binding_for_location(
        &self,
        location: &str,
    ) -> Result<Option<IcebergObjectStoreBinding>, String> {
        let location = self
            .resources
            .access_resolver()
            .parse_location(location)
            .map_err(|error| format!("parse Iceberg output location: {error}"))?;
        if location.scheme() != novarocks_fs::FsScheme::ObjectStore {
            return Ok(None);
        }
        let bucket = location.authority().ok_or_else(|| {
            format!(
                "Iceberg object-store output location is missing a bucket: {}",
                location.original()
            )
        })?;
        let (endpoint_config, secret_material) = self
            .object_store_access_context()
            .map_err(|error| error.to_string())?;
        let config = novarocks_fs::ObjectStoreConfig {
            endpoint: endpoint_config.endpoint,
            access_key_id: secret_material.access_key_id,
            access_key_secret: secret_material.access_key_secret,
            session_token: secret_material.session_token,
            enable_path_style_access: endpoint_config.enable_path_style_access,
            region: endpoint_config.region,
            retry_max_times: endpoint_config.retry_max_times,
            retry_min_delay_ms: endpoint_config.retry_min_delay_ms,
            retry_max_delay_ms: endpoint_config.retry_max_delay_ms,
            timeout_ms: endpoint_config.timeout_ms,
            io_timeout_ms: endpoint_config.io_timeout_ms,
        };
        Ok(Some(IcebergObjectStoreBinding {
            bucket: bucket.to_string(),
            config,
        }))
    }

    pub fn is_object_store_location(&self, location: &str) -> Result<bool, String> {
        self.resources
            .access_resolver()
            .parse_location(location)
            .map(|location| location.scheme() == FsScheme::ObjectStore)
            .map_err(|error| format!("parse Iceberg output location: {error}"))
    }

    pub fn resolve_access(&self, location: &str) -> Result<FsAccessHandle, ConnectorError> {
        self.resolve_access_for_locations(std::iter::once(location))
    }

    pub fn resolve_access_for_locations<I, S>(
        &self,
        locations: I,
    ) -> Result<FsAccessHandle, ConnectorError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let locations = locations
            .into_iter()
            .map(|location| location.as_ref().to_string())
            .collect::<Vec<_>>();
        let first = locations
            .first()
            .ok_or_else(|| invalid("Iceberg filesystem locations are empty"))?;
        let parsed = self
            .resources
            .access_resolver()
            .parse_location(first)
            .map_err(file_error)?;
        if matches!(
            self.storage_access.as_ref(),
            Some(IcebergStorageAccess::VendedObjectStore { .. })
        ) {
            return self.resolve_vended_access_for_locations(locations, parsed.scheme());
        }
        let access_domain = self.access_domain_for_location(&parsed)?;
        let object_store_access = self.object_store_access_context_for_scheme(parsed.scheme())?;
        self.resources
            .access_resolver()
            .resolve_locations(access_domain, locations, object_store_access)
            .map_err(file_error)
    }

    fn access_domain_for_location(
        &self,
        location: &novarocks_fs::FsLocation,
    ) -> Result<StorageAccessDomainId, ConnectorError> {
        match self.storage_access.as_ref().ok_or_else(|| {
            invalid("Iceberg filesystem operation has no admitted storage capability")
        })? {
            IcebergStorageAccess::StaticObjectStore { access_domain, .. } => {
                if location.scheme() != FsScheme::ObjectStore {
                    return Err(invalid(
                        "Iceberg object-store capability cannot resolve an uncredentialed location",
                    ));
                }
                Ok(*access_domain)
            }
            IcebergStorageAccess::VendedObjectStore { .. } => Err(invalid(
                "Iceberg vended object-store access must resolve through the query storage resolver",
            )),
            IcebergStorageAccess::Uncredentialed {
                provider_id,
                catalog_name,
                config_format_version,
                non_secret_properties,
            } => {
                let (kind, authority) = match location.scheme() {
                    FsScheme::Local => (CatalogUncredentialedStorageKind::Local, None),
                    FsScheme::Hdfs => {
                        (CatalogUncredentialedStorageKind::Hdfs, location.authority())
                    }
                    FsScheme::ObjectStore => {
                        return Err(invalid(
                            "Iceberg object-store location has no admitted exact credential binding",
                        ));
                    }
                };
                CatalogStorageAccessDomainInput::try_new_uncredentialed(
                    provider_id.clone(),
                    catalog_name.clone(),
                    *config_format_version,
                    non_secret_properties.clone(),
                    kind,
                    authority,
                )
                .map(|input| input.derive_access_domain())
            }
        }
    }

    fn resolve_vended_access_for_locations(
        &self,
        locations: Vec<String>,
        scheme: FsScheme,
    ) -> Result<FsAccessHandle, ConnectorError> {
        if scheme != FsScheme::ObjectStore {
            return Err(invalid(
                "Iceberg vended object-store capability cannot resolve an uncredentialed location",
            ));
        }
        let IcebergStorageAccess::VendedObjectStore {
            owner,
            endpoint_config,
        } = self
            .storage_access
            .as_ref()
            .expect("checked vended storage access")
        else {
            unreachable!("vended path requires vended storage access");
        };
        let context = self.request_context.as_ref().ok_or_else(|| {
            invalid("Iceberg vended object-store operation has no query storage resolver")
        })?;
        let resolver = context.storage_resolver().ok_or_else(|| {
            invalid("Iceberg vended object-store operation has no query storage resolver")
        })?;
        let mut resolved = locations
            .iter()
            .map(|location| {
                StorageAccessRequest::try_new(owner.clone(), location)
                    .and_then(|request| resolver.resolve_vended_s3(&request))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let selected = resolved
            .drain(..)
            .next()
            .ok_or_else(|| invalid("Iceberg filesystem locations are empty"))?;
        if resolved.iter().any(|other| {
            other.storage_access_domain_id() != selected.storage_access_domain_id()
                || other.lease_id() != selected.lease_id()
                || other.epoch() != selected.epoch()
                || other.matched_prefix() != selected.matched_prefix()
                || other.not_after_unix_ms() != selected.not_after_unix_ms()
        }) {
            return Err(invalid(
                "Iceberg vended filesystem locations require different credential selections",
            ));
        }
        let expires_at = credential_expiration(selected.not_after_unix_ms())?;
        let object_store_access = ObjectStoreAccessContext::new(
            endpoint_config.clone(),
            ObjectStoreCredentialProviderIdentity::Vended {
                lease_id: selected.lease_id(),
                epoch: selected.epoch(),
            },
            ObjectStoreSecretMaterial {
                access_key_id: selected.access_key_id().clone(),
                access_key_secret: selected.secret_access_key().clone(),
                session_token: Some(selected.session_token().clone()),
            },
            self.resources.object_store_provider_pool(),
        )
        .with_credential_expiration(expires_at);
        self.resources
            .access_resolver()
            .resolve_locations(
                selected.storage_access_domain_id(),
                locations,
                Some(object_store_access),
            )
            .map_err(file_error)
    }

    fn object_store_access_context_for_scheme(
        &self,
        scheme: FsScheme,
    ) -> Result<Option<ObjectStoreAccessContext<'_>>, ConnectorError> {
        if scheme == FsScheme::ObjectStore {
            let (endpoint_config, secret_material) = self.object_store_access_context()?;
            let IcebergStorageAccess::StaticObjectStore {
                credential_reference,
                ..
            } = self
                .storage_access
                .as_ref()
                .expect("checked by object-store access context")
            else {
                return Err(invalid(
                    "Iceberg object-store operation lacks an exact binding",
                ));
            };
            return Ok(Some(ObjectStoreAccessContext::new(
                endpoint_config,
                ObjectStoreCredentialProviderIdentity::Static(credential_reference.clone()),
                secret_material,
                self.resources.object_store_provider_pool(),
            )));
        }
        Ok(None)
    }

    fn object_store_access_context(
        &self,
    ) -> Result<(ObjectStoreEndpointConfig, ObjectStoreSecretMaterial), ConnectorError> {
        let IcebergStorageAccess::StaticObjectStore {
            endpoint_config,
            credential_reference,
            ..
        } = self.storage_access.as_ref().ok_or_else(|| {
            invalid("Iceberg filesystem operation has no admitted storage capability")
        })?
        else {
            return Err(invalid(
                "Iceberg object-store operation lacks an exact binding",
            ));
        };
        let resolver = self.credential_resolver.as_ref().ok_or_else(|| {
            invalid("Iceberg object-store operation has no role-local credential resolver")
        })?;
        let secret_material = resolver.resolve_object_store_static(credential_reference)?;
        Ok((endpoint_config.clone(), secret_material))
    }

    pub fn file_read_context(
        &self,
        cancellation: novarocks_fs::FileCancellation,
        deadline: std::time::Instant,
    ) -> Result<FileReadContext, ConnectorError> {
        self.storage_access.as_ref().ok_or_else(|| {
            invalid("Iceberg filesystem operation has no admitted storage capability")
        })?;
        Ok(FileReadContext {
            cancellation,
            deadline: Some(deadline),
            runtime: Arc::clone(self.resources.file_runtime()),
            task_spawner: Arc::clone(self.resources.file_task_spawner()),
        })
    }

    pub fn file_size(
        &self,
        path: &str,
        access: &FsAccessHandle,
        context: &FileReadContext,
    ) -> Result<u64, ConnectorError> {
        let file = access
            .bind_location(path, FileIdentity::new(path, 0, None))
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })?;
        let cancellation = context.cancellation.clone();
        context
            .runtime
            .block_on_u64(Box::pin(async move { file.stat(&cancellation).await }))
            .map_err(|error: FileError| {
                ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
                    .with_retryable_before_progress()
            })
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn credential_expiration(not_after_unix_ms: u64) -> Result<Instant, ConnectorError> {
    let now_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    let remaining_ms = not_after_unix_ms
        .checked_sub(now_unix_ms)
        .ok_or_else(|| invalid("Iceberg vended object-store credential has expired"))?;
    Instant::now()
        .checked_add(Duration::from_millis(remaining_ms))
        .ok_or_else(|| invalid("Iceberg vended object-store credential expiration is invalid"))
}

fn file_error(error: novarocks_fs::FileError) -> ConnectorError {
    invalid(error.to_string())
}

struct TestCredentialResolver {
    object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
}

impl IcebergStaticCredentialResolver for TestCredentialResolver {
    fn resolve_object_store_static(
        &self,
        reference: &StaticCredentialReference,
    ) -> Result<ObjectStoreSecretMaterial, ConnectorError> {
        let expected = StaticCredentialReference::try_new("iceberg-test-object-store", "test")
            .expect("static test credential reference");
        if reference != &expected {
            return Err(invalid(
                "test credential resolver received an unexpected reference",
            ));
        }
        self.object_store_config
            .as_ref()
            .map(novarocks_fs::ObjectStoreConfig::secret_material)
            .ok_or_else(|| invalid("test credential resolver has no object-store material"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use novarocks_fs::{FileCancellation, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
        CatalogProperties, CatalogProperty, CatalogVersion, ConnectorCancellation,
        ConnectorInstanceId, ConnectorProviderId, ConnectorStorageResolver, CredentialConsumerRole,
        ResolvedVendedS3Access, StorageAccessRequest,
    };

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct RejectingVendedResolver {
        calls: AtomicUsize,
    }

    impl ConnectorStorageResolver for RejectingVendedResolver {
        fn resolve_vended_s3(
            &self,
            request: &StorageAccessRequest,
        ) -> Result<ResolvedVendedS3Access, ConnectorError> {
            assert_eq!(request.owner().catalog_name().as_str(), "vended-test");
            assert_eq!(request.location(), "s3://warehouse/table/data.parquet");
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "test vended resolver denial",
            ))
        }
    }

    fn vended_catalog_properties() -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("vended-test").expect("catalog"),
                CatalogVersion::from_bytes([0x61; 32]),
            ),
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            vec![
                CatalogProperty::new("aws.s3.endpoint", "http://minio:9000")
                    .expect("endpoint property"),
            ],
            vec![
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::FrontendAndBackend,
                    CatalogCredentialMode::Vended,
                )
                .expect("vended binding"),
            ],
        )
        .expect("catalog properties")
    }

    #[test]
    fn requires_a_composition_owned_runtime() {
        let runtime = tokio::runtime::Runtime::new().expect("build explicit Tokio runtime");
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::clone(&file_runtime),
            Arc::clone(&task_spawner),
        );

        let context = binding
            .file_read_context(
                FileCancellation::new(),
                Instant::now() + Duration::from_secs(1),
            )
            .expect("build file read context");

        assert!(Arc::ptr_eq(&context.runtime, &file_runtime));
        assert!(Arc::ptr_eq(&context.task_spawner, &task_spawner));
    }

    #[test]
    fn object_store_writer_binding_never_discovers_credentials() {
        let runtime = tokio::runtime::Runtime::new().expect("build Tokio runtime");
        let config = novarocks_fs::ObjectStoreConfig {
            endpoint: "http://minio:9000".to_string(),
            access_key_id: novarocks_fs::SecretValue::new("test"),
            access_key_secret: novarocks_fs::SecretValue::new("test"),
            session_token: None,
            enable_path_style_access: Some(true),
            region: None,
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        };
        let binding = IcebergReadBinding::new(
            Some(config),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );

        let selected = binding
            .object_store_binding_for_location("s3://warehouse/staging/data.parquet")
            .expect("select object store")
            .expect("object store binding");
        assert_eq!(selected.bucket(), "warehouse");
        assert_eq!(selected.config().endpoint, "http://minio:9000");
        assert!(
            binding
                .object_store_binding_for_location("file:///tmp/data.parquet")
                .expect("local location")
                .is_none()
        );
    }

    #[test]
    fn vended_object_store_refuses_to_use_the_static_resolver_without_request_context() {
        let runtime = tokio::runtime::Runtime::new().expect("build Tokio runtime");
        let resources = FsAccessResources::new(
            Arc::new(
                novarocks_fs::ObjectStoreProviderPool::new(
                    novarocks_fs::ObjectStoreProviderPoolOptions::default(),
                )
                .expect("provider pool"),
            ),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let binding = IcebergReadBinding::from_catalog_properties(
            resources,
            Arc::new(TestCredentialResolver {
                object_store_config: None,
            }),
            &vended_catalog_properties(),
        )
        .expect("vended binding");
        assert!(binding.requires_request_storage_resolver());

        let error = binding
            .resolve_access("s3://warehouse/table/data.parquet")
            .expect_err("vended access without request resolver must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.message().contains("query storage resolver"));
    }

    #[test]
    fn vended_object_store_uses_the_request_resolver_instead_of_static_credentials() {
        let runtime = tokio::runtime::Runtime::new().expect("build Tokio runtime");
        let resources = FsAccessResources::new(
            Arc::new(
                novarocks_fs::ObjectStoreProviderPool::new(
                    novarocks_fs::ObjectStoreProviderPoolOptions::default(),
                )
                .expect("provider pool"),
            ),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let binding = IcebergReadBinding::from_catalog_properties(
            resources,
            Arc::new(TestCredentialResolver {
                object_store_config: None,
            }),
            &vended_catalog_properties(),
        )
        .expect("vended binding");
        let resolver = Arc::new(RejectingVendedResolver {
            calls: AtomicUsize::new(0),
        });
        let request = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            1024,
            2048,
        )
        .expect("request context")
        .with_storage_resolver(resolver.clone());

        let error = binding
            .for_request(request)
            .resolve_access("s3://warehouse/table/data.parquet")
            .expect_err("resolver denial must not fall back to static credentials");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    }
}
