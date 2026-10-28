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
    AcquisitionFailure, AuthorityCapabilityPath, AuthorityMaterial, AuthorityMaterialSource,
    FileIdentity, FileIoRuntime, FileReadContext, FileTaskSpawner, FsAccessHandle,
    FsAccessResolver, FsAccessResources, FsScheme, ObjectStoreAccessContext,
    ObjectStoreEndpointConfig, ObjectStoreSecretMaterial, RefreshExecutor, StorageAuthorityId,
};
use novarocks_spi::connector::{
    CatalogCredentialMode, CatalogCredentialPurpose, CatalogNonSecretProperty, CatalogProperties,
    CatalogStorageAccessDomainInput, CatalogUncredentialedStorageKind, ConnectorError,
    ConnectorErrorKind, ConnectorProviderId, ConnectorRequestContext, CredentialRenewalPath,
    NodeLocalCredentialReference, StaticCredentialReference, StorageAccessDomainId,
    StorageAccessRequest,
};

/// Role-local resolver for exact deployment-bound credential references.
///
/// The composition root owns the immutable registry. This connector only
/// receives a sealed resolver and never reads configuration or discovers
/// another role's credentials.
/// The catalog identity an execution node authenticates its own credential
/// acquisition with (CAD-1 D1).
///
/// Deliberately not an object-store credential: this material is exchanged for
/// data credentials and never signs a storage request. The two REST shapes are
/// the ones the role-local registry already models.
#[derive(Clone)]
pub enum IcebergRestAuthMaterial {
    Oauth2 {
        client_id: String,
        client_secret: novarocks_fs::SecretValue,
    },
    Bearer {
        token: novarocks_fs::SecretValue,
    },
}

impl std::fmt::Debug for IcebergRestAuthMaterial {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Oauth2 { client_id, .. } => formatter
                .debug_struct("IcebergRestAuthMaterial::Oauth2")
                .field("client_id", client_id)
                .field("client_secret", &"<redacted>")
                .finish(),
            Self::Bearer { .. } => formatter.write_str("IcebergRestAuthMaterial::Bearer(REDACTED)"),
        }
    }
}

pub trait IcebergCredentialResolver: Send + Sync {
    fn resolve_node_local_object_store(
        &self,
        _purpose: CatalogCredentialPurpose,
        _reference: &NodeLocalCredentialReference,
    ) -> Result<novarocks_fs::AliyunEcsRamRoleConfig, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "role-local resolver provides no node-local object-store source",
        ))
    }

    fn resolve_object_store_static(
        &self,
        reference: &StaticCredentialReference,
    ) -> Result<ObjectStoreSecretMaterial, ConnectorError>;

    fn resolve_object_store_metadata_static(
        &self,
        _reference: &StaticCredentialReference,
    ) -> Result<ObjectStoreSecretMaterial, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "role-local resolver does not provide Iceberg metadata credentials",
        ))
    }

    /// The execution node's own catalog identity, when this role has one.
    ///
    /// The default refusal is the honest answer for every role that does not:
    /// a coordinator must never resolve one, and an execution node whose
    /// deployment declared no vending binding has nothing to return. Reporting
    /// `Unsupported` here is what lets the authority say "this capability
    /// cannot renew" instead of failing vaguely later.
    fn resolve_data_credential_vending(
        &self,
        _reference: &StaticCredentialReference,
    ) -> Result<IcebergRestAuthMaterial, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "role-local resolver provides no data-credential-vending identity",
        ))
    }

    /// The coordinator's own catalog-control identity (L16).
    ///
    /// Only a coordinator holds one: it authenticates the control-plane
    /// session of a catalog generation. The default refusal is the answer of
    /// every role that does not. Unlike the vending identity, a catalog that
    /// declares this binding cannot proceed without it, so a caller must not
    /// read `Unsupported` as "no identity".
    fn resolve_catalog_control(
        &self,
        _reference: &StaticCredentialReference,
    ) -> Result<IcebergRestAuthMaterial, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "role-local resolver provides no catalog-control identity",
        ))
    }
}

#[derive(Clone)]
enum IcebergStorageAccess {
    NodeLocalObjectStore {
        owner: novarocks_spi::connector::CatalogHandle,
        access_domain: StorageAccessDomainId,
        endpoint_config: ObjectStoreEndpointConfig,
        credential_reference: NodeLocalCredentialReference,
    },
    StaticObjectStore {
        access_domain: StorageAccessDomainId,
        endpoint_config: ObjectStoreEndpointConfig,
        credential_reference: StaticCredentialReference,
    },
    VendedObjectStore {
        owner: novarocks_spi::connector::CatalogHandle,
        endpoint_config: ObjectStoreEndpointConfig,
        /// The frozen, non-secret catalog definition this node re-opens a
        /// control-plane client from when it acquires for itself (CAD-1 D1).
        catalog_definition: Arc<Vec<(String, String)>>,
        /// The role-local identity this node authenticates that acquisition
        /// with. Absent means this deployment declared none, which is the
        /// seeded-without-renewal shape rather than a failure (CAD-1 D11).
        vending_reference: Option<StaticCredentialReference>,
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
    #[cfg(novarocks_dev_controls)]
    observation_slot: Option<crate::test_observation::IcebergObservationSlot>,
    resources: FsAccessResources,
    range_service: Option<Arc<novarocks_fs::FileRangeService>>,
    credential_resolver: Option<Arc<dyn IcebergCredentialResolver>>,
    credential_purpose: CatalogCredentialPurpose,
    storage_access: Option<IcebergStorageAccess>,
    /// The coordinator identity the catalog definition names for its
    /// control-plane session. Absent when the definition declares none, and
    /// always absent on an execution node: the backend projection of a
    /// catalog definition never carries this binding.
    catalog_control_reference: Option<StaticCredentialReference>,
    /// Whether the catalog definition declares a `DataCredentialVending`
    /// identity for its execution nodes. A fact of the definition, not of this
    /// binding's purpose: the coordinator binds for metadata access yet must
    /// still know whether execution nodes can acquire (L25).
    data_credential_vending_declared: bool,
    request_context: Option<ConnectorRequestContext>,
    /// The bridge this role drives its own catalog calls on.
    ///
    /// Optional on purpose: only an execution node acquires data credentials
    /// under its own identity, so only an execution-node composition installs
    /// one. A binding without it cannot declare a renewing authority, which is
    /// the seeded-without-renewal shape rather than a silent downgrade.
    catalog_runtime: Option<crate::resources::IcebergCatalogRuntime>,
}

/// Provider-local credentials selected for one Iceberg object-store location.
/// This is process-local construction state, never a connector handle or
/// durable catalog property.
#[derive(Clone, Debug)]
pub struct IcebergObjectStoreBinding {
    bucket: String,
    endpoint_config: ObjectStoreEndpointConfig,
}

impl IcebergObjectStoreBinding {
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn endpoint_config(&self) -> &ObjectStoreEndpointConfig {
        &self.endpoint_config
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
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn test_storage_authority_registry(
        &self,
    ) -> &Arc<novarocks_fs::StorageAuthorityRegistry> {
        self.resources.storage_authority_registry()
    }

    #[cfg(novarocks_dev_controls)]
    pub fn with_test_observation(
        mut self,
        slot: crate::test_observation::IcebergObservationSlot,
    ) -> Self {
        self.observation_slot = Some(slot);
        self
    }
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn test_observation(
        &self,
    ) -> Option<&crate::test_observation::IcebergObservationSlot> {
        self.observation_slot.as_ref()
    }

    /// Builds an Iceberg filesystem binding from resources supplied by the
    /// process composition root.
    pub fn from_resources(resources: FsAccessResources) -> Self {
        Self {
            resources,
            range_service: None,
            credential_resolver: None,
            credential_purpose: CatalogCredentialPurpose::ObjectStoreData,
            storage_access: None,
            catalog_control_reference: None,
            data_credential_vending_declared: false,
            request_context: None,
            catalog_runtime: None,
            #[cfg(novarocks_dev_controls)]
            observation_slot: None,
        }
    }

    /// Build a role-local template that can be rebound only to an admitted
    /// immutable catalog definition.
    pub fn with_credential_resolver(
        resources: FsAccessResources,
        credential_resolver: Arc<dyn IcebergCredentialResolver>,
    ) -> Self {
        Self {
            resources,
            range_service: None,
            credential_resolver: Some(credential_resolver),
            credential_purpose: CatalogCredentialPurpose::ObjectStoreData,
            storage_access: None,
            catalog_control_reference: None,
            data_credential_vending_declared: false,
            request_context: None,
            catalog_runtime: None,
            #[cfg(novarocks_dev_controls)]
            observation_slot: None,
        }
    }

    /// Build the FE generation template used only for metadata, manifest and
    /// statistics observation. It cannot select an execution-data credential.
    pub fn with_metadata_credential_resolver(
        resources: FsAccessResources,
        credential_resolver: Arc<dyn IcebergCredentialResolver>,
    ) -> Self {
        Self {
            resources,
            range_service: None,
            credential_resolver: Some(credential_resolver),
            credential_purpose: CatalogCredentialPurpose::ObjectStoreMetadata,
            storage_access: None,
            catalog_control_reference: None,
            data_credential_vending_declared: false,
            request_context: None,
            catalog_runtime: None,
            #[cfg(novarocks_dev_controls)]
            observation_slot: None,
        }
    }

    /// Bind a catalog definition to its exact, role-local credential
    /// resolver. The catalog carrier contains only non-secret facts. A vended
    /// data binding records the attempt-owned acquisition mode without placing
    /// response credentials in this generation template.
    pub fn from_catalog_properties(
        resources: FsAccessResources,
        credential_resolver: Arc<dyn IcebergCredentialResolver>,
        properties: &CatalogProperties,
    ) -> Result<Self, ConnectorError> {
        Self::from_catalog_properties_for_purpose(
            resources,
            credential_resolver,
            CatalogCredentialPurpose::ObjectStoreData,
            properties,
        )
    }

    fn from_catalog_properties_for_purpose(
        resources: FsAccessResources,
        credential_resolver: Arc<dyn IcebergCredentialResolver>,
        credential_purpose: CatalogCredentialPurpose,
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
            .find(|binding| binding.purpose() == credential_purpose);

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
                    CatalogCredentialMode::NodeLocal(reference) => {
                        let domain_input = CatalogStorageAccessDomainInput::try_new(
                            provider_id,
                            properties.handle().catalog_name().clone(),
                            properties.config_format_version(),
                            non_secret_properties,
                            binding.clone(),
                            vec![],
                        )?;
                        // Resolve only the immutable definition here. Material
                        // acquisition starts at the actual signing boundary.
                        credential_resolver
                            .resolve_node_local_object_store(credential_purpose, reference)?;
                        IcebergStorageAccess::NodeLocalObjectStore {
                            owner: properties.handle().clone(),
                            access_domain: domain_input.derive_access_domain(),
                            endpoint_config,
                            credential_reference: reference.clone(),
                        }
                    }
                    CatalogCredentialMode::Vended => IcebergStorageAccess::VendedObjectStore {
                        owner: properties.handle().clone(),
                        endpoint_config,
                        catalog_definition: Arc::new(
                            properties
                                .execution_properties()
                                .iter()
                                .map(|property| {
                                    (property.key().to_string(), property.value().to_string())
                                })
                                .collect(),
                        ),
                        vending_reference: vending_reference(properties),
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
            range_service: None,
            credential_resolver: Some(credential_resolver),
            credential_purpose,
            storage_access: Some(storage_access),
            catalog_control_reference: catalog_control_reference(properties),
            data_credential_vending_declared: vending_reference(properties).is_some(),
            request_context: None,
            catalog_runtime: None,
            #[cfg(novarocks_dev_controls)]
            observation_slot: None,
        })
    }

    /// Rebind this role-local template to one immutable catalog definition.
    /// Templates without a credential resolver are deliberately unusable for
    /// catalog I/O: production composition must supply an exact resolver.
    pub fn bind_catalog(&self, properties: &CatalogProperties) -> Result<Self, ConnectorError> {
        let resolver = self.credential_resolver.clone().ok_or_else(|| {
            invalid("Iceberg catalog access binding has no role-local credential resolver")
        })?;
        let bound = Self::from_catalog_properties_for_purpose(
            self.resources.clone(),
            resolver,
            self.credential_purpose,
            properties,
        )?;
        Ok(Self {
            catalog_runtime: self.catalog_runtime.clone(),
            #[cfg(novarocks_dev_controls)]
            observation_slot: self.observation_slot.clone(),
            range_service: self.range_service.clone(),
            ..bound
        })
    }

    /// Whether the catalog this binding was bound for declares a
    /// `DataCredentialVending` identity for its execution nodes.
    pub(crate) fn declares_data_credential_vending(&self) -> bool {
        self.data_credential_vending_declared
    }

    /// Resolve the coordinator identity that authenticates this catalog's
    /// control-plane session (L16).
    ///
    /// `None` means the catalog definition declares no `CatalogControl`
    /// binding, and the coordinator's REST requests then carry no
    /// Authorization. A declared binding this role cannot resolve is an
    /// error, never "no identity": silently dropping a declared identity
    /// would send the session unauthenticated.
    pub fn resolve_catalog_control_identity(
        &self,
    ) -> Result<Option<IcebergRestAuthMaterial>, ConnectorError> {
        let Some(reference) = self.catalog_control_reference.as_ref() else {
            return Ok(None);
        };
        let resolver = self.credential_resolver.as_ref().ok_or_else(|| {
            invalid("Iceberg catalog-control binding has no role-local credential resolver")
        })?;
        resolver.resolve_catalog_control(reference).map(Some)
    }

    /// Install the bridge this role drives its own catalog calls on.
    ///
    /// Only an execution-node composition calls this. Without it a vended
    /// binding still works — it is seeded and cannot renew — so the absence is
    /// a deployment fact rather than a construction error (CAD-1 D11).
    pub fn with_catalog_runtime(
        mut self,
        catalog_runtime: crate::resources::IcebergCatalogRuntime,
    ) -> Self {
        self.catalog_runtime = Some(catalog_runtime);
        self
    }

    /// Install the BE-owned shared scan range scheduler. The service remains
    /// process-local and is never carried in a plan or connector handle.
    pub fn with_range_service(
        mut self,
        range_service: Arc<novarocks_fs::FileRangeService>,
    ) -> Self {
        self.range_service = Some(range_service);
        self
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
            .expect("build object-store provider pool"),
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
            range_service: None,
            credential_resolver: Some(resolver),
            credential_purpose: CatalogCredentialPurpose::ObjectStoreData,
            storage_access: Some(storage_access),
            catalog_control_reference: None,
            data_credential_vending_declared: false,
            request_context: None,
            catalog_runtime: None,
            #[cfg(novarocks_dev_controls)]
            observation_slot: None,
        }
    }

    /// Bind this provider template to one admitted request. This local view is
    /// carried only by an active reader or writer, never by a table or handle.
    pub(crate) fn request_context(&self) -> Option<&ConnectorRequestContext> {
        self.request_context.as_ref()
    }

    pub fn for_request(&self, request_context: ConnectorRequestContext) -> Self {
        Self {
            resources: self.resources.clone(),
            range_service: self.range_service.clone(),
            credential_resolver: self.credential_resolver.clone(),
            credential_purpose: self.credential_purpose,
            storage_access: self.storage_access.clone(),
            catalog_control_reference: self.catalog_control_reference.clone(),
            data_credential_vending_declared: self.data_credential_vending_declared,
            request_context: Some(request_context),
            catalog_runtime: self.catalog_runtime.clone(),
            #[cfg(novarocks_dev_controls)]
            observation_slot: self.observation_slot.clone(),
        }
    }

    pub(crate) fn operation_control(
        &self,
    ) -> Option<Arc<dyn novarocks_spi::connector::ConnectorOperationControl>> {
        self.request_context.as_ref().map(|request| {
            Arc::new(request.clone())
                as Arc<dyn novarocks_spi::connector::ConnectorOperationControl>
        })
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
        let endpoint_config = match self.storage_access.as_ref() {
            Some(IcebergStorageAccess::StaticObjectStore {
                endpoint_config, ..
            })
            | Some(IcebergStorageAccess::NodeLocalObjectStore {
                endpoint_config, ..
            }) => endpoint_config.clone(),
            _ => {
                return Err(
                    "Iceberg output location lacks a deployment-bound object-store source".into(),
                );
            }
        };
        Ok(Some(IcebergObjectStoreBinding {
            bucket: bucket.to_string(),
            endpoint_config,
        }))
    }

    pub fn is_object_store_location(&self, location: &str) -> Result<bool, String> {
        self.resources
            .access_resolver()
            .parse_location(location)
            .map(|location| location.scheme() == FsScheme::ObjectStore)
            .map_err(|error| format!("parse Iceberg output location: {error}"))
    }

    /// Borrow the original endpoint selection before any credential loader or
    /// operator is constructed. Bucket/user/path never define an HTTP domain.
    pub(crate) fn selected_io_dependency(
        &self,
        location: &str,
    ) -> Result<novarocks_spi::connector::SharedDependency, ConnectorError> {
        let endpoint = match self.storage_access.as_ref() {
            Some(IcebergStorageAccess::StaticObjectStore {
                endpoint_config, ..
            })
            | Some(IcebergStorageAccess::NodeLocalObjectStore {
                endpoint_config, ..
            })
            | Some(IcebergStorageAccess::VendedObjectStore {
                endpoint_config, ..
            }) => Some(endpoint_config),
            _ => None,
        };
        self.resources
            .access_resolver()
            .selected_dependency(location, endpoint)
            .map_err(file_error)
    }

    /// Same explicit binding and pool, awaited directly by metadata I/O.
    pub(crate) async fn resolve_access_for_request_async(
        &self,
        location: &str,
    ) -> Result<FsAccessHandle, ConnectorError> {
        let Some(request) = self.request_context.as_ref() else {
            return self.resolve_access(location);
        };
        let (domain, locations, access) = self.access_resolution(std::iter::once(location))?;
        let cancellation = novarocks_fs::FileCancellation::from_connector_request(request);
        self.resources
            .access_resolver()
            .resolve_locations_async(domain, locations, access, &cancellation)
            .await
            .map(|access| access.with_request_context(request.clone()))
            .map_err(file_error)
    }
    pub(crate) async fn admit_owned_stream(
        &self,
        location: &str,
    ) -> Result<Option<novarocks_spi::connector::ConnectorIoPermit>, ConnectorError> {
        match self.request_context.as_ref() {
            Some(request) => {
                request
                    .admit_selected_io(self.selected_io_dependency(location)?)
                    .await
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn admit_non_http_io(
        &self,
        location: &str,
    ) -> Result<Option<novarocks_spi::connector::ConnectorIoPermit>, ConnectorError> {
        let Some(request) = self.request_context.as_ref() else {
            return Ok(None);
        };
        if self
            .resources
            .access_resolver()
            .parse_location(location)
            .map_err(file_error)?
            .scheme()
            == FsScheme::ObjectStore
        {
            // The S3 signer admits the actual SDK-selected request before
            // credential acquisition, and retains it in the real response.
            return Ok(None);
        }
        request
            .admit_selected_io(self.selected_io_dependency(location)?)
            .await
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
        let (access_domain, locations, object_store_access) = self.access_resolution(locations)?;
        self.resources
            .access_resolver()
            .resolve_locations(access_domain, locations, object_store_access)
            .map_err(file_error)
    }

    /// Awaited [`Self::resolve_access_for_locations`] for a read that must
    /// not block: it waits for a shared object-store client under the read's
    /// own stop and deadline instead of parking a thread.
    pub async fn resolve_access_for_locations_async<I, S>(
        &self,
        locations: I,
        context: &FileReadContext,
    ) -> Result<FsAccessHandle, ConnectorError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let (access_domain, locations, object_store_access) = self.access_resolution(locations)?;
        let cancellation = context.bounded_cancellation();
        self.resources
            .access_resolver()
            .resolve_locations_async(access_domain, locations, object_store_access, &cancellation)
            .await
            .map(|access| match self.request_context.as_ref() {
                Some(request) => access.with_request_context(request.clone()),
                None => access,
            })
            .map_err(file_error)
    }

    /// Everything a resolution needs before it reaches the filesystem
    /// resolver: the access domain, the locations, and the object-store
    /// access this binding holds for them.
    fn access_resolution<I, S>(
        &self,
        locations: I,
    ) -> Result<
        (
            StorageAccessDomainId,
            Vec<String>,
            Option<ObjectStoreAccessContext<'_>>,
        ),
        ConnectorError,
    >
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
            return self.vended_access_resolution(locations, parsed.scheme());
        }
        let access_domain = self.access_domain_for_location(&parsed)?;
        let object_store_access = if matches!(
            self.storage_access,
            Some(IcebergStorageAccess::NodeLocalObjectStore { .. })
        ) {
            Some(self.node_local_object_store_access(&locations)?)
        } else {
            self.object_store_access_context_for_scheme(parsed.scheme())?
        };
        Ok((access_domain, locations, object_store_access))
    }

    fn access_domain_for_location(
        &self,
        location: &novarocks_fs::FsLocation,
    ) -> Result<StorageAccessDomainId, ConnectorError> {
        match self.storage_access.as_ref().ok_or_else(|| {
            invalid("Iceberg filesystem operation has no admitted storage capability")
        })? {
            IcebergStorageAccess::StaticObjectStore { access_domain, .. }
            | IcebergStorageAccess::NodeLocalObjectStore { access_domain, .. } => {
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

    fn vended_access_resolution(
        &self,
        locations: Vec<String>,
        scheme: FsScheme,
    ) -> Result<
        (
            StorageAccessDomainId,
            Vec<String>,
            Option<ObjectStoreAccessContext<'_>>,
        ),
        ConnectorError,
    > {
        if scheme != FsScheme::ObjectStore {
            return Err(invalid(
                "Iceberg vended object-store capability cannot resolve an uncredentialed location",
            ));
        }
        let IcebergStorageAccess::VendedObjectStore {
            owner,
            endpoint_config,
            catalog_definition,
            vending_reference,
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
        let resolved = locations
            .iter()
            .map(|location| {
                StorageAccessRequest::try_new(owner.clone(), location)
                    .and_then(|request| resolver.resolve_vended_s3(&request))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut resolved = resolved.into_iter();
        let selected = resolved
            .next()
            .ok_or_else(|| invalid("Iceberg filesystem locations are empty"))?;
        if resolved.any(|other| {
            other.storage_access_domain_id() != selected.storage_access_domain_id()
                || other.lease_id() != selected.lease_id()
                || other.epoch() != selected.epoch()
                || other.matched_prefix() != selected.matched_prefix()
                || other.renewal_path() != selected.renewal_path()
                || other.seed_grant() != selected.seed_grant()
                || other.provider().map(|source| source.identity())
                    != selected.provider().map(|source| source.identity())
        }) {
            return Err(invalid(
                "Iceberg vended filesystem locations require different credential selections",
            ));
        }
        // The authority is looked up, never minted here. The operator pool now
        // keys on the authority identity, so a resolution that built its own
        // owner would leave the resident operator signing with the one it
        // captured at construction: material installed later would never reach
        // it, and it would stop working once that first material expired. The
        // registry is what keeps the signer and the pool key the same object
        // (CAD-1 D0 with D10).
        // Whether this role can acquire is decided once, here, and it is a
        // fact about the role rather than about the read.
        //
        // Two ways to hold a renewal, and only one of them can apply. A
        // coordinator already holds the provider capability, because it planned
        // the query in this process; an execution node holds an identity and an
        // announced path instead. They key differently, so the two never share
        // an authority even when one process runs both roles (D0 with D9).
        // Neither is the seeded shape, which D11 keeps first-class rather than
        // degraded.
        if selected.seed().is_some() && selected.seed_grant().is_none() {
            return Err(invalid(
                "Iceberg vended seed has no original grant identity",
            ));
        }
        let in_process_provider = selected.provider().cloned();
        let renewal = match in_process_provider {
            Some(_) => None,
            None => {
                self.vended_renewal_identity(vending_reference.as_ref(), selected.renewal_path())?
            }
        };
        let capability = match (&in_process_provider, &renewal) {
            (Some(provider), _) => {
                if provider.identity().renewal_path() != selected.renewal_path()
                    || !provider
                        .identity()
                        .prefixes()
                        .contains(selected.matched_prefix())
                {
                    return Err(invalid(
                        "Iceberg in-process acquisition differs from its original selection",
                    ));
                }
                AuthorityCapabilityPath::InProcessProvider(provider.identity().clone())
            }
            (None, Some(renewal)) => renewal.capability_path(),
            (None, None) => AuthorityCapabilityPath::SeededWithoutRenewal {
                grant: selected.seed_grant(),
            },
        };
        let authority_id =
            StorageAuthorityId::new(owner.clone(), selected.matched_prefix().clone(), capability);
        let catalog_definition = Arc::clone(catalog_definition);
        let catalog_name = owner.catalog_name().as_str().to_string();
        // The factory runs only on first residence. Building the client on
        // every resolution would give a process-lived authority a fresh OAuth2
        // exchange per scan.
        // Expiry retires only the material. The original authority still
        // owns acquisition, even when no usable seed remains.
        let seed_expiration = selected
            .seed()
            .map(|seed| credential_expiration(seed.not_after_unix_ms()))
            .transpose()?
            .flatten();
        let authority = self.resources.storage_authority_registry().authority(
            &authority_id,
            Instant::now(),
            || {
                let source: Arc<dyn AuthorityMaterialSource> = match (in_process_provider, renewal)
                {
                    (Some(provider), _) => Arc::new(
                        crate::authority_source::IcebergAuthorityMaterialSource::new(
                            Arc::clone(provider.refresher()),
                            selected.matched_prefix().clone(),
                        ),
                    ),
                    (None, Some(renewal)) => Arc::new(
                        crate::authority_source::IcebergAuthorityMaterialSource::new(
                            Arc::new(renewal.into_refresher(catalog_name, catalog_definition)),
                            selected.matched_prefix().clone(),
                        ),
                    ),
                    (None, None) => Arc::new(SeededWithoutRenewal),
                };
                // This closure runs under the registry's unique miss. A hit
                // neither clones old secrets nor overwrites renewed material.
                let seed = selected
                    .seed()
                    .zip(seed_expiration)
                    .and_then(|(seed, expiration)| {
                        (expiration > Instant::now()).then(|| {
                            AuthorityMaterial::new(
                                seed.access_key_id().clone(),
                                seed.secret_access_key().clone(),
                                Some(seed.session_token().clone()),
                                expiration,
                            )
                        })
                    });
                (source, seed)
            },
        );
        let object_store_access = ObjectStoreAccessContext::for_authority(
            endpoint_config.clone(),
            authority,
            self.resources.object_store_provider_pool(),
        );
        Ok((
            selected.storage_access_domain_id(),
            locations,
            Some(object_store_access),
        ))
    }

    /// Decide, once per resolution, whether this role can acquire for itself.
    ///
    /// Three distinguishable answers, in the order they are ruled out:
    ///
    /// * the catalog declared no vending binding, or advertised no acquisition
    ///   address — nothing to acquire against;
    /// * this role has no such identity (`Unsupported` from its own registry),
    ///   which is what a coordinator always answers;
    /// * this role has one — and then it must also have the catalog bridge to
    ///   use it, or its composition is wrong and says so rather than quietly
    ///   downgrading to a seeded authority.
    fn vended_renewal_identity(
        &self,
        vending_reference: Option<&StaticCredentialReference>,
        renewal_path: Option<&CredentialRenewalPath>,
    ) -> Result<Option<VendedRenewalIdentity>, ConnectorError> {
        let (Some(reference), Some(announced)) = (vending_reference, renewal_path) else {
            return Ok(None);
        };
        let resolver = self.credential_resolver.as_ref().ok_or_else(|| {
            invalid("Iceberg vended binding has no role-local credential resolver")
        })?;
        let material = match resolver.resolve_data_credential_vending(reference) {
            Ok(material) => material,
            Err(error) if error.kind() == ConnectorErrorKind::Unsupported => return Ok(None),
            Err(error) => return Err(error),
        };
        let runtime = self.catalog_runtime.clone().ok_or_else(|| {
            invalid(
                "Iceberg role holds a data-credential-vending identity but no catalog runtime to use it",
            )
        })?;
        Ok(Some(VendedRenewalIdentity {
            reference: reference.clone(),
            path: crate::execution_authority::ExecutionNodeAcquisitionPath::project(announced)?,
            material,
            runtime,
        }))
    }

    fn node_local_object_store_access(
        &self,
        locations: &[String],
    ) -> Result<ObjectStoreAccessContext<'_>, ConnectorError> {
        let Some(IcebergStorageAccess::NodeLocalObjectStore {
            owner,
            endpoint_config,
            credential_reference,
            ..
        }) = &self.storage_access
        else {
            return Err(invalid("Iceberg node-local storage binding is absent"));
        };
        let resolver = self
            .credential_resolver
            .as_ref()
            .ok_or_else(|| invalid("Iceberg node-local binding has no role-local resolver"))?;
        let definition = resolver
            .resolve_node_local_object_store(self.credential_purpose, credential_reference)?;
        let scope = definition
            .scope_for_locations(locations)
            .map_err(file_error)?;
        let id = StorageAuthorityId::new(
            owner.clone(),
            scope,
            AuthorityCapabilityPath::NodeLocalProvider {
                purpose: self.credential_purpose,
                principal: credential_reference.clone(),
                provider: definition.clone(),
            },
        );
        let authority =
            self.resources
                .storage_authority_registry()
                .authority(&id, Instant::now(), || {
                    (
                        definition.material_source(self.resources.file_runtime().clone()),
                        None,
                    )
                });
        Ok(ObjectStoreAccessContext::for_authority(
            endpoint_config.clone(),
            authority,
            self.resources.object_store_provider_pool(),
        ))
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
            return Ok(Some(ObjectStoreAccessContext::for_static(
                endpoint_config,
                credential_reference.clone(),
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
        let secret_material = match self.credential_purpose {
            CatalogCredentialPurpose::ObjectStoreMetadata => {
                resolver.resolve_object_store_metadata_static(credential_reference)?
            }
            CatalogCredentialPurpose::ObjectStoreData => {
                resolver.resolve_object_store_static(credential_reference)?
            }
            CatalogCredentialPurpose::CatalogControl
            | CatalogCredentialPurpose::DataCredentialVending => {
                // Neither is a storage credential. The vending identity is the
                // thing an execution node exchanges for data credentials; it
                // never signs an object-store request itself.
                return Err(invalid(
                    "Iceberg filesystem binding cannot sign with a catalog identity",
                ));
            }
        };
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
        // A BE scan reads through the shared range service only for the one
        // execution source its request was bound to, whose operations every
        // request is admitted to before it starts.
        let range = match &self.range_service {
            None => None,
            Some(service) => {
                let (scope, operations) = self
                    .request_context
                    .as_ref()
                    .and_then(|request| request.range_scope().zip(request.source_operations()))
                    .ok_or_else(|| invalid("BE Iceberg scan requires an exact execution source"))?;
                let (query_high, query_low, attempt, fragment_high, fragment_low, node_id) =
                    scope.parts();
                let scope = novarocks_fs::FileRangeScope::try_new(
                    query_high,
                    query_low,
                    attempt,
                    fragment_high,
                    fragment_low,
                    node_id,
                )
                .map_err(|error| invalid(error.to_string()))?;
                Some(service.bind(scope, operations.clone()))
            }
        };
        Ok(FileReadContext {
            cancellation,
            deadline: Some(deadline),
            runtime: Arc::clone(self.resources.file_runtime()),
            task_spawner: Arc::clone(self.resources.file_task_spawner()),
            range,
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
            .map_err(crate::file_reader::map_file_error)
    }
}

/// The data-credential-vending binding this catalog declared, if any.
///
/// It is deliberately a separate purpose from the object-store binding: the
/// same catalog names one identity it signs storage requests with and another
/// it exchanges for storage credentials, and collapsing them would let a
/// coordinator's control identity reach an execution node (CAD-1 D9).
fn vending_reference(properties: &CatalogProperties) -> Option<StaticCredentialReference> {
    properties
        .credential_bindings()
        .iter()
        .find(|binding| binding.purpose() == CatalogCredentialPurpose::DataCredentialVending)
        .and_then(|binding| match binding.mode() {
            CatalogCredentialMode::Static(reference) => Some(reference.clone()),
            // Binding admission rejects NodeLocal for this purpose; neither
            // storage mode is a catalog credential-vending principal.
            CatalogCredentialMode::Vended | CatalogCredentialMode::NodeLocal(_) => None,
        })
}

/// The catalog-control binding this catalog declared, if any.
///
/// Its own purpose for the same reason as the vending binding: the
/// coordinator's control identity and an execution node's acquisition
/// identity are different principals (CAD-1 D9). The backend projection of a
/// catalog definition drops this binding, so an execution node never sees it.
fn catalog_control_reference(properties: &CatalogProperties) -> Option<StaticCredentialReference> {
    properties
        .credential_bindings()
        .iter()
        .find(|binding| binding.purpose() == CatalogCredentialPurpose::CatalogControl)
        .and_then(|binding| binding.static_reference().cloned())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

/// The acquisition capability a CAD-1 M1 consumer does not have yet.
///
/// Material still arrives through the coordinator's supply path, so an Iceberg
/// One role's ability to acquire data credentials for itself, resolved before
/// the authority identity is formed because the identity depends on it.
#[derive(Debug)]
struct VendedRenewalIdentity {
    reference: StaticCredentialReference,
    path: crate::execution_authority::ExecutionNodeAcquisitionPath,
    material: IcebergRestAuthMaterial,
    runtime: crate::resources::IcebergCatalogRuntime,
}

impl VendedRenewalIdentity {
    /// The authority identity this acquisition forms.
    ///
    /// Principal and path both belong in it: two roles acquiring the same scope
    /// along different paths, or under different principals, are different
    /// clients and must not share a provider-pool entry (CAD-1 D0, D7).
    fn capability_path(&self) -> AuthorityCapabilityPath {
        match &self.path {
            crate::execution_authority::ExecutionNodeAcquisitionPath::CredentialsEndpoint {
                routing,
            } => AuthorityCapabilityPath::CredentialsEndpoint {
                principal: self.reference.clone(),
                routing: routing.clone(),
            },
            crate::execution_authority::ExecutionNodeAcquisitionPath::LoadTableDelegation {
                routing,
                table,
                expected_table_uuid,
            } => AuthorityCapabilityPath::LoadTableDelegation {
                principal: self.reference.clone(),
                routing: routing.clone(),
                namespace: Arc::from(table.namespace().to_url_string().as_str()),
                table: Arc::from(table.name()),
                table_uuid: Arc::from(expected_table_uuid.to_string().as_str()),
            },
        }
    }

    fn into_refresher(
        self,
        catalog_name: String,
        catalog_definition: Arc<Vec<(String, String)>>,
    ) -> crate::execution_authority::ExecutionNodeCredentialsEndpointRefresher {
        crate::execution_authority::ExecutionNodeCredentialsEndpointRefresher::new(
            catalog_name,
            catalog_definition.as_ref().clone(),
            self.material,
            self.runtime,
            self.path,
        )
    }
}

/// vended authority is `AuthorityCapabilityPath::SeededWithoutRenewal` and
/// `StorageAuthority` structurally never reaches either method: it only
/// acquires along a capability path that can renew.
struct SeededWithoutRenewal;

impl AuthorityMaterialSource for SeededWithoutRenewal {
    fn acquire(&self, _deadline: Instant) -> Result<AuthorityMaterial, AcquisitionFailure> {
        Err(AcquisitionFailure::NoRenewalCapability)
    }
}

impl RefreshExecutor for SeededWithoutRenewal {
    fn execute(&self, _job: Box<dyn FnOnce() + Send + 'static>) {
        // Dropping a refresh job would strand whoever is waiting on it, so
        // this stays a loud contract violation rather than a silent no-op.
        debug_assert!(
            false,
            "a seeded storage authority must never schedule a refresh"
        );
    }
}

/// Translate the vended grant's wall-clock deadline into the monotonic instant
/// the authority judges usability against.
fn credential_expiration(not_after_unix_ms: u64) -> Result<Option<Instant>, ConnectorError> {
    let now_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    let Some(remaining_ms) = not_after_unix_ms
        .checked_sub(now_unix_ms)
        .filter(|remaining| *remaining > 0)
    else {
        return Ok(None);
    };
    Instant::now()
        .checked_add(Duration::from_millis(remaining_ms))
        .map(Some)
        .ok_or_else(|| invalid("Iceberg vended object-store credential expiration is invalid"))
}

fn file_error(error: novarocks_fs::FileError) -> ConnectorError {
    invalid(error.to_string())
}

struct TestCredentialResolver {
    object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
}

impl IcebergCredentialResolver for TestCredentialResolver {
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
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use novarocks_fs::{FileCancellation, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
        CatalogProperties, CatalogProperty, CatalogVersion, ConnectorInstanceId,
        ConnectorProviderId, ConnectorRangeScope, ConnectorStorageResolver, CredentialConsumerRole,
        ResolvedVendedS3Access, StorageAccessRequest,
    };

    #[test]
    fn be_range_service_requires_and_preserves_exact_scan_scope() {
        let runtime = tokio::runtime::Runtime::new().expect("scan runtime");
        let spawner: Arc<dyn novarocks_fs::FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        let range_service = novarocks_fs::FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            Arc::clone(&spawner),
            runtime.handle().clone(),
        );
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            spawner,
        )
        .with_range_service(Arc::clone(&range_service));
        let deadline = Instant::now() + Duration::from_secs(1);
        let request = ConnectorRequestContext::try_new(
            deadline,
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("connector request");
        let missing = binding
            .for_request(request.clone())
            .file_read_context(FileCancellation::from_connector_request(&request), deadline);
        assert!(
            missing.is_err(),
            "BE scan cannot invent a scheduling source"
        );
        let scope = ConnectorRangeScope::try_new(1, 2, 3, 4, 5, 6).expect("scope");
        let operations = novarocks_spi::connector::read_stack::ConnectorSourceOperations::new();
        let request = request.with_execution_source(scope, operations.clone());
        let context = binding
            .for_request(request.clone())
            .file_read_context(FileCancellation::from_connector_request(&request), deadline)
            .expect("scoped file context");
        let range = context
            .range
            .expect("BE scan reads through the range service");
        assert!(Arc::ptr_eq(range.service(), &range_service));
        assert_eq!(
            range.scope(),
            novarocks_fs::FileRangeScope::try_new(1, 2, 3, 4, 5, 6).unwrap()
        );
        let ticket = range.operations().admit(Arc::new(|| {})).expect("admitted");
        assert_eq!(
            operations.live_operations(),
            1,
            "file requests are admitted to the request's own source operations"
        );
        drop(ticket);
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

    const MULTI_VENDED_LOCATIONS: [&str; 2] = [
        "s3://warehouse/table/a/data.parquet",
        "s3://warehouse/table/b/data.parquet",
    ];

    #[derive(Clone, Copy, Debug)]
    enum SelectionDifference {
        Lease,
        Epoch,
        Domain,
        Prefix,
        RenewalPath,
        MissingRenewalPath,
    }

    /// Both values come through the real request resolver. Only the second
    /// location varies one actual selection dimension; no comparison is mocked.
    struct TwoLocationVendedResolver {
        difference: Option<SelectionDifference>,
        calls: AtomicUsize,
    }
    impl ConnectorStorageResolver for TwoLocationVendedResolver {
        fn resolve_vended_s3(
            &self,
            request: &StorageAccessRequest,
        ) -> Result<ResolvedVendedS3Access, ConnectorError> {
            assert_eq!(request.owner(), vended_catalog_properties().handle());
            assert!(MULTI_VENDED_LOCATIONS.contains(&request.location()));
            self.calls.fetch_add(1, Ordering::SeqCst);
            let difference = (request.location() == MULTI_VENDED_LOCATIONS[1])
                .then_some(self.difference)
                .flatten();
            let mut lease = [0x21; 16];
            let mut epoch = 1;
            let mut domain = [0x31; 32];
            let mut prefix = "s3://warehouse/table";
            let mut path = Some(endpoint_path());
            match difference {
                Some(SelectionDifference::Lease) => lease = [0x22; 16],
                Some(SelectionDifference::Epoch) => epoch = 2,
                Some(SelectionDifference::Domain) => domain = [0x32; 32],
                Some(SelectionDifference::Prefix) => prefix = "s3://warehouse/table/b",
                Some(SelectionDifference::RenewalPath) => {
                    path = Some(CredentialRenewalPath::CredentialsEndpoint(routing(
                        "https://rest/v2/credentials",
                    )))
                }
                Some(SelectionDifference::MissingRenewalPath) => path = None,
                None => {}
            }
            Ok(ResolvedVendedS3Access::new(
                StorageAccessDomainId::from_bytes(domain),
                novarocks_spi::connector::CredentialLeaseId::try_from_bytes(lease).unwrap(),
                epoch,
                novarocks_spi::connector::StorageCredentialScopePrefix::try_from_normalized(prefix)
                    .unwrap(),
                path,
                None,
                None,
            ))
        }
    }

    struct ExpiredSeedResolver {
        grant: novarocks_spi::connector::VendedS3SeedGrantId,
    }

    impl ConnectorStorageResolver for ExpiredSeedResolver {
        fn resolve_vended_s3(
            &self,
            request: &StorageAccessRequest,
        ) -> Result<ResolvedVendedS3Access, ConnectorError> {
            let original = TwoLocationVendedResolver {
                difference: None,
                calls: AtomicUsize::new(0),
            }
            .resolve_vended_s3(request)?;
            Ok(ResolvedVendedS3Access::new(
                original.storage_access_domain_id(),
                original.lease_id(),
                original.epoch(),
                original.matched_prefix().clone(),
                original.renewal_path().cloned(),
                Some(self.grant),
                Some(novarocks_spi::connector::VendedS3SeedMaterial::new(
                    1,
                    novarocks_fs::SecretValue::new("expired-access"),
                    novarocks_fs::SecretValue::new("expired-secret"),
                    novarocks_fs::SecretValue::new("expired-token"),
                )),
            ))
        }
    }

    #[test]
    fn expired_seed_resolution_keeps_the_actual_renewing_authority_unseeded() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let binding = vending_binding(
            &runtime,
            Some(IcebergRestAuthMaterial::Bearer {
                token: novarocks_fs::SecretValue::new("node-token"),
            }),
            true,
        )
        .for_request(
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(1),
                novarocks_spi::connector::ConnectorStopOwner::new().view(),
                1024,
                2048,
            )
            .unwrap()
            .with_storage_resolver(Arc::new(ExpiredSeedResolver {
                grant: novarocks_spi::connector::VendedS3SeedGrantId::new(),
            })),
        );
        binding
            .resolve_access_for_locations(MULTI_VENDED_LOCATIONS)
            .unwrap();
        let id = StorageAuthorityId::new(
            vended_catalog_properties().handle().clone(),
            novarocks_spi::connector::StorageCredentialScopePrefix::try_from_normalized(
                "s3://warehouse/table",
            )
            .unwrap(),
            binding
                .vended_renewal_identity(
                    Some(&StaticCredentialReference::try_new("executor", "v1").unwrap()),
                    Some(&endpoint_path()),
                )
                .unwrap()
                .unwrap()
                .capability_path(),
        );
        let registry = binding.resources.storage_authority_registry();
        assert!(
            registry.is_resident(&id),
            "the original renewal path was retained"
        );
        let authority = registry.authority(&id, Instant::now(), || {
            panic!("the actual resolution already installed this exact authority")
        });
        assert!(authority.id().capability().can_renew());
        assert_eq!(authority.metrics().refreshes_applied, 0);
        assert_eq!(registry.metrics().misses, 1);
        // No signing or acquisition is requested here. The FS component test
        // drives a real queued renewal and verifies the subsequent resident hit.
    }

    #[test]
    fn vended_multi_location_refuses_each_different_original_selection() {
        let runtime = tokio::runtime::Runtime::new().expect("binding runtime");
        for difference in [
            SelectionDifference::Lease,
            SelectionDifference::Epoch,
            SelectionDifference::Domain,
            SelectionDifference::Prefix,
            SelectionDifference::RenewalPath,
            SelectionDifference::MissingRenewalPath,
        ] {
            let resolver = Arc::new(TwoLocationVendedResolver {
                difference: Some(difference),
                calls: AtomicUsize::new(0),
            });
            // Reuse the real catalog binding whose role has no independent
            // vending identity. A selection refusal must precede authority or
            // operator admission, rather than fail later for missing material.
            let binding = vending_binding(&runtime, None, false).for_request(
                ConnectorRequestContext::try_new(
                    Instant::now() + Duration::from_secs(1),
                    novarocks_spi::connector::ConnectorStopOwner::new().view(),
                    1024,
                    2048,
                )
                .unwrap()
                .with_storage_resolver(resolver.clone()),
            );
            let registry = Arc::clone(binding.resources.storage_authority_registry());
            let error = binding
                .resolve_access_for_locations(MULTI_VENDED_LOCATIONS)
                .expect_err("different selections cannot share the first location's operator");
            assert_eq!(
                error.kind(),
                ConnectorErrorKind::InvalidRequest,
                "{difference:?}"
            );
            assert_eq!(
                error.message(),
                "Iceberg vended filesystem locations require different credential selections",
                "{difference:?}: refusal must be the actual multi-location selection gate"
            );
            assert_eq!(resolver.calls.load(Ordering::SeqCst), 2, "{difference:?}");
            assert_eq!(registry.metrics().resident, 0, "{difference:?}");
            assert_eq!(registry.metrics().misses, 0, "{difference:?}");
        }
    }

    #[test]
    fn vended_multi_location_same_selection_resolves_both_paths_without_io() {
        let runtime = tokio::runtime::Runtime::new().expect("binding runtime");
        let resolver = Arc::new(TwoLocationVendedResolver {
            difference: None,
            calls: AtomicUsize::new(0),
        });
        let binding = vending_binding(&runtime, None, false).for_request(
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(1),
                novarocks_spi::connector::ConnectorStopOwner::new().view(),
                1024,
                2048,
            )
            .unwrap()
            .with_storage_resolver(resolver.clone()),
        );
        // Resolution builds the ordinary operator. No read, stat, signing, or
        // acquisition is requested; no secret or network fixture is needed.
        let access = binding
            .resolve_access_for_locations(MULTI_VENDED_LOCATIONS)
            .expect("the original same-selection path remains accepted");
        assert_eq!(
            access.access_domain(),
            StorageAccessDomainId::from_bytes([0x31; 32])
        );
        assert_eq!(access.scheme(), FsScheme::ObjectStore);
        assert_eq!(access.authority(), Some("warehouse"));
        assert_eq!(access.paths().len(), 2);
        for (actual, expected) in access.paths().iter().zip(MULTI_VENDED_LOCATIONS) {
            assert_eq!(actual.location().original(), expected);
        }
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            binding
                .resources
                .storage_authority_registry()
                .metrics()
                .resident,
            1
        );
        assert_eq!(
            binding
                .resources
                .storage_authority_registry()
                .metrics()
                .misses,
            1
        );
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
                    CredentialConsumerRole::Backend,
                    CatalogCredentialMode::Vended,
                )
                .expect("vended binding"),
            ],
        )
        .expect("catalog properties")
    }

    /// Catalog properties that also declare this node's own catalog identity.
    fn vending_catalog_properties() -> CatalogProperties {
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
                CatalogProperty::new("iceberg.catalog.type", "rest").expect("type property"),
                CatalogProperty::new("uri", "http://rest:8181/catalog").expect("uri property"),
            ],
            vec![
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::Backend,
                    CatalogCredentialMode::Vended,
                )
                .expect("vended binding"),
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::DataCredentialVending,
                    CredentialConsumerRole::Backend,
                    CatalogCredentialMode::Static(
                        StaticCredentialReference::try_new("executor", "v1")
                            .expect("vending reference"),
                    ),
                )
                .expect("vending binding"),
            ],
        )
        .expect("catalog properties")
    }

    fn routing(url: &str) -> novarocks_spi::connector::CredentialRoutingContext {
        novarocks_spi::connector::CredentialRoutingContext::try_new(
            url,
            [(Arc::from("x-tenant"), Arc::from("t1"))],
        )
        .expect("routing context")
    }

    fn endpoint_path() -> CredentialRenewalPath {
        CredentialRenewalPath::CredentialsEndpoint(routing("https://rest/v1/credentials"))
    }

    /// A resolver that answers the vending question the way one exact role
    /// would: either it holds such an identity, or it structurally has none.
    struct VendingResolver {
        material: Option<IcebergRestAuthMaterial>,
    }

    impl IcebergCredentialResolver for VendingResolver {
        fn resolve_object_store_static(
            &self,
            _reference: &StaticCredentialReference,
        ) -> Result<ObjectStoreSecretMaterial, ConnectorError> {
            Err(invalid("this test resolver vends no object-store material"))
        }

        fn resolve_data_credential_vending(
            &self,
            reference: &StaticCredentialReference,
        ) -> Result<IcebergRestAuthMaterial, ConnectorError> {
            assert_eq!(reference.name(), "executor");
            match &self.material {
                Some(IcebergRestAuthMaterial::Bearer { token }) => {
                    Ok(IcebergRestAuthMaterial::Bearer {
                        token: token.clone(),
                    })
                }
                Some(_) => unreachable!("test resolver only models the bearer shape"),
                None => Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "role-local resolver provides no data-credential-vending identity",
                )),
            }
        }
    }

    fn vending_binding(
        runtime: &tokio::runtime::Runtime,
        material: Option<IcebergRestAuthMaterial>,
        with_catalog_runtime: bool,
    ) -> IcebergReadBinding {
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
            Arc::new(VendingResolver { material }),
            &vending_catalog_properties(),
        )
        .expect("vending binding");
        if with_catalog_runtime {
            binding.with_catalog_runtime(crate::resources::IcebergCatalogRuntime::new(
                runtime.handle().clone(),
            ))
        } else {
            binding
        }
    }

    #[test]
    fn a_role_that_holds_no_vending_identity_is_seeded_rather_than_broken() {
        // CAD-1 D11. A coordinator answers `Unsupported` here on every read, so
        // treating that as a failure would break every coordinator-side vended
        // read rather than describing the role honestly.
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = vending_binding(&runtime, None, false);

        let renewal = binding
            .vended_renewal_identity(
                Some(&StaticCredentialReference::try_new("executor", "v1").expect("reference")),
                Some(&endpoint_path()),
            )
            .expect("an absent identity is an answer");
        assert!(renewal.is_none());
    }

    #[test]
    fn an_acquisition_needs_both_a_declared_identity_and_an_advertised_address() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = vending_binding(
            &runtime,
            Some(IcebergRestAuthMaterial::Bearer {
                token: novarocks_fs::SecretValue::new("node-token"),
            }),
            true,
        );
        let reference = StaticCredentialReference::try_new("executor", "v1").expect("reference");

        // Neither half alone can acquire, and neither is an error.
        assert!(
            binding
                .vended_renewal_identity(Some(&reference), None)
                .expect("no address is an answer")
                .is_none()
        );
        assert!(
            binding
                .vended_renewal_identity(None, Some(&endpoint_path()))
                .expect("no declared identity is an answer")
                .is_none()
        );

        let renewal = binding
            .vended_renewal_identity(Some(&reference), Some(&endpoint_path()))
            .expect("resolved identity")
            .expect("both halves present");
        assert_eq!(renewal.reference, reference);
        assert!(matches!(
            renewal.capability_path(),
            AuthorityCapabilityPath::CredentialsEndpoint { routing: route, .. }
                if route == routing("https://rest/v1/credentials")
        ));
    }

    #[test]
    fn a_vending_identity_without_a_catalog_bridge_is_a_composition_error() {
        // D12's deployment consequence caught at its own boundary: a role that
        // was given an identity but no way to use it must say so, not silently
        // fall back to the seeded shape and expire mid-scan.
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = vending_binding(
            &runtime,
            Some(IcebergRestAuthMaterial::Bearer {
                token: novarocks_fs::SecretValue::new("node-token"),
            }),
            false,
        );

        let error = binding
            .vended_renewal_identity(
                Some(&StaticCredentialReference::try_new("executor", "v1").expect("reference")),
                Some(&endpoint_path()),
            )
            .expect_err("a usable identity with no bridge cannot be silently downgraded");
        assert!(error.message().contains("catalog runtime"));
    }

    #[test]
    fn a_catalog_that_vends_only_through_load_table_is_a_renewal_path_too() {
        // CAD-1 acceptance 9. Unity Catalog OSS does not implement the
        // credentials endpoint at all, so a design that only knew that path
        // would leave such a deployment unable to renew anything.
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = vending_binding(
            &runtime,
            Some(IcebergRestAuthMaterial::Bearer {
                token: novarocks_fs::SecretValue::new("node-token"),
            }),
            true,
        );
        let reference = StaticCredentialReference::try_new("executor", "v1").expect("reference");
        let announced = CredentialRenewalPath::LoadTableDelegation {
            routing: routing("https://rest/v1/namespaces/sales%1Feu/tables/orders"),
            table: novarocks_spi::connector::CredentialLoadTableDelegation::try_new(
                vec![Arc::from("sales"), Arc::from("eu")],
                Arc::from("orders"),
                Arc::from("8f1d0c6e-0000-4000-8000-000000000001"),
            )
            .expect("delegation"),
        };

        let renewal = binding
            .vended_renewal_identity(Some(&reference), Some(&announced))
            .expect("resolved identity")
            .expect("a load-table catalog can still renew");
        match renewal.capability_path() {
            AuthorityCapabilityPath::LoadTableDelegation {
                principal,
                routing: route,
                namespace,
                table,
                table_uuid,
            } => {
                assert_eq!(principal, reference);
                assert_eq!(
                    route,
                    routing("https://rest/v1/namespaces/sales%1Feu/tables/orders")
                );
                // The canonical Iceberg multi-level separator, not a dot: a
                // level may contain a dot, and a key that joined on one would
                // make two different namespaces the same authority.
                assert_eq!(namespace.as_ref(), "sales\u{1f}eu");
                assert_eq!(table.as_ref(), "orders");
                assert_eq!(table_uuid.as_ref(), "8f1d0c6e-0000-4000-8000-000000000001");
            }
            other => panic!("expected a load-table capability, got {other:?}"),
        }
    }

    #[test]
    fn an_unparseable_announcement_is_refused_rather_than_degraded() {
        // The identity check is the only thing standing between a response and
        // material installed for the wrong table, so an announcement this node
        // cannot parse must stop the acquisition rather than weaken it.
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = vending_binding(
            &runtime,
            Some(IcebergRestAuthMaterial::Bearer {
                token: novarocks_fs::SecretValue::new("node-token"),
            }),
            true,
        );
        let announced = CredentialRenewalPath::LoadTableDelegation {
            routing: routing("https://rest/v1/namespaces/sales/tables/orders"),
            table: novarocks_spi::connector::CredentialLoadTableDelegation::try_new(
                vec![Arc::from("sales")],
                Arc::from("orders"),
                Arc::from("not-a-uuid"),
            )
            .expect("delegation"),
        };

        let error = binding
            .vended_renewal_identity(
                Some(&StaticCredentialReference::try_new("executor", "v1").expect("reference")),
                Some(&announced),
            )
            .expect_err("an unparseable table identity cannot be acquired against");
        assert!(error.message().contains("uuid"));
    }

    #[test]
    fn the_two_capability_paths_are_different_authority_identities() {
        // CAD-1 D0: material is part of the storage client's identity, so a
        // seeded authority and an acquiring one must not share a pool entry --
        // otherwise a renewing node would sign with material it cannot replace.
        let owner = CatalogHandle::new(
            ConnectorInstanceId::parse("vended-test").expect("catalog"),
            CatalogVersion::from_bytes([0x61; 32]),
        );
        let scope = novarocks_spi::connector::StorageCredentialScopePrefix::try_from_normalized(
            "s3://warehouse/table",
        )
        .expect("prefix");
        let seeded = StorageAuthorityId::new(
            owner.clone(),
            scope.clone(),
            AuthorityCapabilityPath::SeededWithoutRenewal { grant: None },
        );
        let acquiring = StorageAuthorityId::new(
            owner,
            scope,
            AuthorityCapabilityPath::CredentialsEndpoint {
                principal: StaticCredentialReference::try_new("executor", "v1").expect("reference"),
                routing: routing("https://rest/v1/credentials"),
            },
        );
        assert_ne!(seeded, acquiring);
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
        assert_eq!(selected.endpoint_config().endpoint, "http://minio:9000");
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
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
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

#[cfg(test)]
mod catalog_control_tests {
    use std::sync::Arc;

    use super::*;
    use novarocks_fs::{TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
        CatalogProperties, CatalogProperty, CatalogVersion, ConnectorInstanceId,
        ConnectorProviderId, CredentialConsumerRole,
    };

    fn control_reference() -> StaticCredentialReference {
        StaticCredentialReference::try_new("coordinator", "v1").expect("control reference")
    }

    fn rest_catalog_properties(with_control: bool) -> CatalogProperties {
        let mut bindings = vec![
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreData,
                CredentialConsumerRole::Backend,
                CatalogCredentialMode::Vended,
            )
            .expect("vended binding"),
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::DataCredentialVending,
                CredentialConsumerRole::Backend,
                CatalogCredentialMode::Static(
                    StaticCredentialReference::try_new("executor", "v1").expect("reference"),
                ),
            )
            .expect("vending binding"),
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreMetadata,
                CredentialConsumerRole::Frontend,
                CatalogCredentialMode::Static(
                    StaticCredentialReference::try_new("metadata", "v1").expect("reference"),
                ),
            )
            .expect("metadata binding"),
        ];
        if with_control {
            bindings.push(
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::CatalogControl,
                    CredentialConsumerRole::Frontend,
                    CatalogCredentialMode::Static(control_reference()),
                )
                .expect("control binding"),
            );
        }
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("control-test").expect("catalog"),
                CatalogVersion::from_bytes([0x43; 32]),
            ),
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            vec![
                CatalogProperty::new("aws.s3.endpoint", "http://minio:9000").expect("endpoint"),
                CatalogProperty::new("iceberg.catalog.type", "rest").expect("type"),
                CatalogProperty::new("uri", "http://rest:8181/catalog").expect("uri"),
            ],
            bindings,
        )
        .expect("catalog properties")
    }

    /// A resolver modelling one exact role: a coordinator holds the control
    /// identity, an execution node structurally has none.
    struct ControlResolver {
        material: Option<IcebergRestAuthMaterial>,
    }

    impl IcebergCredentialResolver for ControlResolver {
        fn resolve_object_store_static(
            &self,
            _reference: &StaticCredentialReference,
        ) -> Result<ObjectStoreSecretMaterial, ConnectorError> {
            Err(invalid("this test resolver vends no object-store material"))
        }

        fn resolve_catalog_control(
            &self,
            reference: &StaticCredentialReference,
        ) -> Result<IcebergRestAuthMaterial, ConnectorError> {
            assert_eq!(reference, &control_reference());
            self.material.clone().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "role-local resolver provides no catalog-control identity",
                )
            })
        }
    }

    struct DefaultResolver;

    impl IcebergCredentialResolver for DefaultResolver {
        fn resolve_object_store_static(
            &self,
            _reference: &StaticCredentialReference,
        ) -> Result<ObjectStoreSecretMaterial, ConnectorError> {
            Err(invalid("this test resolver vends no object-store material"))
        }
    }

    fn resources(runtime: &tokio::runtime::Runtime) -> FsAccessResources {
        FsAccessResources::new(
            Arc::new(
                novarocks_fs::ObjectStoreProviderPool::new(
                    novarocks_fs::ObjectStoreProviderPoolOptions::default(),
                )
                .expect("provider pool"),
            ),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        )
    }

    #[test]
    fn the_coordinator_metadata_binding_resolves_its_declared_control_identity() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        // The coordinator's generation binds its metadata template to the
        // admitted definition; the control identity travels with that bind.
        let template = IcebergReadBinding::with_metadata_credential_resolver(
            resources(&runtime),
            Arc::new(ControlResolver {
                material: Some(IcebergRestAuthMaterial::Bearer {
                    token: novarocks_fs::SecretValue::new("control-token"),
                }),
            }),
        );
        let bound = template
            .bind_catalog(&rest_catalog_properties(true))
            .expect("bound coordinator binding");

        match bound
            .resolve_catalog_control_identity()
            .expect("declared identity resolves")
        {
            Some(IcebergRestAuthMaterial::Bearer { token }) => {
                assert_eq!(token.expose_secret(), "control-token");
            }
            other => panic!("unexpected control identity: {other:?}"),
        }
    }

    /// The coordinator binds for metadata access, yet still learns that its
    /// execution nodes hold a vending identity: route admission (L25) reads
    /// the definition, not the purpose this role bound for.
    #[test]
    fn the_coordinator_metadata_binding_reports_the_declared_vending_identity() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let template = IcebergReadBinding::with_metadata_credential_resolver(
            resources(&runtime),
            Arc::new(DefaultResolver),
        );
        assert!(!template.declares_data_credential_vending());
        let bound = template
            .bind_catalog(&rest_catalog_properties(false))
            .expect("bound coordinator binding");
        assert!(bound.declares_data_credential_vending());
    }

    #[test]
    fn a_definition_without_a_control_binding_has_no_coordinator_identity() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = IcebergReadBinding::from_catalog_properties(
            resources(&runtime),
            Arc::new(ControlResolver {
                material: Some(IcebergRestAuthMaterial::Bearer {
                    token: novarocks_fs::SecretValue::new("unused"),
                }),
            }),
            &rest_catalog_properties(false),
        )
        .expect("binding");

        assert!(
            binding
                .resolve_catalog_control_identity()
                .expect("an absent binding is an answer")
                .is_none()
        );
    }

    #[test]
    fn a_declared_control_identity_the_role_cannot_resolve_is_an_error() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        for resolver in [
            Arc::new(ControlResolver { material: None }) as Arc<dyn IcebergCredentialResolver>,
            Arc::new(DefaultResolver),
        ] {
            let binding = IcebergReadBinding::from_catalog_properties(
                resources(&runtime),
                resolver,
                &rest_catalog_properties(true),
            )
            .expect("binding");
            let error = binding
                .resolve_catalog_control_identity()
                .expect_err("a declared identity is never silently dropped");
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        }
    }

    #[test]
    fn the_execution_projection_never_carries_the_control_identity() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let projected = rest_catalog_properties(true)
            .backend_execution_projection()
            .expect("backend projection");
        let binding = IcebergReadBinding::from_catalog_properties(
            resources(&runtime),
            Arc::new(ControlResolver {
                material: Some(IcebergRestAuthMaterial::Bearer {
                    token: novarocks_fs::SecretValue::new("must-not-reach-backend"),
                }),
            }),
            &projected,
        )
        .expect("backend binding");

        assert!(
            binding
                .resolve_catalog_control_identity()
                .expect("projection has no control binding")
                .is_none()
        );
    }
}

#[cfg(test)]
mod node_local_tests {
    use super::*;
    use novarocks_fs::{
        AliyunEcsRamRoleConfig, ObjectStoreProviderPool, TokioFileIoRuntime, TokioFileTaskSpawner,
    };
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogHandle, CatalogProperty, CatalogVersion,
        ConnectorInstanceId, CredentialConsumerRole,
    };

    struct NodeLocalResolver;
    impl IcebergCredentialResolver for NodeLocalResolver {
        fn resolve_object_store_static(
            &self,
            _: &StaticCredentialReference,
        ) -> Result<ObjectStoreSecretMaterial, ConnectorError> {
            panic!("node-local mode cannot export a static snapshot")
        }
        fn resolve_node_local_object_store(
            &self,
            purpose: CatalogCredentialPurpose,
            reference: &NodeLocalCredentialReference,
        ) -> Result<AliyunEcsRamRoleConfig, ConnectorError> {
            assert_eq!(purpose, CatalogCredentialPurpose::ObjectStoreData);
            assert_eq!(reference.name(), "ecs-data");
            AliyunEcsRamRoleConfig::try_new("test-role", vec!["s3://bucket/allowed/".into()])
                .map_err(file_error)
        }
    }
    fn binding(runtime: &tokio::runtime::Runtime) -> IcebergReadBinding {
        let properties = CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("node-local-test").unwrap(),
                CatalogVersion::from_bytes([0x49; 32]),
            ),
            ConnectorProviderId::parse("iceberg").unwrap(),
            3,
            vec![
                CatalogProperty::new(
                    "aws.s3.endpoint",
                    "https://s3.oss-cn-beijing-internal.aliyuncs.com",
                )
                .unwrap(),
                CatalogProperty::new("aws.s3.enable_path_style_access", "false").unwrap(),
                CatalogProperty::new("aws.s3.region", "cn-beijing").unwrap(),
            ],
            vec![
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::Backend,
                    CatalogCredentialMode::NodeLocal(
                        NodeLocalCredentialReference::try_new("ecs-data", "v1").unwrap(),
                    ),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let resources = FsAccessResources::new(
            Arc::new(
                ObjectStoreProviderPool::new(
                    novarocks_fs::ObjectStoreProviderPoolOptions::default(),
                )
                .unwrap(),
            ),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        IcebergReadBinding::from_catalog_properties(
            resources,
            Arc::new(NodeLocalResolver),
            &properties,
        )
        .unwrap()
    }

    #[test]
    fn node_local_binding_is_lazy_and_reuses_its_original_authority() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let binding = binding(&runtime);
        let facts = binding
            .object_store_binding_for_location("s3://bucket/allowed/data")
            .unwrap()
            .unwrap();
        assert_eq!(facts.bucket(), "bucket");
        assert_eq!(
            facts.endpoint_config().region.as_deref(),
            Some("cn-beijing")
        );
        // Both operations prepare handles only. An eager metadata request on
        // this non-ECS test machine would fail rather than preparing a handle.
        binding.resolve_access("s3://bucket/allowed/one").unwrap();
        binding.resolve_access("s3a://bucket/allowed/two").unwrap();
        let metrics = binding.resources.storage_authority_registry().metrics();
        assert_eq!(metrics.misses, 1);
        assert_eq!(metrics.hits, 1);
    }

    #[test]
    fn node_local_binding_refuses_a_sibling_before_operator_or_metadata_access() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let binding = binding(&runtime);
        assert!(
            binding
                .resolve_access("s3://bucket/allowed-sibling/data")
                .is_err()
        );
        assert_eq!(
            binding
                .resources
                .storage_authority_registry()
                .metrics()
                .misses,
            0
        );
    }
}
