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

//! Provider-side construction for an unpublished Iceberg control generation.
//!
//! Server composition owns the resources passed here.  The eventual SPI
//! factory uses this value to construct every capability before Frontend
//! publishes the returned binding; neither a Core catalog registry nor a
//! process-global credential/runtime lookup participates in this step.

use crate::PROVIDER_ID;
use crate::catalog_config::parse_catalog_configuration_with_object_store_binding;
use crate::catalog_control::IcebergCatalogControlState;
use crate::catalog_control::cleanup_maintenance::IcebergCleanupMaintenanceAdapter;
use crate::catalog_control::data_mutation::IcebergDataMutationAdapter;
use crate::catalog_control::metadata_maintenance::IcebergMetadataMaintenanceAdapter;
use crate::catalog_control::staged_create::IcebergStagedCreateAdapter;
use crate::catalog_control::unanchored_ctas_cleanup::IcebergUnanchoredCtasCleanupAdapter;
use crate::catalog_runtime::RestAccessDelegationMode;
use crate::commit::IcebergWriteControl;
use crate::distributed_rewrite::IcebergDistributedRewriteControl;
use crate::metadata::IcebergMetadata;
use crate::metadata_context::IcebergMetadataContext;
use crate::provider_binding::IcebergInstanceDistribution;
use crate::resources::IcebergMetadataResources;
use crate::typed_boundary::{IcebergTypedBoundary, IcebergTypedRequestControlFactory};
use novarocks_spi::connector::read_stack::{
    ConnectorReadMetadata, ConnectorReadRegistrationLease, ConnectorReadRequestControlFactory,
    ConnectorReadSplitManager,
};
use novarocks_spi::connector::{
    CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle, ConnectorControlBinding,
    ConnectorControlCreation, ConnectorControlFactory, ConnectorControlFactoryRequest,
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorProviderBindingKey,
    ConnectorProviderId, ConnectorReadWireEncoder, ProviderBindingEpoch,
};
use std::sync::Arc;

/// Fallible composition-root installation of one completed coordinator read
/// unit.
///
/// The concrete boundary must share the control generation's own catalog
/// runtime, so it can only be minted here.  The Server composition root turns
/// these SPI services and matching provider codec into its role-local bundle,
/// returning the strong lease the Iceberg generation retains.  A failure is
/// propagated: a control binding is never returned without its required read
/// registration.
pub type IcebergReadControlInstaller = Arc<
    dyn Fn(
            CatalogHandle,
            Arc<dyn ConnectorReadMetadata>,
            Arc<dyn ConnectorReadSplitManager>,
            Arc<dyn ConnectorReadWireEncoder>,
            Arc<dyn ConnectorReadRequestControlFactory>,
        ) -> Result<Arc<dyn ConnectorReadRegistrationLease>, ConnectorError>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct IcebergConnectorFactory {
    control_resources: IcebergMetadataResources,
    provider_id: ConnectorProviderId,
    read_control_installer: Option<IcebergReadControlInstaller>,
}

impl IcebergConnectorFactory {
    pub fn new(control_resources: IcebergMetadataResources) -> Self {
        Self {
            control_resources,
            provider_id: ConnectorProviderId::parse(PROVIDER_ID)
                .expect("static Iceberg provider ID is valid"),
            read_control_installer: None,
        }
    }

    /// Register the composition root's fallible coordinator read-unit sink.
    pub fn with_read_control_installer(mut self, installer: IcebergReadControlInstaller) -> Self {
        self.read_control_installer = Some(installer);
        self
    }

    pub fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    /// Build provider-private state before an attachment is durably recorded
    /// or a binding is published.  Dropping the returned value releases the
    /// catalog client and all generation-local reservations.
    pub fn prepare_unpublished(
        &self,
        request: &ConnectorControlFactoryRequest,
    ) -> Result<IcebergUnpublishedControl, ConnectorError> {
        if request.provider_id() != &self.provider_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg control factory received a request for another provider",
            ));
        }
        let catalog_properties = request.catalog_properties().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg control factory requires typed catalog properties before construction",
            )
        })?;
        if catalog_properties.provider_id().as_str() != "iceberg" {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg control factory received catalog properties for another provider",
            ));
        }
        let properties = catalog_properties
            .execution_properties()
            .iter()
            .map(|property| (property.key().to_string(), property.value().to_string()))
            .collect::<Vec<_>>();
        let rest_access_delegation = rest_access_delegation_mode(catalog_properties, &properties)?;
        // Both modes bind the immutable catalog definition.  Vended binding
        // carries only the owner and endpoint; it deliberately has no static
        // secret and therefore requires a request storage resolver before any
        // object-store I/O.  Leaving it as an unbound template loses that
        // mode information and makes optional control-generation probes try
        // to resolve an unauthorised warehouse instead of deferring them.
        let planning_binding = self
            .control_resources
            .planning_binding()
            .bind_catalog(catalog_properties)?;
        let control_resources = self
            .control_resources
            .clone()
            .with_planning_binding(planning_binding.clone());
        let object_store_config =
            matches!(rest_access_delegation, RestAccessDelegationMode::Static)
                .then(|| {
                    properties
                        .iter()
                        .find(|(key, _)| key == "iceberg.catalog.warehouse" || key == "warehouse")
                        .map(|(_, location)| {
                            planning_binding.object_store_binding_for_location(location)
                        })
                        .transpose()
                        .map_err(invalid)
                        .map(|binding| binding.flatten())
                })
                .transpose()?
                .flatten();
        let configuration = parse_catalog_configuration_with_object_store_binding(
            request.instance_id().as_str(),
            &properties,
            object_store_config.as_ref().map(|binding| binding.config()),
        )
        .map_err(invalid)?
        .without_object_store_config();
        // CatalogProperties is the already-validated, credential-free durable
        // definition supplied by the Frontend. Parsing may normalize aliases
        // and add provider-private defaults for this runtime, but a factory
        // must never turn that private representation into new desired state.
        let durable_properties = properties.clone();
        let runtime = Arc::new(
            IcebergMetadataContext::try_new_with_rest_access_delegation(
                IcebergCatalogControlState::new(configuration),
                control_resources,
                rest_access_delegation,
            )
            .map_err(unavailable)?,
        );
        Ok(IcebergUnpublishedControl {
            runtime,
            durable_properties,
        })
    }
}

impl IcebergConnectorFactory {
    /// Create one control generation and keep a handle on the catalog runtime
    /// it minted.
    ///
    /// `create_control` is the SPI shape and can only return the creation, but
    /// Iceberg's own role-binding factory needs the *same* generation's runtime
    /// to build its frontend write session. Preparing a second unpublished
    /// control would mint a second catalog client and a second generation, so
    /// the runtime is handed out here instead. It stays crate-private: no role
    /// host, registry, or installer ever sees it.
    pub(crate) fn create_control_with_runtime(
        &self,
        request: ConnectorControlFactoryRequest,
    ) -> Result<(ConnectorControlCreation, Arc<IcebergMetadataContext>), ConnectorError> {
        let unpublished = self.prepare_unpublished(&request)?;
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: self.provider_id.clone(),
            instance_id: request.instance_id().clone(),
        };
        let incarnation = ProviderBindingEpoch::new();
        let provider = Arc::new(IcebergMetadata::new(
            descriptor.clone(),
            incarnation,
            Arc::clone(&unpublished.runtime),
        ));
        let key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        let metadata_maintenance = Arc::new(IcebergMetadataMaintenanceAdapter::new(
            key.clone(),
            Arc::clone(&unpublished.runtime),
        )?);
        let write_control = Arc::new(IcebergWriteControl::new(
            descriptor.clone(),
            incarnation,
            Arc::clone(&unpublished.runtime),
        ));
        let data_mutation = Arc::new(IcebergDataMutationAdapter::try_new(Arc::clone(&provider))?);
        let distributed_rewrite = Arc::new(IcebergDistributedRewriteControl::new(
            descriptor.clone(),
            incarnation,
            Arc::clone(&unpublished.runtime),
            Arc::clone(&provider),
        )?);
        let cleanup_maintenance = Arc::new(IcebergCleanupMaintenanceAdapter::new(
            key.clone(),
            Arc::clone(&unpublished.runtime),
        )?);
        // Slot presence answers "does this provider implement this operation
        // family", which is a property of the Iceberg connector, not of the
        // catalog this generation happens to be pointed at. Gating on
        // `rest_catalog().is_some()` conflated the two and made an absent slot
        // stand in for an unsupported request.
        //
        // The adapters therefore attach for every generation, and whether a
        // specific request is supported is decided by the catalog owner at
        // admission, before the first side effect.
        let staged_create = Some(Arc::new(IcebergStagedCreateAdapter::try_new(Arc::clone(
            &provider,
        ))?));
        let unanchored_ctas_cleanup = match IcebergUnanchoredCtasCleanupAdapter::try_new(
            descriptor.clone(),
            incarnation,
            Arc::clone(&unpublished.runtime),
        ) {
            Ok(capability) => Some(Arc::new(capability)),
            // Sweeping the unanchored namespace needs a warehouse this
            // generation can enumerate and delete under. A catalog without one
            // still attaches for reads and ordinary table operations; CTAS is
            // what cannot run, and it is refused at admission rather than by a
            // missing slot here.
            Err(error) if error.kind() == ConnectorErrorKind::Unsupported => None,
            Err(error) => return Err(error),
        };
        let mut binding = ConnectorControlBinding::try_new_with_all_maintenance_capabilities_cleanup_and_staged_create(
                descriptor.clone(),
                incarnation,
                provider.clone(),
                provider.clone(),
                Arc::new(IcebergInstanceDistribution::new(descriptor.clone(), incarnation)),
                Some(provider.clone()),
                Some(data_mutation),
                Some(metadata_maintenance),
                Some(distributed_rewrite),
                Some(cleanup_maintenance),
                staged_create.map(|capability| capability as Arc<dyn novarocks_spi::connector::ConnectorStagedCreate>),
                Some(write_control),
                Some(provider.clone()),
            )?
            .try_with_unanchored_ctas_cleanup(unanchored_ctas_cleanup.map(|capability| {
                capability as Arc<dyn novarocks_spi::connector::ConnectorUnanchoredCtasCleanup>
            }))?
            .try_with_view_metadata(Some(provider.clone()))?;
        // Desired state, not this FE-local control generation, owns the
        // immutable catalog version. Defer typed-read registration until that
        // owner stamps the binding with its exact CatalogHandle.
        if let Some(installer) = self.read_control_installer.as_ref() {
            let installer = Arc::clone(installer);
            let runtime = Arc::clone(unpublished.runtime());
            let descriptor = descriptor.clone();
            let provider = Arc::clone(&provider);
            binding = binding.with_catalog_handle_installer(Arc::new(move |catalog_handle| {
                let boundary = Arc::new(IcebergTypedBoundary::new(
                    descriptor.clone(),
                    incarnation,
                    catalog_handle.clone(),
                    crate::typed_read::table_handle::HiveTransactionHandle::new(
                        true,
                        catalog_handle.version().as_bytes()[..16]
                            .try_into()
                            .expect("a CatalogVersion always has 32 bytes"),
                    ),
                    Arc::clone(&runtime),
                ));
                let adapter = Arc::new(Arc::clone(&boundary).read_runtime_adapter());
                let encoder: Arc<dyn ConnectorReadWireEncoder> =
                    Arc::new(crate::typed_read::IcebergConnectorReadWireAdapter::new(
                        adapter.as_ref().clone(),
                    ));
                let request_control_factory: Arc<dyn ConnectorReadRequestControlFactory> = Arc::new(
                    IcebergTypedRequestControlFactory::new(Arc::clone(&boundary)),
                );
                let lease = installer(
                    catalog_handle.clone(),
                    Arc::clone(&adapter) as Arc<dyn ConnectorReadMetadata>,
                    adapter as Arc<dyn ConnectorReadSplitManager>,
                    encoder,
                    request_control_factory,
                )?;
                provider.install_read_registration_lease(lease)
            }));
        }
        let runtime = Arc::clone(unpublished.runtime());
        let creation = ConnectorControlCreation::try_new(
            &request,
            binding,
            unpublished.durable_properties().to_vec(),
        )?;
        Ok((creation, runtime))
    }
}

impl ConnectorControlFactory for IcebergConnectorFactory {
    fn provider_id(&self) -> &ConnectorProviderId {
        self.provider_id()
    }

    fn create_control(
        &self,
        request: ConnectorControlFactoryRequest,
    ) -> Result<ConnectorControlCreation, ConnectorError> {
        self.create_control_with_runtime(request)
            .map(|(creation, _runtime)| creation)
    }
}

#[allow(dead_code)] // Held until the provider has assembled every capability.
#[derive(Debug)]
pub struct IcebergUnpublishedControl {
    runtime: Arc<IcebergMetadataContext>,
    durable_properties: Vec<(String, String)>,
}

impl IcebergUnpublishedControl {
    pub(crate) fn runtime(&self) -> &Arc<IcebergMetadataContext> {
        &self.runtime
    }

    pub fn durable_properties(&self) -> &[(String, String)] {
        &self.durable_properties
    }
}

#[cfg(test)]
fn credential_like_property(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    [
        "password",
        "secret",
        "token",
        "credential",
        "accesskey",
        "access_key",
        "private-key",
        "private_key",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

fn rest_access_delegation_mode(
    properties: &novarocks_spi::connector::CatalogProperties,
    execution_properties: &[(String, String)],
) -> Result<RestAccessDelegationMode, ConnectorError> {
    let object_store_binding = properties
        .credential_bindings()
        .iter()
        .find(|binding| binding.purpose() == CatalogCredentialPurpose::ObjectStoreData);
    let vended = matches!(
        object_store_binding.map(|binding| binding.mode()),
        Some(CatalogCredentialMode::Vended)
    );
    if !vended {
        return Ok(RestAccessDelegationMode::Static);
    }
    let rest = execution_properties.iter().any(|(key, value)| {
        (key == "iceberg.catalog.type" && value.eq_ignore_ascii_case("rest"))
            || (key == "type" && value.eq_ignore_ascii_case("rest"))
    });
    if !rest {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg vended object-store credentials are supported only for REST catalogs",
        ));
    }
    Ok(RestAccessDelegationMode::Vended)
}

fn invalid(error: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, error)
}

fn unavailable(error: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, error)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, Weak};

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::read_stack::ConnectorReadRegistrationLease;
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
        CatalogProperties, CatalogProperty, CatalogVersion, ConnectorInstanceId,
        ConnectorProviderId, CredentialConsumerRole, StaticCredentialReference,
    };

    use super::*;

    fn object_store_config() -> novarocks_fs::ObjectStoreConfig {
        novarocks_fs::ObjectStoreConfig {
            endpoint: "http://minio:9000".to_string(),
            access_key_id: novarocks_fs::SecretValue::new("server-access"),
            access_key_secret: novarocks_fs::SecretValue::new("server-secret"),
            session_token: None,
            enable_path_style_access: Some(true),
            region: None,
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        }
    }

    fn catalog_properties(
        instance_id: ConnectorInstanceId,
        properties: &[(String, String)],
        credential_bindings: Vec<CatalogCredentialBinding>,
    ) -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(instance_id, CatalogVersion::from_bytes([0; 32])),
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            properties
                .iter()
                .map(|(key, value)| {
                    CatalogProperty::new(key, value).expect("non-secret catalog property")
                })
                .collect(),
            credential_bindings,
        )
        .expect("valid desired-state catalog properties")
    }

    fn factory_request_with_catalog_properties(
        factory: &IcebergConnectorFactory,
        instance_id: ConnectorInstanceId,
        request_properties: Vec<(String, String)>,
        catalog_properties: CatalogProperties,
    ) -> ConnectorControlFactoryRequest {
        ConnectorControlFactoryRequest::try_new(
            factory.provider_id().clone(),
            instance_id,
            request_properties,
        )
        .and_then(|request| request.with_catalog_properties(catalog_properties))
        .expect("factory request with typed catalog properties")
    }

    fn factory_request(
        factory: &IcebergConnectorFactory,
        instance_id: &str,
        properties: Vec<(String, String)>,
    ) -> ConnectorControlFactoryRequest {
        let instance_id = ConnectorInstanceId::parse(instance_id).expect("instance ID");
        let catalog_properties = catalog_properties(instance_id.clone(), &properties, Vec::new());
        factory_request_with_catalog_properties(
            factory,
            instance_id,
            properties,
            catalog_properties,
        )
    }

    fn rest_factory_request(
        factory: &IcebergConnectorFactory,
        uri: String,
        extra: Vec<(String, String)>,
    ) -> ConnectorControlFactoryRequest {
        let mut properties = vec![
            ("iceberg.catalog.type".to_string(), "rest".to_string()),
            ("uri".to_string(), uri),
            (
                "iceberg.catalog.warehouse".to_string(),
                "file:///tmp/novarocks-rest-factory-warehouse".to_string(),
            ),
        ];
        properties.extend(extra);
        factory_request(factory, "ice", properties)
    }

    struct TestReadRegistrationLease;

    impl ConnectorReadRegistrationLease for TestReadRegistrationLease {}

    #[test]
    fn factory_request_rejects_duplicate_properties_before_provider_construction() {
        let error = ConnectorControlFactoryRequest::try_new(
            ConnectorProviderId::parse(PROVIDER_ID).expect("provider ID"),
            ConnectorInstanceId::parse("ice").expect("instance ID"),
            vec![
                (
                    "iceberg.catalog.warehouse".to_string(),
                    "/tmp/first".to_string(),
                ),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    "/tmp/second".to_string(),
                ),
            ],
        )
        .expect_err("duplicate properties must fail before provider construction");

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error
                .to_string()
                .contains("duplicate connector catalog property")
        );
    }

    #[test]
    fn factory_rejects_invalid_catalog_properties_before_runtime_construction() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let request = factory_request(
            &factory,
            "ice",
            vec![("iceberg.catalog.type".to_string(), "unknown".to_string())],
        );

        let error = factory
            .prepare_unpublished(&request)
            .expect_err("invalid catalog properties must not create a runtime");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.to_string().contains("hadoop|rest|hive"));
    }

    #[test]
    fn unpublished_generation_uses_only_typed_catalog_properties() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let request_properties = vec![
            ("type".to_string(), "iceberg".to_string()),
            (
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            ),
            ("aws.s3.access_key".to_string(), "not-durable".to_string()),
            ("aws.s3.secret_key".to_string(), "not-durable".to_string()),
        ];
        let typed_properties = vec![
            ("type".to_string(), "iceberg".to_string()),
            (
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            ),
        ];
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
        let request = factory_request_with_catalog_properties(
            &factory,
            instance_id.clone(),
            request_properties,
            catalog_properties(instance_id, &typed_properties, Vec::new()),
        );
        let unpublished = factory.prepare_unpublished(&request).expect("runtime");
        let mut expected_durable_properties = typed_properties.clone();
        expected_durable_properties.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(
            unpublished.durable_properties(),
            expected_durable_properties.as_slice(),
            "provider runtime normalization must not rewrite the typed durable definition"
        );
        assert!(
            unpublished
                .durable_properties()
                .iter()
                .all(|(key, _)| !credential_like_property(key))
        );
        let client = unpublished.runtime().novarocks_catalog().vendored_client();
        assert!(Arc::strong_count(&client) >= 1);
    }

    #[test]
    fn rejects_request_credentials_when_the_role_has_no_matching_binding() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let request_properties = vec![
            (
                "iceberg.catalog.warehouse".to_string(),
                "s3://warehouse/iceberg".to_string(),
            ),
            (
                "aws.s3.endpoint".to_string(),
                "http://minio:9000".to_string(),
            ),
            ("aws.s3.access_key".to_string(), "request-only".to_string()),
            ("aws.s3.secret_key".to_string(), "request-only".to_string()),
        ];
        let typed_properties = request_properties
            .iter()
            .filter(|(key, _)| !credential_like_property(key))
            .cloned()
            .collect::<Vec<_>>();
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
        let request = factory_request_with_catalog_properties(
            &factory,
            instance_id.clone(),
            request_properties,
            catalog_properties(instance_id, &typed_properties, Vec::new()),
        );

        let error = factory
            .prepare_unpublished(&request)
            .expect_err("request-only credentials must not create a generation");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error
                .to_string()
                .contains("exact object-store credential binding")
        );
    }

    #[test]
    fn restore_reuses_server_credentials_from_typed_binding() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let config = object_store_config();
        let binding = crate::access_binding::IcebergReadBinding::new(
            Some(config.clone()),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let typed_properties = vec![
            (
                "iceberg.catalog.warehouse".to_string(),
                "s3://warehouse/iceberg".to_string(),
            ),
            ("aws.s3.endpoint".to_string(), config.endpoint.clone()),
            (
                "aws.s3.enable_path_style_access".to_string(),
                "true".to_string(),
            ),
        ];
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
        let catalog_properties = catalog_properties(
            instance_id.clone(),
            &typed_properties,
            vec![
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::FrontendAndBackend,
                    CatalogCredentialMode::Static(
                        StaticCredentialReference::try_new("iceberg-test-object-store", "test")
                            .expect("test credential reference"),
                    ),
                )
                .expect("valid test credential binding"),
            ],
        );
        let request = factory_request_with_catalog_properties(
            &factory,
            instance_id.clone(),
            typed_properties,
            catalog_properties.clone(),
        );
        let first = factory.prepare_unpublished(&request).expect("create");
        let durable = first.durable_properties().to_vec();
        assert!(
            durable
                .iter()
                .all(|(key, _)| !credential_like_property(key))
        );
        drop(first);

        let restored = factory_request_with_catalog_properties(
            &factory,
            instance_id,
            durable,
            catalog_properties,
        );
        factory
            .prepare_unpublished(&restored)
            .expect("restore with server credentials");
    }

    #[test]
    fn created_binding_installs_exact_generation_control_capabilities() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let request = factory_request(
            &factory,
            "ice",
            vec![(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        );

        let creation = factory.create_control(request).expect("control creation");
        let maintenance = creation
            .binding()
            .metadata_maintenance()
            .expect("metadata maintenance");

        assert_eq!(maintenance.descriptor(), creation.binding().descriptor());
        assert_eq!(
            maintenance.binding_key().incarnation,
            creation.binding().incarnation()
        );
        let write = creation.binding().write().expect("write control");
        assert_eq!(
            write.binding_key().instance_id,
            creation.binding().descriptor().instance_id
        );
        assert_eq!(
            write.binding_key().incarnation,
            creation.binding().incarnation()
        );
        let mutation = creation.binding().mutation().expect("catalog mutation");
        assert_eq!(mutation.descriptor(), creation.binding().descriptor());
        assert_eq!(mutation.incarnation(), creation.binding().incarnation());
        let data_mutation = creation.binding().data_mutation().expect("data mutation");
        assert_eq!(data_mutation.descriptor(), creation.binding().descriptor());
        assert_eq!(
            data_mutation.binding_key().incarnation,
            creation.binding().incarnation()
        );
        let distributed_rewrite = creation
            .binding()
            .distributed_rewrite()
            .expect("distributed rewrite");
        assert_eq!(
            distributed_rewrite.descriptor(),
            creation.binding().descriptor()
        );
        assert_eq!(
            distributed_rewrite.binding_key().incarnation,
            creation.binding().incarnation()
        );
        let cleanup = creation
            .binding()
            .cleanup_maintenance()
            .expect("cleanup maintenance");
        assert_eq!(cleanup.descriptor(), creation.binding().descriptor());
        assert_eq!(
            cleanup.binding_key().incarnation,
            creation.binding().incarnation()
        );
        // The slot says the Iceberg connector implements staged creation, not
        // that this catalog can serve any particular request. A Hadoop
        // generation installs it and refuses CTAS at admission instead.
        assert!(
            creation.binding().staged_create().is_some(),
            "every Iceberg generation installs the staged-create adapter"
        );
        // The sweeper attaches too, and deliberately does not ask whether this
        // catalog can CTAS. One that cannot has never staged anything
        // unanchored, so the sweep finds nothing; gating it here would replace
        // the catalog's own explanation of why CTAS is impossible with a
        // generic "no cleanup capability" from the lease derivation.
        assert!(
            creation.binding().unanchored_ctas_cleanup().is_some(),
            "the sweeper attaches wherever its warehouse is usable"
        );
        let views = creation.binding().view_metadata().expect("view metadata");
        assert_eq!(views.descriptor(), creation.binding().descriptor());
        assert_eq!(views.incarnation(), creation.binding().incarnation());
        let statistics = creation.binding().statistics().expect("statistics");
        assert_eq!(statistics.descriptor(), creation.binding().descriptor());
        assert_eq!(statistics.incarnation(), creation.binding().incarnation());
        assert!(statistics.collection().is_some());
    }

    #[test]
    fn read_registration_is_fallible_and_is_owned_by_the_completed_catalog_handle() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let observed_key = Arc::new(Mutex::new(None));
        let observed_lease: Arc<Mutex<Option<Weak<dyn ConnectorReadRegistrationLease>>>> =
            Arc::new(Mutex::new(None));
        let key_sink = Arc::clone(&observed_key);
        let lease_sink = Arc::clone(&observed_lease);
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ))
        .with_read_control_installer(Arc::new(
            move |handle, metadata, splits, codec, request_factory| {
                // The installer receives one complete matching unit, never an
                // exposed provider boundary or a separately selected codec.
                assert_eq!(codec.owner(), handle.catalog_name().as_str());
                let _ = (metadata, splits, request_factory);
                *key_sink.lock().expect("key lock") = Some(handle);
                let lease: Arc<dyn ConnectorReadRegistrationLease> =
                    Arc::new(TestReadRegistrationLease);
                *lease_sink.lock().expect("lease lock") = Some(Arc::downgrade(&lease));
                Ok(lease)
            },
        ));
        let request = factory_request(
            &factory,
            "ice",
            vec![(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        );

        let creation = factory.create_control(request).expect("control creation");
        assert!(observed_key.lock().expect("key lock").is_none());
        let (binding, _) = creation.into_parts();
        let expected_handle = CatalogHandle::new(
            binding.descriptor().instance_id.clone(),
            CatalogVersion::from_bytes([7; 32]),
        );
        let binding = binding
            .with_catalog_properties(
                CatalogProperties::new(
                    expected_handle.clone(),
                    ConnectorProviderId::parse("iceberg").expect("static provider ID"),
                    1,
                    Vec::new(),
                    Vec::new(),
                )
                .expect("valid desired-state catalog properties"),
            )
            .expect("desired state stamps the catalog handle");
        assert_eq!(
            observed_key.lock().expect("key lock").as_ref(),
            Some(&expected_handle)
        );
        let lease = observed_lease
            .lock()
            .expect("lease lock")
            .clone()
            .expect("installer recorded lease");
        assert!(lease.upgrade().is_some());

        // The callback's local strong reference is gone.  The only remaining
        // strong edge is the private generation owner carried by metadata and
        // planning capabilities in the returned binding.
        drop(binding);
        assert!(lease.upgrade().is_none());
    }

    #[test]
    fn read_registration_error_prevents_catalog_handle_stamping() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ))
        .with_read_control_installer(Arc::new(|_, _, _, _, _| {
            Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "read-control registration rejected",
            ))
        }));
        let request = factory_request(
            &factory,
            "ice",
            vec![(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        );

        let creation = factory.create_control(request).expect("control creation");
        let (binding, _) = creation.into_parts();
        let error = match binding.with_catalog_properties(
            CatalogProperties::new(
                CatalogHandle::new(
                    ConnectorInstanceId::parse("ice").expect("instance ID"),
                    CatalogVersion::from_bytes([8; 32]),
                ),
                ConnectorProviderId::parse("iceberg").expect("static provider ID"),
                1,
                Vec::new(),
                Vec::new(),
            )
            .expect("valid desired-state catalog properties"),
        ) {
            Ok(_) => panic!("registration failure must prevent catalog-handle stamping"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Internal);
        assert!(error.message().contains("registration rejected"));
    }

    #[test]
    fn rest_factory_exposes_standard_staged_create() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let creation = factory
            .create_control(rest_factory_request(
                &factory,
                "http://127.0.0.1:1".to_string(),
                Vec::new(),
            ))
            .expect("standard REST control");
        assert!(
            creation.binding().staged_create().is_some(),
            "standard REST staged create must not depend on a private capability"
        );
    }

    #[test]
    fn rest_factory_without_unanchored_cleanup_still_attaches_for_non_ctas_work() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let request = factory_request(
            &factory,
            "ice",
            vec![
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                ("uri".to_string(), "http://127.0.0.1:1".to_string()),
            ],
        );

        let creation = factory
            .create_control(request)
            .expect("REST control without a GC-capable warehouse");
        assert!(creation.binding().staged_create().is_some());
        assert!(
            creation.binding().unanchored_ctas_cleanup().is_none(),
            "missing cleanup capability belongs to CTAS preflight, not catalog attachment"
        );
    }

    #[test]
    fn hive_generation_installs_the_staged_create_adapter_and_refuses_at_admission() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
            binding,
            runtime.handle().clone(),
        ));
        let request = factory_request(
            &factory,
            "hive",
            vec![
                ("iceberg.catalog.type".to_string(), "hive".to_string()),
                (
                    "hive.metastore.uris".to_string(),
                    "thrift://127.0.0.1:9083".to_string(),
                ),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    warehouse.path().display().to_string(),
                ),
            ],
        );

        let creation = factory.create_control(request).expect("Hive control");
        // Slot presence answers "does this provider implement staged creation",
        // which is a property of the connector. Whether *this* catalog can run
        // a CTAS is a separate question, and it is answered by the catalog
        // owner before the statement's source executes.
        assert!(
            creation.binding().staged_create().is_some(),
            "every Iceberg generation installs the staged-create adapter"
        );
    }
}
