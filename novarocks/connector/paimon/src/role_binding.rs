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

//! Registered Paimon roles with explicitly suspended capabilities.
use crate::{PROVIDER_ID, definition::paimon_contract_definition};
use futures::future::BoxFuture;
use novarocks_spi::connector::*;
use std::sync::Arc;
const PAIMON_CATALOG_TYPE_KEY: &str = "paimon.catalog.type";
const PAIMON_WAREHOUSE_KEY: &str = "warehouse";
const PAIMON_FILESYSTEM_BINDING: &str = "filesystem";

/// Shared refusal for catalog operations and BE execution binding.
// Design: ADR-0200 (docs/adr/ADR-0200-upstream-iceberg-backports-and-provider-suspension.md)
pub fn suspended() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "Paimon provider is suspended while upgrading Arrow/Parquet to 60; restore it after paimon-rust and its readers release Arrow 60 and zstd 0.14 support",
    )
}
struct PaimonGenericControl {
    descriptor: ConnectorInstanceDescriptor,
}
impl ConnectorMetadata for PaimonGenericControl {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.descriptor.instance_id
    }
    fn list_namespaces(
        &self,
        _request: ConnectorListNamespacesRequest,
    ) -> Result<Vec<ConnectorNamespaceIdentity>, ConnectorError> {
        Err(suspended())
    }
    fn list_namespaces_async(
        &self,
        _request: ConnectorListNamespacesRequest,
    ) -> futures::future::BoxFuture<'_, Result<Vec<ConnectorNamespaceIdentity>, ConnectorError>>
    {
        Box::pin(async { Err(suspended()) })
    }
    fn namespace_exists(
        &self,
        _request: ConnectorNamespaceRequest,
    ) -> Result<bool, ConnectorError> {
        Err(suspended())
    }
    fn namespace_exists_async(
        &self,
        _request: ConnectorNamespaceRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<bool, ConnectorError>> + Send + '_>,
    > {
        Box::pin(async { Err(suspended()) })
    }
    fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        Err(suspended())
    }
    fn table_exists_async(
        &self,
        _request: ConnectorTableRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<bool, ConnectorError>> + Send + '_>,
    > {
        Box::pin(async { Err(suspended()) })
    }
    fn list_tables(
        &self,
        _request: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        Err(suspended())
    }
    fn list_tables_async(
        &self,
        _request: ConnectorListTablesRequest,
    ) -> futures::future::BoxFuture<'_, Result<Vec<ConnectorTableIdentity>, ConnectorError>> {
        Box::pin(async { Err(suspended()) })
    }
    fn prepare_table(
        &self,
        _request: ConnectorTableRequest,
    ) -> novarocks_spi::connector::ConnectorTablePreparationFuture<'_> {
        Box::pin(async { Err(suspended()) })
    }
    fn create_prepared_table(
        &self,
        _preparation: novarocks_spi::connector::ConnectorTablePreparation,
        _creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        Err(suspended().into())
    }
    fn load_table(
        &self,
        _request: ConnectorTableRequest,
        _creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        Err(suspended().into())
    }

    fn read_reference_facts(
        &self,
        _request: ConnectorReadReferenceFactsRequest,
    ) -> Result<ConnectorReadReferenceFacts, ConnectorError> {
        Err(suspended())
    }
    fn read_reference_facts_async(
        &self,
        _request: ConnectorReadReferenceFactsRequest,
    ) -> BoxFuture<'_, Result<ConnectorReadReferenceFacts, ConnectorError>> {
        Box::pin(async { Err(suspended()) })
    }
    fn capture_table_object_binding(
        &self,
        _request: ConnectorTableObjectCaptureRequest,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        Err(suspended())
    }
    fn capture_table_object_binding_async(
        &self,
        _request: ConnectorTableObjectCaptureRequest,
    ) -> BoxFuture<'_, Result<ConnectorTableObjectBinding, ConnectorError>> {
        Box::pin(async { Err(suspended()) })
    }
    fn exact_semantic_revision(
        &self,
        _table: &ConnectorTableHandle,
        _selector: ConnectorReadSelector,
    ) -> Result<ConnectorExactSemanticRevision, ConnectorError> {
        Err(suspended())
    }
}

impl ConnectorScanPlanning for PaimonGenericControl {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.descriptor.instance_id
    }
    fn begin_scan(
        &self,
        _table: &ConnectorTableHandle,
        _request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        Err(suspended())
    }
    fn plan_splits(
        &self,
        _scan: &ConnectorScanHandle,
        _request: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        Err(suspended())
    }
}

impl ConnectorExecutionDistribution for PaimonGenericControl {
    fn declaration(
        &self,
        _context: &ConnectorRequestContext,
    ) -> Result<ConnectorProviderBinding, ConnectorError> {
        Err(suspended())
    }
}

#[derive(Clone, Default)]
pub struct PaimonControlRoleBindingFactory;
impl ConnectorControlRoleBindingFactory for PaimonControlRoleBindingFactory {
    fn provider_id(&self) -> ConnectorProviderId {
        provider_id()
    }
    fn normalize_and_validate(
        &self,
        properties: CatalogProperties,
    ) -> Result<NormalizedCatalogProperties, ConnectorMaterializationError> {
        validate_properties(&properties)?;
        NormalizedCatalogProperties::try_new(properties).map_err(invalid_definition)
    }
    fn open_catalog(
        &self,
        _identity: CatalogScopeIdentity,
    ) -> Arc<dyn ProviderCatalogControlFactory> {
        Arc::new(self.clone())
    }
}
impl ProviderCatalogControlFactory for PaimonControlRoleBindingFactory {
    fn materialize(
        &self,
        properties: NormalizedCatalogProperties,
        context: MaterializationContext,
    ) -> BoxFuture<'static, Result<ConnectorControlRoleBinding, ConnectorMaterializationError>>
    {
        Box::pin(async move {
            context.check_active()?;
            let catalog_properties = properties.as_catalog_properties().clone();
            validate_properties(&catalog_properties)?;
            let descriptor = descriptor(catalog_properties.handle());
            let generic = Arc::new(PaimonGenericControl {
                descriptor: descriptor.clone(),
            });
            let control = ConnectorControlBinding::try_new(
                descriptor,
                ProviderBindingEpoch::new(),
                generic.clone(),
                generic.clone(),
                generic,
                None,
            )
            .and_then(|binding| binding.with_catalog_properties(catalog_properties))
            .map_err(ConnectorMaterializationError::from)?;
            context.check_active()?;
            ConnectorControlRoleBinding::try_new(properties, Arc::new(control), None, None)
                .map_err(ConnectorMaterializationError::from)
        })
    }
}
#[derive(Clone, Default)]
pub struct PaimonExecutionRoleBindingFactory;
impl ConnectorExecutionRoleBindingFactory for PaimonExecutionRoleBindingFactory {
    fn provider_id(&self) -> ConnectorProviderId {
        provider_id()
    }
    fn bind(
        &self,
        _properties: &NormalizedCatalogProperties,
    ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError> {
        Err(suspended().into())
    }
}
pub fn paimon_role_definition(
    control: PaimonControlRoleBindingFactory,
    execution: PaimonExecutionRoleBindingFactory,
) -> Result<ProviderRoleDefinition, ConnectorCodecError> {
    ProviderRoleDefinition::read_only(
        paimon_contract_definition()?,
        Arc::new(control),
        Arc::new(execution),
    )
}

fn validate_properties(
    properties: &CatalogProperties,
) -> Result<&str, ConnectorMaterializationError> {
    if properties.provider_id().as_str() != PROVIDER_ID {
        return Err(invalid_definition(
            "Paimon role factory received another provider identity",
        ));
    }
    let mut catalog_type = None;
    let mut warehouse = None;
    for property in properties.execution_properties() {
        match property.key() {
            PAIMON_CATALOG_TYPE_KEY => catalog_type = Some(property.value()),
            PAIMON_WAREHOUSE_KEY => warehouse = Some(property.value()),
            key if key.starts_with("paimon.") => {
                return Err(invalid_definition(format!(
                    "unsupported Paimon catalog property: {}",
                    property.key()
                )));
            }
            _ => {}
        }
    }
    if catalog_type != Some(PAIMON_FILESYSTEM_BINDING) {
        return Err(invalid_definition(
            "Paimon paimon.catalog.type must be filesystem",
        ));
    }
    warehouse.ok_or_else(|| invalid_definition("Paimon warehouse property is required"))
}

fn descriptor(handle: &CatalogHandle) -> ConnectorInstanceDescriptor {
    ConnectorInstanceDescriptor {
        provider_id: provider_id(),
        instance_id: handle.catalog_name().clone(),
    }
}

fn provider_id() -> ConnectorProviderId {
    ConnectorProviderId::parse(PROVIDER_ID).expect("static Paimon provider identity")
}

fn invalid_definition(detail: impl AsRef<str>) -> ConnectorMaterializationError {
    ConnectorMaterializationError::new(
        ConnectorMaterializationErrorClass::InvalidDefinition,
        ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
        detail,
    )
}

#[cfg(test)]
mod suspension_tests {
    use super::*;
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};

    fn properties(version: u8) -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("paimon.suspended").expect("catalog"),
                CatalogVersion::from_bytes([version; 32]),
            ),
            provider_id(),
            1,
            vec![
                CatalogProperty::new(PAIMON_CATALOG_TYPE_KEY, PAIMON_FILESYSTEM_BINDING)
                    .expect("catalog type"),
                CatalogProperty::new(PAIMON_WAREHOUSE_KEY, "file:///suspended-warehouse")
                    .expect("warehouse"),
            ],
            Vec::new(),
        )
        .expect("catalog properties")
    }

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            ConnectorStopOwner::new().view(),
            1024,
            4096,
        )
        .expect("request context")
    }

    fn table_request(context: &ConnectorRequestContext) -> ConnectorTableRequest {
        ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id: properties(1).handle().catalog_name().clone(),
                namespace: Arc::from("db"),
                table: Arc::from("events"),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context.clone(),
        }
    }

    async fn materialize(version: u8) -> ConnectorControlRoleBinding {
        let factory = PaimonControlRoleBindingFactory;
        let properties = factory
            .normalize_and_validate(properties(version))
            .expect("valid suspended catalog definition");
        let owner = factory.open_catalog(CatalogScopeIdentity::new(
            provider_id(),
            properties
                .as_catalog_properties()
                .handle()
                .catalog_name()
                .clone(),
            Default::default(),
        ));
        owner
            .materialize(
                properties,
                MaterializationContext::new(Instant::now() + Duration::from_secs(5)),
            )
            .await
            .expect("suspension does not make the catalog unavailable")
    }

    fn assert_suspended(error: ConnectorError) {
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(error.message(), suspended().message());
        assert!(error.message().contains("suspended"));
        assert!(error.message().contains("paimon-rust"));
        assert!(error.message().contains("Arrow 60"));
        assert!(error.message().contains("zstd 0.14"));
        assert!(error.message().len() <= 512);
    }

    fn assert_refused<T>(result: Result<T, ConnectorError>) {
        match result {
            Err(error) => assert_suspended(error),
            Ok(_) => panic!("a suspended capability must not succeed"),
        }
    }

    fn assert_schema_refused<T>(
        result: Result<T, novarocks_connector_contract::PureProviderCompileError<ConnectorError>>,
    ) {
        match result {
            Err(novarocks_connector_contract::PureProviderCompileError::Provider(error)) => {
                assert_suspended(error);
            }
            _ => panic!("schema access must preserve the typed suspension refusal"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn suspended_catalog_materializes_independent_generations_without_read_or_write() {
        let first = materialize(1).await;
        let second = materialize(2).await;
        for (binding, version) in [(&first, 1), (&second, 2)] {
            assert_eq!(
                binding.properties().as_catalog_properties(),
                &properties(version)
            );
            assert_eq!(
                binding.control().catalog_properties().unwrap(),
                &properties(version)
            );
            assert!(binding.read().is_none());
            assert!(binding.write().is_none());
            assert!(binding.control().write().is_none());
            assert_eq!(
                binding.control().metadata().instance_id(),
                properties(version).handle().catalog_name()
            );
            assert_eq!(
                binding.control().planning().instance_id(),
                properties(version).handle().catalog_name()
            );
        }
        assert_ne!(
            first.control().control_runtime_id(),
            second.control().control_runtime_id()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn every_catalog_and_schema_entry_refuses_with_the_same_restoration_condition() {
        let binding = materialize(1).await;
        let metadata = binding.control().metadata();
        let context = request_context();
        let table = table_request(&context);
        let namespace = ConnectorNamespaceIdentity {
            instance_id: table.table.instance_id.clone(),
            namespace: table.table.namespace.clone(),
        };
        let namespaces = ConnectorListNamespacesRequest {
            instance_id: table.table.instance_id.clone(),
            bound: ConnectorListingBound::V1,
            context: context.clone(),
        };
        let namespace_request = ConnectorNamespaceRequest {
            namespace: namespace.clone(),
            context: context.clone(),
        };
        let tables = ConnectorListTablesRequest {
            namespace,
            bound: ConnectorListingBound::V1,
            context: context.clone(),
        };
        let references = ConnectorReadReferenceFactsRequest {
            table: table.table.clone(),
            context: context.clone(),
        };
        let capture = ConnectorTableObjectCaptureRequest {
            table: table.table.clone(),
            resolution: table.resolution,
            selector: ConnectorTableObjectSelector::Current,
            context,
        };
        assert_refused(metadata.list_namespaces(namespaces.clone()));
        assert_refused(metadata.list_namespaces_async(namespaces).await);
        assert_refused(metadata.namespace_exists(namespace_request.clone()));
        assert_refused(metadata.namespace_exists_async(namespace_request).await);
        assert_refused(metadata.table_exists(table.clone()));
        assert_refused(metadata.table_exists_async(table.clone()).await);
        assert_refused(metadata.list_tables(tables.clone()));
        assert_refused(metadata.list_tables_async(tables).await);
        assert_refused(metadata.prepare_table(table.clone()).await);
        assert_refused(metadata.read_reference_facts(references.clone()));
        assert_refused(metadata.read_reference_facts_async(references).await);
        assert_refused(metadata.capture_table_object_binding(capture.clone()));
        assert_refused(metadata.capture_table_object_binding_async(capture).await);
        let table_handle =
            ConnectorTableHandle::try_new(table.table.instance_id.clone(), bytes::Bytes::new())
                .expect("table handle");
        for selector in [
            ConnectorReadSelector::Current,
            ConnectorReadSelector::SnapshotId(1),
            ConnectorReadSelector::TimestampMicros(1),
        ] {
            assert_refused(metadata.exact_semantic_revision(&table_handle, selector));
        }

        // Refusal precedes any schema allocation or provider preparation.
        let mut request = |_| panic!("suspended schema access must not request capacity");
        let mut work = |_| panic!("suspended schema access must not start work");
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        assert_schema_refused(metadata.load_table(table.clone(), &mut creation));
        assert_schema_refused(metadata.create_prepared_table(
            ConnectorTablePreparation::new(table.table.instance_id, ()),
            &mut creation,
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scan_and_distribution_refuse_before_consuming_a_table_payload() {
        let binding = materialize(1).await;
        let context = request_context();
        let instance = properties(1).handle().catalog_name().clone();
        let table = ConnectorTableHandle::try_new(instance.clone(), bytes::Bytes::new())
            .expect("table handle");
        let scan =
            ConnectorScanHandle::try_new(instance, bytes::Bytes::new()).expect("scan handle");
        assert_refused(binding.control().planning().begin_scan(
            &table,
            ConnectorBeginScanRequest {
                projection: Vec::new(),
                static_predicates: Vec::new(),
                selection: ConnectorScanSelection::Snapshot(ConnectorReadSelector::Current),
                purpose: ConnectorReadPurpose::Query,
                limit: None,
                batch: ConnectorBatchBudget {
                    max_rows: NonZeroUsize::new(1).unwrap(),
                    max_bytes: NonZeroUsize::new(1).unwrap(),
                },
                context: context.clone(),
            },
        ));
        assert_refused(binding.control().planning().plan_splits(
            &scan,
            ConnectorSplitPlanningRequest {
                target_parallelism: NonZeroUsize::new(1).unwrap(),
                max_split_bytes: None,
                context: context.clone(),
            },
        ));
        assert_refused(
            binding
                .control()
                .execution_distribution()
                .declaration(&context),
        );
    }

    #[test]
    fn execution_binding_retains_the_unsupported_cause_without_transient_retry() {
        let properties = PaimonControlRoleBindingFactory
            .normalize_and_validate(properties(1))
            .expect("normalized properties");
        let error = match PaimonExecutionRoleBindingFactory.bind(&properties) {
            Err(error) => error,
            Ok(_) => panic!("suspended execution must not bind"),
        };
        assert_eq!(
            error.class(),
            ConnectorMaterializationErrorClass::InvalidDefinition
        );
        assert_eq!(
            error.disposition(),
            ConnectorMaterializationRetryDisposition::UntilDefinitionChanges
        );
        assert_eq!(error.detail(), suspended().message());
        let cause = std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<ConnectorError>())
            .expect("materialization retains the original connector error");
        assert_suspended(cause.clone());
    }
}
