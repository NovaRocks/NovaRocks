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

//! Complete read-only FE and BE bindings for one Paimon catalog generation.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use bytes::Bytes;
use futures::future::BoxFuture;
use novarocks_spi::connector::provider::ProviderReadTypes;
use novarocks_spi::connector::read_stack::adapter::{
    ProviderReadColumnBinding, ProviderReadFactory, ProviderReadFactoryAdapter,
    ProviderReadFilterResult, ProviderReadLimitApplication, ProviderReadMetadata,
    ProviderReadPageSourceProvider, ProviderReadRuntime, ProviderReadSplitManager,
    ProviderReadSplitSource, ProviderReadSystemTablePlan, ProviderReadSystemTableProvider,
    ReadRuntimeAdapter,
};
use novarocks_spi::connector::read_stack::{
    Assignment, ConnectorPageSource, ConnectorPageSourceProviderOptions, ConnectorReadChangeWindow,
    ConnectorReadColumnHandle, ConnectorReadRelation, ConnectorReadRelationKind,
    ConnectorReadRelationVersion, ConnectorReadRequestControl, ConnectorReadRequestControlFactory,
    ConnectorReadSplit, ConnectorReadSplitFacts, ConnectorReadTableExecuteProcedure,
    ConnectorReadTransactionHandle, ConnectorSession, ConnectorSplitBatch, Constraint,
    DynamicFilter, DynamicFilterSnapshot, SchemaTableName, SplitSourceProfile,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogProperties, ConnectorBeginScanRequest, ConnectorCodecCategory,
    ConnectorCodecError, ConnectorCodecErrorKind, ConnectorCodecRevision, ConnectorControlBinding,
    ConnectorDecodeContext, ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorEncodedPayload,
    ConnectorEnvelopeHeader, ConnectorError, ConnectorErrorKind, ConnectorExecutionDistribution,
    ConnectorFieldPath, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorListNamespacesRequest, ConnectorListTablesRequest, ConnectorMetadata,
    ConnectorNamespaceIdentity, ConnectorNamespaceRequest, ConnectorPinnedFileSet,
    ConnectorProviderBinding, ConnectorProviderId, ConnectorReadRelationPayload,
    ConnectorReadSplitCategory, ConnectorReadSplitPayload, ConnectorReadWireDecoder,
    ConnectorReadWireEncoder, ConnectorRequestContext, ConnectorScan, ConnectorScanHandle,
    ConnectorScanPlanning, ConnectorSplitPlanningRequest, ConnectorSplitPlanningResult,
    ConnectorTableDefinitionFacts, ConnectorTableHandle, ConnectorTableIdentity,
    ConnectorTableMetadata, ConnectorTablePlanningFacts, ConnectorTableRequest,
    ProviderBindingEpoch,
};
use novarocks_spi::connector::{
    ConnectorControlReadBinding, ConnectorControlRoleBinding, ConnectorControlRoleBindingFactory,
    ConnectorExecutionReadBinding, ConnectorExecutionRoleBinding,
    ConnectorExecutionRoleBindingFactory, ConnectorMaterializationError,
    ConnectorMaterializationErrorClass, ConnectorMaterializationRetryDisposition,
    MaterializationContext, NormalizedCatalogProperties, ProviderRoleDefinition,
};
use paimon::catalog::Identifier;
use paimon::io::FileIO;
use paimon::spec::{BinaryRow, BinaryTableStats, DataFileMeta, Schema, TableSchema};
use paimon::{DataSplit, DataSplitBuilder, DeletionFile, RowRange, Table};
use sha2::{Digest, Sha256};

use crate::PROVIDER_ID;
use crate::catalog::{PaimonFileSystemCatalog, map_sdk_error};
use crate::definition::{PAIMON_READ_CODEC_REVISION, paimon_contract_definition};
use crate::domain::{PaimonColumn, PaimonReadTypes, PaimonReadView, PaimonSplit, PaimonTable};
use crate::io::PaimonHostFileIo;
use crate::metadata::{PaimonFrozenRead, columns_from_schema};
use crate::page_source::PaimonPageSource;
use crate::reader::PaimonReader;
use crate::resources::PaimonRequestResources;
use crate::sdk_control::PaimonSdkReadControl;
use crate::split_source::{PaimonSplitPlanningLimits, PaimonSplitSource, plan_splits};
use crate::wire::read::PaimonReadWireCodec;

const PAIMON_CATALOG_TYPE_KEY: &str = "paimon.catalog.type";
const PAIMON_WAREHOUSE_KEY: &str = "warehouse";
const PAIMON_FILESYSTEM_BINDING: &str = "filesystem";
const MAX_PRIVATE_READ_BYTES: usize = 16 * 1024 * 1024;
const MAX_PRIVATE_RETAINED_BYTES: usize = 64 * 1024 * 1024;

/// Server-owned authorization hook. Implementations resolve one exact role,
/// catalog generation and admitted request into a warehouse-bounded FileIO.
/// The provider has no environment, global credential or storage fallback.
// Design: ADR-0138 (docs/adr/ADR-0138-vendored-paimon-read-host-patch.md)
pub trait PaimonRoleFileIoFactory: Send + Sync {
    fn bind_file_io(
        &self,
        properties: &CatalogProperties,
        warehouse: &str,
        request: &ConnectorRequestContext,
    ) -> Result<PaimonHostFileIo, ConnectorError>;
}

#[derive(Clone)]
struct PaimonAsyncRuntime {
    handle: tokio::runtime::Handle,
}

impl PaimonAsyncRuntime {
    fn new(handle: tokio::runtime::Handle) -> Self {
        Self { handle }
    }

    fn block_on<F>(&self, future: F) -> Result<F::Output, ConnectorError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = self.handle.clone();
        std::thread::Builder::new()
            .name("paimon-read-runtime".to_string())
            .spawn(move || handle.block_on(future))
            .map_err(|error| internal(format!("spawn Paimon runtime bridge: {error}")))?
            .join()
            .map_err(|_| internal("Paimon runtime bridge panicked"))
    }

    fn handle(&self) -> tokio::runtime::Handle {
        self.handle.clone()
    }
}

#[derive(Default)]
struct PaimonRequestCache {
    frozen: Mutex<
        HashMap<
            (CatalogHandle, SchemaTableName),
            Arc<OnceLock<Result<Arc<PaimonFrozenRead>, ConnectorError>>>,
        >,
    >,
}

impl PaimonRequestCache {
    fn get_or_prepare(
        &self,
        catalog: &CatalogHandle,
        name: &SchemaTableName,
        prepare: impl FnOnce() -> Result<Arc<PaimonFrozenRead>, ConnectorError>,
    ) -> Result<Arc<PaimonFrozenRead>, ConnectorError> {
        let slot = self
            .frozen
            .lock()
            .expect("Paimon request cache lock")
            .entry((catalog.clone(), name.clone()))
            .or_insert_with(|| Arc::new(OnceLock::new()))
            .clone();
        slot.get_or_init(prepare).clone()
    }
}

#[derive(Clone, Debug)]
struct PaimonBoundTable {
    table: PaimonTable,
    view: PaimonReadView,
    frozen: Option<Arc<PaimonFrozenRead>>,
}

impl PaimonBoundTable {
    fn frozen(value: Arc<PaimonFrozenRead>) -> Self {
        Self {
            table: value.table().clone(),
            view: value.view().clone(),
            frozen: Some(value),
        }
    }

    fn decoded(table: PaimonTable, view: PaimonReadView) -> Result<Self, ConnectorError> {
        if table.location() != view.table_location() {
            return Err(invalid(
                "Paimon table and read view refer to different locations",
            ));
        }
        Ok(Self {
            table,
            view,
            frozen: None,
        })
    }
}

#[derive(Clone)]
struct PaimonReadRuntime {
    descriptor: ConnectorInstanceDescriptor,
    catalog_handle: CatalogHandle,
    async_runtime: PaimonAsyncRuntime,
    catalog: Option<PaimonFileSystemCatalog>,
    resources: Option<PaimonRequestResources>,
    request_cache: Option<Arc<PaimonRequestCache>>,
}

impl PaimonReadRuntime {
    fn template(
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
        async_runtime: PaimonAsyncRuntime,
    ) -> Self {
        Self {
            descriptor,
            catalog_handle,
            async_runtime,
            catalog: None,
            resources: None,
            request_cache: None,
        }
    }

    fn for_request(
        &self,
        catalog: PaimonFileSystemCatalog,
        resources: PaimonRequestResources,
        cache: Arc<PaimonRequestCache>,
    ) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            catalog_handle: self.catalog_handle.clone(),
            async_runtime: self.async_runtime.clone(),
            catalog: Some(catalog),
            resources: Some(resources),
            request_cache: Some(cache),
        }
    }

    fn prepare_read(
        &self,
        name: &SchemaTableName,
    ) -> Result<Arc<PaimonFrozenRead>, ConnectorError> {
        let catalog = self.catalog.clone().ok_or_else(request_binding_required)?;
        let cache = self
            .request_cache
            .as_ref()
            .ok_or_else(request_binding_required)?;
        let request_name = name.clone();
        cache.get_or_prepare(&self.catalog_handle, name, || {
            self.async_runtime
                .block_on(async move { catalog.prepare_read(&request_name).await })?
        })
    }
}

impl ProviderReadRuntime for PaimonReadRuntime {
    type Table = PaimonBoundTable;
    type Column = PaimonColumn;
    type Transaction = Option<PaimonReadView>;
    type Split = PaimonSplit;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    fn transaction(&self) -> Self::Transaction {
        None
    }
}

impl ProviderReadMetadata for PaimonReadRuntime {
    fn get_table_handle(
        &self,
        _session: &ConnectorSession,
        name: &SchemaTableName,
        version: ConnectorReadRelationVersion,
        reference: Option<&str>,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        if version != ConnectorReadRelationVersion::Current || reference.is_some() {
            return Err(unsupported(
                "Paimon snapshot and named-reference reads are unsupported in PAI-1",
            ));
        }
        self.prepare_read(name)
            .map(PaimonBoundTable::frozen)
            .map(Some)
    }

    fn get_pinned_file_set_handle(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
        _pinned: &ConnectorPinnedFileSet,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Err(unsupported("Paimon pinned-file-set reads are unsupported"))
    }

    fn get_column_bindings(
        &self,
        _session: &ConnectorSession,
        table: &Self::Table,
    ) -> Result<Vec<ProviderReadColumnBinding<Self::Column>>, ConnectorError> {
        let frozen = table.frozen.as_ref().ok_or_else(request_binding_required)?;
        Ok(frozen
            .columns()
            .iter()
            .cloned()
            .map(|column| ProviderReadColumnBinding::new(column.name(), column.clone(), false))
            .collect())
    }

    fn apply_filter(
        &self,
        _session: &ConnectorSession,
        _table: &Self::Table,
        _constraint: &Constraint<Self::Column>,
    ) -> ProviderReadFilterResult<Self::Table, Self::Column> {
        Ok(None)
    }

    fn apply_projection(
        &self,
        _session: &ConnectorSession,
        _table: &Self::Table,
        _assignments: &[Assignment<Self::Column>],
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Ok(None)
    }

    fn apply_limit(
        &self,
        _session: &ConnectorSession,
        _table: &Self::Table,
        _limit: u64,
    ) -> Result<Option<ProviderReadLimitApplication<Self::Table>>, ConnectorError> {
        Ok(None)
    }

    fn get_system_table_plan(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
    ) -> Result<Option<ProviderReadSystemTablePlan<Self::Table>>, ConnectorError> {
        Err(unsupported("Paimon system tables are unsupported in PAI-1"))
    }

    fn get_change_window_plan(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
        _window: ConnectorReadChangeWindow,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Err(unsupported("Paimon change-window reads are unsupported"))
    }

    fn get_table_execute_plan(
        &self,
        _session: &ConnectorSession,
        _name: &SchemaTableName,
        _procedure: ConnectorReadTableExecuteProcedure,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        Err(unsupported("Paimon table procedures are unsupported"))
    }
}

struct BoundPaimonSplitSource(PaimonSplitSource);

impl ProviderReadSplitSource<PaimonReadRuntime> for BoundPaimonSplitSource {
    fn profile_snapshot(&self) -> SplitSourceProfile {
        novarocks_spi::connector::read_stack::ConnectorSplitSource::profile_snapshot(&self.0)
    }

    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &DynamicFilterSnapshot<PaimonColumn>,
    ) -> Result<ConnectorSplitBatch<PaimonSplit>, ConnectorError> {
        novarocks_spi::connector::read_stack::ConnectorSplitSource::next_batch(
            &mut self.0,
            max_size,
            dynamic_filter,
        )
    }

    fn is_finished(&self) -> bool {
        novarocks_spi::connector::read_stack::ConnectorSplitSource::is_finished(&self.0)
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        novarocks_spi::connector::read_stack::ConnectorSplitSource::close(&mut self.0)
    }
}

impl ProviderReadSplitManager for PaimonReadRuntime {
    fn get_splits(
        &self,
        _session: &ConnectorSession,
        table: &Self::Table,
        _columns: &[Assignment<Self::Column>],
        _dynamic_filter_columns: &BTreeSet<Self::Column>,
        _constraint: &Constraint<Self::Column>,
    ) -> Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError> {
        let frozen = Arc::clone(table.frozen.as_ref().ok_or_else(request_binding_required)?);
        let resources = self
            .resources
            .clone()
            .ok_or_else(request_binding_required)?;
        let planning_resources = resources.clone();
        let planned = self.async_runtime.block_on(async move {
            plan_splits(
                frozen.as_ref(),
                planning_resources,
                PaimonSplitPlanningLimits::default(),
            )
            .await
        })??;
        Ok(Box::new(BoundPaimonSplitSource(PaimonSplitSource::new(
            planned, resources,
        )?)))
    }
}

struct PaimonRequestControlFactory {
    template: PaimonReadRuntime,
    properties: CatalogProperties,
    warehouse: Arc<str>,
    access: Arc<dyn PaimonRoleFileIoFactory>,
}

impl PaimonRequestControlFactory {
    fn bind_runtime(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<ReadRuntimeAdapter<PaimonReadRuntime>, ConnectorError> {
        active(request)?;
        let resources = PaimonRequestResources::new(request.resources()?.clone());
        let host_io = self
            .access
            .bind_file_io(&self.properties, &self.warehouse, request)?;
        let catalog =
            PaimonFileSystemCatalog::try_new(&self.warehouse, host_io, resources.clone())?;
        let cache = request.request_scope_extension_or_insert_with(PaimonRequestCache::default);
        Ok(ReadRuntimeAdapter::new(Arc::new(
            self.template.for_request(catalog, resources, cache),
        )))
    }
}

impl ConnectorReadRequestControlFactory for PaimonRequestControlFactory {
    fn for_request(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<ConnectorReadRequestControl, ConnectorError> {
        let adapter = self.bind_runtime(request)?;
        Ok(ConnectorReadRequestControl::new(
            Arc::new(adapter.clone()),
            Arc::new(adapter),
        ))
    }
}

#[derive(Clone)]
struct PaimonGenericControl {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    properties: CatalogProperties,
    warehouse: Arc<str>,
    access: Arc<dyn PaimonRoleFileIoFactory>,
    async_runtime: PaimonAsyncRuntime,
}

impl PaimonGenericControl {
    fn catalog(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<PaimonFileSystemCatalog, ConnectorError> {
        active(request)?;
        let resources = PaimonRequestResources::new(request.resources()?.clone());
        let host_io = self
            .access
            .bind_file_io(&self.properties, &self.warehouse, request)?;
        PaimonFileSystemCatalog::try_new(&self.warehouse, host_io, resources)
    }

    fn prepare_read(
        &self,
        name: SchemaTableName,
        request: &ConnectorRequestContext,
    ) -> Result<Arc<PaimonFrozenRead>, ConnectorError> {
        let cache = request.request_scope_extension_or_insert_with(PaimonRequestCache::default);
        let catalog = self.catalog(request)?;
        let request_name = name.clone();
        cache.get_or_prepare(self.properties.handle(), &name, || {
            self.async_runtime
                .block_on(async move { catalog.prepare_read(&request_name).await })?
        })
    }

    fn ensure_instance(&self, instance: &ConnectorInstanceId) -> Result<(), ConnectorError> {
        if instance != &self.descriptor.instance_id {
            return Err(invalid("Paimon metadata request names another catalog"));
        }
        Ok(())
    }
}

impl ConnectorMetadata for PaimonGenericControl {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.descriptor.instance_id
    }

    fn list_namespaces(
        &self,
        request: ConnectorListNamespacesRequest,
    ) -> Result<Vec<ConnectorNamespaceIdentity>, ConnectorError> {
        self.ensure_instance(&request.instance_id)?;
        let catalog = self.catalog(&request.context)?;
        let entries = self
            .async_runtime
            .block_on(async move { catalog.list_databases().await })??;
        Ok(entries.map(|entries| {
            entries
                .into_iter()
                .map(|namespace| ConnectorNamespaceIdentity {
                    instance_id: self.descriptor.instance_id.clone(),
                    namespace: Arc::from(namespace),
                })
                .collect()
        }))
    }

    fn namespace_exists(&self, request: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        self.ensure_instance(&request.namespace.instance_id)?;
        Ok(self
            .list_namespaces(ConnectorListNamespacesRequest {
                instance_id: request.namespace.instance_id.clone(),
                context: request.context,
            })?
            .iter()
            .any(|value| value.namespace == request.namespace.namespace))
    }

    fn table_exists(&self, request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        self.ensure_instance(&request.table.instance_id)?;
        let catalog = self.catalog(&request.context)?;
        let namespace = request.table.namespace.clone();
        let entries = self
            .async_runtime
            .block_on(async move { catalog.list_tables(&namespace).await })??;
        Ok(entries
            .entries()
            .iter()
            .any(|name| name == request.table.table.as_ref()))
    }

    fn list_tables(
        &self,
        request: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        self.ensure_instance(&request.namespace.instance_id)?;
        let catalog = self.catalog(&request.context)?;
        let namespace = request.namespace.namespace.clone();
        let entries = self
            .async_runtime
            .block_on(async move { catalog.list_tables(&namespace).await })??;
        Ok(entries.map(|entries| {
            entries
                .into_iter()
                .map(|table| ConnectorTableIdentity {
                    instance_id: self.descriptor.instance_id.clone(),
                    namespace: request.namespace.namespace.clone(),
                    table: Arc::from(table),
                })
                .collect()
        }))
    }

    fn load_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        self.ensure_instance(&request.table.instance_id)?;
        let name = SchemaTableName::try_new(&request.table.namespace, &request.table.table)?;
        let frozen = self.prepare_read(name, &request.context)?;
        let schema = paimon::arrow::build_target_arrow_schema(frozen.output_schema().fields())
            .map_err(map_sdk_error)?;
        let payload = novarocks_spi::connector::ConnectorPrivateEncoder::encode_private(
            &PaimonReadWireCodec,
            frozen.table(),
        )
        .map_err(codec_as_connector_error)?;
        let table = ConnectorTableHandle::try_new(self.descriptor.instance_id.clone(), payload)?;
        Ok(ConnectorTableMetadata {
            identity: request.table,
            schema,
            planning_facts: ConnectorTablePlanningFacts::empty(),
            definition_facts: ConnectorTableDefinitionFacts::empty(),
            version: Some(Bytes::copy_from_slice(
                &frozen.view().schema_id().to_be_bytes(),
            )),
            statistics_data_version: None,
            table,
        })
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
        Err(unsupported(
            "Paimon reads require the provider typed read host",
        ))
    }

    fn plan_splits(
        &self,
        _scan: &ConnectorScanHandle,
        _request: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        Err(unsupported(
            "Paimon reads require the provider typed read host",
        ))
    }
}

impl ConnectorExecutionDistribution for PaimonGenericControl {
    fn declaration(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorProviderBinding, ConnectorError> {
        active(context)?;
        ConnectorProviderBinding::try_new(
            self.descriptor.provider_id.clone(),
            self.descriptor.instance_id.as_str(),
            self.incarnation.to_bytes(),
            PAIMON_FILESYSTEM_BINDING,
        )
    }
}

/// FE-only factory for one exact Paimon catalog generation.
#[derive(Clone)]
pub struct PaimonControlRoleBindingFactory {
    access: Arc<dyn PaimonRoleFileIoFactory>,
    async_runtime: PaimonAsyncRuntime,
}

impl PaimonControlRoleBindingFactory {
    pub fn new(access: Arc<dyn PaimonRoleFileIoFactory>, runtime: tokio::runtime::Handle) -> Self {
        Self {
            access,
            async_runtime: PaimonAsyncRuntime::new(runtime),
        }
    }
}

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

    fn materialize(
        &self,
        properties: NormalizedCatalogProperties,
        context: MaterializationContext,
    ) -> BoxFuture<'static, Result<ConnectorControlRoleBinding, ConnectorMaterializationError>>
    {
        let access = Arc::clone(&self.access);
        let async_runtime = self.async_runtime.clone();
        Box::pin(async move {
            context.check_active()?;
            let catalog_properties = properties.as_catalog_properties().clone();
            let warehouse = Arc::<str>::from(validate_properties(&catalog_properties)?);
            let descriptor = descriptor(catalog_properties.handle());
            let incarnation = ProviderBindingEpoch::new();
            let generic = Arc::new(PaimonGenericControl {
                descriptor: descriptor.clone(),
                incarnation,
                properties: catalog_properties.clone(),
                warehouse: Arc::clone(&warehouse),
                access: Arc::clone(&access),
                async_runtime: async_runtime.clone(),
            });
            let control = ConnectorControlBinding::try_new(
                descriptor.clone(),
                incarnation,
                generic.clone(),
                generic.clone(),
                generic,
                None,
            )
            .and_then(|binding| binding.with_catalog_properties(catalog_properties.clone()))
            .map_err(ConnectorMaterializationError::from)?;

            let template = PaimonReadRuntime::template(
                descriptor,
                catalog_properties.handle().clone(),
                async_runtime,
            );
            let adapter = ReadRuntimeAdapter::new(Arc::new(template.clone()));
            let codec: Arc<dyn ConnectorReadWireEncoder> =
                Arc::new(PaimonConnectorReadWireAdapter::new(adapter.clone()));
            let request_factory = Arc::new(PaimonRequestControlFactory {
                template,
                properties: catalog_properties,
                warehouse,
                access,
            });
            let read = ConnectorControlReadBinding::new(
                Arc::new(adapter.clone()),
                Arc::new(adapter),
                Some(request_factory),
                codec,
            );
            context.check_active()?;
            ConnectorControlRoleBinding::try_new(properties, Arc::new(control), Some(read), None)
                .map_err(ConnectorMaterializationError::from)
        })
    }
}

/// BE-only factory. Binding is pure and opens neither catalog nor object IO.
#[derive(Clone)]
pub struct PaimonExecutionRoleBindingFactory {
    access: Arc<dyn PaimonRoleFileIoFactory>,
    async_runtime: PaimonAsyncRuntime,
}

impl PaimonExecutionRoleBindingFactory {
    pub fn new(access: Arc<dyn PaimonRoleFileIoFactory>, runtime: tokio::runtime::Handle) -> Self {
        Self {
            access,
            async_runtime: PaimonAsyncRuntime::new(runtime),
        }
    }
}

impl ConnectorExecutionRoleBindingFactory for PaimonExecutionRoleBindingFactory {
    fn provider_id(&self) -> ConnectorProviderId {
        provider_id()
    }

    fn bind(
        &self,
        properties: &NormalizedCatalogProperties,
    ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError> {
        let catalog_properties = properties.as_catalog_properties();
        let warehouse = Arc::<str>::from(validate_properties(catalog_properties)?);
        let runtime = PaimonReadRuntime::template(
            descriptor(catalog_properties.handle()),
            catalog_properties.handle().clone(),
            self.async_runtime.clone(),
        );
        let adapter = ReadRuntimeAdapter::new(Arc::new(runtime));
        let decoder: Arc<dyn ConnectorReadWireDecoder> =
            Arc::new(PaimonConnectorReadWireAdapter::new(adapter.clone()));
        let factory = Arc::new(ProviderReadFactoryAdapter::new(
            adapter,
            Arc::new(PaimonExecutionReadFactory {
                properties: catalog_properties.clone(),
                warehouse,
                access: Arc::clone(&self.access),
                async_runtime: self.async_runtime.clone(),
            }),
        ));
        let read = ConnectorExecutionReadBinding::new(factory, decoder);
        ConnectorExecutionRoleBinding::try_new(properties.clone(), Some(read), None)
            .map_err(ConnectorMaterializationError::from)
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

struct PaimonExecutionReadFactory {
    properties: CatalogProperties,
    warehouse: Arc<str>,
    access: Arc<dyn PaimonRoleFileIoFactory>,
    async_runtime: PaimonAsyncRuntime,
}

impl ProviderReadFactory<PaimonReadRuntime> for PaimonExecutionReadFactory {
    fn create_page_source_provider(
        &self,
        request: &ConnectorRequestContext,
        _options: ConnectorPageSourceProviderOptions,
    ) -> Result<Arc<dyn ProviderReadPageSourceProvider<PaimonReadRuntime>>, ConnectorError> {
        active(request)?;
        let resources = PaimonRequestResources::new(request.resources()?.clone());
        let host_io = self
            .access
            .bind_file_io(&self.properties, &self.warehouse, request)?;
        Ok(Arc::new(PaimonExecutionPageSourceProvider {
            resources,
            host_io,
            async_runtime: self.async_runtime.clone(),
        }))
    }

    fn create_system_table_provider(
        &self,
        _request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn ProviderReadSystemTableProvider<PaimonReadRuntime>>, ConnectorError> {
        Err(unsupported("Paimon system tables are unsupported in PAI-1"))
    }
}

struct PaimonExecutionPageSourceProvider {
    resources: PaimonRequestResources,
    host_io: PaimonHostFileIo,
    async_runtime: PaimonAsyncRuntime,
}

fn validate_local_split_binding(
    table: &PaimonBoundTable,
    split: &PaimonSplit,
) -> Result<(), ConnectorError> {
    if table.table.location() != table.view.table_location()
        || split.schema_id() != table.view.schema_id()
        || table.view.snapshot_id() != Some(split.snapshot_id())
    {
        return Err(invalid(
            "Paimon table, frozen view and split do not identify one generation",
        ));
    }
    Ok(())
}

fn projected_columns(
    columns: &[Assignment<PaimonColumn>],
) -> Result<Vec<PaimonColumn>, ConnectorError> {
    columns
        .iter()
        .enumerate()
        .map(|(ordinal, assignment)| {
            let column = assignment.column();
            PaimonColumn::try_new(
                column.field_id(),
                column.name(),
                column.data_type(),
                column.nullable(),
                u32::try_from(ordinal)
                    .map_err(|_| exhausted("Paimon projection ordinal overflow"))?,
            )
        })
        .collect()
}

impl ProviderReadPageSourceProvider<PaimonReadRuntime> for PaimonExecutionPageSourceProvider {
    fn create_page_source(
        &self,
        _session: &ConnectorSession,
        table: &PaimonBoundTable,
        split: &PaimonSplit,
        _scheduled_split_sequence_id: u64,
        columns: &[Assignment<PaimonColumn>],
        _dynamic_filter: &Arc<dyn DynamicFilter<PaimonColumn>>,
    ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
        self.resources.checkpoint()?;
        validate_local_split_binding(table, split)?;
        let (sdk_table, options) = self.rebuild_table(table)?;
        if split
            .files()
            .iter()
            .any(|file| file.compression() != options.data_compression)
        {
            return Err(invalid(
                "Paimon split compression differs from the frozen read recipe",
            ));
        }
        let sdk_split = rebuild_split(split)?;
        let projected = projected_columns(columns)?;
        let reader = PaimonReader::try_new_with_runtime(
            Arc::new(sdk_table),
            &table.table,
            &table.view,
            split,
            sdk_split,
            &projected,
            Some(self.async_runtime.handle()),
        )?;
        Ok(Box::new(PaimonPageSource::new(
            Box::new(reader),
            self.resources.clone(),
            None,
        )))
    }
}

impl PaimonExecutionPageSourceProvider {
    fn rebuild_table(
        &self,
        table: &PaimonBoundTable,
    ) -> Result<(Table, crate::options::PaimonReadOptions), ConnectorError> {
        if table.table.location() != table.view.table_location() {
            return Err(invalid(
                "Paimon decoded table and view locations do not match",
            ));
        }
        let control = PaimonSdkReadControl::new(self.resources.clone());
        let file_io = FileIO::from_read_only(Arc::new(self.host_io.clone()), Arc::new(control));
        let name = novarocks_spi::connector::read_stack::ConnectorTableHandle::schema_table_name(
            &table.table,
        );
        let identifier = Identifier::new(name.schema_name(), name.table_name());
        let empty = Schema::builder().build().map_err(map_sdk_error)?;
        let placeholder = Table::new(
            file_io.clone(),
            identifier.clone(),
            table.table.location().to_owned(),
            TableSchema::new(0, &empty),
            None,
        );
        let schema_id = table.view.schema_id();
        let exact_schema = self
            .async_runtime
            .block_on(async move { placeholder.schema_manager().schema(schema_id).await })?
            .map_err(map_sdk_error)?;
        let options = validate_exact_schema(&exact_schema, &table.table, &table.view)?;
        let exact = Table::new(
            file_io,
            identifier,
            table.table.location().to_owned(),
            exact_schema.as_ref().clone(),
            None,
        );
        let table = match table.view.snapshot_id() {
            Some(snapshot_id) => exact.copy_with_options(HashMap::from([(
                "scan.snapshot-id".to_string(),
                snapshot_id.to_string(),
            )])),
            None => exact,
        };
        Ok((table, options))
    }
}

fn rebuild_split(split: &PaimonSplit) -> Result<DataSplit, ConnectorError> {
    let files = split
        .files()
        .iter()
        .map(|file| {
            let facts = file.facts();
            let creation_time = facts
                .creation_time_millis
                .map(|millis| {
                    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(millis)
                        .ok_or_else(|| corrupt("Paimon file creation time is out of range"))
                })
                .transpose()?;
            Ok(DataFileMeta {
                file_name: facts.file_name.clone(),
                file_size: i64::try_from(facts.file_size)
                    .map_err(|_| corrupt("Paimon file size overflows the SDK"))?,
                row_count: i64::try_from(facts.row_count)
                    .map_err(|_| corrupt("Paimon row count overflows the SDK"))?,
                min_key: facts.min_key.clone(),
                max_key: facts.max_key.clone(),
                key_stats: BinaryTableStats::new(
                    facts.key_stats.min_values().to_vec(),
                    facts.key_stats.max_values().to_vec(),
                    facts.key_stats.null_counts().to_vec(),
                ),
                value_stats: BinaryTableStats::new(
                    facts.value_stats.min_values().to_vec(),
                    facts.value_stats.max_values().to_vec(),
                    facts.value_stats.null_counts().to_vec(),
                ),
                min_sequence_number: facts.min_sequence_number,
                max_sequence_number: facts.max_sequence_number,
                schema_id: facts.schema_id,
                level: facts.level,
                extra_files: facts.extra_files.clone(),
                creation_time,
                delete_row_count: facts
                    .delete_row_count
                    .map(i64::try_from)
                    .transpose()
                    .map_err(|_| corrupt("Paimon delete row count overflows the SDK"))?,
                embedded_index: facts.embedded_index.clone(),
                file_source: facts.file_source,
                value_stats_cols: facts.value_stats_cols.clone(),
                external_path: facts.external_path.clone(),
                first_row_id: facts.first_row_id,
                write_cols: facts.write_cols.clone(),
            })
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let mut builder = DataSplitBuilder::new()
        .with_snapshot(split.snapshot_id())
        .with_partition(BinaryRow::from_bytes(
            split.partition_arity(),
            split.partition().to_vec(),
        ))
        .with_bucket(split.bucket())
        .with_bucket_path(split.bucket_path().to_owned())
        .with_total_buckets(split.total_buckets())
        .with_data_files(files)
        .with_raw_convertible(split.raw_convertible());
    if let Some(deletions) = split.data_deletion_files() {
        builder = builder.with_data_deletion_files(
            deletions
                .iter()
                .map(|deletion| {
                    deletion.as_ref().map(|value| {
                        DeletionFile::new(
                            value.path().to_owned(),
                            i64::try_from(value.offset())
                                .expect("validated Paimon deletion offset"),
                            i64::try_from(value.length())
                                .expect("validated Paimon deletion length"),
                            value.cardinality().map(|value| {
                                i64::try_from(value).expect("validated Paimon deletion cardinality")
                            }),
                        )
                    })
                })
                .collect(),
        );
    }
    if let Some(ranges) = split.row_ranges() {
        builder = builder.with_row_ranges(
            ranges
                .iter()
                .map(|range| RowRange::new(range.from(), range.to()))
                .collect(),
        );
    }
    builder.build().map_err(map_sdk_error)
}

fn validate_exact_schema(
    schema: &TableSchema,
    table: &PaimonTable,
    view: &PaimonReadView,
) -> Result<crate::options::PaimonReadOptions, ConnectorError> {
    if schema.id() != view.schema_id() {
        return Err(corrupt(
            "Paimon exact schema lookup returned another schema",
        ));
    }
    let columns = columns_from_schema(schema)?;
    if schema_fingerprint(schema)? != *view.schema_fingerprint() {
        return Err(corrupt(
            "Paimon exact schema does not match the frozen fingerprint",
        ));
    }
    let properties = schema
        .options()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let options = crate::options::PaimonReadOptions::analyze(
        &properties,
        &columns,
        table.primary_key_field_ids(),
        table.partition_field_ids(),
    )?;
    if read_recipe_digest(table, view.snapshot_id(), &options, &columns)
        != *view.read_recipe_digest()
    {
        return Err(corrupt(
            "Paimon exact schema does not match the frozen read recipe",
        ));
    }
    Ok(options)
}

#[derive(Clone)]
struct PaimonConnectorReadWireAdapter {
    adapter: ReadRuntimeAdapter<PaimonReadRuntime>,
    owner: Arc<str>,
}

impl PaimonConnectorReadWireAdapter {
    fn new(adapter: ReadRuntimeAdapter<PaimonReadRuntime>) -> Self {
        Self {
            owner: Arc::from(adapter.binding().descriptor().instance_id.as_str()),
            adapter,
        }
    }

    fn invalid(&self, path: ConnectorFieldPath, detail: impl AsRef<str>) -> ConnectorCodecError {
        ConnectorCodecError::new(path, ConnectorCodecErrorKind::InvalidValue, detail)
    }

    fn inconsistent(
        &self,
        path: ConnectorFieldPath,
        detail: impl AsRef<str>,
    ) -> ConnectorCodecError {
        ConnectorCodecError::new(path, ConnectorCodecErrorKind::InconsistentFields, detail)
    }

    fn private_rejection(&self, error: ConnectorCodecError) -> ConnectorCodecError {
        ConnectorCodecError::new(
            ConnectorFieldPath::root("provider_payload"),
            error.kind(),
            format!("{}: {}", error.path(), error.detail()),
        )
    }

    fn revision() -> ConnectorCodecRevision {
        ConnectorCodecRevision::try_new(PAIMON_READ_CODEC_REVISION)
            .expect("Paimon read codec revision is non-zero")
    }

    fn header(&self, category: ConnectorCodecCategory) -> ConnectorEnvelopeHeader {
        ConnectorEnvelopeHeader::new(
            self.adapter.binding().descriptor().provider_id.clone(),
            self.adapter.binding().catalog_handle().clone(),
            category,
            Self::revision(),
        )
    }

    fn envelope(
        &self,
        category: ConnectorCodecCategory,
        payload: Bytes,
    ) -> ConnectorEncodedPayload {
        ConnectorEncodedPayload::new(self.header(category), payload)
    }

    fn decode_limits() -> ConnectorDecodeLimits {
        ConnectorDecodeLimits::try_new(
            MAX_PRIVATE_READ_BYTES,
            MAX_PRIVATE_RETAINED_BYTES,
            MAX_PRIVATE_READ_BYTES,
            1_000_000,
            64,
        )
        .expect("Paimon read decode limits are finite")
    }

    fn decode_private<T>(
        &self,
        payload: &ConnectorEncodedPayload,
        category: ConnectorCodecCategory,
        decode: impl FnOnce(&[u8], &mut ConnectorDecodeContext<'_>) -> Result<T, ConnectorCodecError>,
    ) -> Result<T, ConnectorCodecError> {
        let header = self.header(category);
        payload
            .header()
            .validate_expected(
                header.provider_id(),
                header.catalog(),
                header.category(),
                header.codec_revision(),
            )
            .map_err(|error| self.private_rejection(error))?;
        let mut ledger = ConnectorDecodeLedger::new(Self::decode_limits());
        let mut context = ConnectorDecodeContext::new(&header, &mut ledger);
        decode(payload.payload(), &mut context).map_err(|error| self.private_rejection(error))
    }
}

impl ConnectorReadWireDecoder for PaimonConnectorReadWireAdapter {
    fn owner(&self) -> &str {
        &self.owner
    }

    fn decode_relation_payload(
        &self,
        relation: &ConnectorReadRelationPayload,
    ) -> Result<ConnectorReadRelation, ConnectorCodecError> {
        if relation.kind() != ConnectorReadRelationKind::Table {
            return Err(self.invalid(
                ConnectorFieldPath::root("catalog_table_handle").field("relation"),
                "Paimon supports only ordinary table relations in PAI-1",
            ));
        }
        let view = self.decode_private(
            relation.view(),
            ConnectorCodecCategory::ReadView,
            |payload, context| decode_read_view(payload, context),
        )?;
        let table = self.decode_private(
            relation.table(),
            ConnectorCodecCategory::ReadTable,
            |payload, context| decode_table(payload, context),
        )?;
        let bound = PaimonBoundTable::decoded(table, view.clone()).map_err(|error| {
            self.inconsistent(
                ConnectorFieldPath::root("catalog_table_handle"),
                error.to_string(),
            )
        })?;
        Ok(ConnectorReadRelation::new(
            ConnectorReadRelationKind::Table,
            self.adapter.wrap_table(bound),
            self.adapter.wrap_transaction(Some(view)),
        ))
    }

    fn decode_column_payload(
        &self,
        column: &ConnectorEncodedPayload,
    ) -> Result<ConnectorReadColumnHandle, ConnectorCodecError> {
        let value = self.decode_private(
            column,
            ConnectorCodecCategory::ReadColumn,
            |payload, context| decode_column(payload, context),
        )?;
        Ok(self.adapter.wrap_column(value))
    }

    fn decode_transaction_payload(
        &self,
        transaction: &ConnectorEncodedPayload,
    ) -> Result<ConnectorReadTransactionHandle, ConnectorCodecError> {
        let value = self.decode_private(
            transaction,
            ConnectorCodecCategory::ReadView,
            |payload, context| decode_read_view(payload, context),
        )?;
        Ok(self.adapter.wrap_transaction(Some(value)))
    }

    fn decode_split_payload(
        &self,
        split: &ConnectorReadSplitPayload,
        facts: &ConnectorReadSplitFacts,
    ) -> Result<ConnectorReadSplit, ConnectorCodecError> {
        if split.category() != ConnectorReadSplitCategory::Data {
            return Err(self.invalid(
                ConnectorFieldPath::root("connector_split").field("category"),
                "Paimon supports only ordinary data splits in PAI-1",
            ));
        }
        let value = self.decode_private(
            split.provider_payload(),
            ConnectorCodecCategory::ReadSplit,
            |payload, context| PaimonReadWireCodec.decode_split_private(payload, facts, context),
        )?;
        Ok(self.adapter.wrap_split(value))
    }
}

impl ConnectorReadWireEncoder for PaimonConnectorReadWireAdapter {
    fn owner(&self) -> &str {
        &self.owner
    }

    fn encode_relation_payload(
        &self,
        relation: &ConnectorReadRelation,
    ) -> Result<ConnectorReadRelationPayload, ConnectorCodecError> {
        if relation.kind() != ConnectorReadRelationKind::Table {
            return Err(self.invalid(
                ConnectorFieldPath::root("relation").field("kind"),
                "Paimon supports only ordinary table relations in PAI-1",
            ));
        }
        let table = self.adapter.table(relation.table()).map_err(|error| {
            self.invalid(
                ConnectorFieldPath::root("relation").field("table"),
                error.to_string(),
            )
        })?;
        let transaction = self
            .adapter
            .transaction(relation.transaction())
            .map_err(|error| {
                self.invalid(
                    ConnectorFieldPath::root("relation").field("transaction"),
                    error.to_string(),
                )
            })?;
        if transaction.is_some() {
            return Err(self.inconsistent(
                ConnectorFieldPath::root("relation").field("transaction"),
                "frontend Paimon relation carries a second read-view authority",
            ));
        }
        let table_payload =
            encode_table(&table.table).map_err(|error| self.private_rejection(error))?;
        let view_payload =
            encode_read_view(&table.view).map_err(|error| self.private_rejection(error))?;
        Ok(ConnectorReadRelationPayload::new(
            ConnectorReadRelationKind::Table,
            self.envelope(ConnectorCodecCategory::ReadTable, table_payload),
            self.envelope(ConnectorCodecCategory::ReadView, view_payload),
        ))
    }

    fn encode_column_payload(
        &self,
        column: &ConnectorReadColumnHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
        let value = self.adapter.column(column).map_err(|error| {
            self.invalid(ConnectorFieldPath::root("column_handle"), error.to_string())
        })?;
        let payload = encode_column(value).map_err(|error| self.private_rejection(error))?;
        Ok(self.envelope(ConnectorCodecCategory::ReadColumn, payload))
    }

    fn encode_transaction_payload(
        &self,
        _transaction: &ConnectorReadTransactionHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
        Err(self.invalid(
            ConnectorFieldPath::root("transaction_handle"),
            "Paimon read views are encoded atomically with their table relation",
        ))
    }

    fn encode_split_payload(
        &self,
        split: &ConnectorReadSplit,
    ) -> Result<ConnectorReadSplitPayload, ConnectorCodecError> {
        let value = self.adapter.split(split).map_err(|error| {
            self.invalid(
                ConnectorFieldPath::root("connector_split"),
                error.to_string(),
            )
        })?;
        let payload = encode_split(value).map_err(|error| self.private_rejection(error))?;
        Ok(ConnectorReadSplitPayload::new(
            ConnectorReadSplitCategory::Data,
            self.envelope(ConnectorCodecCategory::ReadSplit, payload),
        ))
    }
}

fn encode_table(value: &PaimonTable) -> Result<Bytes, ConnectorCodecError> {
    let _: &<PaimonReadTypes as ProviderReadTypes>::Table = value;
    novarocks_spi::connector::ConnectorPrivateEncoder::encode_private(&PaimonReadWireCodec, value)
}

fn decode_table(
    payload: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<PaimonTable, ConnectorCodecError> {
    novarocks_spi::connector::ConnectorPrivateDecoder::decode_private(
        &PaimonReadWireCodec,
        payload,
        context,
    )
}

fn encode_column(value: &PaimonColumn) -> Result<Bytes, ConnectorCodecError> {
    novarocks_spi::connector::ConnectorPrivateEncoder::encode_private(&PaimonReadWireCodec, value)
}

fn decode_column(
    payload: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<PaimonColumn, ConnectorCodecError> {
    novarocks_spi::connector::ConnectorPrivateDecoder::decode_private(
        &PaimonReadWireCodec,
        payload,
        context,
    )
}

fn encode_read_view(value: &PaimonReadView) -> Result<Bytes, ConnectorCodecError> {
    novarocks_spi::connector::ConnectorPrivateEncoder::encode_private(&PaimonReadWireCodec, value)
}

fn decode_read_view(
    payload: &[u8],
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<PaimonReadView, ConnectorCodecError> {
    novarocks_spi::connector::ConnectorPrivateDecoder::decode_private(
        &PaimonReadWireCodec,
        payload,
        context,
    )
}

fn encode_split(value: &PaimonSplit) -> Result<Bytes, ConnectorCodecError> {
    novarocks_spi::connector::ConnectorPrivateEncoder::encode_private(&PaimonReadWireCodec, value)
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

fn active(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "Paimon connector request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Paimon connector request deadline elapsed",
        ));
    }
    context.resources()?.checkpoint().map(|_| ())
}

fn invalid_definition(detail: impl AsRef<str>) -> ConnectorMaterializationError {
    ConnectorMaterializationError::new(
        ConnectorMaterializationErrorClass::InvalidDefinition,
        ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
        detail,
    )
}

fn request_binding_required() -> ConnectorError {
    invalid("Paimon read operation requires an admitted request binding")
}

fn codec_as_connector_error(error: ConnectorCodecError) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        format!("Paimon private codec rejected trusted metadata: {error}"),
    )
}

fn invalid(detail: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, detail)
}

fn unsupported(detail: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, detail)
}

fn corrupt(detail: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, detail)
}

fn exhausted(detail: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, detail)
}

fn internal(detail: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, detail)
}

fn schema_fingerprint(schema: &TableSchema) -> Result<[u8; 32], ConnectorError> {
    let columns = columns_from_schema(schema)?;
    let mut hash = Sha256::new();
    digest_i64(&mut hash, schema.id());
    for column in columns {
        digest_i64(&mut hash, i64::from(column.field_id()));
        digest_bytes(&mut hash, column.name().as_bytes());
        digest_bytes(&mut hash, format!("{:?}", column.data_type()).as_bytes());
        hash.update([u8::from(column.nullable())]);
    }
    digest_names(&mut hash, schema.primary_keys());
    digest_names(&mut hash, schema.partition_keys());
    Ok(hash.finalize().into())
}

fn read_recipe_digest(
    table: &PaimonTable,
    snapshot_id: Option<i64>,
    options: &crate::options::PaimonReadOptions,
    columns: &[PaimonColumn],
) -> [u8; 32] {
    let mut hash = Sha256::new();
    digest_bytes(&mut hash, b"novarocks-paimon-read-v1");
    digest_bytes(&mut hash, table.location().as_bytes());
    digest_i64(&mut hash, snapshot_id.unwrap_or(-1));
    digest_bytes(&mut hash, format!("{:?}", options.merge_engine).as_bytes());
    digest_bytes(&mut hash, format!("{:?}", options.bucket_mode).as_bytes());
    digest_bytes(
        &mut hash,
        format!("{:?}", options.data_compression).as_bytes(),
    );
    digest_i64(&mut hash, options.sequence_field_id.unwrap_or(-1) as i64);
    for column in columns {
        digest_i64(&mut hash, i64::from(column.field_id()));
        digest_bytes(&mut hash, column.name().as_bytes());
        digest_bytes(&mut hash, format!("{:?}", column.data_type()).as_bytes());
        hash.update([u8::from(column.nullable())]);
    }
    hash.finalize().into()
}

fn digest_names(hash: &mut Sha256, values: &[String]) {
    for value in values {
        digest_bytes(hash, value.as_bytes());
    }
}

fn digest_i64(hash: &mut Sha256, value: i64) {
    hash.update(value.to_be_bytes());
}

fn digest_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::read_stack::{Assignment, ConnectorValueType};
    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperty, CatalogVersion, ConnectorInstanceId,
    };

    use super::*;

    fn properties(provider: &str, catalog_type: &str) -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("lake").expect("catalog"),
                CatalogVersion::from_bytes([7; 32]),
            ),
            ConnectorProviderId::parse(provider).expect("provider"),
            1,
            vec![
                CatalogProperty::new(PAIMON_CATALOG_TYPE_KEY, catalog_type).expect("property"),
                CatalogProperty::new(PAIMON_WAREHOUSE_KEY, "s3://warehouse/paimon")
                    .expect("property"),
            ],
            Vec::new(),
        )
        .expect("catalog properties")
    }

    #[test]
    fn role_properties_accept_only_the_filesystem_contract() {
        assert_eq!(
            validate_properties(&properties(PROVIDER_ID, PAIMON_FILESYSTEM_BINDING)).unwrap(),
            "s3://warehouse/paimon"
        );
        assert!(validate_properties(&properties(PROVIDER_ID, "rest")).is_err());
        assert!(validate_properties(&properties("iceberg", PAIMON_FILESYSTEM_BINDING)).is_err());
    }

    #[test]
    fn role_definition_exposes_no_write_contract() {
        let contract = paimon_contract_definition().expect("Paimon contract");
        assert!(contract.write().is_none());
        assert_eq!(contract.declarations().len(), 4);
    }

    #[test]
    fn request_cache_keys_same_named_tables_by_exact_catalog_generation() {
        let cache = PaimonRequestCache::default();
        let name = SchemaTableName::try_new("db", "same_name").expect("table name");
        let catalog_a = CatalogHandle::new(
            ConnectorInstanceId::parse("catalog_a").expect("catalog"),
            CatalogVersion::from_bytes([1; 32]),
        );
        let catalog_b = CatalogHandle::new(
            ConnectorInstanceId::parse("catalog_b").expect("catalog"),
            CatalogVersion::from_bytes([2; 32]),
        );

        let first = cache
            .get_or_prepare(&catalog_a, &name, || Err(invalid("catalog-a")))
            .expect_err("fixture preparation fails");
        let second = cache
            .get_or_prepare(&catalog_b, &name, || Err(invalid("catalog-b")))
            .expect_err("another catalog has an independent slot");
        let replay = cache
            .get_or_prepare(&catalog_a, &name, || Err(invalid("must-not-run")))
            .expect_err("same generation replays its cached result");

        assert_eq!(first.message(), "catalog-a");
        assert_eq!(second.message(), "catalog-b");
        assert_eq!(replay.message(), "catalog-a");
    }

    #[test]
    fn projected_columns_rebase_subset_reorder_and_duplicate_ordinals() {
        let id = PaimonColumn::try_new(1, "id", crate::schema::PaimonDataType::Int64, false, 0)
            .expect("id column");
        let value = PaimonColumn::try_new(2, "value", crate::schema::PaimonDataType::Utf8, true, 1)
            .expect("value column");
        let assignments = vec![
            Assignment::try_new("v", value.clone(), ConnectorValueType::Varchar)
                .expect("value assignment"),
            Assignment::try_new("i", id, ConnectorValueType::BigInt).expect("id assignment"),
            Assignment::try_new("v_again", value, ConnectorValueType::Varchar)
                .expect("duplicate column assignment"),
        ];

        let projected = projected_columns(&assignments).expect("rebased projection");
        assert_eq!(
            projected
                .iter()
                .map(|column| (column.field_id(), column.output_ordinal()))
                .collect::<Vec<_>>(),
            vec![(2, 0), (1, 1), (2, 2)]
        );
    }
}
