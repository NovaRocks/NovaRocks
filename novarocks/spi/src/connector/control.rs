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

use std::sync::{Arc, Mutex};

use super::{
    CatalogHandle, CatalogProperties, ConnectorBeginScanRequest, ConnectorCatalogMutation,
    ConnectorCatalogMutationResolver, ConnectorCleanupMaintenance,
    ConnectorCleanupMaintenanceResolver, ConnectorControlRuntimeId, ConnectorDataMutation,
    ConnectorDataMutationResolver, ConnectorDistributedRewrite,
    ConnectorDistributedRewriteResolver, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMetadata,
    ConnectorMetadataMaintenance, ConnectorMetadataMaintenanceResolver, ConnectorProviderBinding,
    ConnectorProviderBindingKey, ConnectorProviderId, ConnectorRequestContext, ConnectorScan,
    ConnectorScanHandle, ConnectorSplitPlanningRequest, ConnectorSplitPlanningResult,
    ConnectorStagedCreate, ConnectorStagedCreateLease, ConnectorStatistics,
    ConnectorStatisticsLease, ConnectorStatisticsResolver, ConnectorTableHandle,
    ConnectorUnanchoredCtasCleanup, ConnectorUnanchoredCtasCleanupLease, ConnectorViewMetadata,
    ConnectorWriteControl, ConnectorWriteLease, ProviderBindingEpoch,
};

/// FE-only capability for planning a read after metadata has resolved a table.
/// It intentionally has no reader-opening method.
pub trait ConnectorScanPlanning: Send + Sync {
    fn instance_id(&self) -> &ConnectorInstanceId;

    fn begin_scan(
        &self,
        table: &ConnectorTableHandle,
        request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError>;

    fn plan_splits(
        &self,
        scan: &ConnectorScanHandle,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError>;
}

/// FE-only capability that turns a logical control binding into the typed
/// Protocol declaration accepted by a BE execution installer.
pub trait ConnectorExecutionDistribution: Send + Sync {
    fn declaration(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorProviderBinding, ConnectorError>;
}

/// Process-local input used by a frontend composition root when a catalog
/// attachment is created or restored. Properties are intentionally kept out of
/// the native wire contract; the provider uses them to resolve local clients
/// and credentials before returning a control binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorControlFactoryRequest {
    provider_id: ConnectorProviderId,
    instance_id: ConnectorInstanceId,
    properties: Vec<(String, String)>,
    catalog_properties: Option<CatalogProperties>,
}

impl ConnectorControlFactoryRequest {
    pub fn try_new(
        provider_id: ConnectorProviderId,
        instance_id: ConnectorInstanceId,
        properties: Vec<(String, String)>,
    ) -> Result<Self, ConnectorError> {
        let mut seen = std::collections::BTreeSet::new();
        for (key, _) in &properties {
            if key.trim().is_empty() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector catalog property key must not be empty",
                ));
            }
            if !seen.insert(key.as_str()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("duplicate connector catalog property: {key}"),
                ));
            }
        }
        Ok(Self {
            provider_id,
            instance_id,
            properties,
            catalog_properties: None,
        })
    }

    pub fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    pub fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    pub fn properties(&self) -> &[(String, String)] {
        &self.properties
    }

    pub fn into_properties(self) -> Vec<(String, String)> {
        self.properties
    }

    /// Supplies the immutable desired-state definition that authorizes this
    /// process-local control construction. Provider factories which require
    /// filesystem or credential access must require this value before opening
    /// a client; the untyped request properties are only their durable input
    /// representation.
    pub fn with_catalog_properties(
        mut self,
        catalog_properties: CatalogProperties,
    ) -> Result<Self, ConnectorError> {
        if catalog_properties.handle().catalog_name() != &self.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog properties owner does not match connector control factory request",
            ));
        }
        self.catalog_properties = Some(catalog_properties);
        Ok(self)
    }

    pub fn catalog_properties(&self) -> Option<&CatalogProperties> {
        self.catalog_properties.as_ref()
    }
}

/// The result of provider-local control construction. Durable properties are
/// the only attachment properties that the application owner may persist;
/// credentials, tokens, and process-local client handles must never be
/// returned in this field.
pub struct ConnectorControlCreation {
    binding: ConnectorControlBinding,
    durable_properties: Vec<(String, String)>,
}

impl ConnectorControlCreation {
    pub fn try_new(
        request: &ConnectorControlFactoryRequest,
        binding: ConnectorControlBinding,
        durable_properties: Vec<(String, String)>,
    ) -> Result<Self, ConnectorError> {
        let descriptor = binding.descriptor();
        if descriptor.provider_id != *request.provider_id()
            || descriptor.instance_id != *request.instance_id()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector control factory returned a binding for a different owner",
            ));
        }
        validate_durable_properties(&durable_properties)?;
        Ok(Self {
            binding,
            durable_properties,
        })
    }

    pub fn binding(&self) -> &ConnectorControlBinding {
        &self.binding
    }

    pub fn durable_properties(&self) -> &[(String, String)] {
        &self.durable_properties
    }

    pub fn into_parts(self) -> (ConnectorControlBinding, Vec<(String, String)>) {
        (self.binding, self.durable_properties)
    }
}

fn validate_durable_properties(properties: &[(String, String)]) -> Result<(), ConnectorError> {
    let mut seen = std::collections::BTreeSet::new();
    for (key, _) in properties {
        if key.trim().is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "durable connector property key must not be empty",
            ));
        }
        if !seen.insert(key.as_str()) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("duplicate durable connector property: {key}"),
            ));
        }
        let normalized = key.to_ascii_lowercase();
        if [
            "password",
            "secret",
            "token",
            "credential",
            "access-key",
            "access_key",
            "private-key",
            "private_key",
        ]
        .iter()
        .any(|marker| normalized.contains(marker))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("credential-like property cannot be durable: {key}"),
            ));
        }
    }
    Ok(())
}

/// Provider-owned factory for one frontend control generation.
pub trait ConnectorControlFactory: Send + Sync {
    fn provider_id(&self) -> &ConnectorProviderId;

    fn create_control(
        &self,
        request: ConnectorControlFactoryRequest,
    ) -> Result<ConnectorControlCreation, ConnectorError>;
}

/// Narrow frontend port used by Core restore and attachment code. The
/// resolver owns provider lookup, duplicate rejection, and returned-binding
/// validation; Core only submits this typed request.
pub trait ConnectorControlFactoryResolver: Send + Sync {
    fn create_control(
        &self,
        request: ConnectorControlFactoryRequest,
    ) -> Result<ConnectorControlCreation, ConnectorError>;
}

/// A control-plane Connector generation. Metadata, scan planning, and
/// execution distribution must all describe the same logical descriptor and
/// legacy effect generation. Its separately minted control-runtime ID is an
/// FE-local owner and is deliberately unable to open a batch reader.
pub struct ConnectorControlBinding {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    control_runtime_id: ConnectorControlRuntimeId,
    /// Immutable BE materialization input assigned by the FE desired-state
    /// owner before this control binding becomes query-admissible.
    ///
    /// Retaining the complete value, rather than only its handle, is what lets
    /// query assembly carry one exact `CatalogSet` in Init without re-reading
    /// desired state or reconstructing provider configuration.
    catalog_properties: Option<CatalogProperties>,
    catalog_handle_installer:
        Option<Arc<dyn Fn(&CatalogHandle) -> Result<(), ConnectorError> + Send + Sync>>,
    metadata: Arc<dyn ConnectorMetadata>,
    planning: Arc<dyn ConnectorScanPlanning>,
    distribution: Arc<dyn ConnectorExecutionDistribution>,
    mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
    data_mutation: Option<Arc<dyn ConnectorDataMutation>>,
    metadata_maintenance: Option<Arc<dyn ConnectorMetadataMaintenance>>,
    distributed_rewrite: Option<Arc<dyn ConnectorDistributedRewrite>>,
    cleanup_maintenance: Option<Arc<dyn ConnectorCleanupMaintenance>>,
    staged_create: Option<Arc<dyn ConnectorStagedCreate>>,
    unanchored_ctas_cleanup: Option<Arc<dyn ConnectorUnanchoredCtasCleanup>>,
    write: Option<Arc<dyn ConnectorWriteControl>>,
    statistics: Option<Arc<dyn ConnectorStatistics>>,
    view_metadata: Option<Arc<dyn ConnectorViewMetadata>>,
}

impl ConnectorControlBinding {
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_capabilities(
            descriptor,
            incarnation,
            metadata,
            planning,
            distribution,
            mutation,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_write(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
        write: Option<Arc<dyn ConnectorWriteControl>>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_capabilities(
            descriptor,
            incarnation,
            metadata,
            planning,
            distribution,
            mutation,
            write,
            None,
        )
    }

    pub fn try_new_with_statistics(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
        statistics: Option<Arc<dyn ConnectorStatistics>>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_capabilities(
            descriptor,
            incarnation,
            metadata,
            planning,
            distribution,
            mutation,
            None,
            statistics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_data_mutation(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
        data_mutation: Option<Arc<dyn ConnectorDataMutation>>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_all_capabilities(
            descriptor,
            incarnation,
            metadata,
            planning,
            distribution,
            mutation,
            data_mutation,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_capabilities(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
        write: Option<Arc<dyn ConnectorWriteControl>>,
        statistics: Option<Arc<dyn ConnectorStatistics>>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_all_capabilities(
            descriptor,
            incarnation,
            metadata,
            planning,
            distribution,
            mutation,
            None,
            write,
            statistics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_all_capabilities(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
        data_mutation: Option<Arc<dyn ConnectorDataMutation>>,
        write: Option<Arc<dyn ConnectorWriteControl>>,
        statistics: Option<Arc<dyn ConnectorStatistics>>,
    ) -> Result<Self, ConnectorError> {
        if metadata.instance_id() != &descriptor.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector metadata capability owner does not match its control binding",
            ));
        }
        if planning.instance_id() != &descriptor.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector scan planning capability owner does not match its control binding",
            ));
        }
        if let Some(mutation) = &mutation
            && (mutation.descriptor() != &descriptor || mutation.incarnation() != incarnation)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector mutation capability owner does not match its control binding generation",
            ));
        }
        if let Some(data_mutation) = &data_mutation {
            super::data_mutation::validate_data_mutation_owner(
                &descriptor,
                incarnation,
                data_mutation.as_ref(),
            )?;
        }
        if write.as_ref().is_some_and(|write| {
            write.binding_key().instance_id != descriptor.instance_id
                || write.binding_key().incarnation != incarnation
        }) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write capability owner does not match its control binding generation",
            ));
        }
        if let Some(statistics) = &statistics {
            super::statistics::validate_statistics_owner(
                &descriptor,
                incarnation,
                statistics.as_ref(),
            )?;
        }
        Ok(Self {
            descriptor,
            incarnation,
            control_runtime_id: ConnectorControlRuntimeId::new(),
            catalog_properties: None,
            catalog_handle_installer: None,
            metadata,
            planning,
            distribution,
            mutation,
            data_mutation,
            metadata_maintenance: None,
            distributed_rewrite: None,
            cleanup_maintenance: None,
            staged_create: None,
            unanchored_ctas_cleanup: None,
            write,
            statistics,
            view_metadata: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_all_capabilities_and_metadata_maintenance(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
        data_mutation: Option<Arc<dyn ConnectorDataMutation>>,
        metadata_maintenance: Option<Arc<dyn ConnectorMetadataMaintenance>>,
        write: Option<Arc<dyn ConnectorWriteControl>>,
        statistics: Option<Arc<dyn ConnectorStatistics>>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_all_capabilities_and_staged_create(
            descriptor,
            incarnation,
            metadata,
            planning,
            distribution,
            mutation,
            data_mutation,
            metadata_maintenance,
            None,
            write,
            statistics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_all_capabilities_and_staged_create(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
        data_mutation: Option<Arc<dyn ConnectorDataMutation>>,
        metadata_maintenance: Option<Arc<dyn ConnectorMetadataMaintenance>>,
        staged_create: Option<Arc<dyn ConnectorStagedCreate>>,
        write: Option<Arc<dyn ConnectorWriteControl>>,
        statistics: Option<Arc<dyn ConnectorStatistics>>,
    ) -> Result<Self, ConnectorError> {
        if let Some(maintenance) = &metadata_maintenance {
            super::metadata_maintenance::validate_metadata_maintenance_owner(
                &descriptor,
                incarnation,
                maintenance.as_ref(),
            )?;
        }
        if staged_create.as_ref().is_some_and(|capability| {
            capability.descriptor() != &descriptor || capability.incarnation() != incarnation
        }) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "staged-create capability owner does not match its control binding generation",
            ));
        }
        let mut binding = Self::try_new_with_all_capabilities(
            descriptor,
            incarnation,
            metadata,
            planning,
            distribution,
            mutation,
            data_mutation,
            write,
            statistics,
        )?;
        binding.metadata_maintenance = metadata_maintenance;
        binding.staged_create = staged_create;
        Ok(binding)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_all_maintenance_capabilities(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
        data_mutation: Option<Arc<dyn ConnectorDataMutation>>,
        metadata_maintenance: Option<Arc<dyn ConnectorMetadataMaintenance>>,
        distributed_rewrite: Option<Arc<dyn ConnectorDistributedRewrite>>,
        write: Option<Arc<dyn ConnectorWriteControl>>,
        statistics: Option<Arc<dyn ConnectorStatistics>>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_all_maintenance_capabilities_and_staged_create(
            descriptor,
            incarnation,
            metadata,
            planning,
            distribution,
            mutation,
            data_mutation,
            metadata_maintenance,
            distributed_rewrite,
            None,
            write,
            statistics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_all_maintenance_capabilities_and_staged_create(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
        data_mutation: Option<Arc<dyn ConnectorDataMutation>>,
        metadata_maintenance: Option<Arc<dyn ConnectorMetadataMaintenance>>,
        distributed_rewrite: Option<Arc<dyn ConnectorDistributedRewrite>>,
        staged_create: Option<Arc<dyn ConnectorStagedCreate>>,
        write: Option<Arc<dyn ConnectorWriteControl>>,
        statistics: Option<Arc<dyn ConnectorStatistics>>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_all_maintenance_capabilities_cleanup_and_staged_create(
            descriptor,
            incarnation,
            metadata,
            planning,
            distribution,
            mutation,
            data_mutation,
            metadata_maintenance,
            distributed_rewrite,
            None,
            staged_create,
            write,
            statistics,
        )
    }

    /// Constructs a control binding with all FE-only maintenance facets.
    /// Cleanup deliberately remains absent from BE execution bindings.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_all_maintenance_capabilities_cleanup_and_staged_create(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
        data_mutation: Option<Arc<dyn ConnectorDataMutation>>,
        metadata_maintenance: Option<Arc<dyn ConnectorMetadataMaintenance>>,
        distributed_rewrite: Option<Arc<dyn ConnectorDistributedRewrite>>,
        cleanup_maintenance: Option<Arc<dyn ConnectorCleanupMaintenance>>,
        staged_create: Option<Arc<dyn ConnectorStagedCreate>>,
        write: Option<Arc<dyn ConnectorWriteControl>>,
        statistics: Option<Arc<dyn ConnectorStatistics>>,
    ) -> Result<Self, ConnectorError> {
        if let Some(rewrite) = &distributed_rewrite {
            super::distributed_rewrite::validate_distributed_rewrite_owner(
                &descriptor,
                incarnation,
                rewrite.as_ref(),
            )?;
        }
        if let Some(cleanup) = &cleanup_maintenance {
            let key = ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            };
            super::cleanup_maintenance::validate_cleanup_maintenance_owner(
                &descriptor,
                &key,
                cleanup.as_ref(),
            )?;
        }
        let mut binding = Self::try_new_with_all_capabilities_and_staged_create(
            descriptor,
            incarnation,
            metadata,
            planning,
            distribution,
            mutation,
            data_mutation,
            metadata_maintenance,
            staged_create,
            write,
            statistics,
        )?;
        binding.distributed_rewrite = distributed_rewrite;
        binding.cleanup_maintenance = cleanup_maintenance;
        Ok(binding)
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }

    /// Returns this process-local FE control runtime identity. It is distinct
    /// from the provider's legacy effect generation and from the BE-visible
    /// `CatalogHandle`; neither may be derived from the other.
    pub fn control_runtime_id(&self) -> ConnectorControlRuntimeId {
        self.control_runtime_id
    }

    /// Stamps this FE-local control generation with the exact immutable BE
    /// materialization input derived from authoritative desired state. Provider
    /// factories cannot mint this value because it must not depend on a
    /// process-local control incarnation.
    pub fn with_catalog_properties(
        mut self,
        catalog_properties: CatalogProperties,
    ) -> Result<Self, ConnectorError> {
        let catalog_handle = catalog_properties.handle();
        if catalog_handle.catalog_name() != &self.descriptor.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog handle owner does not match its control binding",
            ));
        }
        if let Some(installer) = self.catalog_handle_installer.as_ref() {
            installer(catalog_handle)?;
        }
        self.catalog_properties = Some(catalog_properties);
        Ok(self)
    }

    /// Attach provider-owned, FE-local work which can only be installed after
    /// desired state has frozen the immutable catalog identity.
    pub fn with_catalog_handle_installer(
        mut self,
        installer: Arc<dyn Fn(&CatalogHandle) -> Result<(), ConnectorError> + Send + Sync>,
    ) -> Self {
        self.catalog_handle_installer = Some(installer);
        self
    }

    /// Returns the execution identity when this control generation also owns
    /// a BE-materializable catalog binding.
    ///
    /// A control-only generation legitimately returns `None`; callers that
    /// require execution properties must continue to use `catalog_handle` or
    /// `catalog_properties` and fail closed.
    pub fn execution_catalog_handle(&self) -> Option<&CatalogHandle> {
        self.catalog_properties
            .as_ref()
            .map(CatalogProperties::handle)
    }

    /// Returns the desired-state-derived execution identity. A binding which
    /// was not admitted through the catalog application owner must fail
    /// closed instead of deriving an identity from its control incarnation.
    pub fn catalog_handle(&self) -> Result<&CatalogHandle, ConnectorError> {
        self.catalog_properties().map(CatalogProperties::handle)
    }

    /// Returns the complete frozen materialization input for the exact catalog
    /// handle. Query assembly must carry this value verbatim in its Init
    /// `CatalogSet`; it must not derive a substitute from a control runtime.
    pub fn catalog_properties(&self) -> Result<&CatalogProperties, ConnectorError> {
        self.catalog_properties.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector control binding has no catalog execution properties",
            )
        })
    }

    pub fn metadata(&self) -> &Arc<dyn ConnectorMetadata> {
        &self.metadata
    }

    pub fn planning(&self) -> &Arc<dyn ConnectorScanPlanning> {
        &self.planning
    }

    pub fn mutation(&self) -> Option<&Arc<dyn ConnectorCatalogMutation>> {
        self.mutation.as_ref()
    }

    pub fn data_mutation(&self) -> Option<&Arc<dyn ConnectorDataMutation>> {
        self.data_mutation.as_ref()
    }

    pub fn metadata_maintenance(&self) -> Option<&Arc<dyn ConnectorMetadataMaintenance>> {
        self.metadata_maintenance.as_ref()
    }

    pub fn distributed_rewrite(&self) -> Option<&Arc<dyn ConnectorDistributedRewrite>> {
        self.distributed_rewrite.as_ref()
    }

    pub fn cleanup_maintenance(&self) -> Option<&Arc<dyn ConnectorCleanupMaintenance>> {
        self.cleanup_maintenance.as_ref()
    }

    pub fn staged_create(&self) -> Option<&Arc<dyn ConnectorStagedCreate>> {
        self.staged_create.as_ref()
    }

    pub fn unanchored_ctas_cleanup(&self) -> Option<&Arc<dyn ConnectorUnanchoredCtasCleanup>> {
        self.unanchored_ctas_cleanup.as_ref()
    }

    pub fn write(&self) -> Option<&Arc<dyn ConnectorWriteControl>> {
        self.write.as_ref()
    }

    pub fn execution_distribution(&self) -> &Arc<dyn ConnectorExecutionDistribution> {
        &self.distribution
    }

    pub fn statistics(&self) -> Option<&Arc<dyn ConnectorStatistics>> {
        self.statistics.as_ref()
    }

    pub fn view_metadata(&self) -> Option<&Arc<dyn ConnectorViewMetadata>> {
        self.view_metadata.as_ref()
    }

    /// Attaches the optional view metadata capability to this exact control
    /// generation after the common mandatory capabilities have been validated.
    pub fn try_with_view_metadata(
        mut self,
        view_metadata: Option<Arc<dyn ConnectorViewMetadata>>,
    ) -> Result<Self, ConnectorError> {
        if let Some(capability) = &view_metadata {
            super::view_metadata::validate_view_metadata_owner(
                &self.descriptor,
                self.incarnation,
                capability.as_ref(),
            )?;
        }
        self.view_metadata = view_metadata;
        Ok(self)
    }

    /// Attaches the catalog-wide CTAS staging-root collector to this exact
    /// generation. The capability is independent from table-owned cleanup:
    /// before a CREATE publishes its target, no table location can anchor the
    /// staged root.
    pub fn try_with_unanchored_ctas_cleanup(
        mut self,
        capability: Option<Arc<dyn ConnectorUnanchoredCtasCleanup>>,
    ) -> Result<Self, ConnectorError> {
        if let Some(capability) = &capability
            && (capability.descriptor() != &self.descriptor
                || capability.incarnation() != self.incarnation)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "unanchored CTAS cleanup capability owner does not match its control binding generation",
            ));
        }
        self.unanchored_ctas_cleanup = capability;
        Ok(self)
    }

    pub fn provider_binding(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorProviderBinding, ConnectorError> {
        let declaration = self.distribution.declaration(context)?;
        let key = declaration.binding_key();
        if declaration.provider_id() != &self.descriptor.provider_id
            || key.instance_id != self.descriptor.instance_id
            || key.incarnation != self.incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector provider binding does not match its control binding generation",
            ));
        }
        Ok(declaration)
    }
}

/// Narrow consumer port used by core planning code. Its implementation belongs
/// to the frontend process; core neither owns the control registry nor creates
/// a control binding.
pub trait ConnectorControlResolver: Send + Sync {
    /// Read the active binding identity without retaining a generation. SQL
    /// preparation uses this only as an observation to be checked again when
    /// the frontend acquires its exact lifecycle lease.
    fn observe_current_binding(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorProviderBindingKey, ConnectorError>;

    /// Observe the active FE-local control runtime without retaining it.
    /// This identity is intentionally separate from the legacy effect key and
    /// must never be sent to a backend catalog lookup.
    fn observe_current_control_runtime(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorControlRuntimeId, ConnectorError>;

    fn acquire_current(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorControlPlanningLease, ConnectorError>;
}

/// Lifecycle port owned by the frontend composition root. Core may register
/// or retire a logical control generation, but it never owns the registry.
pub trait ConnectorControlRegistry:
    ConnectorControlResolver
    + ConnectorCatalogMutationResolver
    + ConnectorDataMutationResolver
    + ConnectorMetadataMaintenanceResolver
    + ConnectorDistributedRewriteResolver
    + ConnectorCleanupMaintenanceResolver
    + ConnectorStatisticsResolver
{
    fn register(&self, binding: ConnectorControlBinding) -> Result<(), ConnectorError>;

    fn retire_current(&self, instance_id: &ConnectorInstanceId) -> Result<(), ConnectorError>;
}

/// Keeps one control generation live from metadata/planning until the caller
/// completes the execution-binding barrier. The opaque release action is
/// frontend-owned and is never part of a wire contract.
#[derive(Clone)]
pub struct ConnectorControlPlanningLease {
    binding: Arc<ConnectorControlBinding>,
    _release: Arc<PlanningLeaseRelease>,
}

struct PlanningLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorControlPlanningLease {
    pub fn new(
        binding: Arc<ConnectorControlBinding>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Self {
        Self {
            binding,
            _release: Arc::new(PlanningLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        }
    }

    pub fn binding(&self) -> &Arc<ConnectorControlBinding> {
        &self.binding
    }

    /// Returns the FE-local control runtime fenced by this lease. It is not a
    /// BE catalog identity or a write operation identity.
    pub fn control_runtime_id(&self) -> ConnectorControlRuntimeId {
        self.binding.control_runtime_id()
    }

    /// Derive a writer lease from this retained planning generation.
    ///
    /// A refresh preparation may observe and retain a connector generation
    /// while resolving scans. The later write must use that exact generation,
    /// not acquire whichever incarnation happens to be current at execution
    /// time. Retaining this lease inside the derived writer lease keeps the
    /// generation alive through staging without a second registry lookup.
    pub fn derive_write_lease(&self) -> Result<ConnectorWriteLease, ConnectorError> {
        let write = self.binding.write().cloned().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector control generation has no distributed write capability",
            )
        })?;
        let distribution = self.binding.execution_distribution().clone();
        let provider_id = self.binding.descriptor().provider_id.clone();
        let catalog_properties = self.binding.catalog_properties()?.clone();
        let key = write.binding_key().clone();
        let retained_planning_lease = self.clone();
        ConnectorWriteLease::new_with_execution_distribution(
            self.control_runtime_id(),
            key,
            write,
            provider_id,
            distribution,
            move || drop(retained_planning_lease),
        )
        .and_then(|lease| lease.with_catalog_properties(catalog_properties))
        .map(|lease| lease.with_metadata(Arc::clone(&self.binding.metadata)))
    }

    /// Derive a statistics lease from this retained control generation.
    ///
    /// Statistics collection first asks the provider to pin a data version and
    /// then executes a normal connector read.  Both operations must therefore
    /// be fenced by the same generation; acquiring a new statistics or
    /// planning lease between them could silently mix provider incarnations.
    pub fn derive_statistics_lease(&self) -> Result<ConnectorStatisticsLease, ConnectorError> {
        let statistics = self.binding.statistics().cloned().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector control generation has no statistics capability",
            )
        })?;
        let descriptor = self.binding.descriptor().clone();
        let incarnation = self.binding.incarnation();
        let retained_planning_lease = self.clone();
        ConnectorStatisticsLease::new(descriptor, incarnation, statistics, move || {
            drop(retained_planning_lease)
        })
    }

    /// Derive a catalog-mutation lease from this retained planning generation.
    ///
    /// CREATE-adjacent operations which first inspect or prepare against a
    /// binding must not reacquire whichever mutation generation is current
    /// later. Keeping the parent planning lease alive makes the mutation and
    /// any subsequent writer lease generation-identical by construction.
    pub fn derive_mutation_lease(
        &self,
    ) -> Result<super::ConnectorCatalogMutationLease, ConnectorError> {
        let mutation = self.binding.mutation().cloned().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector control generation has no catalog mutation capability",
            )
        })?;
        let descriptor = self.binding.descriptor().clone();
        let control_runtime_id = self.binding.control_runtime_id();
        let provider_incarnation = self.binding.incarnation();
        let retained_planning_lease = self.clone();
        super::ConnectorCatalogMutationLease::new(
            descriptor,
            control_runtime_id,
            provider_incarnation,
            mutation,
            move || drop(retained_planning_lease),
        )
    }

    /// Derive the exact-generation atomic staged-publication capability.
    /// Unsupported providers fail before any source execution or external
    /// create/write side effect.
    pub fn derive_staged_create_lease(&self) -> Result<ConnectorStagedCreateLease, ConnectorError> {
        let capability = self.binding.staged_create().cloned().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector control generation has no atomic staged-create capability",
            )
        })?;
        let descriptor = self.binding.descriptor().clone();
        let control_runtime_id = self.binding.control_runtime_id();
        let provider_incarnation = self.binding.incarnation();
        let retained_planning_lease = self.clone();
        ConnectorStagedCreateLease::new_with_control_runtime(
            descriptor,
            control_runtime_id,
            provider_incarnation,
            capability,
            move || drop(retained_planning_lease),
        )
    }

    /// Derive the catalog-wide unanchored CTAS cleanup capability from this
    /// planning generation. It is retained alongside staged-create so a
    /// retired connector generation cannot use a newer catalog's warehouse.
    pub fn derive_unanchored_ctas_cleanup_lease(
        &self,
    ) -> Result<ConnectorUnanchoredCtasCleanupLease, ConnectorError> {
        let capability = self
            .binding
            .unanchored_ctas_cleanup()
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "connector control generation has no unanchored CTAS cleanup capability",
                )
            })?;
        let descriptor = self.binding.descriptor().clone();
        let control_runtime_id = self.binding.control_runtime_id();
        let provider_incarnation = self.binding.incarnation();
        let retained_planning_lease = self.clone();
        ConnectorUnanchoredCtasCleanupLease::new_with_control_runtime(
            descriptor,
            control_runtime_id,
            provider_incarnation,
            capability,
            move || drop(retained_planning_lease),
        )
    }
}

impl Drop for PlanningLeaseRelease {
    fn drop(&mut self) {
        let Ok(mut release) = self.release.lock() else {
            return;
        };
        if let Some(release) = release.take() {
            release();
        }
    }
}
