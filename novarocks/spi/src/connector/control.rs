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
    ConnectorBeginScanRequest, ConnectorCatalogMutation, ConnectorCatalogMutationResolver,
    ConnectorDataMutation, ConnectorDataMutationResolver, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorExecutionDeclaration, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorMetadata,
    ConnectorMetadataMaintenance, ConnectorMetadataMaintenanceResolver, ConnectorRequestContext,
    ConnectorScan, ConnectorScanHandle, ConnectorSplitPlanningRequest,
    ConnectorSplitPlanningResult, ConnectorStatistics, ConnectorStatisticsResolver,
    ConnectorTableHandle, ConnectorWriteControl, ConnectorWriteLease, ConnectorWriteResolver,
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

/// FE-only capability that turns a logical control binding into the bounded,
/// opaque declaration accepted by a BE execution installer.
pub trait ConnectorExecutionDistribution: Send + Sync {
    fn declaration(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError>;
}

/// A control-plane Connector generation. Metadata, scan planning, and
/// execution distribution must all describe the same logical descriptor and
/// incarnation. It is deliberately unable to open a batch reader.
pub struct ConnectorControlBinding {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    metadata: Arc<dyn ConnectorMetadata>,
    planning: Arc<dyn ConnectorScanPlanning>,
    distribution: Arc<dyn ConnectorExecutionDistribution>,
    mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
    data_mutation: Option<Arc<dyn ConnectorDataMutation>>,
    metadata_maintenance: Option<Arc<dyn ConnectorMetadataMaintenance>>,
    write: Option<Arc<dyn ConnectorWriteControl>>,
    statistics: Option<Arc<dyn ConnectorStatistics>>,
}

impl ConnectorControlBinding {
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
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
        incarnation: ConnectorInstanceIncarnation,
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
        incarnation: ConnectorInstanceIncarnation,
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
        incarnation: ConnectorInstanceIncarnation,
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
        incarnation: ConnectorInstanceIncarnation,
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
        incarnation: ConnectorInstanceIncarnation,
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
            metadata,
            planning,
            distribution,
            mutation,
            data_mutation,
            metadata_maintenance: None,
            write,
            statistics,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_all_capabilities_and_metadata_maintenance(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        mutation: Option<Arc<dyn ConnectorCatalogMutation>>,
        data_mutation: Option<Arc<dyn ConnectorDataMutation>>,
        metadata_maintenance: Option<Arc<dyn ConnectorMetadataMaintenance>>,
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
        Ok(binding)
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
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

    pub fn write(&self) -> Option<&Arc<dyn ConnectorWriteControl>> {
        self.write.as_ref()
    }

    pub fn execution_distribution(&self) -> &Arc<dyn ConnectorExecutionDistribution> {
        &self.distribution
    }

    pub fn statistics(&self) -> Option<&Arc<dyn ConnectorStatistics>> {
        self.statistics.as_ref()
    }

    pub fn execution_declaration(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        let declaration = self.distribution.declaration(context)?;
        if declaration.descriptor() != &self.descriptor
            || declaration.incarnation() != self.incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector execution declaration does not match its control binding generation",
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
    ) -> Result<ConnectorExecutionBindingKey, ConnectorError>;

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
    + ConnectorWriteResolver
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
        let key = write.binding_key().clone();
        let retained_planning_lease = self.clone();
        Ok(ConnectorWriteLease::new_with_execution_distribution(
            key,
            write,
            distribution,
            move || drop(retained_planning_lease),
        )?)
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
        let incarnation = self.binding.incarnation();
        let retained_planning_lease = self.clone();
        super::ConnectorCatalogMutationLease::new(descriptor, incarnation, mutation, move || {
            drop(retained_planning_lease)
        })
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
