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

//! Iceberg's selected cleanup capability and original source factory.

use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorCleanupMaintenance, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorProviderBindingKey,
};

use crate::metadata_context::IcebergMetadataContext;

#[derive(Clone)]
pub struct IcebergCleanupMaintenanceAdapter {
    key: ConnectorProviderBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    runtime: Arc<IcebergMetadataContext>,
}

impl IcebergCleanupMaintenanceAdapter {
    pub fn new(
        key: ConnectorProviderBindingKey,
        runtime: Arc<IcebergMetadataContext>,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
                instance_id: key.instance_id.clone(),
            },
            key,
            runtime,
        })
    }
}

#[path = "cleanup_maintenance/async_original.rs"]
pub(crate) mod async_original;
impl ConnectorCleanupMaintenance for IcebergCleanupMaintenanceAdapter {
    fn metadata_cpu_source_type_id(&self) -> Result<std::any::TypeId, ConnectorError> {
        Ok(std::any::TypeId::of::<
            super::statistics::scoped_cpu::IcebergMetadataCpuOriginal,
        >())
    }

    fn begin_async_in(
        &self,
        start: &mut Option<novarocks_spi::connector::ConnectorCleanupAsyncStart>,
        metadata: Arc<dyn novarocks_spi::connector::ConnectorMetadata>,
        cpu: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
        target: &mut Option<novarocks_spi::connector::ConnectorCleanupAsyncExecution>,
    ) -> Result<(), ConnectorError> {
        async_original::install_in(self, start, metadata, cpu, target)
    }
    fn initial_dependencies(
        &self,
    ) -> Result<Vec<novarocks_spi::connector::SharedDependency>, ConnectorError> {
        self.runtime.metadata_action_initial_dependencies()
    }

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}
