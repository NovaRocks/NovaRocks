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

//! Frontend-local implementations of Core domain contracts for query kernels.

use crate::catalog_application::statement::{CatalogDropContext, CatalogMutationContext};
use novarocks::connector::data_mutation::DataMutationCacheFinalizer;
use novarocks::connector::metadata_maintenance::MetadataMaintenanceCacheFinalizer;
use novarocks_spi::connector::{
    ConnectorControlRegistry, ConnectorError, ConnectorErrorKind, ConnectorTableIdentity,
};

use super::kernels::{CatalogCommandKernel, DmlExecutionKernel, MaintenanceExecutionKernel};

fn invalidate_table(
    catalog_service: &crate::catalog_application::query_catalog::QueryCatalogService,
    table: &ConnectorTableIdentity,
) -> Result<(), ConnectorError> {
    catalog_service
        .invalidate_table(
            table.instance_id.as_str(),
            table.namespace.as_ref(),
            table.table.as_ref(),
        )
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))
}

impl DataMutationCacheFinalizer for DmlExecutionKernel {
    fn invalidate_generic_table(
        &self,
        table: &ConnectorTableIdentity,
    ) -> Result<(), ConnectorError> {
        invalidate_table(self.catalog_service().as_ref(), table)
    }
}

impl MetadataMaintenanceCacheFinalizer for MaintenanceExecutionKernel {
    fn invalidate_generic_table(
        &self,
        table: &ConnectorTableIdentity,
    ) -> Result<(), ConnectorError> {
        invalidate_table(self.catalog_service().as_ref(), table)
    }
}

impl CatalogDropContext for CatalogCommandKernel {
    fn connector_control(&self) -> &dyn ConnectorControlRegistry {
        self.connector_control().as_ref()
    }

    fn mv_repository(&self) -> &dyn crate::mv::domain::repository::MvRepository {
        self.mv_repository().as_ref()
    }

    fn mv_storage_observation(&self) -> &dyn novarocks_spi::connector::MvStorageObservationPort {
        self.mv_storage_observation().as_ref()
    }
}

impl CatalogMutationContext for CatalogCommandKernel {
    fn connector_control(&self) -> &dyn ConnectorControlRegistry {
        self.connector_control().as_ref()
    }
}
