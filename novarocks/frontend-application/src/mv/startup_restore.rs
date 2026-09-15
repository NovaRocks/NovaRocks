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

//! Frontend-owned MV startup restore.
//!
//! Installing this makes the frontend the owner of *when* MV state is restored at
//! startup, which is what "Frontend orchestrates through the installed
//! provider/runtime boundary" asks for. The lake-reading work itself stays in the
//! engine, because a production SQL procedure calls the same targeted rebuild; the
//! decision that moved here is the orchestration, not the code.
//!
//! Every input is a port the frontend already holds. Nothing here reaches into
//! aggregate engine state, which is precisely why this implementation can exist.

use std::sync::Arc;

use crate::catalog_application::CatalogRuntimeProjection;
use crate::mv::domain::readiness::MvReadinessPort;
use crate::mv::domain::startup_restore::MvStartupRestore;
use novarocks_catalog_application::CatalogApplicationPort;
use novarocks_spi::connector::ConnectorControlRegistry;
use novarocks_spi::connector::MvStorageObservationPort;

/// The frontend's implementation of the ordered startup restore steps.
pub(crate) struct FrontendMvStartupRestore {
    connector_control: Arc<dyn ConnectorControlRegistry>,
    catalog_runtime_projection: Arc<CatalogRuntimeProjection>,
    catalog_application: Arc<dyn CatalogApplicationPort>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    readiness: Arc<MvReadinessPort>,
}

impl FrontendMvStartupRestore {
    pub(crate) fn new(
        connector_control: Arc<dyn ConnectorControlRegistry>,
        catalog_runtime_projection: Arc<CatalogRuntimeProjection>,
        catalog_application: Arc<dyn CatalogApplicationPort>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        readiness: Arc<MvReadinessPort>,
    ) -> Self {
        Self {
            connector_control,
            catalog_runtime_projection,
            catalog_application,
            mv_storage_observation,
            readiness,
        }
    }
}

impl MvStartupRestore for FrontendMvStartupRestore {
    fn rebuild_cache_from_lake(&self) -> Result<(), String> {
        // Always enter the bounded discovery sweep. The admitted catalog
        // projection and provider observations naturally determine whether any
        // lake package is eligible for rebuild.
        crate::mv::domain::lake_rebuild::rebuild_imv_cache_from_lake(
            &crate::mv::domain::lake_rebuild::LakeRebuildContext {
                catalog_runtime_projection: Some(&self.catalog_runtime_projection),
                catalog_application: Some(self.catalog_application.as_ref()),
                connector_control: self.connector_control.as_ref(),
                mv_storage_observation: self.mv_storage_observation.as_ref(),
                readiness: self.readiness.as_ref(),
            },
        )
    }

    fn restore_targets(&self) -> Result<(), String> {
        crate::mv::domain::iceberg_refresh::restore_iceberg_mv_targets(
            &crate::mv::domain::iceberg_refresh::MvTargetRestoreContext {
                connector_control: self.connector_control.as_ref(),
                readiness: self.readiness.as_ref(),
            },
        )
    }
}
