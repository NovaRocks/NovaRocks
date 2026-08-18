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

use crate::catalog_application::{CatalogApplicationPort, CatalogRuntimeProjection};
use crate::mv::domain::repository::MvRepository;
use crate::mv::domain::startup_restore::MvStartupRestore;
use crate::mv::domain::storage_observation::MvStorageObservationPort;
use novarocks_spi::connector::ConnectorControlRegistry;

/// The frontend's implementation of the ordered startup restore steps.
pub(crate) struct FrontendMvStartupRestore {
    connector_control: Arc<dyn ConnectorControlRegistry>,
    catalog_runtime_projection: Arc<CatalogRuntimeProjection>,
    catalog_application: Arc<dyn CatalogApplicationPort>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    mv_repository: Arc<dyn MvRepository>,
    /// Reconciles unfinished refresh attempts. Held as a closure because the MV
    /// application service is constructed after this value's other inputs, and
    /// threading a half-built service through would be worse than a callback.
    recover: Box<dyn Fn() -> Result<(), String> + Send + Sync>,
}

impl FrontendMvStartupRestore {
    pub(crate) fn new(
        connector_control: Arc<dyn ConnectorControlRegistry>,
        catalog_runtime_projection: Arc<CatalogRuntimeProjection>,
        catalog_application: Arc<dyn CatalogApplicationPort>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        mv_repository: Arc<dyn MvRepository>,
        recover: Box<dyn Fn() -> Result<(), String> + Send + Sync>,
    ) -> Self {
        Self {
            connector_control,
            catalog_runtime_projection,
            catalog_application,
            mv_storage_observation,
            mv_repository,
            recover,
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
                mv_repository: self.mv_repository.as_ref(),
            },
        )
    }

    fn restore_targets(&self) -> Result<(), String> {
        crate::mv::domain::iceberg_refresh::restore_iceberg_mv_targets(
            &crate::mv::domain::iceberg_refresh::MvTargetRestoreContext {
                connector_control: self.connector_control.as_ref(),
                mv_repository: self.mv_repository.as_ref(),
            },
        )
    }

    fn recover_unfinished_refreshes(&self) -> Result<(), String> {
        (self.recover)()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// The recovery step must run the caller's closure exactly once per restore,
    /// and surface its failure rather than swallowing it -- a swallowed recovery
    /// failure would let startup proceed with unreconciled attempts.
    #[test]
    fn recovery_delegates_to_the_installed_closure() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);

        struct OnlyRecovery {
            recover: Box<dyn Fn() -> Result<(), String> + Send + Sync>,
        }
        impl MvStartupRestore for OnlyRecovery {
            fn rebuild_cache_from_lake(&self) -> Result<(), String> {
                Ok(())
            }
            fn restore_targets(&self) -> Result<(), String> {
                Ok(())
            }
            fn recover_unfinished_refreshes(&self) -> Result<(), String> {
                (self.recover)()
            }
        }

        let restore = OnlyRecovery {
            recover: Box::new(move || {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        };
        crate::mv::domain::startup_restore::run_mv_startup_restore(&restore)
            .expect("restore succeeds");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let failing = OnlyRecovery {
            recover: Box::new(|| Err("recovery failed".to_string())),
        };
        let error = crate::mv::domain::startup_restore::run_mv_startup_restore(&failing)
            .expect_err("a failing recovery must not be swallowed");
        assert!(error.contains("recovery failed"), "{error}");
    }
}
