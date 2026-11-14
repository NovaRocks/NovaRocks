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

//! Frontend-owned, current-process table maintenance service.
//!
//! Statement-owned table maintenance and its shared target gate.

use novarocks_state_store_api::StateStore;
use novarocks_state_store_runtime::StateStoreRunPolicy;
use novarocks_table_maintenance::product::TableMaintenanceProduct;
use novarocks_table_maintenance::runtime::TerminalError;
use novarocks_table_maintenance::{MaintenanceActionRequest, MaintenanceTarget};
use std::sync::Arc;

pub(crate) use self::admission::{
    ParsedMaintenanceAction, ParsedMaintenanceStatement, lower_semantic_maintenance_statement,
};
use self::result::action_result;
use self::worker::LegacyMaintenanceEffects;
use crate::query_execution::maintenance::{
    MaintenanceRequestContext, MaintenanceStatementResult, TableMaintenanceEngine,
    TableMaintenanceService,
};

pub mod admission;
pub mod result;
#[cfg(novarocks_dev_controls)]
pub(crate) mod test_controls;
#[cfg(novarocks_dev_controls)]
#[doc(hidden)]
pub use test_controls::MaintenanceTestControls;
pub mod worker;

pub struct FrontendTableMaintenanceService {
    product: Arc<TableMaintenanceProduct>,
    #[cfg(novarocks_dev_controls)]
    controls: Option<Arc<test_controls::MaintenanceTestControls>>,
}
impl FrontendTableMaintenanceService {
    pub async fn open(
        durable: Option<(Arc<dyn StateStore>, StateStoreRunPolicy)>,
    ) -> Result<Self, String> {
        Ok(Self {
            product: Arc::new(TableMaintenanceProduct::open(durable).await?),
            #[cfg(novarocks_dev_controls)]
            controls: None,
        })
    }
    pub fn with_lake_publication_runtime_policy(
        self,
        policy: novarocks_query_application::publication::LakePublicationRuntimePolicy,
    ) -> Self {
        Self { product: Arc::new(Arc::try_unwrap(self.product).unwrap_or_else(|_| panic!("table-maintenance product is uniquely configured before service publication")).with_safe_gc_age(policy.safe_gc_age())), #[cfg(novarocks_dev_controls)] controls: self.controls }
    }
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn with_test_controls(
        mut self,
        controls: Option<Arc<test_controls::MaintenanceTestControls>>,
    ) -> Self {
        self.controls = controls;
        self
    }
}
#[async_trait::async_trait]
impl TableMaintenanceService for FrontendTableMaintenanceService {
    #[cfg(novarocks_dev_controls)]
    fn statement_test_controls(&self) -> Option<Arc<test_controls::MaintenanceTestControls>> {
        self.controls.clone()
    }
    fn original_optimize_host(
        &self,
    ) -> Result<crate::query_execution::maintenance::OriginalOptimizeHost, String> {
        Ok(crate::query_execution::maintenance::OriginalOptimizeHost {
            product: self.product.clone(),
        })
    }
    async fn handle_typed_statement(
        &self,
        engine: &dyn TableMaintenanceEngine,
        state: &crate::query_execution::maintenance::MaintenanceActionState,
        statement: ParsedMaintenanceStatement,
        spark_procedure: bool,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, TerminalError> {
        let ParsedMaintenanceStatement::Execute { name_parts, action } = statement else {
            return Err(TerminalError::pre_dispatch_failed(
                "OPTIMIZE requires its original statement action",
            ));
        };
        let target = engine.resolve_target(&name_parts, context)?;
        let request = action.into_request(engine, target)?;
        let outcome = self
            .product
            .execute_user_action(&LegacyMaintenanceEffects::new(engine, state), request)
            .await?;
        if spark_procedure {
            action_result(outcome).map_err(TerminalError::from)
        } else {
            Ok(MaintenanceStatementResult::Ok)
        }
    }
}

impl ParsedMaintenanceAction {
    fn into_request(
        self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<MaintenanceActionRequest, String> {
        match self {
            Self::RewriteDataFiles {
                options,
                branch,
                where_clause,
            } => Ok(MaintenanceActionRequest::RewriteDataFiles {
                target: target.clone(),
                base_snapshot_id: engine.current_snapshot_id(&target)?,
                job_id: None,
                options,
                branch,
                where_clause,
            }),
            Self::RewriteManifests {
                use_caching,
                spec_id,
            } => Ok(MaintenanceActionRequest::RewriteManifests {
                target,
                use_caching,
                spec_id,
            }),
            Self::ExpireSnapshots {
                older_than_ms,
                retain_last,
            } => Ok(MaintenanceActionRequest::ExpireSnapshots {
                target,
                older_than_ms,
                retain_last,
            }),
            Self::RemoveOrphanFiles { older_than_ms } => {
                Ok(MaintenanceActionRequest::RemoveOrphanFiles {
                    target,
                    older_than_ms,
                })
            }
            Self::RewritePositionDeleteFiles {
                options,
                where_clause,
            } => Ok(MaintenanceActionRequest::RewritePositionDeleteFiles {
                target,
                options,
                where_clause,
            }),
        }
    }
}
