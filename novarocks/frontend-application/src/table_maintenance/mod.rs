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

use novarocks_table_maintenance::product::TableMaintenanceProduct;
use novarocks_table_maintenance::runtime::TerminalError;
use novarocks_table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget,
};
use std::sync::Arc;

pub(crate) use self::admission::{ParsedMaintenanceAction, ParsedMaintenanceStatement};
use self::result::procedure_metrics_result;
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
impl Default for FrontendTableMaintenanceService {
    fn default() -> Self {
        Self {
            product: Arc::new(TableMaintenanceProduct::default()),
            #[cfg(novarocks_dev_controls)]
            controls: None,
        }
    }
}
#[cfg(novarocks_dev_controls)]
impl FrontendTableMaintenanceService {
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
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, TerminalError> {
        let ParsedMaintenanceStatement { name_parts, action } = statement;
        let target = engine.resolve_target(&name_parts, context)?;
        let request = action.into_request(target);
        let outcome = self
            .product
            .execute_user_action(&LegacyMaintenanceEffects::new(engine, state), request)
            .await?;
        match outcome {
            MaintenanceActionOutcome::Procedure { metrics } => procedure_metrics_result(metrics)
                .map(MaintenanceStatementResult::Query)
                .map_err(TerminalError::from),
            _ => Ok(MaintenanceStatementResult::Ok),
        }
    }
}

impl ParsedMaintenanceAction {
    fn into_request(self, target: MaintenanceTarget) -> MaintenanceActionRequest {
        match self {
            Self::RewriteManifests {
                use_caching,
                spec_id,
            } => MaintenanceActionRequest::RewriteManifests {
                target,
                use_caching,
                spec_id,
            },
            Self::ExpireSnapshots {
                older_than_ms,
                retain_last,
            } => MaintenanceActionRequest::ExpireSnapshots {
                target,
                older_than_ms,
                retain_last,
            },
        }
    }
}
