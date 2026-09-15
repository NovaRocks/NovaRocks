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
//! No table-maintenance job, operation, lease, checkpoint, or recovery fact
//! survives a frontend restart. The sole durable exception is the separately
//! named GC first-observation accelerator; it never owns a catalog mutation.

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use novarocks_state_store_api::StateStore;
use novarocks_state_store_runtime::StateStoreRunPolicy;
use novarocks_workload_control::RootAdmissionHandle;
use tokio::runtime::Handle;

pub(crate) use self::admission::{
    ParsedMaintenanceAction, ParsedMaintenanceStatement, ParsedShowOptimize,
    lower_semantic_maintenance_statement, lower_semantic_show_optimize,
};
use self::result::{action_result, optimize_jobs_result};
use self::worker::{
    FrontendMaintenanceEffectPort, FrontendOptimizeJobAdmissionPort,
    FrontendOptimizeJobExecutionPort, FrontendOptimizeTargetCapturePort,
};
use crate::query_execution::maintenance::{
    MaintenanceRequestContext, MaintenanceStatementResult, TableMaintenanceEngine,
    TableMaintenanceService,
};
use novarocks_table_maintenance::product::TableMaintenanceProduct;
use novarocks_table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget, OptimizeSubmission,
};

pub mod admission;
pub mod result;
pub mod worker;

// Design: ADR-0111 (docs/adr/ADR-0111-frontend-process-runtime-jobs-and-gc-observation-accelerator.md)
pub struct FrontendTableMaintenanceService {
    product: Arc<TableMaintenanceProduct>,
    runtime: Handle,
    root_admission: RootAdmissionHandle,
}

impl FrontendTableMaintenanceService {
    /// Opens the service.
    ///
    /// The store and the policy that governs its use arrive together, so a
    /// half-configured owner holding one without the other cannot be built.
    pub async fn open(
        durable: Option<(Arc<dyn StateStore>, StateStoreRunPolicy)>,
        runtime: Handle,
        root_admission: RootAdmissionHandle,
    ) -> Result<Self, String> {
        Self::open_inner(durable, runtime, root_admission).await
    }

    async fn open_inner(
        durable: Option<(Arc<dyn StateStore>, StateStoreRunPolicy)>,
        runtime: Handle,
        root_admission: RootAdmissionHandle,
    ) -> Result<Self, String> {
        let product = TableMaintenanceProduct::open(durable).await?;
        Ok(Self {
            product: Arc::new(product),
            runtime,
            root_admission,
        })
    }

    pub fn with_lake_publication_runtime_policy(
        self,
        policy: novarocks_query_application::publication::LakePublicationRuntimePolicy,
    ) -> Self {
        Self {
            product: Arc::new(
                Arc::try_unwrap(self.product)
                    .unwrap_or_else(|_| {
                        panic!("table-maintenance product is uniquely configured before service publication")
                    })
                    .with_safe_gc_age(policy.safe_gc_age()),
            ),
            runtime: self.runtime,
            root_admission: self.root_admission,
        }
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        if Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }

    fn submit_optimize(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        let capture = FrontendOptimizeTargetCapturePort::new(engine);
        self.block_on(self.product.submit_optimize(target, &capture))
    }

    fn show_optimize(
        &self,
        statement: ParsedShowOptimize,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, String> {
        let mut jobs = self.block_on(self.product.list_optimize())?;
        if let Some(catalog) = statement.catalog.as_deref().or(context.current_catalog) {
            jobs.retain(|job| job.target.catalog == catalog);
        }
        jobs.retain(|job| {
            job.target.namespace
                == statement
                    .database
                    .as_deref()
                    .unwrap_or(context.current_database)
        });
        if let Some(table) = statement.table_name.as_deref() {
            jobs.retain(|job| job.target.table == table);
        }
        if statement.order_by_create_time_desc {
            jobs.sort_by_key(|job| std::cmp::Reverse(job.created_at_ms));
        }
        if let Some(limit) = statement.limit {
            jobs.truncate(limit);
        }
        optimize_jobs_result(jobs)
    }

    pub(crate) async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        self.product.shutdown_until(deadline).await
    }

    pub(crate) fn request_shutdown_for_process_exit(&self) {
        self.product.request_shutdown_for_process_exit();
    }
}

#[async_trait::async_trait]
impl TableMaintenanceService for FrontendTableMaintenanceService {
    fn start(&self, engine: Arc<dyn TableMaintenanceEngine>) -> Result<(), String> {
        self.product.start(
            &self.runtime,
            Arc::new(FrontendOptimizeJobAdmissionPort::new(
                self.root_admission.clone(),
            )),
            Arc::new(FrontendOptimizeJobExecutionPort::new(
                Arc::downgrade(&engine),
                Arc::downgrade(&self.product),
            )),
        )
    }

    async fn handle_typed_statement(
        &self,
        engine: &dyn TableMaintenanceEngine,
        statement: ParsedMaintenanceStatement,
        spark_procedure: bool,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, String> {
        match statement {
            ParsedMaintenanceStatement::Execute { name_parts, action } => {
                let target = engine.resolve_target(&name_parts, context)?;
                let request = action.into_request(engine, target)?;
                let outcome = self
                    .product
                    .execute_user_action(&FrontendMaintenanceEffectPort::new(engine), request)
                    .await?;
                if spark_procedure {
                    action_result(outcome)
                } else {
                    Ok(MaintenanceStatementResult::Ok)
                }
            }
            ParsedMaintenanceStatement::SubmitOptimize { name_parts } => {
                engine.reject_user_action_on_mv(&engine.resolve_target(&name_parts, context)?)?;
                self.submit_optimize(engine, engine.resolve_target(&name_parts, context)?)
                    .map(|_| MaintenanceStatementResult::Ok)
            }
            ParsedMaintenanceStatement::ShowOptimize => Err(
                "SHOW ALTER TABLE OPTIMIZE belongs to the read-only maintenance owner".to_string(),
            ),
        }
    }

    fn handle_typed_show_optimize(
        &self,
        statement: ParsedShowOptimize,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceStatementResult, String> {
        self.show_optimize(statement, context)
    }

    async fn execute_automatic_action(
        &self,
        engine: &dyn TableMaintenanceEngine,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        self.product
            .execute_action(&FrontendMaintenanceEffectPort::new(engine), request)
            .await
    }

    fn submit_automatic_optimize(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        self.submit_optimize(engine, target)
    }

    async fn wait_for_automatic_optimize(
        &self,
        handle: novarocks_table_maintenance::runtime::JobHandle,
    ) -> Result<novarocks_table_maintenance::runtime::MaintenanceJobState, String> {
        self.product.wait_optimize(handle).await
    }

    fn execute_automatic_optimize_durably(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        let submission = self.submit_optimize(engine, target)?;
        let Some(handle) = submission.handle() else {
            return Ok(submission);
        };
        let terminal = self.block_on(self.wait_for_automatic_optimize(handle))?;
        if terminal == novarocks_table_maintenance::runtime::MaintenanceJobState::Finished {
            Ok(submission)
        } else {
            Err(format!(
                "optimize job {} completed with terminal state {}",
                handle.job_id(),
                terminal.as_str()
            ))
        }
    }

    async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        FrontendTableMaintenanceService::shutdown_until(self, deadline).await
    }

    fn request_shutdown_for_process_exit(&self) {
        FrontendTableMaintenanceService::request_shutdown_for_process_exit(self);
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
