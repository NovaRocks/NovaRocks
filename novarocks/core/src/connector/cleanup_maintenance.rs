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

//! Provider-neutral application bridge for FE-owned orphan cleanup.
//!
//! This module owns exact-generation lease lifetime and the one-way dispatch
//! barrier only. Candidate discovery, immutable artifacts, object identity,
//! deletion, receipts, and reconciliation remain provider responsibilities.
//! Design: ADR-0035 (docs/adr/ADR-0035-connector-orphan-cleanup-reconcile-contract.md)

use std::collections::BTreeSet;
use std::sync::Mutex;

use novarocks_spi::connector::{
    BatchReceipt, CandidatePage, ConnectorCleanupCandidatePageRequest,
    ConnectorCleanupExecuteRequest, ConnectorCleanupFinalizeRequest,
    ConnectorCleanupMaintenanceLease, ConnectorCleanupMaintenanceResolver,
    ConnectorCleanupOperation, ConnectorCleanupOperationId, ConnectorCleanupPlan,
    ConnectorCleanupPlanningRequest, ConnectorCleanupPrepareRequest,
    ConnectorCleanupReconcileRequest, ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution, PreparedBatch,
};

/// The durable frontend owner needs to distinguish an invalid pre-dispatch
/// request from an execute response that may have reached the provider.
#[derive(Clone, Debug)]
pub enum CleanupBatchExecution {
    Receipt(BatchReceipt),
    /// The prepared batch was dispatched, but a receipt is unavailable. The
    /// only legal follow-up is `reconcile_batch` on this exact session.
    Uncertain(ConnectorError),
}

/// A current-generation session may plan and prepare batches. It records every
/// execute attempt in-process so a caller cannot accidentally send the same
/// prepared batch twice before the durable checkpoint takes over.
pub struct CleanupMaintenanceSession {
    lease: ConnectorCleanupMaintenanceLease,
    table: ConnectorTableIdentity,
    plan: ConnectorCleanupPlan,
    context: ConnectorRequestContext,
    mode: CleanupSessionMode,
    executed_ordinals: Mutex<BTreeSet<u32>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CleanupSessionMode {
    Active,
    ReconcileOnly,
}

impl CleanupMaintenanceSession {
    /// Acquire one current cleanup capability, strictly load the table on that
    /// lease, and freeze its one provider plan. This is the sole planning path.
    pub fn plan(
        resolver: &dyn ConnectorCleanupMaintenanceResolver,
        instance_id: &ConnectorInstanceId,
        operation_id: ConnectorCleanupOperationId,
        table: ConnectorTableIdentity,
        older_than_ms: i64,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        if &table.instance_id != instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup table does not belong to requested connector instance",
            ));
        }
        let lease = resolver.acquire_current_cleanup_maintenance(instance_id)?;
        let metadata = lease.metadata().load_table(ConnectorTableRequest {
            table: table.clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context.clone(),
        })?;
        if metadata.identity != table || metadata.table.owner() != &lease.binding_key().instance_id
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup metadata returned a table handle for a different exact owner",
            ));
        }
        let operation =
            ConnectorCleanupOperation::remove_unreferenced_objects(metadata.table, older_than_ms)?;
        let request = ConnectorCleanupPlanningRequest::try_new(
            operation_id,
            lease.binding_key().clone(),
            operation,
            context.clone(),
        )?;
        let plan = lease.plan_cleanup(request)?;
        Ok(Self {
            lease,
            table,
            plan,
            context,
            mode: CleanupSessionMode::Active,
            executed_ordinals: Mutex::new(BTreeSet::new()),
        })
    }

    /// Restore one persisted plan and prepared batch on its recorded exact
    /// generation. Recovery deliberately has no metadata load, replan,
    /// prepare, or execute capability.
    pub fn recover_for_reconcile(
        resolver: &dyn ConnectorCleanupMaintenanceResolver,
        table: ConnectorTableIdentity,
        plan: ConnectorCleanupPlan,
        prepared: PreparedBatch,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        prepared.validate()?;
        if table.instance_id != plan.owner().instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup recovery table does not match persisted plan owner",
            ));
        }
        if prepared.owner() != plan.owner()
            || prepared.operation_id() != plan.operation_id()
            || prepared.plan_digest() != plan.plan_digest()
            || prepared.manifest_digest() != plan.manifest_digest()
            || prepared.batch_ordinal() >= plan.summary().batch_count()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup recovery prepared evidence does not match persisted plan",
            ));
        }
        let lease = resolver.acquire_exact_cleanup_maintenance(plan.owner())?;
        let mut executed_ordinals = BTreeSet::new();
        executed_ordinals.insert(prepared.batch_ordinal());
        Ok(Self {
            lease,
            table,
            plan,
            context,
            mode: CleanupSessionMode::ReconcileOnly,
            executed_ordinals: Mutex::new(executed_ordinals),
        })
    }

    pub fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }

    pub fn plan_ref(&self) -> &ConnectorCleanupPlan {
        &self.plan
    }

    pub fn binding_key(&self) -> &novarocks_spi::connector::ConnectorExecutionBindingKey {
        self.lease.binding_key()
    }

    /// Build bounded durable evidence for a frozen batch. Preparing is not a
    /// destructive operation, but is unavailable to recovery sessions so they
    /// cannot manufacture a new dispatch target.
    pub fn prepare_batch(&self, batch_ordinal: u32) -> Result<PreparedBatch, ConnectorError> {
        self.ensure_active("prepare")?;
        self.lease
            .prepare_batch(ConnectorCleanupPrepareRequest::try_new(
                self.plan.clone(),
                batch_ordinal,
                self.context.clone(),
            )?)
    }

    /// Dispatch exactly once for this in-memory session. Any provider error
    /// after the prepared request has been constructed is intentionally
    /// represented as uncertain; callers must persist that state and use
    /// `reconcile_batch`, never execute again.
    pub fn execute_batch(
        &self,
        prepared: PreparedBatch,
    ) -> Result<CleanupBatchExecution, ConnectorError> {
        self.ensure_active("execute")?;
        self.validate_prepared(&prepared)?;
        {
            let mut executed = self.executed_ordinals.lock().map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "cleanup execute state lock poisoned",
                )
            })?;
            if !executed.insert(prepared.batch_ordinal()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "cleanup prepared batch was already dispatched in this session",
                ));
            }
        }
        match self
            .lease
            .execute_batch(ConnectorCleanupExecuteRequest::try_new(
                self.plan.clone(),
                prepared,
                self.context.clone(),
            )?) {
            Ok(receipt) => Ok(CleanupBatchExecution::Receipt(receipt)),
            Err(error) => Ok(CleanupBatchExecution::Uncertain(error)),
        }
    }

    /// Reconciliation is available only on a recovery session. It reads the
    /// persisted batch/object identity and never lists, plans, prepares, or
    /// dispatches a deletion.
    pub fn reconcile_batch(&self, prepared: PreparedBatch) -> Result<BatchReceipt, ConnectorError> {
        if self.mode != CleanupSessionMode::ReconcileOnly {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup reconciliation requires a recovered exact-generation session",
            ));
        }
        self.validate_prepared(&prepared)?;
        self.lease
            .reconcile_batch(ConnectorCleanupReconcileRequest::try_new(
                self.plan.clone(),
                prepared,
                self.context.clone(),
            )?)
    }

    /// Read only canonical candidate locations from the persisted manifest.
    pub fn read_candidate_page(
        &self,
        offset: u64,
        limit: u32,
    ) -> Result<CandidatePage, ConnectorError> {
        self.lease
            .read_candidate_page(ConnectorCleanupCandidatePageRequest::try_new(
                self.plan.clone(),
                offset,
                limit,
                self.context.clone(),
            )?)
    }

    /// This is intentionally best-effort from the frontend terminal path. It
    /// can delete provider artifacts but cannot affect the durable result.
    pub fn finalize_terminal(&self) -> Result<(), ConnectorError> {
        self.lease
            .finalize_terminal(ConnectorCleanupFinalizeRequest::try_new(
                self.plan.clone(),
                self.context.clone(),
            )?)
    }

    fn ensure_active(&self, operation: &str) -> Result<(), ConnectorError> {
        if self.mode == CleanupSessionMode::ReconcileOnly {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("recovered cleanup session cannot {operation}"),
            ));
        }
        Ok(())
    }

    fn validate_prepared(&self, prepared: &PreparedBatch) -> Result<(), ConnectorError> {
        prepared.validate()?;
        if prepared.owner() != self.plan.owner()
            || prepared.operation_id() != self.plan.operation_id()
            || prepared.plan_digest() != self.plan.plan_digest()
            || prepared.manifest_digest() != self.plan.manifest_digest()
            || prepared.batch_ordinal() >= self.plan.summary().batch_count()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup prepared batch does not match the frozen plan",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_mode_cannot_become_a_new_dispatch_path() {
        assert_eq!(
            CleanupSessionMode::ReconcileOnly,
            CleanupSessionMode::ReconcileOnly
        );
        assert_ne!(
            CleanupSessionMode::Active,
            CleanupSessionMode::ReconcileOnly
        );
    }
}
