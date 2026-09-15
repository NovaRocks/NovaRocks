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
//! deletion and receipts remain provider responsibilities.

use std::collections::BTreeSet;
use std::sync::Mutex;

use novarocks_spi::connector::{
    BatchReceipt, CandidatePage, ConnectorCleanupCandidatePageRequest,
    ConnectorCleanupExecuteRequest, ConnectorCleanupFinalizeRequest,
    ConnectorCleanupMaintenanceLease, ConnectorCleanupMaintenanceResolver,
    ConnectorCleanupOperation, ConnectorCleanupOperationId, ConnectorCleanupOwnedRefSelection,
    ConnectorCleanupPlan, ConnectorCleanupPrepareRequest, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceId, ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution, PreparedBatch,
};

/// The durable frontend owner needs to distinguish an invalid pre-dispatch
/// request from an execute response that may have reached the provider.
#[derive(Clone, Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "The typed frontend protocol model intentionally keeps payloads inline."
)]
pub enum CleanupBatchExecution {
    Receipt(BatchReceipt),
    /// The prepared batch was dispatched, but a receipt is unavailable.
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
    executed_ordinals: Mutex<BTreeSet<u32>>,
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
        if metadata.identity != table || metadata.table.owner() != &lease.descriptor().instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup metadata returned a table handle for a different exact owner",
            ));
        }
        let operation =
            ConnectorCleanupOperation::remove_unreferenced_objects(metadata.table, older_than_ms)?;
        let plan = lease.plan_operation(operation_id, operation, context.clone())?;
        Self::from_plan(lease, table, plan, context)
    }

    /// Freeze the second cleanup plan after the frontend has durably observed
    /// the exact owned refs that survived the age window. This route is
    /// deliberately distinct from discovery: even an empty selection remains
    /// an owned-ref plan and can never fall through to an object sweep.
    pub fn plan_selected_owned_refs(
        resolver: &dyn ConnectorCleanupMaintenanceResolver,
        instance_id: &ConnectorInstanceId,
        operation_id: ConnectorCleanupOperationId,
        table: ConnectorTableIdentity,
        older_than_ms: i64,
        selection: ConnectorCleanupOwnedRefSelection,
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
        if metadata.identity != table || metadata.table.owner() != &lease.descriptor().instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup metadata returned a table handle for a different exact owner",
            ));
        }
        let operation =
            ConnectorCleanupOperation::remove_unreferenced_objects(metadata.table, older_than_ms)?;
        let plan =
            lease.plan_selected_owned_refs(operation_id, operation, selection, context.clone())?;
        Self::from_plan(lease, table, plan, context)
    }

    fn from_plan(
        lease: ConnectorCleanupMaintenanceLease,
        table: ConnectorTableIdentity,
        plan: ConnectorCleanupPlan,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            lease,
            table,
            plan,
            context,
            executed_ordinals: Mutex::new(BTreeSet::new()),
        })
    }

    pub fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }

    pub fn plan_ref(&self) -> &ConnectorCleanupPlan {
        &self.plan
    }

    pub const fn control_runtime_id(&self) -> novarocks_spi::connector::ConnectorControlRuntimeId {
        self.lease.control_runtime_id()
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
    /// represented as uncertain; the current process never re-executes or
    /// recovers that batch.
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

    fn ensure_active(&self, _operation: &str) -> Result<(), ConnectorError> {
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
