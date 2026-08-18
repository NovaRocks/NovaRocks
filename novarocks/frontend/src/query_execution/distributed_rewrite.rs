// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Provider-neutral orchestration state for distributed table rewrites.
//!
//! The provider freezes its groups before this module is entered.  Core turns
//! those opaque group plans into one sealed C1 write operation, keeps the
//! exact composite lease alive, and records the provider's durable checkpoint
//! for every accepted or superseded attempt.  It intentionally knows neither
//! files, manifests, nor provider report formats.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use novarocks_spi::connector::{
    ConnectorDistributedRewriteAttemptCheckpoint, ConnectorDistributedRewriteAttemptDisposition,
    ConnectorDistributedRewriteLease, ConnectorDistributedRewritePlan,
    ConnectorDistributedRewriteReceipt, ConnectorError, ConnectorErrorKind,
    ConnectorRequestContext, ConnectorTableHandle, ConnectorWriteAbortOutcome,
    ConnectorWriteAttemptCompletion, ConnectorWriteCohortId, ConnectorWriteExecutionId,
    ConnectorWriteOperationId, ConnectorWriteReceipt, ExternalMutationEvidence,
    ExternalMutationOutcome,
};

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::common::backend_topology::BackendTopologySnapshot;
use crate::query_execution::contract::{
    ConnectorWriteExecutionRegistration, ConnectorWriteOperationRegistration,
    ConnectorWritePlanningTemplate,
};
use crate::query_execution::outcome::ConnectorWriteCompletion;
use crate::query_execution::preparation::scan::PlannedConnectorRead;
use crate::query_execution::service::QueryExecutionService;
use crate::query_execution::write_operation::ConnectorWriteOperationSession;
use novarocks::connector::distributed_rewrite_application::{
    DistributedRewriteApplicationSession, DistributedRewriteSealing, SealedDistributedRewrite,
};
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::query_execution::{
    FrozenConnectorScanIdentity, FrozenConnectorScanPlan,
};

/// The Frontend-owned maintenance session shape produced by query assembly.
pub(crate) type DistributedRewriteMaintenanceSession =
    DistributedRewriteApplicationSession<ConnectorDistributedRewriteSession>;

impl SealedDistributedRewrite for ConnectorDistributedRewriteSession {
    fn plan(&self) -> &ConnectorDistributedRewritePlan {
        ConnectorDistributedRewriteSession::plan(self)
    }

    fn is_noop(&self) -> bool {
        ConnectorDistributedRewriteSession::is_noop(self)
    }
}

impl DistributedRewriteSealing for QueryExecutionService {
    type Sealed = ConnectorDistributedRewriteSession;

    fn seal_distributed_rewrite(
        &self,
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
        context: ConnectorRequestContext,
    ) -> Result<Self::Sealed, String> {
        self.begin_distributed_rewrite_operation_with_lease(plan, lease, context)
            .map_err(|error| error.to_string())
    }
}

/// Plan one frozen source through the scan-planning capability retained by a
/// composite rewrite lease.  The plan is opaque to this module: it has no
/// Iceberg files, catalog client, or provider report decoding.
pub(crate) fn plan_frozen_rewrite_connector_read(
    lease: &ConnectorDistributedRewriteLease,
    topology: &BackendTopologySnapshot,
    source: &ConnectorTableHandle,
    expected_schema: &SchemaRef,
    projection: Vec<usize>,
    context: ConnectorRequestContext,
) -> Result<PlannedConnectorRead, ConnectorError> {
    crate::query_execution::frozen_connector_read::plan_frozen_connector_read(
        lease.planning_lease(),
        topology,
        source,
        expected_schema,
        projection,
        context,
    )
}

/// Admit the synthetic source used by one opaque frozen rewrite read.
///
/// The exact connector authority remains in `FrozenRewriteReadResolver`, but
/// the physical SQL artifact still carries a request-local token from the
/// same store retained through preparation.  This prevents the resolver-only
/// path from reintroducing an unbound scan carrier after `ConnectorPinned`
/// was removed.
pub(crate) fn admit_frozen_rewrite_scan_binding(
    bindings: &QueryTableBindingStore,
    input_schema: &arrow::datatypes::SchemaRef,
) -> Result<SqlTableBindingId, String> {
    crate::query_execution::frozen_connector_read::admit_frozen_connector_scan_binding(
        bindings,
        &frozen_rewrite_identity(),
        input_schema,
    )
}

/// Build the minimal physical source for one opaque frozen rewrite read.
/// Execution preparation replaces this SQL-owned connector-read node exactly
/// once with the `PlannedConnectorRead` above; no normal table lookup may
/// run.
pub(crate) fn frozen_rewrite_scan_physical_plan(
    input_schema: &arrow::datatypes::SchemaRef,
    binding: SqlTableBindingId,
) -> FrozenConnectorScanPlan {
    crate::query_execution::frozen_connector_read::frozen_connector_scan_physical_plan(
        &frozen_rewrite_identity(),
        input_schema,
        binding,
    )
}

/// One-shot injection point for the exact frozen source plan.  Keeping this
/// local to rewrite execution makes a second provider catalog lookup during
/// fragment preparation structurally impossible.
pub(crate) type FrozenRewriteReadResolver =
    crate::query_execution::frozen_connector_read::FrozenConnectorReadResolver;

pub(crate) fn frozen_rewrite_read_resolver(
    binding: SqlTableBindingId,
    read: PlannedConnectorRead,
) -> FrozenRewriteReadResolver {
    FrozenRewriteReadResolver::new(binding, frozen_rewrite_identity(), read)
}

fn frozen_rewrite_identity() -> FrozenConnectorScanIdentity {
    FrozenConnectorScanIdentity::new(
        "__distributed_rewrite",
        "__distributed_rewrite",
        "__connector_frozen_rewrite",
    )
}

/// One frozen rewrite operation.  A non-empty plan is sealed into C1 before
/// any caller can obtain a cohort execution registration.  Empty plans are a
/// deterministic no-op and deliberately have no writer session.
#[derive(Clone)]
pub struct ConnectorDistributedRewriteSession {
    inner: Arc<ConnectorDistributedRewriteSessionInner>,
}

struct ConnectorDistributedRewriteSessionInner {
    plan: ConnectorDistributedRewritePlan,
    lease: ConnectorDistributedRewriteLease,
    write_session: Option<ConnectorWriteOperationSession>,
    checkpoints: Mutex<BTreeMap<AttemptKey, ConnectorDistributedRewriteAttemptCheckpoint>>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AttemptKey {
    cohort_id: ConnectorWriteCohortId,
    execution_id: ConnectorWriteExecutionId,
    disposition: u8,
}

impl ConnectorDistributedRewriteSession {
    /// Validate a provider-frozen plan, activate its exact provider service,
    /// and seal all C1 cohorts in one step.  There is no API that adds a
    /// cohort afterwards.
    pub fn try_begin(
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        if plan.owner() != lease.binding_key() {
            return Err(invalid(
                "distributed rewrite plan does not belong to the exact rewrite lease",
            ));
        }

        let write_session = if plan.cohorts().is_empty() {
            None
        } else {
            let activation = lease.activate_rewrite(&plan, context.clone())?;
            let write_lease = lease.derive_write_lease()?;
            let templates = plan
                .cohorts()
                .iter()
                .map(|cohort| {
                    let activated = activation.cohort(cohort.cohort_id()).ok_or_else(|| {
                        invalid("distributed rewrite activation omitted a sealed cohort")
                    })?;
                    ConnectorWritePlanningTemplate::from_activated_cohort(
                        activated,
                        context.clone(),
                        write_lease.clone(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let registration = ConnectorWriteOperationRegistration::try_new(templates)
                .map_err(|error| invalid(format!("register rewrite cohorts: {error}")))?;
            Some(ConnectorWriteOperationSession::try_begin_unfenced(
                registration,
                write_lease,
                "distributed rewrite is arbitrated by ordinary Iceberg base-state CAS, \
                 not by the distributed-write external fence",
            )?)
        };

        Ok(Self {
            inner: Arc::new(ConnectorDistributedRewriteSessionInner {
                plan,
                lease,
                write_session,
                checkpoints: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    pub fn plan(&self) -> &ConnectorDistributedRewritePlan {
        &self.inner.plan
    }

    /// Exact composite lease retained from frozen planning through terminal
    /// commit or abort.  Provider-facing execution may use only this lease to
    /// plan the opaque frozen source.
    pub fn lease(&self) -> &ConnectorDistributedRewriteLease {
        &self.inner.lease
    }

    pub fn operation_id(&self) -> ConnectorWriteOperationId {
        self.inner.plan.operation_id()
    }

    pub fn is_noop(&self) -> bool {
        self.inner.write_session.is_none()
    }

    pub fn write_session(&self) -> Option<&ConnectorWriteOperationSession> {
        self.inner.write_session.as_ref()
    }

    /// Produce the only execution registration for a sealed rewrite cohort.
    /// Calling this before `try_begin` has sealed the full frozen group set is
    /// structurally impossible.
    pub fn execution_registration(
        &self,
        cohort_id: ConnectorWriteCohortId,
    ) -> Result<ConnectorWriteExecutionRegistration, ConnectorError> {
        let session = self
            .inner
            .write_session
            .clone()
            .ok_or_else(|| invalid("distributed rewrite no-op has no staging cohort"))?;
        ConnectorWriteExecutionRegistration::try_new(session, cohort_id)
            .map_err(|error| invalid(format!("register rewrite cohort execution: {error}")))
    }

    /// Record a completed C1 attempt as accepted and persist its opaque report
    /// set through the provider before frontend durable state advances.
    pub fn checkpoint_accepted(
        &self,
        completion: &ConnectorWriteCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
        self.checkpoint(
            ConnectorDistributedRewriteAttemptDisposition::Accepted,
            completion,
        )
    }

    /// Move a previously accepted C1 attempt to superseded and checkpoint it
    /// for operation-wide cleanup.  It can never contribute to C1 completeness
    /// after this call.
    pub fn checkpoint_superseded(
        &self,
        completion: &ConnectorWriteCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
        let attempt = self.validate_completion(completion)?;
        self.inner
            .write_session
            .as_ref()
            .expect("validated rewrite completion has a write session")
            .supersede_attempt(completion.attachment()?, completion.input()?)?;
        self.persist_checkpoint(
            ConnectorDistributedRewriteAttemptDisposition::Superseded,
            attempt,
        )
    }

    /// Restore one provider-durable attempt only for terminal abort/recovery.
    /// It returns the opaque C1 completion to the caller; this session never
    /// installs it as an accepted staging attempt, so recovery cannot resume
    /// an old execution.
    pub fn restore_for_abort(
        &self,
        checkpoint: &ConnectorDistributedRewriteAttemptCheckpoint,
    ) -> Result<ConnectorWriteAttemptCompletion, ConnectorError> {
        self.validate_checkpoint(checkpoint)?;
        let completion = self
            .inner
            .lease
            .restore_attempt(&self.inner.plan, checkpoint)?;
        if completion.owner() != self.inner.plan.owner()
            || completion.operation_id() != self.operation_id()
            || completion.cohort_id() != checkpoint.cohort_id
            || completion.execution_id() != checkpoint.execution_id
            || completion.digest() != checkpoint.attempt_digest
        {
            return Err(invalid(
                "distributed rewrite restored attempt does not match its checkpoint",
            ));
        }
        self.inner
            .write_session
            .as_ref()
            .expect("non-empty rewrite plan has a write session")
            .restore_for_abort(checkpoint.disposition, completion.clone())?;
        Ok(completion)
    }

    /// Restore the persisted aggregate C1 decision before marker-only
    /// reconcile.  This has no staging path and therefore cannot re-submit a
    /// rewrite that may already have reached the catalog.
    pub fn restore_for_reconcile(&self, aggregate_digest: [u8; 32]) -> Result<(), ConnectorError> {
        self.inner
            .write_session
            .as_ref()
            .ok_or_else(|| invalid("distributed rewrite no-op has no C1 reconcile"))?
            .restore_for_reconcile(aggregate_digest)
    }

    /// Commit every accepted cohort through the same exact C1 control lease.
    pub fn commit(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.inner
            .write_session
            .as_ref()
            .ok_or_else(|| invalid("distributed rewrite no-op has no C1 commit"))?
            .commit(context)
    }

    /// Abort all checkpointed and in-memory staged cohorts through C1.
    pub fn abort(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        self.inner
            .write_session
            .as_ref()
            .ok_or_else(|| invalid("distributed rewrite no-op has no C1 abort"))?
            .abort(context)
    }

    /// Reconcile only after the C1 session made its aggregate commit decision.
    pub fn reconcile(
        &self,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.inner
            .write_session
            .as_ref()
            .ok_or_else(|| invalid("distributed rewrite no-op has no C1 reconcile"))?
            .reconcile(evidence, context)
    }

    /// Project a known-committed C1 receipt only through the provider that
    /// froze this operation.  Callers must not decode the receipt in core.
    pub fn finalize_committed(
        &self,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<ConnectorDistributedRewriteReceipt, ConnectorError> {
        self.inner.lease.finalize_rewrite(&self.inner.plan, receipt)
    }

    pub fn checkpointed_attempts(
        &self,
    ) -> Result<Vec<ConnectorDistributedRewriteAttemptCheckpoint>, ConnectorError> {
        let checkpoints = self.inner.checkpoints.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "distributed rewrite checkpoint state lock poisoned",
            )
        })?;
        Ok(checkpoints.values().cloned().collect())
    }

    fn checkpoint(
        &self,
        disposition: ConnectorDistributedRewriteAttemptDisposition,
        completion: &ConnectorWriteCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
        let attempt = self.validate_completion(completion)?;
        self.persist_checkpoint(disposition, attempt)
    }

    fn validate_completion(
        &self,
        completion: &ConnectorWriteCompletion,
    ) -> Result<ConnectorWriteAttemptCompletion, ConnectorError> {
        let write_session = self.inner.write_session.as_ref().ok_or_else(|| {
            invalid("distributed rewrite no-op cannot checkpoint a staged attempt")
        })?;
        if completion.session().operation_id() != self.operation_id()
            || completion.session().owner() != self.inner.plan.owner()
            || completion.session().sealed().digest() != write_session.sealed().digest()
        {
            return Err(invalid(
                "distributed rewrite completion does not belong to the sealed session",
            ));
        }
        completion.attempt_completion()
    }

    fn persist_checkpoint(
        &self,
        disposition: ConnectorDistributedRewriteAttemptDisposition,
        attempt: ConnectorWriteAttemptCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
        let key = AttemptKey {
            cohort_id: attempt.cohort_id(),
            execution_id: attempt.execution_id(),
            disposition: disposition_tag(disposition),
        };
        let checkpoint =
            self.inner
                .lease
                .checkpoint_attempt(&self.inner.plan, disposition, &attempt)?;
        self.validate_checkpoint(&checkpoint)?;
        if checkpoint.cohort_id != key.cohort_id
            || checkpoint.execution_id != key.execution_id
            || checkpoint.attempt_digest != attempt.digest()
        {
            return Err(invalid(
                "distributed rewrite provider returned a foreign attempt checkpoint",
            ));
        }
        let mut checkpoints = self.inner.checkpoints.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "distributed rewrite checkpoint state lock poisoned",
            )
        })?;
        match checkpoints.get(&key) {
            Some(existing) if existing == &checkpoint => Ok(checkpoint),
            Some(_) => Err(invalid(
                "distributed rewrite attempt checkpoint replay changed durable facts",
            )),
            None => {
                checkpoints.insert(key, checkpoint.clone());
                Ok(checkpoint)
            }
        }
    }

    fn validate_checkpoint(
        &self,
        checkpoint: &ConnectorDistributedRewriteAttemptCheckpoint,
    ) -> Result<(), ConnectorError> {
        checkpoint.validate()?;
        if !self
            .inner
            .plan
            .cohorts()
            .iter()
            .any(|cohort| cohort.cohort_id() == checkpoint.cohort_id)
        {
            return Err(invalid(
                "distributed rewrite checkpoint references an unknown cohort",
            ));
        }
        Ok(())
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

const fn disposition_tag(disposition: ConnectorDistributedRewriteAttemptDisposition) -> u8 {
    match disposition {
        ConnectorDistributedRewriteAttemptDisposition::Accepted => 0,
        ConnectorDistributedRewriteAttemptDisposition::Superseded => 1,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorControlBinding, ConnectorDistributedRewrite,
        ConnectorDistributedRewriteCohortPlan, ConnectorDistributedRewritePlanSummary,
        ConnectorDistributedRewritePlanningRequest, ConnectorExecutionBindingKey,
        ConnectorExecutionDeclaration, ConnectorExecutionDistribution, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorMetadata, ConnectorProviderId,
        ConnectorScanPlanning, ConnectorTableHandle, ConnectorWriteActivationIntent,
        ConnectorWriteActivationRequest, ConnectorWriteActivationSource, ConnectorWriteBaseVersion,
        ConnectorWriteCohortId, ConnectorWriteControl, ConnectorWriteFieldBinding,
        ConnectorWriteFieldToken, ConnectorWriteInputShape, ConnectorWriteIntent,
        ConnectorWritePlan, ConnectorWritePlanningRequest, ConnectorWritePreparation,
    };

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .unwrap()
    }

    fn preparation(
        owner: ConnectorExecutionBindingKey,
        table: ConnectorTableHandle,
        schema: &arrow::datatypes::SchemaRef,
    ) -> ConnectorWritePreparation {
        let fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([index as u8 + 1; 32]),
                    field.as_ref().clone(),
                )
            })
            .collect();
        ConnectorWritePreparation::try_new(
            owner,
            table,
            novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            ConnectorWriteIntent::Overwrite,
            ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).unwrap(),
            ConnectorWriteInputShape::Data { fields },
            Bytes::from_static(b"prepared"),
        )
        .unwrap()
    }

    struct TestMetadata {
        instance: ConnectorInstanceId,
    }

    struct TestPlanning {
        instance: ConnectorInstanceId,
    }

    impl ConnectorScanPlanning for TestPlanning {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance
        }

        fn begin_scan(
            &self,
            _table: &ConnectorTableHandle,
            _request: novarocks_spi::connector::ConnectorBeginScanRequest,
        ) -> Result<novarocks_spi::connector::ConnectorScan, ConnectorError> {
            unreachable!("rewrite session does not plan scans")
        }

        fn plan_splits(
            &self,
            _scan: &novarocks_spi::connector::ConnectorScanHandle,
            _request: novarocks_spi::connector::ConnectorSplitPlanningRequest,
        ) -> Result<novarocks_spi::connector::ConnectorSplitPlanningResult, ConnectorError>
        {
            unreachable!("rewrite session does not plan scans")
        }
    }

    impl ConnectorMetadata for TestMetadata {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance
        }
        fn namespace_exists(
            &self,
            _request: novarocks_spi::connector::ConnectorNamespaceRequest,
        ) -> Result<bool, ConnectorError> {
            unreachable!("rewrite session does not load metadata")
        }
        fn table_exists(
            &self,
            _request: novarocks_spi::connector::ConnectorTableRequest,
        ) -> Result<bool, ConnectorError> {
            unreachable!("rewrite session does not load metadata")
        }
        fn list_tables(
            &self,
            _request: novarocks_spi::connector::ConnectorListTablesRequest,
        ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
            unreachable!("rewrite session does not load metadata")
        }
        fn load_table(
            &self,
            _request: novarocks_spi::connector::ConnectorTableRequest,
        ) -> Result<novarocks_spi::connector::ConnectorTableMetadata, ConnectorError> {
            unreachable!("rewrite session does not load metadata")
        }
    }

    struct TestRewrite {
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorExecutionBindingKey,
    }

    impl ConnectorDistributedRewrite for TestRewrite {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }
        fn plan_rewrite(
            &self,
            _request: ConnectorDistributedRewritePlanningRequest,
        ) -> Result<ConnectorDistributedRewritePlan, ConnectorError> {
            unreachable!()
        }
        fn activate_rewrite(
            &self,
            plan: &ConnectorDistributedRewritePlan,
            context: ConnectorRequestContext,
        ) -> Result<novarocks_spi::connector::ConnectorWriteActivation, ConnectorError> {
            let source = plan.cohorts().first().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "test rewrite activation requires a cohort",
                )
            })?;
            novarocks_spi::connector::ConnectorWriteActivation::try_new(
                self.key.clone(),
                &ConnectorWriteActivationRequest {
                    operation_id: plan.operation_id(),
                    source: ConnectorWriteActivationSource::Prepared(source.preparation().clone()),
                    intent: ConnectorWriteActivationIntent::Ordinary,
                    context,
                },
                plan.cohorts()
                    .iter()
                    .map(|cohort| (cohort.cohort_id(), cohort.preparation().clone()))
                    .collect(),
            )
        }
        fn checkpoint_attempt(
            &self,
            _plan: &ConnectorDistributedRewritePlan,
            _disposition: ConnectorDistributedRewriteAttemptDisposition,
            _completion: &ConnectorWriteAttemptCompletion,
        ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
            unreachable!()
        }
        fn restore_attempt(
            &self,
            _plan: &ConnectorDistributedRewritePlan,
            _checkpoint: &ConnectorDistributedRewriteAttemptCheckpoint,
        ) -> Result<ConnectorWriteAttemptCompletion, ConnectorError> {
            unreachable!()
        }
        fn finalize_rewrite(
            &self,
            _plan: &ConnectorDistributedRewritePlan,
            _receipt: &ConnectorWriteReceipt,
        ) -> Result<ConnectorDistributedRewriteReceipt, ConnectorError> {
            unreachable!()
        }
    }

    struct TestWrite {
        key: ConnectorExecutionBindingKey,
    }
    impl ConnectorWriteControl for TestWrite {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }
        fn plan_write(
            &self,
            _request: ConnectorWritePlanningRequest,
        ) -> Result<ConnectorWritePlan, ConnectorError> {
            unreachable!()
        }
        fn commit(
            &self,
            _request: novarocks_spi::connector::ConnectorWriteCommitRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            unreachable!()
        }
        fn abort(
            &self,
            _request: novarocks_spi::connector::ConnectorWriteAbortRequest,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            unreachable!()
        }
        fn reconcile(
            &self,
            _request: novarocks_spi::connector::ConnectorWriteReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            unreachable!()
        }
    }

    struct TestDistribution {
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorExecutionBindingKey,
    }
    impl ConnectorExecutionDistribution for TestDistribution {
        fn declaration(
            &self,
            _context: &ConnectorRequestContext,
        ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
            ConnectorExecutionDeclaration::try_new(
                self.descriptor.clone(),
                self.key.incarnation,
                Bytes::from_static(b"test"),
            )
        }
    }

    fn fixture(
        cohorts: usize,
    ) -> (
        ConnectorDistributedRewritePlan,
        ConnectorDistributedRewriteLease,
    ) {
        let provider = ConnectorProviderId::parse("rewrite-session-test").unwrap();
        let instance = ConnectorInstanceId::parse("rewrite-session-instance").unwrap();
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: provider,
            instance_id: instance.clone(),
        };
        let key = ConnectorExecutionBindingKey {
            instance_id: instance.clone(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        };
        let operation_id = ConnectorWriteOperationId::new();
        let table =
            ConnectorTableHandle::try_new(instance.clone(), Bytes::from_static(b"table")).unwrap();
        let request = ConnectorDistributedRewritePlanningRequest::try_new(
            operation_id,
            key.clone(),
            novarocks_spi::connector::ConnectorDistributedRewriteOperation::RewriteDataFiles {
                table: table.clone(),
                rewrite_all: true,
            },
            context(),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let cohort_plans = (0..cohorts)
            .map(|index| {
                let digest = [u8::try_from(index).unwrap_or_default(); 32];
                ConnectorDistributedRewriteCohortPlan::try_new(
                    ConnectorWriteCohortId::derive(operation_id, b"test", digest).unwrap(),
                    table.clone(),
                    schema.clone(),
                    [3; 32],
                    preparation(key.clone(), table.clone(), &schema),
                    digest,
                )
                .unwrap()
            })
            .collect();
        let plan = ConnectorDistributedRewritePlan::try_new(
            &request,
            [1; 32],
            [2; 32],
            ConnectorDistributedRewritePlanSummary {
                groups: cohorts as u64,
                ..Default::default()
            },
            Bytes::from_static(b"plan"),
            cohort_plans,
        )
        .unwrap();
        let rewrite = Arc::new(TestRewrite {
            descriptor: descriptor.clone(),
            key: key.clone(),
        });
        let lease = ConnectorDistributedRewriteLease::new(
            descriptor.clone(),
            key.clone(),
            novarocks_spi::connector::ConnectorControlPlanningLease::new(
                Arc::new(
                    ConnectorControlBinding::try_new(
                        descriptor.clone(),
                        key.incarnation,
                        Arc::new(TestMetadata {
                            instance: key.instance_id.clone(),
                        }),
                        Arc::new(TestPlanning {
                            instance: key.instance_id.clone(),
                        }),
                        Arc::new(TestDistribution {
                            descriptor: descriptor.clone(),
                            key: key.clone(),
                        }),
                        None,
                    )
                    .unwrap(),
                ),
                || {},
            ),
            Arc::new(TestMetadata {
                instance: instance.clone(),
            }),
            Arc::new(TestPlanning { instance }),
            rewrite,
            Arc::new(TestWrite { key: key.clone() }),
            Arc::new(TestDistribution { descriptor, key }),
            || {},
        )
        .unwrap();
        (plan, lease)
    }

    #[test]
    fn seals_every_frozen_cohort_before_execution_registration() {
        let (plan, lease) = fixture(2);
        let cohort_ids = plan
            .cohorts()
            .iter()
            .map(|cohort| cohort.cohort_id())
            .collect::<Vec<_>>();
        let session =
            ConnectorDistributedRewriteSession::try_begin(plan, lease, context()).unwrap();
        assert!(!session.is_noop());
        let sealed = session.write_session().unwrap().sealed();
        assert_eq!(sealed.cohorts().len(), 2);
        for cohort_id in cohort_ids {
            assert_eq!(
                session
                    .execution_registration(cohort_id)
                    .unwrap()
                    .single_cohort_id()
                    .unwrap(),
                cohort_id
            );
        }
    }

    #[test]
    fn empty_plan_is_noop_without_writer_session() {
        let (plan, lease) = fixture(0);
        let session =
            ConnectorDistributedRewriteSession::try_begin(plan, lease, context()).unwrap();
        assert!(session.is_noop());
        assert!(session.write_session().is_none());
    }
}
