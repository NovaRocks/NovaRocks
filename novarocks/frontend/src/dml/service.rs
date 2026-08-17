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

use std::sync::{Arc, RwLock};

use novarocks::catalog_application::query_catalog::QueryCatalogService;
use novarocks::query_execution::dml::ctas::CtasEngine;
use tokio::runtime::Handle;

use crate::coordination::FrontendCoordinationRuntime;
use crate::dml::coordination::{ActiveDmlOperation, DmlCoordinator};
use crate::dml::ctas::recovery::{CtasRecoveryProfile, CtasRecoveryProgress};
use crate::dml::error::DmlError;
use crate::dml::journal::OperationJournal;
use crate::dml::model::{
    CreatePreparingRequest, CreateStatementOperationRequest, DmlOperationId, DmlRecoveryCandidate,
    StoredOperation, WriteTransactionOutcome, WriteTransactionSpec,
};
use crate::dml::runner::{
    ActiveWriteTransactionRunner, AlwaysAdmit, WriteAdmission, WriteExecutor,
    WriteTransactionRunner, preparing_request,
};
use crate::dml::statement_recovery::{
    HistoricalDataMutationRecoveryResolver, StatementRecoveryProfile, StatementRecoveryProgress,
    direct_mutation_kind, is_authority_loss,
};
use crate::dml::write_recovery::HistoricalWriteRecoveryResolver;
use crate::statistics::{FrontendStatisticsService, StatisticsColumn};

/// The frontend DML application owner. Composes the narrow ports (journal +
/// admission) and drives write transactions. Constructed from narrow handles —
/// never from the host or a service locator.
pub struct DmlService {
    journal: Option<Arc<dyn OperationJournal>>,
    statistics: Arc<FrontendStatisticsService>,
    local_catalog: RwLock<Option<Arc<QueryCatalogService>>>,
    admission: Arc<dyn WriteAdmission>,
    coordinator: Option<DmlCoordinator>,
    allow_unfenced_focused_test_support: bool,
    /// The CP-3C direct data-mutation recovery profile, installed only when the
    /// host can resolve the current provider generation's historical facet. A
    /// service without it defers statement-family recovery instead of
    /// classifying anything.
    statement_recovery: Option<StatementRecoveryProfile>,
    ctas_recovery: RwLock<Option<Arc<dyn CtasEngine>>>,
    ctas_write_recovery: RwLock<Option<Arc<dyn HistoricalWriteRecoveryResolver>>>,
}

#[derive(Debug)]
pub(crate) enum DmlRecoveryProgress {
    Statement(StatementRecoveryProgress),
    Ctas(CtasRecoveryProgress),
}

impl DmlService {
    /// Build a journal-backed service with frontend-local statistics observation.
    ///
    /// Production composition uses [`Self::compose`]; this constructor keeps
    /// the statement-agnostic DML-1 runner usable in focused tests.
    #[doc(hidden)]
    pub fn new(journal: Arc<dyn OperationJournal>) -> Self {
        Self::compose(Some(journal), Arc::new(FrontendStatisticsService::new()))
    }

    /// Compose the production DML owner from optional StateStore capability
    /// and the host-owned statistics service.
    #[doc(hidden)]
    pub fn compose(
        journal: Option<Arc<dyn OperationJournal>>,
        statistics: Arc<FrontendStatisticsService>,
    ) -> Self {
        Self {
            journal,
            statistics,
            local_catalog: RwLock::new(None),
            admission: Arc::new(AlwaysAdmit),
            coordinator: None,
            allow_unfenced_focused_test_support: true,
            statement_recovery: None,
            ctas_recovery: RwLock::new(None),
            ctas_write_recovery: RwLock::new(None),
        }
    }

    /// Compose with real coordination.
    ///
    /// Hidden from the public API but reachable from integration tests, so a
    /// route test can exercise the fenced dispatch path instead of the
    /// unfenced focused-test seam.
    #[doc(hidden)]
    pub fn compose_with_coordination(
        journal: Option<Arc<dyn OperationJournal>>,
        statistics: Arc<FrontendStatisticsService>,
        frontend: Arc<FrontendCoordinationRuntime>,
        runtime: Handle,
    ) -> Self {
        Self {
            journal,
            statistics,
            local_catalog: RwLock::new(None),
            admission: Arc::new(AlwaysAdmit),
            coordinator: Some(DmlCoordinator::new(frontend, runtime)),
            allow_unfenced_focused_test_support: false,
            statement_recovery: None,
            ctas_recovery: RwLock::new(None),
            ctas_write_recovery: RwLock::new(None),
        }
    }

    /// Install the CP-3C direct data-mutation recovery profile.
    ///
    /// The resolver reaches the *current* provider generation's separately
    /// installed historical facet. Without it the bounded controller keeps
    /// deferring TRUNCATE and ADD FILES rather than guessing anything about
    /// external truth.
    pub fn install_statement_recovery(
        &mut self,
        resolver: Arc<dyn HistoricalDataMutationRecoveryResolver>,
    ) {
        self.statement_recovery = Some(StatementRecoveryProfile::new(resolver));
    }

    /// Install the current-generation Core CTAS historical reverse port.
    /// The bounded controller may start before this late-bound dependency;
    /// until installation, CTAS candidates are safely deferred.
    pub fn install_ctas_recovery(&self, engine: Arc<dyn CtasEngine>) {
        *self
            .ctas_recovery
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(engine);
    }

    /// Install the current-generation CP-3B historical write resolver used by
    /// CTAS takeover before any retained staged target may be cleaned up.
    pub fn install_ctas_write_recovery(&self, resolver: Arc<dyn HistoricalWriteRecoveryResolver>) {
        *self
            .ctas_write_recovery
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(resolver);
    }

    /// Build a service with a custom admission gate (CP-3 fencing).
    pub(crate) fn with_admission(
        journal: Option<Arc<dyn OperationJournal>>,
        statistics: Arc<FrontendStatisticsService>,
        admission: Arc<dyn WriteAdmission>,
    ) -> Self {
        Self {
            journal,
            statistics,
            local_catalog: RwLock::new(None),
            admission,
            coordinator: None,
            allow_unfenced_focused_test_support: true,
            statement_recovery: None,
            ctas_recovery: RwLock::new(None),
            ctas_write_recovery: RwLock::new(None),
        }
    }

    pub(crate) fn begin_write_operation(
        &self,
        request: CreatePreparingRequest,
    ) -> Result<ActiveDmlOperation, DmlError> {
        let journal = self.require_journal_arc()?;
        let Some(coordinator) = self.coordinator.as_ref() else {
            if self.allow_unfenced_focused_test_support {
                let operation_id = journal.create_preparing(request)?;
                let operation = journal.load(operation_id)?.ok_or_else(|| {
                    DmlError::journal_unresolved(format!(
                        "created DML operation {operation_id} cannot be read back"
                    ))
                })?;
                return Ok(ActiveDmlOperation::legacy(journal, operation));
            }
            return Err(DmlError::coordination_unresolved(
                "frontend DML coordination is not installed for this service",
            ));
        };
        let operation_id = journal.create_preparing_admitted(request, coordinator.admission()?)?;
        let operation = journal.load(operation_id)?.ok_or_else(|| {
            DmlError::journal_unresolved(format!(
                "created DML operation {operation_id} cannot be read back"
            ))
        })?;
        coordinator.claim_foreground(journal, operation)
    }

    pub(crate) fn begin_statement_operation(
        &self,
        request: CreateStatementOperationRequest,
    ) -> Result<ActiveDmlOperation, DmlError> {
        let journal = self.require_journal_arc()?;
        let Some(coordinator) = self.coordinator.as_ref() else {
            if self.allow_unfenced_focused_test_support {
                let operation = journal.create_statement_operation(request)?;
                return Ok(ActiveDmlOperation::legacy(journal, operation));
            }
            return Err(DmlError::coordination_unresolved(
                "frontend DML coordination is not installed for this service",
            ));
        };
        let operation =
            journal.create_statement_operation_admitted(request, coordinator.admission()?)?;
        coordinator.claim_foreground(journal, operation)
    }

    pub(crate) async fn shutdown_coordination(&self) -> Result<(), DmlError> {
        if let Some(coordinator) = &self.coordinator {
            coordinator.shutdown().await?;
        }
        Ok(())
    }

    pub(crate) fn recovery_candidates(
        &self,
        shard: u8,
        due_at_or_before_ms: i64,
    ) -> Result<Vec<DmlRecoveryCandidate>, DmlError> {
        self.require_journal()?
            .recovery_candidates(shard, due_at_or_before_ms)
    }

    pub(crate) fn defer_recovery_candidate(
        &self,
        candidate: DmlRecoveryCandidate,
        next_due_at_ms: i64,
    ) -> Result<(), DmlError> {
        let Some(mut active) = self.claim_recovery_candidate(candidate)? else {
            return Ok(());
        };
        let result = active.reschedule_recovery_due(Some(next_due_at_ms));
        let release = active.release();
        result.and(release)
    }

    /// Drive one claimed recovery candidate through its family profile.
    ///
    /// The claim is taken under the candidate's exact operation lease, exactly
    /// as the blanket deferral took it. CTAS converges through CP-3D, direct
    /// mutations through CP-3C, and unsupported families keep the bounded
    /// deferral. The lease is released either way.
    pub(crate) fn drive_recovery_candidate(
        &self,
        candidate: DmlRecoveryCandidate,
        now_ms: i64,
        deferred_due_at_ms: i64,
    ) -> Result<Option<DmlRecoveryProgress>, DmlError> {
        let Some(mut active) = self.claim_recovery_candidate(candidate)? else {
            return Ok(None);
        };
        // The scan candidate carries no family, so the decision is taken from
        // the claimed operation itself. CTAS and direct mutations share this
        // one bounded scheduler but retain independent typed profiles.
        let result = if active.stored.operation_kind
            == crate::dml::model::OperationKind::CreateTableAsSelect
        {
            let engine = self
                .ctas_recovery
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            match engine {
                Some(engine) => {
                    let write_recovery = self
                        .ctas_write_recovery
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone();
                    CtasRecoveryProfile::new(engine, write_recovery)
                }
                .drive(&mut active, now_ms)
                .map(|progress| Some(DmlRecoveryProgress::Ctas(progress))),
                None => active
                    .reschedule_recovery_due(Some(deferred_due_at_ms))
                    .map(|()| None),
            }
        } else if direct_mutation_kind(active.stored.operation_kind).is_some() {
            match self.statement_recovery.as_ref() {
                Some(profile) => profile
                    .drive(&mut active, now_ms)
                    .map(|progress| Some(DmlRecoveryProgress::Statement(progress))),
                None => active
                    .reschedule_recovery_due(Some(deferred_due_at_ms))
                    .map(|()| None),
            }
        } else {
            active
                .reschedule_recovery_due(Some(deferred_due_at_ms))
                .map(|()| None)
        };
        // The lease is released whichever way the cycle went: a profile failure
        // must never strand the operation under a dead owner.
        let release = active.release();
        match result {
            Ok(progress) => release.map(|()| progress),
            Err(error) => {
                // Losing the lease mid-cycle is an ordinary takeover, not a
                // fault: the new owner re-drives the same immutable request.
                // The release failure, if any, is subordinate to the cycle
                // failure that already explains this operation's state.
                if let Err(release_error) = release {
                    tracing::debug!(
                        error = %release_error,
                        "historical data mutation recovery could not release its lease"
                    );
                }
                if is_authority_loss(&error) {
                    tracing::debug!(
                        error = %error,
                        "historical data mutation recovery lost its authority mid-cycle"
                    );
                }
                Err(error)
            }
        }
    }

    /// Re-read and claim one recovery candidate under its exact operation
    /// lease, or report that it moved on since the scan observed it.
    fn claim_recovery_candidate(
        &self,
        candidate: DmlRecoveryCandidate,
    ) -> Result<Option<ActiveDmlOperation>, DmlError> {
        let journal = self.require_journal_arc()?;
        let Some(operation) = journal.load(candidate.operation_id)? else {
            return Ok(None);
        };
        if operation.revision != candidate.operation_revision
            || operation.last_mutation_id != candidate.last_mutation_id
            || operation.recovery_due_at_ms != Some(candidate.recovery_due_at_ms)
        {
            return Ok(None);
        }
        self.require_coordinator()?
            .claim_recovery(journal, operation)
            .map(Some)
    }

    /// Run one Iceberg write transaction with the given executor.
    pub fn run_write<E: WriteExecutor>(
        &self,
        spec: WriteTransactionSpec,
        executor: &E,
    ) -> Result<WriteTransactionOutcome, DmlError> {
        if self.coordinator.is_some() {
            let operation = self.begin_write_operation(preparing_request(&spec))?;
            return ActiveWriteTransactionRunner::new(operation, executor).run(spec);
        }
        if !self.allow_unfenced_focused_test_support {
            return Err(DmlError::coordination_unresolved(
                "frontend DML coordination is not installed for this service",
            ));
        }
        let journal = self.require_journal()?;
        let runner = WriteTransactionRunner::new(journal, executor, self.admission.as_ref());
        runner.run(spec)
    }

    pub(crate) fn require_journal(&self) -> Result<&dyn OperationJournal, DmlError> {
        self.journal.as_deref().ok_or_else(|| {
            DmlError::journal_unavailable(
                "state store is required for Iceberg DML; configure [state_store]",
            )
        })
    }

    fn require_journal_arc(&self) -> Result<Arc<dyn OperationJournal>, DmlError> {
        self.journal.clone().ok_or_else(|| {
            DmlError::journal_unavailable(
                "state store is required for Iceberg DML; configure [state_store]",
            )
        })
    }

    fn require_coordinator(&self) -> Result<&DmlCoordinator, DmlError> {
        self.coordinator.as_ref().ok_or_else(|| {
            DmlError::coordination_unresolved(
                "frontend DML coordination is not installed for this service",
            )
        })
    }

    pub(crate) fn statistics(&self) -> &FrontendStatisticsService {
        self.statistics.as_ref()
    }

    /// Install the frontend-local catalog used solely to update legacy
    /// in-memory statistics observation after a successful local DML command.
    /// This is not a connector metadata path and cannot resolve an external
    /// schema after the command's admitted target has been released.
    pub(crate) fn install_local_catalog(&self, catalog: Arc<QueryCatalogService>) {
        *self
            .local_catalog
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(catalog);
    }

    pub(crate) fn local_statistics_columns(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Option<Vec<StatisticsColumn>>, DmlError> {
        let catalog = self
            .local_catalog
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(catalog) = catalog else {
            return Ok(None);
        };
        match local_table_columns(catalog.as_ref(), database, table) {
            Ok(columns) => Ok(Some(columns)),
            Err(error)
                if error.starts_with("unknown database:")
                    || error.starts_with("unknown table:") =>
            {
                Ok(None)
            }
            Err(error) => Err(DmlError::executor(error)),
        }
    }

    /// Load a stored operation by id.
    pub fn load_operation(
        &self,
        operation_id: DmlOperationId,
    ) -> Result<Option<StoredOperation>, DmlError> {
        self.require_journal()?.load(operation_id)
    }

    /// List all durable operations for lifecycle inspection and recovery audits.
    pub fn list_operations(&self) -> Result<Vec<StoredOperation>, DmlError> {
        self.require_journal()?.list_operations()
    }

    /// List operations that have not reached a terminal state (recovery input).
    pub fn list_unfinished_operations(&self) -> Result<Vec<StoredOperation>, DmlError> {
        self.require_journal()?.list_unfinished()
    }
}

fn local_table_columns(
    catalog_service: &QueryCatalogService,
    database: &str,
    table: &str,
) -> Result<Vec<StatisticsColumn>, String> {
    let catalog = catalog_service
        .local()
        .read()
        .expect("frontend local catalog read lock");
    let table = novarocks_sql::planning::catalog::local_catalog_table(&catalog, database, table)?;
    Ok(table
        .columns
        .iter()
        .map(|column| StatisticsColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::DmlService;
    use crate::dml::journal::testing::InMemoryOperationJournal;
    use crate::dml::model::{OperationKind, OperationState, OperationTarget, WriteTransactionSpec};
    use crate::dml::runner::{CoordinatedWriteReport, WriteExecutor};
    use novarocks::catalog_application::query_catalog::new_query_catalog_service;
    use novarocks_catalog::schema::ColumnDef;

    struct OkExecutor;

    impl WriteExecutor for OkExecutor {
        type CommitHandle = ();
        type AbortHandle = std::convert::Infallible;

        fn run_coordinated_write(
            &self,
            _spec: &WriteTransactionSpec,
        ) -> Result<CoordinatedWriteReport<()>, String> {
            Ok(CoordinatedWriteReport::CommitRequired(()))
        }

        fn abort(
            &self,
            _spec: &WriteTransactionSpec,
            handle: &Self::AbortHandle,
        ) -> Result<novarocks_spi::connector::ConnectorWriteAbortOutcome, String> {
            match *handle {}
        }

        fn commit(
            &self,
            _spec: &WriteTransactionSpec,
            _handle: &(),
        ) -> Result<
            novarocks_spi::connector::ExternalMutationOutcome<
                novarocks_spi::connector::ConnectorWriteReceipt,
            >,
            String,
        > {
            Ok(
                novarocks_spi::connector::ExternalMutationOutcome::KnownCommitted {
                    effect: novarocks_spi::connector::ExternalMutationEffect::Applied,
                    receipt: novarocks_spi::connector::ConnectorWriteReceipt::try_new(
                        bytes::Bytes::from_static(b"test-receipt"),
                    )
                    .expect("receipt"),
                    finalization: novarocks_spi::connector::ExternalMutationFinalization::Complete,
                },
            )
        }

        fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
            Ok(())
        }
    }

    fn spec() -> WriteTransactionSpec {
        WriteTransactionSpec {
            target: OperationTarget {
                catalog: "c".to_string(),
                namespace: "n".to_string(),
                table: "t".to_string(),
                ref_name: None,
            },
            operation_kind: OperationKind::InsertAppend,
            operation_subkind: None,
            attempt_id: "a".to_string(),
            base_snapshot_id: None,
            base_snapshot_map: BTreeMap::new(),
        }
    }

    #[test]
    fn service_runs_write_and_exposes_operation() {
        let service = DmlService::new(Arc::new(InMemoryOperationJournal::default()));
        let outcome = service.run_write(spec(), &OkExecutor).unwrap();
        let id = outcome.operation_id.unwrap();
        assert_eq!(
            service.load_operation(id).unwrap().unwrap().state,
            OperationState::Finalized
        );
        let operations = service.list_operations().unwrap();
        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].operation_id, id);
        assert_eq!(operations[0].state, OperationState::Finalized);
        assert!(service.list_unfinished_operations().unwrap().is_empty());
    }

    #[test]
    fn local_statistics_observation_never_resolves_an_external_schema() {
        let catalog = Arc::new(new_query_catalog_service());
        {
            let mut local = catalog
                .local()
                .write()
                .expect("frontend local catalog write lock");
            local.create_database("analytics").expect("create database");
            novarocks_sql::planning::catalog::register_test_connector_read_table(
                &mut local,
                "analytics",
                "orders",
                vec![ColumnDef {
                    name: "order_id".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
            )
            .expect("register local table schema");
        }
        let service = DmlService::new(Arc::new(InMemoryOperationJournal::default()));
        service.install_local_catalog(catalog);

        let columns = service
            .local_statistics_columns("analytics", "orders")
            .expect("load local schema")
            .expect("local table has a schema");
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "order_id");
        assert!(
            service
                .local_statistics_columns("analytics", "missing")
                .expect("unknown local table is not an observation failure")
                .is_none()
        );
    }
}
