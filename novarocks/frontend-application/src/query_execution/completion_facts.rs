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

//! How this process answers what the compiler asks.
//!
//! Query application decides what to ask and in what order, and knows nothing
//! about where an answer comes from. These adapters are the other half: each
//! one knows a single owner in this process and nothing about compilation.
//! Neither half can be made to do the other's job, which is the point - it is
//! how the compiler stays pure while its answers come from a catalog, a
//! statistics resolver and a materialized-view inventory that are anything but.
//!
//! Physical observations await their original async owners directly. Schema,
//! revision and selection interpretation then consume those completed facts;
//! the shared connector worker remains only for explicitly authored leaf work.

mod planning_cpu;
mod provider_source;
pub(crate) use planning_cpu::{
    CompletedReadCpuInput, FrontendPlanningCpuSource, LogicalFinishInput, ReadEncodingInput,
    ReadEncodingPurpose,
};
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(novarocks_dev_controls)]
pub(crate) use planning_cpu::ReadRewriteInput;
pub(crate) use provider_source::FrontendCompletionCustody;

/// Borrow the original Provider and pure CPU sources for one completion.
pub(crate) struct FrontendCompletionPorts<'a> {
    pub(crate) provider: &'a FrontendCompletionCustody,
    pub(crate) cpu: &'a FrontendPlanningCpuSource,
}

use std::sync::Arc;

use async_trait::async_trait;
use novarocks_catalog_application::{CatalogApplicationPort, ConnectorControlHost};
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_query_application::preparation::{
    CatalogFactPort, MaterializedViewFactPort, ProviderReadFactPort, QueryCompletionFactSource,
    StatisticsFactPort,
};
use novarocks_spi::connector::{
    ConnectorRequestContext, MvStorageObservationPort, read_stack::ConnectorSession,
};
use novarocks_sql::compiler::{
    CatalogLookupTarget, CatalogRelationFact, CatalogRelationNeed, MaterializedViewFact,
    MaterializedViewNeed, StatisticsFact, StatisticsNeed,
};

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::catalog_application::query_catalog::{
    CatalogResolutionError, CatalogResolutionResult, QueryCatalogService,
};
use crate::catalog_application::query_materializer::{
    CatalogServiceMaterializer, iceberg_table_binding_loader,
};
use crate::connector::UnifiedStatisticsResolver;
use crate::mv::domain::readiness::MvCandidateReader;
use crate::mv::domain::rewrite_prep::freeze_materialized_view_fact_with_ports_async;
use crate::query_execution::planning::statistics::resolve_statistics_need_async;
use crate::query_execution::provider_read_facts::{
    FrontendProviderReadFacts, FrozenProviderRead, ProviderCreatorMode,
};
use crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor;

/// The owners in this process that can answer a compilation question, and the
/// lane their synchronous calls are admitted through.
///
/// One value per process: nothing here is statement-scoped.
#[derive(Clone)]
pub(crate) struct CompletionFactOwners {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<ConnectorControlHost>,
    statistics: Arc<UnifiedStatisticsResolver>,
    materialized_views: MvCandidateReader,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    blocking: ConnectorBlockingIoSupervisor,
}

impl CompletionFactOwners {
    pub(crate) const fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<ConnectorControlHost>,
        statistics: Arc<UnifiedStatisticsResolver>,
        materialized_views: MvCandidateReader,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        blocking: ConnectorBlockingIoSupervisor,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            statistics,
            materialized_views,
            mv_storage_observation,
            blocking,
        }
    }
}

/// What one statement adds to those owners.
///
/// The binding store is the statement's own: every fact about a relation is
/// answered against the bindings that statement was admitted under, never
/// against whatever the catalog holds at the moment the question is asked.
#[derive(Clone)]
pub(crate) struct StatementFactScope {
    state_store: StatementStateStore,
    creator_mode: ProviderCreatorMode,
    bindings: Arc<QueryTableBindingStore>,
    connector_context: ConnectorRequestContext,
    current_catalog: Option<Arc<str>>,
    capacity: QueryResultCapacityBinding,
    query_local_overlays:
        Vec<crate::catalog_application::query_materializer::QueryLocalTableOverlay>,
}

#[derive(Clone)]
enum StatementStateStore {
    Legacy,
    Governed(Option<Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>>),
}
impl StatementFactScope {
    pub(crate) fn binding_only(
        bindings: Arc<QueryTableBindingStore>,
        connector_context: ConnectorRequestContext,
        current_catalog: Option<&str>,
        work_scope: &novarocks_workload_control::WorkScope,
        capacity: QueryResultCapacityBinding,
    ) -> Result<Self, novarocks_workload_control::WorkError> {
        if !capacity.window_alias().is_for_scope(work_scope) {
            return Err(novarocks_workload_control::WorkError::Conflict);
        }
        Ok(Self {
            creator_mode: ProviderCreatorMode::BindingOnly,
            state_store: StatementStateStore::Legacy,
            bindings,
            connector_context,
            current_catalog: current_catalog.map(Arc::from),
            capacity,
            query_local_overlays: Vec::new(),
        })
    }
    /// Retained pre-Original callers select their legacy MV adapter explicitly.
    /// Their Provider I/O is governed, but their StateStore migration is separate.
    pub(crate) fn governed_read_io(
        bindings: Arc<QueryTableBindingStore>,
        connector_context: ConnectorRequestContext,
        current_catalog: Option<&str>,
        work_scope: &novarocks_workload_control::WorkScope,
        capacity: QueryResultCapacityBinding,
    ) -> Result<Self, novarocks_workload_control::WorkError> {
        let mut scope = Self::governed_read_io_in(
            bindings,
            connector_context,
            current_catalog,
            work_scope,
            capacity,
            None,
        )?;
        scope.state_store = StatementStateStore::Legacy;
        Ok(scope)
    }
    /// Migrated Read explicitly supplies its request view. Missing Original
    /// execution makes optional MV rewrite unavailable, never a legacy fallback.
    pub(crate) fn governed_read_io_in(
        bindings: Arc<QueryTableBindingStore>,
        connector_context: ConnectorRequestContext,
        current_catalog: Option<&str>,
        work_scope: &novarocks_workload_control::WorkScope,
        capacity: QueryResultCapacityBinding,
        state_store: Option<Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>>,
    ) -> Result<Self, novarocks_workload_control::WorkError> {
        if connector_context
            .io_admission()
            .and_then(|p| p.upgrade())
            .is_none()
        {
            return Err(novarocks_workload_control::WorkError::NotReady);
        }
        let mut scope = Self::binding_only(
            bindings,
            connector_context,
            current_catalog,
            work_scope,
            capacity,
        )?;
        scope.creator_mode = ProviderCreatorMode::GovernedReadIo;
        scope.state_store = StatementStateStore::Governed(state_store);
        Ok(scope)
    }
    pub(crate) fn with_query_local_overlays(
        mut self,
        overlays: Vec<crate::catalog_application::query_materializer::QueryLocalTableOverlay>,
    ) -> Self {
        self.query_local_overlays = overlays;
        self
    }
}

/// Answers relation lookups from the frontend catalog.
pub(crate) struct FrontendCatalogFacts {
    owners: CompletionFactOwners,
    scope: StatementFactScope,
    // One original fact source serializes its own observation rounds. This
    // does not allocate another binding identity or provider cache.
    resolving: tokio::sync::Mutex<()>,
}

impl FrontendCatalogFacts {
    pub(crate) const fn new(owners: CompletionFactOwners, scope: StatementFactScope) -> Self {
        Self {
            owners,
            scope,
            resolving: tokio::sync::Mutex::const_new(()),
        }
    }
}

#[async_trait]
impl CatalogFactPort for FrontendCatalogFacts {
    async fn resolve_relations(
        &self,
        needs: &[CatalogRelationNeed],
    ) -> Result<Vec<CatalogRelationFact>, String> {
        let _round = self.resolving.lock().await;
        let loader = iceberg_table_binding_loader(
            self.owners.connector_control.as_ref(),
            self.scope.connector_context.clone(),
        );
        let materializer = CatalogServiceMaterializer::new_with_query_local_overlays(
            self.scope.current_catalog.as_deref(),
            self.owners.catalog_service.as_ref(),
            Arc::clone(&self.scope.bindings),
            loader,
            self.scope.query_local_overlays.clone(),
        )
        .with_catalog_application(self.owners.catalog_application.as_deref());
        let mut facts = Vec::with_capacity(needs.len());
        for need in needs {
            let relation = need.relation();
            let resolved = match need.target() {
                CatalogLookupTarget::Table { .. } => {
                    materializer
                        .resolve_table_for_analysis_async(
                            Some(&relation.catalog),
                            &relation.namespace,
                            &relation.table,
                        )
                        .await
                }
                CatalogLookupTarget::IcebergMetadata { kind } => {
                    materializer
                        .resolve_metadata_table_async(
                            Some(&relation.catalog),
                            &relation.namespace,
                            &relation.table,
                            kind,
                        )
                        .await
                }
            };
            facts.push(catalog_fact_from(need, resolved)?);
        }
        Ok(facts)
    }
}

/// One relation, resolved and turned back into the answer to its own question.
///
/// Absence and failure are different answers, and only absence is the
/// compiler's to interpret: a relation that is not there is a fact about the
/// query, while a catalog that could not say is a fact about this process. The
/// second must not be flattened into the first, or a query over a healthy table
/// would report that the table does not exist whenever the catalog is down.
pub(super) fn catalog_fact_from(
    need: &CatalogRelationNeed,
    resolved: CatalogResolutionResult<novarocks_sql::planning::catalog::ResolvedAnalyzerTable>,
) -> Result<CatalogRelationFact, String> {
    match resolved {
        Ok(table) => CatalogRelationFact::resolved(need, table),
        Err(CatalogResolutionError::Missing { reason }) => {
            CatalogRelationFact::missing(need, reason)
        }
        Err(error @ CatalogResolutionError::Failed { .. }) => {
            return Err(error.into_message());
        }
    }
    .map_err(|error| format!("build catalog completion fact: {error}"))
}

/// Answers statistics questions from the one unified resolver.
pub(crate) struct FrontendStatisticsFacts {
    owners: CompletionFactOwners,
    scope: StatementFactScope,
}

impl FrontendStatisticsFacts {
    pub(crate) const fn new(owners: CompletionFactOwners, scope: StatementFactScope) -> Self {
        Self { owners, scope }
    }
}

#[async_trait]
impl StatisticsFactPort for FrontendStatisticsFacts {
    async fn resolve_statistics(
        &self,
        needs: &[StatisticsNeed],
    ) -> Result<Vec<StatisticsFact>, String> {
        let mut facts = Vec::with_capacity(needs.len());
        for need in needs {
            facts.push(
                resolve_statistics_need_async(
                    self.owners.statistics.as_ref(),
                    self.scope.bindings.as_ref(),
                    need,
                    &self.scope.connector_context,
                )
                .await?,
            );
        }
        Ok(facts)
    }
}

/// Answers materialized-view discovery from the request-local inventory.
pub(crate) struct FrontendMaterializedViewFacts {
    owners: CompletionFactOwners,
    scope: StatementFactScope,
}

impl FrontendMaterializedViewFacts {
    pub(crate) const fn new(owners: CompletionFactOwners, scope: StatementFactScope) -> Self {
        Self { owners, scope }
    }
}

#[async_trait]
impl MaterializedViewFactPort for FrontendMaterializedViewFacts {
    /// Rewrite is an accelerator, so an inventory that cannot answer produces a
    /// fact saying the view is unavailable. There is deliberately no error
    /// path here that a working query could take: a query that would have run
    /// without the rewrite must not fail because of it.
    async fn resolve_materialized_views(
        &self,
        needs: &[MaterializedViewNeed],
    ) -> Result<Vec<MaterializedViewFact>, String> {
        let candidate = match &self.scope.state_store {
            StatementStateStore::Legacy => self.owners.materialized_views.clone(),
            StatementStateStore::Governed(Some(requests)) => self
                .owners
                .materialized_views
                .in_requests(Arc::clone(requests)),
            StatementStateStore::Governed(None) => {
                return Ok(needs
                    .iter()
                    .map(|need| {
                        MaterializedViewFact::missing(
                            need,
                            "materialized-view original execution is unavailable",
                        )
                        .expect("a static MV capability diagnostic is non-empty")
                    })
                    .collect());
            }
        };
        let mut facts = Vec::with_capacity(needs.len());
        for need in needs {
            facts.push(
                freeze_materialized_view_fact_with_ports_async(
                    need,
                    &candidate,
                    self.owners.connector_control.as_ref(),
                    self.owners.mv_storage_observation.as_ref(),
                    &self.scope.connector_context,
                )
                .await,
            );
        }
        Ok(facts)
    }
}

/// Install the same concrete Provider and fact ports in the original action's
/// fixed source before constructing the driver future. Refusal never replaces
/// an older, potentially unfinished source.
pub(crate) fn install_frontend_fact_source(
    custody: &FrontendCompletionCustody,
    owners: CompletionFactOwners,
    scope: StatementFactScope,
    session: ConnectorSession,
) -> Result<(), String> {
    let provider = Arc::new(
        FrontendProviderReadFacts::binding_only(
            Arc::clone(&owners.connector_control),
            Arc::clone(&scope.bindings),
            session,
            scope.connector_context.clone(),
            owners.blocking.clone(),
            scope.capacity.clone(),
        )
        .with_creator_mode(scope.creator_mode),
    );
    let facts = Arc::new(statement_fact_source(owners, scope, provider.clone()));
    custody.install(facts, provider).map_err(|original| {
        // No work can have been issued by this rejected new port. Destruction
        // is outside the fixed custody lock; the old source remains unchanged.
        drop(original);
        "completion Provider source is already installed".to_owned()
    })
}

/// The one fact source one statement's completion is driven with.
///
/// Assembling it in a single place is what makes "each kind of question reaches
/// exactly one owner" true of a whole statement rather than of one call site at
/// a time. Provider reads are supplied separately because, unlike the other
/// three, answering them takes a capability that belongs to the attempt.
pub(crate) fn statement_fact_source<A>(
    owners: CompletionFactOwners,
    scope: StatementFactScope,
    provider_reads: Arc<dyn ProviderReadFactPort<Access = A>>,
) -> QueryCompletionFactSource<A> {
    QueryCompletionFactSource::new(
        Arc::new(FrontendCatalogFacts::new(owners.clone(), scope.clone())),
        Arc::new(FrontendStatisticsFacts::new(owners.clone(), scope.clone())),
        Arc::new(FrontendMaterializedViewFacts::new(owners, scope)),
        provider_reads,
    )
}

#[cfg(test)]
mod tests {
    use novarocks_sql::compiler::{CatalogLookupTarget, CatalogRelationOutcome, fixtures};
    use novarocks_sql::planning::catalog::TableLookupMode;
    use novarocks_types::naming::TableIdentity;

    use super::*;

    fn a_need() -> CatalogRelationNeed {
        fixtures::catalog_relation_need(
            1,
            TableIdentity::new("iceberg", "db", "orders"),
            CatalogLookupTarget::Table {
                mode: TableLookupMode::SchemaOnly,
            },
        )
        .expect("catalog relation need")
    }

    /// A relation that is not there is a fact about the query. The compiler
    /// can act on it - a missing table is an error it raises itself, with the
    /// name it asked about.
    #[test]
    fn an_absent_relation_becomes_a_fact() {
        let need = a_need();
        let fact = catalog_fact_from(&need, Err(CatalogResolutionError::missing("no such table")))
            .expect("absence is an answer");
        assert!(matches!(
            fact.outcome(),
            CatalogRelationOutcome::Missing { .. }
        ));
        assert_eq!(fact.relation(), need.relation());
    }

    /// A catalog that could not say is a fact about this process, and it must
    /// not become the other one. Flattened into absence, a query over a
    /// healthy table would report that the table does not exist for as long as
    /// its catalog was unreachable.
    #[test]
    fn a_catalog_that_could_not_answer_fails_the_query() {
        let error = catalog_fact_from(
            &a_need(),
            Err(CatalogResolutionError::failed("REST timeout")),
        )
        .expect_err("a catalog that could not say has answered nothing");
        assert!(error.contains("REST timeout"), "{error}");
    }
}

#[cfg(test)]
mod held_port_tests;
