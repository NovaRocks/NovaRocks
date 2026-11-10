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
mod system_source;
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
    pub(crate) cpu: &'a Arc<FrontendPlanningCpuSource>,
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
    CatalogRelationFact, CatalogRelationNeed, MaterializedViewFact, MaterializedViewNeed,
    StatisticsFact, StatisticsNeed,
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
    view_service: Arc<dyn novarocks_query_application::view::ViewService>,
    system_relations: Arc<novarocks_query_application::system_tables::SystemTableRegistry>,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<ConnectorControlHost>,
    statistics: Arc<UnifiedStatisticsResolver>,
    materialized_views: MvCandidateReader,
    mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    state_store: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    blocking: ConnectorBlockingIoSupervisor,
}

impl CompletionFactOwners {
    pub(crate) fn with_state_store_original(
        mut self,
        binding: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    ) -> Self {
        self.state_store = binding;
        self
    }
    pub(crate) fn system_relations(
        &self,
    ) -> &Arc<novarocks_query_application::system_tables::SystemTableRegistry> {
        &self.system_relations
    }
    pub(crate) const fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<ConnectorControlHost>,
        statistics: Arc<UnifiedStatisticsResolver>,
        materialized_views: MvCandidateReader,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        blocking: ConnectorBlockingIoSupervisor,
        view_service: Arc<dyn novarocks_query_application::view::ViewService>,
        system_relations: Arc<novarocks_query_application::system_tables::SystemTableRegistry>,
        mv_readiness: Arc<crate::mv::domain::readiness::MvReadinessPort>,
    ) -> Self {
        Self {
            view_service,
            system_relations,
            catalog_service,
            catalog_application,
            connector_control,
            statistics,
            materialized_views,
            mv_readiness,
            state_store: None,
            mv_storage_observation,
            blocking,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SystemTableSourcePolicy {
    Query,
    Mutation,
}

/// What one statement adds to those owners.
///
/// The binding store is the statement's own: every fact about a relation is
/// answered against the bindings that statement was admitted under, never
/// against whatever the catalog holds at the moment the question is asked.
#[derive(Clone)]
pub(crate) struct StatementFactScope {
    state_store: StatementStateStore,
    system_table_sources: SystemTableSourcePolicy,
    creator_mode: ProviderCreatorMode,
    bindings: Arc<QueryTableBindingStore>,
    connector_context: ConnectorRequestContext,
    current_catalog: Option<Arc<str>>,
    capacity: QueryResultCapacityBinding,
    planning: Option<StatementPlanningInputs>,
    query_local_overlays:
        Vec<crate::catalog_application::query_materializer::QueryLocalTableOverlay>,
}

#[derive(Clone)]
struct StatementPlanningInputs {
    cancellation: novarocks_query_application::cancellation::QueryCancellationView,
    batch_target_rows: usize,
    control: novarocks_sql::compiler::SqlCompileControl,
    cpu: Option<Arc<FrontendPlanningCpuSource>>,
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
            system_table_sources: SystemTableSourcePolicy::Query,
            bindings,
            connector_context,
            current_catalog: current_catalog.map(Arc::from),
            capacity,
            planning: None,
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
    pub(crate) fn with_snapshot_planning_owned(
        mut self,
        control: novarocks_sql::compiler::SqlCompileControl,
        cancellation: novarocks_query_application::cancellation::QueryCancellationView,
        options: Option<&novarocks_proto_codec::lifecycle::QueryOptions>,
    ) -> Self {
        self.planning = Some(StatementPlanningInputs {
            cancellation,
            batch_target_rows: snapshot_batch_target(options),
            control,
            cpu: None,
        });
        self
    }
    pub(crate) fn with_snapshot_planning(
        mut self,
        control: novarocks_sql::compiler::SqlCompileControl,
        cancellation: novarocks_query_application::cancellation::QueryCancellationView,
        options: Option<&novarocks_proto_codec::lifecycle::QueryOptions>,
        cpu: Arc<FrontendPlanningCpuSource>,
    ) -> Self {
        self.planning = Some(StatementPlanningInputs {
            cancellation,
            batch_target_rows: snapshot_batch_target(options),
            control,
            cpu: Some(cpu),
        });
        self
    }
    pub(crate) fn with_system_table_source_policy(
        mut self,
        policy: SystemTableSourcePolicy,
    ) -> Self {
        self.system_table_sources = policy;
        self
    }
    pub(crate) fn with_query_local_overlays(
        mut self,
        overlays: Vec<crate::catalog_application::query_materializer::QueryLocalTableOverlay>,
    ) -> Self {
        self.query_local_overlays = overlays;
        self
    }
}

fn snapshot_batch_target(
    options: Option<&novarocks_proto_codec::lifecycle::QueryOptions>,
) -> usize {
    let resolved =
        crate::query_execution::contract::ResolvedQueryOptions::from_upstream(options.cloned());
    novarocks_execution::runtime::query_options::chunk_size(Some(resolved.runtime_options()))
}

/// Answers relation lookups from the frontend catalog.
pub(crate) struct FrontendCatalogFacts {
    local_views: Result<
        novarocks_query_application::view::LocalViewSnapshot,
        novarocks_sql::compiler::SqlCompileError,
    >,
    owners: CompletionFactOwners,
    scope: StatementFactScope,
    // One original fact source serializes its own observation rounds. This
    // does not allocate another binding identity or provider cache.
    resolving: tokio::sync::Mutex<()>,
}

impl FrontendCatalogFacts {
    pub(crate) fn new(owners: CompletionFactOwners, scope: StatementFactScope) -> Self {
        let local_views = scope
            .bindings
            .capture_local_views(|| owners.view_service.catalog_snapshot());
        Self {
            local_views,
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
    ) -> Result<Vec<CatalogRelationFact>, novarocks_sql::compiler::SqlCompileError> {
        let _round = self.resolving.lock().await;
        if self.scope.system_table_sources == SystemTableSourcePolicy::Mutation
            && needs.iter().any(|need| {
                need.target() == novarocks_sql::compiler::CatalogLookupTarget::SystemTable
            })
        {
            return Err(novarocks_sql::compiler::SqlCompileError::CompilationSource(
                novarocks_failure_contract::SharedFailureCause::new(
                    novarocks_spi::connector::ConnectorError::new(
                        novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                        "system tables are unsupported in DELETE, UPDATE and MERGE sources",
                    ),
                ),
            ));
        }

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
        .with_catalog_application(self.owners.catalog_application.as_deref())
        .with_local_views(self.local_views.clone()?)
        .with_system_relations(self.owners.system_relations().clone());
        let mut facts = Vec::with_capacity(needs.len());
        for need in needs {
            // Local views and generated overlays retain their precedence.
            if let Some(fact) = materializer.local_view_fact(need) {
                facts.push(fact?);
                continue;
            }
            facts.push(materializer.resolve_relation_fact_async(need).await?);
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
pub(crate) fn catalog_fact_from(
    need: &CatalogRelationNeed,
    resolved: CatalogResolutionResult<novarocks_sql::planning::catalog::ResolvedAnalyzerTable>,
) -> Result<CatalogRelationFact, novarocks_sql::compiler::SqlCompileError> {
    match resolved {
        Ok(table) => CatalogRelationFact::resolved(need, table),
        Err(CatalogResolutionError::Missing { reason }) => {
            CatalogRelationFact::missing(need, reason)
        }
        Err(error @ CatalogResolutionError::Failed { .. }) => {
            return Err(error.into_compile());
        }
    }
    .map_err(|error| {
        novarocks_sql::compiler::SqlCompileError::Compilation(format!(
            "build catalog completion fact: {error}"
        ))
    })
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
    let mut scope = scope;
    let owned_catalog_cpu = scope.planning.as_mut().and_then(|input| {
        if input.cpu.is_some() {
            return None;
        }
        let cpu = FrontendPlanningCpuSource::new(owners.blocking.clone());
        input.cpu = Some(Arc::clone(&cpu));
        Some(cpu)
    });
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
    let system = system_source::FrontendSystemTableFacts::new(owners.clone(), scope.clone())
        .map_err(|error| error.to_string())?;
    let facts = Arc::new(statement_fact_source(
        owners,
        scope,
        provider.clone(),
        system.clone(),
    ));
    custody
        .install(facts, provider, owned_catalog_cpu, system)
        .map_err(|original| {
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
    system_tables: Arc<dyn novarocks_query_application::preparation::SystemTableSnapshotFactPort>,
) -> QueryCompletionFactSource<A> {
    QueryCompletionFactSource::new(
        Arc::new(FrontendCatalogFacts::new(owners.clone(), scope.clone())),
        Arc::new(FrontendStatisticsFacts::new(owners.clone(), scope.clone())),
        Arc::new(FrontendMaterializedViewFacts::new(owners, scope)),
        provider_reads,
        system_tables,
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
        assert!(error.to_string().contains("REST timeout"), "{error}");
    }

    #[test]
    fn a_provider_source_is_not_an_absent_relation_or_a_string_failure() {
        use novarocks_type_contract::{
            CompileControlError, owned_resources::layout::LayoutResourceError,
        };
        let original = CompileControlError::from_domain(LayoutResourceError::SourceModel);
        let error = catalog_fact_from(
            &a_need(),
            Err(CatalogResolutionError::from_connector(
                novarocks_spi::connector::ConnectorError::compile_interruption(original.clone()),
            )),
        )
        .expect_err("the original provider refusal is not a catalog fact");
        assert_eq!(
            error,
            novarocks_sql::compiler::SqlCompileError::from(original)
        );
        let novarocks_sql::compiler::SqlCompileError::CompilationSource(source) = error else {
            panic!("original domain source");
        };
        assert_eq!(
            source.downcast_ref::<LayoutResourceError>(),
            Some(&LayoutResourceError::SourceModel)
        );
    }
}

#[cfg(test)]
mod held_port_tests;
