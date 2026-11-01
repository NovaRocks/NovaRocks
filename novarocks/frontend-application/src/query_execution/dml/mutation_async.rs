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

//! Asynchronous mutation preparation. The supervised action owns each actual
//! computation and keeps the prepared sources, original identity and session.

use super::*;
use crate::query_execution::completion_facts::FrontendCompletionCustody;
use crate::query_execution::dml::mutation::OwnedMutationStatement;
use crate::query_execution::write_session::WriteBeginReceiving;
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorRequestContext, ConnectorRowMutationPreparation,
    ConnectorRowMutationSelection, ConnectorRowMutationStrategy, ConnectorWriteOperationId,
};

type DmlError = crate::dml::error::DmlExecutionError;
type Target = crate::catalog_application::resolver::TargetBackend;

/// Pure provider-signed preparation of the exact admitted UPDATE or MERGE.
/// COW has not begun a write; its matching computation must run first.
pub(crate) enum PreparedMutationFacts {
    Update(PreparedUpdateMutation),
    Merge(PreparedMergeMutation),
}

pub(crate) async fn prepare_mutation_facts_async(
    state: &DmlExecutionKernel,
    statement: &OwnedMutationStatement,
    target: &Target,
    target_ref: &str,
    current_catalog: Option<&str>,
    planning_lease: ConnectorControlPlanningLease,
    execution: QueryExecutionContext,
    connector_context: &ConnectorRequestContext,
    operation_id: ConnectorWriteOperationId,
) -> Result<PreparedMutationFacts, crate::dml::error::DmlExecutionError> {
    let mutation = match statement {
        OwnedMutationStatement::Update(_) => {
            crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Update
        }
        OwnedMutationStatement::Merge(_) => {
            crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Merge
        }
    };
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_planning_lease_and_context_async(
        state.mv_storage_observation().as_ref(), &planning_lease,
        state.mv_readiness().as_ref(), target, mutation, connector_context.clone(),
    ).await?;
    let context =
        crate::connector::context_for_planning_lease(&planning_lease, connector_context.clone())?;
    let metadata = crate::connector::schema_creation::load_frontend_table_async(
        planning_lease.binding().metadata().as_ref(),
        novarocks_spi::connector::ConnectorTableRequest {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: planning_lease.binding().descriptor().instance_id.clone(),
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
            context,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    let binding =
        crate::connector::write_target::ConnectorWriteTargetBinding::new(metadata, planning_lease);
    // The same pure signers own strategy, base, complete target types and
    // assignment/partition admission. No second preparation or lease is minted.
    match statement {
        OwnedMutationStatement::Update(statement) => finish_update_mutation_preparation(
            statement,
            current_catalog,
            target.clone(),
            target_ref.to_owned(),
            binding,
            &execution,
            connector_context,
            operation_id,
        )
        .map(PreparedMutationFacts::Update),
        OwnedMutationStatement::Merge(statement) => finish_merge_mutation_preparation(
            statement,
            current_catalog,
            target.clone(),
            target_ref.to_owned(),
            binding,
            &execution,
            connector_context,
            operation_id,
        )
        .map(PreparedMutationFacts::Merge),
    }
    .map_err(Into::into)
}

impl PreparedMutationFacts {
    pub(crate) fn target(&self) -> &Target {
        match self {
            Self::Update(p) => &p.target,
            Self::Merge(p) => &p.target,
        }
    }
    pub(crate) fn target_ref(&self) -> &str {
        match self {
            Self::Update(p) => &p.target_ref,
            Self::Merge(p) => &p.target_ref,
        }
    }
    pub(crate) fn is_copy_on_write(&self) -> bool {
        self.strategy() == ConnectorRowMutationStrategy::CopyOnWrite
    }
    pub(crate) fn execution(&self) -> &QueryExecutionContext {
        match self {
            Self::Update(p) => &p.execution,
            Self::Merge(p) => &p.execution,
        }
    }
    pub(crate) fn context(&self) -> &ConnectorRequestContext {
        match self {
            Self::Update(p) => &p.connector_context,
            Self::Merge(p) => &p.connector_context,
        }
    }
    pub(crate) fn finalize(&self, state: &DmlExecutionKernel) -> Result<(), String> {
        crate::catalog_application::resolver::invalidate_iceberg_caches(state, self.target())
    }
    fn strategy(&self) -> ConnectorRowMutationStrategy {
        match self {
            Self::Update(p) => p.mode,
            Self::Merge(p) => p.table_write_mode,
        }
    }
    fn columns(&self) -> &[novarocks_types::schema::ColumnDef] {
        match self {
            Self::Update(p) => &p.target_columns,
            Self::Merge(p) => &p.target_columns,
        }
    }
    fn planning_lease(&self) -> &ConnectorControlPlanningLease {
        match self {
            Self::Update(p) => &p.planning_lease,
            Self::Merge(p) => &p.planning_lease,
        }
    }
    fn write_lease(&self) -> &novarocks_spi::connector::ConnectorWriteLease {
        match self {
            Self::Update(p) => &p.write_lease,
            Self::Merge(p) => &p.write_lease,
        }
    }
    fn signed(&self) -> Result<&ConnectorRowMutationPreparation, String> {
        let preparations = match self {
            Self::Update(p) => p.cow_preparations.as_ref().or(p.mor_preparations.as_ref()),
            Self::Merge(p) => p.cow_preparations.as_ref().or(p.mor_preparations.as_ref()),
        }
        .ok_or_else(|| "mutation preparation lost its original signed provider facts".to_owned())?;
        Ok(&preparations.preparation)
    }

    pub(crate) async fn prepare_mor_write_async(
        self: &Arc<Self>,
        state: &DmlExecutionKernel,
        receiving: Arc<WriteBeginReceiving>,
    ) -> Result<PreparedMorMutationWrite, DmlError> {
        if self.strategy() != ConnectorRowMutationStrategy::MergeOnRead {
            return Err("MOR preparation requires the signed merge-on-read strategy".into());
        }
        let catalog_properties = self
            .write_lease()
            .catalog_properties()
            .cloned()
            .ok_or_else(|| {
                "connector write lease has no immutable catalog runtime identity".to_owned()
            })?;
        let session = ConnectorWriteSession::begin_async_in(
            crate::connector::write_target::derive_write_stack_lease(
                state.typed_connector_control(),
                self.planning_lease(),
            )?,
            catalog_properties,
            crate::query_execution::dml::iceberg_writer::connector_write_begin_request(
                self.target(),
                self.target_ref(),
                novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
                mor_change_stream_input_request(self.columns())?,
                novarocks_spi::connector::ConnectorWriteAdmissionPurpose::OrdinaryDml,
                novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::RowMutation,
                self.context().clone(),
            )?,
            receiving.as_ref(),
        )
        .await
        .map_err(|error| format!("begin connector write session: {error}"))?;
        // Same receiving owns the accepted session before query shaping can fail.
        let (query, kind, assertion) = match self.as_ref() {
            Self::Update(p) => build_update_mor_producer(
                state,
                &p.target,
                &p.stmt,
                p.current_catalog.as_deref(),
                &p.target_columns,
                &p.target_ref,
            )?,
            Self::Merge(p) => build_merge_mor_producer(
                state,
                &p.target,
                &p.stmt,
                p.current_catalog.as_deref(),
                &p.target_columns,
                p.insert_columns_resolved.as_deref(),
                &p.target_ref,
            )?,
        };
        Ok(PreparedMorMutationWrite {
            state: state.clone(),
            facts: Arc::clone(self),
            session,
            query,
            kind,
            assertion,
            bindings: Arc::new(QueryTableBindingStore::try_new()?),
        })
    }

    pub(crate) fn prepare_cow_selection(
        self: &Arc<Self>,
        state: &DmlExecutionKernel,
    ) -> Result<PreparedCowSelection, DmlError> {
        if !self.is_copy_on_write() {
            return Err("COW selection requires its signed copy-on-write strategy".into());
        }
        if let Self::Merge(prepared) = self.as_ref()
            && matches!(
                prepared.stmt.matched.as_ref().map(|clause| &clause.action),
                Some(PreparedMergeMatchedAction::Delete)
            )
        {
            // The original stage always required its MOR target for a matched
            // delete. A disagreeing provider strategy must not enter COW.
            return Err("MOR MERGE reached stage without an admitted frozen write target".into());
        }
        let preparation = self.signed()?;
        let query = match self.as_ref() {
            Self::Update(p) => {
                let source_sql = mutation_source_to_sql(
                    state,
                    &p.stmt.source,
                    p.current_catalog.as_deref(),
                    &p.target,
                )?;
                build_exact_cow_update_selection_query(
                    &p.target,
                    &p.stmt,
                    source_sql.as_deref(),
                    preparation,
                )?
            }
            Self::Merge(p) => build_exact_cow_merge_selection_query(
                state,
                &p.target,
                &p.stmt,
                p.current_catalog.as_deref(),
                p.insert_columns_resolved.as_deref(),
                preparation,
            )?,
        };
        let (schema, _) = cow_selection_layout(preparation)?;
        let declarations = cow_selection_execution_types(preparation)?;
        Ok(PreparedCowSelection {
            state: state.clone(),
            facts: Arc::clone(self),
            query,
            schema,
            declarations,
            bindings: Arc::new(QueryTableBindingStore::try_new()?),
        })
    }

    pub(crate) async fn prepare_cow_write_async(
        self: &Arc<Self>,
        state: &DmlExecutionKernel,
        selection: ConnectorRowMutationSelection,
        selection_execution: QueryExecutionContext,
        receiving: Arc<WriteBeginReceiving>,
    ) -> Result<PreparedCowMutationWrite, DmlError> {
        if !self.is_copy_on_write() {
            return Err("COW write requires its signed copy-on-write strategy".into());
        }
        if selection.row_count() == 0 {
            return Err("empty COW selection has no write session to begin".into());
        }
        let input = prepare_cow_begin_input(
            state,
            self.target(),
            self.signed()?,
            selection,
            self.planning_lease(),
            &selection_execution,
            self.context(),
        )?;
        let session =
            crate::query_execution::write_session::begin_cow_connector_write_session_async_in(
                input.lease,
                self.write_lease(),
                input.request,
                input.original,
                receiving.as_ref(),
            )
            .await?;
        // Original receiving retains the exact checked session across every
        // subsequent construction failure. The original scope is never reset.
        let write = build_cow_update_distributed_write(
            state.constant_policy(),
            &CowMaterializedCompileControl(self.context()),
            self.target(),
            self.planning_lease().clone(),
            self.signed()?,
            Arc::clone(&session),
        )?;
        Ok(PreparedCowMutationWrite {
            state: state.clone(),
            facts: Arc::clone(self),
            write,
        })
    }
}

pub(crate) struct PreparedMorMutationWrite {
    state: DmlExecutionKernel,
    facts: Arc<PreparedMutationFacts>,
    session: Arc<ConnectorWriteSession>,
    query: novarocks_parser::ast::Query,
    kind: DmlChangeStreamKind,
    assertion: Option<DmlPreExpandKeyedAssert>,
    bindings: Arc<QueryTableBindingStore>,
}
impl PreparedMorMutationWrite {
    pub(crate) async fn compile_async(
        &self,
        provider: &FrontendCompletionCustody,
        options: Option<QueryOptions>,
    ) -> Result<crate::query_execution::compiler::PreparedDmlWriteAssembly, DmlError> {
        let (routes, statistics_targets) = bind_change_stream_routes(
            self.facts.target(),
            &self.session,
            self.facts.planning_lease(),
            self.bindings.as_ref(),
        )?;
        install_fact_source(&self.state, &self.facts, &self.bindings, provider)?;
        let mut query = self.query.clone();
        if crate::query_execution::planning::time_travel::has_time_travel_refs(&query) {
            crate::query_execution::planning::time_travel::rewrite_time_travel_refs_async(
                &self.state,
                Some(&self.facts.target().catalog),
                &self.facts.target().namespace,
                &mut query,
                self.facts.context(),
            )
            .await?;
        }
        let control = crate::query_execution::planning::sql_compile_control_from_execution(
            self.facts.execution(),
        );
        let preparation = novarocks_sql::compiler::SqlDmlCatalogPreparation::try_new_for_intent(
            novarocks_sql::compiler::SqlStatementInput::parsed_query(Box::new(query)),
            novarocks_sql::compiler::SqlSessionContext {
                sql_semantics: self.facts.execution().sql_semantics().clone(),
                // The old analyzer materializer supplied this effective catalog
                // for unqualified mutation sources; freeze that same fact here.
                current_catalog: Some(self.facts.target().catalog.clone()),
                current_database: self.facts.target().namespace.clone(),
                optimizer_settings: dml_change_stream_optimizer_settings(),
            },
            control.clone(),
            novarocks_sql::compiler::SqlCompileIntent::ChangeStreamWrite,
            None,
        )
        .map_err(DmlError::from_compile)?;
        let catalog = provider
            .resolve_catalog_relations(preparation.needs())
            .await?;
        let analyzed = preparation
            .analyze(
                &catalog,
                self.state.function_catalog().as_ref(),
                crate::query_execution::constant_eval::constant_evaluator(),
                self.state.constant_policy(),
            )
            .map_err(DmlError::from_compile)?;
        let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings_async(
            &self.state, Arc::clone(&self.bindings), self.facts.context(),
        ).await?;
        let optimize_request = novarocks_sql::compiler::SqlOptimizeRequest::new(
            analyzed,
            &statistics,
            control.clone(),
        );
        let decimal_overflow_policy = optimize_request.decimal_overflow_policy();
        let (completion, needs) = novarocks_sql::planning::dml::begin_final_dml_change_stream(
            DmlChangeStreamCompileRequest {
                optimize_request,
                kind: self.kind.clone(),
                routes,
                statistics_targets,
                pre_expand_keyed_assert: self.assertion.clone(),
                shape: novarocks_sql::planning::dml::DmlWritePlanShape::Dataflow,
            },
            decimal_overflow_policy,
        )?;
        let sink = novarocks_query_application::preparation::ReadAccessSink::new();
        let facts = provider.resolve_provider_reads(&needs, &sink).await?;
        provider
            .close()
            .await
            .map_err(|error| format!("{}: {}", error.detail(), error.cause()))?;
        let access = sink
            .try_into_access()
            .map_err(|(error, _)| error.to_string())?;
        let options = Some(options.unwrap_or_else(|| {
            crate::query_execution::contract::synthetic_statement_query_options(
                self.facts.execution(),
            )
        }));
        finish_change_stream_write(
            &self.state,
            completion,
            facts,
            access,
            &self.session,
            options,
            &control,
        )
    }
}

fn install_fact_source(
    state: &DmlExecutionKernel,
    facts: &PreparedMutationFacts,
    bindings: &Arc<QueryTableBindingStore>,
    provider: &FrontendCompletionCustody,
) -> Result<(), String> {
    let capacity = facts
        .execution()
        .result_capacity()
        .cloned()
        .ok_or_else(|| {
            "async mutation compilation requires its admitted result capacity".to_owned()
        })?;
    let scope = crate::query_execution::completion_facts::StatementFactScope::binding_only(
        Arc::clone(bindings),
        facts.context().clone(),
        Some(&facts.target().catalog),
        capacity.scope(),
        capacity.clone(),
    )
    .map_err(|error| error.to_string())?;
    crate::query_execution::completion_facts::install_frontend_fact_source(
        provider,
        state.completion_fact_owners()?,
        scope,
        crate::query_execution::compiler::typed_connector_session()?,
    )
}

pub(crate) struct PreparedCowSelection {
    state: DmlExecutionKernel,
    facts: Arc<PreparedMutationFacts>,
    query: novarocks_parser::ast::Query,
    schema: arrow::datatypes::SchemaRef,
    declarations: Vec<novarocks_type_contract::FunctionValueType>,
    bindings: Arc<QueryTableBindingStore>,
}
impl PreparedCowSelection {
    pub(crate) async fn compile_async(
        &self,
        provider: &FrontendCompletionCustody,
        options: Option<QueryOptions>,
    ) -> Result<PreparedCowMatchAssembly, DmlError> {
        install_fact_source(&self.state, &self.facts, &self.bindings, provider)?;
        let control = crate::query_execution::planning::sql_compile_control_from_execution(
            self.facts.execution(),
        );
        let preparation = novarocks_sql::compiler::SqlDmlCatalogPreparation::try_new_for_intent(
            novarocks_sql::compiler::SqlStatementInput::parsed_query(Box::new(self.query.clone())),
            novarocks_sql::compiler::SqlSessionContext {
                sql_semantics: self.facts.execution().sql_semantics().clone(),
                current_catalog: Some(self.facts.target().catalog.clone()),
                current_database: self.facts.target().namespace.clone(),
                optimizer_settings: self.facts.execution().optimizer_settings().clone(),
            },
            control.clone(),
            novarocks_sql::compiler::SqlCompileIntent::DmlInternalRead,
            Some(self.declarations.clone()),
        )
        .map_err(DmlError::from_compile)?;
        let catalog = provider
            .resolve_catalog_relations(preparation.needs())
            .await?;
        let analyzed = preparation
            .analyze(
                &catalog,
                self.state.function_catalog().as_ref(),
                crate::query_execution::constant_eval::constant_evaluator(),
                self.state.constant_policy(),
            )
            .map_err(DmlError::from_compile)?;
        let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings_async(
            &self.state, Arc::clone(&self.bindings), self.facts.context(),
        ).await?;
        let optimize = novarocks_sql::compiler::SqlOptimizeRequest::new(
            analyzed,
            &statistics,
            control.clone(),
        );
        let (completion, needs) = novarocks_sql::planning::dml::begin_final_dml_read_plan(
            optimize,
            self.facts.execution().optimizer_settings(),
        )?;
        let sink = novarocks_query_application::preparation::ReadAccessSink::new();
        let facts = provider.resolve_provider_reads(&needs, &sink).await?;
        provider
            .close()
            .await
            .map_err(|error| format!("{}: {}", error.detail(), error.cause()))?;
        let access = sink
            .try_into_access()
            .map_err(|(error, _)| error.to_string())?;
        let (description, template) = finish_cow_match_plan(
            &self.state,
            completion,
            facts,
            access,
            &self.declarations,
            options.as_ref(),
            &control,
        )?;
        let signed = self.facts.signed()?;
        Ok(PreparedCowMatchAssembly {
            description,
            template,
            seed: crate::query_execution::internal_consumer::decoded::CowMatchComputationSeed {
                context: self.facts.context().clone(),
                signed_schema: Arc::clone(&self.schema),
                declarations: self.declarations.clone(),
                contract: signed.match_contract().clone(),
                intent: signed.intent().clone(),
            },
        })
    }
}

/// A completed COW selection plan is not a selection or a write result. Only
/// the real closed consumer can produce its matching ComputationReady token.
pub(crate) struct PreparedCowMatchAssembly {
    description: novarocks_query_application::preparation::FrozenExecutionDescription,
    template: crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
    seed: crate::query_execution::internal_consumer::decoded::CowMatchComputationSeed,
}
impl PreparedCowMatchAssembly {
    pub(crate) fn prepare_computation(
        self,
        initial: crate::query_execution::logical_execution::InitialComputationReservation,
        runtime: crate::query_execution::internal_consumer::decoded::DecodedConsumerRuntime,
        options: Option<QueryOptions>,
    ) -> Result<
        (
            crate::query_execution::logical_execution::PreparedLogicalComputation,
            novarocks_query_application::api::ReservedExecutionIdentity,
            crate::query_execution::internal_consumer::decoded::CowComputationOutput,
        ),
        novarocks_query_application::api::QueryExecutionError,
    > {
        crate::query_execution::internal_consumer::decoded::prepare_cow_computation(
            self.description,
            self.template,
            Arc::new(
                crate::query_execution::contract::ResolvedQueryOptions::from_upstream(options),
            ),
            self.seed,
            initial,
            runtime,
        )
    }
}

pub(crate) struct PreparedCowMutationWrite {
    state: DmlExecutionKernel,
    facts: Arc<PreparedMutationFacts>,
    write: CowUpdateDistributedWrite,
}
impl PreparedCowMutationWrite {
    pub(crate) fn target_count(&self) -> usize {
        self.write.targets.len()
    }
    pub(crate) async fn compile_target_async(
        &self,
        index: usize,
        execution: &QueryExecutionContext,
        context: &ConnectorRequestContext,
        provider: &FrontendCompletionCustody,
        options: Option<QueryOptions>,
    ) -> Result<crate::query_execution::compiler::PreparedDmlWriteAssembly, DmlError> {
        let plan = self
            .write
            .targets
            .get(index)
            .ok_or_else(|| "COW target index is outside its sealed branch set".to_owned())?;
        let (sink, bindings, overlays) = prepare_cow_target_binding(
            self.facts.target(),
            plan,
            &self.write.planning_lease,
            &self.write.write_session,
        )?;
        let assembly =
            crate::query_execution::compiler::prepare_query_as_iceberg_write_at_write_target_async(
                &self.state,
                Some(&self.facts.target().catalog),
                &self.facts.target().namespace,
                &plan.query,
                plan.materialized_site,
                plan.materialized_rows.clone(),
                sink,
                bindings,
                options,
                novarocks_sql::compiler::RootDistributionRequirement::Any,
                execution,
                context,
                Arc::clone(&self.write.write_session),
                plan.ordinal,
                &overlays,
                provider,
            )
            .await?;
        plan.original.check_active().map_err(|error| {
            CowFailure::construction(
                super::super::cow_closed_ast::BuildError::Original(error),
                plan.original.clone(),
            )
        })?;
        Ok(assembly)
    }
}
