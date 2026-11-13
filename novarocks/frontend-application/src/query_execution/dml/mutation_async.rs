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
use crate::query_execution::dml::mutation_match_read::{
    bind_mutation_match_read, mutation_match_select,
};
use crate::query_execution::write_session::WriteBeginReceiving;
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorRequestContext, ConnectorRowMutationPreparation,
    ConnectorRowMutationStrategy, ConnectorWriteOperationId,
};

type DmlError = crate::dml::error::DmlExecutionError;
type Target = crate::catalog_application::resolver::TargetBackend;

/// Pure provider-signed preparation of the exact admitted UPDATE or MERGE.
/// Its original signed facts remain stable through asynchronous preparation.
pub(crate) enum PreparedMutationFacts {
    Update(Box<PreparedUpdateMutation>),
    Merge(Box<PreparedMergeMutation>),
}

pub(crate) async fn prepare_mutation_facts_async(
    state: &DmlExecutionKernel,
    statement: &OwnedMutationStatement,
    target: &Target,
    target_ref: &str,
    current_catalog: Option<&str>,
    planning_pin: ConnectorControlPlanningPin,
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
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_planning_pin_and_context_async(
        state.mv_storage_observation().as_ref(), &planning_pin,
        state.mv_readiness().as_ref(), target, mutation, connector_context.clone(),
    ).await?;
    let context =
        crate::connector::context_for_planning_pin(&planning_pin, connector_context.clone())?;
    let metadata = crate::connector::schema_creation::load_frontend_table_async(
        planning_pin.binding().metadata().as_ref(),
        novarocks_spi::connector::ConnectorTableRequest {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: planning_pin.binding().descriptor().instance_id.clone(),
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
        crate::connector::write_target::ConnectorWriteTargetBinding::new(metadata, planning_pin);
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
        .map(Box::new)
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
        .map(Box::new)
        .map(PreparedMutationFacts::Merge),
    }
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
    fn planning_pin(&self) -> &ConnectorControlPlanningPin {
        match self {
            Self::Update(p) => &p.planning_pin,
            Self::Merge(p) => &p.planning_pin,
        }
    }
    fn write(&self) -> &novarocks_spi::connector::ConnectorWrite {
        match self {
            Self::Update(p) => &p.write,
            Self::Merge(p) => &p.write,
        }
    }
    fn signed(&self) -> Result<&ConnectorRowMutationPreparation, String> {
        let preparations = match self {
            Self::Update(p) => p.mor_preparations.as_ref(),
            Self::Merge(p) => p.mor_preparations.as_ref(),
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
        let catalog_properties = self.write().catalog_properties().cloned().ok_or_else(|| {
            "connector write lease has no immutable catalog runtime identity".to_owned()
        })?;
        let session = ConnectorWriteSession::begin_async_in(
            crate::connector::write_target::derive_write_stack_lease(
                state.typed_connector_control(),
                self.planning_pin(),
            )?,
            catalog_properties,
            crate::query_execution::dml::iceberg_writer::connector_write_begin_request_on_base(
                self.target(),
                self.target_ref(),
                novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
                mor_change_stream_input_request(self.columns())?,
                novarocks_spi::connector::ConnectorWriteAdmissionPurpose::OrdinaryDml,
                Some(self.signed()?.base_version().clone()),
                novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::RowMutation,
                self.context().clone(),
            )?,
            receiving.as_ref(),
        )
        .await
        .map_err(|error| format!("begin connector write session: {error}"))?;
        // Same receiving owns the accepted session before query shaping can fail.
        let (mut query, kind, assertion) = match self.as_ref() {
            Self::Update(p) => build_update_mor_producer(
                &p.target,
                &p.stmt,
                p.current_catalog.as_deref(),
                &p.target_columns,
                &p.target_ref,
            )?,
            Self::Merge(p) => build_merge_mor_producer(
                &p.target,
                &p.stmt,
                p.current_catalog.as_deref(),
                &p.target_columns,
                p.insert_columns_resolved.as_deref(),
                &p.target_ref,
            )?,
        };
        let from = mutation_match_select(&mut query)?;
        let (target_factor, preparations) = match self.as_ref() {
            Self::Update(p) => (&mut from.relation, p.mor_preparations.as_ref()),
            Self::Merge(p) => {
                let [join] = from.joins.as_mut_slice() else {
                    return Err("generated MERGE lost its match target join".into());
                };
                (&mut join.relation, p.mor_preparations.as_ref())
            }
        };
        let preparations = preparations.ok_or("MOR lost its original match preparation")?;
        let original = preparations
            .match_observation
            .as_ref()
            .ok_or("MOR lost its original match observation")?;
        let match_read = bind_mutation_match_read(
            self.target(),
            target_factor,
            true,
            &preparations.preparation,
            original,
            self.planning_pin(),
        )?;
        Ok(PreparedMorMutationWrite {
            state: state.clone(),
            facts: Arc::clone(self),
            session,
            query,
            kind,
            assertion,
            bindings: Arc::new(QueryTableBindingStore::try_new()?),
            match_read,
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
    match_read: crate::catalog_application::query_materializer::QueryLocalTableOverlay,
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
            self.facts.planning_pin(),
            self.bindings.as_ref(),
        )?;
        compile_routed_mutation_write(
            RoutedMutationWrite {
                state: &self.state,
                target: self.facts.target(),
                execution: self.facts.execution(),
                context: self.facts.context(),
                bindings: &self.bindings,
                match_read: &self.match_read,
                session: &self.session,
                query: &self.query,
                kind: self.kind.clone(),
                assertion: self.assertion.clone(),
                routes,
                statistics_targets,
            },
            provider,
            options,
        )
        .await
    }
}

pub(crate) struct RoutedMutationWrite<'a> {
    pub state: &'a DmlExecutionKernel,
    pub target: &'a crate::catalog_application::resolver::TargetBackend,
    pub execution: &'a QueryExecutionContext,
    pub context: &'a novarocks_spi::connector::ConnectorRequestContext,
    pub bindings: &'a Arc<QueryTableBindingStore>,
    pub match_read: &'a crate::catalog_application::query_materializer::QueryLocalTableOverlay,
    pub session: &'a Arc<ConnectorWriteSession>,
    pub query: &'a novarocks_parser::ast::Query,
    pub kind: DmlChangeStreamKind,
    pub assertion: Option<DmlPreExpandKeyedAssert>,
    pub routes: Vec<novarocks_sql::planning::dml::DmlChangeStreamRoute>,
    pub statistics_targets: Vec<novarocks_sql::planning::dml::DmlChangeStreamStatisticsTarget>,
}

pub(crate) async fn compile_routed_mutation_write(
    write: RoutedMutationWrite<'_>,
    provider: &FrontendCompletionCustody,
    options: Option<QueryOptions>,
) -> Result<crate::query_execution::compiler::PreparedDmlWriteAssembly, DmlError> {
    let RoutedMutationWrite {
        state,
        target,
        execution,
        context,
        bindings,
        match_read,
        session,
        query,
        kind,
        assertion,
        routes,
        statistics_targets,
    } = write;
    install_dml_fact_source(
        state,
        execution,
        target,
        context,
        bindings,
        std::slice::from_ref(match_read),
        provider,
    )?;
    let mut query = query.clone();
    if crate::query_execution::planning::time_travel::has_time_travel_refs(&query) {
        crate::query_execution::planning::time_travel::rewrite_time_travel_refs_async(
            state,
            Some(&target.catalog),
            &target.namespace,
            &mut query,
            context,
        )
        .await?;
    }
    let control = crate::query_execution::planning::sql_compile_control_from_execution(execution);
    let mut preparation = novarocks_sql::compiler::SqlDmlCatalogPreparation::try_new_for_intent(
        novarocks_sql::compiler::SqlStatementInput::parsed_query(Box::new(query)),
        novarocks_sql::compiler::SqlSessionContext {
            sql_semantics: execution.sql_semantics().clone(),
            // The old analyzer materializer supplied this effective catalog
            // for unqualified mutation sources; freeze that same fact here.
            current_catalog: Some(target.catalog.clone()),
            current_database: target.namespace.clone(),
            optimizer_settings: dml_change_stream_optimizer_settings(),
        },
        control.clone(),
        novarocks_sql::compiler::SqlCompileIntent::ChangeStreamWrite,
        None,
    )
    .map_err(DmlError::from_compile)?;
    while !preparation.needs().is_empty() {
        let facts = provider
            .resolve_catalog_relations(preparation.needs())
            .await
            .map_err(DmlError::from_compile)?;
        preparation = preparation
            .continue_catalog(facts.into_boxed_slice())
            .map_err(DmlError::from_compile)?;
    }
    let analyzed = preparation
        .analyze(
            &[],
            state.function_catalog().as_ref(),
            crate::query_execution::constant_eval::constant_evaluator(),
            state.constant_policy(),
        )
        .map_err(DmlError::from_compile)?;
    let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings_async(
        state, Arc::clone(bindings), context,
    ).await?;
    let optimize_request =
        novarocks_sql::compiler::SqlOptimizeRequest::new(analyzed, &statistics, control.clone());
    let decimal_overflow_policy = optimize_request.decimal_overflow_policy();
    let (completion, needs) = novarocks_sql::planning::dml::begin_final_dml_change_stream(
        DmlChangeStreamCompileRequest {
            optimize_request,
            kind,
            routes,
            statistics_targets,
            pre_expand_keyed_assert: assertion,
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
        crate::query_execution::contract::synthetic_statement_query_options(execution)
    }));
    finish_change_stream_write(
        state,
        completion,
        facts,
        access,
        session,
        options,
        &control,
        crate::query_execution::physical_encoding::encoding_capacity(execution)?,
    )
}

fn install_dml_fact_source(
    state: &DmlExecutionKernel,
    execution: &QueryExecutionContext,
    target: &crate::catalog_application::resolver::TargetBackend,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    bindings: &Arc<QueryTableBindingStore>,
    overlays: &[crate::catalog_application::query_materializer::QueryLocalTableOverlay],
    provider: &FrontendCompletionCustody,
) -> Result<(), String> {
    let capacity = execution.result_capacity().cloned().ok_or_else(|| {
        "async mutation compilation requires its admitted result capacity".to_owned()
    })?;
    let scope = crate::query_execution::completion_facts::StatementFactScope::binding_only(
        Arc::clone(bindings),
        context.clone(),
        Some(&target.catalog),
        capacity.scope(),
        capacity.clone(),
    )
    .map_err(|error| error.to_string())?
    .with_query_local_overlays(overlays.to_vec())
    .with_system_table_source_policy(
        crate::query_execution::completion_facts::SystemTableSourcePolicy::Mutation,
    );
    crate::query_execution::completion_facts::install_frontend_fact_source(
        provider,
        state.completion_fact_owners()?,
        scope.with_snapshot_planning_owned(
            crate::query_execution::planning::sql_compile_control_from_execution(execution),
            execution.cancellation().clone(),
            None,
        ),
        crate::query_execution::compiler::typed_connector_session()?,
    )
}
