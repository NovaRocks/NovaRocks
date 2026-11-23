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
    bind_mutation_match_read, load_row_mutation_target, mutation_match_select,
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
    let binding = load_row_mutation_target(target, planning_pin, connector_context).await?;
    crate::mv::domain::iceberg_guard::reject_from_admitted_iceberg_table(
        binding.planning_pin(),
        state.mv_readiness().as_ref(),
        target,
        mutation,
        binding.metadata(),
        connector_context.clone(),
    )?;
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
                mor_change_stream_input_request(self.columns(), self.signed()?.match_contract())?,
                novarocks_spi::connector::ConnectorWriteAdmissionPurpose::OrdinaryDml,
                Some(self.signed()?.base_version().clone()),
                novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::RowMutation {
                    admission: self.signed()?.admission(),
                },
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
            Self::Update(p) => {
                let target = match from {
                    novarocks_parser::ast::Relation::Join(join) => join.left.as_mut(),
                    relation => relation,
                };
                (target, p.mor_preparations.as_ref())
            }
            Self::Merge(p) => {
                let novarocks_parser::ast::Relation::Join(join) = from else {
                    return Err("generated MERGE lost its match target join".into());
                };
                (join.right.as_mut(), p.mor_preparations.as_ref())
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
        let bindings = Arc::new(QueryTableBindingStore::try_new()?);
        bindings.register_row_mutation_current(
            original,
            self.planning_pin(),
            &preparations.preparation,
        )?;
        Ok(PreparedMorMutationWrite {
            state: state.clone(),
            facts: Arc::clone(self),
            session,
            query,
            kind,
            assertion,
            bindings,
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
    let assignments = mutation_write_assignments(&query, &kind, &routes, &control)
        .map_err(DmlError::from_compile)?;
    let qualifications = crate::query_execution::compiler::observe_dml_catalog_qualifications(
        &query,
        Some(target.catalog.as_str()),
        &target.namespace,
        &control,
        bindings.as_ref(),
        state.catalog_application().map(Arc::as_ref),
    )
    .map_err(DmlError::from_compile)?;
    let mut preparation =
        novarocks_sql::compiler::SqlDmlCatalogPreparation::try_new_for_intent_with_qualifications(
            execution.sql_statement_input(Box::new(query), false),
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
            qualifications,
        )
        .map_err(DmlError::from_compile)?;
    if let Some(assignments) = assignments {
        preparation = preparation
            .with_write_assignments(assignments)
            .map_err(DmlError::from_compile)?;
    }
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
            Arc::clone(state.sealed_functions()),
            state.constant_policy(),
        )
        .map_err(DmlError::from_compile)?;
    let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings_async(
        state, Arc::clone(bindings), context,
    ).await?;
    let match_binding = bindings
        .row_mutation_match_binding()?
        .ok_or("row mutation lost its original target binding")?;
    let optimize_request =
        novarocks_sql::compiler::SqlOptimizeRequest::new(analyzed, &statistics, control.clone())
            .with_row_mutation_match(Some(match_binding));
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
        ),
        crate::query_execution::compiler::typed_connector_session()?,
        Arc::clone(execution.session_property_inputs()),
    )
}

enum MutationWriteTypeError {
    Source(novarocks_types::ColumnValueTypeError),
    Control(novarocks_type_contract::CompileControlError),
}
impl From<novarocks_types::ColumnValueTypeError> for MutationWriteTypeError {
    fn from(error: novarocks_types::ColumnValueTypeError) -> Self {
        Self::Source(error)
    }
}
impl From<novarocks_type_contract::ValueTypeError> for MutationWriteTypeError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Source(error.into())
    }
}
impl MutationWriteTypeError {
    fn into_compile(self) -> novarocks_sql::compiler::SqlCompileError {
        use novarocks_sql::compiler::SqlCompileError;
        use novarocks_type_contract::ValueTypeError;
        match self {
            Self::Control(error) => error.into(),
            Self::Source(novarocks_types::ColumnValueTypeError::Type(
                ValueTypeError::TooDeep | ValueTypeError::TooManyNodes,
            )) => SqlCompileError::ResourceExhausted,
            Self::Source(error) => SqlCompileError::Compilation(error.to_string()),
        }
    }
}

/// Bind generated match-query occurrences to the original routed declarations.
/// The ordinal formulas belong to the UPDATE/MERGE query constructors above;
/// names only identify which already-resolved target column an alias denotes.
fn mutation_write_assignments(
    query: &ast::Query,
    kind: &DmlChangeStreamKind,
    routes: &[DmlChangeStreamRoute],
    control: &novarocks_sql::compiler::SqlCompileControl,
) -> Result<
    Option<Arc<novarocks_sql::compiler::SqlWriteAssignments>>,
    novarocks_sql::compiler::SqlCompileError,
> {
    use novarocks_sql::compiler::{SqlCompileError, SqlWriteAssignmentSite, SqlWriteAssignments};
    use novarocks_type_contract::PureCompileControl;
    let (columns, old_start, wildcard) = match kind {
        DmlChangeStreamKind::Delete { .. } => return Ok(None),
        DmlChangeStreamKind::Update { target_columns, .. } => (target_columns, 6, true),
        DmlChangeStreamKind::Merge {
            matched_update: false,
            not_matched_insert: false,
            ..
        } => return Ok(None),
        DmlChangeStreamKind::Merge { target_columns, .. } => (target_columns, 7, false),
    };
    let invalid = |detail: &str| SqlCompileError::InvalidRequest(detail.into());
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(invalid("generated mutation lost its exact SELECT source"));
    };
    let declaration = |column: &novarocks_types::schema::ColumnDef| -> Result<_, SqlCompileError> {
        let mut declared = None;
        for route in routes {
            control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 1)?;
            for (name, input, storage) in route.sink.write_field_declarations() {
                if name != column.name {
                    continue;
                }
                let input = input
                    .declared_value_type_observed::<MutationWriteTypeError>(|| {
                        control
                            .checkpoint(novarocks_type_contract::CompilePhase::Validate, 1)
                            .map_err(MutationWriteTypeError::Control)
                    })
                    .map_err(MutationWriteTypeError::into_compile)?;
                let next = (input, storage.cloned());
                if declared.as_ref().is_some_and(|previous| previous != &next) {
                    return Err(invalid(
                        "mutation target has ambiguous routed write declarations",
                    ));
                }
                declared = Some(next);
            }
        }
        declared.ok_or_else(|| {
            invalid("mutation target is absent from its original routed write declarations")
        })
    };
    let mut sites = Vec::new();
    for (ordinal, column) in columns.iter().enumerate() {
        let (input, storage) = declaration(column)?;
        sites.push(SqlWriteAssignmentSite::try_new(
            old_start + ordinal,
            input,
            storage,
            control,
        )?);
    }
    let first_assignment = if wildcard {
        7
    } else {
        old_start + columns.len()
    };
    if wildcard {
        let valid = if let Some(ast::SelectItem::AllColumns(columns)) = select.projection.get(6) {
            if columns.aliases.is_empty() {
                if let Some(target) = &columns.target {
                    ast::identifier_chain_observed(target, |_| {
                        control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 1)
                    })?
                    .is_some()
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };
        if !valid {
            return Err(invalid(
                "generated UPDATE lost its target wildcard occurrence",
            ));
        }
    }
    for (index, item) in select.projection.iter().enumerate().skip(first_assignment) {
        let ast::SelectItem::SingleColumn(single_column) = item else {
            return Err(invalid("mutation assignment lost its exact output alias"));
        };
        let Some(alias) = &single_column.alias else {
            return Err(invalid("mutation assignment lost its exact output alias"));
        };
        let expr = &single_column.expression;

        let name = alias
            .value
            .strip_prefix("__nr_new_")
            .or_else(|| alias.value.strip_prefix("__nr_ins_"))
            .ok_or_else(|| invalid("mutation assignment has an unknown generated output kind"))?;
        let column = columns
            .iter()
            .find(|column| column.name == name)
            .ok_or_else(|| {
                invalid("mutation assignment alias differs from its resolved target column")
            })?;
        let ordinal = if wildcard {
            6 + columns.len() + index - first_assignment
        } else {
            index
        };
        let (input, storage) = declaration(column)?;
        sites.push(
            SqlWriteAssignmentSite::try_new(ordinal, input, storage, control)?
                .with_source_span(expr.span()),
        );
    }
    Ok(Some(Arc::new(SqlWriteAssignments::try_new(
        sites.into_boxed_slice(),
        control,
    )?)))
}

#[cfg(test)]
mod typed_assignment_tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_type_contract::FunctionValueType;
    fn route(width: i32) -> DmlChangeStreamRoute {
        use novarocks_spi::connector::write_stack::{
            ConnectorWriteColocation, ConnectorWriteLayoutPreference,
            ConnectorWriteRouteRequirements, WriteTargetOrdinal,
        };
        use novarocks_sql::planning::dml::{
            ConnectorWriteInputBinding, DmlWritePlanInput, DmlWriteSinkMode, DmlWriteTarget,
            DmlWriteTargetField,
        };
        let column = novarocks_types::schema::ColumnDef::from_value_type(
            "v".into(),
            FunctionValueType::new(DataType::Binary, false),
            None,
        )
        .unwrap();
        let binding = novarocks_sql::binding::SqlTableBindingAllocator::new_unique()
            .unwrap()
            .allocate()
            .unwrap();
        let target = DmlWriteTarget {
            binding,
            catalog: "ice".into(),
            namespace: "db".into(),
            table: "t".into(),
            fields: vec![DmlWriteTargetField {
                token: novarocks_spi::connector::ConnectorWriteFieldToken::from_bytes([1; 32]),
                column: column.clone(),
                storage_type: Some(FunctionValueType::new(
                    DataType::FixedSizeBinary(width),
                    false,
                )),
                is_hidden: false,
            }],
        };
        let sink = DmlWritePlanInput::try_new(
            DmlWriteSinkMode::Data,
            target,
            vec![column],
            ConnectorWriteInputBinding::RootOutputByOrdinal,
        )
        .unwrap();
        DmlChangeStreamRoute {
            route_id: novarocks_spi::connector::ConnectorWriteRouteId::from_bytes([1; 32]),
            write_target_ordinal: WriteTargetOrdinal::try_new(1).unwrap(),
            accepted_effects: vec![novarocks_spi::connector::ConnectorRowMutationEffect::Replace],
            input_ordinals: Vec::new(),
            requirements: ConnectorWriteRouteRequirements {
                colocation: ConnectorWriteColocation::None,
                layout: ConnectorWriteLayoutPreference::None,
            },
            sink,
        }
    }
    fn query(sql: &str) -> ast::Query {
        let query = novarocks_parser::parser::parse_query(sql).unwrap();
        query.clone()
    }
    fn source_columns() -> Vec<novarocks_types::schema::ColumnDef> {
        vec![
            novarocks_types::schema::ColumnDef::from_value_type(
                "v".into(),
                FunctionValueType::new(DataType::FixedSizeBinary(4), false),
                None,
            )
            .unwrap(),
        ]
    }
    #[test]
    fn update_generated_occurrences_retain_original_storage_and_user_span() {
        let query = query("SELECT 1, 2, 3, 4, 5, 6, t.*, NULL AS __nr_new_v FROM t");
        let assignments = mutation_write_assignments(
            &query,
            &DmlChangeStreamKind::Update {
                file_metadata: Some([4, 5]),
                target_columns: source_columns(),
            },
            &[route(4)],
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            assignments
                .sites()
                .iter()
                .map(|site| site.output_ordinal())
                .collect::<Vec<_>>(),
            [6, 7]
        );
        assert_eq!(
            assignments.sites()[0].input().data_type(),
            &DataType::Binary
        );
        assert_eq!(
            assignments.sites()[0].storage().unwrap().data_type(),
            &DataType::FixedSizeBinary(4)
        );
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("SELECT");
        };
        let ast::SelectItem::SingleColumn(single_column) = &select.projection[7] else {
            panic!("assignment");
        };
        let Some(_) = &single_column.alias else {
            panic!("assignment");
        };
        let expr = &single_column.expression;

        assert_eq!(assignments.sites()[1].source_span(), Some(expr.span()));
    }
    #[test]
    fn merge_delete_only_has_no_data_projection_and_ambiguous_data_declarations_reject() {
        let query = query("SELECT 1, 2, 3, 4, 5, 6, 0, NULL AS v, NULL AS __nr_new_v FROM t");
        let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
        let delete = DmlChangeStreamKind::Merge {
            file_metadata: Some([4, 5]),
            target_columns: source_columns(),
            matched_update: false,
            matched_delete: true,
            not_matched_insert: false,
        };
        assert!(
            mutation_write_assignments(&query, &delete, &[], &control)
                .unwrap()
                .is_none()
        );
        let update = DmlChangeStreamKind::Merge {
            file_metadata: Some([4, 5]),
            target_columns: source_columns(),
            matched_update: true,
            matched_delete: false,
            not_matched_insert: false,
        };
        assert!(
            mutation_write_assignments(&query, &update, &[route(4), route(8)], &control).is_err()
        );
    }
}
