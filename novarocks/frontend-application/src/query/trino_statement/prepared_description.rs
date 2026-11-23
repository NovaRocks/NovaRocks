// SPDX-License-Identifier: Apache-2.0

//! Statement-local description through the original catalog and CPU owners.

use super::*;
use crate::query::read_action::{
    ReadNativeServices, close_provider, io_source::ReadMetadataIoSource,
    local_preparation_profile_with_dependencies,
};
use crate::query::session_property_computation::QuerySessionPropertyCpuInput;
use crate::query::statement_computation::statement_work_error;
use crate::query_execution::completion_facts::{
    FrontendCompletionCustody, FrontendPlanningCpuSource, StatementFactScope,
    install_frontend_fact_source,
};
use arrow::{
    array::{ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_query_application::{
    api::{
        LocalResultBound, LocalResultProducer, OwnedLocalResult, ResultField,
        StatementNativeHealthFailure,
    },
    session_control::{GovernedActionContext, GovernedStatementAction, StatementActionValue},
    sql::trino_admission::{TrinoDescriptionAction, TrinoDescriptionKind, TrinoStatementSource},
};
use novarocks_result_contract::{ClientPresentationFamily, TrinoJsonFacts};
use novarocks_spi::connector::ConnectorIoAdmissionPort;
use novarocks_sql::compiler::{
    ConstantPolicy, SqlCapturedCatalogPreparation, SqlDescribedColumn, SqlExpressionConstants,
    SqlFunctionCatalog, SqlPreparedParameters, SqlSessionContext, SqlWriteAssignments,
};
use novarocks_type_contract::{
    CharacterKind, CharacterType, FunctionValueType, ValueKind, ValueLayout, ValueTypeRef,
    ValueTypeStorageLifetime,
};
use std::task::{Context, Poll};

impl FrontendQuerySession {
    pub(super) async fn execute_trino_describe(
        &self,
        source: TrinoStatementSource,
        description: TrinoDescriptionAction,
        parameters: Option<Arc<SqlPreparedParameters>>,
        statement_started_at: std::time::SystemTime,
        statement_time: novarocks_type_contract::StatementTimeEnvironment,
    ) -> Result<QuerySessionStatement, QueryServiceError> {
        let parameters =
            parameters.ok_or_else(|| internal_error("Description has no parameter declaration"))?;
        let kind = match description.kind {
            TrinoDescriptionKind::Input => StatementKind::DescribeInput,
            TrinoDescriptionKind::Output => StatementKind::DescribeOutput,
        };
        let plan = CommandPlan::management(kind, StatementOutputShape::ResultSet, false);
        let result = match self
            .start_trino_command(
                source.sql(),
                plan,
                statement_started_at,
                statement_time,
                None,
            )
            .await?
        {
            CommandStart::Ready(entry) => {
                let CommandEntry { statement, input } = *entry;
                let action = self.build_description_action(source, description, parameters, input);
                match action {
                    Ok(action) => {
                        self.execute_observed_statement_action(statement, Box::new(action))
                            .await?
                    }
                    Err(error) => self.governed_typed_error(error, statement),
                }
            }
            CommandStart::Error(result) => *result,
        };
        Ok(QuerySessionStatement::output_owned(result, kind))
    }

    fn build_description_action(
        &self,
        source: TrinoStatementSource,
        description: TrinoDescriptionAction,
        parameters: Arc<SqlPreparedParameters>,
        input: CommandContextInput,
    ) -> Result<DescriptionAction, QueryServiceError> {
        let compiler = self.service.query_compiler.clone();
        let cpu = compiler.new_planning_cpu_source();
        let catalog = FrontendCompletionCustody::new();
        let scope = input
            .request
            .execution()
            .result_capacity()
            .ok_or_else(|| internal_error("Description request has no Local capacity"))?
            .scope()
            .clone();
        let profile =
            local_preparation_profile_with_dependencies(&[]).map_err(statement_work_error)?;
        let io = ReadMetadataIoSource::new(scope, profile.clone());
        io.configure_read_source(&profile)
            .map_err(connector_error)?;
        let job = DescriptionPreparation {
            source,
            description,
            parameters,
            input,
            compiler,
            insert_engine: self.service.insert_engine.clone(),
            delete_engine: self.service.delete_engine.clone(),
            mutation_engine: self.service.mutation_engine.clone(),
            ctas_engine: self.service.ctas_engine.clone(),
            registry: self.service.session_properties.clone(),
            session_catalogs: self.service.session_catalog_resolver.clone(),
            interpreter: self.service.required_constant_interpreter.clone(),
            original: self.state.lock().map_err(poisoned_state)?.clone(),
        };
        let future = Box::pin(prepare_description(
            job,
            catalog.clone(),
            cpu.clone(),
            io.clone(),
        ));
        Ok(DescriptionAction {
            services: ReadNativeServices::new(None, io),
            catalog,
            cpu,
            future: Some(future),
            health: None,
            ready: false,
            output: None,
            io_error: None,
        })
    }
}

struct DescriptionPreparation {
    source: TrinoStatementSource,
    description: TrinoDescriptionAction,
    parameters: Arc<SqlPreparedParameters>,
    input: CommandContextInput,
    compiler: compiler::FrontendQueryCompiler,
    insert_engine: Arc<dyn crate::query_execution::dml::insert::InsertEngine>,
    delete_engine: Arc<dyn crate::query_execution::dml::delete::DeleteEngine>,
    mutation_engine: Arc<dyn crate::query_execution::dml::mutation::MutationEngine>,
    ctas_engine: Arc<dyn crate::query_execution::dml::ctas::CtasEngine>,
    registry: Arc<novarocks_query_application::sql::session::registry::SessionPropertyRegistry>,
    session_catalogs: novarocks_query_application::sql::catalog::SessionCatalogService,
    interpreter: Arc<crate::RequiredConstantInterpreter>,
    original: SessionSqlState,
}

struct DescriptionAction {
    services: ReadNativeServices,
    catalog: Arc<FrontendCompletionCustody>,
    cpu: Arc<FrontendPlanningCpuSource>,
    future: Option<Pin<Box<dyn Future<Output = DescriptionResult> + Send>>>,
    output: Option<Result<OwnedLocalResult, QueryServiceError>>,
    io_error: Option<QueryServiceError>,
    health: Option<StatementNativeHealthFailure>,
    ready: bool,
}
type DescriptionResult =
    Result<Result<OwnedLocalResult, QueryServiceError>, StatementNativeHealthFailure>;

impl GovernedStatementAction for DescriptionAction {
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.services.io().health_failure())
            .or_else(|| self.catalog.local_health_failure())
            .or_else(|| self.cpu.local_health_failure())
    }

    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        let work = owner
            .execution_owner_slot()
            .as_ref()
            .expect("Description retains its original Root");
        if self.output.is_none() {
            match self.services.io().poll_progress(work, cx) {
                Ok(ready) => self.ready |= ready,
                Err(error) => {
                    self.io_error
                        .get_or_insert_with(|| statement_work_error(error));
                    if !self.ready {
                        drop(self.future.take());
                        self.output = self.io_error.take().map(Err);
                        if self
                            .services
                            .withdraw_unstarted_io(&mut self.health)
                            .is_err()
                        {
                            return Poll::Pending;
                        }
                    }
                }
            }
            if self.ready {
                let future = self.future.as_mut().expect("Description's original future");
                match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(output)) => {
                        drop(self.future.take());
                        self.output = Some(output);
                    }
                    Poll::Ready(Err(health)) => {
                        self.health = Some(health);
                        return Poll::Pending;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
        if self.output.is_none() {
            return Poll::Pending;
        }
        match self.services.poll_io_quiescent(cx, &mut self.health) {
            Poll::Ready(Ok(())) => {}
            Poll::Pending | Poll::Ready(Err(_)) => return Poll::Pending,
        }
        Poll::Ready(
            match self.output.take().expect("description output was ready") {
                Err(error) => Err(error),
                Ok(result) => match self.io_error.take() {
                    Some(error) => Err(error),
                    None => Ok(StatementActionValue::Local(result)),
                },
            },
        )
    }
}

async fn prepare_description(
    job: DescriptionPreparation,
    catalog: Arc<FrontendCompletionCustody>,
    cpu: Arc<FrontendPlanningCpuSource>,
    io: Arc<ReadMetadataIoSource>,
) -> DescriptionResult {
    let mut health = None;
    let result = run_description(job, &catalog, &cpu, io, &mut health).await;
    close_provider(catalog, cpu).await?;
    match health {
        Some(health) => Err(health),
        None => Ok(result),
    }
}

async fn run_description(
    job: DescriptionPreparation,
    catalog: &FrontendCompletionCustody,
    cpu: &FrontendPlanningCpuSource,
    io: Arc<ReadMetadataIoSource>,
    health: &mut Option<StatementNativeHealthFailure>,
) -> Result<OwnedLocalResult, QueryServiceError> {
    let execution = job.input.request.execution();
    let session = job.input.request.session();
    let capacity = execution
        .result_capacity()
        .ok_or_else(|| internal_error("Description lost its Local scope"))?;
    let control = crate::query_execution::planning::sql_compile_control_from_execution(execution);
    let source = job.source.sql();
    let sql_error = |error| {
        routed_execution_service_error(routed_compiler_failure(
            compiler::FrontendQueryCompilerError::from_compile(error),
            source,
        ))
    };
    let connector = job
        .input
        .command
        .connector_context()
        .clone()
        .with_io_admission(io);
    let (mut query, write, overlays) = match *job.description.target {
        TrinoStatementAction::Query(TrinoQueryAction::Read(query)) => (query, None, Vec::new()),
        TrinoStatementAction::Prepare(preparation @ TrinoStatementPreparation::Insert(_)) => {
            let result_field = preparation_plan(&preparation)?
                .admission
                .shape
                .command_row_field()
                .ok_or_else(|| internal_error("INSERT has no command row declaration"))?;
            let TrinoStatementPreparation::Insert(node) = preparation else {
                unreachable!("already matched INSERT preparation");
            };
            let target_span = node.table.span;
            let (command, reference) =
                dml_result(crate::dml::insert::prepare_trino_insert_shape(node, source))
                    .map_err(routed_execution_service_error)?;
            let prepared = job
                .insert_engine
                .describe_insert(
                    crate::query_execution::dml::insert::InsertDescriptionInput {
                        command,
                        reference,
                        request: job.input.request.clone(),
                        connector: connector.clone(),
                        control: control.clone(),
                        source: source.clone(),
                        target_span,
                    },
                )
                .await?;
            (
                prepared.query,
                Some(DescriptionWrite {
                    assignments: Some(Arc::new(prepared.assignments)),
                    constants: prepared.constants,
                    ctas_target: None,
                    result_field,
                }),
                Vec::new(),
            )
        }
        TrinoStatementAction::Prepare(preparation @ TrinoStatementPreparation::Delete(_)) => {
            let result_field = preparation_plan(&preparation)?
                .admission
                .shape
                .command_row_field()
                .ok_or_else(|| internal_error("DELETE has no command row declaration"))?;
            let TrinoStatementPreparation::Delete(node) = preparation else {
                unreachable!("already matched DELETE preparation");
            };
            let target_span = node.table.span;
            let statement = dml_result(crate::dml::prepare_trino_delete_shape(node, source))
                .map_err(routed_execution_service_error)?;
            let preparation = job
                .delete_engine
                .describe_delete(
                    crate::query_execution::dml::delete::DeleteDescriptionInput {
                        statement,
                        request: job.input.request.clone(),
                        connector: connector.clone(),
                        parameters: job.parameters.clone(),
                        target_span,
                        source: source.clone(),
                    },
                )
                .await
                .map_err(|error| error.into_dml_error(Some(source)));
            let preparation = dml_result(preparation).map_err(routed_execution_service_error)?;
            (
                preparation.query,
                Some(DescriptionWrite {
                    assignments: None,
                    constants: None,
                    ctas_target: None,
                    result_field,
                }),
                vec![preparation.overlay],
            )
        }
        TrinoStatementAction::Prepare(
            preparation @ (TrinoStatementPreparation::Update(_)
            | TrinoStatementPreparation::Merge(_)),
        ) => {
            let result_field = preparation_plan(&preparation)?
                .admission
                .shape
                .command_row_field()
                .ok_or_else(|| internal_error("Mutation has no command row declaration"))?;
            let (statement, target_span) = match preparation {
                TrinoStatementPreparation::Update(node) => {
                    let span = node.table.span;
                    (
                        crate::dml::mutation::prepare_trino_update_shape(node, source),
                        span,
                    )
                }
                TrinoStatementPreparation::Merge(node) => {
                    let span = node.target.span();
                    (
                        crate::dml::mutation::prepare_trino_merge_shape(node, source),
                        span,
                    )
                }
                _ => unreachable!("already matched UPDATE or MERGE preparation"),
            };
            let statement = dml_result(statement).map_err(routed_execution_service_error)?;
            use crate::query_execution::dml::mutation::{
                MutationDescriptionInput, OwnedMutationStatement,
            };
            let merge = matches!(statement, OwnedMutationStatement::Merge(_));
            let input = MutationDescriptionInput {
                statement,
                request: job.input.request.clone(),
                connector: connector.clone(),
                control: control.clone(),
                source: source.clone(),
                target_span,
            };
            let prepared = if merge {
                job.mutation_engine.describe_merge(input).await
            } else {
                job.mutation_engine.describe_update(input).await
            }
            .map_err(|error| error.into_dml_error(Some(source)));
            let prepared = dml_result(prepared).map_err(routed_execution_service_error)?;
            (
                prepared.query,
                Some(DescriptionWrite {
                    assignments: prepared.assignments,
                    constants: None,
                    ctas_target: None,
                    result_field,
                }),
                vec![prepared.overlay],
            )
        }
        TrinoStatementAction::Prepare(
            preparation @ TrinoStatementPreparation::CreateTableAsSelect(_),
        ) => {
            let result_field = preparation_plan(&preparation)?
                .admission
                .shape
                .command_row_field()
                .ok_or_else(|| internal_error("CTAS has no command row declaration"))?;
            let TrinoStatementPreparation::CreateTableAsSelect(node) = preparation else {
                unreachable!("already matched CTAS preparation");
            };
            let prepared = job
                .ctas_engine
                .describe_ctas(crate::query_execution::dml::ctas::CtasDescriptionInput {
                    statement: node,
                    request: job.input.request.clone(),
                    command: job.input.command.clone(),
                    connector: connector.clone(),
                    source: source.clone(),
                    control: control.clone(),
                    parameters: job.parameters.clone(),
                    compiler: job.compiler.clone(),
                    interpreter: job.interpreter.clone(),
                    cpu,
                })
                .await
                .map_err(|failure| {
                    *health = Some(failure);
                    internal_error("CTAS description preparation has no proven exit")
                })??;
            (
                prepared.query,
                Some(DescriptionWrite {
                    assignments: None,
                    constants: None,
                    ctas_target: prepared.target,
                    result_field,
                }),
                Vec::new(),
            )
        }
        _ => {
            return Err(QueryServiceError::new(
                QueryServiceErrorKind::Unsupported,
                "DESCRIBE is unavailable for this statement kind",
            ));
        }
    };
    for property in &query.session_properties {
        if let Some(span) = job.parameters.first_in_expression(&property.value) {
            return Err(sql_error(
                novarocks_sql::analyze_error::AnalyzeError::unsupported_query_shape(
                    "DESCRIBE requires a bound WITH SESSION value",
                    span,
                )
                .into(),
            ));
        }
    }
    let mut sql_session = SqlSessionContext {
        current_catalog: session.current_catalog().map(str::to_owned),
        current_database: session.current_database().to_owned(),
        sql_semantics: execution.sql_semantics().clone(),
        optimizer_settings: execution.optimizer_settings().clone(),
    };
    let mut property_inputs = execution.session_property_inputs().clone();
    if !query.session_properties.is_empty() {
        let context = job.compiler.property_cpu_context(
            sql_session.current_database.clone(),
            sql_session.sql_semantics.clone(),
            job.interpreter,
            control.clone(),
            capacity.scope().clone(),
            capacity.window_alias().clone(),
        );
        let prepared = cpu
            .query_session_properties(QuerySessionPropertyCpuInput {
                source: source.clone(),
                parameters: Some(job.parameters.clone()),
                statement: super::super::query_session_action::QueryFamily::Read(Box::new(query)),
                original: job.original,
                registry: job.registry,
                catalogs: job.session_catalogs,
                context,
            })
            .await
            .map_err(|failure| {
                *health = Some(failure);
                internal_error("Description session CPU has no proven exit")
            })??;
        let super::super::query_session_action::QueryFamily::Read(body) = prepared.statement else {
            return Err(internal_error(
                "Description session CPU changed the query family",
            ));
        };
        query = *body;
        let (catalog, database, _, optimizer, settings, properties) =
            prepared.state.into_query_attempt_inputs();
        sql_session = SqlSessionContext {
            current_catalog: catalog,
            current_database: database,
            optimizer_settings: optimizer,
            sql_semantics: match sql_session.sql_semantics.statement_time() {
                Some(time) => settings.with_statement_time(time),
                None => settings,
            },
        };
        property_inputs = Arc::new(properties);
    }
    let bindings = Arc::new(
        crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()
            .map_err(internal_error)?,
    );
    let candidates = novarocks_sql::compiler::quoted_metadata_catalogs(
        &query,
        sql_session.current_catalog.as_deref(),
        &sql_session.current_database,
        &control,
    )
    .map_err(sql_error)?;
    let qualifications = job
        .compiler
        .describe_catalog_qualifications(&bindings, &candidates)
        .map_err(sql_error)?;
    let mut preparation = SqlCapturedCatalogPreparation::try_new_description(
        query,
        &job.parameters,
        &sql_session,
        control.clone(),
        qualifications,
    )
    .map_err(sql_error)?;
    install_frontend_fact_source(
        catalog,
        job.compiler.completion_fact_owners(),
        StatementFactScope::governed_read_io_in(
            bindings,
            connector,
            sql_session.current_catalog.as_deref(),
            capacity.scope(),
            capacity.clone(),
            None,
        )
        .map_err(statement_work_error)?
        .with_snapshot_planning_owned(control.clone(), execution.cancellation().clone())
        .with_query_local_overlays(overlays),
        crate::query_execution::compiler::typed_connector_session().map_err(internal_error)?,
        property_inputs,
    )
    .map_err(internal_error)?;
    while !preparation.needs().is_empty() {
        let facts = catalog
            .resolve_catalog_relations(preparation.needs())
            .await
            .map_err(sql_error)?;
        preparation = preparation
            .continue_catalog(facts.into_boxed_slice())
            .map_err(sql_error)?;
    }
    let family = session
        .presentation()
        .copied()
        .ok_or_else(|| internal_error("Description requires client presentation"))?;
    let producer =
        LocalResultProducer::try_new(capacity.scope(), capacity.window_alias().clone(), family)
            .map_err(internal_error)?;
    cpu.describe_prepared(DescriptionCpuInput {
        source: job.source,
        kind: job.description.kind,
        parameters: job.parameters,
        preparation,
        session: sql_session,
        functions: job.compiler.description_functions(),
        policy: job.compiler.constant_policy(),
        write,
        family,
        producer,
    })
    .await
    .map_err(|failure| {
        *health = Some(failure);
        internal_error("Description CPU has no proven exit")
    })?
}

fn connector_error(cause: novarocks_spi::connector::ConnectorError) -> QueryServiceError {
    QueryServiceError::from_failure_cause(novarocks_failure_contract::SharedFailureCause::new(
        cause,
    ))
}

pub(crate) struct DescriptionCpuInput {
    source: TrinoStatementSource,
    kind: TrinoDescriptionKind,
    parameters: Arc<SqlPreparedParameters>,
    preparation: SqlCapturedCatalogPreparation,
    session: SqlSessionContext,
    functions: Arc<dyn SqlFunctionCatalog>,
    policy: ConstantPolicy,
    write: Option<DescriptionWrite>,
    family: ClientPresentationFamily,
    producer: LocalResultProducer,
}

struct DescriptionWrite {
    assignments: Option<Arc<SqlWriteAssignments>>,
    constants: Option<SqlExpressionConstants>,
    ctas_target: Option<crate::query::ctas_action::trino::CtasDescriptionTarget>,
    result_field: ResultField,
}

impl DescriptionCpuInput {
    pub(crate) fn run(self) -> Result<OwnedLocalResult, QueryServiceError> {
        let mut write = self.write;
        let constants = write.as_mut().and_then(|write| write.constants.take());
        let ctas = write.as_mut().and_then(|write| write.ctas_target.take());
        let described = match ctas.as_ref() {
            Some(_) => self.preparation.describe_ctas_source(
                &self.parameters,
                &self.session,
                self.functions.as_ref(),
                self.policy,
            ),
            None => self.preparation.describe(
                &self.parameters,
                &self.session,
                self.functions.as_ref(),
                self.policy,
                write
                    .as_ref()
                    .and_then(|write| write.assignments.as_deref()),
                constants,
            ),
        };
        let mut described = described.map_err(|error| {
            routed_execution_service_error(routed_compiler_failure(
                compiler::FrontendQueryCompilerError::from_compile(error),
                self.source.sql(),
            ))
        })?;
        if let Some(target) = ctas {
            target.validate_source(&described.columns)?;
        }
        if let Some(write) = write {
            described.columns = vec![SqlDescribedColumn {
                name: write.result_field.name().to_owned(),
                origin: None,
                value_type: write.result_field.value_type().clone(),
                aliased: false,
            }];
        }
        let ClientPresentationFamily::TrinoJson(facts) = self.family;
        self.producer
            .produce(|| match self.kind {
                TrinoDescriptionKind::Input => input_result(described.parameters, facts),
                TrinoDescriptionKind::Output => output_result(described.columns, facts),
            })
            .map_err(internal_error)
    }
}

fn type_text(value: &FunctionValueType, facts: TrinoJsonFacts) -> Result<String, String> {
    let field = novarocks_sql::compiler::render_field_for_declaration(value)?;
    novarocks_trino_adapter::described_type_name(&field.native_type, facts)
}

fn input_result(
    parameters: Vec<FunctionValueType>,
    facts: TrinoJsonFacts,
) -> Result<QueryResult, String> {
    let rows = parameters.len();
    let mut retained = DescriptionBound::new(rows, 2)?;
    let mut types = Vec::with_capacity(rows);
    for parameter in parameters {
        let text = type_text(&parameter, facts)?;
        retained.add_text(&text)?;
        types.push(text);
    }
    let display = text_field("Type", &types)?;
    let (position, array) =
        numeric_column("Position", (0..rows).map(|ordinal| ordinal as i32), rows);
    local_result(
        vec![position, display],
        vec![array, Arc::new(StringArray::from(types))],
    )
}

fn output_result(
    columns: Vec<SqlDescribedColumn>,
    facts: TrinoJsonFacts,
) -> Result<QueryResult, String> {
    let rows = columns.len();
    let mut retained = DescriptionBound::new(rows, 7)?;
    let mut text: [Vec<String>; 5] = std::array::from_fn(|_| Vec::with_capacity(rows));
    let mut sizes = Vec::with_capacity(rows);
    let mut aliases = Vec::with_capacity(rows);
    for field in columns {
        let ty = type_text(&field.value_type, facts)?;
        sizes.push(field.trino_type_size());
        aliases.push(field.aliased);
        let (catalog, schema, table) = field.origin.map_or_else(
            || (String::new(), String::new(), String::new()),
            |origin| (origin.catalog, origin.namespace, origin.table),
        );
        for (column, value) in text
            .iter_mut()
            .zip([field.name, catalog, schema, table, ty])
        {
            retained.add_text(&value)?;
            column.push(value);
        }
    }
    let mut fields = Vec::with_capacity(7);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(7);
    for (name, values) in ["Column Name", "Catalog", "Schema", "Table", "Type"]
        .into_iter()
        .zip(text)
    {
        fields.push(text_field(name, &values)?);
        arrays.push(Arc::new(StringArray::from(values)));
    }
    let (size_type, size) = numeric_column("Type Size", sizes.into_iter(), rows);
    fields.push(size_type);
    arrays.push(size);
    fields.push(ResultField::new(
        "Aliased",
        FunctionValueType::new(DataType::Boolean, false),
    ));
    arrays.push(Arc::new(BooleanArray::from(aliases)));
    local_result(fields, arrays)
}

struct DescriptionBound {
    rows: usize,
    bytes: usize,
}
impl DescriptionBound {
    fn new(rows: usize, columns: usize) -> Result<Self, String> {
        let bytes = rows
            .saturating_mul(columns)
            .saturating_mul(LocalResultBound::cell_bytes(size_of::<String>()));
        LocalResultBound::V1.admit(rows, bytes)?;
        Ok(Self { rows, bytes })
    }
    fn add_text(&mut self, text: &str) -> Result<(), String> {
        // The source string and its eventual Arrow copy share the original window.
        self.bytes = self.bytes.saturating_add(text.len().saturating_mul(2));
        LocalResultBound::V1.admit(self.rows, self.bytes)
    }
}

fn text_field(name: &'static str, strings: &[String]) -> Result<ResultField, String> {
    let Some(width) = strings.iter().map(|text| text.chars().count()).max() else {
        return Ok(ResultField::new(
            name,
            FunctionValueType::new(DataType::Utf8, false),
        ));
    };
    let width = u32::try_from(width).map_err(|error| error.to_string())?;
    let character = CharacterType::new(CharacterKind::Varchar, width)
        .ok_or("Description text exceeds VARCHAR width")?;
    let storage = ValueTypeStorageLifetime::pure();
    let value = ValueTypeRef::try_new(
        ValueKind::BoundedCharacter(character),
        false,
        storage.clone(),
    )
    .map_err(|error| error.to_string())?;
    let layout = ValueLayout::try_from_normalized_arrow(DataType::Utf8, storage)
        .map_err(|error| error.to_string())?;
    let ty = FunctionValueType::try_from_value(value, layout).map_err(|error| error.to_string())?;
    Ok(ResultField::new(name, ty))
}

fn numeric_column(
    name: &'static str,
    values: impl Iterator<Item = i32>,
    rows: usize,
) -> (ResultField, ArrayRef) {
    if rows == 0 {
        (
            ResultField::new(name, FunctionValueType::new(DataType::Int64, false)),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        )
    } else {
        (
            ResultField::new(name, FunctionValueType::new(DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(values)),
        )
    }
}

fn local_result(columns: Vec<ResultField>, arrays: Vec<ArrayRef>) -> Result<QueryResult, String> {
    LocalResultBound::V1.check_arrays(&arrays)?;
    let schema = Schema::new(
        columns
            .iter()
            .map(|field| Field::new(field.name(), field.data_type().clone(), field.nullable()))
            .collect::<Vec<_>>(),
    );
    let batch =
        RecordBatch::try_new(Arc::new(schema), arrays).map_err(|error| error.to_string())?;
    Ok(QueryResult {
        columns,
        batches: vec![batch],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_result_contract::NativeRenderType;

    #[test]
    fn prepared_statement_metadata_uses_final_text_and_exact_empty_shape() {
        let empty = input_result(Vec::new(), TrinoJsonFacts::default()).unwrap();
        assert_eq!(empty.columns[0].data_type(), &DataType::Int64);
        assert_eq!(empty.row_count(), 0);
        let name = "é😀";
        let output = output_result(
            vec![SqlDescribedColumn {
                name: name.to_owned(),
                origin: None,
                value_type: FunctionValueType::new(DataType::Int64, false),
                aliased: true,
            }],
            TrinoJsonFacts::default(),
        )
        .unwrap();
        assert_eq!(output.row_count(), 1);
        let render = |ordinal: usize| {
            novarocks_sql::compiler::render_field_for_declaration(
                output.columns[ordinal].value_type(),
            )
            .unwrap()
            .native_type
        };
        assert_eq!(
            render(0),
            NativeRenderType::BoundedCharacter(
                CharacterType::new(CharacterKind::Varchar, 2).unwrap()
            )
        );
        assert_eq!(
            render(1),
            NativeRenderType::BoundedCharacter(
                CharacterType::new(CharacterKind::Varchar, 0).unwrap()
            )
        );
        assert_eq!(output.columns[5].data_type(), &DataType::Int32);
    }
}
