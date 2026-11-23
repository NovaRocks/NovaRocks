// SPDX-License-Identifier: Apache-2.0

//! Temporary typed statement entrance for Trino compatibility tests.

use super::*;
use crate::query_execution::dml::truncate::{TruncateCommand, TruncateTarget};
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    session::{QuerySessionStatement, StatementAdmission, StatementOutputShape},
    session_control::{GovernedQueryStatementOwner, StatementCompletionProjection},
    sql::trino_admission::{
        TrinoQueryAction, TrinoStatementAction, TrinoStatementPreparation, admit_trino_statement,
    },
    statement_kind::StatementKind,
};

struct CommandPlan {
    admission: StatementAdmission,
    work_class: WorkClass,
    window_class: ResultWindowClass,
    waits_for_topology: bool,
}

impl CommandPlan {
    fn management(kind: StatementKind, shape: StatementOutputShape) -> Self {
        Self {
            admission: StatementAdmission { kind, shape },
            work_class: WorkClass::Management,
            window_class: ResultWindowClass::Local,
            waits_for_topology: false,
        }
    }

    fn write(kind: StatementKind, waits_for_topology: bool) -> Self {
        Self {
            admission: StatementAdmission {
                kind,
                shape: StatementOutputShape::Rows,
            },
            work_class: WorkClass::Query,
            window_class: ResultWindowClass::Internal,
            waits_for_topology,
        }
    }
}

struct CommandContextInput {
    request: RequestContext,
    command: CommandContext,
    statement_started_at: std::time::SystemTime,
    options: QueryOptions,
    cancellation: QueryCancellationView,
    deadline: Option<Instant>,
    timeout_duration: Option<Duration>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}

struct CommandEntry {
    statement: GovernedQueryStatementOwner,
    input: CommandContextInput,
}

enum CommandStart {
    Ready(Box<CommandEntry>),
    Error(Box<StatementResult>),
}

impl FrontendQuerySession {
    pub(crate) async fn execute_trino_one_statement(
        &self,
        source: Arc<str>,
    ) -> Result<QuerySessionStatement, QueryServiceError> {
        let (statement_started_at, statement_time) = self
            .service
            .statement_clock
            .capture()
            .map_err(internal_error)?;
        let statement = novarocks_query_application::sql::parse_single_statement(&source)
            .map_err(|error| QueryServiceError::from_user_error(error.to_user_error(&source)))?;
        match admit_trino_statement(&source, statement)? {
            TrinoStatementAction::Query(action) => {
                let kind = match &action {
                    TrinoQueryAction::Read(_) => StatementKind::Query,
                    TrinoQueryAction::Explain(_) | TrinoQueryAction::ExplainAnalyze(_) => {
                        StatementKind::Explain
                    }
                };
                let result = self
                    .execute_trino_query_family(source, action, statement_time)
                    .await?;
                Ok(QuerySessionStatement::output_owned(result, kind))
            }
            TrinoStatementAction::Session(action) => {
                self.execute_trino_session_action(source, action, statement_time)
                    .await
            }
            TrinoStatementAction::Product(command) => {
                let plan = product_plan(&command)?;
                let kind = plan.admission.kind;
                let result = match self
                    .start_trino_command(&source, plan, statement_started_at, statement_time)
                    .await?
                {
                    CommandStart::Ready(entry) => {
                        self.execute_trino_product(command, *entry).await?
                    }
                    CommandStart::Error(result) => *result,
                };
                Ok(QuerySessionStatement::output_owned(result, kind))
            }
            TrinoStatementAction::Prepare(preparation) => {
                let plan = preparation_plan(&preparation)?;
                let kind = plan.admission.kind;
                let result = match self
                    .start_trino_command(&source, plan, statement_started_at, statement_time)
                    .await?
                {
                    CommandStart::Ready(entry) => {
                        self.execute_trino_preparation(source, preparation, *entry)
                            .await?
                    }
                    CommandStart::Error(result) => *result,
                };
                Ok(QuerySessionStatement::output_owned(result, kind))
            }
        }
    }

    async fn start_trino_command(
        &self,
        source: &Arc<str>,
        plan: CommandPlan,
        statement_started_at: std::time::SystemTime,
        statement_time: novarocks_type_contract::StatementTimeEnvironment,
    ) -> Result<CommandStart, QueryServiceError> {
        self.admission.publish(plan.admission);
        *self.output_shape.lock().map_err(poisoned_state)? = Some(plan.admission.shape);
        let (state, deadline) = {
            let state = self.state.lock().map_err(poisoned_state)?;
            let (session_deadline, _) = governed_query_deadline(&state)?;
            (state.clone(), session_deadline)
        };
        let timeout_duration =
            deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
        let timeout_ms = timeout_duration.map(timeout_message_millis);
        let token = self.token()?;
        let statement = if plan.work_class == WorkClass::Query {
            self.service
                .query_control
                .begin_queued_governed_query_statement_with_result(
                    token,
                    &self.service.workload_root_admission,
                    deadline.map(tokio::time::Instant::from_std),
                    timeout_ms,
                    Some(source.clone()),
                    plan.window_class,
                )
                .await
        } else {
            self.service
                .query_control
                .begin_governed_statement_with_result(
                    token,
                    &self.service.workload_root_admission,
                    plan.work_class,
                    deadline.map(tokio::time::Instant::from_std),
                    timeout_ms,
                    Some(source.clone()),
                    plan.window_class,
                )
        }
        .map_err(|error| self.governed_statement_begin_error(error))?;
        #[cfg(novarocks_dev_controls)]
        let observation = match self
            .service
            .statement_observation
            .as_ref()
            .map(|owner| owner.bind_statement(statement.token(), source))
            .transpose()
        {
            Ok(handle) => handle.flatten(),
            Err(error) => {
                return Ok(CommandStart::Error(Box::new(
                    self.governed_typed_error(internal_error(error), statement),
                )));
            }
        };
        let cancellation = QueryCancellationView::governed(
            statement.cancellation().clone(),
            statement.timeout_ms(),
        );
        let topology = self
            .service
            .topology
            .snapshot()
            .map_err(|error| internal_error(error.to_string()));
        let topology = match topology {
            Ok(snapshot) if plan.waits_for_topology && snapshot.targets().is_empty() => {
                wait_for_eligible_query_topology_after(
                    &self.service.topology,
                    &cancellation,
                    snapshot.revision(),
                    deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(30)),
                )
                .await
            }
            other => other,
        };
        let topology = match topology {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Ok(CommandStart::Error(Box::new(
                    self.governed_typed_error(error, statement),
                )));
            }
        };
        let presentation = state.presentation().copied();
        let statement_time = state.statement_time(statement_time);
        let (catalog, database, settings, mut optimizer, semantics, properties) =
            state.into_query_attempt_inputs();
        if optimizer.optimizer_query_mem_limit_bytes.is_none() {
            optimizer.optimizer_query_mem_limit_bytes =
                Some(self.service.optimizer_query_mem_limit_bytes as f64);
        }
        let options =
            with_query_sql_semantics(query_options_from_session_settings(&settings), &semantics);
        let entered = (|| {
            let window = statement
                .result_window_alias()
                .ok_or_else(|| internal_error("typed statement has no admitted result capacity"))?;
            let capacity = QueryResultCapacityBinding::try_new(statement.scope(), window.clone())
                .map_err(|error| internal_error(error.to_string()))?;
            let request = RequestContext::admit(
                RequestAdmission::new(
                    catalog,
                    database,
                    self.service.role,
                    topology,
                    deadline,
                    cancellation.clone(),
                    optimizer,
                    semantics.with_statement_time(statement_time),
                    presentation,
                )
                .with_session_property_inputs(properties),
            )
            .with_result_capacity(capacity)
            .map_err(|error| internal_error(error.to_string()))?;
            let connector = crate::connector::connector_request_context_for_execution_on_runtime(
                Some(&options),
                request.execution(),
                self.service.query_compiler.connector_runtime(),
            )
            .map_err(|error| internal_error(error.to_string()))?;
            #[cfg(novarocks_dev_controls)]
            let request = match observation.as_ref().and_then(|handle| handle.nomination()) {
                Some(nomination) => {
                    let installed =
                        connector.request_scope_extension_or_insert_with(|| nomination.clone());
                    if installed.as_ref() != &nomination {
                        return Err(internal_error(
                            "typed statement nomination conflicts with its request scope",
                        ));
                    }
                    request
                        .with_test_statement_nomination(Arc::new(nomination))
                        .map_err(internal_error)?
                }
                None => request,
            };
            let command = CommandContext::new(
                statement.scope().clone(),
                connector,
                statement.token(),
                self.principal.clone(),
            )
            .with_result_window(window)
            .map_err(|error| internal_error(error.to_string()))?;
            Ok(CommandContextInput {
                request,
                command,
                statement_started_at,
                options,
                cancellation,
                deadline,
                timeout_duration,
                #[cfg(novarocks_dev_controls)]
                observation,
            })
        })();
        match entered {
            Ok(input) => Ok(CommandStart::Ready(Box::new(CommandEntry {
                statement,
                input,
            }))),
            Err(error) => Ok(CommandStart::Error(Box::new(
                self.governed_typed_error(error, statement),
            ))),
        }
    }
}

fn preparation_plan(
    preparation: &TrinoStatementPreparation,
) -> Result<CommandPlan, QueryServiceError> {
    use StatementKind::*;
    use StatementOutputShape::{Empty, ResultSet, UndeterminedCall};
    use TrinoStatementPreparation as Prep;
    use novarocks_query_application::sql::table_procedure::{
        TableProcedureKind as Procedure, declared_table_procedure_kind,
    };

    let management = |kind| CommandPlan::management(kind, Empty);
    let result = match preparation {
        Prep::CreateCatalog(_) => management(CreateCatalog),
        Prep::CreateSchema(_) => management(CreateDatabase),
        Prep::DropSchemaCascade(_) => management(DropDatabase),
        Prep::RenameSchema(_) => management(CreateDatabase),
        Prep::CreateTable(_) => management(CreateTable),
        Prep::CreateTableAsSelect(_) => CommandPlan::write(CreateTable, true),
        Prep::RenameTable(_) => management(CreateTable),
        Prep::AddColumn(_) => management(AddColumn),
        Prep::RenameColumn(_) => management(RenameColumn),
        Prep::DropColumn(_) => management(DropColumn),
        Prep::SetColumnType(_) => management(AlterColumnType),
        Prep::DropNotNullConstraint(_) => management(DropColumnNotNull),
        Prep::SetTableProperties { .. } => management(SetTableProperties),
        Prep::CommentTable(_) => management(CommentTable),
        Prep::CommentColumn(_) => management(CommentColumn),
        Prep::TableExecute(node) => match declared_table_procedure_kind(&node.procedure_name) {
            Some(Procedure::Optimize) => CommandPlan {
                admission: StatementAdmission {
                    kind: Optimize,
                    shape: ResultSet,
                },
                work_class: WorkClass::Query,
                window_class: ResultWindowClass::Internal,
                waits_for_topology: false,
            },
            Some(Procedure::OptimizeManifests) => {
                CommandPlan::management(RewriteManifests, ResultSet)
            }
            Some(Procedure::ExpireSnapshots) => CommandPlan::management(ExpireSnapshots, ResultSet),
            Some(Procedure::RemoveOrphanFiles) => {
                CommandPlan::management(RemoveOrphanFiles, ResultSet)
            }
            Some(Procedure::AddFiles) => CommandPlan::management(AddFiles, ResultSet),
            Some(Procedure::MoveColumn) => management(ReorderColumn),
            Some(Procedure::SetColumnNotNull) => management(SetColumnNotNull),
            Some(Procedure::UnsetExtraProperties) => management(UnsetTableProperties),
            Some(Procedure::CreateTag) => management(CreateTag),
            Some(Procedure::DropTag) => management(DropTag),
            #[cfg(novarocks_dev_controls)]
            Some(Procedure::AddEqualityDelete) => CommandPlan {
                admission: StatementAdmission {
                    kind: AddEqualityDelete,
                    shape: Empty,
                },
                ..CommandPlan::write(AddEqualityDelete, true)
            },
            None => CommandPlan::management(Call, UndeterminedCall),
        },
        Prep::Analyze(_) => CommandPlan::write(Analyze, false),
        Prep::CreateBranch(_) => management(CreateBranch),
        Prep::DropBranch(_) => management(DropBranch),
        Prep::FastForwardBranch(_) | Prep::ShowBranches(_) => management(Call),
        Prep::Insert(_) => CommandPlan::write(Insert, true),
        Prep::Delete(_) => CommandPlan::write(Delete, true),
        Prep::DeleteEntireBranch { .. } => {
            CommandPlan::management(Delete, StatementOutputShape::Rows)
        }
        Prep::Update(_) => CommandPlan::write(Update, true),
        Prep::Merge(_) => CommandPlan::write(Merge, true),
        Prep::Call(_) => CommandPlan::management(Call, UndeterminedCall),
        Prep::CreateView(_) => management(CreateView),
        Prep::DropView(_) => management(DropView),
        Prep::ShowCreateView(_) => CommandPlan::management(ShowCreateView, ResultSet),
        Prep::ShowCreateMaterializedView(_) => {
            CommandPlan::management(ShowCreateMaterializedView, ResultSet)
        }
        Prep::CreateMaterializedView(_) => management(CreateMaterializedView),
        Prep::DropMaterializedView(_) => management(DropMaterializedView),
        Prep::RefreshMaterializedView(_) => CommandPlan {
            admission: StatementAdmission {
                kind: RefreshMaterializedView,
                shape: StatementOutputShape::Rows,
            },
            work_class: WorkClass::Management,
            window_class: ResultWindowClass::Internal,
            waits_for_topology: false,
        },
        Prep::ExplainRefreshMaterializedView(..) => {
            CommandPlan::management(ExplainMaterializedViewRefresh, ResultSet)
        }
        Prep::SetMaterializedViewProperties { properties, .. } => {
            let mut plan = management(AlterMaterializedViewProperties);
            if properties
                .iter()
                .any(|property| property.name.value.eq_ignore_ascii_case("partitioning"))
            {
                plan.window_class = ResultWindowClass::Internal;
            }
            plan
        }
    };
    Ok(result)
}

fn product_plan(command: &ProductSqlCommand) -> Result<CommandPlan, QueryServiceError> {
    use StatementKind::*;
    use StatementOutputShape::{Empty, ResultSet};
    use novarocks_sql::semantic::{CatalogSqlCommand, StatisticsSqlCommand};
    match command {
        ProductSqlCommand::Catalog(command) => {
            let plan = match command.as_ref() {
                CatalogSqlCommand::DropCatalog { .. } => {
                    CommandPlan::management(DropCatalog, Empty)
                }
                CatalogSqlCommand::DropDatabase { .. } => {
                    CommandPlan::management(DropDatabase, Empty)
                }
                CatalogSqlCommand::DropTable { .. } => CommandPlan::management(DropTable, Empty),
                CatalogSqlCommand::TruncateTable { .. } => {
                    CommandPlan::management(TruncateTable, Empty)
                }
                CatalogSqlCommand::ShowCreateTable { .. } => {
                    CommandPlan::management(ShowCreateTable, ResultSet)
                }
                _ => {
                    return Err(internal_error(
                        "Product admission returned an unclassified catalog command",
                    ));
                }
            };
            Ok(plan)
        }
        ProductSqlCommand::Statistics(StatisticsSqlCommand::ShowStats { .. }) => {
            Ok(CommandPlan::management(ShowStats, ResultSet))
        }
        _ => Err(internal_error(
            "Product admission returned an unclassified command",
        )),
    }
}

impl FrontendQuerySession {
    async fn execute_trino_product(
        &self,
        command: ProductSqlCommand,
        entry: CommandEntry,
    ) -> Result<StatementResult, QueryServiceError> {
        use novarocks_sql::semantic::CatalogSqlCommand;
        let CommandEntry {
            mut statement,
            input,
        } = entry;
        let router = self.service.product_command_router.clone();
        let command = match command {
            ProductSqlCommand::Catalog(command) => match *command {
                command @ (CatalogSqlCommand::CreateCatalog(_)
                | CatalogSqlCommand::DropCatalog { .. }) => {
                    let action = router
                        .prepare_catalog_statement_action(command, input.request, input.command)
                        .map_err(product_command_service_error);
                    return self.run_trino_action(statement, action).await;
                }
                CatalogSqlCommand::TruncateTable {
                    table,
                    target_ref: _,
                } => {
                    return self
                        .execute_trino_truncate(
                            TruncateCommand {
                                target_parts: table.parts,
                                target: TruncateTarget::Main,
                            },
                            None,
                            CommandEntry { statement, input },
                        )
                        .await;
                }
                command => ProductSqlCommand::Catalog(Box::new(command)),
            },
            ProductSqlCommand::Statistics(command) => {
                let action = router
                    .prepare_statistics_statement_action(command, input.request, input.command)
                    .map_err(product_command_service_error);
                return self.run_trino_action(statement, action).await;
            }
        };
        let execution_owner = statement
            .take_execution_owner()
            .expect("SQL command has one execution owner");
        let mut worker = Box::pin(execute_product_statement(
            router,
            command,
            input.request,
            input.command,
            execution_owner,
        )) as TypedCommandWorker;
        self.finish_command_worker(
            statement,
            input.cancellation,
            input.timeout_duration,
            &mut worker,
        )
        .await
    }

    async fn run_trino_action(
        &self,
        statement: GovernedQueryStatementOwner,
        action: Result<Box<dyn GovernedStatementAction>, QueryServiceError>,
    ) -> Result<StatementResult, QueryServiceError> {
        match action {
            Ok(action) => {
                self.execute_observed_statement_action(statement, action)
                    .await
            }
            Err(error) => Ok(self.governed_typed_error(error, statement)),
        }
    }
}

impl FrontendQuerySession {
    async fn execute_trino_truncate(
        &self,
        command: TruncateCommand,
        count_projection: Option<StatementCompletionProjection>,
        entry: CommandEntry,
    ) -> Result<StatementResult, QueryServiceError> {
        let CommandEntry {
            mut statement,
            input,
        } = entry;
        let execution_owner = statement
            .take_execution_owner()
            .expect("SQL command has one execution owner");
        let main_truncate = matches!(&command.target, TruncateTarget::Main);
        let dml = Arc::clone(&self.service.dml);
        let engine = Arc::clone(&self.service.truncate_engine);
        let prepare_dml = Arc::clone(&dml);
        let prepare_engine = Arc::clone(&engine);
        let mut worker = Box::pin(execute_prepared_dml_statement(
            self.service.query_blocking_executor.clone(),
            input.cancellation.clone(),
            execution_owner,
            input.command.result_window_alias(),
            move || {
                prepare_dml
                    .prepare_truncate(
                        prepare_engine.as_ref(),
                        command,
                        &input.request,
                        Some(&input.options),
                    )
                    .map_err(|error| RoutedExecutionError::Engine(error.to_string()))
            },
            move |prepared| {
                let result = dml.execute_prepared_truncate(engine.as_ref(), prepared);
                if main_truncate {
                    dml_result(result)
                } else {
                    result.map_err(|error| RoutedExecutionError::Engine(error.to_string()))
                }
                .map(|()| StatementResult::Ok)
            },
        )) as TypedCommandWorker;
        self.finish_command_worker_with_unknown_count(
            statement,
            input.cancellation,
            input.timeout_duration,
            &mut worker,
            count_projection,
        )
        .await
    }
}

impl FrontendQuerySession {
    async fn execute_trino_preparation(
        &self,
        source: Arc<str>,
        preparation: TrinoStatementPreparation,
        entry: CommandEntry,
    ) -> Result<StatementResult, QueryServiceError> {
        use super::{
            mv_property_action::TrinoMvProperties, table_create_action::TablePropertyInput,
        };
        use TrinoStatementPreparation as Prep;
        use novarocks_query_application::sql::{
            lower_object_name, trino_admission::unsupported_statement, trino_table_command as table,
        };
        use novarocks_sql::semantic::CatalogSqlCommand;

        let CommandEntry { statement, input } = entry;
        if let Err(error) =
            novarocks_query_application::sql::trino_admission::admit_persisted_definition_semantics(
                &source,
                &preparation,
                input.request.execution().sql_semantics(),
            )
        {
            return Ok(self.governed_typed_error(error, statement));
        }
        let catalog = |value| ProductSqlCommand::Catalog(Box::new(value));
        let explain_refresh = matches!(&preparation, Prep::ExplainRefreshMaterializedView(..));
        let command = match preparation {
            Prep::CreateCatalog(node) => {
                return self
                    .execute_observed_trino_catalog_create(
                        source,
                        node,
                        input.request,
                        input.command,
                        input.deadline,
                        statement,
                    )
                    .await;
            }
            Prep::CreateSchema(node) => {
                let action = super::schema_create_action::prepare(
                    &self.service.query_compiler,
                    self.service.required_constant_interpreter.clone(),
                    &self.service.product_command_router,
                    super::schema_create_action::SchemaCreateInput {
                        statement: node,
                        source,
                        context: input.request,
                        command: input.command,
                        deadline: input.deadline,
                        cancellation: input.cancellation,
                    },
                );
                return self.run_trino_action(statement, action).await;
            }
            Prep::DropSchemaCascade(node) => Ok(catalog(CatalogSqlCommand::DropDatabase {
                name: lower_object_name(&node.schema_name),
                if_exists: node.exists,
                force: true,
            })),
            Prep::RenameSchema(node) => Err(unsupported_statement(
                &source,
                node.span,
                "ALTER SCHEMA RENAME",
            )),
            Prep::CreateTable(node) => {
                return self
                    .execute_observed_table_properties(
                        source,
                        TablePropertyInput::TrinoCreate(node),
                        input.request,
                        input.command,
                        input.deadline,
                        statement,
                    )
                    .await;
            }
            Prep::SetTableProperties {
                name, properties, ..
            } => {
                return self
                    .execute_observed_table_properties(
                        source,
                        TablePropertyInput::TrinoSet { name, properties },
                        input.request,
                        input.command,
                        input.deadline,
                        statement,
                    )
                    .await;
            }
            Prep::CreateTableAsSelect(node) => {
                let action = self.service.ctas_engine.prepare_trino_statement_action(
                    crate::query_execution::dml::ctas::TrinoCtasInput {
                        statement: node,
                        execution: super::ctas_action::CtasExecutionInput {
                            sql: source,
                            context: input.request,
                            query_options: input.options,
                            launcher: self.statement_logical_execution_launcher(),
                            #[cfg(novarocks_dev_controls)]
                            observation: input.observation,
                        },
                        command: input.command,
                        deadline: input.deadline,
                        cancellation: input.cancellation,
                        compiler: self.service.query_compiler.clone(),
                        interpreter: self.service.required_constant_interpreter.clone(),
                    },
                );
                return self.run_trino_action(statement, action).await;
            }
            Prep::RenameTable(node) => table::lower_trino_rename_table(&source, &node).map(catalog),
            Prep::AddColumn(node) => table::lower_trino_add_column(
                &source,
                &node,
                &novarocks_sql::compiler::SqlCompileControl::new(
                    input.deadline,
                    crate::query_execution::planning::sql_cancellation_observation(
                        input.cancellation.clone(),
                    ),
                ),
            )
            .map(catalog),
            Prep::RenameColumn(node) => {
                table::lower_trino_rename_column(&source, &node).map(catalog)
            }
            Prep::DropColumn(node) => table::lower_trino_drop_column(&source, &node).map(catalog),
            Prep::SetColumnType(node) => table::lower_trino_set_column_type(
                &source,
                &node,
                &novarocks_sql::compiler::SqlCompileControl::new(
                    input.deadline,
                    crate::query_execution::planning::sql_cancellation_observation(
                        input.cancellation.clone(),
                    ),
                ),
            )
            .map(catalog),
            Prep::DropNotNullConstraint(node) => {
                table::lower_trino_drop_not_null(&source, &node).map(catalog)
            }
            Prep::CommentTable(node) | Prep::CommentColumn(node) => {
                table::lower_trino_table_comment(&source, &node).map(catalog)
            }
            Prep::TableExecute(node) => {
                return self
                    .execute_trino_table_procedure(source, node, CommandEntry { statement, input })
                    .await;
            }
            Prep::Analyze(node) => {
                return self
                    .execute_trino_analyze(source, node, CommandEntry { statement, input })
                    .await;
            }
            Prep::CreateBranch(node) => {
                return self
                    .execute_trino_create_branch(source, node, CommandEntry { statement, input })
                    .await;
            }
            Prep::DropBranch(node) => {
                let mut command = table::lower_trino_drop_branch(&node);
                let CatalogSqlCommand::AlterIcebergTable(reference) = &mut command else {
                    unreachable!("DROP BRANCH produces its existing reference action");
                };
                match novarocks_types::naming::resolve_catalog_table_name(
                    &reference.table.parts,
                    input.request.session().current_catalog(),
                    input.request.session().current_database(),
                ) {
                    Ok(target) => {
                        reference.table.parts =
                            vec![target.catalog, target.namespace, target.table];
                        Ok(catalog(command))
                    }
                    Err(error) => Err(internal_error(error)),
                }
            }
            Prep::FastForwardBranch(node) => {
                table::lower_trino_fast_forward_branch(&source, &node).map(catalog)
            }
            Prep::ShowBranches(node) => {
                table::lower_trino_show_branches(&source, &node).map(catalog)
            }
            Prep::Insert(node) => {
                return self
                    .execute_trino_write(
                        source,
                        TrinoWrite::Insert(node),
                        CommandEntry { statement, input },
                    )
                    .await;
            }
            Prep::DeleteEntireBranch { table, branch } => {
                let projection = self
                    .state
                    .lock()
                    .map_err(poisoned_state)?
                    .presentation()
                    .copied()
                    .map(|family| {
                        StatementCompletionProjection::new(StatementOutputShape::Rows, family)
                    });
                let Some(projection) = projection else {
                    return Ok(self.governed_typed_error(
                        internal_error("client statement requires an explicit presentation"),
                        statement,
                    ));
                };
                return self
                    .execute_trino_truncate(
                        TruncateCommand {
                            target_parts: table.parts,
                            target: TruncateTarget::WholeBranch(branch),
                        },
                        Some(projection),
                        CommandEntry { statement, input },
                    )
                    .await;
            }
            Prep::Delete(node) => {
                return self
                    .execute_trino_write(
                        source,
                        TrinoWrite::Delete(node),
                        CommandEntry { statement, input },
                    )
                    .await;
            }
            Prep::Update(node) => {
                return self
                    .execute_trino_write(
                        source,
                        TrinoWrite::Update(node),
                        CommandEntry { statement, input },
                    )
                    .await;
            }
            Prep::Merge(node) => {
                return self
                    .execute_trino_write(
                        source,
                        TrinoWrite::Merge(node),
                        CommandEntry { statement, input },
                    )
                    .await;
            }
            Prep::Call(node) => {
                return self
                    .execute_trino_call(node, CommandEntry { statement, input })
                    .await;
            }
            Prep::CreateView(node) => {
                return self
                    .execute_trino_view(
                        source,
                        TrinoViewInput::Create(Box::new(node)),
                        CommandEntry { statement, input },
                    )
                    .await;
            }
            Prep::DropView(node) => {
                return self
                    .execute_trino_view(
                        source,
                        TrinoViewInput::Drop(node),
                        CommandEntry { statement, input },
                    )
                    .await;
            }
            Prep::ShowCreateView(node) => {
                return self
                    .execute_trino_view(
                        source,
                        TrinoViewInput::ShowCreate(node),
                        CommandEntry { statement, input },
                    )
                    .await;
            }
            Prep::ShowCreateMaterializedView(node) => {
                return self
                    .execute_trino_show_create_mv(source, node, CommandEntry { statement, input })
                    .await;
            }
            Prep::CreateMaterializedView(node) => {
                return self
                    .execute_observed_trino_mv_properties(
                        source,
                        TrinoMvProperties::Create(Box::new(node)),
                        input.request,
                        input.command,
                        input.deadline,
                        statement,
                    )
                    .await;
            }
            Prep::SetMaterializedViewProperties {
                name,
                properties,
                span,
            } => {
                return self
                    .execute_observed_trino_mv_properties(
                        source,
                        TrinoMvProperties::Alter {
                            name,
                            properties,
                            span,
                        },
                        input.request,
                        input.command,
                        input.deadline,
                        statement,
                    )
                    .await;
            }
            Prep::DropMaterializedView(node) => {
                let action = self
                    .service
                    .command_executor
                    .mv_call
                    .prepare_trino_drop(node, &input.request, &input.command)
                    .map_err(product_command_service_error);
                return self.run_trino_action(statement, action).await;
            }
            Prep::RefreshMaterializedView(node) | Prep::ExplainRefreshMaterializedView(node, _) => {
                let action = self
                    .service
                    .command_executor
                    .mv_call
                    .prepare_trino_refresh(node, explain_refresh, &input.request, &input.command)
                    .map_err(product_command_service_error);
                return self.run_trino_action(statement, action).await;
            }
        };
        match command {
            Ok(command) => {
                self.execute_trino_product(command, CommandEntry { statement, input })
                    .await
            }
            Err(error) => Ok(self.governed_typed_error(error, statement)),
        }
    }
}

enum TrinoWrite {
    Insert(ast::TrinoInsert),
    Delete(ast::TrinoDelete),
    Update(ast::TrinoUpdate),
    Merge(ast::TrinoMerge),
}

enum TrinoViewInput {
    Create(Box<ast::TrinoCreateView>),
    Drop(ast::TrinoDropView),
    ShowCreate(ast::TrinoShowCreate),
}

impl FrontendQuerySession {
    async fn execute_trino_write(
        &self,
        source: Arc<str>,
        command: TrinoWrite,
        entry: CommandEntry,
    ) -> Result<StatementResult, QueryServiceError> {
        use crate::dml::{
            insert::prepare_trino_insert_shape,
            mutation::{prepare_trino_merge_shape, prepare_trino_update_shape},
            prepare_trino_delete_shape,
        };
        use crate::query_execution::dml::{
            delete::DeleteStatementInput, insert::InsertStatementInput,
            mutation::MutationStatementInput,
        };
        let CommandEntry { statement, input } = entry;
        let dml_error =
            |error| routed_execution_service_error(dml_result::<()>(Err(error)).unwrap_err());
        let action = (|| {
            let sql = source.to_string();
            let launcher = self.statement_logical_execution_launcher();
            match command {
                TrinoWrite::Insert(node) => {
                    let (command, reference) =
                        prepare_trino_insert_shape(node, &source).map_err(dml_error)?;
                    self.service
                        .insert_engine
                        .prepare_statement_action(InsertStatementInput {
                            command,
                            reference,
                            sql,
                            context: input.request,
                            query_options: input.options,
                            launcher,
                            #[cfg(novarocks_dev_controls)]
                            observation: input.observation,
                        })
                        .map_err(|error| error.into_service())
                }
                TrinoWrite::Delete(node) => {
                    let statement = prepare_trino_delete_shape(node, &source).map_err(dml_error)?;
                    self.service
                        .delete_engine
                        .prepare_statement_action(DeleteStatementInput {
                            statement,
                            sql,
                            context: input.request,
                            query_options: input.options,
                            launcher,
                            #[cfg(novarocks_dev_controls)]
                            observation: input.observation,
                        })
                        .map_err(|error| error.into_service())
                }
                TrinoWrite::Update(_) | TrinoWrite::Merge(_) => {
                    let statement = match command {
                        TrinoWrite::Update(node) => prepare_trino_update_shape(node, &source),
                        TrinoWrite::Merge(node) => prepare_trino_merge_shape(node, &source),
                        _ => unreachable!("selected row mutation"),
                    }
                    .map_err(dml_error)?;
                    self.service
                        .mutation_engine
                        .prepare_statement_action(MutationStatementInput {
                            statement,
                            sql,
                            context: input.request,
                            query_options: input.options,
                            launcher,
                            #[cfg(novarocks_dev_controls)]
                            observation: input.observation,
                        })
                        .map_err(|error| error.into_service())
                }
            }
        })();
        self.run_trino_action(statement, action).await
    }
}

impl FrontendQuerySession {
    async fn execute_trino_show_create_mv(
        &self,
        source: Arc<str>,
        node: ast::TrinoShowCreate,
        entry: CommandEntry,
    ) -> Result<StatementResult, QueryServiceError> {
        let CommandEntry {
            mut statement,
            input,
        } = entry;
        let name = novarocks_query_application::sql::lower_object_name(&node.name);
        let binding = match self
            .service
            .product_command_router
            .acquire_table_read_binding(&name, &input.request)
        {
            Ok(binding) => binding,
            Err(error) => {
                return Ok(
                    self.governed_typed_error(product_command_service_error(error), statement)
                );
            }
        };
        let execution_owner = statement
            .take_execution_owner()
            .expect("one original materialized view definition owner");
        let context = input.command.connector_context().clone();
        let mut worker = Box::pin(async move {
            let result =
                crate::mv::domain::show_create::execute(binding, context, &source, node.span)
                    .await
                    .map(StatementResult::Query)
                    .map_err(RoutedExecutionError::Application);
            Ok((result, execution_owner))
        }) as TypedCommandWorker;
        self.finish_command_worker(
            statement,
            input.cancellation,
            input.timeout_duration,
            &mut worker,
        )
        .await
    }

    async fn execute_trino_view(
        &self,
        source: Arc<str>,
        command: TrinoViewInput,
        entry: CommandEntry,
    ) -> Result<StatementResult, QueryServiceError> {
        use novarocks_query_application::view::ViewCommand;

        let CommandEntry {
            mut statement,
            input,
        } = entry;
        let execution_owner = statement
            .take_execution_owner()
            .expect("one original View owner");
        let view = self.service.command_executor.view.clone();
        let executor = self.service.query_blocking_executor.clone();
        let mut worker = Box::pin(async move {
            let body = move |request: &RequestContext, context: &CommandContext| {
                let command = match &command {
                    TrinoViewInput::Create(node) => {
                        ViewCommand::trino_create(&source, node).map_err(procedure_command_error)?
                    }
                    TrinoViewInput::Drop(node) => ViewCommand::Drop {
                        name: &node.name,
                        if_exists: node.exists,
                    },
                    TrinoViewInput::ShowCreate(node) => {
                        ViewCommand::ShowCreate { name: &node.name }
                    }
                };
                view.execute_command(
                    command,
                    request.session().current_catalog(),
                    request.session().current_database(),
                    context.connector_context(),
                )
                .map_err(|error| error.with_analysis_source(&source))
            };
            let result = execute_product_command_edge(executor, input.request, input.command, body)
                .await
                .map_err(product_command_error);
            Ok((result, execution_owner))
        }) as TypedCommandWorker;
        self.finish_command_worker(
            statement,
            input.cancellation,
            input.timeout_duration,
            &mut worker,
        )
        .await
    }

    async fn execute_trino_call(
        &self,
        syntax: ast::TrinoCall,
        entry: CommandEntry,
    ) -> Result<StatementResult, QueryServiceError> {
        use novarocks_query_application::sql::PreparedMaintenanceCall;
        let CommandEntry {
            mut statement,
            input,
        } = entry;
        if !crate::mv::command::MvCommandExecutor::owns_typed_call(&syntax.name) {
            let error = novarocks_query_application::sql::trino_admission::procedure_not_registered(
                &syntax.name,
            );
            return Ok(self.governed_typed_error(error, statement));
        }
        let command = match PreparedMaintenanceCall::from_trino(&syntax) {
            Ok(call) => Arc::new(call),
            Err(error) => {
                return Ok(
                    self.governed_typed_error(product_command_service_error(error), statement)
                );
            }
        };
        let execution_owner = statement
            .take_execution_owner()
            .expect("one original CALL owner");
        let specialized = self.service.command_executor.clone();
        let mut worker = Box::pin(async move {
            let result = specialized
                .execute_maintenance_call(command, &input.request, &input.command)
                .await
                .map_err(|error| RoutedExecutionError::Engine(error.to_string()))
                .and_then(|output| {
                    output.ok_or_else(|| {
                        RoutedExecutionError::Engine(
                            "Registered CALL procedure did not produce a result".to_string(),
                        )
                    })
                });
            Ok((result, execution_owner))
        }) as TypedCommandWorker;
        self.finish_command_worker(
            statement,
            input.cancellation,
            input.timeout_duration,
            &mut worker,
        )
        .await
    }
}

impl FrontendQuerySession {
    async fn execute_trino_analyze(
        &self,
        source: Arc<str>,
        syntax: ast::TrinoAnalyze,
        entry: CommandEntry,
    ) -> Result<StatementResult, QueryServiceError> {
        use super::analyze_property_computation::AnalyzeProperties;

        let CommandEntry { statement, input } = entry;
        let action = (|| {
            let window = input.command.result_window_alias().ok_or_else(|| {
                internal_error("ANALYZE properties have no original result window")
            })?;
            let control = novarocks_sql::compiler::SqlCompileControl::new(
                input.deadline,
                crate::query_execution::planning::sql_cancellation_observation(input.cancellation),
            );
            let compiler = &self.service.query_compiler;
            let context = compiler.property_cpu_context(
                input.request.session().current_database().to_owned(),
                input.request.execution().sql_semantics().clone(),
                self.service.required_constant_interpreter.clone(),
                control,
                input.command.scope().clone(),
                window,
            );
            self.service
                .statistics_command
                .prepare_trino_analyze(
                    AnalyzeProperties {
                        source,
                        syntax,
                        context,
                        cpu: compiler.new_planning_cpu_source(),
                    },
                    &input.request,
                    input.command.connector_context(),
                )
                .map_err(product_command_service_error)
        })();
        self.run_trino_action(statement, action).await
    }
}

impl FrontendQuerySession {
    async fn execute_trino_create_branch(
        &self,
        source: Arc<str>,
        node: ast::TrinoCreateBranch,
        entry: CommandEntry,
    ) -> Result<StatementResult, QueryServiceError> {
        let CommandEntry {
            mut statement,
            input,
        } = entry;
        let Some(window) = input.command.result_window_alias() else {
            return Ok(self.governed_typed_error(
                internal_error("CREATE BRANCH has no original result window"),
                statement,
            ));
        };
        let control = novarocks_sql::compiler::SqlCompileControl::new(
            input.deadline,
            crate::query_execution::planning::sql_cancellation_observation(
                input.cancellation.clone(),
            ),
        );
        let cpu_context = self.service.query_compiler.property_cpu_context(
            input.request.session().current_database().to_owned(),
            input.request.execution().sql_semantics().clone(),
            self.service.required_constant_interpreter.clone(),
            control,
            input.command.scope().clone(),
            window.clone(),
        );
        let execution_owner = statement
            .take_execution_owner()
            .expect("one original branch owner");
        let engine = self.service.iceberg_ref_command.clone();
        let connector = input.command.connector_context().clone();
        let prepare_connector = connector.clone();
        let mut worker = Box::pin(execute_prepared_dml_statement(
            self.service.query_blocking_executor.clone(),
            input.cancellation.clone(),
            execution_owner,
            Some(window),
            move || {
                let mut constants = cpu_context.constant_port();
                engine
                    .prepare_trino_branch(
                        &source,
                        &node,
                        &input.request,
                        &prepare_connector,
                        &mut constants,
                        &cpu_context.control,
                    )
                    .map_err(RoutedExecutionError::Application)
            },
            move |prepared| prepared.execute(&connector).map_err(product_command_error),
        )) as TypedCommandWorker;
        self.finish_command_worker(
            statement,
            input.cancellation,
            input.timeout_duration,
            &mut worker,
        )
        .await
    }
}

impl FrontendQuerySession {
    async fn execute_trino_table_procedure(
        &self,
        source: Arc<str>,
        node: ast::TrinoTableExecute,
        entry: CommandEntry,
    ) -> Result<StatementResult, QueryServiceError> {
        use super::maintenance_action::{PreparedTrinoTableExecute, prepare_trino_table_execute};

        let CommandEntry {
            mut statement,
            input,
        } = entry;
        let Some(window) = input.command.result_window_alias() else {
            return Ok(self.governed_typed_error(
                internal_error("ALTER TABLE EXECUTE has no original result window"),
                statement,
            ));
        };
        let control = novarocks_sql::compiler::SqlCompileControl::new(
            input.deadline,
            crate::query_execution::planning::sql_cancellation_observation(
                input.cancellation.clone(),
            ),
        );
        let cpu_context = self.service.query_compiler.property_cpu_context(
            input.request.session().current_database().to_owned(),
            input.request.execution().sql_semantics().clone(),
            self.service.required_constant_interpreter.clone(),
            control,
            input.command.scope().clone(),
            window.clone(),
        );
        let maintenance = self.service.maintenance_command.clone();
        let request = input.request.clone();
        let command = input.command.clone();
        let statement_started_at = input.statement_started_at;
        let node = Arc::new(node);
        #[cfg(novarocks_dev_controls)]
        let equality_source = source.clone();
        #[cfg(novarocks_dev_controls)]
        let observation = input.observation.clone();
        let (result, execution_owner, window) = execute_synchronous_stage(
            self.service.query_blocking_executor.clone(),
            input.cancellation.clone(),
            statement
                .take_execution_owner()
                .expect("one original table-procedure owner"),
            Some(window),
            move || {
                prepare_trino_table_execute(
                    &maintenance,
                    &source,
                    &node,
                    &request,
                    &command,
                    statement_started_at,
                    &mut cpu_context.constant_port(),
                    #[cfg(novarocks_dev_controls)]
                    observation,
                )
                .map_err(product_command_error)
            },
        )
        .await
        .map_err(internal_error)?;
        statement.restore_execution_owner(execution_owner);
        let resolved = match result {
            Ok(PreparedTrinoTableExecute::Action(action)) => {
                return self
                    .execute_observed_statement_action(statement, action)
                    .await;
            }
            Ok(PreparedTrinoTableExecute::Mutation(prepared)) => {
                let execution_owner = statement
                    .take_execution_owner()
                    .expect("same original table procedure owner");
                let executor = self.service.query_blocking_executor.clone();
                let cancellation = input.cancellation.clone();
                let connector = input.command.connector_context().clone();
                let mut worker = Box::pin(async move {
                    execute_synchronous_stage(
                        executor,
                        cancellation,
                        execution_owner,
                        window,
                        move || prepared.execute(&connector).map_err(product_command_error),
                    )
                    .await
                    .map(|(result, owner, _window)| (result, owner))
                }) as TypedCommandWorker;
                return self
                    .finish_command_worker(
                        statement,
                        input.cancellation,
                        input.timeout_duration,
                        &mut worker,
                    )
                    .await;
            }
            Ok(PreparedTrinoTableExecute::AddFiles(resolved)) => resolved,
            #[cfg(novarocks_dev_controls)]
            Ok(PreparedTrinoTableExecute::EqualityDelete(prepared)) => {
                use crate::query_execution::dml::delete::DeleteStatementInput;
                let action = self
                    .service
                    .delete_engine
                    .prepare_statement_action(DeleteStatementInput {
                        statement: prepared,
                        sql: equality_source.to_string(),
                        context: input.request,
                        query_options: input.options,
                        launcher: self.statement_logical_execution_launcher(),
                        observation: input.observation,
                    })
                    .map_err(|error| error.into_service());
                return self.run_trino_action(statement, action).await;
            }
            Err(error) => {
                return Ok(
                    self.governed_typed_error(routed_execution_service_error(error), statement)
                );
            }
        };

        let dml = Arc::clone(&self.service.dml);
        let engine = Arc::clone(&self.service.add_files_engine);
        let planner = Arc::clone(&dml);
        let prepare_engine = Arc::clone(&engine);
        let Some(window) = window else {
            return Ok(self.governed_typed_error(
                internal_error("ADD FILES has no original result window"),
                statement,
            ));
        };
        let byte_author = crate::dml::add_files::registration_byte_author(window.clone());
        let planning_cancellation = input.cancellation.clone();
        let dispatch_cancellation = input.cancellation.clone();
        let mut worker = Box::pin(execute_prepared_dml_statement(
            self.service.query_blocking_executor.clone(),
            input.cancellation.clone(),
            statement
                .take_execution_owner()
                .expect("same original ADD FILES owner"),
            Some(window),
            move || {
                add_files_dml_result(
                    planner.prepare_resolved_add_files(
                        prepare_engine.as_ref(),
                        resolved,
                        &input.request,
                        Some(&input.options),
                    ),
                    &planning_cancellation,
                )
            },
            move |prepared| {
                let count = add_files_dml_result(
                    dml.execute_prepared_add_files(engine.as_ref(), prepared, byte_author),
                    &dispatch_cancellation,
                )?;
                crate::table_maintenance::result::add_files_result(count)
                    .map(StatementResult::Query)
                    .map_err(RoutedExecutionError::Engine)
            },
        )) as TypedCommandWorker;
        self.finish_command_worker(
            statement,
            input.cancellation,
            input.timeout_duration,
            &mut worker,
        )
        .await
    }
}

fn add_files_dml_result<T>(
    result: Result<T, crate::dml::DmlError>,
    cancellation: &QueryCancellationView,
) -> Result<T, RoutedExecutionError> {
    dml_result(result).map_err(|error| match error {
        RoutedExecutionError::Application(error) => {
            RoutedExecutionError::Application(error.reconcile_local_stop_observation(cancellation))
        }
        other => other,
    })
}

#[cfg(test)]
mod add_files_control_tests {
    use super::*;
    use novarocks_failure_contract::SharedFailureCause;
    use novarocks_query_application::cancellation::QueryCancellationSource;
    use novarocks_workload_control::{CancellationReason, WorkError};

    #[test]
    fn add_files_stop_uses_the_original_accepted_deadline_and_retains_the_cause() {
        let cancellation = QueryCancellationSource::new();
        let cause = SharedFailureCause::new(WorkError::Cancelled(CancellationReason::Requested));
        let dml_error = || {
            crate::dml::DmlError::from_application(QueryServiceError::from_failure_cause(
                cause.clone(),
            ))
        };
        let observed = routed_execution_service_error(
            add_files_dml_result::<()>(Err(dml_error()), &cancellation.view()).unwrap_err(),
        );
        assert_ne!(observed.kind(), QueryServiceErrorKind::Timeout);
        let _ = cancellation.request(QueryCancellationReason::DeadlineExceeded { timeout_ms: 51 });
        let accepted = routed_execution_service_error(
            add_files_dml_result::<()>(Err(dml_error()), &cancellation.view()).unwrap_err(),
        );
        assert_eq!(accepted.kind(), QueryServiceErrorKind::Timeout);
        assert_eq!(accepted.failure_cause(), Some(&cause));
    }
}

#[cfg(test)]
mod branch_delete_tests {
    use super::*;

    #[test]
    fn whole_branch_delete_uses_the_same_target_with_delete_completion() {
        let sql = "DELETE FROM lake.db.t @dev";
        let parsed = novarocks_parser::parser::parse_statement(sql).unwrap();
        let TrinoStatementAction::Prepare(preparation) =
            admit_trino_statement(sql, parsed).unwrap()
        else {
            panic!("expected whole-branch preparation");
        };
        let plan = preparation_plan(&preparation).unwrap();
        assert_eq!(plan.admission.kind, StatementKind::Delete);
        assert_eq!(plan.admission.shape, StatementOutputShape::Rows);
        assert_eq!(plan.work_class, WorkClass::Management);
        assert_eq!(plan.window_class, ResultWindowClass::Local);
        assert!(!plan.waits_for_topology);

        let TrinoStatementPreparation::DeleteEntireBranch { table, branch } = preparation else {
            panic!("expected whole-branch delete preparation");
        };
        assert_eq!(table.parts, ["lake", "db", "t"]);
        assert_eq!(branch, "dev");
    }
}

#[cfg(test)]
mod call_routing_tests {
    use super::*;
    use crate::mv::command::MvCommandExecutor;
    use novarocks_query_application::sql::trino_admission::procedure_not_registered;

    fn name(sql: &str) -> ast::ObjectName {
        let ast::TrinoStatement::Call(call) =
            novarocks_parser::parser::parse_statement(sql).unwrap()
        else {
            panic!("expected CALL");
        };
        call.name
    }

    #[test]
    fn registered_management_and_development_calls_keep_their_owner() {
        for procedure in [
            "novarocks_mv_management_status",
            "novarocks_mv_resume_management",
            "novarocks_mv_set_owner",
        ] {
            assert!(MvCommandExecutor::owns_typed_call(&name(&format!(
                "CALL {procedure}()"
            ))));
        }
        for procedure in [
            "novarocks_mv_document_expiry",
            "novarocks_imv_stateless_rebuild",
        ] {
            assert_eq!(
                MvCommandExecutor::owns_typed_call(&name(&format!(
                    "CALL ice.system.{procedure}()"
                ))),
                cfg!(novarocks_dev_controls)
            );
        }
    }

    #[test]
    fn retired_and_unregistered_calls_have_the_same_typed_error() {
        for procedure in [
            "rewrite_data_files",
            "rewrite_position_delete_files",
            "rewrite_manifests",
            "expire_snapshots",
            "remove_orphan_files",
            "rollback_to_snapshot",
            "arbitrary_unknown_procedure",
        ] {
            let name = name(&format!(
                "CALL ice.system.{}()",
                procedure.to_ascii_uppercase()
            ));
            assert!(!MvCommandExecutor::owns_typed_call(&name));
            let error = procedure_not_registered(&name);
            let user_error = error.user_error().expect("typed user error");
            assert_eq!(user_error.code().as_str(), "sql.admit.procedure_not_found");
            assert_eq!(
                user_error.message(),
                format!("Procedure not registered: system.{procedure}")
            );
        }
    }
}
