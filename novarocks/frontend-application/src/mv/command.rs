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

//! Closed typed executor for Iceberg MV statements.

use crate::mv::domain::application::{
    MvAlterAction, MvAlterStatement, MvCreateDistribution, MvCreatePartitionField,
    MvCreateRefreshPolicy, MvCreateStatement, MvDropStatement, MvRefreshRequest, MvShowStatement,
};
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use novarocks_parser::ast::{
    CallStatement, Literal, LiteralKind, MaterializedViewAlterAction as TypedAlterAction,
    MaterializedViewExplainLevel, MaterializedViewPartitionArgument,
    MaterializedViewPartitionField, MaterializedViewRefreshMode,
    MaterializedViewRefreshPolicy as TypedRefreshPolicy, MaterializedViewStatement,
    ObjectName as TypedObjectName,
};
use novarocks_query_application::admitted_query_context::RequestContext;
use novarocks_query_application::api::{
    CommandContext, MaterializedViewCommand, MaterializedViewCommandConsumer,
};
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_sql::semantic::IcebergPartitionFieldExpr;
use novarocks_types::naming::normalize_identifier;

use super::FrontendMvProductAdapter;
use crate::mv::domain::refresh::resolve_refresh_mv_target;
use std::sync::Arc;

#[derive(Clone)]
pub struct MvCommandExecutor {
    execution_services: Option<crate::query::mv_action::MvExecutionServices>,
    ports: IcebergMvCorePorts,
    refresh_service: Arc<FrontendMvProductAdapter>,
    /// The one process-local owner of management continuation. Absent only on
    /// a composition with no management authority at all, where the management
    /// procedures have nothing to report and say so.
    continuation: Option<Arc<novarocks_mv_application::management::ManagementContinuationService>>,
    /// Where an operator's declaration is recorded. Absent means the
    /// declaration commands refuse: an unrecorded declaration must not act.
    management_audit: Option<Arc<dyn novarocks_mv_application::management::ManagementAuditSink>>,
}

impl MvCommandExecutor {
    pub fn new(
        ports: IcebergMvCorePorts,
        refresh_service: Arc<FrontendMvProductAdapter>,
        _storage_observation: Arc<dyn MvStorageObservationPort>,
        continuation: Option<
            Arc<novarocks_mv_application::management::ManagementContinuationService>,
        >,
        management_audit: Option<
            Arc<dyn novarocks_mv_application::management::ManagementAuditSink>,
        >,
    ) -> Self {
        Self {
            execution_services: None,
            ports,
            refresh_service,
            continuation,
            management_audit,
        }
    }

    pub(crate) fn with_execution_services(
        mut self,
        services: crate::query::mv_action::MvExecutionServices,
    ) -> Self {
        self.execution_services = Some(services);
        self
    }

    fn prepare_statement_action(
        &self,
        statement: &MaterializedViewStatement,
        context: &RequestContext,
        command_context: &CommandContext,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        novarocks_query_application::api::CommandError,
    > {
        if matches!(
            statement,
            MaterializedViewStatement::Show(_) | MaterializedViewStatement::ExplainRefresh(_)
        ) {
            let services = self.execution_services.clone().ok_or_else(|| {
                novarocks_query_application::api::CommandError::new(
                    novarocks_query_application::api::CommandErrorKind::Unsupported,
                    "MV execution services are unavailable",
                )
            })?;
            let request = match statement {
                MaterializedViewStatement::Show(show) => crate::query::mv_read_action::ReadRequest::Show {
                    catalog: context.session().current_catalog().map(str::to_owned),
                    statement: lower_typed_show(show)?,
                },
                MaterializedViewStatement::ExplainRefresh(explain) => crate::query::mv_read_action::ReadRequest::Explain(Box::new(
                    crate::query_execution::mv_assembly::refresh_explain::MvExplainPreparationInput {
                        ports: self.ports.clone(),
                        current_catalog: context.session().current_catalog().map(str::to_owned),
                        current_database: context.session().current_database().to_owned(),
                        statement: lower_typed_refresh(&explain.refresh)?,
                        level: lower_typed_explain_level(explain.level),
                        context: command_context.connector_context().clone(),
                    })),
                _ => unreachable!(),
            };
            return crate::query::mv_read_action::prepare(
                self.ports.clone(),
                services,
                request,
                context.execution().clone(),
                *context.session().presentation(),
            );
        }
        use crate::query::mv_action::{CreateRequest, MvCommandRequest, prepare};
        let catalog = context.session().current_catalog();
        let database = context.session().current_database();
        let request = match statement {
            MaterializedViewStatement::Refresh(refresh) => {
                let statement = lower_typed_refresh(refresh)?;
                let requested =
                    resolve_refresh_mv_target(catalog, database, &statement.name_parts)?;
                MvCommandRequest::Refresh {
                    statement,
                    requested,
                }
            }
            MaterializedViewStatement::Alter(alter)
                if matches!(&alter.action, TypedAlterAction::Repartition(_)) =>
            {
                let alter = lower_typed_alter(alter)?;
                let MvAlterAction::Repartition(fields) = alter.action else {
                    unreachable!()
                };
                let target = resolve_refresh_mv_target(catalog, database, &alter.name_parts)?;
                MvCommandRequest::Repartition {
                    statement: MvRefreshRequest {
                        name_parts: alter.name_parts,
                        full: false,
                    },
                    target,
                    fields: fields.iter().map(IcebergPartitionFieldExpr::from).collect(),
                }
            }
            MaterializedViewStatement::Create(create) => {
                let statement = lower_typed_create(create)?;
                let target = resolve_refresh_mv_target(catalog, database, &statement.name_parts)?;
                MvCommandRequest::Create(Box::new(CreateRequest { target, statement }))
            }
            MaterializedViewStatement::Drop(drop) => {
                let statement = MvDropStatement {
                    name_parts: lower_typed_name_parts(&drop.name)?,
                    if_exists: drop.if_exists,
                };
                let target = resolve_refresh_mv_target(catalog, database, &statement.name_parts)?;
                MvCommandRequest::Control(crate::query::mv_action::MvControlRequest::Drop {
                    target,
                    statement,
                })
            }
            MaterializedViewStatement::Alter(alter) => {
                let statement = lower_typed_alter(alter)?;
                let target = resolve_refresh_mv_target(catalog, database, &statement.name_parts)?;
                MvCommandRequest::Control(crate::query::mv_action::MvControlRequest::Alter {
                    target,
                    statement,
                })
            }
            MaterializedViewStatement::Show(_) | MaterializedViewStatement::ExplainRefresh(_) => {
                unreachable!("MV read statements were selected before the mutation action")
            }
        };
        let services = self.execution_services.clone().ok_or_else(|| {
            novarocks_query_application::api::CommandError::new(
                novarocks_query_application::api::CommandErrorKind::Unsupported,
                "MV execution services are unavailable",
            )
        })?;
        let (product, activation) = self.refresh_service.original_refresh_ports();
        prepare(
            catalog.map(str::to_owned),
            database.to_owned(),
            self.ports.clone(),
            activation,
            product,
            services,
            request,
            context.execution().clone(),
            command_context.connector_context().clone(),
        )
    }

    /// Executes the MV management procedures and the test-only stateless
    /// rebuild directly from parser-owned `CALL` syntax. Other procedures
    /// remain a route miss.
    pub fn try_execute_typed_call(
        &self,
        statement: &CallStatement,
        current_database: &str,
        session_principal: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Option<StatementResult>, String> {
        if let Some(result) = crate::mv::domain::management_call::try_execute_management_call(
            self.continuation.as_ref(),
            self.management_audit.as_ref(),
            self,
            session_principal,
            statement,
        )? {
            return Ok(Some(result));
        }
        #[cfg(novarocks_dev_controls)]
        if let Some(result) = crate::mv::domain::document_expiry::try_execute_document_expiry(
            &self.ports,
            statement,
            current_database,
            connector_context,
            &|catalog, target, operation_id, context| {
                crate::mv::domain::staged_create::install_admitted_current_projection(
                    self.ports.management_entrance()?.as_ref(),
                    self.ports.readiness().as_ref(),
                    self.ports.connector_control(),
                    catalog,
                    target,
                    operation_id,
                    context,
                )
            },
            &|catalog, target, operation_id, context| {
                crate::mv::domain::staged_create::install_configured_current_projection(
                    self.ports.management_entrance()?.as_ref(),
                    self.ports.readiness().as_ref(),
                    self.ports.connector_control(),
                    catalog,
                    target,
                    operation_id,
                    context,
                )
            },
        )? {
            return Ok(Some(result));
        }
        #[cfg(novarocks_dev_controls)]
        return crate::mv::domain::execute_typed_novarocks_imv_stateless_rebuild(
            self.ports.connector_control(),
            self.ports.readiness().as_ref(),
            self.refresh_service.product_service(),
            statement,
            current_database,
            connector_context.clone(),
        );
        #[cfg(not(novarocks_dev_controls))]
        {
            let _ = (current_database, connector_context);
            Ok(None)
        }
    }
}

pub(crate) fn mv_application_command_error(
    error: crate::mv::domain::application::MvApplicationError,
) -> novarocks_query_application::api::CommandError {
    use crate::mv::domain::application::MvApplicationErrorKind;
    use novarocks_query_application::api::StatementExternalEffectDisposition;
    // These two kinds are emitted only with the original publication's fact.
    // Other business failures alone do not prove the absence of an effect.
    let disposition = match error.kind() {
        MvApplicationErrorKind::CommitUnknown => {
            Some(StatementExternalEffectDisposition::CommitUnknown)
        }
        MvApplicationErrorKind::KnownCommittedFinalizeFailed => {
            Some(StatementExternalEffectDisposition::KnownCommitted)
        }
        _ => None,
    };
    let command = if let Some(cause) = error.original_failure() {
        // A Provider source is not a compiler failure. Retain a compile
        // sidecar only when the actual compiler already supplied one.
        novarocks_query_application::api::CommandError::from_failure_cause(cause.clone())
            .with_compile_control(error.compile_control_error())
    } else {
        mv_command_error(mv_compile_error(error))
    };
    command.with_external_effect_disposition(disposition)
}

fn mv_compile_error(
    error: crate::mv::domain::application::MvApplicationError,
) -> novarocks_sql::compiler::SqlCompileError {
    match error.compile_control_error() {
        Some(error) => error.into(),
        None => match error.unavailable_implementation() {
            Some(analyze) => novarocks_sql::compiler::SqlCompileError::Analyze(analyze.clone()),
            None => novarocks_sql::compiler::SqlCompileError::Compilation(error.to_string()),
        },
    }
}

#[cfg(test)]
pub(crate) fn project_product_failure_for_test(
    error: crate::mv::domain::application::MvApplicationError,
) -> novarocks_query_application::api::CommandError {
    mv_application_command_error(error)
}

/// Role-local implementation of the Query Application MV consumer. It owns
/// Connector and query-execution adapters, while product lifecycle remains in
/// the injected `FrontendMvProductAdapter` composition.
#[derive(Clone)]
pub struct FrontendMvCommandConsumer {
    executor: MvCommandExecutor,
}

impl FrontendMvCommandConsumer {
    pub fn new(executor: MvCommandExecutor) -> Self {
        Self { executor }
    }
}

impl MaterializedViewCommandConsumer for FrontendMvCommandConsumer {
    fn prepare_statement_action(
        &self,
        command: MaterializedViewCommand,
        context: RequestContext,
        command_context: CommandContext,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        novarocks_query_application::api::CommandError,
    > {
        self.executor
            .prepare_statement_action(command.statement(), &context, &command_context)
    }
}

fn mv_command_error(
    error: novarocks_sql::compiler::SqlCompileError,
) -> novarocks_query_application::api::CommandError {
    match error {
        novarocks_sql::compiler::SqlCompileError::Cancelled => {
            novarocks_query_application::api::CommandError::from_compile_control(
                novarocks_type_contract::CompileControlError::Cancelled,
            )
        }
        novarocks_sql::compiler::SqlCompileError::DeadlineExceeded => {
            novarocks_query_application::api::CommandError::from_compile_control(
                novarocks_type_contract::CompileControlError::DeadlineExceeded,
            )
        }
        novarocks_sql::compiler::SqlCompileError::ResourceExhausted => {
            novarocks_query_application::api::CommandError::from_compile_control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        }
        novarocks_sql::compiler::SqlCompileError::CompilationSource(source) => {
            novarocks_query_application::api::CommandError::from_compile_control(
                novarocks_type_contract::CompileControlError::Source(source),
            )
        }
        novarocks_sql::compiler::SqlCompileError::Analyze(error) => match error.control_error() {
            Some(error) => {
                novarocks_query_application::api::CommandError::from_compile_control(error)
            }
            None if error.kind()
                == novarocks_sql::analyze_error::AnalyzeErrorKind::UnavailableImplementation =>
            {
                novarocks_query_application::api::CommandError::from_user_error(
                    error.to_user_error(None),
                )
            }
            None => novarocks_query_application::api::CommandError::new(
                novarocks_query_application::api::CommandErrorKind::Failed,
                error.to_string(),
            ),
        },
        error => novarocks_query_application::api::CommandError::new(
            novarocks_query_application::api::CommandErrorKind::Failed,
            error.to_string(),
        ),
    }
}

fn lower_typed_create(
    statement: &novarocks_parser::ast::CreateMaterializedView,
) -> Result<MvCreateStatement, String> {
    let distribution = statement.distribution.as_ref().ok_or_else(|| {
        "CREATE MATERIALIZED VIEW requires a DISTRIBUTED BY HASH(...) BUCKETS n clause".to_string()
    })?;
    let bucket_count = distribution
        .buckets
        .as_ref()
        .ok_or_else(|| {
            "CREATE MATERIALIZED VIEW requires a DISTRIBUTED BY HASH(...) BUCKETS n clause"
                .to_string()
        })
        .and_then(|value| lower_typed_u32(value, "BUCKETS count"))?;
    let primary_key = statement
        .primary_key
        .as_ref()
        .map(|columns| {
            let mut seen = std::collections::BTreeSet::new();
            let mut lowered = Vec::with_capacity(columns.len());
            for column in columns {
                if !seen.insert(column.value.to_ascii_lowercase()) {
                    return Err(format!(
                        "duplicate column `{}` in PRIMARY KEY clause",
                        column.value
                    ));
                }
                lowered.push(column.value.clone());
            }
            Ok(lowered)
        })
        .transpose()?;
    let select_query = statement.query.clone();

    let partition_by = statement
        .partition_by
        .as_ref()
        .map(|fields| {
            lower_typed_partition_fields(fields)?;
            Ok::<_, String>(fields.iter().map(MvCreatePartitionField::from).collect())
        })
        .transpose()?;

    Ok(MvCreateStatement {
        name_parts: lower_typed_name_parts(&statement.name)?,
        if_not_exists: statement.if_not_exists,
        partition_by,
        distribution: Some(MvCreateDistribution {
            hash_columns: distribution
                .hash_columns
                .iter()
                .map(|column| normalize_identifier(&column.value))
                .collect::<Result<Vec<_>, _>>()?,
            bucket_count: Some(bucket_count),
        }),
        refresh_policy: lower_typed_refresh_policy(
            statement
                .refresh
                .as_ref()
                .unwrap_or(&TypedRefreshPolicy::Manual { deferred: false }),
        )?,
        select_sql: novarocks_parser::printer::print_query(&select_query),
        select_query,
        properties: lower_typed_properties(&statement.properties)?,
        primary_key,
    })
}

fn lower_typed_alter(
    statement: &novarocks_parser::ast::AlterMaterializedView,
) -> Result<MvAlterStatement, String> {
    let action = match &statement.action {
        TypedAlterAction::SetRefresh(policy) => {
            MvAlterAction::SetRefresh(lower_typed_refresh_policy(policy)?)
        }
        TypedAlterAction::SetProperties(properties) => {
            let properties = lower_typed_properties(properties)?;
            if properties.is_empty() {
                return Err(
                    "ALTER MATERIALIZED VIEW SET TBLPROPERTIES requires at least one key=value pair"
                        .to_string(),
                );
            }
            let mut seen = std::collections::BTreeSet::new();
            for (key, _) in &properties {
                if !seen.insert(key.clone()) {
                    return Err(format!(
                        "duplicate key '{key}' in ALTER MATERIALIZED VIEW SET TBLPROPERTIES"
                    ));
                }
            }
            MvAlterAction::SetProperties(properties)
        }
        TypedAlterAction::PauseRefresh => MvAlterAction::PauseRefresh,
        TypedAlterAction::ResumeRefresh => MvAlterAction::ResumeRefresh,
        TypedAlterAction::Repartition(fields) => {
            MvAlterAction::Repartition(fields.iter().map(MvCreatePartitionField::from).collect())
        }
    };
    Ok(MvAlterStatement {
        name_parts: lower_typed_name_parts(&statement.name)?,
        action,
    })
}

fn lower_typed_refresh(
    statement: &novarocks_parser::ast::RefreshMaterializedView,
) -> Result<MvRefreshRequest, String> {
    if matches!(statement.mode, Some(MaterializedViewRefreshMode::Async)) {
        return Err(
            "REFRESH MATERIALIZED VIEW ... WITH ASYNC MODE is not supported yet".to_string(),
        );
    }
    Ok(MvRefreshRequest {
        name_parts: lower_typed_name_parts(&statement.name)?,
        full: statement.full,
    })
}

fn lower_typed_explain_level(
    level: MaterializedViewExplainLevel,
) -> novarocks_sql::compiler::ExplainLevel {
    match level {
        MaterializedViewExplainLevel::Default => novarocks_sql::compiler::ExplainLevel::Normal,
        MaterializedViewExplainLevel::Verbose => novarocks_sql::compiler::ExplainLevel::Verbose,
        MaterializedViewExplainLevel::Costs => novarocks_sql::compiler::ExplainLevel::Costs,
    }
}

fn lower_typed_show(
    statement: &novarocks_parser::ast::ShowMaterializedViews,
) -> Result<MvShowStatement, String> {
    let database = statement
        .database
        .as_ref()
        .map(|database| match database.parts.as_slice() {
            [database] => Ok(database.value.clone()),
            _ => Err("SHOW MATERIALIZED VIEWS FROM expects one database identifier".to_string()),
        })
        .transpose()?;
    Ok(MvShowStatement { database })
}

fn lower_typed_refresh_policy(
    policy: &TypedRefreshPolicy,
) -> Result<MvCreateRefreshPolicy, String> {
    match policy {
        TypedRefreshPolicy::Immediate => Err("REFRESH IMMEDIATE is not supported yet".to_string()),
        TypedRefreshPolicy::Manual { .. } => Ok(MvCreateRefreshPolicy::Manual),
        TypedRefreshPolicy::AsyncOnChange { .. } => Ok(MvCreateRefreshPolicy::AsyncOnChange),
        TypedRefreshPolicy::AsyncEvery { interval, unit, .. } => {
            let interval = lower_typed_u64(interval, "REFRESH ASYNC interval")?;
            if interval == 0 {
                return Err("REFRESH ASYNC interval must be positive".to_string());
            }
            let multiplier = match unit.value.to_ascii_uppercase().as_str() {
                "SECOND" | "SECONDS" => 1_000_u64,
                "MINUTE" | "MINUTES" => 60_000_u64,
                "HOUR" | "HOURS" => 3_600_000_u64,
                "DAY" | "DAYS" => 86_400_000_u64,
                _ => {
                    return Err(format!(
                        "unsupported REFRESH ASYNC interval unit `{}`; expected SECOND, MINUTE, HOUR, or DAY",
                        unit.value
                    ));
                }
            };
            let interval_ms = interval
                .checked_mul(multiplier)
                .ok_or_else(|| "REFRESH ASYNC interval is too large".to_string())?
                .try_into()
                .map_err(|_| "REFRESH ASYNC interval is too large".to_string())?;
            Ok(MvCreateRefreshPolicy::AsyncInterval { interval_ms })
        }
    }
}

fn lower_typed_partition_fields(
    fields: &[MaterializedViewPartitionField],
) -> Result<Vec<IcebergPartitionFieldExpr>, String> {
    fields.iter().map(lower_typed_partition_field).collect()
}

fn lower_typed_partition_field(
    field: &MaterializedViewPartitionField,
) -> Result<IcebergPartitionFieldExpr, String> {
    match field {
        MaterializedViewPartitionField::Identity(column) => {
            Ok(IcebergPartitionFieldExpr::Identity {
                column: normalize_identifier(&column.value)?,
            })
        }
        MaterializedViewPartitionField::Transform {
            name, arguments, ..
        } => {
            let transform = normalize_identifier(&name.value)?;
            let column = typed_partition_column(arguments, &transform)?;
            match transform.as_str() {
                "identity" => require_partition_argument_count(arguments, 1, "identity")
                    .map(|()| IcebergPartitionFieldExpr::Identity { column }),
                "year" => require_partition_argument_count(arguments, 1, "year")
                    .map(|()| IcebergPartitionFieldExpr::Year { column }),
                "month" => require_partition_argument_count(arguments, 1, "month")
                    .map(|()| IcebergPartitionFieldExpr::Month { column }),
                "day" => require_partition_argument_count(arguments, 1, "day")
                    .map(|()| IcebergPartitionFieldExpr::Day { column }),
                "hour" => require_partition_argument_count(arguments, 1, "hour")
                    .map(|()| IcebergPartitionFieldExpr::Hour { column }),
                "void" => require_partition_argument_count(arguments, 1, "void")
                    .map(|()| IcebergPartitionFieldExpr::Void { column }),
                "bucket" => Ok(IcebergPartitionFieldExpr::Bucket {
                    column,
                    num_buckets: typed_partition_positive_u32(arguments, "bucket count")?,
                }),
                "truncate" => Ok(IcebergPartitionFieldExpr::Truncate {
                    column,
                    width: typed_partition_positive_u32(arguments, "truncate width")?,
                }),
                _ => Err(format!(
                    "unsupported Iceberg partition transform `{}`",
                    name.value
                )),
            }
        }
    }
}

fn typed_partition_column(
    arguments: &[MaterializedViewPartitionArgument],
    transform: &str,
) -> Result<String, String> {
    let Some(MaterializedViewPartitionArgument::Ident(column)) = arguments.first() else {
        return Err(format!(
            "expected column argument for partition transform `{transform}`"
        ));
    };
    normalize_identifier(&column.value)
}

fn require_partition_argument_count(
    arguments: &[MaterializedViewPartitionArgument],
    expected: usize,
    transform: &str,
) -> Result<(), String> {
    if arguments.len() == expected {
        Ok(())
    } else if transform == "identity" {
        Err("identity() requires exactly one column argument: identity(column)".to_string())
    } else {
        Err(format!(
            "expected {expected} argument(s) for partition transform `{transform}`"
        ))
    }
}

fn typed_partition_positive_u32(
    arguments: &[MaterializedViewPartitionArgument],
    label: &str,
) -> Result<u32, String> {
    require_partition_argument_count(arguments, 2, label)?;
    let Some(MaterializedViewPartitionArgument::Literal(value)) = arguments.get(1) else {
        return Err(format!("expected numeric {label}"));
    };
    let value = lower_typed_u32(value, label)?;
    if value == 0 {
        return Err(format!("{label} must be positive"));
    }
    Ok(value)
}

fn lower_typed_properties(
    properties: &[novarocks_parser::ast::MaterializedViewProperty],
) -> Result<Vec<(String, String)>, String> {
    properties
        .iter()
        .map(|property| {
            Ok((
                lower_typed_string(&property.key, "MV property key")?,
                lower_typed_string(&property.value, "MV property value")?,
            ))
        })
        .collect()
}

fn lower_typed_name_parts(name: &TypedObjectName) -> Result<Vec<String>, String> {
    if name.parts.is_empty() {
        return Err("materialized view name must not be empty".to_string());
    }
    Ok(name.parts.iter().map(|part| part.value.clone()).collect())
}

fn lower_typed_u32(value: &Literal, context: &str) -> Result<u32, String> {
    lower_typed_u64(value, context)?
        .try_into()
        .map_err(|_| format!("{context} is too large"))
}

fn lower_typed_u64(value: &Literal, context: &str) -> Result<u64, String> {
    let LiteralKind::Number(value) = &value.kind else {
        return Err(format!("{context} expects a number"));
    };
    value
        .parse::<u64>()
        .map_err(|error| format!("invalid {context} `{value}`: {error}"))
}

fn lower_typed_string(value: &Literal, context: &str) -> Result<String, String> {
    let LiteralKind::String(value) = &value.kind else {
        return Err(format!("{context} expects a string"));
    };
    Ok(value.clone())
}

/// Readmit a target this process has admitted an operator's declaration for.
///
/// The provider observation happens here because the adapter owns the exact
/// connector generation; the declaration that permits it was already spent by
/// the management service.
impl crate::mv::domain::management_call::MvManagementOperations for MvCommandExecutor {
    fn readmit(
        &self,
        target: &crate::mv::domain::management_call::ManagementCallTarget,
        previous_incarnation: novarocks_mv_application::management::ProcessIncarnation,
        permits: Vec<novarocks_mv_application::management::ReadmissionPermit>,
    ) -> Result<(), String> {
        let entrance = self.ports.management_entrance()?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| format!("parse the readmitted MV catalog identity: {error}"))?;
        let lease = novarocks_spi::connector::ConnectorControlResolver::acquire_current(
            self.ports.connector_control(),
            &instance_id,
        )
        .map_err(|error| format!("acquire the readmitted MV catalog generation: {error}"))?;
        let catalog = lease
            .binding()
            .catalog_handle()
            .map_err(|error| format!("bind the readmitted MV catalog generation: {error}"))?
            .clone();
        let product_target = novarocks_mv_application::product::MvTarget::from_parts(
            Some(target.catalog.as_str()),
            &target.database,
            &target.name,
        );
        crate::mv::domain::staged_create::readmit_declared_target(
            entrance.as_ref(),
            self.ports.readiness().as_ref(),
            self.ports.connector_control(),
            catalog,
            product_target,
            uuid::Uuid::now_v7(),
            previous_incarnation,
            permits,
            // The old writer's effects are settled by declaration, not by this
            // process; observe under a scope that cannot be mistaken for part
            // of one of them.
            crate::connector::connector_request_context(
                None,
                novarocks_spi::connector::ConnectorStopOwner::new().view(),
            )?
            .after_external_effect(),
        )
    }
    /// Hand one target's ownership to another deployment.
    ///
    /// The observation the marker is replaced against has to come from the
    /// exact connector generation this adapter holds, which is why the whole
    /// handover runs here rather than in the management service: the service
    /// owns whether the statement may act, not what the target currently says.
    fn hand_over(
        &self,
        target: &crate::mv::domain::management_call::ManagementCallTarget,
        new_owner: novarocks_mv_application::management::DeploymentOwner,
    ) -> Result<crate::mv::domain::management_handover::MvOwnerHandoverOutcome, String> {
        let entrance = self.ports.management_entrance()?;
        let table = target.connector_table()?;
        let context = crate::connector::connector_request_context(
            None,
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
        )?
        .after_external_effect();
        let outcome = crate::mv::domain::management_handover::hand_over_managed_target(
            entrance.as_ref(),
            self.ports.connector_control(),
            target,
            &table,
            &new_owner,
            context.clone(),
        )?;
        let planning = crate::connector::acquire_metadata_planning_lease(
            self.ports.connector_control(),
            &target.catalog,
        )?;
        let catalog = planning
            .binding()
            .catalog_handle()
            .map_err(|e| e.to_string())?
            .clone();
        let product_target = novarocks_mv_application::product::MvTarget::from_parts(
            Some(&target.catalog),
            &target.database,
            &target.name,
        );
        if new_owner == *entrance.owner()
            && entrance.management_phase(&table)
                != novarocks_mv_application::management::MvManagementPhase::NotObserved
        {
            crate::mv::domain::staged_create::install_configured_current_projection(
                entrance.as_ref(),
                self.ports.readiness().as_ref(),
                self.ports.connector_control(),
                catalog,
                product_target,
                uuid::Uuid::now_v7(),
                context.after_external_effect(),
            )?;
        } else {
            crate::mv::domain::lake_rebuild::rebuild_one_lake_package_if_missing_verified(
                self.ports.readiness().as_ref(),
                self.ports.connector_control(),
                catalog,
                product_target,
                context.after_external_effect(),
            )?;
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::{
        ast::{MaterializedViewStatement, Statement},
        parse,
    };

    fn typed_mv(source: &str) -> MaterializedViewStatement {
        let statements = parse(source).expect("typed MV statement should parse");
        let [Statement::MaterializedView(statement)] = statements.as_slice() else {
            panic!("expected one typed MV statement for {source}");
        };
        statement.clone()
    }

    #[test]
    fn lowers_create_header_and_parses_only_embedded_query_slice() {
        let statement = typed_mv(
            "CREATE MATERIALIZED VIEW `analytics`.`orders_mv` \
             PARTITION BY (month(order_date)) \
             DISTRIBUTED BY HASH(order_id) BUCKETS 4 \
             REFRESH ASYNC EVERY INTERVAL 2 HOUR \
             PROPERTIES ('storage_engine' = 'iceberg') \
             AS SELECT order_id, order_date FROM ice.analytics.orders",
        );
        let MaterializedViewStatement::Create(create) = statement else {
            panic!("expected typed CREATE MATERIALIZED VIEW");
        };

        let lowered = lower_typed_create(&create).expect("typed create should lower");
        assert_eq!(lowered.name_parts, vec!["analytics", "orders_mv"]);
        assert_eq!(
            lowered.refresh_policy,
            MvCreateRefreshPolicy::AsyncInterval {
                interval_ms: 7_200_000
            }
        );
        assert!(matches!(
            lowered.partition_by.as_deref(),
            Some([MvCreatePartitionField::Month { column }]) if column == "order_date"
        ));
        assert_eq!(
            novarocks_parser::printer::print_query(&lowered.select_query),
            lowered.select_sql
        );
    }

    #[test]
    fn lowers_typed_alter_and_show_without_raw_command_parse() {
        let alter = typed_mv(
            "ALTER MATERIALIZED VIEW `analytics`.`orders_mv` \
             SET TBLPROPERTIES ('refresh_priority' = 'high')",
        );
        let MaterializedViewStatement::Alter(alter) = alter else {
            panic!("expected typed ALTER MATERIALIZED VIEW");
        };
        assert_eq!(
            lower_typed_alter(&alter)
                .expect("typed alter should lower")
                .action,
            MvAlterAction::SetProperties(vec![(
                "refresh_priority".to_string(),
                "high".to_string(),
            )])
        );

        let show = typed_mv("SHOW MATERIALIZED VIEWS FROM analytics");
        let MaterializedViewStatement::Show(show) = show else {
            panic!("expected typed SHOW MATERIALIZED VIEWS");
        };
        assert_eq!(
            lower_typed_show(&show).expect("typed show should lower"),
            MvShowStatement {
                database: Some("analytics".to_string())
            }
        );
    }

    #[test]
    fn typed_lowering_reapplies_legacy_mv_semantic_limits() {
        let immediate = typed_mv(
            "CREATE MATERIALIZED VIEW mv DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             REFRESH IMMEDIATE AS SELECT k1 FROM ice.db.source",
        );
        let MaterializedViewStatement::Create(immediate) = immediate else {
            panic!("expected typed CREATE MATERIALIZED VIEW");
        };
        assert!(
            lower_typed_create(&immediate)
                .expect_err("legacy MV create rejects immediate refresh")
                .contains("IMMEDIATE")
        );

        let refresh = typed_mv("REFRESH MATERIALIZED VIEW db.mv WITH ASYNC MODE");
        let MaterializedViewStatement::Refresh(refresh) = refresh else {
            panic!("expected typed REFRESH MATERIALIZED VIEW");
        };
        assert!(
            lower_typed_refresh(&refresh)
                .expect_err("legacy refresh rejects async mode")
                .contains("ASYNC")
        );
    }

    #[test]
    fn typed_explain_refresh_preserves_requested_presentation_level() {
        for (sql, expected) in [
            (
                "EXPLAIN REFRESH MATERIALIZED VIEW db.mv",
                novarocks_sql::compiler::ExplainLevel::Normal,
            ),
            (
                "EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW db.mv",
                novarocks_sql::compiler::ExplainLevel::Verbose,
            ),
            (
                "EXPLAIN COSTS REFRESH MATERIALIZED VIEW db.mv",
                novarocks_sql::compiler::ExplainLevel::Costs,
            ),
        ] {
            let statement = typed_mv(sql);
            let MaterializedViewStatement::ExplainRefresh(explain) = statement else {
                panic!("expected typed EXPLAIN REFRESH MATERIALIZED VIEW");
            };
            assert_eq!(lower_typed_explain_level(explain.level), expected);
        }
    }
}

#[cfg(test)]
mod refresh_refusal_projection_tests {
    use super::mv_application_command_error;
    use crate::mv::domain::application::MvApplicationErrorKind;
    use crate::query_execution::mv_assembly::refresh_preparation::async_facts::PreparationError;
    use novarocks_functions::{FunctionBindingError, FunctionOverloadId};
    use novarocks_query_application::api::CommandErrorKind;
    use novarocks_sql::{
        analyze_error::{AnalyzeError, AnalyzeErrorKind},
        compiler::SqlCompileError,
    };
    use novarocks_type_contract::CompileControlError;

    fn prepare_failure(
        error: SqlCompileError,
    ) -> crate::mv::domain::application::MvApplicationError {
        let failure = PreparationError::from(error).0;
        assert_eq!(failure.kind(), MvApplicationErrorKind::InvalidRequest);
        failure
    }

    #[test]
    fn refresh_prepare_step_missing_implementation_reaches_command_sql_code() {
        let compile = SqlCompileError::from(FunctionBindingError::UnavailableImplementation(
            FunctionOverloadId::try_new("fixture/mv/prepare-step/missing-pure-v1").unwrap(),
        ));
        let SqlCompileError::Analyze(expected) = &compile else {
            panic!("the binding owner must return typed analysis");
        };
        let expected_user = expected.to_user_error(None).to_string();
        let failure = prepare_failure(compile.clone());
        let analyze = failure
            .unavailable_implementation()
            .expect("preparation must keep the typed analysis cause");
        assert_eq!(analyze, expected);
        assert_eq!(analyze.kind(), AnalyzeErrorKind::UnavailableImplementation);
        assert_eq!(
            analyze.code().as_str(),
            "sql.analyze.unavailable_implementation"
        );
        let user = mv_application_command_error(failure);
        assert_eq!(user.kind(), CommandErrorKind::Failed);
        assert_eq!(user.compile_control_error(), None);
        assert_eq!(user.to_string(), expected_user);
    }

    #[test]
    fn refresh_prepare_step_source_reaches_command_without_reauthoring() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        for original in [
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability("router".into()),
            novarocks_plan_codec::PhysicalEncodeError::Invalid("unsupported diagnostic".into()),
        ] {
            let source = SharedFailureCause::new(original);
            let fact = source.failure_fact();
            let failure = prepare_failure(SqlCompileError::CompilationSource(source.clone()));
            assert_eq!(failure.unavailable_implementation(), None);
            assert_eq!(failure.original_failure(), Some(&source));
            let actual = mv_application_command_error(failure.clone());
            assert_eq!(actual.kind(), CommandErrorKind::Failed);
            assert_eq!(actual.failure_cause(), Some(&source));
            assert_eq!(actual.failure_cause().unwrap().failure_fact(), fact);
            assert_eq!(actual.execution_failure().unwrap().fact(), fact);
            assert_eq!(
                actual.compile_control_error(),
                Some(CompileControlError::Source(source.clone()))
            );
            assert_eq!(
                actual.to_string(),
                actual.execution_failure().unwrap().user_error().to_string()
            );
        }
    }

    #[test]
    fn refresh_prepare_step_diagnostic_text_does_not_gain_refusal_identity() {
        let message = "unsupported diagnostic: UnavailableImplementation UnsupportedCompiledShape";
        for compile in [
            SqlCompileError::Analyze(AnalyzeError::internal(message)),
            SqlCompileError::Compilation(message.into()),
        ] {
            let failure = prepare_failure(compile);
            assert_eq!(failure.unavailable_implementation(), None);
            let user = mv_application_command_error(failure);
            assert_eq!(user.failure_cause(), None);
            assert_eq!(user.execution_failure(), None);
            assert_eq!(user.user_error(), None);
            assert_eq!(user.kind(), CommandErrorKind::Failed);
            assert_eq!(user.compile_control_error(), None);
            assert_eq!(user.to_string(), message);
        }
    }

    #[test]
    fn refresh_prepare_step_control_keeps_command_cause_and_priority() {
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for compile in [
                SqlCompileError::from(control.clone()),
                SqlCompileError::Analyze(AnalyzeError::control(control.clone())),
            ] {
                let failure = prepare_failure(compile);
                assert_eq!(failure.compile_control_error(), Some(control.clone()));
                assert_eq!(failure.unavailable_implementation(), None);
                let user = mv_application_command_error(failure);
                assert_eq!(user.compile_control_error(), Some(control.clone()));
                assert_eq!(user.to_string(), control.to_string());
                assert_eq!(
                    user.kind(),
                    if control == CompileControlError::Cancelled {
                        CommandErrorKind::Cancelled
                    } else {
                        CommandErrorKind::Failed
                    }
                );
            }
            let missing = SqlCompileError::from(FunctionBindingError::UnavailableImplementation(
                FunctionOverloadId::try_new("fixture/mv/controlled/missing-pure-v1").unwrap(),
            ));
            let failure = prepare_failure(missing).with_compile_control(Some(control.clone()));
            assert!(failure.unavailable_implementation().is_some());
            let user = mv_application_command_error(failure);
            assert_eq!(user.compile_control_error(), Some(control.clone()));
            assert_eq!(user.to_string(), control.to_string());
        }
    }
}

#[cfg(test)]
mod original_publication_effect_tests {
    use super::*;
    use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
    use novarocks_query_application::api::StatementExternalEffectDisposition;

    #[test]
    fn original_publication_fact_survives_the_independent_provider_failure() {
        let source = SharedFailureCause::new(novarocks_workload_control::WorkError::Capacity(
            "original provider operation failed",
        ));
        let original_fact = source.failure_fact();
        for (kind, effect) in [
            (
                MvApplicationErrorKind::CommitUnknown,
                Some(StatementExternalEffectDisposition::CommitUnknown),
            ),
            (
                MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                Some(StatementExternalEffectDisposition::KnownCommitted),
            ),
            (MvApplicationErrorKind::Engine, None),
            (MvApplicationErrorKind::InvalidRequest, None),
        ] {
            let command = mv_application_command_error(
                MvApplicationError::new(kind, "MV operation failed")
                    .with_original_failure(source.clone()),
            );
            assert_eq!(command.failure_cause(), Some(&source));
            assert_eq!(
                command.failure_cause().unwrap().failure_fact(),
                original_fact
            );
            assert_eq!(command.compile_control_error(), None);
            assert_eq!(command.external_effect_disposition(), effect);
            if let Some(effect) = effect {
                assert!(effect.do_not_retry());
            }
        }
        // The publication fact is independent of whether this older error
        // already carries a typed cause, so a legacy diagnostic keeps it too.
        for (kind, effect) in [
            (
                MvApplicationErrorKind::CommitUnknown,
                StatementExternalEffectDisposition::CommitUnknown,
            ),
            (
                MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                StatementExternalEffectDisposition::KnownCommitted,
            ),
        ] {
            let command =
                mv_application_command_error(MvApplicationError::new(kind, "publication failed"));
            assert_eq!(command.external_effect_disposition(), Some(effect));
            assert_eq!(command.failure_cause(), None);
        }
    }
}
