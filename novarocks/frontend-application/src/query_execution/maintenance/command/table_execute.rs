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

//! Bind an EXECUTE statement once and hand off to the original owners.

use std::time::SystemTime;

use super::*;
use crate::catalog_application::command::{IcebergTableMutation, PreparedIcebergTableMutation};
use crate::catalog_application::statement::{ColumnPath, connector_schema_path};
use crate::connector::metadata_maintenance::MetadataMaintenanceIntent;
use crate::mv::domain::ref_flow::{self, PreparedRefMutation, TrinoTagAction};
use crate::table_maintenance::{ParsedMaintenanceAction, ParsedMaintenanceStatement};
use novarocks_parser::{Span, ast::TrinoTableExecute};
use novarocks_query_application::{
    admitted_query_context::QueryExecutionContext,
    api::CommandErrorKind,
    session_control::GovernedStatementAction,
    session_error::{QueryServiceError, QueryServiceErrorKind},
    sql::{
        lower_object_name,
        property_evaluation::DdlPropertyConstantPort,
        table_procedure::{
            BoundProcedureArgument, BoundProcedureValue, BoundTableProcedure,
            ProcedureArgumentDefault, ProcedureRetentionDuration, TableProcedureKind,
            invalid_procedure_argument,
        },
        trino_table_command::bind_trino_table_execute,
    },
};
use novarocks_result_contract::ClientPresentationFamily;
use novarocks_spi::connector::{
    ConnectorColumnPosition, ConnectorControlPlanningPin, ConnectorError, ConnectorErrorKind,
    ConnectorExpireSnapshotsExecute, ConnectorInstanceId, ConnectorMaintenanceDuration,
    ConnectorMaintenanceRetention, ConnectorPropertyChange, ConnectorProviderBindingKey,
    ConnectorRequestContext, ConnectorSchemaChange, DdlPropertyValue,
};
use novarocks_table_maintenance::MaintenanceTarget;
use novarocks_type_contract::PureCompileControl;

pub(crate) enum PreparedTableExecute {
    Action(Box<dyn GovernedStatementAction>),
    Metadata(MaintenanceCommandCall),
    AddFiles(crate::query_execution::dml::add_files::ResolvedAddFiles),
    Mutation(PreparedTableProcedureMutation),
    #[cfg(novarocks_dev_controls)]
    EqualityDelete(crate::query_execution::dml::delete::OwnedDeleteStatement),
}

pub(crate) enum PreparedTableProcedureMutation {
    Table(PreparedIcebergTableMutation),
    Reference(PreparedRefMutation),
}

impl PreparedTableProcedureMutation {
    pub(crate) fn execute(
        self,
        connector: &ConnectorRequestContext,
    ) -> Result<StatementResult, CommandError> {
        match self {
            Self::Table(mutation) => mutation.execute(connector),
            Self::Reference(mutation) => mutation.execute(connector),
        }
    }
}

pub(super) struct MetadataPreparation {
    pub(super) target: MaintenanceTarget,
    pub(super) planning: ConnectorControlPlanningPin,
    pub(super) intent: MetadataMaintenanceIntent,
}

impl MaintenanceCommandExecutor {
    /// Called in the original statement's preparation, with its own constant
    /// interpreter on the admitted CPU and the already-frozen request facts.
    pub(crate) fn prepare_trino_table_execute(
        &self,
        source: &str,
        statement: &Arc<TrinoTableExecute>,
        context: MaintenanceRequestContext<'_>,
        execution: &QueryExecutionContext,
        connector: &ConnectorRequestContext,
        statement_started_at: SystemTime,
        constants: &mut dyn DdlPropertyConstantPort,
        family: ClientPresentationFamily,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Result<PreparedTableExecute, CommandError> {
        let target = crate::catalog_application::resolver::resolve_existing_table_target(
            &self.kernel,
            &lower_object_name(&statement.table.name),
            context.current_catalog,
            context.current_database,
        )
        .map_err(maintenance_target_error)?;
        let instance = ConnectorInstanceId::parse(&target.catalog)
            .map_err(TerminalError::from_original_cause)
            .map_err(crate::query::maintenance_command_error)?;
        let planning = self
            .kernel
            .connector_control()
            .acquire_current(&instance)
            .map_err(TerminalError::from_original_cause)
            .map_err(crate::query::maintenance_command_error)?;
        let descriptor = planning.binding().descriptor();
        let provider = &descriptor.provider_id;
        if provider != &target.provider_id || descriptor.instance_id != instance {
            return Err(CommandError::new(
                CommandErrorKind::Conflict,
                "Table procedure catalog admission does not match its retained generation",
            ));
        }
        let control =
            crate::query_execution::planning::sql_compile_control_from_execution(execution);
        let bound = bind_trino_table_execute(
            source,
            statement,
            provider,
            constants,
            &target.catalog,
            &control,
        )
        .map_err(crate::query::procedure_command_error)?;
        #[cfg(novarocks_dev_controls)]
        if bound.declaration().kind() == TableProcedureKind::AddEqualityDelete {
            use crate::query_execution::dml::delete::{
                OwnedDeleteStatement, ResolvedEqualityDeleteTarget,
                equality::PreparedEqualityDelete,
            };
            let statement = PreparedEqualityDelete::from_bound(source, statement, bound, &control)
                .map_err(crate::query::procedure_command_error)?;
            return Ok(PreparedTableExecute::EqualityDelete(
                OwnedDeleteStatement::TrinoEquality {
                    statement,
                    resolved: ResolvedEqualityDeleteTarget { target, planning },
                },
            ));
        }
        let procedure = project_bound(
            source,
            statement.span,
            bound,
            statement_started_at,
            &control,
            |keys, if_exists| {
                planning
                    .binding()
                    .mutation()
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::Unsupported,
                            "table procedure requires this catalog generation's mutation capability",
                        )
                    })?
                    .prepare_extra_property_unsets(keys, if_exists)
            },
        )
        .map_err(crate::query::procedure_command_error)?;

        if let Procedure::AddFiles(arguments) = procedure {
            return Ok(PreparedTableExecute::AddFiles(
                crate::query_execution::dml::add_files::ResolvedAddFiles {
                    target,
                    planning,
                    arguments,
                },
            ));
        }

        if let Procedure::CatalogMutation(mutation) = procedure {
            return Ok(PreparedTableExecute::Mutation(
                PreparedTableProcedureMutation::Table(PreparedIcebergTableMutation {
                    target,
                    instance_id: instance,
                    planning,
                    mutation,
                    catalog_service: self.kernel.catalog_service().clone(),
                    storage_observation: self.kernel.mv_storage_observation().clone(),
                    readiness: self.kernel.mv_readiness().clone(),
                }),
            ));
        }

        if let Procedure::Tag(action) = procedure {
            let prepared = ref_flow::prepare_resolved_tag(
                self.kernel.mv_storage_observation().as_ref(),
                self.kernel.mv_readiness().as_ref(),
                &target,
                planning,
                action,
                connector,
            )?;
            return Ok(PreparedTableExecute::Mutation(
                PreparedTableProcedureMutation::Reference(prepared),
            ));
        }

        let target = MaintenanceTarget {
            catalog: target.catalog,
            namespace: target.namespace,
            table: target.table,
        };
        match procedure {
            Procedure::Optimize => super::super::rewrite_job::direct::prepare_resolved_optimize(
                &self.kernel,
                RequestScopedMaintenanceEngine::new(
                    self.kernel.clone(),
                    execution.clone(),
                    connector.clone(),
                ),
                target,
                Some(&planning),
                execution,
                family,
                #[cfg(novarocks_dev_controls)]
                observation,
            )
            .map(PreparedTableExecute::Action)
            .map_err(crate::query::maintenance_command_error),
            Procedure::RemoveOrphanFiles {
                older_than_ms,
                duration,
            } => {
                use novarocks_spi::connector::REMOVE_ORPHAN_FILES_MIN_RETENTION_SESSION_PROPERTY;
                let retention = frozen_retention(
                    execution,
                    &planning,
                    duration,
                    REMOVE_ORPHAN_FILES_MIN_RETENTION_SESSION_PROPERTY,
                )?;
                super::super::cleanup_action::prepare(
                    &self.kernel,
                    target,
                    older_than_ms,
                    super::super::cleanup_action::CleanupEntry::TableExecute(retention),
                    planning,
                    execution,
                    connector,
                    family,
                )
                .map(PreparedTableExecute::Action)
                .map_err(crate::query::maintenance_command_error)
            }
            Procedure::RewriteManifests => self.prepare_bound_metadata(
                context,
                execution,
                connector,
                target,
                planning,
                ParsedMaintenanceAction::RewriteManifests {
                    use_caching: None,
                    spec_id: None,
                },
                MetadataMaintenanceIntent::TrinoOptimizeManifests,
            ),
            Procedure::ExpireSnapshots {
                older_than_ms,
                retain_last,
                duration,
                clean_expired_metadata,
            } => {
                use novarocks_spi::connector::EXPIRE_SNAPSHOTS_MIN_RETENTION_SESSION_PROPERTY;
                let retention = frozen_retention(
                    execution,
                    &planning,
                    duration,
                    EXPIRE_SNAPSHOTS_MIN_RETENTION_SESSION_PROPERTY,
                )?;
                self.prepare_bound_metadata(
                    context,
                    execution,
                    connector,
                    target,
                    planning,
                    ParsedMaintenanceAction::ExpireSnapshots {
                        older_than_ms: Some(older_than_ms),
                        retain_last,
                    },
                    MetadataMaintenanceIntent::TrinoExpireSnapshots {
                        older_than_ms,
                        retain_last,
                        inputs: ConnectorExpireSnapshotsExecute {
                            retention,
                            clean_expired_metadata,
                        },
                    },
                )
            }
            Procedure::AddFiles(_) => unreachable!("ADD FILES has its original DML owner"),
            Procedure::CatalogMutation(_) => {
                unreachable!("table mutations have their catalog owner")
            }
            Procedure::Tag(_) => unreachable!("tags have their reference mutation owner"),
        }
    }

    fn prepare_bound_metadata(
        &self,
        context: MaintenanceRequestContext<'_>,
        execution: &QueryExecutionContext,
        connector: &ConnectorRequestContext,
        target: MaintenanceTarget,
        planning: ConnectorControlPlanningPin,
        action: ParsedMaintenanceAction,
        intent: MetadataMaintenanceIntent,
    ) -> Result<PreparedTableExecute, CommandError> {
        let lease = self
            .kernel
            .connector_control()
            .acquire_exact_metadata_maintenance(planning.control_runtime_id())
            .map_err(TerminalError::from_original_cause)
            .map_err(crate::query::maintenance_command_error)?;
        let name_parts = vec![
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
        ];
        self.prepare_metadata_source(
            ParsedMaintenanceStatement { name_parts, action },
            false,
            context,
            execution,
            connector,
            lease,
            Some(MetadataPreparation {
                target,
                planning,
                intent,
            }),
        )
        .map(PreparedTableExecute::Metadata)
    }
}

#[derive(Debug, PartialEq)]
enum Procedure {
    Optimize,
    RewriteManifests,
    ExpireSnapshots {
        older_than_ms: i64,
        retain_last: Option<u32>,
        duration: ConnectorMaintenanceDuration,
        clean_expired_metadata: bool,
    },
    RemoveOrphanFiles {
        older_than_ms: i64,
        duration: ConnectorMaintenanceDuration,
    },
    AddFiles(crate::query_execution::dml::add_files::AddFilesArguments),
    CatalogMutation(IcebergTableMutation),
    Tag(TrinoTagAction),
}

fn project_schema_change(
    source: &str,
    span: Span,
    bound: BoundTableProcedure<'_>,
) -> Result<ConnectorSchemaChange, QueryServiceError> {
    let kind = bound.declaration().kind();
    let mut arguments = bound.into_arguments();
    let mut next_text = || match arguments.next().map(|(_, argument)| argument) {
        Some(BoundProcedureArgument::Present {
            value: BoundProcedureValue::Value(DdlPropertyValue::Varchar(text)),
            span: argument_span,
        }) => Ok(Some((text, argument_span.unwrap_or(span)))),
        Some(BoundProcedureArgument::Omitted | BoundProcedureArgument::ExplicitDefault(_)) => {
            Ok(None)
        }
        _ => Err(invariant()),
    };
    let invalid = |location, message: &str| invalid_procedure_argument(source, location, message);
    let (column, column_span) = next_text()?.ok_or_else(|| invalid(span, "Column is required"))?;
    let path = ColumnPath::parse(&column).map_err(|error| invalid(column_span, &error))?;
    let path = connector_schema_path(path);
    if kind == TableProcedureKind::SetColumnNotNull {
        return Ok(ConnectorSchemaChange::SetColumnNullability {
            path,
            nullable: false,
        });
    }

    let (position, position_span) =
        next_text()?.ok_or_else(|| invalid(span, "Position is required"))?;
    let reference = next_text()?;
    let position = match (position.to_ascii_uppercase().as_str(), reference) {
        ("FIRST", None) => ConnectorColumnPosition::First,
        ("FIRST", Some((_, reference_span))) => {
            return Err(invalid(
                reference_span,
                "FIRST does not take a reference column",
            ));
        }
        (order @ ("AFTER" | "BEFORE"), Some((reference, reference_span))) => {
            let reference =
                ColumnPath::parse(&reference).map_err(|error| invalid(reference_span, &error))?;
            // The existing schema mutation selects the reference leaf in the
            // moved column's parent. The authored path is validated first.
            let column = Arc::from(reference.last().ok_or_else(invariant)?);
            if order == "AFTER" {
                ConnectorColumnPosition::After { column }
            } else {
                ConnectorColumnPosition::Before { column }
            }
        }
        ("AFTER" | "BEFORE", None) => {
            return Err(invalid(span, "AFTER and BEFORE require a reference column"));
        }
        _ => {
            return Err(invalid(
                position_span,
                "Position must be FIRST, AFTER, or BEFORE",
            ));
        }
    };
    Ok(ConnectorSchemaChange::ReorderColumn { path, position })
}

fn project_property_unsets(
    source: &str,
    span: Span,
    bound: BoundTableProcedure<'_>,
    control: &dyn PureCompileControl,
    prepare: impl FnOnce(&[String], bool) -> Result<Vec<ConnectorPropertyChange>, ConnectorError>,
) -> Result<Vec<ConnectorPropertyChange>, QueryServiceError> {
    let mut arguments = bound.into_arguments();
    let Some((
        _,
        BoundProcedureArgument::Present {
            value: BoundProcedureValue::Value(DdlPropertyValue::ArrayVarchar(keys)),
            span: key_span,
        },
    )) = arguments.next()
    else {
        return Err(invalid_procedure_argument(
            source,
            span,
            "A keys array is required",
        ));
    };
    let key_span = key_span.unwrap_or(span);
    if keys.is_empty() {
        return Err(invalid_procedure_argument(
            source,
            key_span,
            "Keys must not be empty",
        ));
    }
    let mut non_null_keys = Vec::with_capacity(keys.len());
    for key in keys {
        control
            .checkpoint(novarocks_type_contract::CompilePhase::Validate, 1)
            .map_err(QueryServiceError::from_compile_control)?;
        non_null_keys.push(key.ok_or_else(|| {
            invalid_procedure_argument(source, key_span, "Keys must not contain NULL")
        })?);
    }
    let Some((slot, argument)) = arguments.next() else {
        return Err(invariant());
    };
    let if_exists = match argument {
        BoundProcedureArgument::Present {
            value: BoundProcedureValue::Value(DdlPropertyValue::Boolean(value)),
            ..
        } => value,
        BoundProcedureArgument::Omitted | BoundProcedureArgument::ExplicitDefault(_) => {
            let Some(ProcedureArgumentDefault::Boolean(value)) = slot.default else {
                return Err(invariant());
            };
            value
        }
        BoundProcedureArgument::Present {
            span: argument_span,
            ..
        } => {
            return Err(invalid_procedure_argument(
                source,
                argument_span.unwrap_or(span),
                "If_exists requires a non-null BOOLEAN",
            ));
        }
    };
    prepare(&non_null_keys, if_exists).map_err(|error| {
        if error.kind() == ConnectorErrorKind::InvalidRequest {
            invalid_procedure_argument(source, key_span, error.to_string())
        } else {
            QueryServiceError::from_failure_cause(
                novarocks_failure_contract::SharedFailureCause::new(error),
            )
        }
    })
}

fn project_bound(
    source: &str,
    span: Span,
    bound: BoundTableProcedure<'_>,
    statement_started_at: SystemTime,
    control: &dyn PureCompileControl,
    prepare_unsets: impl FnOnce(&[String], bool) -> Result<Vec<ConnectorPropertyChange>, ConnectorError>,
) -> Result<Procedure, QueryServiceError> {
    let kind = bound.declaration().kind();
    match kind {
        TableProcedureKind::Optimize => return Ok(Procedure::Optimize),
        TableProcedureKind::OptimizeManifests => return Ok(Procedure::RewriteManifests),
        TableProcedureKind::AddFiles => {
            use crate::query_execution::dml::add_files::AddFilesArguments;

            let mut arguments = AddFilesArguments::default();
            for (slot, value) in bound.into_arguments() {
                match (slot.name, value) {
                    (
                        "location",
                        BoundProcedureArgument::Present {
                            value: BoundProcedureValue::Value(DdlPropertyValue::Varchar(location)),
                            ..
                        },
                    ) => arguments.location = Some(location.into()),
                    (
                        "format",
                        BoundProcedureArgument::Present {
                            value: BoundProcedureValue::AddFilesFormat(format),
                            ..
                        },
                    ) => arguments.format = Some(format),
                    (
                        "recursive_directory",
                        BoundProcedureArgument::Present {
                            value: BoundProcedureValue::AddFilesRecursion(recursion),
                            ..
                        },
                    ) => arguments.recursion = recursion,
                    (
                        _,
                        BoundProcedureArgument::Omitted
                        | BoundProcedureArgument::ExplicitDefault(_),
                    ) => {}
                    _ => return Err(invariant()),
                }
            }
            return Ok(Procedure::AddFiles(arguments));
        }
        TableProcedureKind::MoveColumn | TableProcedureKind::SetColumnNotNull => {
            return project_schema_change(source, span, bound)
                .map(IcebergTableMutation::Schema)
                .map(Procedure::CatalogMutation);
        }
        TableProcedureKind::UnsetExtraProperties => {
            return project_property_unsets(source, span, bound, control, prepare_unsets)
                .map(IcebergTableMutation::UnsetProperties)
                .map(Procedure::CatalogMutation);
        }
        TableProcedureKind::CreateTag | TableProcedureKind::DropTag => {
            return ref_flow::project_trino_tag(source, span, bound).map(Procedure::Tag);
        }
        TableProcedureKind::ExpireSnapshots | TableProcedureKind::RemoveOrphanFiles => {}
        #[cfg(novarocks_dev_controls)]
        TableProcedureKind::AddEqualityDelete => return Err(invariant()),
    }

    let mut retention = None;
    let mut retain_last = None;
    let mut clean_expired_metadata = false;
    for (slot, argument) in bound.into_arguments() {
        match (slot.name, argument) {
            (
                "retention_threshold",
                BoundProcedureArgument::Present {
                    value: BoundProcedureValue::RetentionDuration(duration),
                    span: Some(value_span),
                },
            ) => retention = Some((duration, value_span)),
            ("retention_threshold", default_argument) => {
                let Some(ProcedureArgumentDefault::Varchar(default)) = slot.default else {
                    return Err(invariant());
                };
                let value_span = match default_argument {
                    BoundProcedureArgument::Omitted => span,
                    BoundProcedureArgument::ExplicitDefault(value_span) => value_span,
                    _ => return Err(invariant()),
                };
                retention = Some((
                    ProcedureRetentionDuration::parse(source, value_span, default, control)?,
                    value_span,
                ));
            }
            (_, BoundProcedureArgument::Omitted | BoundProcedureArgument::ExplicitDefault(_)) => {}
            (
                "retain_last",
                BoundProcedureArgument::Present {
                    value: BoundProcedureValue::Value(DdlPropertyValue::Integer(value)),
                    ..
                },
            ) => retain_last = Some(u32::try_from(value).map_err(|_| invariant())?),
            (
                "clean_expired_metadata",
                BoundProcedureArgument::Present {
                    value: BoundProcedureValue::Value(DdlPropertyValue::Boolean(value)),
                    ..
                },
            ) => clean_expired_metadata = value,
            _ => return Err(invariant()),
        }
    }
    let (retention, value_span) = retention.ok_or_else(invariant)?;
    let older_than_ms =
        relative_cutoff(source, value_span, &retention, statement_started_at, kind)?;
    let duration = retention.connector_duration();
    match kind {
        TableProcedureKind::ExpireSnapshots => Ok(Procedure::ExpireSnapshots {
            older_than_ms,
            retain_last,
            duration,
            clean_expired_metadata,
        }),
        TableProcedureKind::RemoveOrphanFiles => Ok(Procedure::RemoveOrphanFiles {
            older_than_ms,
            duration,
        }),
        _ => Err(invariant()),
    }
}

fn frozen_retention(
    execution: &QueryExecutionContext,
    planning: &ConnectorControlPlanningPin,
    requested: ConnectorMaintenanceDuration,
    property: &str,
) -> Result<ConnectorMaintenanceRetention, CommandError> {
    use novarocks_query_application::sql::session::registry::SessionPropertyValue;
    let binding = planning.binding();
    let key = ConnectorProviderBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    let session_minimum = match execution
        .session_property_inputs()
        .read_catalog_override_for_binding(&key, property)
    {
        None => None,
        Some(SessionPropertyValue::Varchar(value)) => Some(
            ConnectorMaintenanceDuration::parse(value)
                .ok_or_else(|| crate::query::procedure_command_error(invariant()))?,
        ),
        Some(_) => return Err(crate::query::procedure_command_error(invariant())),
    };
    Ok(ConnectorMaintenanceRetention {
        requested,
        session_minimum,
    })
}

fn relative_cutoff(
    source: &str,
    span: Span,
    duration: &ProcedureRetentionDuration,
    statement_started_at: SystemTime,
    procedure: TableProcedureKind,
) -> Result<i64, QueryServiceError> {
    let millis = duration.rounded_millis().ok_or_else(|| {
        invalid_procedure_argument(
            source,
            span,
            "Table procedure retention_threshold is too large",
        )
    })?;
    let started_ms = statement_started_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|started| i64::try_from(started.as_millis()).ok())
        .ok_or_else(|| {
            QueryServiceError::new(
                QueryServiceErrorKind::Internal,
                "Statement start time cannot be represented as Unix epoch milliseconds",
            )
        })?;
    let cutoff_ms = started_ms
        .checked_sub(millis)
        .filter(|cutoff| match procedure {
            TableProcedureKind::ExpireSnapshots => *cutoff >= 0,
            TableProcedureKind::RemoveOrphanFiles => *cutoff > 0,
            _ => unreachable!("only the retention procedures have a relative cutoff"),
        });
    cutoff_ms.ok_or_else(|| {
        invalid_procedure_argument(
            source,
            span,
            "Table procedure retention_threshold produces a cutoff outside the supported timestamp range",
        )
    })
}

fn invariant() -> QueryServiceError {
    QueryServiceError::new(
        QueryServiceErrorKind::Internal,
        "Table procedure value differs from its declaration",
    )
}

#[cfg(test)]
mod tests;
