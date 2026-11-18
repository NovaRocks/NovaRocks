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
use crate::connector::metadata_maintenance::MetadataMaintenanceIntent;
use crate::table_maintenance::{ParsedMaintenanceAction, ParsedMaintenanceStatement};
use novarocks_parser::{Span, ast::TrinoTableExecute};
use novarocks_query_application::{
    admitted_query_context::QueryExecutionContext,
    session_control::GovernedStatementAction,
    session_error::{QueryServiceError, QueryServiceErrorKind},
    sql::{
        lower_object_name,
        property_evaluation::DdlPropertyConstantPort,
        table_procedure::{
            BoundTableProcedure, ProcedureArgumentDefault, ProcedureRetentionDuration,
            TableProcedureAbsoluteTimePort, invalid_procedure_argument,
        },
        trino_admission::unsupported_statement,
        trino_table_command::bind_trino_table_execute,
    },
};
use novarocks_result_contract::ClientPresentationFamily;
use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorInstanceId, ConnectorRequestContext, DdlPropertyValue,
};
use novarocks_table_maintenance::MaintenanceTarget;
use novarocks_type_contract::PureCompileControl;

pub(crate) enum PreparedTableExecute {
    Action(Box<dyn GovernedStatementAction>),
    Metadata(MaintenanceCommandCall),
    AddFiles(crate::query_execution::dml::add_files::ResolvedAddFiles),
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
        statement: &TrinoTableExecute,
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
        .map_err(legacy_target_error)?;
        let instance = ConnectorInstanceId::parse(&target.catalog)
            .map_err(TerminalError::from_original_cause)
            .map_err(crate::query::maintenance_command_error)?;
        let planning = self
            .kernel
            .connector_control()
            .acquire_current(&instance)
            .map_err(TerminalError::from_original_cause)
            .map_err(crate::query::maintenance_command_error)?;
        let provider = &planning.binding().descriptor().provider_id;
        if provider != &target.provider_id {
            return Err(CommandError::new(
                CommandErrorKind::Conflict,
                "Table procedure catalog admission and retained generation selected different providers",
            ));
        }
        let control =
            crate::query_execution::planning::sql_compile_control_from_execution(execution);
        let mut absolute_time = OriginalTimestamp { source };
        let bound = bind_trino_table_execute(
            source,
            statement,
            provider,
            constants,
            &mut absolute_time,
            &control,
        )
        .map_err(crate::query::procedure_command_error)?;
        let procedure = project_bound(
            source,
            statement.span,
            bound,
            statement_started_at,
            &control,
        )
        .map_err(crate::query::procedure_command_error)?;

        if let Procedure::AddFiles(location) = procedure {
            return Ok(PreparedTableExecute::AddFiles(
                crate::query_execution::dml::add_files::ResolvedAddFiles {
                    target,
                    planning,
                    location,
                },
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
            Procedure::RemoveOrphanFiles(older_than_ms) => super::super::cleanup_action::prepare(
                &self.kernel,
                target,
                older_than_ms,
                Some(planning),
                false,
                execution,
                connector,
                family,
            )
            .map(PreparedTableExecute::Action)
            .map_err(crate::query::maintenance_command_error),
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
            ),
            Procedure::ExpireSnapshots {
                older_than_ms,
                retain_last,
            } => self.prepare_bound_metadata(
                context,
                execution,
                connector,
                target,
                planning,
                ParsedMaintenanceAction::ExpireSnapshots {
                    older_than_ms,
                    retain_last,
                },
            ),
            Procedure::AddFiles(_) => unreachable!("ADD FILES has its original DML owner"),
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
    ) -> Result<PreparedTableExecute, CommandError> {
        let intent = match &action {
            ParsedMaintenanceAction::RewriteManifests {
                use_caching: None,
                spec_id: None,
            } => MetadataMaintenanceIntent::rewrite_metadata_layout(),
            ParsedMaintenanceAction::ExpireSnapshots {
                older_than_ms,
                retain_last,
            } => MetadataMaintenanceIntent::expire_table_versions(*older_than_ms, *retain_last),
            _ => return Err(crate::query::procedure_command_error(invariant())),
        };
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
            ParsedMaintenanceStatement::Execute { name_parts, action },
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
        older_than_ms: Option<i64>,
        retain_last: Option<u32>,
    },
    RemoveOrphanFiles(i64),
    AddFiles(String),
}

fn project_bound(
    source: &str,
    span: Span,
    bound: BoundTableProcedure<'_>,
    statement_started_at: SystemTime,
    control: &dyn PureCompileControl,
) -> Result<Procedure, QueryServiceError> {
    let kind = bound.declaration().kind();
    match kind {
        TableProcedureKind::Optimize => return Ok(Procedure::Optimize),
        TableProcedureKind::OptimizeManifests => return Ok(Procedure::RewriteManifests),
        TableProcedureKind::AddFiles => {
            for (slot, value) in bound.into_arguments() {
                if slot.name == "location" {
                    return match value {
                        BoundProcedureArgument::Present {
                            value: BoundProcedureValue::Value(DdlPropertyValue::Varchar(location)),
                            span: actual_span,
                        } => {
                            if location.is_empty() {
                                return Err(invalid_procedure_argument(
                                    source,
                                    actual_span.unwrap_or(span),
                                    "Table procedure ADD_FILES requires a non-empty location",
                                ));
                            }
                            Ok(Procedure::AddFiles(location))
                        }
                        BoundProcedureArgument::Omitted
                        | BoundProcedureArgument::ExplicitDefault(_) => {
                            Err(invalid_procedure_argument(
                                source,
                                span,
                                "Table procedure ADD_FILES requires 'location'",
                            ))
                        }
                        _ => Err(invariant()),
                    };
                }
            }
            return Err(invariant());
        }
        TableProcedureKind::ExpireSnapshots | TableProcedureKind::RemoveOrphanFiles => {}
        _ => {
            return Err(unsupported_statement(
                source,
                span,
                bound.declaration().name(),
            ));
        }
    }

    let mut older_than_ms = None;
    let mut retain_last = None;
    let mut retention_default = None;
    for (slot, argument) in bound.into_arguments() {
        if slot.name == "retention_threshold" {
            let Some(ProcedureArgumentDefault::Varchar(default)) = slot.default else {
                return Err(invariant());
            };
            retention_default = Some(default);
            let (duration, value_span) = match argument {
                BoundProcedureArgument::Omitted => continue,
                BoundProcedureArgument::ExplicitDefault(span) => (
                    ProcedureRetentionDuration::parse(source, span, default, control)?,
                    span,
                ),
                BoundProcedureArgument::Present {
                    value: BoundProcedureValue::RetentionDuration(duration),
                    span: Some(span),
                } => (duration, span),
                _ => return Err(invariant()),
            };
            older_than_ms = Some(relative_cutoff(
                source,
                value_span,
                &duration,
                statement_started_at,
                kind,
            )?);
            continue;
        }
        match (slot.name, argument) {
            (_, BoundProcedureArgument::Omitted) => {}
            (
                "older_than",
                BoundProcedureArgument::Present {
                    value: BoundProcedureValue::Value(DdlPropertyValue::Bigint(value)),
                    ..
                },
            ) => older_than_ms = Some(value),
            (
                "older_than",
                BoundProcedureArgument::Present {
                    value: BoundProcedureValue::Value(DdlPropertyValue::Varchar(value)),
                    span: argument_span,
                },
            ) => {
                older_than_ms = Some(original_timestamp(
                    source,
                    argument_span.ok_or_else(invariant)?,
                    &value,
                )?)
            }
            (
                "retain_last",
                BoundProcedureArgument::Present {
                    value: BoundProcedureValue::Value(DdlPropertyValue::Integer(value)),
                    ..
                },
            ) => retain_last = Some(u32::try_from(value).map_err(|_| invariant())?),
            _ => return Err(invariant()),
        }
    }
    // A retain-only expiration keeps the existing Provider's age policy.
    if older_than_ms.is_none() && retain_last.is_none() {
        let duration = ProcedureRetentionDuration::parse(
            source,
            span,
            retention_default.ok_or_else(invariant)?,
            control,
        )?;
        older_than_ms = Some(relative_cutoff(
            source,
            span,
            &duration,
            statement_started_at,
            kind,
        )?);
    }
    match kind {
        TableProcedureKind::ExpireSnapshots => Ok(Procedure::ExpireSnapshots {
            older_than_ms,
            retain_last,
        }),
        TableProcedureKind::RemoveOrphanFiles => Ok(Procedure::RemoveOrphanFiles(
            older_than_ms.ok_or_else(invariant)?,
        )),
        _ => Err(invariant()),
    }
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

struct OriginalTimestamp<'a> {
    source: &'a str,
}

impl TableProcedureAbsoluteTimePort for OriginalTimestamp<'_> {
    fn legacy_timestamp_literal_millis(
        &mut self,
        text: &str,
        span: Span,
    ) -> Result<i64, QueryServiceError> {
        original_timestamp(self.source, span, text)
    }
}

fn original_timestamp(source: &str, span: Span, value: &str) -> Result<i64, QueryServiceError> {
    crate::table_maintenance::admission::parse_timestamp_millis(value)
        .map_err(|message| invalid_procedure_argument(source, span, message))
}

fn invariant() -> QueryServiceError {
    QueryServiceError::new(
        QueryServiceErrorKind::Internal,
        "Table procedure value differs from its declaration",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, time::Duration};

    use arrow::{
        array::{Array, ArrayData, Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field},
    };
    use novarocks_functions::{ConstantPool, ConstantValue};
    use novarocks_parser::{
        ast::{Expr, LiteralKind, TrinoStatement},
        trino_parser::parse_statement,
    };
    use novarocks_query_application::sql::property_evaluation::DdlPropertyConstantError;
    use novarocks_spi::connector::ConnectorProviderId;
    use novarocks_sql::compiler::{RequiredConstantTarget, SqlCompileControl};
    use novarocks_type_contract::{CompilePhase, FunctionValueType};

    struct LiteralValues;
    impl DdlPropertyConstantPort for LiteralValues {
        fn evaluate(
            &mut self,
            expression: &Expr,
            target: RequiredConstantTarget,
        ) -> Result<ConstantValue, DdlPropertyConstantError> {
            let Expr::Literal(literal) = expression else {
                panic!("the test port only supplies checked literal values");
            };
            let data: ArrayData = match (target, &literal.kind) {
                (RequiredConstantTarget::Varchar, LiteralKind::String(text)) => {
                    StringArray::from(vec![text.as_str()]).to_data()
                }
                (RequiredConstantTarget::Integer, LiteralKind::Long { parsed_value, .. }) => {
                    Int32Array::from(vec![i32::try_from(*parsed_value).unwrap()]).to_data()
                }
                (RequiredConstantTarget::Bigint, LiteralKind::Long { parsed_value, .. }) => {
                    Int64Array::from(vec![*parsed_value]).to_data()
                }
                _ => panic!("unexpected required-constant target"),
            };
            let data_type: DataType = data.data_type().clone();
            let pool = ConstantPool::try_new(
                Arc::new(Field::new("argument", data_type.clone(), false)),
                FunctionValueType::new(data_type, false),
                data,
                crate::application::test_constant_policy(),
                CompilePhase::Validate,
                &SqlCompileControl::unbounded(),
            )
            .unwrap();
            Ok(pool.value(0).unwrap())
        }
    }

    fn project_at(
        source: &str,
        statement_started_at: SystemTime,
    ) -> Result<Procedure, QueryServiceError> {
        let TrinoStatement::TableExecute(statement) = parse_statement(source).unwrap() else {
            panic!("expected a table procedure");
        };
        let control = SqlCompileControl::unbounded();
        let bound = bind_trino_table_execute(
            source,
            &statement,
            &ConnectorProviderId::parse("iceberg").unwrap(),
            &mut LiteralValues,
            &mut OriginalTimestamp { source },
            &control,
        )?;
        project_bound(
            source,
            statement.span,
            bound,
            statement_started_at,
            &control,
        )
    }

    fn project(source: &str) -> Result<Procedure, QueryServiceError> {
        project_at(
            source,
            SystemTime::UNIX_EPOCH + Duration::from_millis(800_000_123),
        )
    }

    #[test]
    fn typed_table_procedure_uses_original_absolute_time_conversion() {
        assert_eq!(
            project("ALTER TABLE lake.ns.t EXECUTE optimize_manifests").unwrap(),
            Procedure::RewriteManifests,
        );
        assert_eq!(
            project("ALTER TABLE lake.ns.t EXECUTE expire_snapshots(older_than => TIMESTAMP '1970-01-01 00:00:07')").unwrap(),
            Procedure::ExpireSnapshots { older_than_ms: Some(7000), retain_last: None },
        );
        assert_eq!(
            project("ALTER TABLE lake.ns.t EXECUTE remove_orphan_files(older_than => TIMESTAMP '1970-01-01 00:00:03')").unwrap(),
            Procedure::RemoveOrphanFiles(3000),
        );
        for (source, expected) in [
            (
                "ALTER TABLE t EXECUTE remove_orphan_files(older_than => 7001)",
                7001,
            ),
            (
                "ALTER TABLE t EXECUTE remove_orphan_files(older_than => '1970-01-01T01:00:07.123456+01:00')",
                7123,
            ),
            (
                "ALTER TABLE t EXECUTE remove_orphan_files(older_than => TIMESTAMP '1970-01-01T01:00:07.123456+01:00')",
                7123,
            ),
            (
                "ALTER TABLE t EXECUTE remove_orphan_files(older_than => TIMESTAMP '7001')",
                7001,
            ),
        ] {
            assert_eq!(
                project(source).unwrap(),
                Procedure::RemoveOrphanFiles(expected),
                "{source}"
            );
        }
    }

    #[test]
    fn relative_procedures_use_bound_duration_and_one_supplied_statement_start() {
        for source in [
            "ALTER TABLE lake.ns.t EXECUTE expire_snapshots",
            "ALTER TABLE lake.ns.t EXECUTE expire_snapshots(retention_threshold => '7d')",
        ] {
            assert_eq!(
                project(source).unwrap(),
                Procedure::ExpireSnapshots {
                    older_than_ms: Some(195_200_123),
                    retain_last: None,
                }
            );
        }
        assert_eq!(
            project("ALTER TABLE lake.ns.t EXECUTE remove_orphan_files").unwrap(),
            Procedure::RemoveOrphanFiles(195_200_123),
        );
        assert_eq!(
            project(
                "ALTER TABLE lake.ns.t EXECUTE remove_orphan_files(retention_threshold => '0.5ms')"
            )
            .unwrap(),
            Procedure::RemoveOrphanFiles(800_000_122),
        );
        assert_eq!(
            project("ALTER TABLE lake.ns.t EXECUTE expire_snapshots(retention_threshold => '0d', retain_last => 2)").unwrap(),
            Procedure::ExpireSnapshots { older_than_ms: Some(800_000_123), retain_last: Some(2) },
        );
        assert_eq!(
            project("ALTER TABLE lake.ns.t EXECUTE expire_snapshots(retain_last => 2)").unwrap(),
            Procedure::ExpireSnapshots {
                older_than_ms: None,
                retain_last: Some(2)
            },
        );
    }

    #[test]
    fn bad_retention_is_reported_at_its_argument_before_a_later_bad_retain_count() {
        let source = "ALTER TABLE t EXECUTE expire_snapshots(retention_threshold => 'one day', retain_last => 0)";
        let error = project(source).unwrap_err();
        let user = error.user_error().unwrap();
        assert_eq!(
            user.code().as_str(),
            "sql.admit.invalid_table_procedure_argument"
        );
        assert!(user.message().contains("retention_threshold duration"));
        let start = source.find("retention_threshold").unwrap();
        let end = source.find(',').unwrap();
        assert_eq!(
            user.location(),
            Some(&Span::new(start, end).to_user_error_location(source))
        );
    }

    #[test]
    fn relative_procedures_keep_typed_argument_errors_and_original_cutoff_limits() {
        let start = SystemTime::UNIX_EPOCH + Duration::from_millis(7);
        assert_eq!(
            project_at(
                "ALTER TABLE t EXECUTE expire_snapshots(retention_threshold => '7ms')",
                start
            )
            .unwrap(),
            Procedure::ExpireSnapshots {
                older_than_ms: Some(0),
                retain_last: None
            },
        );
        for source in [
            "ALTER TABLE t EXECUTE remove_orphan_files(retention_threshold => '7ms')",
            "ALTER TABLE t EXECUTE expire_snapshots(retention_threshold => '8ms')",
            "ALTER TABLE t EXECUTE expire_snapshots(retention_threshold => 'one day')",
            "ALTER TABLE t EXECUTE remove_orphan_files(retention_threshold => '1d', older_than => TIMESTAMP '1970-01-01 00:00:07')",
        ] {
            let error = project_at(source, start).unwrap_err();
            assert_eq!(
                error.user_error().unwrap().code().as_str(),
                "sql.admit.invalid_table_procedure_argument",
                "{source}",
            );
        }
    }
}
