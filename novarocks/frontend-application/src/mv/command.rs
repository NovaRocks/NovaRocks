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

mod trino;
pub(crate) use trino::TrinoMvPropertyPreparation;
#[cfg(test)]
pub(crate) use trino::{create_query_source, resolve_trino_target};

use crate::mv::domain::application::{
    MvAlterAction, MvAlterStatement, MvCreateStatement, MvDropStatement, MvRefreshRequest,
    MvRepartitionFields,
};
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use novarocks_parser::ast::ObjectName;
use novarocks_query_application::admitted_query_context::RequestContext;
use novarocks_query_application::api::{CommandContext, CommandError, CommandErrorKind};
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_query_application::sql::PreparedMaintenanceCall;
use novarocks_spi::connector::{DdlPropertyChange, DdlPropertyValue, MvStorageObservationPort};

use super::FrontendMvProductAdapter;
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

    fn prepare_refresh_explain(
        &self,
        catalog: Option<&str>,
        statement: MvRefreshRequest,
        level: novarocks_sql::compiler::ExplainLevel,
        context: &RequestContext,
        command_context: &CommandContext,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        CommandError,
    > {
        let services = self.execution_services.clone().ok_or_else(|| {
            CommandError::new(
                CommandErrorKind::Unsupported,
                "MV execution services are unavailable",
            )
        })?;
        let request = Box::new(
            crate::query_execution::mv_assembly::refresh_explain::MvExplainPreparationInput {
                ports: self.ports.clone(),
                current_catalog: catalog.map(str::to_owned),
                current_database: context.session().current_database().to_owned(),
                statement,
                level,
                context: command_context.connector_context().clone(),
            },
        );
        let presentation = context.session().presentation().copied().ok_or_else(|| {
            CommandError::new(
                CommandErrorKind::Rejected,
                "MV client result requires an explicit presentation",
            )
        })?;
        crate::query::mv_read_action::prepare(
            services,
            request,
            context.execution().clone(),
            presentation,
        )
    }

    pub(crate) fn prepare_trino_refresh(
        &self,
        refresh: novarocks_parser::ast::TrinoRefreshMaterializedView,
        explain: bool,
        context: &RequestContext,
        command_context: &CommandContext,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        CommandError,
    > {
        let (_, target) = trino::resolve_trino_target(
            &refresh.table.name,
            context.session().current_catalog(),
            context.session().current_database(),
        )
        .map_err(crate::query::procedure_command_error)?;
        let statement = MvRefreshRequest {
            name_parts: vec![target.namespace.clone(), target.table.clone()],
            full: false,
        };
        if explain {
            return self.prepare_refresh_explain(
                Some(&target.catalog),
                statement,
                refresh_explain_level(context.session().property_inputs())?,
                context,
                command_context,
            );
        }
        self.prepare_command_request(
            crate::query::mv_action::MvCommandRequest::Refresh {
                statement,
                requested: novarocks_sql::planning::mv::SqlMvTarget {
                    catalog: Some(target.catalog),
                    database: target.namespace,
                    name: target.table,
                },
                session_properties: Arc::clone(context.session().property_inputs()),
            },
            context,
            command_context,
        )
    }

    pub(crate) fn prepare_trino_drop(
        &self,
        drop: novarocks_parser::ast::TrinoDropMaterializedView,
        context: &RequestContext,
        command_context: &CommandContext,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        CommandError,
    > {
        let (_, target) = trino::resolve_trino_target(
            &drop.name,
            context.session().current_catalog(),
            context.session().current_database(),
        )
        .map_err(crate::query::procedure_command_error)?;
        let statement = MvDropStatement {
            name_parts: vec![target.namespace.clone(), target.table.clone()],
            if_exists: drop.exists,
        };
        let target = novarocks_sql::planning::mv::SqlMvTarget {
            catalog: Some(target.catalog),
            database: target.namespace,
            name: target.table,
        };
        self.prepare_command_request(
            crate::query::mv_action::MvCommandRequest::Control(
                crate::query::mv_action::MvControlRequest::Drop { target, statement },
            ),
            context,
            command_context,
        )
    }

    pub(crate) fn trino_property_planning(
        &self,
        name: &ObjectName,
        context: &RequestContext,
        source: &str,
    ) -> Result<
        novarocks_spi::connector::ConnectorControlPlanningPin,
        novarocks_query_application::session_error::QueryServiceError,
    > {
        use novarocks_query_application::session_error::{
            QueryServiceError, QueryServiceErrorKind,
        };
        use novarocks_spi::connector::{ConnectorControlResolver, ConnectorInstanceId};
        let (_, target) = trino::resolve_trino_target(
            name,
            context.session().current_catalog(),
            context.session().current_database(),
        )?;
        if novarocks_sql::system_table::is_system_table_name(&target.catalog, &target.namespace) {
            return Err(
                novarocks_query_application::sql::trino_admission::unsupported_statement(
                    source,
                    name.span,
                    "Modifying a system catalog or information_schema",
                ),
            );
        }
        let services = self.execution_services.as_ref().ok_or_else(|| {
            QueryServiceError::new(
                QueryServiceErrorKind::Unsupported,
                "MV execution services are unavailable",
            )
        })?;
        let instance = ConnectorInstanceId::parse(&target.catalog).map_err(|error| {
            QueryServiceError::new(QueryServiceErrorKind::InvalidValue, error.to_string())
        })?;
        services
            .connector_control
            .acquire_current(&instance)
            .map_err(|error| {
                QueryServiceError::from_failure_cause(
                    novarocks_failure_contract::SharedFailureCause::new(error),
                )
            })
    }

    /// Send the admitted CREATE properties to the original staged target Action.
    pub(crate) fn prepare_evaluated_create(
        &self,
        statement: MvCreateStatement,
        context: &RequestContext,
        command_context: &CommandContext,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        CommandError,
    > {
        use crate::query::mv_action::{CreateRequest, MvCommandRequest};
        let target = &statement.storage.target;
        let target = novarocks_sql::planning::mv::SqlMvTarget {
            catalog: Some(target.catalog.clone()),
            database: target.namespace.clone(),
            name: target.table.clone(),
        };
        self.prepare_command_request(
            MvCommandRequest::Create(Box::new(CreateRequest { target, statement })),
            context,
            command_context,
        )
    }

    /// Start the original governed Alter with the exact resolved target.
    pub(crate) fn prepare_evaluated_alter(
        &self,
        target: novarocks_sql::planning::mv::SqlMvTarget,
        statement: MvAlterStatement,
        context: &RequestContext,
        command_context: &CommandContext,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        CommandError,
    > {
        let request = alter_command_for_target(target, statement)?;
        self.prepare_command_request(request, context, command_context)
    }

    fn prepare_command_request(
        &self,
        request: crate::query::mv_action::MvCommandRequest,
        context: &RequestContext,
        command_context: &CommandContext,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        CommandError,
    > {
        let catalog = context.session().current_catalog();
        let database = context.session().current_database();
        let services = self.execution_services.clone().ok_or_else(|| {
            novarocks_query_application::api::CommandError::new(
                novarocks_query_application::api::CommandErrorKind::Unsupported,
                "MV execution services are unavailable",
            )
        })?;
        let (product, activation) = self.refresh_service.original_refresh_ports();
        crate::query::mv_action::prepare(
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
    /// rebuild from prepared procedure arguments. Other procedures remain a
    /// route miss.
    pub fn try_execute_typed_call(
        &self,
        statement: &PreparedMaintenanceCall,
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
        None => match error.analyze_error() {
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
            None => novarocks_query_application::api::CommandError::from_user_error(
                error.to_user_error(None),
            ),
        },
        error => novarocks_query_application::api::CommandError::new(
            novarocks_query_application::api::CommandErrorKind::Failed,
            error.to_string(),
        ),
    }
}

fn alter_command_for_target(
    target: novarocks_sql::planning::mv::SqlMvTarget,
    statement: MvAlterStatement,
) -> Result<crate::query::mv_action::MvCommandRequest, CommandError> {
    use crate::query::mv_action::{MvCommandRequest, MvControlRequest};
    let fields = match &statement.action {
        MvAlterAction::Storage { owner, properties }
        | MvAlterAction::RefreshAndStorage {
            owner, properties, ..
        } => {
            let mut partitioning = properties
                .iter()
                .enumerate()
                .filter(|(_, (name, _))| name.as_ref() == "partitioning");
            if let Some((partitioning_index, (_, change))) = partitioning.next() {
                if partitioning.next().is_some() {
                    return Err(CommandError::new(
                        CommandErrorKind::Invalid,
                        "Duplicate evaluated MV partitioning property",
                    ));
                }
                let value = match change {
                    DdlPropertyChange::Set(value) | DdlPropertyChange::Reset(value) => value,
                };
                if !matches!(value, DdlPropertyValue::ArrayVarchar(_)) {
                    return Err(CommandError::new(
                        CommandErrorKind::Invalid,
                        "Evaluated MV partitioning must be array(varchar)",
                    ));
                }
                Some(MvRepartitionFields {
                    owner: owner.clone(),
                    properties: Arc::clone(properties),
                    partitioning_index,
                    configuration: match &statement.action {
                        MvAlterAction::RefreshAndStorage { changes, .. } => Some(changes.clone()),
                        _ => None,
                    },
                })
            } else {
                None
            }
        }
        _ => None,
    };
    Ok(if let Some(fields) = fields {
        MvCommandRequest::Repartition {
            statement: MvRefreshRequest {
                name_parts: statement.name_parts,
                full: false,
            },
            target,
            fields,
        }
    } else {
        MvCommandRequest::Control(MvControlRequest::Alter { target, statement })
    })
}

fn refresh_explain_level(
    properties: &novarocks_query_application::sql::session::registry::SessionPropertyInputs,
) -> Result<novarocks_sql::compiler::ExplainLevel, CommandError> {
    use novarocks_query_application::sql::session::{ExplainDetail, session_property_error};
    let detail = properties.explain_detail().map_err(|error| {
        let error = session_property_error(error);
        match error.failure_cause() {
            Some(cause) => CommandError::from_failure_cause(cause.clone()),
            None => CommandError::new(CommandErrorKind::Failed, error.message()),
        }
    })?;
    if detail == ExplainDetail::Contract {
        return Err(CommandError::new(
            CommandErrorKind::Unsupported,
            "EXPLAIN REFRESH MATERIALIZED VIEW has no physical plan contract",
        ));
    }
    // REFRESH always uses the original logical refresh planner.
    Ok(crate::query::compiler::explain_detail_mode(detail).0)
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
        let planning = crate::connector::acquire_metadata_planning_pin(
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
    use novarocks_mv_application::engine_properties::MvRefreshPropertyChanges;

    #[test]
    fn evaluated_mv_storage_and_partitioning_take_their_original_distinct_commands() {
        use crate::query::mv_action::{MvCommandRequest, MvControlRequest};
        use novarocks_spi::connector::{
            ConnectorInstanceId, ConnectorProviderBindingKey, ProviderBindingEpoch,
        };

        let owner = ConnectorProviderBindingKey {
            instance_id: ConnectorInstanceId::parse("ice").unwrap(),
            incarnation: ProviderBindingEpoch::new(),
        };
        let storage = (
            Arc::from("extra_properties"),
            DdlPropertyChange::Set(DdlPropertyValue::MapVarcharVarchar(vec![(
                "read.split.target-size".into(),
                Some("16777216".into()),
            )])),
        );
        let partitioning = (
            Arc::from("partitioning"),
            DdlPropertyChange::Set(DdlPropertyValue::ArrayVarchar(vec![Some(
                "bucket(id, 16)".into(),
            )])),
        );
        let target = novarocks_sql::planning::mv::SqlMvTarget {
            catalog: Some("ice".into()),
            database: "db".into(),
            name: "t".into(),
        };
        let route = |properties: Vec<_>| {
            alter_command_for_target(
                target.clone(),
                MvAlterStatement {
                    name_parts: vec!["t".into()],
                    action: MvAlterAction::Storage {
                        owner: owner.clone(),
                        properties: properties.into(),
                    },
                },
            )
        };
        let MvCommandRequest::Control(MvControlRequest::Alter { statement, .. }) =
            route(vec![storage.clone()]).ok().unwrap()
        else {
            panic!("storage properties use the original metadata control");
        };
        let MvAlterAction::Storage {
            properties,
            owner: actual,
        } = statement.action
        else {
            panic!("the typed property must reach the control request");
        };
        assert_eq!(properties.as_ref(), std::slice::from_ref(&storage));
        assert_eq!(actual, owner);
        let MvCommandRequest::Repartition { fields, .. } =
            route(vec![partitioning.clone()]).ok().unwrap()
        else {
            panic!("partitioning must retain the managed data publication");
        };
        let MvRepartitionFields {
            properties,
            partitioning_index,
            configuration,
            owner: actual,
        } = fields;
        assert_eq!(properties.as_ref(), std::slice::from_ref(&partitioning));
        assert_eq!(partitioning_index, 0);
        assert!(configuration.is_none());
        assert_eq!(actual, owner);
        let empty = (
            Arc::from("partitioning"),
            DdlPropertyChange::Reset(DdlPropertyValue::ArrayVarchar(vec![])),
        );
        let MvCommandRequest::Repartition {
            fields:
                MvRepartitionFields {
                    properties,
                    partitioning_index,
                    ..
                },
            ..
        } = route(vec![empty.clone()]).ok().unwrap()
        else {
            panic!("an explicit empty default still selects managed repartition");
        };
        assert_eq!(properties.as_ref(), std::slice::from_ref(&empty));
        assert_eq!(partitioning_index, 0);

        let properties: Arc<[(Arc<str>, DdlPropertyChange)]> = vec![storage, partitioning].into();
        let changes = MvRefreshPropertyChanges::set_paused(true);
        let MvCommandRequest::Repartition {
            fields:
                MvRepartitionFields {
                    properties: retained,
                    partitioning_index,
                    configuration: Some(configuration),
                    owner: actual,
                },
            ..
        } = alter_command_for_target(
            target,
            MvAlterStatement {
                name_parts: vec!["t".into()],
                action: MvAlterAction::RefreshAndStorage {
                    changes: changes.clone(),
                    owner: owner.clone(),
                    properties: Arc::clone(&properties),
                },
            },
        )
        .ok()
        .unwrap()
        else {
            panic!("combined properties must remain on one data publication");
        };
        assert!(Arc::ptr_eq(&retained, &properties));
        assert_eq!(partitioning_index, 1);
        assert_eq!(configuration, changes);
        assert_eq!(actual, owner);
    }

    #[test]
    fn explain_refresh_uses_the_frozen_session_detail() {
        use novarocks_query_application::sql::session::registry::{
            SessionPropertyInputs, SessionPropertyRegistry, SessionPropertyValue,
        };
        use novarocks_sql::compiler::ExplainLevel;

        let registry = SessionPropertyRegistry::new().unwrap();
        let mut inputs = SessionPropertyInputs::default();
        assert_eq!(
            refresh_explain_level(&inputs).unwrap(),
            ExplainLevel::Normal
        );
        inputs.insert(
            registry
                .prepare_sql(
                    "explain_detail",
                    SessionPropertyValue::Varchar("LOGICAL_COSTS".into()),
                )
                .unwrap(),
        );
        assert_eq!(refresh_explain_level(&inputs).unwrap(), ExplainLevel::Costs);
        inputs.insert(
            registry
                .prepare_sql(
                    "explain_detail",
                    SessionPropertyValue::Varchar("CONTRACT".into()),
                )
                .unwrap(),
        );
        assert_eq!(
            refresh_explain_level(&inputs).unwrap_err().kind(),
            CommandErrorKind::Unsupported,
        );
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
            .analyze_error()
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
    fn definition_errors_retain_original_identity_through_refresh_and_product_bridges() {
        use novarocks_mv_application::ports::{MvProviderFailure, MvProviderFailureKind};
        for original in [
            AnalyzeError::missing_column_name("missing position 1", None),
            AnalyzeError::duplicate_column_name("duplicate a", None),
            AnalyzeError::mismatched_column_aliases("wrong width", None),
            AnalyzeError::invalid_view("invalid stored body", None),
            AnalyzeError::view_is_stale("changed header", None),
            AnalyzeError::view_is_recursive("recursive body", None),
        ] {
            let refresh = crate::mv::domain::lifecycle::RefreshError::from(
                SqlCompileError::Analyze(original.clone()),
            );
            let prepared = PreparationError::from(refresh)
                .with_context("refresh preparation")
                .0;
            assert_eq!(prepared.analyze_error(), Some(&original));
            let product =
                MvProviderFailure::new(MvProviderFailureKind::InvalidRequest, prepared.message())
                    .with_analyze_error(prepared.analyze_error().cloned())
                    .into_product_error();
            let application =
                crate::mv::domain::application::MvApplicationError::from_refresh_product(&product);
            assert_eq!(application.analyze_error(), Some(&original));
            let command = mv_application_command_error(application);
            let expected = original.to_user_error(None);
            assert_eq!(command.user_error(), Some(&expected));
            assert_eq!(command.compile_control_error(), None);
        }
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
            assert_eq!(failure.analyze_error(), None);
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
            let expected = match &compile {
                SqlCompileError::Analyze(error) => Some(error.clone()),
                _ => None,
            };
            let failure = prepare_failure(compile);
            assert_eq!(failure.analyze_error(), expected.as_ref());
            let user = mv_application_command_error(failure);
            assert_eq!(user.failure_cause(), None);
            assert_eq!(user.execution_failure(), None);
            assert_eq!(
                user.user_error().map(|error| error.code().as_str()),
                expected.as_ref().map(|error| error.code().as_str())
            );
            assert_eq!(user.kind(), CommandErrorKind::Failed);
            assert_eq!(user.compile_control_error(), None);
            assert_eq!(
                user.to_string(),
                expected.map_or_else(
                    || message.to_string(),
                    |error| error.to_user_error(None).to_string()
                )
            );
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
                let expected = match &compile {
                    SqlCompileError::Analyze(error) => Some(error.clone()),
                    _ => None,
                };
                let failure = prepare_failure(compile);
                assert_eq!(failure.compile_control_error(), Some(control.clone()));
                assert_eq!(failure.analyze_error(), expected.as_ref());
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
            assert!(failure.analyze_error().is_some());
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
