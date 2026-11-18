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

//! Engine dispatch for `ALTER TABLE … (CREATE|DROP) BRANCH|TAG`.
//!
//! Bridges a Query-Application semantic command to connector mutation DTOs.
//! The provider owns authoritative ref/snapshot validation and the external
//! catalog commit.

use std::sync::Arc;

use novarocks_parser::Span;
use novarocks_query_application::{
    api::{CommandError, CommandErrorKind},
    protocol_delivery::QuerySessionOutput as StatementResult,
    session_error::{QueryServiceError, QueryServiceErrorKind},
    sql::table_procedure::{
        BoundProcedureArgument, BoundProcedureValue, BoundTableProcedure, LegacyProcedureValue,
        ProcedureArgumentDefault, TableProcedureArgument, TableProcedureKind,
        bind_legacy_procedure_arguments, invalid_procedure_argument,
        resolve_legacy_table_procedure,
    },
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationSession,
    ConnectorControlPlanningPin, ConnectorInstanceId, ConnectorProviderId, ConnectorRefAction,
    ConnectorRefKind, ConnectorRequestContext, ConnectorTableIdentity, CreateOrReplacePolicy,
    DdlPropertyValue, DropPolicy, ExternalMutationFinalization, MvStorageObservationPort,
};
use novarocks_sql::semantic::ObjectName;
use novarocks_sql::semantic::command::{
    AlterIcebergTableSqlCommand, IcebergReferenceKindSql, IcebergReferenceSqlAction,
    IcebergTableSqlAction,
};

use crate::catalog_application::resolver::{TargetBackend, require_external_target};
use crate::mv::domain::readiness::MvReadinessPort;

pub(crate) struct RefMutationBinding {
    pin: ConnectorControlPlanningPin,
    mutation: ConnectorCatalogMutationSession,
    table: ConnectorTableIdentity,
}

impl RefMutationBinding {
    pub(crate) fn mutation(&self) -> &ConnectorCatalogMutationSession {
        &self.mutation
    }
}

pub(crate) struct PreparedRefMutation {
    binding: RefMutationBinding,
    action: ConnectorRefAction,
}

#[derive(Debug, PartialEq)]
pub(crate) struct TrinoTagAction(ConnectorRefAction);

pub(crate) fn project_trino_tag(
    source: &str,
    span: Span,
    bound: BoundTableProcedure<'_>,
) -> Result<TrinoTagAction, QueryServiceError> {
    project_tag(bound, Some((source, span))).map(TrinoTagAction)
}

/// The procedure binder and the ref mutation retain the same planning pin.
pub(crate) fn prepare_resolved_tag(
    storage_observation: &dyn MvStorageObservationPort,
    readiness: &MvReadinessPort,
    target: &TargetBackend,
    planning: ConnectorControlPlanningPin,
    action: TrinoTagAction,
    connector_context: &ConnectorRequestContext,
) -> Result<PreparedRefMutation, CommandError> {
    crate::connector::validate_request_context(connector_context)?;
    require_external_target(&target.catalog, &target.namespace)?;
    if target.provider_id.as_str() != "iceberg"
        || planning.binding().descriptor().provider_id != target.provider_id
    {
        return Err(CommandError::new(
            CommandErrorKind::Conflict,
            "Tag procedure target does not match its retained Iceberg generation",
        ));
    }
    let binding = bind_resolved_target(
        storage_observation,
        readiness,
        target,
        planning,
        connector_context,
    )?;
    Ok(PreparedRefMutation {
        binding,
        action: action.0,
    })
}

/// Execute an Iceberg ref mutation using only the explicit connector-control
/// and MV storage-observation ports required for MV-target admission.
pub(crate) fn execute_with_ports(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    _current_database: &str,
    stmt: &AlterIcebergTableSqlCommand,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, novarocks_query_application::api::CommandError> {
    crate::connector::validate_request_context(connector_context)?;
    // 1. Resolve qualified name — must be 3-part (catalog.namespace.table).
    let IcebergTableSqlAction::Reference(action) = &stmt.action else {
        return Err(("Iceberg ref executor received a non-reference action".to_string()).into());
    };
    let binding = prepare_target(
        connector_control,
        storage_observation,
        readiness,
        &stmt.table,
        connector_context,
    )?;
    prepare_action(binding, action, connector_context)?.execute(connector_context)
}

pub(crate) fn prepare_target(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    table: &ObjectName,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RefMutationBinding, CommandError> {
    crate::connector::validate_request_context(connector_context)?;
    let (catalog_name, namespace, table_name) = resolve_table_parts(table)?;
    require_external_target(&catalog_name, &namespace)?;
    let exact_lease =
        crate::connector::acquire_metadata_planning_pin(connector_control, &catalog_name)?;
    let target = TargetBackend {
        provider_id: ConnectorProviderId::parse("iceberg").expect("static Iceberg provider ID"),
        catalog: catalog_name,
        namespace,
        table: table_name,
    };
    bind_resolved_target(
        storage_observation,
        readiness,
        &target,
        exact_lease,
        connector_context,
    )
}

fn bind_resolved_target(
    storage_observation: &dyn MvStorageObservationPort,
    readiness: &MvReadinessPort,
    target: &TargetBackend,
    planning: ConnectorControlPlanningPin,
    connector_context: &ConnectorRequestContext,
) -> Result<RefMutationBinding, CommandError> {
    // Retain one exact generation across MV admission and the ref mutation.
    // The application never decodes the provider-owned table handle.
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_planning_pin_and_context(
        storage_observation,
        &planning,
        readiness,
        target,
        crate::mv::domain::iceberg_guard::IcebergMvUserMutation::AlterTable,
        connector_context.clone(),
    )
    .map_err(|error| format!("guard ALTER TABLE reference: {error}"))?;
    let instance_id =
        ConnectorInstanceId::parse(&target.catalog).map_err(|error| error.to_string())?;
    if planning.binding().descriptor().instance_id != instance_id {
        return Err(
            ("connector planning pin identity changed during ALTER TABLE ref".to_string()).into(),
        );
    }
    let mutation = planning
        .derive_mutation_lease()
        .map_err(|error| error.to_string())?;
    Ok(RefMutationBinding {
        pin: planning,
        mutation,
        table: ConnectorTableIdentity {
            instance_id,
            namespace: Arc::from(target.namespace.as_str()),
            table: Arc::from(target.table.as_str()),
        },
    })
}

pub(crate) fn prepare_action(
    binding: RefMutationBinding,
    action: &IcebergReferenceSqlAction,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedRefMutation, CommandError> {
    let control = crate::query_execution::planning::sql_compile_control_from_connector_request(
        connector_context,
    );
    let action = connector_ref_action(
        action,
        &binding.pin.binding().descriptor().provider_id,
        &control,
    )?;
    Ok(PreparedRefMutation { binding, action })
}

impl PreparedRefMutation {
    pub(crate) fn execute(
        self,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, CommandError> {
        let outcome = crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
            &self.binding.mutation,
            novarocks_spi::connector::ConnectorMutationOperationId::new(),
            ConnectorCatalogMutationOperation::AlterRef {
                table: self.binding.table,
                action: self.action,
            },
            connector_context.clone(),
        );
        match outcome {
            crate::connector::mutation::ResolvedCatalogMutation::KnownCommitted(completed) => {
                if let ExternalMutationFinalization::Failed(failure) = completed.finalization {
                    return Err((
                    novarocks_query_application::engine_error::EngineError::commit_known_committed_finalize_failed(
                        failure.to_string(),
                    )
                    .to_string()).into());
                }
            }
            crate::connector::mutation::ResolvedCatalogMutation::KnownUncommitted {
                failure,
                cleanup,
            } => {
                return Err(
                (novarocks_query_application::engine_error::EngineError::commit_known_uncommitted(
                    crate::connector::mutation::known_uncommitted_message(failure, &cleanup),
                )
                .to_string())
                .into(),
            );
            }
            crate::connector::mutation::ResolvedCatalogMutation::CommitUnknown {
                failure, ..
            } => {
                return Err(
                    (novarocks_query_application::engine_error::EngineError::commit_unknown(
                        failure.to_string(),
                    )
                    .to_string())
                    .into(),
                );
            }
            crate::connector::mutation::ResolvedCatalogMutation::ContractFailure {
                error, ..
            } => {
                return Err((error.to_string()).into());
            }
        }

        Ok(StatementResult::Ok)
    }
}

fn connector_ref_action(
    action: &IcebergReferenceSqlAction,
    provider: &ConnectorProviderId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ConnectorRefAction, CommandError> {
    match action {
        IcebergReferenceSqlAction::Create {
            kind,
            name,
            snapshot_id,
            if_not_exists,
            or_replace,
        } => {
            if *kind == IcebergReferenceKindSql::Branch {
                return Ok(create_ref(
                    ConnectorRefKind::Branch,
                    Arc::from(name.as_str()),
                    *snapshot_id,
                    *or_replace,
                    *if_not_exists,
                ));
            }
            let mut inputs = Vec::with_capacity(4);
            inputs.push(("name", LegacyProcedureValue::Varchar(name)));
            if let Some(snapshot_id) = snapshot_id {
                inputs.push(("snapshot_id", LegacyProcedureValue::Bigint(*snapshot_id)));
            }
            inputs.push(("replace", LegacyProcedureValue::Boolean(*or_replace)));
            inputs.push((
                "if_not_exists",
                LegacyProcedureValue::Boolean(*if_not_exists),
            ));
            bind_tag(provider, "create_tag", &inputs, control)
        }
        IcebergReferenceSqlAction::Drop {
            kind,
            name,
            if_exists,
        } => {
            if *kind == IcebergReferenceKindSql::Branch {
                return Ok(drop_ref(
                    ConnectorRefKind::Branch,
                    Arc::from(name.as_str()),
                    *if_exists,
                ));
            }
            bind_tag(
                provider,
                "drop_tag",
                &[
                    ("name", LegacyProcedureValue::Varchar(name)),
                    ("if_exists", LegacyProcedureValue::Boolean(*if_exists)),
                ],
                control,
            )
        }
    }
}

fn bind_tag(
    provider: &ConnectorProviderId,
    procedure: &str,
    inputs: &[(&str, LegacyProcedureValue<'_>)],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ConnectorRefAction, CommandError> {
    let procedure = resolve_legacy_table_procedure(provider, procedure)
        .map_err(crate::query::procedure_command_error)?;
    let bound = bind_legacy_procedure_arguments(procedure, inputs, control)
        .map_err(crate::query::procedure_command_error)?;
    project_tag(bound, None).map_err(crate::query::procedure_command_error)
}

fn project_tag(
    bound: BoundTableProcedure<'_>,
    location: Option<(&str, Span)>,
) -> Result<ConnectorRefAction, QueryServiceError> {
    let declaration = bound.declaration();
    let mut slots = bound.into_arguments();
    let name = match tag_argument(&mut slots, "name")? {
        Some(DdlPropertyValue::Varchar(name)) => Arc::from(name),
        None => {
            return Err(match location {
                Some((source, span)) => invalid_procedure_argument(
                    source,
                    span,
                    format!("Table procedure {} requires 'name'", declaration.name()),
                ),
                None => tag_binding_invariant(),
            });
        }
        _ => return Err(tag_binding_invariant()),
    };
    match declaration.kind() {
        TableProcedureKind::CreateTag => {
            let snapshot_id = match tag_argument(&mut slots, "snapshot_id")? {
                Some(DdlPropertyValue::Bigint(id)) => Some(id),
                None => None,
                _ => return Err(tag_binding_invariant()),
            };
            let (
                Some(DdlPropertyValue::Boolean(replace)),
                Some(DdlPropertyValue::Boolean(if_not_exists)),
            ) = (
                tag_argument(&mut slots, "replace")?,
                tag_argument(&mut slots, "if_not_exists")?,
            )
            else {
                return Err(tag_binding_invariant());
            };
            Ok(create_ref(
                ConnectorRefKind::Tag,
                name,
                snapshot_id,
                replace,
                if_not_exists,
            ))
        }
        TableProcedureKind::DropTag => {
            let Some(DdlPropertyValue::Boolean(if_exists)) = tag_argument(&mut slots, "if_exists")?
            else {
                return Err(tag_binding_invariant());
            };
            Ok(drop_ref(ConnectorRefKind::Tag, name, if_exists))
        }
        _ => Err(tag_binding_invariant()),
    }
}

fn tag_argument<'a>(
    slots: &mut impl Iterator<Item = (&'static TableProcedureArgument, BoundProcedureArgument<'a>)>,
    name: &str,
) -> Result<Option<DdlPropertyValue>, QueryServiceError> {
    match slots.next() {
        Some((slot, argument)) if slot.name == name => match argument {
            BoundProcedureArgument::Omitted => match slot.default {
                Some(ProcedureArgumentDefault::Boolean(value)) => {
                    Ok(Some(DdlPropertyValue::Boolean(value)))
                }
                None => Ok(None),
                _ => Err(tag_binding_invariant()),
            },
            BoundProcedureArgument::Present {
                value: BoundProcedureValue::Value(value),
                ..
            } => Ok(Some(value)),
            _ => Err(tag_binding_invariant()),
        },
        _ => Err(tag_binding_invariant()),
    }
}

fn tag_binding_invariant() -> QueryServiceError {
    QueryServiceError::new(
        QueryServiceErrorKind::Internal,
        "Tag binding differs from its original procedure declaration",
    )
}

fn create_ref(
    kind: ConnectorRefKind,
    name: Arc<str>,
    snapshot_id: Option<i64>,
    replace: bool,
    if_not_exists: bool,
) -> ConnectorRefAction {
    let policy = if replace {
        CreateOrReplacePolicy::ReplaceIfExists
    } else if if_not_exists {
        CreateOrReplacePolicy::NoOpIfExists
    } else {
        CreateOrReplacePolicy::FailIfExists
    };
    ConnectorRefAction::Create {
        kind,
        name,
        snapshot_id,
        policy,
        expected_table_uuid: None,
    }
}

fn drop_ref(kind: ConnectorRefKind, name: Arc<str>, if_exists: bool) -> ConnectorRefAction {
    ConnectorRefAction::Drop {
        kind,
        name,
        policy: if if_exists {
            DropPolicy::NoOpIfMissing
        } else {
            DropPolicy::FailIfMissing
        },
    }
}

fn resolve_table_parts(name: &ObjectName) -> Result<(String, String, String), String> {
    let parts = &name.parts;
    match parts.len() {
        3 => Ok((parts[0].clone(), parts[1].clone(), parts[2].clone())),
        2 => Err(format!(
            "iceberg ref: qualify table with catalog (got '{}.{}')",
            parts[0], parts[1]
        )),
        1 => Err(format!(
            "iceberg ref: qualify table with catalog and namespace (got '{}')",
            parts[0]
        )),
        _ => Err(format!(
            "iceberg ref: invalid table name (parts: {})",
            parts.len()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use novarocks_query_application::sql::{
        ProductSqlCommand, lower_product_sql_command, parse_single_statement,
    };
    use novarocks_sql::semantic::CatalogSqlCommand;

    #[test]
    fn semantic_create_ref_preserves_anchor_and_policy() {
        let statement = parse_single_statement(
            "ALTER TABLE ice.db.orders CREATE BRANCH IF NOT EXISTS release AS OF VERSION 42",
        )
        .unwrap();
        let Some(ProductSqlCommand::Catalog(command)) =
            lower_product_sql_command(&statement).unwrap()
        else {
            panic!("expected a catalog command");
        };
        let CatalogSqlCommand::AlterIcebergTable(command) = *command else {
            panic!("expected an Iceberg table command");
        };
        let IcebergTableSqlAction::Reference(action) = command.action else {
            panic!("expected a reference action");
        };
        let ConnectorRefAction::Create {
            kind,
            name,
            snapshot_id,
            policy,
            ..
        } = connector_ref_action(
            &action,
            &ConnectorProviderId::parse("iceberg").unwrap(),
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap()
        else {
            panic!("expected create ref action");
        };
        assert_eq!(kind, ConnectorRefKind::Branch);
        assert_eq!(name.as_ref(), "release");
        assert_eq!(snapshot_id, Some(42));
        assert_eq!(policy, CreateOrReplacePolicy::NoOpIfExists);
    }

    #[test]
    fn legacy_tag_binding_keeps_bigint_snapshot_and_drop_policy() {
        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
        let action = |sql| {
            let statement = parse_single_statement(sql).unwrap();
            let Some(ProductSqlCommand::Catalog(command)) =
                lower_product_sql_command(&statement).unwrap()
            else {
                panic!("expected catalog command");
            };
            let CatalogSqlCommand::AlterIcebergTable(command) = *command else {
                panic!("reference command");
            };
            let IcebergTableSqlAction::Reference(action) = command.action else {
                panic!("reference action");
            };
            connector_ref_action(&action, &provider, &control).unwrap()
        };
        let ConnectorRefAction::Create {
            kind,
            name,
            snapshot_id,
            policy,
            ..
        } = action(
            "ALTER TABLE ice.db.orders CREATE TAG IF NOT EXISTS release AS OF VERSION 9223372036854775806",
        )
        else {
            panic!("create tag");
        };
        assert_eq!(kind, ConnectorRefKind::Tag);
        assert_eq!(name.as_ref(), "release");
        assert_eq!(snapshot_id, Some(i64::MAX - 1));
        assert_eq!(policy, CreateOrReplacePolicy::NoOpIfExists);
        let ConnectorRefAction::Drop { kind, name, policy } =
            action("ALTER TABLE ice.db.orders DROP TAG IF EXISTS release")
        else {
            panic!("drop tag");
        };
        assert_eq!(kind, ConnectorRefKind::Tag);
        assert_eq!(name.as_ref(), "release");
        assert_eq!(policy, DropPolicy::NoOpIfMissing);
    }

    struct LiteralConstants;

    impl novarocks_query_application::sql::property_evaluation::DdlPropertyConstantPort
        for LiteralConstants
    {
        fn evaluate(
            &mut self,
            expression: &novarocks_parser::ast::Expr,
            target: novarocks_sql::compiler::RequiredConstantTarget,
        ) -> Result<
            novarocks_functions::ConstantValue,
            novarocks_query_application::sql::property_evaluation::DdlPropertyConstantError,
        > {
            use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
            use novarocks_parser::ast::{Expr, LiteralKind};
            use novarocks_sql::compiler::RequiredConstantTarget;

            let Expr::Literal(literal) = expression else {
                panic!("expected a literal in tag procedure test");
            };
            let data = match (target, &literal.kind) {
                (RequiredConstantTarget::Varchar, LiteralKind::String(value)) => {
                    StringArray::from(vec![value.as_str()]).to_data()
                }
                (RequiredConstantTarget::Bigint, LiteralKind::Long { parsed_value, .. }) => {
                    Int64Array::from(vec![*parsed_value]).to_data()
                }
                (RequiredConstantTarget::Boolean, LiteralKind::Boolean(value)) => {
                    BooleanArray::from(vec![*value]).to_data()
                }
                _ => panic!("unexpected tag procedure literal"),
            };
            let data_type = data.data_type().clone();
            let pool = novarocks_functions::ConstantPool::try_new(
                Arc::new(arrow::datatypes::Field::new(
                    "tag argument",
                    data_type.clone(),
                    false,
                )),
                novarocks_type_contract::FunctionValueType::new(data_type, false),
                data,
                crate::application::test_constant_policy(),
                novarocks_type_contract::CompilePhase::Validate,
                &novarocks_sql::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
            Ok(pool.value(0).unwrap())
        }
    }

    struct NoTimestampArguments;

    impl novarocks_query_application::sql::table_procedure::TableProcedureAbsoluteTimePort
        for NoTimestampArguments
    {
        fn legacy_timestamp_literal_millis(
            &mut self,
            _text: &str,
            _span: Span,
        ) -> Result<i64, QueryServiceError> {
            panic!("tag procedures do not accept timestamps");
        }
    }

    fn project_sql(source: &str) -> Result<ConnectorRefAction, QueryServiceError> {
        use novarocks_parser::{ast::TrinoStatement, trino_parser::parse_statement};
        use novarocks_query_application::sql::trino_table_command::bind_trino_table_execute;

        let TrinoStatement::TableExecute(statement) = parse_statement(source).unwrap() else {
            panic!("expected a Trino table procedure");
        };
        let bound = bind_trino_table_execute(
            source,
            &statement,
            &ConnectorProviderId::parse("iceberg").unwrap(),
            &mut LiteralConstants,
            &mut NoTimestampArguments,
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )?;
        project_trino_tag(source, statement.span, bound).map(|action| action.0)
    }

    #[test]
    fn trino_tag_procedure_retains_exact_anchor_and_create_policy() {
        let expected = |snapshot_id, policy| ConnectorRefAction::Create {
            kind: ConnectorRefKind::Tag,
            name: Arc::from("Release-v1"),
            snapshot_id,
            policy,
            expected_table_uuid: None,
        };
        assert_eq!(
            project_sql("ALTER TABLE ice.db.orders EXECUTE create_tag('Release-v1')").unwrap(),
            expected(None, CreateOrReplacePolicy::FailIfExists),
        );
        assert_eq!(
            project_sql(
                "ALTER TABLE ice.db.orders EXECUTE create_tag(snapshot_id => 9223372036854775806, if_not_exists => true, name => 'Release-v1')",
            )
            .unwrap(),
            expected(Some(i64::MAX - 1), CreateOrReplacePolicy::NoOpIfExists),
        );
        assert_eq!(
            project_sql(
                "ALTER TABLE ice.db.orders EXECUTE create_tag(name => 'Release-v1', replace => true, if_not_exists => true)",
            )
            .unwrap(),
            expected(None, CreateOrReplacePolicy::ReplaceIfExists),
        );
    }

    #[test]
    fn trino_tag_procedure_retains_drop_policy_and_locates_missing_name() {
        for (sql, policy) in [
            (
                "ALTER TABLE ice.db.orders EXECUTE drop_tag('Release-v1')",
                DropPolicy::FailIfMissing,
            ),
            (
                "ALTER TABLE ice.db.orders EXECUTE DROP_TAG(if_exists => true, name => 'Release-v1')",
                DropPolicy::NoOpIfMissing,
            ),
        ] {
            assert_eq!(
                project_sql(sql).unwrap(),
                ConnectorRefAction::Drop {
                    kind: ConnectorRefKind::Tag,
                    name: Arc::from("Release-v1"),
                    policy,
                },
            );
        }
        let source = "ALTER TABLE ice.db.orders EXECUTE create_tag(replace => true)";
        let error = project_sql(source).unwrap_err();
        let user = error.user_error().unwrap();
        assert_eq!(
            user.code().as_str(),
            "sql.admit.invalid_table_procedure_argument"
        );
        assert_eq!(
            user.location(),
            Some(&Span::new(0, source.len()).to_user_error_location(source)),
        );
        assert!(user.message().contains("requires 'name'"));
    }

    #[test]
    fn semantic_ref_rejects_non_numeric_version_anchor() {
        let statement =
            parse_single_statement("ALTER TABLE ice.db.orders CREATE TAG v1 AS OF VERSION '42'")
                .unwrap();
        assert_eq!(
            lower_product_sql_command(&statement).unwrap_err(),
            "Iceberg reference version must be a numeric literal"
        );
    }

    #[test]
    fn semantic_ref_rejects_uninterpreted_retention_options() {
        let statement =
            parse_single_statement("ALTER TABLE ice.db.orders CREATE BRANCH release RETAIN 7 DAYS")
                .unwrap();
        assert_eq!(
            lower_product_sql_command(&statement).unwrap_err(),
            "Iceberg reference retention options are not supported"
        );
    }
}
