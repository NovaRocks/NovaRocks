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
        BoundProcedureArgument, BoundProcedureValue, BoundTableProcedure, ProcedureArgumentDefault,
        TableProcedureArgument, TableProcedureKind, invalid_procedure_argument,
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
    _planning: ConnectorControlPlanningPin,
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
    project_tag(source, span, bound).map(TrinoTagAction)
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
    prepare_action(binding, action)?.execute(connector_context)
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
        _planning: planning,
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
) -> Result<PreparedRefMutation, CommandError> {
    let action = connector_branch_action(action)?;
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

fn connector_branch_action(
    action: &IcebergReferenceSqlAction,
) -> Result<ConnectorRefAction, CommandError> {
    let action = match action {
        IcebergReferenceSqlAction::Create {
            kind: IcebergReferenceKindSql::Branch,
            name,
            snapshot_id,
            if_not_exists,
            or_replace,
        } => create_ref(
            ConnectorRefKind::Branch,
            Arc::from(name.as_str()),
            *snapshot_id,
            *or_replace,
            *if_not_exists,
        ),
        IcebergReferenceSqlAction::Drop {
            kind: IcebergReferenceKindSql::Branch,
            name,
            if_exists,
        } => drop_ref(
            ConnectorRefKind::Branch,
            Arc::from(name.as_str()),
            *if_exists,
        ),
        _ => {
            return Err(CommandError::new(
                CommandErrorKind::Failed,
                "A branch mutation received a different reference kind",
            ));
        }
    };
    Ok(action)
}

fn project_tag(
    source: &str,
    span: Span,
    bound: BoundTableProcedure<'_>,
) -> Result<ConnectorRefAction, QueryServiceError> {
    let declaration = bound.declaration();
    let mut slots = bound.into_arguments();
    let name = match tag_argument(&mut slots, "name")? {
        Some(DdlPropertyValue::Varchar(name)) => Arc::from(name),
        None => {
            return Err(invalid_procedure_argument(
                source,
                span,
                format!("Table procedure {} requires 'name'", declaration.name()),
            ));
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

    #[test]
    fn semantic_create_ref_preserves_anchor_and_policy() {
        use novarocks_parser::{ast::TrinoStatement, parser::parse_statement};
        use novarocks_query_application::sql::trino_table_command::prepare_trino_create_branch;
        use novarocks_spi::connector::{DdlPropertyDeclaration, DdlPropertyType};
        use novarocks_sql::semantic::CatalogSqlCommand;

        let source =
            "CREATE BRANCH IF NOT EXISTS release WITH (snapshot_id = 42) IN TABLE ice.db.orders";
        let TrinoStatement::CreateBranch(statement) = parse_statement(source).unwrap() else {
            panic!("expected a branch statement");
        };
        let declarations = [DdlPropertyDeclaration::new(
            "snapshot_id",
            DdlPropertyType::Bigint,
            DdlPropertyValue::Null,
            false,
        )
        .unwrap()];
        let command = prepare_trino_create_branch(
            source,
            &statement,
            &declarations,
            &mut LiteralConstants,
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let CatalogSqlCommand::AlterIcebergTable(command) = command else {
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
        } = connector_branch_action(&action).unwrap()
        else {
            panic!("expected create ref action");
        };
        assert_eq!(kind, ConnectorRefKind::Branch);
        assert_eq!(name.as_ref(), "release");
        assert_eq!(snapshot_id, Some(42));
        assert_eq!(policy, CreateOrReplacePolicy::NoOpIfExists);
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

    fn project_sql(source: &str) -> Result<ConnectorRefAction, QueryServiceError> {
        use novarocks_parser::{ast::TrinoStatement, parser::parse_statement};
        use novarocks_query_application::sql::trino_table_command::bind_trino_table_execute;

        let TrinoStatement::TableExecute(statement) = parse_statement(source).unwrap() else {
            panic!("expected a Trino table procedure");
        };
        let bound = bind_trino_table_execute(
            source,
            &statement,
            &ConnectorProviderId::parse("iceberg").unwrap(),
            &mut LiteralConstants,
            "iceberg",
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
    fn retired_ref_retention_syntax_is_rejected_before_binding() {
        let source = "ALTER TABLE ice.db.orders CREATE BRANCH release RETAIN 7 DAYS";
        let error = novarocks_parser::parser::parse_statement(source).unwrap_err();
        assert_eq!(
            error.to_user_error(source).code().as_str(),
            "sql.parse.unexpected_token"
        );
    }
}
