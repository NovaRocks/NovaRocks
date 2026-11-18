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

use novarocks_query_application::{
    api::{CommandError, CommandErrorKind},
    protocol_delivery::QuerySessionOutput as StatementResult,
    sql::table_procedure::{
        BoundProcedureArgument, BoundProcedureValue, LegacyProcedureValue,
        bind_legacy_procedure_arguments, resolve_legacy_table_procedure,
    },
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorInstanceId, ConnectorProviderId,
    ConnectorRefAction, ConnectorRefKind, ConnectorTableIdentity, CreateOrReplacePolicy,
    DdlPropertyValue, DropPolicy, ExternalMutationFinalization,
};
use novarocks_sql::semantic::ObjectName;
use novarocks_sql::semantic::command::{
    AlterIcebergTableSqlCommand, IcebergReferenceKindSql, IcebergReferenceSqlAction,
    IcebergTableSqlAction,
};

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
    let (catalog_name, namespace, table_name) = resolve_table_parts(&stmt.table)?;
    crate::catalog_application::resolver::require_external_target(&catalog_name, &namespace)?;

    // Retain one exact generation across MV admission and the ref mutation.
    // The application never decodes the provider-owned table handle.
    let exact_lease =
        crate::connector::acquire_metadata_planning_pin(connector_control, &catalog_name)?;
    let target = crate::catalog_application::resolver::TargetBackend {
        provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .expect("static Iceberg provider ID"),
        catalog: catalog_name.clone(),
        namespace: namespace.clone(),
        table: table_name.clone(),
    };
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_planning_pin_and_context(
        storage_observation,
        &exact_lease,
        readiness,
        &target,
        crate::mv::domain::iceberg_guard::IcebergMvUserMutation::AlterTable,
        connector_context.clone(),
    )
    .map_err(|error| format!("guard ALTER TABLE reference: {error}"))?;
    let instance_id =
        ConnectorInstanceId::parse(&catalog_name).map_err(|error| error.to_string())?;
    if exact_lease.binding().descriptor().instance_id != instance_id {
        return Err(
            ("connector planning pin identity changed during ALTER TABLE ref".to_string()).into(),
        );
    }
    let mutation_lease = exact_lease
        .derive_mutation_lease()
        .map_err(|error| error.to_string())?;
    let control = crate::query_execution::planning::sql_compile_control_from_connector_request(
        connector_context,
    );
    let action = connector_ref_action(
        action,
        &exact_lease.binding().descriptor().provider_id,
        &control,
    )?;
    let outcome = crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
        &mutation_lease,
        novarocks_spi::connector::ConnectorMutationOperationId::new(),
        ConnectorCatalogMutationOperation::AlterRef {
            table: ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(namespace.as_str()),
                table: Arc::from(table_name.as_str()),
            },
            action,
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
        crate::connector::mutation::ResolvedCatalogMutation::CommitUnknown { failure, .. } => {
            return Err(
                (novarocks_query_application::engine_error::EngineError::commit_unknown(
                    failure.to_string(),
                )
                .to_string())
                .into(),
            );
        }
        crate::connector::mutation::ResolvedCatalogMutation::ContractFailure { error, .. } => {
            return Err((error.to_string()).into());
        }
    }

    Ok(StatementResult::Ok)
}

fn connector_ref_action(
    action: &IcebergReferenceSqlAction,
    provider: &ConnectorProviderId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ConnectorRefAction, CommandError> {
    let policy = |replace: bool, if_not_exists: bool| {
        if replace {
            CreateOrReplacePolicy::ReplaceIfExists
        } else if if_not_exists {
            CreateOrReplacePolicy::NoOpIfExists
        } else {
            CreateOrReplacePolicy::FailIfExists
        }
    };
    Ok(match action {
        IcebergReferenceSqlAction::Create {
            kind,
            name,
            snapshot_id,
            if_not_exists,
            or_replace,
        } => {
            let (name, snapshot_id, or_replace, if_not_exists) =
                if *kind == IcebergReferenceKindSql::Tag {
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
                    let bound = bind_tag(provider, "create_tag", &inputs, control)?;
                    let mut slots = bound.into_arguments();
                    let (
                        Some(DdlPropertyValue::Varchar(name)),
                        snapshot,
                        Some(DdlPropertyValue::Boolean(or_replace)),
                        Some(DdlPropertyValue::Boolean(if_not_exists)),
                    ) = (
                        tag_argument(&mut slots, "name")?,
                        tag_argument(&mut slots, "snapshot_id")?,
                        tag_argument(&mut slots, "replace")?,
                        tag_argument(&mut slots, "if_not_exists")?,
                    )
                    else {
                        return Err(tag_binding_invariant());
                    };
                    let snapshot = match snapshot {
                        Some(DdlPropertyValue::Bigint(id)) => Some(id),
                        None => None,
                        _ => return Err(tag_binding_invariant()),
                    };
                    (Arc::from(name), snapshot, or_replace, if_not_exists)
                } else {
                    (
                        Arc::from(name.as_str()),
                        *snapshot_id,
                        *or_replace,
                        *if_not_exists,
                    )
                };
            ConnectorRefAction::Create {
                kind: connector_ref_kind(*kind),
                name,
                snapshot_id,
                policy: policy(or_replace, if_not_exists),
                expected_table_uuid: None,
            }
        }
        IcebergReferenceSqlAction::Drop {
            kind,
            name,
            if_exists,
        } => {
            let (name, if_exists) = if *kind == IcebergReferenceKindSql::Tag {
                let bound = bind_tag(
                    provider,
                    "drop_tag",
                    &[
                        ("name", LegacyProcedureValue::Varchar(name)),
                        ("if_exists", LegacyProcedureValue::Boolean(*if_exists)),
                    ],
                    control,
                )?;
                let mut slots = bound.into_arguments();
                let (
                    Some(DdlPropertyValue::Varchar(name)),
                    Some(DdlPropertyValue::Boolean(if_exists)),
                ) = (
                    tag_argument(&mut slots, "name")?,
                    tag_argument(&mut slots, "if_exists")?,
                )
                else {
                    return Err(tag_binding_invariant());
                };
                (Arc::from(name), if_exists)
            } else {
                (Arc::from(name.as_str()), *if_exists)
            };
            ConnectorRefAction::Drop {
                kind: connector_ref_kind(*kind),
                name,
                policy: if if_exists {
                    DropPolicy::NoOpIfMissing
                } else {
                    DropPolicy::FailIfMissing
                },
            }
        }
    })
}

fn bind_tag<'a>(
    provider: &ConnectorProviderId,
    procedure: &str,
    arguments: &[(&str, LegacyProcedureValue<'a>)],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<novarocks_query_application::sql::table_procedure::BoundTableProcedure<'a>, CommandError>
{
    let procedure = resolve_legacy_table_procedure(provider, procedure)
        .map_err(crate::query::procedure_command_error)?;
    bind_legacy_procedure_arguments(procedure, arguments, control)
        .map_err(crate::query::procedure_command_error)
}

fn tag_argument<'a>(
    slots: &mut impl Iterator<
        Item = (
            &'static novarocks_query_application::sql::table_procedure::TableProcedureArgument,
            BoundProcedureArgument<'a>,
        ),
    >,
    name: &str,
) -> Result<Option<DdlPropertyValue>, CommandError> {
    match slots.next() {
        Some((slot, argument)) if slot.name == name => match argument {
            BoundProcedureArgument::Omitted => Ok(None),
            BoundProcedureArgument::Present {
                value: BoundProcedureValue::Value(value),
                ..
            } => Ok(Some(value)),
            _ => Err(tag_binding_invariant()),
        },
        _ => Err(tag_binding_invariant()),
    }
}

fn tag_binding_invariant() -> CommandError {
    CommandError::new(
        CommandErrorKind::Failed,
        "Legacy tag binding differs from its original procedure declaration",
    )
}

const fn connector_ref_kind(kind: IcebergReferenceKindSql) -> ConnectorRefKind {
    match kind {
        IcebergReferenceKindSql::Branch => ConnectorRefKind::Branch,
        IcebergReferenceKindSql::Tag => ConnectorRefKind::Tag,
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
