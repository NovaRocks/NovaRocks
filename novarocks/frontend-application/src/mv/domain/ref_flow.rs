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

use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorInstanceId, ConnectorRefAction, ConnectorRefKind,
    ConnectorTableIdentity, CreateOrReplacePolicy, DropPolicy, ExternalMutationFinalization,
};
use novarocks_sql::semantic::ObjectName;
use novarocks_sql::semantic::command::{
    AlterIcebergTableSqlCommand, CommandLiteral, IcebergReferenceKindSql,
    IcebergReferenceSqlAction, IcebergTableSqlAction, ReferenceAnchorSql,
};

/// Execute an Iceberg ref mutation using only the explicit connector-control
/// and MV storage-observation ports required for MV-target admission.
pub(crate) fn execute_with_ports(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    _current_database: &str,
    stmt: &AlterIcebergTableSqlCommand,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    crate::connector::validate_request_context(connector_context)?;
    // 1. Resolve qualified name — must be 3-part (catalog.namespace.table).
    let IcebergTableSqlAction::Reference(action) = &stmt.action else {
        return Err("Iceberg ref executor received a non-reference action".to_string());
    };
    let (catalog_name, namespace, table_name) = resolve_table_parts(&stmt.table)?;

    // Retain one exact generation across MV admission and the ref mutation.
    // The application never decodes the provider-owned table handle.
    let exact_lease =
        crate::connector::acquire_metadata_planning_lease(connector_control, &catalog_name)?;
    let target = crate::catalog_application::resolver::TargetBackend {
        provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .expect("static Iceberg provider ID"),
        catalog: catalog_name.clone(),
        namespace: namespace.clone(),
        table: table_name.clone(),
    };
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_planning_lease_and_context(
        storage_observation,
        &exact_lease,
        &target,
        crate::mv::domain::iceberg_guard::IcebergMvUserMutation::AlterTable,
        connector_context.clone(),
    )
    .map_err(|error| format!("guard ALTER TABLE reference: {error}"))?;
    let instance_id =
        ConnectorInstanceId::parse(&catalog_name).map_err(|error| error.to_string())?;
    if exact_lease.binding().descriptor().instance_id != instance_id {
        return Err("connector planning lease identity changed during ALTER TABLE ref".to_string());
    }
    let mutation_lease = exact_lease
        .derive_mutation_lease()
        .map_err(|error| error.to_string())?;
    let outcome = crate::connector::mutation::dispatch_catalog_mutation_once_with_lease(
        &mutation_lease,
        novarocks_spi::connector::ConnectorMutationOperationId::new(),
        ConnectorCatalogMutationOperation::AlterRef {
            table: ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(namespace.as_str()),
                table: Arc::from(table_name.as_str()),
            },
            action: connector_ref_action(action)?,
        },
        connector_context.clone(),
    );
    match outcome {
        crate::connector::mutation::ResolvedCatalogMutation::KnownCommitted(completed) => {
            if let ExternalMutationFinalization::Failed(failure) = completed.finalization {
                return Err(
                    novarocks_query_application::engine_error::EngineError::commit_known_committed_finalize_failed(
                        failure.to_string(),
                    )
                    .to_string(),
                );
            }
        }
        crate::connector::mutation::ResolvedCatalogMutation::KnownUncommitted {
            failure,
            cleanup,
        } => {
            return Err(
                novarocks_query_application::engine_error::EngineError::commit_known_uncommitted(
                    crate::connector::mutation::known_uncommitted_message(failure, &cleanup),
                )
                .to_string(),
            );
        }
        crate::connector::mutation::ResolvedCatalogMutation::CommitUnknown { failure, .. } => {
            return Err(
                novarocks_query_application::engine_error::EngineError::commit_unknown(
                    failure.to_string(),
                )
                .to_string(),
            );
        }
        crate::connector::mutation::ResolvedCatalogMutation::ContractFailure { error, .. } => {
            return Err(error.to_string());
        }
    }

    Ok(StatementResult::Ok)
}

fn connector_ref_action(action: &IcebergReferenceSqlAction) -> Result<ConnectorRefAction, String> {
    let policy = |replace: bool, if_not_exists: bool| {
        if replace {
            CreateOrReplacePolicy::ReplaceIfExists
        } else if if_not_exists {
            CreateOrReplacePolicy::NoOpIfExists
        } else {
            CreateOrReplacePolicy::FailIfExists
        }
    };
    let snapshot_anchor = |anchor: &ReferenceAnchorSql| match anchor {
        ReferenceAnchorSql::Version(CommandLiteral::Number(value)) => value
            .parse::<i64>()
            .map(Some)
            .map_err(|_| "Iceberg reference version must fit i64".to_string()),
        ReferenceAnchorSql::Version(_) => {
            Err("Iceberg reference version must be a numeric literal".to_string())
        }
        ReferenceAnchorSql::CurrentMain => Ok(None),
    };
    Ok(match action {
        IcebergReferenceSqlAction::Create {
            kind,
            name,
            anchor,
            if_not_exists,
            or_replace,
            has_uninterpreted_provider_options: _,
        } => ConnectorRefAction::Create {
            kind: connector_ref_kind(*kind),
            name: Arc::from(name.as_str()),
            snapshot_id: snapshot_anchor(anchor)?,
            policy: policy(*or_replace, *if_not_exists),
            expected_table_uuid: None,
        },
        IcebergReferenceSqlAction::Drop {
            kind,
            name,
            if_exists,
        } => ConnectorRefAction::Drop {
            kind: connector_ref_kind(*kind),
            name: Arc::from(name.as_str()),
            policy: if *if_exists {
                DropPolicy::NoOpIfMissing
            } else {
                DropPolicy::FailIfMissing
            },
        },
    })
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

    #[test]
    fn semantic_create_ref_preserves_anchor_and_policy() {
        let action = IcebergReferenceSqlAction::Create {
            kind: IcebergReferenceKindSql::Branch,
            name: "release".to_string(),
            if_not_exists: true,
            or_replace: false,
            anchor: ReferenceAnchorSql::Version(CommandLiteral::Number("42".to_string())),
            has_uninterpreted_provider_options: true,
        };

        let ConnectorRefAction::Create {
            kind,
            name,
            snapshot_id,
            policy,
            ..
        } = connector_ref_action(&action).expect("lower semantic ref")
        else {
            panic!("expected create ref action");
        };
        assert_eq!(kind, ConnectorRefKind::Branch);
        assert_eq!(name.as_ref(), "release");
        assert_eq!(snapshot_id, Some(42));
        assert_eq!(policy, CreateOrReplacePolicy::NoOpIfExists);
    }

    #[test]
    fn semantic_ref_rejects_non_numeric_version_anchor() {
        let action = IcebergReferenceSqlAction::Create {
            kind: IcebergReferenceKindSql::Tag,
            name: "v1".to_string(),
            if_not_exists: false,
            or_replace: false,
            anchor: ReferenceAnchorSql::Version(CommandLiteral::String("42".to_string())),
            has_uninterpreted_provider_options: false,
        };
        assert!(
            connector_ref_action(&action)
                .expect_err("string snapshot ID must fail")
                .contains("numeric literal")
        );
    }
}
