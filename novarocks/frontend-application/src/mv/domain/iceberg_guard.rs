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

use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorControlResolver, ConnectorInstanceId,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableObjectCaptureRequest,
    ConnectorTableObjectSelector, ConnectorTableResolution,
};
use std::collections::HashSet;
use std::sync::Arc;

use crate::catalog_application::resolver::TargetBackend;
use novarocks_types::naming::normalize_identifier;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergMvUserMutation {
    Insert,
    Update,
    Delete,
    Merge,
    Truncate,
    DropTable,
    AlterTable,
    Maintenance,
}

impl IcebergMvUserMutation {
    fn guidance(self) -> &'static str {
        match self {
            IcebergMvUserMutation::Insert
            | IcebergMvUserMutation::Update
            | IcebergMvUserMutation::Delete
            | IcebergMvUserMutation::Merge
            | IcebergMvUserMutation::Truncate => "use REFRESH MATERIALIZED VIEW to update it",
            IcebergMvUserMutation::DropTable => "use DROP MATERIALIZED VIEW",
            IcebergMvUserMutation::AlterTable => {
                "use ALTER MATERIALIZED VIEW for MV metadata changes"
            }
            IcebergMvUserMutation::Maintenance => {
                "use ALTER MATERIALIZED VIEW or DROP MATERIALIZED VIEW"
            }
        }
    }
}

/// Reject a user mutation of an Iceberg-backed materialized-view table.
///
/// The guard observes the canonical managed marker through the exact-generation
/// document lease. Command kernels use this entry directly; they must not
/// reconstruct an application facade or obtain a provider through the retired
/// connector registry.
pub fn reject_if_iceberg_mv_table_with_ports(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    target: &TargetBackend,
    mutation: IcebergMvUserMutation,
) -> Result<(), String> {
    if target.provider_id.as_str() != "iceberg" {
        return Ok(());
    }

    let instance_id = ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| format!("parse Iceberg catalog identity for MV guard: {error}"))?;
    let exact_lease = ConnectorControlResolver::acquire_current(connector_control, &instance_id)
        .map_err(|error| format!("acquire exact Iceberg generation for MV guard: {error}"))?;
    let context = crate::connector::connector_request_context(
        None,
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
    )?;
    reject_if_iceberg_mv_table_with_planning_lease_and_context(
        storage_observation,
        &exact_lease,
        readiness,
        target,
        mutation,
        context,
    )
}

/// Apply the MV mutation guard through an already-admitted exact generation
/// and request context. Query-scoped callers use this form so any vended REST
/// response contributes to the same attempt collector rather than reopening
/// metadata under an unbound control-only context.
pub fn reject_if_iceberg_mv_table_with_planning_lease_and_context(
    _storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    exact_lease: &ConnectorControlPlanningLease,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    target: &TargetBackend,
    mutation: IcebergMvUserMutation,
    context: ConnectorRequestContext,
) -> Result<(), String> {
    if target.provider_id.as_str() != "iceberg" {
        return Ok(());
    }

    let instance_id = ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| format!("parse Iceberg catalog identity for MV guard: {error}"))?;
    if exact_lease.binding().descriptor().instance_id != instance_id {
        return Err(
            "MV mutation guard planning lease does not match the target connector".to_string(),
        );
    }
    let context = crate::connector::context_for_planning_lease(exact_lease, context)?;
    let identity = ConnectorTableIdentity {
        instance_id,
        namespace: Arc::from(target.namespace.as_str()),
        table: Arc::from(target.table.as_str()),
    };
    let binding = exact_lease
        .binding()
        .metadata()
        .capture_table_object_binding(ConnectorTableObjectCaptureRequest {
            table: identity.clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context: context.clone(),
        })
        .map_err(|error| format!("bind MV mutation guard target: {error}"))?;
    if binding.metadata.identity != identity {
        return Err(
            "connector loaded a different table while checking the MV mutation guard".to_string(),
        );
    }
    let documents = exact_lease
        .derive_document_storage_lease()
        .map_err(|error| format!("derive MV mutation guard document lease: {error}"))?;
    let request = crate::mv::domain::staged_create::keyed_request(
        &documents,
        identity,
        binding.object_id,
        context,
    );
    let view = match documents.observe_keyed_documents(request.clone()) {
        Ok(view) => view,
        Err(novarocks_spi::connector::document_storage::KeyedDocumentReadError::LegacyFormat {
            key,
        }) => {
            return if mutation == IcebergMvUserMutation::DropTable {
                Ok(())
            } else {
                Err(format!("LegacyFormat: {key}; only DROP TABLE is supported"))
            };
        }
        Err(error) => return Err(format!("observe MV mutation guard: {error}")),
    };
    if view.management.is_some()
        || view.slots.keys().any(|slot| slot.owner == "novarocks.mv")
        || view.versions.values().any(|v| v.owner() == "novarocks.mv")
    {
        // The Provider marker decides denial. Keep its actual negative
        // management observation, without refreshing an unrelated Accelerator.
        readiness
            .revoke_foreign_current_management(&view, &request.catalog_handle)
            .map_err(|error| error.to_string())?;
        return Err(format!(
            "table {}.{}.{} is a materialized view; {}",
            target.catalog,
            target.namespace,
            target.table,
            mutation.guidance()
        ));
    }

    Ok(())
}

/// Same exact lease/marker/LegacyFormat guard after direct physical awaits.
pub(crate) async fn reject_if_iceberg_mv_table_with_planning_lease_and_context_async(
    _storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    exact_lease: &ConnectorControlPlanningLease,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    target: &TargetBackend,
    mutation: IcebergMvUserMutation,
    context: ConnectorRequestContext,
) -> Result<(), novarocks_query_application::session_error::QueryServiceError> {
    if target.provider_id.as_str() != "iceberg" {
        return Ok(());
    }

    let instance_id = ConnectorInstanceId::parse(&target.catalog).map_err(|error| {
        super::readiness::application_error(format!(
            "parse Iceberg catalog identity for MV guard: {error}"
        ))
    })?;
    if exact_lease.binding().descriptor().instance_id != instance_id {
        return Err(super::readiness::application_error(
            "MV mutation guard planning lease does not match the target connector",
        ));
    }
    let context = crate::connector::context_for_planning_lease_typed(exact_lease, context)
        .map_err(super::readiness::connector_application_error)?;
    let identity = ConnectorTableIdentity {
        instance_id,
        namespace: Arc::from(target.namespace.as_str()),
        table: Arc::from(target.table.as_str()),
    };
    let binding = exact_lease
        .binding()
        .metadata()
        .capture_table_object_binding_async(ConnectorTableObjectCaptureRequest {
            table: identity.clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context: context.clone(),
        })
        .await
        .map_err(super::readiness::connector_application_error)?;
    reject_bound_iceberg_mv_table_async(
        exact_lease,
        readiness,
        target,
        mutation,
        context,
        identity,
        binding,
    )
    .await
}

/// Same original guard algorithm, with the selected generation's governed binding CPU.
pub(crate) async fn reject_if_iceberg_mv_table_with_governed_planning_lease_and_context_async(
    _storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    exact_lease: &ConnectorControlPlanningLease,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    target: &TargetBackend,
    mutation: IcebergMvUserMutation,
    context: ConnectorRequestContext,
    cpu: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
) -> Result<(), novarocks_query_application::session_error::QueryServiceError> {
    if target.provider_id.as_str() != "iceberg" {
        return Ok(());
    }

    let instance_id = ConnectorInstanceId::parse(&target.catalog).map_err(|error| {
        super::readiness::application_error(format!(
            "parse Iceberg catalog identity for MV guard: {error}"
        ))
    })?;
    if exact_lease.binding().descriptor().instance_id != instance_id {
        return Err(super::readiness::application_error(
            "MV mutation guard planning lease does not match the target connector",
        ));
    }
    let context = crate::connector::context_for_planning_lease_typed(exact_lease, context)
        .map_err(super::readiness::connector_application_error)?;
    let identity = ConnectorTableIdentity {
        instance_id,
        namespace: Arc::from(target.namespace.as_str()),
        table: Arc::from(target.table.as_str()),
    };
    let (binding, _) = exact_lease
        .binding()
        .metadata()
        .capture_table_object_with_snapshot_cpu(
            ConnectorTableObjectCaptureRequest {
                table: identity.clone(),
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context: context.clone(),
            },
            cpu,
        )
        .await
        .map_err(super::readiness::connector_application_error)?;
    reject_bound_iceberg_mv_table_async(
        exact_lease,
        readiness,
        target,
        mutation,
        context,
        identity,
        binding,
    )
    .await
}

async fn reject_bound_iceberg_mv_table_async(
    exact_lease: &ConnectorControlPlanningLease,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    target: &TargetBackend,
    mutation: IcebergMvUserMutation,
    context: ConnectorRequestContext,
    identity: ConnectorTableIdentity,
    binding: novarocks_spi::connector::ConnectorTableObjectBinding,
) -> Result<(), novarocks_query_application::session_error::QueryServiceError> {
    if binding.metadata.identity != identity {
        return Err(super::readiness::application_error(
            "connector loaded a different table while checking the MV mutation guard",
        ));
    }
    let documents = exact_lease
        .derive_document_storage_lease()
        .map_err(super::readiness::connector_application_error)?;
    let request = crate::mv::domain::staged_create::keyed_request(
        &documents,
        identity,
        binding.object_id,
        context,
    );
    let view = match documents
        .observe_keyed_documents_async(request.clone())
        .await
    {
        Ok(view) => view,
        Err(novarocks_spi::connector::document_storage::KeyedDocumentReadError::LegacyFormat {
            key,
        }) => {
            return if mutation == IcebergMvUserMutation::DropTable {
                Ok(())
            } else {
                Err(super::readiness::application_error(format!(
                    "LegacyFormat: {key}; only DROP TABLE is supported"
                )))
            };
        }
        Err(novarocks_spi::connector::document_storage::KeyedDocumentReadError::Invalid(error)) => {
            return Err(super::readiness::connector_application_error(error));
        }
    };
    if view.management.is_some()
        || view.slots.keys().any(|slot| slot.owner == "novarocks.mv")
        || view.versions.values().any(|v| v.owner() == "novarocks.mv")
    {
        // Denial needs no projection install, evidence resolution or reload.
        // Existing unsettled effects remain with their original management owner.
        readiness.revoke_foreign_current_management(&view, &request.catalog_handle)?;
        return Err(materialized_view_denial(target, mutation));
    }

    Ok(())
}

fn materialized_view_denial(
    target: &TargetBackend,
    mutation: IcebergMvUserMutation,
) -> novarocks_query_application::session_error::QueryServiceError {
    super::readiness::application_error(format!(
        "table {}.{}.{} is a materialized view; {}",
        target.catalog,
        target.namespace,
        target.table,
        mutation.guidance()
    ))
}

/// Preserve the frontend-owned MV dependency policy before a provider schema
/// mutation. Physical schema and equality-delete validation remain provider
/// responsibilities.
/// Apply the MV dependency policy using ready lake-derived Accelerator
/// projections. A quarantined package must never keep a stale schema guard
/// alive, nor can a consumer read around current-process readiness.
pub(crate) fn reject_drop_column_mv_dependencies_with_readiness(
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    target: &TargetBackend,
    column_path: &crate::catalog_application::statement::ColumnPath,
) -> Result<(), String> {
    let leaf = column_path
        .last()
        .ok_or_else(|| "DROP COLUMN has an empty column path".to_string())?;
    let leaf = normalize_identifier(leaf)?;
    let target = MvDependencyTarget::from_backend(target)?;
    for projection in readiness
        .list_ready_projections()
        .map_err(|error| format!("load materialized view metadata failed: {error}"))?
    {
        let definition = projection.projection.facts.definition();
        let matching_occurrences = definition
            .relation_occurrences
            .iter()
            .filter(|occurrence| {
                occurrence
                    .catalog_at_binding
                    .eq_ignore_ascii_case(&target.catalog)
                    && occurrence
                        .namespace_at_binding
                        .eq_ignore_ascii_case(&target.namespace)
                    && occurrence
                        .relation_at_binding
                        .eq_ignore_ascii_case(&target.table)
            })
            .collect::<Vec<_>>();
        let references_column = matching_occurrences.iter().any(|occurrence| {
            occurrence
                .fields
                .iter()
                .any(|field| field.name_at_binding.eq_ignore_ascii_case(&leaf))
        });
        if !matching_occurrences.is_empty()
            && (references_column
                || sql_projects_target_wildcard(&definition.query.effective_sql, &target))
        {
            return Err(format!(
                "DROP COLUMN `{}` is blocked because a materialized view references it",
                column_path.dotted()
            ));
        }
    }
    Ok(())
}

#[derive(Clone)]
struct MvDependencyTarget {
    catalog: String,
    namespace: String,
    table: String,
}

impl MvDependencyTarget {
    fn from_backend(target: &TargetBackend) -> Result<Self, String> {
        Ok(Self {
            catalog: normalize_identifier(&target.catalog)?,
            namespace: normalize_identifier(&target.namespace)?,
            table: normalize_identifier(&target.table)?,
        })
    }
}

fn sql_projects_target_wildcard(sql: &str, target: &MvDependencyTarget) -> bool {
    let Ok(statements) = novarocks_parser::parse(sql) else {
        return false;
    };
    let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
        return false;
    };
    query_projects_target_wildcard(query, target)
}

fn query_projects_target_wildcard(
    query: &novarocks_parser::ast::Query,
    target: &MvDependencyTarget,
) -> bool {
    if query.with.as_ref().is_some_and(|with| {
        with.ctes
            .iter()
            .any(|cte| query_projects_target_wildcard(&cte.query, target))
    }) {
        return true;
    }
    set_expr_projects_target_wildcard(query.body.as_ref(), target)
}

fn set_expr_projects_target_wildcard(
    set_expr: &novarocks_parser::ast::SetExpr,
    target: &MvDependencyTarget,
) -> bool {
    match set_expr {
        novarocks_parser::ast::SetExpr::Select(select) => {
            select_projects_target_wildcard(select, target)
        }
        novarocks_parser::ast::SetExpr::Query(query) => {
            query_projects_target_wildcard(query, target)
        }
        novarocks_parser::ast::SetExpr::SetOperation(operation) => {
            set_expr_projects_target_wildcard(&operation.left, target)
                || set_expr_projects_target_wildcard(&operation.right, target)
        }
        _ => false,
    }
}

fn select_projects_target_wildcard(
    select: &novarocks_parser::ast::Select,
    target: &MvDependencyTarget,
) -> bool {
    let mut qualifiers = HashSet::new();
    if select
        .from
        .iter()
        .any(|from| collect_target_qualifiers_from_table_with_joins(from, target, &mut qualifiers))
    {
        return true;
    }
    select.projection.iter().any(|item| match item {
        novarocks_parser::ast::SelectItem::Wildcard { .. } => !qualifiers.is_empty(),
        novarocks_parser::ast::SelectItem::QualifiedWildcard { prefix, .. } => prefix
            .iter()
            .map(|ident| normalize_identifier(&ident.value))
            .collect::<Result<Vec<_>, _>>()
            .map(|parts| qualifier_keys_from_parts(&parts))
            .unwrap_or_default()
            .into_iter()
            .any(|key| qualifiers.contains(&key)),
        _ => false,
    })
}

fn collect_target_qualifiers_from_table_with_joins(
    table: &novarocks_parser::ast::TableWithJoins,
    target: &MvDependencyTarget,
    qualifiers: &mut HashSet<String>,
) -> bool {
    collect_target_qualifiers_from_factor(&table.relation, target, qualifiers)
        || table
            .joins
            .iter()
            .any(|join| collect_target_qualifiers_from_factor(&join.relation, target, qualifiers))
}

fn collect_target_qualifiers_from_factor(
    factor: &novarocks_parser::ast::TableFactor,
    target: &MvDependencyTarget,
    qualifiers: &mut HashSet<String>,
) -> bool {
    match factor {
        novarocks_parser::ast::TableFactor::Table { name, alias, .. } => {
            if object_name_matches_target(name, target) {
                qualifiers.extend(object_name_qualifier_keys(name));
                if let Some(alias) = alias
                    && let Ok(normalized) = normalize_identifier(&alias.name.value)
                {
                    qualifiers.insert(normalized);
                }
            }
            false
        }
        novarocks_parser::ast::TableFactor::Derived { subquery, .. } => {
            query_projects_target_wildcard(subquery, target)
        }
        novarocks_parser::ast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_target_qualifiers_from_table_with_joins(table_with_joins, target, qualifiers),
        _ => false,
    }
}

fn object_name_matches_target(
    name: &novarocks_parser::ast::ObjectName,
    target: &MvDependencyTarget,
) -> bool {
    match normalized_object_name_parts(name).as_deref() {
        Some([catalog, namespace, table]) => {
            catalog == &target.catalog && namespace == &target.namespace && table == &target.table
        }
        Some([namespace, table]) => namespace == &target.namespace && table == &target.table,
        Some([table]) => table == &target.table,
        _ => false,
    }
}

fn object_name_qualifier_keys(name: &novarocks_parser::ast::ObjectName) -> Vec<String> {
    normalized_object_name_parts(name)
        .map(|parts| qualifier_keys_from_parts(&parts))
        .unwrap_or_default()
}

fn qualifier_keys_from_parts(parts: &[String]) -> Vec<String> {
    let Some(last) = parts.last() else {
        return Vec::new();
    };
    let mut keys = vec![parts.join("."), last.clone()];
    if parts.len() >= 2 {
        keys.push(parts[parts.len() - 2..].join("."));
    }
    keys
}

fn normalized_object_name_parts(name: &novarocks_parser::ast::ObjectName) -> Option<Vec<String>> {
    name.parts
        .iter()
        .map(|ident| normalize_identifier(&ident.value))
        .collect::<Result<Vec<_>, _>>()
        .ok()
}

#[cfg(test)]
#[path = "iceberg_guard_context_tests.rs"]
mod context_tests;
