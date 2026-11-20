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
    ConnectorControlPlanningPin, ConnectorControlResolver, ConnectorInstanceId,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableObjectCaptureRequest,
    ConnectorTableObjectId, ConnectorTableObjectSelector, ConnectorTableResolution,
};
use std::collections::HashSet;
use std::sync::Arc;

use crate::catalog_application::resolver::TargetBackend;
use novarocks_types::naming::normalize_identifier;

/// Read-only classification from the document owner's actual managed facts.
pub(crate) fn is_materialized_view_documents(
    view: &novarocks_spi::connector::KeyedDocumentView,
) -> bool {
    view.management.is_some()
        || view.slots.keys().any(|slot| slot.owner == "novarocks.mv")
        || view
            .versions
            .values()
            .any(|version| version.owner() == "novarocks.mv")
}

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
    reject_if_iceberg_mv_table_with_planning_pin_and_context(
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
pub fn reject_if_iceberg_mv_table_with_planning_pin_and_context(
    _storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    exact_lease: &ConnectorControlPlanningPin,
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
            "MV mutation guard planning pin does not match the target connector".to_string(),
        );
    }
    let context = crate::connector::context_for_planning_pin(exact_lease, context)?;
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
    if is_materialized_view_documents(&view) {
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
pub(crate) async fn reject_if_iceberg_mv_table_with_planning_pin_and_context_async(
    _storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    exact_lease: &ConnectorControlPlanningPin,
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
            "MV mutation guard planning pin does not match the target connector",
        ));
    }
    let context = crate::connector::context_for_planning_pin_typed(exact_lease, context)
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
    .map(drop)
}

/// Same original guard algorithm, with the selected generation's governed binding CPU.
pub(crate) async fn reject_if_iceberg_mv_table_with_governed_planning_pin_and_context_async(
    _storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    exact_lease: &ConnectorControlPlanningPin,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    target: &TargetBackend,
    mutation: IcebergMvUserMutation,
    context: ConnectorRequestContext,
    cpu: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
) -> Result<(), novarocks_query_application::session_error::QueryServiceError> {
    guarded_iceberg_table_object_with_cpu(exact_lease, readiness, target, mutation, context, cpu)
        .await
        .map(drop)
}

/// Carry the exact object checked by the original MV guard to a later effect.
pub(crate) async fn guarded_iceberg_table_object_with_cpu(
    exact_lease: &ConnectorControlPlanningPin,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    target: &TargetBackend,
    mutation: IcebergMvUserMutation,
    context: ConnectorRequestContext,
    cpu: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
) -> Result<
    Option<ConnectorTableObjectId>,
    novarocks_query_application::session_error::QueryServiceError,
> {
    if target.provider_id.as_str() != "iceberg" {
        return Ok(None);
    }

    let instance_id = ConnectorInstanceId::parse(&target.catalog).map_err(|error| {
        super::readiness::application_error(format!(
            "parse Iceberg catalog identity for MV guard: {error}"
        ))
    })?;
    if exact_lease.binding().descriptor().instance_id != instance_id {
        return Err(super::readiness::application_error(
            "MV mutation guard planning pin does not match the target connector",
        ));
    }
    let context = crate::connector::context_for_planning_pin_typed(exact_lease, context)
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
    .map(Some)
}

async fn reject_bound_iceberg_mv_table_async(
    exact_lease: &ConnectorControlPlanningPin,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    target: &TargetBackend,
    mutation: IcebergMvUserMutation,
    context: ConnectorRequestContext,
    identity: ConnectorTableIdentity,
    binding: novarocks_spi::connector::ConnectorTableObjectBinding,
) -> Result<ConnectorTableObjectId, novarocks_query_application::session_error::QueryServiceError> {
    if binding.metadata.identity != identity {
        return Err(super::readiness::application_error(
            "connector loaded a different table while checking the MV mutation guard",
        ));
    }
    let documents = exact_lease
        .derive_document_storage_lease()
        .map_err(super::readiness::connector_application_error)?;
    let object_id = binding.object_id;
    let request = crate::mv::domain::staged_create::keyed_request(
        &documents,
        identity,
        object_id.clone(),
        context,
    );
    let observed = documents.observe_keyed_documents_async(request).await;
    check_iceberg_mv_observation(
        readiness,
        target,
        mutation,
        documents.catalog_handle(),
        observed,
    )?;
    Ok(object_id)
}

/// Apply the same user mutation guard using the already-admitted table.
pub(crate) fn reject_from_admitted_iceberg_table(
    exact_lease: &ConnectorControlPlanningPin,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    target: &TargetBackend,
    mutation: IcebergMvUserMutation,
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    context: ConnectorRequestContext,
) -> Result<(), novarocks_query_application::session_error::QueryServiceError> {
    if target.provider_id.as_str() != "iceberg" {
        return Ok(());
    }
    let context = crate::connector::context_for_planning_pin_typed(exact_lease, context)
        .map_err(super::readiness::connector_application_error)?;
    let documents = exact_lease
        .derive_document_storage_lease()
        .map_err(super::readiness::connector_application_error)?;
    if metadata.identity.instance_id.as_str() != target.catalog
        || metadata.identity.namespace.as_ref() != target.namespace
        || metadata.identity.table.as_ref() != target.table
    {
        return Err(super::readiness::application_error(
            "MV mutation guard target differs from its admitted table",
        ));
    }
    let observed = documents.project_keyed_documents_from_observed(metadata, context);
    check_iceberg_mv_observation(
        readiness,
        target,
        mutation,
        documents.catalog_handle(),
        observed,
    )
}

fn check_iceberg_mv_observation(
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    target: &TargetBackend,
    mutation: IcebergMvUserMutation,
    catalog: &novarocks_spi::connector::CatalogHandle,
    observed: Result<
        novarocks_spi::connector::KeyedDocumentView,
        novarocks_spi::connector::document_storage::KeyedDocumentReadError,
    >,
) -> Result<(), novarocks_query_application::session_error::QueryServiceError> {
    let view = match observed {
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
    if is_materialized_view_documents(&view) {
        // Denial needs no projection install, evidence resolution or reload.
        // Existing unsettled effects remain with their original management owner.
        readiness.revoke_foreign_current_management(&view, catalog)?;
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
        if matching_occurrences.is_empty() {
            continue;
        }
        let mv = super::refresh::definition::stored_mv_identity(&projection.projection);
        let query = super::refresh::definition::read_stored_mv_query(
            &definition.query.effective_sql,
            &mv,
        )
        .map_err(|error| {
            let public = error.to_user_error(None);
            format!(
                "DROP COLUMN `{}` is blocked because materialized view {} definition cannot be read: [{}] {}",
                column_path.dotted(),
                mv.fqn(),
                public.code().as_str(),
                public.message()
            )
        })?;
        let references_column = matching_occurrences.iter().any(|occurrence| {
            occurrence
                .fields
                .iter()
                .any(|field| field.name_at_binding.eq_ignore_ascii_case(&leaf))
        });
        if references_column || query_projects_target_wildcard(&query, &target) {
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
        novarocks_parser::ast::SetExpr::Query(subquery) => {
            let query = &subquery.query;
            query_projects_target_wildcard(query, target)
        }
        novarocks_parser::ast::SetExpr::SetOperation(operation) => operation
            .inputs
            .iter()
            .any(|input| set_expr_projects_target_wildcard(input, target)),
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
        .any(|from| collect_target_qualifiers_from_factor(from, target, &mut qualifiers))
    {
        return true;
    }
    select.projection.iter().any(|item| {
        let novarocks_parser::ast::SelectItem::AllColumns(columns) = item else {
            return false;
        };
        if !columns.aliases.is_empty() {
            return false;
        }
        let Some(target) = &columns.target else {
            return !qualifiers.is_empty();
        };
        let Some(chain) = novarocks_parser::ast::identifier_chain_observed(target, |_| {
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap_or_else(|never| match never {}) else {
            return false;
        };
        let mut parts = chain.iter_rev().collect::<Vec<_>>();
        parts.reverse();
        parts
            .iter()
            .map(|ident| normalize_identifier(&ident.value))
            .collect::<Result<Vec<_>, _>>()
            .map(|parts| qualifier_keys_from_parts(&parts))
            .unwrap_or_default()
            .into_iter()
            .any(|key| qualifiers.contains(&key))
    })
}

fn collect_target_qualifiers_from_factor(
    factor: &novarocks_parser::ast::Relation,
    target: &MvDependencyTarget,
    qualifiers: &mut HashSet<String>,
) -> bool {
    use novarocks_parser::ast::Relation;
    match factor {
        Relation::Table(table) => {
            if name_parts_match_target(
                novarocks_sql::TableNameView::new(&table.name).parts(),
                target,
            ) {
                qualifiers.extend(name_parts_qualifier_keys(
                    novarocks_sql::TableNameView::new(&table.name).parts(),
                ));
            }
            false
        }
        Relation::Aliased(alias) => {
            if let Relation::Table(table) = alias.relation.as_ref()
                && name_parts_match_target(
                    novarocks_sql::TableNameView::new(&table.name).parts(),
                    target,
                )
                && let Ok(normalized) = normalize_identifier(&alias.alias.value)
            {
                qualifiers.insert(normalized);
            }
            collect_target_qualifiers_from_factor(&alias.relation, target, qualifiers)
        }
        Relation::TableSubquery(node) => query_projects_target_wildcard(&node.query, target),
        Relation::Lateral(node) => query_projects_target_wildcard(&node.query, target),
        Relation::Join(join) => {
            collect_target_qualifiers_from_factor(&join.left, target, qualifiers)
                || collect_target_qualifiers_from_factor(&join.right, target, qualifiers)
        }
        _ => false,
    }
}

fn name_parts_match_target<'a>(
    parts: impl Iterator<Item = &'a str>,
    target: &MvDependencyTarget,
) -> bool {
    match normalized_name_parts(parts).as_deref() {
        Some([catalog, namespace, table]) => {
            catalog == &target.catalog && namespace == &target.namespace && table == &target.table
        }
        Some([namespace, table]) => namespace == &target.namespace && table == &target.table,
        Some([table]) => table == &target.table,
        _ => false,
    }
}

fn name_parts_qualifier_keys<'a>(parts: impl Iterator<Item = &'a str>) -> Vec<String> {
    normalized_name_parts(parts)
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

fn normalized_name_parts<'a>(parts: impl Iterator<Item = &'a str>) -> Option<Vec<String>> {
    parts
        .map(normalize_identifier)
        .collect::<Result<Vec<_>, _>>()
        .ok()
}

#[cfg(test)]
#[path = "iceberg_guard_context_tests.rs"]
mod context_tests;

#[cfg(test)]
#[test]
fn base_wildcard_and_raw_metadata_names_keep_distinct_guards() {
    use novarocks_parser::{
        ast::{Relation, SetExpr, TrinoStatement},
        parser,
    };
    let target = MvDependencyTarget {
        catalog: "ice".into(),
        namespace: "sales".into(),
        table: "orders".into(),
    };
    let query = parser::parse_query("SELECT ice.sales.orders.* FROM ice.sales.orders").unwrap();
    assert!(query_projects_target_wildcard(&query, &target));
    let source = r#"SELECT * FROM ice.sales."orders$files""#;
    let raw_query = parser::parse_query(source).unwrap();
    assert!(!query_projects_target_wildcard(&raw_query, &target));
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("SELECT");
    };
    let Some(Relation::Table(table)) = &select.from else {
        panic!("FROM table");
    };
    let parts = novarocks_sql::TableNameView::new(&table.name).parts();
    assert_eq!(
        name_parts_qualifier_keys(parts),
        ["ice.sales.orders", "orders", "sales.orders"]
    );
    let SetExpr::Select(select) = raw_query.body.as_ref() else {
        panic!("SELECT");
    };
    let Some(Relation::Table(table)) = &select.from else {
        panic!("FROM table");
    };
    let parts = novarocks_sql::TableNameView::new(&table.name).parts();
    assert_eq!(parts.collect::<Vec<_>>(), ["ice", "sales", "orders$files"]);
    let last = table.name.parts.last().unwrap();
    assert!(last.quoted);
    assert_eq!(last.value, "orders$files");
    assert_eq!(
        &source[last.span.start()..last.span.end()],
        r#""orders$files""#
    );
    let source = r#"DELETE FROM ice.sales."orders$files" WHERE id=1"#;
    let TrinoStatement::Delete(delete) = parser::parse_statement(source).unwrap() else {
        panic!("DELETE target");
    };
    let parts = delete.table.name.parts;
    let last = parts.last().unwrap();
    assert_eq!(last.value, "orders$files");
    assert!(!name_parts_match_target(
        parts.iter().map(|part| part.value.as_str()),
        &target
    ));
    assert!(name_parts_qualifier_keys(parts.iter().map(|part| part.value.as_str())).is_empty());
}
