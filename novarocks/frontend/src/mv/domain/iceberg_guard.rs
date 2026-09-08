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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorControlResolver, ConnectorInstanceId,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableResolution,
};

use crate::catalog_application::resolver::TargetBackend;
use crate::mv::domain::persistence::descriptor::MV_DESCRIPTOR_PACKAGE_ID_PROP;
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
        }
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn is_iceberg_mv_table_properties(props: &HashMap<String, String>) -> bool {
    props.contains_key(MV_DESCRIPTOR_PACKAGE_ID_PROP)
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn reject_if_iceberg_mv_properties(
    target: &TargetBackend,
    props: &HashMap<String, String>,
    mutation: IcebergMvUserMutation,
) -> Result<(), String> {
    if target.provider_id.as_str() == "iceberg" && is_iceberg_mv_table_properties(props) {
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

/// Reject a user mutation of an Iceberg-backed materialized-view table.
///
/// The guard deliberately accepts only the exact-generation control resolver
/// and the storage-observation port. Command kernels use this entry directly;
/// they must not reconstruct an application facade or obtain a provider through
/// the retired connector registry.
pub fn reject_if_iceberg_mv_table_with_ports(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
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
    let context =
        crate::connector::connector_request_context(None, Arc::new(AtomicBool::new(false)))?;
    reject_if_iceberg_mv_table_with_planning_lease_and_context(
        storage_observation,
        &exact_lease,
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
    storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    exact_lease: &ConnectorControlPlanningLease,
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
    let identity = ConnectorTableIdentity {
        instance_id,
        namespace: Arc::from(target.namespace.as_str()),
        table: Arc::from(target.table.as_str()),
    };
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        context.clone(),
        &target.namespace,
        &target.table,
        ConnectorTableResolution::StrictBaseTable,
    )?;
    if metadata.identity != identity {
        return Err(
            "connector loaded a different table while checking the MV mutation guard".to_string(),
        );
    }
    if crate::mv::domain::storage_observation::observe_lake_package(
        storage_observation,
        &exact_lease,
        &metadata,
        context,
    )
    .map_err(|error| format!("observe Iceberg MV package for mutation guard: {error}"))?
    .is_some()
    {
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
    let target_key = format!("{}.{}.{}", target.catalog, target.namespace, target.table);
    let target_key_lower = target_key.to_ascii_lowercase();
    let target = MvDependencyTarget::from_backend(target)?;
    for projection in readiness
        .list_ready_projections()
        .map_err(|error| format!("load materialized view metadata failed: {error}"))?
    {
        let definition = projection.definition;
        let references_target = definition
            .base_table_refs
            .iter()
            .any(|base| base.eq_ignore_ascii_case(&target_key))
            || definition
                .query_definition
                .raw_query_source
                .to_ascii_lowercase()
                .contains(&target_key_lower);
        if references_target
            && (sql_mentions_identifier(&definition.query_definition.raw_query_source, &leaf)
                || sql_projects_target_wildcard(
                    &definition.query_definition.raw_query_source,
                    &target,
                ))
        {
            return Err(format!(
                "DROP COLUMN `{}` is blocked because a StarRocks materialized view references it",
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

fn sql_mentions_identifier(sql: &str, normalized_identifier: &str) -> bool {
    sql.split(|ch: char| !(ch == '_' || ch.is_ascii_alphanumeric()))
        .filter(|token| !token.is_empty())
        .any(|token| token.eq_ignore_ascii_case(normalized_identifier))
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
mod tests {
    use super::*;

    fn iceberg_target() -> TargetBackend {
        TargetBackend {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("static Iceberg provider ID"),
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_orders".to_string(),
        }
    }

    #[test]
    fn property_guard_allows_plain_iceberg_tables() {
        let props = HashMap::new();

        reject_if_iceberg_mv_properties(&iceberg_target(), &props, IcebergMvUserMutation::Insert)
            .expect("plain iceberg tables should pass");
    }

    #[test]
    fn property_guard_rejects_mv_tables_with_operation_guidance() {
        let props = HashMap::from([(
            MV_DESCRIPTOR_PACKAGE_ID_PROP.to_string(),
            "analytics.mv_orders".to_string(),
        )]);

        let err = reject_if_iceberg_mv_properties(
            &iceberg_target(),
            &props,
            IcebergMvUserMutation::DropTable,
        )
        .expect_err("iceberg MV tables should reject direct user mutations");

        assert!(err.contains("ice.analytics.mv_orders"), "{err}");
        assert!(err.contains("materialized view"), "{err}");
        assert!(err.contains("DROP MATERIALIZED VIEW"), "{err}");
    }
}
