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
    ConnectorControlResolver, ConnectorInstanceId, ConnectorTableIdentity, ConnectorTableResolution,
};

use crate::catalog_application::resolver::TargetBackend;
use crate::mv::domain::persistence::descriptor::MV_DESCRIPTOR_PACKAGE_ID_PROP;
use novarocks_catalog::identifier::normalize_identifier;

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

pub(crate) fn is_iceberg_mv_table_properties(props: &HashMap<String, String>) -> bool {
    props.contains_key(MV_DESCRIPTOR_PACKAGE_ID_PROP)
}

pub(crate) fn reject_if_iceberg_mv_properties(
    target: &TargetBackend,
    props: &HashMap<String, String>,
    mutation: IcebergMvUserMutation,
) -> Result<(), String> {
    if target.backend_name == "iceberg" && is_iceberg_mv_table_properties(props) {
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
    if target.backend_name != "iceberg" {
        return Ok(());
    }

    let instance_id = ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| format!("parse Iceberg catalog identity for MV guard: {error}"))?;
    let exact_lease = ConnectorControlResolver::acquire_current(connector_control, &instance_id)
        .map_err(|error| format!("acquire exact Iceberg generation for MV guard: {error}"))?;
    let context =
        novarocks::connector::connector_request_context(None, Arc::new(AtomicBool::new(false)))?;
    let identity = ConnectorTableIdentity {
        instance_id,
        namespace: Arc::from(target.namespace.as_str()),
        table: Arc::from(target.table.as_str()),
    };
    let metadata = novarocks::connector::metadata_load_connector_table_with_planning_lease(
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
/// Apply the MV dependency policy using only the durable MV repository.
/// DML kernels call this form directly rather than reaching through a
/// standalone aggregate.
pub(crate) fn reject_drop_column_mv_dependencies_with_repository(
    repository: &dyn crate::mv::domain::repository::MvRepository,
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
    for definition in repository
        .list_definitions()
        .map_err(|error| format!("load materialized view metadata failed: {error}"))?
    {
        let references_target = definition
            .base_table_refs
            .iter()
            .any(|base| base.eq_ignore_ascii_case(&target_key))
            || definition
                .select_sql
                .to_ascii_lowercase()
                .contains(&target_key_lower);
        if references_target
            && (sql_mentions_identifier(&definition.select_sql, &leaf)
                || sql_projects_target_wildcard(&definition.select_sql, &target))
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
    let Ok(normalized) = novarocks_sql::syntax::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(sqlparser::ast::Statement::Query(query)) =
        novarocks_sql::syntax::parse_normalized_sql_raw(&normalized)
    else {
        return false;
    };
    query_projects_target_wildcard(&query, target)
}

fn query_projects_target_wildcard(
    query: &sqlparser::ast::Query,
    target: &MvDependencyTarget,
) -> bool {
    if query.with.as_ref().is_some_and(|with| {
        with.cte_tables
            .iter()
            .any(|cte| query_projects_target_wildcard(&cte.query, target))
    }) {
        return true;
    }
    set_expr_projects_target_wildcard(query.body.as_ref(), target)
}

fn set_expr_projects_target_wildcard(
    set_expr: &sqlparser::ast::SetExpr,
    target: &MvDependencyTarget,
) -> bool {
    match set_expr {
        sqlparser::ast::SetExpr::Select(select) => select_projects_target_wildcard(select, target),
        sqlparser::ast::SetExpr::Query(query) => query_projects_target_wildcard(query, target),
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            set_expr_projects_target_wildcard(left, target)
                || set_expr_projects_target_wildcard(right, target)
        }
        _ => false,
    }
}

fn select_projects_target_wildcard(
    select: &sqlparser::ast::Select,
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
        sqlparser::ast::SelectItem::Wildcard(_) => !qualifiers.is_empty(),
        sqlparser::ast::SelectItem::QualifiedWildcard(kind, _) => match kind {
            sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name) => {
                object_name_qualifier_keys(name)
                    .into_iter()
                    .any(|key| qualifiers.contains(&key))
            }
            sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr) => {
                expr_qualifier_keys(expr)
                    .into_iter()
                    .any(|key| qualifiers.contains(&key))
            }
        },
        _ => false,
    })
}

fn collect_target_qualifiers_from_table_with_joins(
    table: &sqlparser::ast::TableWithJoins,
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
    factor: &sqlparser::ast::TableFactor,
    target: &MvDependencyTarget,
    qualifiers: &mut HashSet<String>,
) -> bool {
    match factor {
        sqlparser::ast::TableFactor::Table { name, alias, .. } => {
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
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            query_projects_target_wildcard(subquery, target)
        }
        sqlparser::ast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_target_qualifiers_from_table_with_joins(table_with_joins, target, qualifiers),
        sqlparser::ast::TableFactor::Pivot { table, .. }
        | sqlparser::ast::TableFactor::Unpivot { table, .. }
        | sqlparser::ast::TableFactor::MatchRecognize { table, .. } => {
            collect_target_qualifiers_from_factor(table, target, qualifiers)
        }
        _ => false,
    }
}

fn expr_qualifier_keys(expr: &sqlparser::ast::Expr) -> Vec<String> {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => normalize_identifier(&ident.value)
            .map(|name| vec![name])
            .unwrap_or_default(),
        sqlparser::ast::Expr::CompoundIdentifier(idents) => idents
            .iter()
            .map(|ident| normalize_identifier(&ident.value))
            .collect::<Result<Vec<_>, _>>()
            .map(|parts| qualifier_keys_from_parts(&parts))
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn object_name_matches_target(
    name: &sqlparser::ast::ObjectName,
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

fn object_name_qualifier_keys(name: &sqlparser::ast::ObjectName) -> Vec<String> {
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

fn normalized_object_name_parts(name: &sqlparser::ast::ObjectName) -> Option<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => normalize_identifier(&ident.value),
            _ => Err("unsupported object name part".to_string()),
        })
        .collect::<Result<Vec<_>, _>>()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iceberg_target() -> TargetBackend {
        TargetBackend {
            backend_name: "iceberg",
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
