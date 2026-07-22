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

use std::sync::Arc;

#[cfg(feature = "compat")]
use crate::connector::starrocks::table::model::StarRocksTableKind;
use crate::engine::StandaloneState;
use crate::meta::repository::mv::CreateMvDependencyRequest;
use crate::mv::analysis::ResolvedTableRef;
use crate::mv::dependency::graph::{
    topological_upstream_order_for_edges, validate_no_cycle_for_edges,
};
use crate::mv::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, iceberg_mv_dependency_ref,
    iceberg_table_dependency_ref,
};
use crate::mv::dependency::refresh::{MvRefreshDependencyStep, refresh_step_for_dependency_object};
use crate::mv::dependency::scope::{
    validate_no_external_dependents_for_scope, validate_no_iceberg_mv_targets_in_scope,
};
use crate::mv::persistence::definition::StoredMvDefinition;
#[cfg(test)]
use crate::mv::persistence::definition::StoredMvRefreshPolicy;
use crate::mv::persistence::dependency::stored_definition_dependency_ref;
use novarocks_catalog::identifier::TableIdentity;

pub(crate) struct ResolvedCreateMvDependencies {
    pub(crate) base_refs: Vec<TableIdentity>,
    pub(crate) dependencies: Vec<CreateMvDependencyRequest>,
}

pub(crate) fn ensure_no_downstream_dependencies(
    state: &Arc<StandaloneState>,
    upstream: &MvDependencyObjectRef,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency drop guard read failed: {e}"))?;
    state
        .mv_repo
        .ensure_no_downstream_dependencies(read.as_ref(), upstream)
        .map_err(|e| e.to_string())
}

fn iceberg_mv_target_ref_for_scope(
    definition: &StoredMvDefinition,
) -> Option<MvDependencyObjectRef> {
    if !definition.storage_engine.eq_ignore_ascii_case("iceberg") {
        return None;
    }
    Some(iceberg_mv_dependency_ref(
        definition.target_catalog.as_deref()?,
        definition.target_namespace.as_deref()?,
        definition.target_table.as_deref()?,
    ))
}

pub(crate) fn resolve_create_mv_dependencies(
    state: &Arc<StandaloneState>,
    resolved_refs: &[ResolvedTableRef],
    created_at_ms: i64,
) -> Result<ResolvedCreateMvDependencies, String> {
    let provider = state.metadata_provider.as_ref().ok_or_else(|| {
        "materialized view dependency resolution requires metadata provider".to_string()
    })?;
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency metadata read transaction failed: {e}"))?;

    let mut base_refs = Vec::new();
    let mut dependencies = Vec::new();
    for table_ref in resolved_refs {
        match table_ref {
            ResolvedTableRef::Iceberg {
                catalog,
                namespace,
                table,
            } => {
                let is_mv_dependency = state
                    .mv_repo
                    .find_by_target(read.as_ref(), catalog, namespace, table)
                    .map_err(|e| format!("load MV target dependency failed: {e}"))?
                    .is_some();
                let base = TableIdentity {
                    catalog: catalog.clone(),
                    namespace: namespace.clone(),
                    table: table.clone(),
                };
                if !base_refs.contains(&base) {
                    base_refs.push(base.clone());
                }
                let upstream = if is_mv_dependency {
                    iceberg_mv_dependency_ref(catalog, namespace, table)
                } else {
                    iceberg_table_dependency_ref(&base)
                };
                dependencies.push(CreateMvDependencyRequest {
                    upstream,
                    created_at_ms,
                });
            }
            ResolvedTableRef::StarRocks { database, table } => {
                #[cfg(not(feature = "compat"))]
                {
                    return Err(format!(
                        "StarRocks table MV dependency `{database}.{table}` requires the compat feature"
                    ));
                }
                #[cfg(feature = "compat")]
                {
                    let starrocks = state
                        .starrocks_table
                        .read()
                        .expect("standalone StarRocks table read lock");
                    let runtime = starrocks.table(database, table).map_err(|err| {
                        format!(
                            "resolve StarRocks table MV dependency {database}.{table} failed: {err}"
                        )
                    })?;
                    if runtime.table.kind != StarRocksTableKind::MaterializedView {
                        return Err(format!(
                            "materialized view base tables must be Iceberg tables or materialized views; found StarRocks table `{database}.{table}`"
                        ));
                    }
                    return Err(format!(
                        "StarRocks table MV-on-MV dependency `{database}.{table}` is recognized but cannot be used as an incremental Iceberg base in this release"
                    ));
                }
            }
        }
    }
    if base_refs.is_empty() {
        return Err("materialized view base tables must be Iceberg tables".to_string());
    }
    Ok(ResolvedCreateMvDependencies {
        base_refs,
        dependencies,
    })
}

pub(crate) fn ensure_no_iceberg_mv_targets_in_scope(
    state: &Arc<StandaloneState>,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV target drop scope read failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for drop target scope check failed: {e}"))?;
    let targets = definitions
        .iter()
        .filter_map(iceberg_mv_target_ref_for_scope)
        .collect::<Vec<_>>();

    validate_no_iceberg_mv_targets_in_scope(scope_catalog, scope_namespace, &targets)
}

/// State-aware wrapper around `validate_no_external_dependents_for_scope`:
/// loads MV definitions and their upstream dependencies from the repository,
/// then delegates to the pure helper.
pub(crate) fn ensure_no_external_iceberg_dependents(
    state: &Arc<StandaloneState>,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency drop scope read failed: {e}"))?;

    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for drop scope check failed: {e}"))?;

    let mut edges: Vec<(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)> =
        Vec::with_capacity(definitions.len());
    for def in &definitions {
        let mv_target = stored_definition_dependency_ref_from_state(state, def)?;
        let upstreams = state
            .mv_repo
            .list_dependencies_by_downstream(read.as_ref(), def.mv_id)
            .map_err(|e| format!("load MV dependencies for drop scope check failed: {e}"))?
            .into_iter()
            .map(|dep| dep.upstream)
            .collect::<Vec<_>>();
        edges.push((mv_target, upstreams));
    }

    validate_no_external_dependents_for_scope(scope_catalog, scope_namespace, &edges)
}

pub(crate) fn build_upstream_refresh_steps(
    state: &Arc<StandaloneState>,
    requested: &MvDependencyObjectRef,
) -> Result<Vec<MvRefreshDependencyStep>, String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(vec![refresh_step_for_dependency_object(requested)?]);
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency refresh graph read failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for refresh graph failed: {e}"))?;

    let mut edges = Vec::new();
    for definition in definitions {
        let target = stored_definition_dependency_ref_from_state(state, &definition)?;
        let upstream_mvs = state
            .mv_repo
            .list_dependencies_by_downstream(read.as_ref(), definition.mv_id)
            .map_err(|e| format!("load MV dependencies for refresh graph failed: {e}"))?
            .into_iter()
            .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
            .map(|dep| dep.upstream)
            .collect::<Vec<_>>();
        edges.push((target, upstream_mvs));
    }

    topological_upstream_order_for_edges(requested, &edges)?
        .iter()
        .map(refresh_step_for_dependency_object)
        .collect()
}

pub(crate) fn validate_no_create_cycle(
    state: &Arc<StandaloneState>,
    new_target: &MvDependencyObjectRef,
    new_dependencies: &[CreateMvDependencyRequest],
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open MV dependency graph read failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load MV definitions for dependency cycle check failed: {e}"))?;
    let mut edges = Vec::new();
    for definition in definitions {
        let target = stored_definition_dependency_ref_from_state(state, &definition)?;
        let dependencies = state
            .mv_repo
            .list_dependencies_by_downstream(read.as_ref(), definition.mv_id)
            .map_err(|e| format!("load MV dependencies for cycle check failed: {e}"))?
            .into_iter()
            .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
            .map(|dep| dep.upstream)
            .collect::<Vec<_>>();
        edges.push((target, dependencies));
    }
    let new_upstreams = new_dependencies
        .iter()
        .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
        .map(|dep| dep.upstream.clone())
        .collect::<Vec<_>>();
    validate_no_cycle_for_edges(new_target, &new_upstreams, &edges)
}

fn stored_definition_dependency_ref_from_state(
    state: &Arc<StandaloneState>,
    definition: &StoredMvDefinition,
) -> Result<MvDependencyObjectRef, String> {
    if definition.storage_engine.eq_ignore_ascii_case("iceberg") {
        return stored_definition_dependency_ref(definition, None);
    }
    let starrocks = state
        .starrocks_table
        .read()
        .expect("standalone StarRocks table read lock");
    let table = starrocks
        .snapshot
        .tables
        .iter()
        .find(|table| table.table_id == definition.mv_id)
        .ok_or_else(|| {
            format!(
                "StarRocks table MV definition {} is missing runtime table metadata",
                definition.mv_id
            )
        })?;
    let database = starrocks
        .snapshot
        .databases
        .iter()
        .find(|database| database.db_id == table.db_id)
        .ok_or_else(|| {
            format!(
                "StarRocks table MV definition {} is missing runtime database metadata",
                definition.mv_id
            )
        })?;
    stored_definition_dependency_ref(definition, Some((&database.name, &table.name)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::dependency::model::iceberg_mv_dependency_ref;
    use crate::mv::dependency::scope as dependency_scope;

    fn stored_mv_definition(
        storage_engine: &str,
        target_catalog: Option<&str>,
        target_namespace: Option<&str>,
        target_table: Option<&str>,
    ) -> StoredMvDefinition {
        StoredMvDefinition {
            mv_id: 1,
            select_sql: "select 1".to_string(),
            base_table_refs: Vec::new(),
            primary_key_columns: Vec::new(),
            storage_engine: storage_engine.to_string(),
            target_catalog: target_catalog.map(str::to_string),
            target_namespace: target_namespace.map(str::to_string),
            target_table: target_table.map(str::to_string),
            schema_contract: None,
            partition_spec: None,
            partition_state_complete: false,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: std::collections::BTreeMap::new(),
            last_refresh_table_uuids: std::collections::BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: std::collections::BTreeMap::new(),
            refresh_policy: StoredMvRefreshPolicy::Manual,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: 0,
        }
    }

    #[test]
    fn iceberg_mv_target_projection_tolerates_legacy_definitions() {
        let definitions = [
            stored_mv_definition(
                "starrocks",
                Some("Catalog"),
                Some("Namespace"),
                Some("Table"),
            ),
            stored_mv_definition("iceberg", None, Some("Namespace"), Some("Table")),
            stored_mv_definition("iceberg", Some("Catalog"), None, Some("Table")),
            stored_mv_definition("iceberg", Some("Catalog"), Some("Namespace"), None),
            stored_mv_definition("Iceberg", Some("Catalog"), Some("Namespace"), Some("Table")),
        ];

        let projected = definitions
            .iter()
            .filter_map(iceberg_mv_target_ref_for_scope)
            .collect::<Vec<_>>();

        assert_eq!(
            projected,
            vec![iceberg_mv_dependency_ref("Catalog", "Namespace", "Table")]
        );
        let err = dependency_scope::validate_no_iceberg_mv_targets_in_scope(
            "catalog",
            Some("namespace"),
            &projected,
        )
        .expect_err("the complete mixed-case target must remain visible to the scope check");
        assert!(err.contains("Catalog.Namespace.Table"), "err: {err}");
        assert!(!err.contains("mv:"), "err: {err}");
    }
}
