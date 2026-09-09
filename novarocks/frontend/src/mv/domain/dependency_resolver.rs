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

use crate::mv::domain::analysis::ResolvedTableRef;
use crate::mv::domain::dependency::graph::validate_no_cycle_for_edges;
use crate::mv::domain::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, iceberg_mv_dependency_ref,
    iceberg_table_dependency_ref,
};
use crate::mv::domain::dependency::scope::{
    validate_no_external_dependents_for_scope, validate_no_iceberg_mv_targets_in_scope,
};
#[cfg(test)]
use crate::mv::domain::persistence::definition::MvDesiredRefreshPolicy;
use crate::mv::domain::persistence::definition::StoredMvDefinition;
use crate::mv::domain::persistence::dependency::CreateMvDependencyRequest;
use crate::mv::domain::persistence::dependency::stored_definition_dependency_ref;
use crate::mv::domain::readiness::MvReadinessPort;
use novarocks_types::naming::TableIdentity;

#[derive(Debug)]
pub(crate) struct ResolvedCreateMvDependencies {
    pub(crate) base_refs: Vec<TableIdentity>,
    pub(crate) dependencies: Vec<CreateMvDependencyRequest>,
}

pub(crate) fn ensure_no_downstream_dependencies_with_readiness(
    readiness: &MvReadinessPort,
    upstream: &MvDependencyObjectRef,
) -> Result<(), String> {
    readiness
        .ensure_no_ready_downstream_dependencies(upstream)
        .map_err(|e| e.to_string())
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
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

pub(crate) fn resolve_create_mv_dependencies_with_readiness(
    readiness: &MvReadinessPort,
    resolved_refs: &[ResolvedTableRef],
    created_at_ms: i64,
) -> Result<ResolvedCreateMvDependencies, String> {
    let mut base_refs = Vec::new();
    let mut dependencies = Vec::new();
    for table_ref in resolved_refs {
        match table_ref {
            ResolvedTableRef::Iceberg {
                catalog,
                namespace,
                table,
            } => {
                let is_mv_dependency = readiness
                    .load_ready(&crate::mv::domain::model::MvTarget {
                        catalog: Some(catalog.clone()),
                        database: namespace.clone(),
                        name: table.clone(),
                    })
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
            ResolvedTableRef::UnsupportedNative { display_name } => {
                return Err(format!(
                    "materialized view base table `{display_name}` requires an external catalog; native internal tables are not supported"
                ));
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

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn ensure_no_iceberg_mv_targets_in_scope_with_readiness(
    readiness: &MvReadinessPort,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    let definitions = readiness
        .list_ready_projections()
        .map_err(|e| format!("load MV projections for drop target scope check failed: {e}"))?
        .into_iter()
        .map(|projection| projection.definition)
        .collect::<Vec<_>>();
    let targets = definitions
        .iter()
        .filter_map(iceberg_mv_target_ref_for_scope)
        .collect::<Vec<_>>();

    validate_no_iceberg_mv_targets_in_scope(scope_catalog, scope_namespace, &targets)
}

/// Loads MV definitions and their upstream dependencies from the repository,
/// then delegates to the pure scope helper.
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn ensure_no_external_iceberg_dependents_with_readiness(
    readiness: &MvReadinessPort,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    let projections = readiness
        .list_ready_projections()
        .map_err(|e| format!("load MV projections for drop scope check failed: {e}"))?;
    let mut edges: Vec<(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)> =
        Vec::with_capacity(projections.len());
    for projection in projections {
        let mv_target = stored_definition_dependency_ref_for_iceberg(&projection.definition)?;
        let upstreams = readiness
            .list_ready_dependencies_by_downstream(&projection)
            .map_err(|e| format!("load MV dependencies for drop scope check failed: {e}"))?
            .into_iter()
            .map(|dep| dep.upstream)
            .collect::<Vec<_>>();
        edges.push((mv_target, upstreams));
    }

    validate_no_external_dependents_for_scope(scope_catalog, scope_namespace, &edges)
}

pub(crate) fn validate_no_create_cycle_with_readiness(
    readiness: &MvReadinessPort,
    new_target: &MvDependencyObjectRef,
    new_dependencies: &[CreateMvDependencyRequest],
) -> Result<(), String> {
    let projections = readiness
        .list_ready_projections()
        .map_err(|e| format!("load MV projections for dependency cycle check failed: {e}"))?;
    let mut edges = Vec::new();
    for projection in projections {
        let target = stored_definition_dependency_ref_for_iceberg(&projection.definition)?;
        let dependencies = readiness
            .list_ready_dependencies_by_downstream(&projection)
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

fn stored_definition_dependency_ref_for_iceberg(
    definition: &StoredMvDefinition,
) -> Result<MvDependencyObjectRef, String> {
    if definition.storage_engine.eq_ignore_ascii_case("iceberg") {
        return stored_definition_dependency_ref(definition, None);
    }
    Err(format!(
        "legacy materialized view definition {} uses an unsupported storage engine",
        definition.mv_id
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::persisted_query_definition::{
        PersistedQueryDefinition, PersistedQueryDialect,
    };
    use crate::mv::domain::dependency::model::iceberg_mv_dependency_ref;
    use crate::mv::domain::dependency::scope as dependency_scope;

    fn stored_mv_definition(
        storage_engine: &str,
        target_catalog: Option<&str>,
        target_namespace: Option<&str>,
        target_table: Option<&str>,
    ) -> StoredMvDefinition {
        StoredMvDefinition {
            mv_id: 1,
            query_definition: PersistedQueryDefinition::new(
                "select 1",
                PersistedQueryDialect::StarRocks,
                "ice",
                target_namespace.unwrap_or("db"),
            )
            .unwrap(),
            base_table_refs: Vec::new(),
            primary_key_columns: Vec::new(),
            storage_engine: storage_engine.to_string(),
            target_catalog: target_catalog.map(str::to_string),
            target_namespace: target_namespace.map(str::to_string),
            target_table: target_table.map(str::to_string),
            schema_contract: None,
            partition_spec: None,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: std::collections::BTreeMap::new(),
            last_refresh_table_object_ids: std::collections::BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_policy: MvDesiredRefreshPolicy::Manual,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            created_at_ms: 0,
            source_revision:
                crate::mv::domain::persistence::definition::MvAcceleratorSourceRevision {
                    target_object_id: novarocks_spi::connector::ConnectorTableObjectId::try_new(
                        bytes::Bytes::from_static(b"dependency-test-target"),
                    )
                    .expect("test object ID"),
                    descriptor_content_hash: "test-descriptor".to_string(),
                    current_target_snapshot_id: None,
                },
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_internal_mv_base_table_is_rejected() {
        let repository = crate::mv::domain::test_repository::InMemoryMvRepository::default();
        let readiness = MvReadinessPort::new(
            std::sync::Arc::new(repository),
            std::sync::Arc::new(crate::mv::process_runtime::ProcessRuntime::default()),
            tokio::runtime::Handle::current(),
        );
        let error = resolve_create_mv_dependencies_with_readiness(
            &readiness,
            &[ResolvedTableRef::UnsupportedNative {
                display_name: "sales.orders".to_string(),
            }],
            1,
        )
        .expect_err("native internal MV base tables must stay unsupported");

        assert_eq!(
            error,
            "materialized view base table `sales.orders` requires an external catalog; native internal tables are not supported"
        );
    }
}
