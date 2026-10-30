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
use crate::mv::domain::dependency::scope::{
    validate_no_external_dependents_for_scope, validate_no_iceberg_mv_targets_in_scope,
};
use crate::mv::domain::readiness::MvReadinessPort;
use novarocks_mv_application::dependency::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks_mv_application::persistence::dependency::{
    CreateMvDependencyRequest, StoredMvDependency, stored_keyed_definition_dependency_ref,
};
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_types::naming::TableIdentity;

#[derive(Debug)]
pub(crate) struct ResolvedCreateMvDependencies {
    pub(crate) base_refs: Vec<TableIdentity>,
    pub(crate) dependencies: Vec<CreateMvDependencyRequest>,
}

pub(crate) fn resolve_create_mv_dependencies(
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
                let base = TableIdentity {
                    catalog: catalog.clone(),
                    namespace: namespace.clone(),
                    table: table.clone(),
                };
                base_refs.push(base);
                // These are occurrence-preserving locators, not exact object
                // facts. CREATE classifies dependencies only after D has been
                // bound to provider identities in the product transaction.
                let upstream = MvDependencyObjectRef {
                    catalog: Some(catalog.clone()),
                    database_or_namespace: namespace.clone(),
                    name: table.clone(),
                    object_type: MvDependencyObjectType::Unclassified,
                    storage_engine: MvDependencyStorageEngine::Unclassified,
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

pub(crate) fn ensure_no_iceberg_mv_targets_in_scope_with_readiness(
    readiness: &MvReadinessPort,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    let projections = readiness
        .list_ready_projections()
        .map_err(|e| format!("load MV projections for drop target scope check failed: {e}"))?;
    let targets = projections
        .iter()
        .map(|loaded| stored_keyed_definition_dependency_ref(&loaded.projection))
        .collect::<Vec<_>>();

    validate_no_iceberg_mv_targets_in_scope(scope_catalog, scope_namespace, &targets)
}

/// Loads ready MV projections and their exact upstream dependency occurrences,
/// then delegates to the pure scope helper.
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
    let inventory = projections
        .iter()
        .map(|loaded| &loaded.projection)
        .collect::<Vec<_>>();
    for loaded in &projections {
        let mv_target = stored_keyed_definition_dependency_ref(&loaded.projection);
        let dependencies = readiness
            .list_ready_dependencies_by_downstream(loaded)
            .map_err(|e| format!("load MV dependencies for drop scope check failed: {e}"))?;
        let upstreams = classify_ready_dependency_occurrences(dependencies, &inventory)?
            .into_iter()
            .map(|dep| dep.upstream)
            .collect::<Vec<_>>();
        edges.push((mv_target, upstreams));
    }

    validate_no_external_dependents_for_scope(scope_catalog, scope_namespace, &edges)
}

/// Read-only preflight for already classified edges. The product must repeat
/// cycle admission after binding CREATE's unclassified locator occurrences to
/// exact D objects; this name-only input cannot prove those new edges.
pub(crate) fn validate_no_create_cycle_from_facts(
    new_target: &MvDependencyObjectRef,
    new_dependencies: &[CreateMvDependencyRequest],
    projections: Vec<
        novarocks_mv_application::state_store_repository::keyed::LoadedKeyedMvProjection,
    >,
    observed_dependencies: Vec<Vec<StoredMvDependency>>,
) -> Result<(), String> {
    assert_eq!(
        projections.len(),
        observed_dependencies.len(),
        "one dependency set per original projection"
    );
    let inventory = projections
        .iter()
        .map(|loaded| &loaded.projection)
        .collect::<Vec<_>>();
    let edges = projections
        .iter()
        .zip(observed_dependencies)
        .map(|(loaded, dependencies)| create_cycle_edge(loaded, dependencies, &inventory))
        .collect::<Result<Vec<_>, _>>()?;
    finish_create_cycle(new_target, new_dependencies, &edges)
}
fn create_cycle_edge(
    loaded: &novarocks_mv_application::state_store_repository::keyed::LoadedKeyedMvProjection,
    dependencies: Vec<StoredMvDependency>,
    inventory: &[&StoredKeyedMvProjection],
) -> Result<(MvDependencyObjectRef, Vec<MvDependencyObjectRef>), String> {
    let dependencies = classify_ready_dependency_occurrences(dependencies, inventory)?
        .into_iter()
        .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
        .map(|dep| dep.upstream)
        .collect::<Vec<_>>();
    Ok((
        stored_keyed_definition_dependency_ref(&loaded.projection),
        dependencies,
    ))
}
fn finish_create_cycle(
    new_target: &MvDependencyObjectRef,
    new_dependencies: &[CreateMvDependencyRequest],
    edges: &[(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)],
) -> Result<(), String> {
    let new_upstreams = new_dependencies
        .iter()
        .filter(|dep| dep.upstream.object_type == MvDependencyObjectType::MaterializedView)
        .map(|dep| dep.upstream.clone())
        .collect::<Vec<_>>();
    validate_no_cycle_for_edges(new_target, &new_upstreams, edges)
}

/// Classify each exact source occurrence against the caller's one ready
/// inventory. A locator is not proof of MV identity, and repeated physical
/// objects remain separate occurrence records throughout this conversion.
pub(crate) fn classify_ready_dependency_occurrences(
    mut dependencies: Vec<StoredMvDependency>,
    ready_inventory: &[&StoredKeyedMvProjection],
) -> Result<Vec<StoredMvDependency>, String> {
    for dependency in &mut dependencies {
        let persisted = novarocks_mv_application::persistence::identity::ObjectIdentity::try_new(
            dependency.upstream_object_id.to_vec(),
        )
        .map_err(|error| {
            format!(
                "MV dependency occurrence {} has an invalid persisted object identity: {error}",
                dependency.occurrence_id
            )
        })?;
        let object_id =
            novarocks_mv_application::persistence::exact_revision::restore_persisted_object(
                &persisted,
            )
            .map_err(|error| {
                format!(
                    "MV dependency occurrence {} cannot restore its exact object identity: {error}",
                    dependency.occurrence_id
                )
            })?;
        let mut matches = ready_inventory.iter().copied().filter(|projection| {
            let source = projection.facts.source_revision();
            dependency.upstream.catalog.as_deref() == Some(source.target.instance_id.as_str())
                && object_id == source.target_object_id
        });
        let matched = matches.next();
        if matches.next().is_some() {
            return Err(format!(
                "MV dependency occurrence {} matches multiple ready target objects",
                dependency.occurrence_id,
            ));
        }
        if let Some(projection) = matched {
            dependency.upstream = stored_keyed_definition_dependency_ref(projection);
        } else {
            dependency.upstream.object_type = MvDependencyObjectType::Table;
            dependency.upstream.storage_engine = MvDependencyStorageEngine::ExternalTable;
        }
    }
    Ok(dependencies)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::mv::domain::dependency::scope as dependency_scope;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use novarocks_mv_application::dependency::iceberg_mv_dependency_ref;
    use novarocks_mv_application::persistence::exact_revision::persist_exact_query_revision;
    use novarocks_mv_application::product::MvTarget;
    use novarocks_query_application::api::{DataVersion, ObjectIdentity, ProviderFactFormat};
    use novarocks_spi::connector::ConnectorTableObjectId;

    fn projection(
        catalog: &str,
        namespace: &str,
        table: &str,
        object: &[u8],
    ) -> StoredKeyedMvProjection {
        let mut fixture =
            ProjectionFixture::new(MvTarget::from_parts(Some(catalog), namespace, table), None);
        fixture.object_id = ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(object))
            .expect("fixture target object");
        StoredKeyedMvProjection {
            mv_id: 1,
            facts: fixture.build().expect("validated document projection"),
        }
    }

    fn dependency(
        occurrence_id: u32,
        catalog: &str,
        table: &str,
        object: &[u8],
    ) -> StoredMvDependency {
        StoredMvDependency {
            downstream_mv_id: 9,
            occurrence_id,
            upstream_object_id: persisted_object(object).into(),
            upstream: MvDependencyObjectRef {
                catalog: Some(catalog.to_string()),
                database_or_namespace: "sales".to_string(),
                name: table.to_string(),
                object_type: MvDependencyObjectType::Unclassified,
                storage_engine: MvDependencyStorageEngine::Unclassified,
            },
            created_at_ms: 1_700_000_000_000,
        }
    }

    fn persisted_object(object: &[u8]) -> Vec<u8> {
        let format = |name| ProviderFactFormat::try_new_versioned("iceberg", name, 1).unwrap();
        let object =
            ObjectIdentity::try_new(format("table-object"), Arc::<[u8]>::from(object.to_vec()))
                .unwrap();
        let data =
            DataVersion::try_new(format("snapshot"), Arc::<[u8]>::from(b"snapshot".to_vec()))
                .unwrap();
        persist_exact_query_revision(&object, &data)
            .unwrap()
            .0
            .as_bytes()
            .to_vec()
    }

    #[test]
    fn target_scope_uses_the_validated_projection_target() {
        let projection = projection("catalog", "Namespace", "Table", b"target-object");
        let projected = [stored_keyed_definition_dependency_ref(&projection)];
        assert_eq!(
            projected,
            [iceberg_mv_dependency_ref("catalog", "Namespace", "Table")]
        );
        let err = dependency_scope::validate_no_iceberg_mv_targets_in_scope(
            "catalog",
            Some("namespace"),
            &projected,
        )
        .expect_err("the exact target must remain visible to the scope check");
        assert!(err.contains("catalog.Namespace.Table"), "err: {err}");
        assert!(!err.contains("mv:"), "err: {err}");
    }

    #[test]
    fn exact_object_match_uses_the_ready_target_locator_and_keeps_occurrences() {
        let upstream = projection("ice", "analytics", "renamed_mv", b"same-object");
        let dependencies = [7, 8]
            .into_iter()
            .map(|id| dependency(id, "ice", "original_name", b"same-object"))
            .collect();
        let classified = classify_ready_dependency_occurrences(dependencies, &[&upstream])
            .expect("same exact object in ready inventory");

        assert_eq!(classified.len(), 2);
        for (actual, occurrence_id) in classified.iter().zip([7, 8]) {
            assert_eq!(actual.occurrence_id, occurrence_id);
            assert_eq!(
                actual.upstream_object_id.as_ref(),
                persisted_object(b"same-object")
            );
            assert_eq!(actual.downstream_mv_id, 9);
            assert_eq!(
                actual.upstream,
                iceberg_mv_dependency_ref("ice", "analytics", "renamed_mv"),
            );
        }
    }

    #[test]
    fn same_locator_with_a_replaced_object_does_not_create_an_mv_edge() {
        let upstream = projection("ice", "sales", "orders", b"new-object");
        let mut source = dependency(7, "ice", "orders", b"old-object");
        source.upstream.object_type = MvDependencyObjectType::MaterializedView;
        source.upstream.storage_engine = MvDependencyStorageEngine::Iceberg;
        let classified = classify_ready_dependency_occurrences(vec![source], &[&upstream])
            .expect("same locator is not proof of exact identity");

        assert_eq!(
            classified[0].upstream.object_type,
            MvDependencyObjectType::Table
        );
        assert_eq!(classified[0].upstream.name, "orders");
        assert_eq!(
            classified[0].upstream_object_id.as_ref(),
            persisted_object(b"old-object")
        );
        assert_eq!(
            classified[0].upstream.storage_engine,
            MvDependencyStorageEngine::ExternalTable,
        );
    }

    #[test]
    fn exact_object_bytes_in_another_catalog_do_not_create_an_mv_edge() {
        let upstream = projection("other", "sales", "orders", b"same-object");
        let classified = classify_ready_dependency_occurrences(
            vec![dependency(7, "ice", "orders", b"same-object")],
            &[&upstream],
        )
        .expect("object identities remain catalog scoped");

        assert_eq!(
            classified[0].upstream.object_type,
            MvDependencyObjectType::Table
        );
    }

    #[test]
    fn a_target_absent_from_the_ready_inventory_cannot_remain_an_mv_edge() {
        let mut source = dependency(7, "ice", "orders", b"same-object");
        source.upstream.object_type = MvDependencyObjectType::MaterializedView;
        let classified = classify_ready_dependency_occurrences(vec![source], &[])
            .expect("unready target is not an executable upstream MV");

        assert_eq!(
            classified[0].upstream.object_type,
            MvDependencyObjectType::Table
        );
    }

    #[test]
    fn invalid_persisted_source_identity_does_not_downgrade_an_mv_edge_to_a_table() {
        let mut source = dependency(7, "ice", "orders", b"same-object");
        source.upstream_object_id = b"same-object".to_vec().into();
        let error = classify_ready_dependency_occurrences(vec![source], &[])
            .expect_err("a source without the application fact envelope must fail closed");
        assert!(error.contains("occurrence 7 cannot restore its exact object identity"));
    }

    #[test]
    fn ambiguous_ready_object_identity_is_rejected_instead_of_picking_a_locator() {
        let first = projection("ice", "sales", "mv_a", b"same-object");
        let second = projection("ice", "sales", "mv_b", b"same-object");
        let error = classify_ready_dependency_occurrences(
            vec![dependency(7, "ice", "original_name", b"same-object")],
            &[&first, &second],
        )
        .expect_err("ambiguous exact target identity must fail closed");

        assert!(error.contains("occurrence 7 matches multiple ready target objects"));
    }

    #[test]
    fn table_occurrences_still_block_dropping_their_source_scope() {
        let target = projection("ice", "analytics", "mv_orders", b"target-object");
        let dependencies = classify_ready_dependency_occurrences(
            vec![dependency(7, "ice", "orders", b"table-object")],
            &[&target],
        )
        .expect("ordinary source table occurrence");
        let edges = [(
            stored_keyed_definition_dependency_ref(&target),
            dependencies
                .into_iter()
                .map(|dependency| dependency.upstream)
                .collect(),
        )];

        let error = dependency_scope::validate_no_external_dependents_for_scope(
            "ice",
            Some("sales"),
            &edges,
        )
        .expect_err("source namespace drop must not orphan an external MV");
        assert!(error.contains("mv:ice.analytics.mv_orders depends on ice.sales.orders"));
    }

    #[test]
    fn refresh_order_uses_exact_ready_mv_edges_after_occurrence_classification() {
        let upstream = projection("ice", "analytics", "mv_renamed", b"upstream-object");
        let downstream = projection("ice", "analytics", "mv_downstream", b"downstream-object");
        let classified = classify_ready_dependency_occurrences(
            vec![
                dependency(7, "ice", "old_mv_name", b"upstream-object"),
                dependency(8, "ice", "old_mv_name", b"upstream-object"),
            ],
            &[&upstream, &downstream],
        )
        .expect("exact upstream occurrences");
        assert_eq!(classified.len(), 2);
        let target = stored_keyed_definition_dependency_ref(&downstream);
        let upstream_target = stored_keyed_definition_dependency_ref(&upstream);
        let edges = [
            (upstream_target.clone(), Vec::new()),
            (
                target.clone(),
                classified.into_iter().map(|entry| entry.upstream).collect(),
            ),
        ];
        let order = crate::mv::domain::dependency::graph::topological_upstream_order_for_edges(
            &target, &edges,
        )
        .expect("refresh graph uses ready canonical MV targets");

        // Both semantic occurrences remain above; the same exact physical MV
        // needs only one refresh action before its downstream.
        assert_eq!(order, vec![upstream_target, target]);
    }

    #[test]
    fn create_preserves_repeated_locators_without_guessing_dependency_kind() {
        let source = ResolvedTableRef::Iceberg {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "orders".to_string(),
        };
        let resolved = resolve_create_mv_dependencies(&[source.clone(), source], 123)
            .expect("CREATE retains source locator occurrences");

        assert_eq!(resolved.base_refs.len(), 2);
        assert_eq!(resolved.base_refs[0], resolved.base_refs[1]);
        assert_eq!(resolved.dependencies.len(), 2);
        for dependency in resolved.dependencies {
            assert_eq!(
                dependency.upstream.object_type,
                MvDependencyObjectType::Unclassified
            );
            assert_eq!(
                dependency.upstream.storage_engine,
                MvDependencyStorageEngine::Unclassified
            );
            assert_eq!(dependency.created_at_ms, 123);
        }
    }

    #[test]
    fn native_internal_mv_base_table_is_rejected() {
        let error = resolve_create_mv_dependencies(
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
