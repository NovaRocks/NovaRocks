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

use crate::mv::domain::dependency::graph::topological_upstream_order_for_edges;
use crate::mv::domain::dependency_resolver::classify_ready_dependency_occurrences;
use crate::mv::domain::model::MvStorageEngine;
use crate::mv::domain::readiness::MvReadinessExecution;
use novarocks_mv_application::dependency::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks_mv_application::persistence::dependency::{
    StoredMvDependency, stored_keyed_definition_dependency_ref,
};
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_mv_application::state_store_repository::keyed::LoadedKeyedMvProjection;
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshDependencyStep {
    object: MvDependencyObjectRef,
    target: MvTarget,
    storage_engine: MvStorageEngine,
}

impl MvRefreshDependencyStep {
    pub fn display_name(&self) -> String {
        self.object.display_name()
    }

    #[cfg(test)]
    pub fn target(&self) -> &MvTarget {
        &self.target
    }

    pub fn into_target(self) -> MvTarget {
        self.target
    }

    pub fn is_iceberg(&self) -> bool {
        self.storage_engine == MvStorageEngine::Iceberg
    }
}

pub(crate) fn refresh_step_for_dependency_object(
    object: &MvDependencyObjectRef,
) -> Result<MvRefreshDependencyStep, String> {
    if object.object_type != MvDependencyObjectType::MaterializedView {
        return Err(format!(
            "refresh dependency object is not a materialized view: {}",
            object.display_name()
        ));
    }
    let storage_engine = match object.storage_engine {
        MvDependencyStorageEngine::StarRocks => MvStorageEngine::StarRocks,
        MvDependencyStorageEngine::Iceberg => MvStorageEngine::Iceberg,
        MvDependencyStorageEngine::ExternalTable => {
            return Err(format!(
                "external table cannot be refreshed as materialized view: {}",
                object.display_name()
            ));
        }
        MvDependencyStorageEngine::Unclassified => {
            return Err(format!(
                "unclassified dependency cannot be refreshed as materialized view: {}",
                object.display_name()
            ));
        }
    };
    Ok(MvRefreshDependencyStep {
        object: object.clone(),
        target: MvTarget {
            catalog: object.catalog.clone(),
            database: object.database_or_namespace.clone(),
            name: object.name.clone(),
        },
        storage_engine,
    })
}

/// Raw observations returned by the named I/O gate. Classification and graph
/// traversal belong to the next original CPU turn, not to this read future.
pub(crate) struct MvRefreshGraphFacts {
    projections: Vec<LoadedKeyedMvProjection>,
    dependencies: Vec<Vec<StoredMvDependency>>,
}

impl MvRefreshGraphFacts {
    pub(crate) fn into_parts(self) -> (Vec<LoadedKeyedMvProjection>, Vec<Vec<StoredMvDependency>>) {
        (self.projections, self.dependencies)
    }
}

pub(crate) async fn capture_refresh_graph(
    readiness: &MvReadinessExecution<'_>,
) -> Result<MvRefreshGraphFacts, novarocks_mv_application::repository::MvRepositoryError> {
    let projections = readiness.list_ready_projections().await?;
    let mut dependencies = Vec::with_capacity(projections.len());
    for loaded in &projections {
        dependencies.push(
            readiness
                .list_ready_dependencies_by_downstream(loaded)
                .await?,
        );
    }
    Ok(MvRefreshGraphFacts {
        projections,
        dependencies,
    })
}

pub(crate) fn finish_refresh_graph(
    facts: MvRefreshGraphFacts,
    requested: &MvDependencyObjectRef,
) -> Result<Vec<MvRefreshDependencyStep>, String> {
    let MvRefreshGraphFacts {
        projections,
        dependencies,
    } = facts;
    assert_eq!(
        projections.len(),
        dependencies.len(),
        "same original refresh graph observations"
    );
    let inventory = projections
        .iter()
        .map(|loaded| &loaded.projection)
        .collect::<Vec<_>>();
    let mut edges = Vec::with_capacity(projections.len());
    for (loaded, dependencies) in projections.iter().zip(dependencies) {
        edges.push(refresh_edge_for_projection(
            loaded,
            dependencies,
            &inventory,
        )?);
    }
    refresh_steps_for_edges(requested, &edges)
}

type RefreshEdge = (MvDependencyObjectRef, Vec<MvDependencyObjectRef>);

fn refresh_edge_for_projection(
    loaded: &LoadedKeyedMvProjection,
    dependencies: Vec<StoredMvDependency>,
    inventory: &[&StoredKeyedMvProjection],
) -> Result<RefreshEdge, String> {
    let target = stored_keyed_definition_dependency_ref(&loaded.projection);
    let upstream_mvs = classify_ready_dependency_occurrences(dependencies, inventory)?
        .into_iter()
        .filter(|dependency| {
            dependency.upstream.object_type == MvDependencyObjectType::MaterializedView
        })
        .map(|dependency| dependency.upstream)
        .collect();
    Ok((target, upstream_mvs))
}

fn refresh_steps_for_edges(
    requested: &MvDependencyObjectRef,
    edges: &[RefreshEdge],
) -> Result<Vec<MvRefreshDependencyStep>, String> {
    topological_upstream_order_for_edges(requested, edges)?
        .iter()
        .map(refresh_step_for_dependency_object)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::{KeyedProjectionFixture, keyed_repository};
    use novarocks_mv_application::dependency::{
        MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
        iceberg_mv_dependency_ref, iceberg_table_object_ref, starrocks_mv_dependency_ref,
    };

    #[tokio::test]
    async fn captured_refresh_graph_uses_exact_objects_and_rejects_cycles() {
        use novarocks_mv_application::persistence::exact_revision::persist_exact_query_revision;
        use novarocks_query_application::api::{DataVersion, ObjectIdentity, ProviderFactFormat};
        use novarocks_spi::connector::ConnectorTableObjectId;
        use std::sync::Arc;

        let repository = keyed_repository().await;
        let mut projections = Vec::new();
        for (name, object) in [
            ("renamed_leaf", b"leaf-object".as_slice()),
            ("middle", b"middle-object".as_slice()),
            ("requested", b"requested-object".as_slice()),
        ] {
            let mut fixture = KeyedProjectionFixture::new(
                novarocks_mv_application::product::MvTarget::from_parts(Some("ice"), "sales", name),
                None,
            );
            fixture.object_id =
                ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(object))
                    .expect("exact target object");
            projections.push(
                repository
                    .install_keyed_projection(
                        uuid::Uuid::now_v7(),
                        fixture.build().expect("validated Current"),
                        None,
                    )
                    .await
                    .expect("actual repository version"),
            );
        }
        let dependency = |downstream: usize, occurrence_id, object: &[u8]| {
            let format = |name| ProviderFactFormat::try_new_versioned("iceberg", name, 1).unwrap();
            let object =
                ObjectIdentity::try_new(format("table-object"), Arc::<[u8]>::from(object.to_vec()))
                    .unwrap();
            let version =
                DataVersion::try_new(format("snapshot"), Arc::<[u8]>::from(b"snapshot".to_vec()))
                    .unwrap();
            StoredMvDependency {
                downstream_mv_id: projections[downstream].projection.mv_id,
                occurrence_id,
                upstream_object_id: persist_exact_query_revision(&object, &version)
                    .unwrap()
                    .0
                    .as_bytes()
                    .to_vec()
                    .into(),
                // The persisted locator is deliberately stale. Only the exact
                // object may select the ready MV's current target.
                upstream: MvDependencyObjectRef {
                    catalog: Some("ice".into()),
                    database_or_namespace: "old_namespace".into(),
                    name: "old_name".into(),
                    object_type: MvDependencyObjectType::Unclassified,
                    storage_engine: MvDependencyStorageEngine::Unclassified,
                },
                created_at_ms: 1,
            }
        };
        let requested = iceberg_mv_dependency_ref("ice", "sales", "requested");
        let facts = MvRefreshGraphFacts {
            projections: projections.clone(),
            dependencies: vec![
                vec![],
                vec![dependency(1, 0, b"leaf-object")],
                vec![
                    dependency(2, 0, b"middle-object"),
                    dependency(2, 1, b"leaf-object"),
                    dependency(2, 2, b"leaf-object"),
                    dependency(2, 3, b"ordinary-table-object"),
                ],
            ],
        };
        let steps = finish_refresh_graph(facts, &requested).expect("same original graph author");
        assert_eq!(
            steps
                .iter()
                .map(|step| step.target().name.as_str())
                .collect::<Vec<_>>(),
            ["renamed_leaf", "middle", "requested"]
        );
        assert!(steps.iter().all(MvRefreshDependencyStep::is_iceberg));

        let cycle = MvRefreshGraphFacts {
            projections: projections.clone(),
            dependencies: vec![
                vec![dependency(0, 0, b"requested-object")],
                vec![dependency(1, 0, b"leaf-object")],
                vec![dependency(2, 0, b"middle-object")],
            ],
        };
        assert_eq!(
            finish_refresh_graph(cycle, &requested).expect_err("actual object cycle"),
            "dependency cycle detected while planning refresh at mv:ice.sales.requested"
        );
    }

    #[test]
    fn refresh_step_maps_materialized_view_storage_engines() {
        let starrocks = starrocks_mv_dependency_ref("sales", "orders_mv");
        let iceberg = iceberg_mv_dependency_ref("ice", "analytics", "orders_mv");

        assert_eq!(
            refresh_step_for_dependency_object(&starrocks).expect("StarRocks MV refresh step"),
            MvRefreshDependencyStep {
                object: starrocks,
                target: MvTarget {
                    catalog: None,
                    database: "sales".to_string(),
                    name: "orders_mv".to_string(),
                },
                storage_engine: MvStorageEngine::StarRocks,
            }
        );
        assert_eq!(
            refresh_step_for_dependency_object(&iceberg).expect("Iceberg MV refresh step"),
            MvRefreshDependencyStep {
                object: iceberg,
                target: MvTarget {
                    catalog: Some("ice".to_string()),
                    database: "analytics".to_string(),
                    name: "orders_mv".to_string(),
                },
                storage_engine: MvStorageEngine::Iceberg,
            }
        );
    }

    #[test]
    fn refresh_step_rejects_table_object() {
        let table = iceberg_table_object_ref("ice", "analytics", "orders");

        assert_eq!(
            refresh_step_for_dependency_object(&table).expect_err("table must not be refreshed"),
            "refresh dependency object is not a materialized view: ice.analytics.orders"
        );
    }

    #[test]
    fn refresh_step_rejects_external_table_materialized_view() {
        let external_mv = MvDependencyObjectRef {
            catalog: Some("external".to_string()),
            database_or_namespace: "analytics".to_string(),
            name: "orders_mv".to_string(),
            object_type: MvDependencyObjectType::MaterializedView,
            storage_engine: MvDependencyStorageEngine::ExternalTable,
        };

        assert_eq!(
            refresh_step_for_dependency_object(&external_mv)
                .expect_err("external table must not be refreshed as an MV"),
            "external table cannot be refreshed as materialized view: mv:external.analytics.orders_mv"
        );
    }

    #[test]
    fn refresh_step_rejects_unclassified_storage_even_for_an_mv_label() {
        let mut dependency = iceberg_mv_dependency_ref("ice", "analytics", "orders_mv");
        dependency.storage_engine = MvDependencyStorageEngine::Unclassified;

        assert_eq!(
            refresh_step_for_dependency_object(&dependency)
                .expect_err("an MV label cannot provide missing storage admission"),
            "unclassified dependency cannot be refreshed as materialized view: mv:ice.analytics.orders_mv",
        );
    }
}
