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
use crate::mv::domain::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use crate::mv::domain::model::{MvStorageEngine, MvTarget};
use crate::mv::domain::persistence::definition::StoredMvDefinition;
use crate::mv::domain::persistence::dependency::stored_definition_dependency_ref;
use crate::mv::domain::repository::MvRepository;

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

/// Resolves the required upstream MV refresh order from persisted dependency
/// edges. The caller owns refresh admission; Core returns only domain steps.
pub fn build_upstream_refresh_steps_with_repository(
    repository: &dyn MvRepository,
    requested: &MvDependencyObjectRef,
) -> Result<Vec<MvRefreshDependencyStep>, String> {
    let definitions = repository
        .list_definitions()
        .map_err(|e| format!("load MV definitions for refresh graph failed: {e}"))?;

    let mut edges = Vec::new();
    for definition in definitions {
        let target = stored_definition_dependency_ref_for_iceberg(&definition)?;
        let upstream_mvs = repository
            .list_dependencies_by_downstream(definition.mv_id)
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
    use crate::mv::domain::dependency::model::{
        MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
        iceberg_mv_dependency_ref, iceberg_table_object_ref, starrocks_mv_dependency_ref,
    };

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
}
