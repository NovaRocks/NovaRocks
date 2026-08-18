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

//! Pure reconstruction of an MV's repository definition-create inputs from its
//! lake package observation (descriptor + publication facts).
//!
//! Given nothing but a validated lake package observation,
//! [`rebuild_mv_definition_from_lake`] reproduces exactly the
//! inputs `create_iceberg_mv` would have persisted at CREATE
//! time, plus the refresh watermark a completed refresh would have recorded.
//! M3 calls this at startup for MVs discovered on the lake but missing from
//! the MV repository. No catalog I/O happens here — every input is already in
//! memory.

use std::collections::BTreeMap;
use std::sync::{Arc, atomic::AtomicBool};

use crate::mv::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use crate::mv::model::MvStorageEngine;
use crate::mv::persistence::definition::CreateMvDefinitionRequest;
use crate::mv::persistence::dependency::CreateMvDependencyRequest;
use crate::mv::persistence::descriptor::DescriptorDependency;
use crate::mv::storage_observation::{
    MvLakePackageObservation, MvLakePublication, discover_mv_lake_packages,
};

/// Output of [`rebuild_mv_definition_from_lake`]: the definition-create
/// request `create_iceberg_mv` would have issued, plus the refresh watermark
/// maps `StoredMvDefinition` separately tracks (`CreateMvDefinitionRequest`
/// has no watermark fields — a freshly-created MV has never refreshed).
pub(crate) struct RebuiltMvDefinition {
    pub create_request: CreateMvDefinitionRequest,
    pub last_refresh_snapshots: BTreeMap<String, i64>,
    pub last_refresh_table_uuids: BTreeMap<String, String>,
}

/// Reconstruct an MV's repository definition-create inputs purely from its lake
/// package (descriptor + optional current-snapshot provenance). Pure: no I/O.
///
pub(crate) fn rebuild_mv_definition_from_lake(
    package: &MvLakePackageObservation,
) -> Result<RebuiltMvDefinition, String> {
    let descriptor = &package.descriptor;

    let base_table_refs = descriptor
        .base_dependencies
        .iter()
        .map(|dep| format!("{}.{}.{}", dep.catalog, dep.namespace, dep.name))
        .collect();

    let schema_contract = descriptor.schema_contract_typed()?;
    let partition_spec = schema_contract
        .as_ref()
        .and_then(|contract| contract.target.partition.clone());

    let create_request = CreateMvDefinitionRequest {
        select_sql: descriptor.logical_sql.clone(),
        base_table_refs,
        // W1 descriptors carry no primary-key metadata; a rebuilt definition
        // is indistinguishable from one created without `PRIMARY KEY (...)`.
        primary_key_columns: Vec::new(),
        storage_engine: MvStorageEngine::Iceberg.as_sql_str().to_string(),
        target_catalog: Some(package.table.instance_id.as_str().to_string()),
        target_namespace: Some(package.table.namespace.to_string()),
        target_table: Some(package.table.table.to_string()),
        schema_contract,
        partition_spec,
        created_at_ms: descriptor.created_at_ms,
    };

    let (last_refresh_snapshots, last_refresh_table_uuids) = match &package.publication {
        MvLakePublication::Published(facts) => {
            let mut snapshots = BTreeMap::new();
            let mut table_uuids = BTreeMap::new();
            for base in &facts.bases {
                snapshots.insert(base.table_fqn.clone(), base.to_snapshot);
                table_uuids.insert(base.table_fqn.clone(), base.table_uuid.clone());
            }
            (snapshots, table_uuids)
        }
        MvLakePublication::NeverPublished => (BTreeMap::new(), BTreeMap::new()),
    };

    Ok(RebuiltMvDefinition {
        create_request,
        last_refresh_snapshots,
        last_refresh_table_uuids,
    })
}

/// Rebuild any lake-native Iceberg MV definitions that are present on the lake
/// but missing from the MV repository, making them visible and refreshable.
///
/// For every admitted Iceberg catalog we
/// enumerate its namespaces, discover the MV packages each namespace carries
/// (MV-table inline descriptor), and for each MV whose target is not already
/// recorded in the repository we
/// reconstruct its definition-create inputs with
/// [`rebuild_mv_definition_from_lake`] and persist them (definition + refresh
/// watermark + dependencies) through the repository's ordinary create path.
///
/// Idempotent: MVs already present in the repository (matched by target
/// `catalog.namespace.table`) are skipped, so calling this at startup
/// when all admitted packages are present is a no-op.
///
/// The state a lake rebuild reads, named explicitly rather than reached through
/// aggregate engine state.
///
/// Naming the inputs is what makes this module movable: it turns "needs the
/// engine" into a short, checkable list, and every one of these is already
/// reachable from a frontend composition.
pub struct LakeRebuildContext<'a> {
    /// Catalogs this process currently admits. An absent projection means no
    /// lease to enumerate namespaces with.
    pub catalog_runtime_projection:
        Option<&'a Arc<crate::catalog_application::CatalogRuntimeProjection>>,
    pub catalog_application: Option<&'a dyn crate::catalog_application::CatalogApplicationPort>,
    pub connector_control: &'a dyn novarocks_spi::connector::ConnectorControlRegistry,
    pub mv_storage_observation: &'a dyn crate::mv::storage_observation::MvStorageObservationPort,
    pub mv_repository: &'a dyn crate::mv::repository::MvRepository,
}

pub fn rebuild_imv_cache_from_lake(ctx: &LakeRebuildContext<'_>) -> Result<(), String> {
    let context =
        crate::connector::connector_request_context(None, Arc::new(AtomicBool::new(false)))?;
    // Only catalogs this process currently admits can be scanned: the durable
    // attachment record belongs to the Frontend controller, and an Unavailable
    // projection has no lease to enumerate namespaces with.
    let Some(projection) = ctx.catalog_runtime_projection else {
        return Ok(());
    };
    let instance_ids = projection
        .published_observations()
        .map_err(|error| format!("list admitted catalogs for MV rebuild failed: {error}"))?
        .into_iter()
        .filter(|observation| {
            observation
                .provider_id
                .as_str()
                .eq_ignore_ascii_case("iceberg")
        })
        .map(|observation| observation.instance_id)
        .collect::<Vec<_>>();
    let packages = discover_mv_lake_packages(
        ctx.connector_control,
        instance_ids,
        ctx.mv_storage_observation,
        context,
    )
    .map_err(|error| format!("discover lake MV packages failed: {error}"))?;
    for package in packages {
        // Startup rediscovery is opportunistic. A package on the lake whose
        // referenced catalogs are not all attached to this cluster is not ours
        // to rebuild: persisting it would create a durable MV definition that
        // references an absent attachment, which the MV writer's attachment
        // assertion correctly refuses. Skipping keeps a foreign or
        // already-dropped package from failing frontend startup, while the
        // targeted rebuild procedure still fails closed on the same condition.
        if !package_catalogs_are_admitted(ctx.catalog_application, &package)? {
            continue;
        }
        rebuild_one_lake_package_if_missing(ctx, &package)?;
    }
    Ok(())
}

/// Whether every catalog this lake MV package references is currently `Ready`
/// on this frontend.
fn package_catalogs_are_admitted(
    application: Option<&dyn crate::catalog_application::CatalogApplicationPort>,
    package: &MvLakePackageObservation,
) -> Result<bool, String> {
    let Some(application) = application else {
        return Ok(false);
    };
    let mut catalogs = std::collections::BTreeSet::new();
    catalogs.insert(package.table.instance_id.as_str().to_string());
    for dependency in &package.descriptor.base_dependencies {
        if !dependency.catalog.is_empty() {
            catalogs.insert(dependency.catalog.clone());
        }
    }
    for catalog in catalogs {
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&catalog)
            .map_err(|error| format!("parse MV rebuild catalog `{catalog}`: {error}"))?;
        if !matches!(
            application.admit_catalog(&instance_id),
            crate::catalog_application::CatalogAdmission::Ready(_)
        ) {
            tracing::info!(
                catalog = catalog.as_str(),
                mv_target = %package.table.table,
                "skipping lake MV rebuild because a referenced catalog attachment is not admitted here"
            );
            return Ok(false);
        }
    }
    Ok(true)
}

/// Persist a single observed lake package's definition if it is not already
/// present in the MV repository. The MV is keyed by its target `catalog.namespace.table`
/// (the same key the create path registers via `find_by_target`).
///
/// Exposed to the crate so the W0 stateless-rebuild harness
/// (`stateless_rebuild::execute_request`) can drive a *targeted* single-MV
/// rebuild for the `full` level, instead of sweeping every registered catalog
/// through [`rebuild_imv_cache_from_lake`].
pub(crate) fn rebuild_one_lake_package_if_missing(
    ctx: &LakeRebuildContext<'_>,
    package: &MvLakePackageObservation,
) -> Result<(), String> {
    rebuild_one_lake_package_if_missing_with_repository(ctx.mv_repository, package)
}

/// Targeted lake-package rebuild with the only capability it actually needs:
/// durable MV repository mutation.  Unlike startup discovery, the caller has
/// already selected and observed one exact package, so it must not acquire a
/// new catalog projection or connector lease while rebuilding the cache.
pub(crate) fn rebuild_one_lake_package_if_missing_with_repository(
    repository: &dyn crate::mv::repository::MvRepository,
    package: &MvLakePackageObservation,
) -> Result<(), String> {
    // Repository-hit check: skip MVs already recorded. The rebuilt target
    // maps to (discovered.catalog, discovered.namespace, discovered.table).
    let existing = repository
        .find_by_target(&crate::mv::model::MvTarget {
            catalog: Some(package.table.instance_id.as_str().to_string()),
            database: package.table.namespace.to_string(),
            name: package.table.table.to_string(),
        })
        .map_err(|e| format!("look up MV definition during lake rebuild failed: {e}"))?;
    if existing.is_some() {
        return Ok(());
    }

    let rebuilt = rebuild_mv_definition_from_lake(package)?;
    let created_at_ms = rebuilt.create_request.created_at_ms;
    let dependencies =
        dependency_requests_from_descriptor(&package.descriptor.base_dependencies, created_at_ms)?;

    let definition = repository
        .create(
            uuid::Uuid::new_v4(),
            crate::mv::repository::CreateMvRepositoryRequest {
                definition: rebuilt.create_request,
                refresh: Default::default(),
                dependencies: dependencies.clone(),
            },
        )
        .map_err(|e| format!("rebuild iceberg MV repository metadata failed: {e}"))?;
    repository
        .initialize_rebuilt_refresh_watermark(
            definition.mv_id,
            rebuilt.last_refresh_snapshots,
            rebuilt.last_refresh_table_uuids,
        )
        .map_err(|e| format!("stamp rebuilt iceberg MV refresh watermark failed: {e}"))?;
    Ok(())
}

/// Map the descriptor's `base_dependencies` back into the repository
/// `CreateMvDependencyRequest` shape used by `replace_dependencies_for_mv`.
/// This is the inverse of `iceberg_refresh::descriptor_dependency_from_request`.
fn dependency_requests_from_descriptor(
    dependencies: &[DescriptorDependency],
    created_at_ms: i64,
) -> Result<Vec<CreateMvDependencyRequest>, String> {
    dependencies
        .iter()
        .map(|dep| {
            Ok(CreateMvDependencyRequest {
                upstream: MvDependencyObjectRef {
                    catalog: (!dep.catalog.is_empty()).then(|| dep.catalog.clone()),
                    database_or_namespace: dep.namespace.clone(),
                    name: dep.name.clone(),
                    object_type: parse_dependency_object_type(&dep.object_type)?,
                    storage_engine: parse_dependency_storage_engine(&dep.storage_engine)?,
                },
                created_at_ms,
            })
        })
        .collect()
}

fn parse_dependency_object_type(value: &str) -> Result<MvDependencyObjectType, String> {
    match value {
        "table" => Ok(MvDependencyObjectType::Table),
        "materialized_view" => Ok(MvDependencyObjectType::MaterializedView),
        other => Err(format!(
            "unknown MV descriptor dependency object type `{other}`"
        )),
    }
}

fn parse_dependency_storage_engine(value: &str) -> Result<MvDependencyStorageEngine, String> {
    match value {
        "starrocks" => Ok(MvDependencyStorageEngine::StarRocks),
        "iceberg" => Ok(MvDependencyStorageEngine::Iceberg),
        "external_table" => Ok(MvDependencyStorageEngine::ExternalTable),
        other => Err(format!(
            "unknown MV descriptor dependency storage engine `{other}`"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::persistence::descriptor::{DescriptorDependency, MvDescriptorV1};
    use crate::mv::persistence::schema::{
        BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind, ExpressionLineage,
        HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
        MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
        TargetContract, TargetVisibleColumn,
    };
    use crate::mv::storage_observation::{
        MvLakePackageObservation, MvLakePublication, MvPublishedBaseFact, MvPublishedLakeFacts,
        MvPublishedRefreshTechnique,
    };
    use novarocks_spi::connector::{ConnectorInstanceId, ConnectorTableIdentity};
    use novarocks_sql::planning::mv::ApplyKeySource;
    use std::sync::Arc;

    fn sample_contract() -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.sales.orders".to_string(),
                table_uuid: "uuid-orders".to_string(),
                alias_at_create: None,
                schema_id_at_create: 1,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: "int".to_string(),
                        required: true,
                    }],
                },
            },
            bases: vec![],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: vec![],
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.analytics.mv_orders".to_string(),
                table_uuid: "uuid-mv".to_string(),
                schema_id_at_create: 1,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: "int".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__nova_base_row_id".to_string(),
                    target_field_id: 99,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: Some(MvPartitionContract {
                    target_spec_id: 0,
                    fields: vec![MvPartitionFieldContract {
                        partition_field_id: 1000,
                        partition_field_name: "id_bucket".to_string(),
                        source_target_field_id: 1,
                        source_column_name: "id".to_string(),
                        transform: MvPartitionTransformContract::Bucket { num_buckets: 4 },
                    }],
                }),
            },
        }
    }

    fn sample_package(publication: MvLakePublication) -> MvLakePackageObservation {
        let mut descriptor = MvDescriptorV1 {
            descriptor_version: 1,
            package_id: "analytics.mv_orders".to_string(),
            logical_sql: "SELECT id FROM ice.sales.orders".to_string(),
            dialect: "starrocks".to_string(),
            visible_columns: vec!["id".to_string()],
            hidden_columns: vec!["__nova_base_row_id".to_string()],
            base_dependencies: vec![DescriptorDependency {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                name: "orders".to_string(),
                object_type: "table".to_string(),
                storage_engine: "iceberg".to_string(),
            }],
            schema_contract: None,
            refresh_contract: None,
            created_at_ms: 123,
        };
        descriptor
            .set_schema_contract(&sample_contract())
            .expect("set schema contract");
        MvLakePackageObservation::try_new(
            ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse("ice").expect("instance ID"),
                namespace: Arc::from("analytics"),
                table: Arc::from("mv_orders"),
            },
            descriptor,
            publication,
        )
        .expect("valid lake package")
    }

    fn sample_publication() -> MvLakePublication {
        MvLakePublication::Published(
            MvPublishedLakeFacts::try_new(
                300,
                7,
                1,
                "token-7".to_string(),
                MvPublishedRefreshTechnique::Incremental,
                vec![MvPublishedBaseFact {
                    table_fqn: "ice.sales.orders".to_string(),
                    table_uuid: "uuid-orders".to_string(),
                    from_snapshot: Some(100),
                    to_snapshot: 200,
                }],
                "fp-abc".to_string(),
                42,
                "provenance-hash".to_string(),
                "waterline-hash".to_string(),
            )
            .expect("valid published facts"),
        )
    }

    #[test]
    fn rebuild_maps_descriptor_and_provenance() {
        let package = sample_package(sample_publication());

        let rebuilt = rebuild_mv_definition_from_lake(&package).expect("rebuild succeeds");

        let request = &rebuilt.create_request;
        assert_eq!(request.select_sql, "SELECT id FROM ice.sales.orders");
        assert_eq!(
            request.base_table_refs,
            vec!["ice.sales.orders".to_string()]
        );
        assert!(request.primary_key_columns.is_empty());
        assert_eq!(
            request.storage_engine,
            MvStorageEngine::Iceberg.as_sql_str()
        );
        assert_eq!(request.target_catalog.as_deref(), Some("ice"));
        assert_eq!(request.target_namespace.as_deref(), Some("analytics"));
        assert_eq!(request.target_table.as_deref(), Some("mv_orders"));
        assert_eq!(request.created_at_ms, 123);

        let contract = request
            .schema_contract
            .as_ref()
            .expect("schema contract present");
        assert_eq!(contract, &sample_contract());

        let partition = request.partition_spec.as_ref().expect("partition spec");
        assert_eq!(partition.target_spec_id, 0);
        assert_eq!(partition.fields.len(), 1);
        assert_eq!(partition.fields[0].partition_field_name, "id_bucket");

        assert_eq!(
            rebuilt.last_refresh_snapshots.get("ice.sales.orders"),
            Some(&200)
        );
        assert_eq!(
            rebuilt.last_refresh_table_uuids.get("ice.sales.orders"),
            Some(&"uuid-orders".to_string())
        );
    }

    #[test]
    fn rebuild_never_published_has_empty_watermark() {
        let package = sample_package(MvLakePublication::NeverPublished);

        let rebuilt = rebuild_mv_definition_from_lake(&package).expect("rebuild succeeds");

        assert!(rebuilt.last_refresh_snapshots.is_empty());
        assert!(rebuilt.last_refresh_table_uuids.is_empty());
        // The create request is still fully valid even with no refresh history.
        assert_eq!(
            rebuilt.create_request.select_sql,
            "SELECT id FROM ice.sales.orders"
        );
        assert!(rebuilt.create_request.schema_contract.is_some());
    }

    struct FixedAdmission(crate::catalog_application::CatalogAdmission);

    impl crate::catalog_application::CatalogApplicationPort for FixedAdmission {
        fn create_catalog(
            &self,
            _command: crate::catalog_application::CatalogCreateCommand,
        ) -> Result<
            crate::catalog_application::CatalogRuntimeObservation,
            crate::catalog_application::CatalogApplicationError,
        > {
            unreachable!("lake rebuild never creates a catalog")
        }

        fn drop_catalog(
            &self,
            _command: crate::catalog_application::CatalogDropCommand,
        ) -> Result<(), crate::catalog_application::CatalogApplicationError> {
            unreachable!("lake rebuild never drops a catalog")
        }

        fn admit_catalog(
            &self,
            _instance_id: &ConnectorInstanceId,
        ) -> crate::catalog_application::CatalogAdmission {
            self.0.clone()
        }
    }

    fn application_with_admission(
        admission: crate::catalog_application::CatalogAdmission,
    ) -> Arc<dyn crate::catalog_application::CatalogApplicationPort> {
        Arc::new(FixedAdmission(admission))
    }

    /// Startup rediscovery must not take the frontend down over a lake package
    /// it has no business rebuilding. Persisting one would create a durable MV
    /// definition pointing at an absent attachment, which the MV writer's
    /// attachment assertion refuses — previously that surfaced as a fatal
    /// "rebuild iceberg MV repository metadata failed" during FE startup.
    #[test]
    fn sweep_skips_a_package_whose_catalog_is_not_admitted_here() {
        let package = sample_package(sample_publication());

        let absent =
            application_with_admission(crate::catalog_application::CatalogAdmission::Absent);
        assert!(
            !package_catalogs_are_admitted(Some(absent.as_ref()), &package)
                .expect("absent admission is decidable"),
            "an absent attachment must make the sweep skip the package"
        );

        let unavailable =
            application_with_admission(crate::catalog_application::CatalogAdmission::Unavailable {
                reason: "projection is stale".to_string(),
            });
        assert!(
            !package_catalogs_are_admitted(Some(unavailable.as_ref()), &package)
                .expect("unavailable admission is decidable"),
            "an unmaterialized attachment must also make the sweep skip the package"
        );

        let ready =
            application_with_admission(crate::catalog_application::CatalogAdmission::Ready(
                crate::catalog_application::CatalogRuntimeObservation {
                    attachment_id: uuid::Uuid::now_v7(),
                    instance_id: ConnectorInstanceId::parse("ice").expect("instance ID"),
                    provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                        .expect("provider ID"),
                    generation: 1,
                },
            ));
        assert!(
            package_catalogs_are_admitted(Some(ready.as_ref()), &package)
                .expect("ready admission is decidable"),
            "a package whose target and upstream catalogs are admitted must be rebuilt"
        );
    }

    /// Without a catalog application there is no attachment authority at all, so
    /// the sweep cannot prove the package belongs to this cluster.
    #[test]
    fn sweep_skips_every_package_without_a_catalog_application() {
        let package = sample_package(sample_publication());
        assert!(
            !package_catalogs_are_admitted(None, &package)
                .expect("missing application is decidable"),
        );
    }
}
