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

//! Application catalog materialization for one admitted SQL request.
//!
//! This is intentionally outside `sql::catalog`: it owns connector-facing
//! resolution and the exact binding store used later by statistics and scan
//! preparation.  SQL sees the resulting neutral table facts solely through
//! `PlannerTableProvider`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use novarocks_catalog::partition::LegacyRangePartition;
use novarocks_catalog::provider::CatalogProvider;
use novarocks_catalog::table::CatalogTable;
use novarocks_spi::connector::ConnectorControlResolver;

use crate::catalog_application::query_bindings::{
    QueryScanMaterialization, QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey,
    QueryTableBindingStore,
};
use crate::catalog_application::query_catalog::{
    ConnectorQueryTableMaterialization, QueryCatalogService,
    load_connector_table_alias_materialization_with_lease,
    load_connector_table_materialization_with_lease,
};
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::catalog::{
    IcebergMetadataTableProvider, PlannerTableProvider, ResolvedAnalyzerTable, TableLookupMode,
};

/// Project a provider-neutral SPI metadata materialization into the
/// request-local SQL binding. Provider aliases retain their separately frozen
/// provider-owned facts until their dedicated adapters run.
pub(crate) fn connector_query_binding_from_materialization(
    materialization: ConnectorQueryTableMaterialization,
    catalog: &str,
    namespace: &str,
    sql_table_name: &str,
    binding: SqlTableBindingId,
) -> Result<QueryTableBinding, String> {
    let sql_materialization = novarocks_sql::planning::catalog::materialize_connector_read_table(
        novarocks_sql::planning::catalog::ConnectorReadTableFacts {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: sql_table_name.to_string(),
            columns: materialization.columns,
            iceberg_row_lineage_metadata_columns: materialization.row_lineage_metadata_columns,
            schema: materialization.read_schema.clone(),
            binding,
            selector: materialization.read_selector,
            planning_facts: materialization.sql_planning_facts,
        },
    )?;
    let frozen_snapshot_materializations = sql_materialization
        .frozen_snapshot_id()
        .into_iter()
        .map(|snapshot_id| {
            (
                snapshot_id,
                QueryScanMaterialization {
                    table: materialization.read_table.clone(),
                    schema: materialization.read_schema.clone(),
                    selector: novarocks_spi::connector::ConnectorReadSelector::SnapshotId(
                        snapshot_id,
                    ),
                    statistics_pin: materialization.statistics_pin.clone(),
                    planning_lease: materialization.planning_lease.clone(),
                },
            )
        })
        .collect();
    Ok(QueryTableBinding {
        resolved: sql_materialization.into_resolved_table(),
        statistics_pin: materialization.statistics_pin.clone(),
        admission: QueryTableBindingAdmission::Exact(materialization.planning_lease.clone()),
        scan_materialization: Some(QueryScanMaterialization {
            table: materialization.read_table,
            schema: materialization.read_schema,
            selector: materialization.read_selector,
            statistics_pin: materialization.statistics_pin,
            planning_lease: materialization.planning_lease,
        }),
        mv_target_read: None,
        write_target_admission: None,
        frozen_snapshot_materializations,
        admitted_change_scans: BTreeMap::new(),
    })
}

/// Admit one provider-owned change window while the caller holds the exact
/// table handle and planning lease. The returned sealed scan is the sole
/// physical authority retained by Core for later preparation.
/// Application materializer for connector-controlled table metadata.  The
/// interface is intentionally application-owned because it returns an exact
/// lease alongside planner facts.  It is not part of SQL's vocabulary.
pub(crate) trait QueryTableBindingLoader: Send + Sync {
    fn load_strict_base_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        binding: SqlTableBindingId,
    ) -> Result<QueryTableBinding, String>;

    fn load_metadata_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
        binding: SqlTableBindingId,
    ) -> Result<QueryTableBinding, String>;
}

/// Application-owned catalog facade.  Its binding store is request-local and
/// retained by the caller as post-compile context; the SQL catalog trait does
/// not expose it.
pub struct CatalogServiceMaterializer<'a> {
    current_catalog: Option<&'a str>,
    service: &'a crate::catalog_application::query_catalog::QueryCatalogService,
    bindings: Arc<QueryTableBindingStore>,
    loader: Box<dyn QueryTableBindingLoader + 'a>,
    /// Frontend-owned attachment admission. The loader still owns the exact
    /// connector lease; this gate preserves Absent versus Unavailable before
    /// Core can materialize an external table.
    catalog_application: Option<&'a dyn crate::catalog_application::CatalogApplicationPort>,
    /// Request-scoped synthetic relations used by application rewrite flows.
    /// They are intentionally kept next to the binding store instead of the
    /// shared memory catalog: SQL can only observe their projected tokenized
    /// scan after this materializer has admitted the exact connector lease.
    query_local_overlays: HashMap<(String, String), QueryLocalTableOverlay>,
}

/// One application-owned relation overlay for a generated query.
///
/// The overlay is a binding factory, not a planner table definition: generated COW and MV
/// reads must supply their frozen provider facts to the request-local store
/// before SQL sees the resulting tokenized table.  Keeping the factory here
/// prevents a synthetic relation from leaking into the shared catalog.
#[derive(Clone)]
pub(crate) struct QueryLocalTableOverlay {
    namespace: String,
    table: String,
    key: QueryTableBindingKey,
    materialize:
        Arc<dyn Fn(SqlTableBindingId) -> Result<QueryTableBinding, String> + Send + Sync + 'static>,
}

impl QueryLocalTableOverlay {
    pub(crate) fn new(
        namespace: impl Into<String>,
        table: impl Into<String>,
        key: QueryTableBindingKey,
        materialize: impl Fn(SqlTableBindingId) -> Result<QueryTableBinding, String>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        let namespace = namespace.into();
        Self {
            table: table.into(),
            namespace,
            key,
            materialize: Arc::new(materialize),
        }
    }

    fn key(&self) -> (String, String) {
        (
            self.namespace.to_ascii_lowercase(),
            self.table.to_ascii_lowercase(),
        )
    }
}

impl<'a> CatalogServiceMaterializer<'a> {
    pub(crate) fn new(
        current_catalog: Option<&'a str>,
        service: &'a crate::catalog_application::query_catalog::QueryCatalogService,
        bindings: Arc<QueryTableBindingStore>,
        loader: Box<dyn QueryTableBindingLoader + 'a>,
    ) -> Self {
        Self::new_with_query_local_overlays(current_catalog, service, bindings, loader, Vec::new())
    }

    pub(crate) fn new_with_query_local_overlays(
        current_catalog: Option<&'a str>,
        service: &'a crate::catalog_application::query_catalog::QueryCatalogService,
        bindings: Arc<QueryTableBindingStore>,
        loader: Box<dyn QueryTableBindingLoader + 'a>,
        overlays: Vec<QueryLocalTableOverlay>,
    ) -> Self {
        Self {
            current_catalog,
            service,
            bindings,
            loader,
            catalog_application: None,
            query_local_overlays: overlays
                .into_iter()
                .map(|overlay| (overlay.key(), overlay))
                .collect(),
        }
    }

    pub(crate) fn query_table_bindings(&self) -> Arc<QueryTableBindingStore> {
        Arc::clone(&self.bindings)
    }

    pub(crate) fn with_catalog_application(
        mut self,
        catalog_application: Option<&'a dyn crate::catalog_application::CatalogApplicationPort>,
    ) -> Self {
        self.catalog_application = catalog_application;
        self
    }

    fn require_catalog_admission(
        &self,
        catalog: &str,
    ) -> Result<Option<crate::catalog_application::CatalogRuntimeObservation>, String> {
        let Some(application) = self.catalog_application else {
            return Ok(None);
        };
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(catalog)
            .map_err(|error| format!("invalid catalog instance `{catalog}`: {error}"))?;
        application
            .admit_catalog(&instance_id)
            .require_ready(&instance_id)
            .map(Some)
            .map_err(|error| error.to_string())
    }

    fn verify_catalog_admission(
        &self,
        catalog: &str,
        expected: Option<&crate::catalog_application::CatalogRuntimeObservation>,
    ) -> Result<(), String> {
        let Some(expected) = expected else {
            return Ok(());
        };
        let current = self
            .require_catalog_admission(catalog)?
            .ok_or_else(|| "catalog admission unexpectedly became legacy".to_string())?;
        if &current != expected {
            return Err(
                "catalog attachment generation changed while acquiring its planning lease"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Publish one application-resolved table only after its scan has been
    /// projected into the SQL vocabulary with the token allocated for this
    /// request.  Provider loaders may temporarily use a legacy carrier while
    /// decoding connector metadata, but that carrier must not escape this
    /// method into analysis or the compiler.
    fn bind_for_sql(
        &self,
        key: QueryTableBindingKey,
        load: impl FnOnce(SqlTableBindingId) -> Result<QueryTableBinding, String>,
    ) -> Result<SqlTableBindingId, String> {
        self.bindings.resolve_or_insert_with_id(key, |binding_id| {
            project_binding_for_sql(binding_id, load(binding_id)?)
        })
    }

    fn effective_catalog<'b>(&'b self, override_catalog: Option<&'b str>) -> Option<&'b str> {
        override_catalog.or(self.current_catalog)
    }

    fn resolve_table_for_analysis_once(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => {
                if let Some(overlay) = self
                    .query_local_overlays
                    .get(&(database.to_ascii_lowercase(), table.to_ascii_lowercase()))
                    .cloned()
                {
                    return self.resolve_query_local_overlay(overlay);
                }
                let local = self
                    .service
                    .local()
                    .read()
                    .expect("catalog service local read lock");
                let resolved = novarocks_sql::planning::catalog::resolve_local_catalog_table(
                    &local, database, table,
                )?;
                let key = QueryTableBindingKey::analysis_lookup("default_catalog", database, table);
                let token = self.bind_for_sql(key, |binding| {
                    Ok(QueryTableBinding::local(resolved, binding))
                })?;
                Ok(self.bindings.binding(token)?.resolved.clone())
            }
            Some(catalog) => {
                let observation = self.require_catalog_admission(catalog)?;
                let key = QueryTableBindingKey::analysis_lookup(catalog, database, table);
                let token = self.bind_for_sql(key, |binding_id| {
                    self.loader
                        .load_strict_base_table(catalog, database, table, binding_id)
                })?;
                self.verify_catalog_admission(catalog, observation.as_ref())?;
                Ok(self.bindings.binding(token)?.resolved.clone())
            }
        }
    }

    /// Materialize a generated local relation through the same request store
    /// as ordinary external tables.  The factory receives the exact token it
    /// must attach to the SQL table, while frozen provider facts remain paired
    /// with that token in the returned application binding.
    fn resolve_query_local_overlay(
        &self,
        overlay: QueryLocalTableOverlay,
    ) -> Result<ResolvedAnalyzerTable, String> {
        let token =
            self.bind_for_sql(overlay.key, |binding_id| (overlay.materialize)(binding_id))?;
        Ok(self.bindings.binding(token)?.resolved.clone())
    }

    fn metadata_table_def(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
    ) -> Result<ResolvedAnalyzerTable, String> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => {
                let local = self
                    .service
                    .local()
                    .read()
                    .expect("catalog service local read lock");
                novarocks_sql::planning::catalog::resolve_local_catalog_table(
                    &local, database, table,
                )
            }
            Some(catalog) => {
                let observation = self.require_catalog_admission(catalog)?;
                let key =
                    QueryTableBindingKey::metadata(catalog, database, table, metadata_table_type);
                let token = self.bind_for_sql(key, |binding_id| {
                    self.loader.load_metadata_table(
                        catalog,
                        database,
                        table,
                        metadata_table_type,
                        binding_id,
                    )
                })?;
                self.verify_catalog_admission(catalog, observation.as_ref())?;
                Ok(self.bindings.binding(token)?.resolved.clone())
            }
        }
    }
}

fn project_binding_for_sql(
    binding_id: SqlTableBindingId,
    binding: QueryTableBinding,
) -> Result<QueryTableBinding, String> {
    binding.validate_sql_scan_binding(binding_id)?;
    Ok(binding)
}

impl CatalogProvider for CatalogServiceMaterializer<'_> {
    fn get_table(&self, database: &str, table: &str) -> Result<CatalogTable, String> {
        self.resolve_table_for_analysis_once(None, database, table)
            .map(|resolved| novarocks_sql::planning::catalog::catalog_table(&resolved))
    }

    fn get_table_in_catalog(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<CatalogTable, String> {
        self.resolve_table_for_analysis_once(catalog, database, table)
            .map(|resolved| novarocks_sql::planning::catalog::catalog_table(&resolved))
    }

    fn get_legacy_range_partition(
        &self,
        database: &str,
        table: &str,
        partition: &str,
    ) -> Result<Option<LegacyRangePartition>, String> {
        self.service
            .local()
            .read()
            .expect("catalog service local read lock")
            .get_legacy_range_partition(database, table, partition)
    }
}

impl PlannerTableProvider for CatalogServiceMaterializer<'_> {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.resolve_table_for_analysis_once(catalog, database, table)
    }

    fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
        Some(self)
    }
}

impl novarocks_sql::compiler::SqlCatalogSnapshot for CatalogServiceMaterializer<'_> {
    fn planner_table_provider(&self) -> &dyn PlannerTableProvider {
        self
    }
}

impl IcebergMetadataTableProvider for CatalogServiceMaterializer<'_> {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.metadata_table_def(catalog, database, table, metadata_table_type)
    }
}

/// Builds the request-local SQL materializer behind the Frontend-owned catalog
/// admission gate.
///
/// Every analyzer entry point passes the state's application port: an external
/// table can only be materialized while its attachment is `Ready` in this
/// process, and there is no ungated variant to fall back to.
pub fn build_catalog_service_provider<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    _lookup_mode: TableLookupMode,
    catalog_application: Option<&'a dyn crate::catalog_application::CatalogApplicationPort>,
) -> CatalogServiceMaterializer<'a> {
    build_catalog_service_provider_with_query_local_overlays(
        current_catalog,
        catalog_service,
        controls,
        connector_context,
        _lookup_mode,
        Vec::new(),
        catalog_application,
    )
}

/// Build the application catalog facade for one admitted query, optionally
/// supplying generated relations that are scoped to that request. These
/// overlays are projected into SQL binding tokens before analysis and never
/// enter the shared local catalog.
pub(crate) fn build_catalog_service_provider_with_query_local_overlays<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    _lookup_mode: TableLookupMode,
    overlays: Vec<QueryLocalTableOverlay>,
    catalog_application: Option<&'a dyn crate::catalog_application::CatalogApplicationPort>,
) -> CatalogServiceMaterializer<'a> {
    let bindings = Arc::new(
        QueryTableBindingStore::try_new()
            .expect("query table binding scope allocation must not fail"),
    );
    build_catalog_service_provider_with_bindings_and_query_local_overlays(
        current_catalog,
        catalog_service,
        controls,
        connector_context,
        bindings,
        overlays,
        catalog_application,
    )
}

pub(crate) fn build_catalog_service_provider_with_bindings_and_query_local_overlays<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    bindings: Arc<QueryTableBindingStore>,
    overlays: Vec<QueryLocalTableOverlay>,
    catalog_application: Option<&'a dyn crate::catalog_application::CatalogApplicationPort>,
) -> CatalogServiceMaterializer<'a> {
    let loader = iceberg_table_binding_loader(controls, connector_context);
    CatalogServiceMaterializer::new_with_query_local_overlays(
        current_catalog,
        catalog_service,
        bindings,
        loader,
        overlays,
    )
    .with_catalog_application(catalog_application)
}

/// Application adapter for the SQL catalog's provider-neutral materialization
/// seam. The resulting binding carries the exact planning lease acquired for
/// metadata; SQL itself never names the Iceberg provider.
pub(crate) fn iceberg_table_binding_loader<'a>(
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Box<dyn QueryTableBindingLoader + 'a> {
    Box::new(IcebergTableBindingLoader {
        controls,
        connector_context,
    })
}

struct IcebergTableBindingLoader<'a> {
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

impl QueryTableBindingLoader for IcebergTableBindingLoader<'_> {
    fn load_strict_base_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        binding_id: SqlTableBindingId,
    ) -> Result<QueryTableBinding, String> {
        let (base_table, snapshot_id) =
            crate::catalog_application::query_bindings::parse_time_travel_overlay_identity(table)
                .map(|(base_table, snapshot_id)| (base_table, Some(snapshot_id)))
                .unwrap_or((table, None));
        let mut materialization = load_connector_table_materialization_with_lease(
            self.controls,
            self.connector_context.clone(),
            catalog,
            namespace,
            base_table,
        )?;
        if let Some(snapshot_id) = snapshot_id {
            materialization.read_selector =
                novarocks_spi::connector::ConnectorReadSelector::SnapshotId(snapshot_id);
        }
        connector_query_binding_from_materialization(
            materialization,
            catalog,
            namespace,
            table,
            binding_id,
        )
    }

    fn load_metadata_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
        binding_id: SqlTableBindingId,
    ) -> Result<QueryTableBinding, String> {
        let alias = format!(
            "{table}${}",
            metadata_table_alias_suffix(metadata_table_type)
        );
        let materialization = load_connector_table_alias_materialization_with_lease(
            self.controls,
            self.connector_context.clone(),
            catalog,
            namespace,
            &alias,
        )?;
        Ok(QueryTableBinding {
            resolved: novarocks_sql::planning::catalog::resolved_metadata_table(
                catalog,
                namespace,
                table,
                metadata_table_type,
                materialization.columns,
                materialization.row_lineage_metadata_columns,
                binding_id,
            ),
            statistics_pin: materialization.statistics_pin.clone(),
            admission: QueryTableBindingAdmission::Exact(materialization.planning_lease.clone()),
            scan_materialization: Some(QueryScanMaterialization {
                table: materialization.read_table,
                schema: materialization.read_schema,
                selector: materialization.read_selector,
                statistics_pin: materialization.statistics_pin,
                planning_lease: materialization.planning_lease,
            }),
            mv_target_read: None,
            write_target_admission: None,
            frozen_snapshot_materializations: BTreeMap::new(),
            admitted_change_scans: BTreeMap::new(),
        })
    }
}

fn metadata_table_alias_suffix(
    kind: novarocks_sql::planning::catalog::MetadataTableKind,
) -> &'static str {
    use novarocks_sql::planning::catalog::MetadataTableKind;

    match kind {
        MetadataTableKind::Snapshots => "SNAPSHOTS",
        MetadataTableKind::History => "HISTORY",
        MetadataTableKind::Refs => "REFS",
        MetadataTableKind::Files => "FILES",
        MetadataTableKind::Manifests => "MANIFESTS",
        MetadataTableKind::Partitions => "PARTITIONS",
        MetadataTableKind::LogicalIcebergMetadata => "LOGICAL_ICEBERG_METADATA",
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use novarocks_sql::binding::SqlTableBindingAllocator;
    use novarocks_sql::planning::catalog::PlannerTableProvider;

    fn binding_id(scope: u64, ordinal: u32) -> SqlTableBindingId {
        let ordinal = NonZeroU32::new(ordinal).expect("non-zero ordinal");
        let mut allocator =
            SqlTableBindingAllocator::try_new(NonZeroU64::new(scope).expect("non-zero scope"))
                .expect("test binding allocator");
        for _ in 1..ordinal.get() {
            allocator.allocate().expect("non-zero test binding ordinal");
        }
        allocator.allocate().expect("non-zero test binding ordinal")
    }

    fn local_binding(binding: SqlTableBindingId) -> QueryTableBinding {
        QueryTableBinding::local(
            test_connector_read_materialization(
                "default_catalog",
                "orders",
                binding,
                novarocks_spi::connector::ConnectorReadSelector::Current,
            ),
            binding,
        )
    }

    fn frozen_overlay_binding(binding: SqlTableBindingId) -> QueryTableBinding {
        QueryTableBinding {
            resolved: test_connector_read_materialization(
                "ice",
                "__nr_cow_orders",
                binding,
                novarocks_spi::connector::ConnectorReadSelector::SnapshotId(7),
            ),
            statistics_pin: None,
            admission: QueryTableBindingAdmission::Local,
            scan_materialization: None,
            write_target_admission: None,
            mv_target_read: None,
            frozen_snapshot_materializations: BTreeMap::new(),
            admitted_change_scans: BTreeMap::new(),
        }
    }

    fn test_connector_read_materialization(
        catalog: &str,
        table: &str,
        binding: SqlTableBindingId,
        selector: novarocks_spi::connector::ConnectorReadSelector,
    ) -> ResolvedAnalyzerTable {
        novarocks_sql::planning::catalog::materialize_connector_read_table(
            novarocks_sql::planning::catalog::ConnectorReadTableFacts {
                catalog: catalog.to_string(),
                namespace: "db".to_string(),
                table: table.to_string(),
                columns: Vec::new(),
                iceberg_row_lineage_metadata_columns: Vec::new(),
                schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                binding,
                selector,
                planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
            },
        )
        .expect("test catalog facts materialize")
        .into_resolved_table()
    }

    struct OverlayLoader;

    impl QueryTableBindingLoader for OverlayLoader {
        fn load_strict_base_table(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
            _binding: SqlTableBindingId,
        ) -> Result<QueryTableBinding, String> {
            Ok(local_binding(_binding))
        }

        fn load_metadata_table(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
            _metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
            _binding: SqlTableBindingId,
        ) -> Result<QueryTableBinding, String> {
            Err("metadata is not part of this overlay fixture".to_string())
        }
    }

    struct UnavailableCatalogApplication;

    struct ChangingCatalogApplication {
        admissions: AtomicUsize,
    }

    impl ChangingCatalogApplication {
        fn observation(generation: u64) -> crate::catalog_application::CatalogRuntimeObservation {
            crate::catalog_application::CatalogRuntimeObservation {
                attachment_id: if generation == 1 {
                    uuid::Uuid::from_u128(1)
                } else {
                    uuid::Uuid::from_u128(2)
                },
                instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("ice")
                    .expect("instance ID"),
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                    .expect("provider ID"),
                generation,
            }
        }
    }

    impl crate::catalog_application::CatalogApplicationPort for ChangingCatalogApplication {
        fn create_catalog(
            &self,
            _command: crate::catalog_application::CatalogCreateCommand,
        ) -> Result<
            crate::catalog_application::CatalogRuntimeObservation,
            crate::catalog_application::CatalogApplicationError,
        > {
            unreachable!("create is not part of this fixture")
        }

        fn drop_catalog(
            &self,
            _command: crate::catalog_application::CatalogDropCommand,
        ) -> Result<(), crate::catalog_application::CatalogApplicationError> {
            unreachable!("drop is not part of this fixture")
        }

        fn admit_catalog(
            &self,
            _instance_id: &novarocks_spi::connector::ConnectorInstanceId,
        ) -> crate::catalog_application::CatalogAdmission {
            let attempt = self.admissions.fetch_add(1, Ordering::SeqCst);
            crate::catalog_application::CatalogAdmission::Ready(Self::observation(
                if attempt == 0 { 1 } else { 2 },
            ))
        }
    }

    impl crate::catalog_application::CatalogApplicationPort for UnavailableCatalogApplication {
        fn create_catalog(
            &self,
            _command: crate::catalog_application::CatalogCreateCommand,
        ) -> Result<
            crate::catalog_application::CatalogRuntimeObservation,
            crate::catalog_application::CatalogApplicationError,
        > {
            Err(crate::catalog_application::CatalogApplicationError::new(
                crate::catalog_application::CatalogApplicationErrorKind::Unavailable,
                "projection is stale",
            ))
        }

        fn drop_catalog(
            &self,
            _command: crate::catalog_application::CatalogDropCommand,
        ) -> Result<(), crate::catalog_application::CatalogApplicationError> {
            Err(crate::catalog_application::CatalogApplicationError::new(
                crate::catalog_application::CatalogApplicationErrorKind::Unavailable,
                "projection is stale",
            ))
        }

        fn admit_catalog(
            &self,
            _instance_id: &novarocks_spi::connector::ConnectorInstanceId,
        ) -> crate::catalog_application::CatalogAdmission {
            crate::catalog_application::CatalogAdmission::Unavailable {
                reason: "projection is stale".to_string(),
            }
        }
    }

    #[test]
    fn sqlx2_application_materializer_projects_local_scan_before_publication() {
        let binding =
            project_binding_for_sql(binding_id(101, 1), local_binding(binding_id(101, 1)))
                .expect("local scan must be tokenized before SQL receives it");

        assert!(matches!(
            novarocks_sql::planning::catalog::table_binding_id(&binding.resolved),
            token if token == binding_id(101, 1)
        ));
    }

    #[test]
    fn sqlx2_application_materializer_rejects_foreign_scan_token() {
        let binding = local_binding(binding_id(102, 2));

        let error = match project_binding_for_sql(binding_id(102, 1), binding) {
            Ok(_) => panic!("foreign token must not enter this request"),
            Err(error) => error,
        };
        assert!(error.contains("different request binding"));
    }

    #[test]
    fn catalog_application_admission_fails_closed_before_external_materialization() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().expect("binding store"));
        let application = UnavailableCatalogApplication;
        let materializer = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            bindings,
            Box::new(OverlayLoader),
        )
        .with_catalog_application(Some(&application));

        let error = materializer
            .resolve_table_for_analysis(None, "db", "orders")
            .expect_err("stale projection must not materialize a table");

        assert_eq!(
            error,
            "catalog `ice` is unavailable on this frontend: projection is stale"
        );
    }

    #[test]
    fn catalog_application_rejects_generation_change_while_acquiring_planning_binding() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().expect("binding store"));
        let application = ChangingCatalogApplication {
            admissions: AtomicUsize::new(0),
        };
        let materializer = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            bindings,
            Box::new(OverlayLoader),
        )
        .with_catalog_application(Some(&application));

        let error = materializer
            .resolve_table_for_analysis(None, "db", "orders")
            .expect_err("drop and recreate must not switch the request to a new generation");

        assert_eq!(
            error,
            "catalog attachment generation changed while acquiring its planning lease"
        );
    }

    #[test]
    fn sqlx2_application_materializer_error_memoizes_by_canonical_identity() {
        let bindings = QueryTableBindingStore::try_new().expect("binding store");
        let attempts = AtomicUsize::new(0);
        let key = QueryTableBindingKey::strict_base("ICEBERG", "DB", "TABLE");

        let first = bindings.resolve_or_insert(key.clone(), || {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err("missing table".to_string())
        });
        let second = bindings.resolve_or_insert(key, || {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err("must not load twice".to_string())
        });

        assert_eq!(first.unwrap_err(), "missing table");
        assert_eq!(second.unwrap_err(), "missing table");
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn sqlx2_application_time_travel_overlay_uses_physical_snapshot_key() {
        let overlay = QueryTableBindingKey::analysis_lookup("ice", "db", "__sqlx1_tt_orders_42");
        let physical = QueryTableBindingKey::snapshot("ICE", "DB", "orders", 42);
        assert_eq!(overlay, physical);
    }

    #[test]
    fn sqlx2_application_cow_overlay_is_tokenized_without_local_catalog_registration() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().expect("binding store"));
        let materializer = CatalogServiceMaterializer::new_with_query_local_overlays(
            Some("default_catalog"),
            &service,
            Arc::clone(&bindings),
            Box::new(OverlayLoader),
            vec![QueryLocalTableOverlay::new(
                "db",
                "__nr_cow_orders",
                QueryTableBindingKey::snapshot("ice", "db", "orders", 7),
                |binding| Ok(frozen_overlay_binding(binding)),
            )],
        );

        let resolved = materializer
            .resolve_table_for_analysis(None, "db", "__nr_cow_orders")
            .expect("query-local overlay resolves");
        let binding_id = novarocks_sql::planning::catalog::table_binding_id(&resolved);
        assert!(binding_id.belongs_to(bindings.scope()));
        assert_eq!(
            novarocks_sql::planning::catalog::frozen_input_snapshot_id(&resolved),
            Some(7)
        );
        assert!(
            bindings
                .scan_materialization(binding_id)
                .expect("binding materialization")
                .is_none(),
            "analysis-only overlays do not manufacture a provider read handle"
        );
        assert!(
            service
                .local()
                .read()
                .expect("catalog read")
                .get("db", "__nr_cow_orders")
                .is_err()
        );
    }
}
