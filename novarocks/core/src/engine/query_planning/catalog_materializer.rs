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

use crate::engine::query_planning::bindings::{
    QueryScanMaterialization, QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey,
    QueryTableBindingStore,
};
use crate::sql::binding::SqlTableBindingId;
use crate::sql::catalog::{
    IcebergMetadataTableProvider, PlannerTableProvider, ResolvedAnalyzerTable,
};
use crate::sql::planner::table::TableDef;

/// Provider-neutral table facts admitted for one request.  Core projects the
/// typed SPI metadata into SQL facts, preserves the opaque scan authority, and
/// never decodes a provider table handle or metadata payload.
#[derive(Clone)]
pub(crate) struct ConnectorQueryTableMaterialization {
    pub(crate) schema_version: Option<Vec<u8>>,
    pub(crate) columns: Vec<novarocks_catalog::schema::ColumnDef>,
    pub(crate) row_lineage_metadata_columns: Vec<novarocks_catalog::schema::ColumnDef>,
    pub(crate) read_table: novarocks_spi::connector::ConnectorTableHandle,
    pub(crate) read_schema: arrow::datatypes::SchemaRef,
    pub(crate) read_selector: novarocks_spi::connector::ConnectorReadSelector,
    pub(crate) sql_ukfk_facts: crate::sql::planner::table::SqlUkFkTableFacts,
    pub(crate) statistics_pin: Option<crate::connector::backend::ResolvedTableStatisticsPin>,
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
}

pub(crate) fn load_connector_table_materialization_with_lease(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<ConnectorQueryTableMaterialization, String> {
    load_connector_table_materialization_with_resolution(
        controls,
        context,
        catalog,
        namespace,
        table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )
}

/// Load one provider-defined read alias through the same opaque metadata
/// contract used for base tables. The alias syntax is application-owned, but
/// Core neither decodes the returned table handle nor names a provider type.
pub(crate) fn load_connector_table_alias_materialization_with_lease(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    alias: &str,
) -> Result<ConnectorQueryTableMaterialization, String> {
    load_connector_table_materialization_with_resolution(
        controls,
        context,
        catalog,
        namespace,
        alias,
        novarocks_spi::connector::ConnectorTableResolution::ProviderReadAlias,
    )
}

fn load_connector_table_materialization_with_resolution(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    resolution: novarocks_spi::connector::ConnectorTableResolution,
) -> Result<ConnectorQueryTableMaterialization, String> {
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorTableIdentity, ConnectorTableRequest,
    };

    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let planning_lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    let metadata = planning_lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution,
            context,
        })
        // An absent relation is a SQL name-resolution failure, not a provider
        // incident: render the vocabulary the rest of the engine already
        // recognizes instead of leaking the provider's own wording.
        .map_err(|error| match error.kind() {
            novarocks_spi::connector::ConnectorErrorKind::NotFound => {
                format!("unknown table: {namespace}.{table}")
            }
            _ => error.to_string(),
        })?;
    connector_table_materialization_from_metadata(metadata, planning_lease)
}

pub(crate) fn connector_table_materialization_from_metadata(
    metadata: novarocks_spi::connector::ConnectorTableMetadata,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<ConnectorQueryTableMaterialization, String> {
    use novarocks_spi::connector::{
        ConnectorTableColumnRole, ConnectorTableColumnSemanticKind, ConnectorTableColumnVisibility,
    };

    let mut columns = Vec::new();
    let mut row_lineage_metadata_columns = Vec::new();
    for (ordinal, field) in metadata.schema.fields().iter().enumerate() {
        let fact = metadata.planning_facts.column_facts().get(ordinal);
        let logical_type = match fact.map(|fact| fact.semantic_kind()) {
            Some(ConnectorTableColumnSemanticKind::Bitmap) => {
                Some(novarocks_catalog::schema::SqlType::Bitmap)
            }
            Some(ConnectorTableColumnSemanticKind::Hll) => {
                Some(novarocks_catalog::schema::SqlType::Hll)
            }
            _ => None,
        };
        let column = novarocks_catalog::schema::ColumnDef {
            name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: crate::connector::connector_write_default_at(
                &metadata.planning_facts,
                ordinal,
            ),
            logical_type,
        };
        match fact.map(|fact| fact.role()) {
            Some(ConnectorTableColumnRole::RowLineageSystem) => {
                row_lineage_metadata_columns.push(column)
            }
            _ if matches!(
                fact.map(|fact| fact.visibility()),
                Some(ConnectorTableColumnVisibility::Hidden)
            ) => {}
            _ => columns.push(column),
        }
    }
    let statistics_pin = metadata
        .statistics_data_version
        .clone()
        .map(
            |data_version| crate::connector::backend::ResolvedTableStatisticsPin {
                table: metadata.table.clone(),
                data_version,
            },
        );
    Ok(ConnectorQueryTableMaterialization {
        schema_version: metadata.version.map(|version| version.to_vec()),
        columns,
        row_lineage_metadata_columns,
        read_table: metadata.table,
        read_schema: metadata.schema.clone(),
        read_selector: novarocks_spi::connector::ConnectorReadSelector::Current,
        sql_ukfk_facts:
            crate::sql::planner::table::SqlUkFkTableFacts::from_connector_planning_facts(
                &metadata.schema,
                &metadata.planning_facts,
            ),
        statistics_pin,
        planning_lease,
    })
}

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
    use crate::sql::planner::table::{ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity};

    let (scan_kind, frozen_snapshot_id) = match materialization.read_selector {
        novarocks_spi::connector::ConnectorReadSelector::Current => (
            SqlScanKind::Data {
                version: crate::sql::planner::table::SqlTableVersionSelector::Current,
            },
            None,
        ),
        novarocks_spi::connector::ConnectorReadSelector::SnapshotId(snapshot_id) => {
            let version =
                crate::sql::planner::table::SqlTableVersionSelector::Snapshot(snapshot_id);
            (SqlScanKind::FrozenInputSet { version }, Some(snapshot_id))
        }
        novarocks_spi::connector::ConnectorReadSelector::TimestampMicros(timestamp_micros) => {
            return Err(format!(
                "connector read selector timestamp {timestamp_micros} must resolve to a snapshot before SQL materialization"
            ));
        }
    };
    let planner = TableDef {
        name: sql_table_name.to_string(),
        columns: materialization.columns,
        iceberg_row_lineage_metadata_columns: materialization.row_lineage_metadata_columns,
        source: ScanSource::Sql(
            SqlScanSource::new(
                binding,
                SqlTableIdentity {
                    catalog: catalog.to_string(),
                    namespace: namespace.to_string(),
                    table: sql_table_name.to_string(),
                },
                scan_kind,
            )
            .with_ukfk_facts(materialization.sql_ukfk_facts),
        ),
    };
    let frozen_snapshot_materializations = frozen_snapshot_id
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
        resolved: ResolvedAnalyzerTable::from_planner(Some(catalog), namespace, planner),
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
pub(crate) fn admit_connector_change_window(
    table: &novarocks_spi::connector::ConnectorTableHandle,
    schema: &arrow::datatypes::SchemaRef,
    planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    window: novarocks_spi::connector::ConnectorChangeWindow,
) -> Result<novarocks_spi::connector::ConnectorScan, String> {
    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorReadPurpose,
        ConnectorScanSelection,
    };

    let binding = planning_lease.binding();
    if table.owner() != &binding.descriptor().instance_id {
        return Err(
            "connector change-window table handle owner does not match its exact planning lease"
                .to_string(),
        );
    }
    let scan = binding
        .planning()
        .begin_scan(
            table,
            ConnectorBeginScanRequest {
                projection: (0..schema.fields().len()).collect(),
                static_predicates: Vec::new(),
                selection: ConnectorScanSelection::ChangeWindow(window),
                purpose: ConnectorReadPurpose::Query,
                limit: None,
                batch: ConnectorBatchBudget {
                    max_rows: std::num::NonZeroUsize::new(4096).expect("batch rows are nonzero"),
                    max_bytes: std::num::NonZeroUsize::new(
                        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                    )
                    .expect("batch bytes are nonzero"),
                },
                context: context.clone(),
            },
        )
        .map_err(|error| error.to_string())?;
    scan.validate(
        &novarocks_spi::connector::ConnectorExecutionBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        },
        ConnectorScanSelection::ChangeWindow(window),
    )
    .map_err(|error| error.to_string())?;
    if scan.output_schema().fields() != schema.fields() {
        return Err(
            "connector change-window scan schema does not match its exact table metadata"
                .to_string(),
        );
    }
    if !scan.predicate_dispositions().is_empty() {
        return Err(
            "connector change-window scan returned dispositions without static predicates"
                .to_string(),
        );
    }
    Ok(scan)
}

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
        metadata_table_type: crate::sql::planner::table::SqlMetadataTableKind,
        binding: SqlTableBindingId,
    ) -> Result<QueryTableBinding, String>;
}

/// Application-owned catalog facade.  Its binding store is request-local and
/// retained by the caller as post-compile context; the SQL catalog trait does
/// not expose it.
pub(crate) struct CatalogServiceMaterializer<'a> {
    current_catalog: Option<&'a str>,
    service: &'a crate::engine::query_planning::catalog_runtime::QueryCatalogService,
    bindings: Arc<QueryTableBindingStore>,
    loader: Box<dyn QueryTableBindingLoader + 'a>,
    /// Request-scoped synthetic relations used by application rewrite flows.
    /// They are intentionally kept next to the binding store instead of the
    /// shared memory catalog: SQL can only observe their projected tokenized
    /// scan after this materializer has admitted the exact connector lease.
    query_local_overlays: HashMap<(String, String), QueryLocalTableOverlay>,
}

/// One application-owned relation overlay for a generated query.
///
/// The overlay is a binding factory, not a `TableDef`: generated COW and MV
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
        service: &'a crate::engine::query_planning::catalog_runtime::QueryCatalogService,
        bindings: Arc<QueryTableBindingStore>,
        loader: Box<dyn QueryTableBindingLoader + 'a>,
    ) -> Self {
        Self::new_with_query_local_overlays(current_catalog, service, bindings, loader, Vec::new())
    }

    pub(crate) fn new_with_query_local_overlays(
        current_catalog: Option<&'a str>,
        service: &'a crate::engine::query_planning::catalog_runtime::QueryCatalogService,
        bindings: Arc<QueryTableBindingStore>,
        loader: Box<dyn QueryTableBindingLoader + 'a>,
        overlays: Vec<QueryLocalTableOverlay>,
    ) -> Self {
        Self {
            current_catalog,
            service,
            bindings,
            loader,
            query_local_overlays: overlays
                .into_iter()
                .map(|overlay| (overlay.key(), overlay))
                .collect(),
        }
    }

    pub(crate) fn query_table_bindings(&self) -> Arc<QueryTableBindingStore> {
        Arc::clone(&self.bindings)
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
                let planner = self
                    .service
                    .local()
                    .read()
                    .expect("catalog service local read lock")
                    .get(database, table)?;
                let key = QueryTableBindingKey::analysis_lookup("default_catalog", database, table);
                let token = self.bind_for_sql(key, |binding| {
                    Ok(QueryTableBinding::local(
                        ResolvedAnalyzerTable::from_planner(
                            Some("default_catalog"),
                            database,
                            planner,
                        ),
                        binding,
                    ))
                })?;
                Ok(self.bindings.binding(token)?.resolved.clone())
            }
            Some(catalog) => {
                let key = QueryTableBindingKey::analysis_lookup(catalog, database, table);
                let token = self.bind_for_sql(key, |binding_id| {
                    self.loader
                        .load_strict_base_table(catalog, database, table, binding_id)
                })?;
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
        metadata_table_type: crate::sql::planner::table::SqlMetadataTableKind,
    ) -> Result<TableDef, String> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => self
                .service
                .local()
                .read()
                .expect("catalog service local read lock")
                .get(database, table),
            Some(catalog) => {
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
                Ok(self.bindings.binding(token)?.resolved.planner.clone())
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
            .map(|resolved| resolved.catalog)
    }

    fn get_table_in_catalog(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<CatalogTable, String> {
        self.resolve_table_for_analysis_once(catalog, database, table)
            .map(|resolved| resolved.catalog)
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

impl IcebergMetadataTableProvider for CatalogServiceMaterializer<'_> {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: crate::sql::planner::table::SqlMetadataTableKind,
    ) -> Result<TableDef, String> {
        self.metadata_table_def(catalog, database, table, metadata_table_type)
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::sql::catalog::PlannerTableProvider;
    use crate::sql::planner::table::ScanSource;

    fn binding_id(scope: u64, ordinal: u32) -> SqlTableBindingId {
        SqlTableBindingId::new(
            crate::sql::binding::SqlTableBindingScopeId::new(
                NonZeroU64::new(scope).expect("non-zero scope"),
            ),
            NonZeroU32::new(ordinal).expect("non-zero ordinal"),
        )
    }

    fn local_binding(binding: SqlTableBindingId) -> QueryTableBinding {
        QueryTableBinding::local(
            ResolvedAnalyzerTable::from_planner(
                Some("default_catalog"),
                "db",
                TableDef {
                    name: "orders".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::sql::planner::table::test_sql_scan_source(
                        crate::sql::planner::table::SqlScanKind::ConnectorRead,
                    ),
                },
            ),
            binding,
        )
    }

    fn frozen_overlay_binding(binding: SqlTableBindingId) -> QueryTableBinding {
        let planner = TableDef {
            name: "__nr_cow_orders".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::Sql(crate::sql::planner::table::SqlScanSource::new(
                binding,
                crate::sql::planner::table::SqlTableIdentity {
                    catalog: "ice".to_string(),
                    namespace: "db".to_string(),
                    table: "orders".to_string(),
                },
                crate::sql::planner::table::SqlScanKind::FrozenInputSet {
                    version: crate::sql::planner::table::SqlTableVersionSelector::Snapshot(7),
                },
            )),
        };
        QueryTableBinding {
            resolved: ResolvedAnalyzerTable::from_planner(Some("ice"), "db", planner),
            statistics_pin: None,
            admission: QueryTableBindingAdmission::Local,
            scan_materialization: None,
            write_target_admission: None,
            mv_target_read: None,
            frozen_snapshot_materializations: BTreeMap::new(),
            admitted_change_scans: BTreeMap::new(),
        }
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
            _metadata_table_type: crate::sql::planner::table::SqlMetadataTableKind,
            _binding: SqlTableBindingId,
        ) -> Result<QueryTableBinding, String> {
            Err("metadata is not part of this overlay fixture".to_string())
        }
    }

    #[test]
    fn sqlx2_application_materializer_projects_local_scan_before_publication() {
        let binding =
            project_binding_for_sql(binding_id(101, 1), local_binding(binding_id(101, 1)))
                .expect("local scan must be tokenized before SQL receives it");

        assert!(matches!(
            binding.resolved.planner.source,
            crate::sql::planner::table::ScanSource::Sql(ref source)
                if source.binding == binding_id(101, 1)
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
        let service = crate::engine::query_planning::catalog_runtime::new_query_catalog_service();
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
        let ScanSource::Sql(source) = resolved.planner.source else {
            panic!("SQL must not receive the overlay's legacy scan source");
        };
        assert!(source.binding.belongs_to(bindings.scope()));
        assert!(matches!(
            source.kind,
            crate::sql::planner::table::SqlScanKind::FrozenInputSet { .. }
        ));
        assert!(
            bindings
                .scan_materialization(source.binding)
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
