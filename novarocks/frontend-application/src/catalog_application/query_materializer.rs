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
use std::sync::{Arc, Mutex};

use novarocks_spi::connector::ConnectorControlResolver;

use crate::catalog_application::query_bindings::{
    QueryScanMaterialization, QueryTableBinding, QueryTableBindingAdmission, QueryTableBindingKey,
    QueryTableBindingStore,
};
use crate::catalog_application::query_catalog::{
    CatalogResolutionError, CatalogResolutionResult, ConnectorQueryTableMaterialization,
    QueryCatalogService, load_connector_table_alias_materialization_with_lease_typed,
    load_connector_table_materialization_with_lease_typed,
};
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::catalog::{
    IcebergMetadataTableProvider, PlannerTableProvider, ResolvedAnalyzerTable, TableLookupMode,
};

/// Project a provider-neutral SPI metadata materialization into the
/// request-local SQL binding. Provider aliases retain their separately frozen
/// provider-owned facts until their dedicated adapters run.
pub fn connector_query_binding_from_materialization(
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
                    selector: novarocks_spi::connector::ConnectorReadSelector::SnapshotId(
                        snapshot_id,
                    ),
                    mv_partition_selection: None,
                    planning_lease: materialization.planning_lease.clone(),
                },
            )
        })
        .collect();
    Ok(QueryTableBinding {
        resolved: sql_materialization.into_resolved_table(),
        statistics_pin: materialization.statistics_pin.clone(),
        admission: QueryTableBindingAdmission::Exact(materialization.planning_lease.clone()),
        source_metadata: Some(materialization.source_metadata),
        scan_materialization: Some(QueryScanMaterialization {
            table: materialization.read_table,
            selector: materialization.read_selector,
            mv_partition_selection: None,
            planning_lease: materialization.planning_lease,
        }),
        mv_target_read: None,
        write_target_admission: None,
        frozen_snapshot_materializations,
        frozen_cohort_read: None,
        prepared_source_reads: BTreeMap::new(),
    })
}

/// Admit one provider-owned change window while the caller holds the exact
/// table handle and planning lease. The returned sealed scan is the sole
/// physical authority retained by Core for later preparation.
/// Application materializer for connector-controlled table metadata.  The
/// interface is intentionally application-owned because it returns an exact
/// lease alongside planner facts.  It is not part of SQL's vocabulary.
pub trait QueryTableBindingLoader: Send + Sync {
    fn load_strict_base_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        binding: SqlTableBindingId,
    ) -> CatalogResolutionResult<QueryTableBinding>;

    fn load_metadata_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
        binding: SqlTableBindingId,
    ) -> CatalogResolutionResult<QueryTableBinding>;

    fn prepare_strict_base_table<'a>(
        &'a self,
        catalog: &'a str,
        namespace: &'a str,
        table: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = CatalogResolutionResult<PreparedQueryTableBinding>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let _ = (catalog, namespace, table);
            Err(CatalogResolutionError::failed(
                "asynchronous table binding loader is not injected",
            ))
        })
    }

    fn prepare_metadata_table<'a>(
        &'a self,
        catalog: &'a str,
        namespace: &'a str,
        table: &'a str,
        kind: novarocks_sql::planning::catalog::MetadataTableKind,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = CatalogResolutionResult<PreparedQueryTableBinding>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let _ = (catalog, namespace, table, kind);
            Err(CatalogResolutionError::failed(
                "asynchronous metadata binding loader is not injected",
            ))
        })
    }
}

/// Completed provider observation consumed by the original SQL binding author.
/// It carries the same planning lease and opaque handle, never a new lookup.
pub struct PreparedQueryTableBinding {
    materialization: ConnectorQueryTableMaterialization,
    catalog: String,
    namespace: String,
    table: String,
    metadata_kind: Option<novarocks_sql::planning::catalog::MetadataTableKind>,
}

impl PreparedQueryTableBinding {
    fn create(self, binding_id: SqlTableBindingId) -> CatalogResolutionResult<QueryTableBinding> {
        let Self {
            materialization,
            catalog,
            namespace,
            table,
            metadata_kind,
        } = self;
        match metadata_kind {
            None => connector_query_binding_from_materialization(
                materialization,
                &catalog,
                &namespace,
                &table,
                binding_id,
            )
            .map_err(CatalogResolutionError::failed),
            Some(kind) => metadata_query_binding_from_materialization(
                materialization,
                &catalog,
                &namespace,
                &table,
                kind,
                binding_id,
            ),
        }
    }
}

/// Application-owned catalog facade.  Its binding store is request-local and
/// retained by the caller as post-compile context; the SQL catalog trait does
/// not expose it.
pub struct CatalogServiceMaterializer<'a> {
    current_catalog: Option<&'a str>,
    service: &'a crate::catalog_application::query_catalog::QueryCatalogService,
    bindings: Arc<QueryTableBindingStore>,
    loader: Box<dyn QueryTableBindingLoader + 'a>,
    /// Typed classification paired with binding-store failure memoization.
    /// The binding store owns the canonical at-most-once load; this sidecar
    /// retains only whether its string-compatible failure was absence or a
    /// hard catalog failure.
    resolution_errors: Mutex<HashMap<QueryTableBindingKey, CatalogResolutionError>>,
    // Original capture uses the same binding cache. This sidecar preserves
    // the producer cause paired with its memoized failure, without reloading.
    original_resolution_errors:
        Mutex<HashMap<QueryTableBindingKey, crate::mv::domain::application::MvApplicationError>>,
    /// Frontend-owned attachment admission. The loader still owns the exact
    /// connector lease; this gate preserves Absent versus Unavailable before
    /// Core can materialize an external table.
    catalog_application: Option<&'a dyn novarocks_catalog_application::CatalogApplicationPort>,
    /// Request-scoped synthetic relations used by application rewrite flows.
    /// They are intentionally kept next to the binding store instead of the
    /// shared memory catalog: SQL can only observe their projected tokenized
    /// scan after this materializer has admitted the exact connector lease.
    query_local_overlays: HashMap<(String, String, String), QueryLocalTableOverlay>,
}

/// One application-owned relation overlay for a generated query.
///
/// The overlay is a binding factory, not a planner table definition: generated COW and MV
/// reads must supply their frozen provider facts to the request-local store
/// before SQL sees the resulting tokenized table.  Keeping the factory here
/// prevents a synthetic relation from leaking into the shared catalog.
#[derive(Clone)]
pub struct QueryLocalTableOverlay {
    catalog: String,
    namespace: String,
    table: String,
    key: QueryTableBindingKey,
    materialize:
        Arc<dyn Fn(SqlTableBindingId) -> Result<QueryTableBinding, String> + Send + Sync + 'static>,
}

impl QueryLocalTableOverlay {
    pub fn new(
        namespace: impl Into<String>,
        table: impl Into<String>,
        key: QueryTableBindingKey,
        materialize: impl Fn(SqlTableBindingId) -> Result<QueryTableBinding, String>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self::for_catalog("default_catalog", namespace, table, key, materialize)
    }

    /// A frozen definition relation retains its canonical catalog identity.
    /// Generated local relations use `new` instead; the two lookup domains do
    /// not alias, even when their namespace and table names are identical.
    pub(crate) fn for_catalog(
        catalog: impl Into<String>,
        namespace: impl Into<String>,
        table: impl Into<String>,
        key: QueryTableBindingKey,
        materialize: impl Fn(SqlTableBindingId) -> Result<QueryTableBinding, String>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            catalog: catalog.into(),
            namespace: namespace.into(),
            table: table.into(),
            key,
            materialize: Arc::new(materialize),
        }
    }

    fn key(&self) -> (String, String, String) {
        (
            self.catalog.to_ascii_lowercase(),
            self.namespace.to_ascii_lowercase(),
            self.table.to_ascii_lowercase(),
        )
    }
}

impl<'a> CatalogServiceMaterializer<'a> {
    #[cfg(test)]
    pub fn new(
        current_catalog: Option<&'a str>,
        service: &'a crate::catalog_application::query_catalog::QueryCatalogService,
        bindings: Arc<QueryTableBindingStore>,
        loader: Box<dyn QueryTableBindingLoader + 'a>,
    ) -> Self {
        Self::new_with_query_local_overlays(current_catalog, service, bindings, loader, Vec::new())
    }

    pub fn new_with_query_local_overlays(
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
            resolution_errors: Mutex::new(HashMap::new()),
            original_resolution_errors: Mutex::new(HashMap::new()),
            catalog_application: None,
            query_local_overlays: overlays
                .into_iter()
                .map(|overlay| (overlay.key(), overlay))
                .collect(),
        }
    }

    pub fn with_catalog_application(
        mut self,
        catalog_application: Option<&'a dyn novarocks_catalog_application::CatalogApplicationPort>,
    ) -> Self {
        self.catalog_application = catalog_application;
        self
    }

    fn require_catalog_admission(
        &self,
        catalog: &str,
    ) -> CatalogResolutionResult<Option<novarocks_catalog_application::CatalogRuntimeObservation>>
    {
        let Some(application) = self.catalog_application else {
            return Ok(None);
        };
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse(catalog).map_err(|error| {
                CatalogResolutionError::failed(format!(
                    "invalid catalog instance `{catalog}`: {error}"
                ))
            })?;
        application
            .admit_catalog(&instance_id)
            .require_ready(&instance_id)
            .map(Some)
            .map_err(|error| CatalogResolutionError::failed(error.to_string()))
    }

    fn verify_catalog_admission(
        &self,
        catalog: &str,
        expected: Option<&novarocks_catalog_application::CatalogRuntimeObservation>,
    ) -> CatalogResolutionResult<()> {
        let Some(expected) = expected else {
            return Ok(());
        };
        let current = self.require_catalog_admission(catalog)?.ok_or_else(|| {
            CatalogResolutionError::failed("catalog admission unexpectedly became legacy")
        })?;
        if &current != expected {
            return Err(CatalogResolutionError::failed(
                "catalog attachment generation changed while acquiring its planning lease",
            ));
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
        load: impl FnOnce(SqlTableBindingId) -> CatalogResolutionResult<QueryTableBinding>,
    ) -> CatalogResolutionResult<SqlTableBindingId> {
        if let Some(error) = self
            .resolution_errors
            .lock()
            .expect("catalog resolution error lock")
            .get(&key)
            .cloned()
        {
            return Err(error);
        }
        let error_key = key.clone();
        let mut typed_error = None;
        let result = self.bindings.resolve_or_insert_with_id(key, |binding_id| {
            let binding = load(binding_id).map_err(|error| {
                let message = error.message().to_string();
                self.resolution_errors
                    .lock()
                    .expect("catalog resolution error lock")
                    .insert(error_key.clone(), error.clone());
                typed_error = Some(error);
                message
            })?;
            project_binding_for_sql(binding_id, binding)
        });
        result.map_err(|message| {
            let error = typed_error
                .or_else(|| {
                    self.resolution_errors
                        .lock()
                        .expect("catalog resolution error lock")
                        .get(&error_key)
                        .cloned()
                })
                .unwrap_or_else(|| CatalogResolutionError::failed(message));
            self.resolution_errors
                .lock()
                .expect("catalog resolution error lock")
                .insert(error_key, error.clone());
            error
        })
    }

    fn effective_catalog<'b>(&'b self, override_catalog: Option<&'b str>) -> Option<&'b str> {
        override_catalog.or(self.current_catalog)
    }

    fn query_overlay(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Option<QueryLocalTableOverlay> {
        self.query_local_overlays
            .get(&(
                self.effective_catalog(catalog)
                    .unwrap_or("default_catalog")
                    .to_ascii_lowercase(),
                database.to_ascii_lowercase(),
                table.to_ascii_lowercase(),
            ))
            .cloned()
    }

    fn resolve_table_for_analysis_once(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => {
                if let Some(overlay) = self.query_overlay(catalog, database, table) {
                    return self.resolve_query_local_overlay(overlay, database, table);
                }
                let local = self
                    .service
                    .local()
                    .read()
                    .expect("catalog service local read lock");
                let resolved = resolve_local_catalog_table_typed(&local, database, table)?;
                let key = QueryTableBindingKey::analysis_lookup("default_catalog", database, table);
                let token = self.bind_for_sql(key, |binding| {
                    Ok(QueryTableBinding::local(resolved, binding))
                })?;
                Ok(self
                    .bindings
                    .binding(token)
                    .map_err(CatalogResolutionError::failed)?
                    .resolved
                    .clone())
            }
            Some(catalog) => {
                let observation = self.require_catalog_admission(catalog)?;
                if let Some(overlay) = self.query_overlay(Some(catalog), database, table) {
                    let resolved = self.resolve_query_local_overlay(overlay, database, table)?;
                    self.verify_catalog_admission(catalog, observation.as_ref())?;
                    return Ok(resolved);
                }
                let key = QueryTableBindingKey::analysis_lookup(catalog, database, table);
                let token = self.bind_for_sql(key, |binding_id| {
                    self.loader
                        .load_strict_base_table(catalog, database, table, binding_id)
                })?;
                self.verify_catalog_admission(catalog, observation.as_ref())?;
                Ok(self
                    .bindings
                    .binding(token)
                    .map_err(CatalogResolutionError::failed)?
                    .resolved
                    .clone())
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
        database: &str,
        table: &str,
    ) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
        let catalog = overlay.catalog.clone();
        let token = self.bind_for_sql(overlay.key, |binding_id| {
            (overlay.materialize)(binding_id).map_err(CatalogResolutionError::failed)
        })?;
        let resolved = self
            .bindings
            .binding(token)
            .map_err(CatalogResolutionError::failed)?
            .resolved
            .clone();
        if catalog != "default_catalog" {
            let identity =
                novarocks_sql::planning::catalog::materialization_identity_facts(&resolved);
            if !identity.catalog().eq_ignore_ascii_case(&catalog)
                || !identity.namespace().eq_ignore_ascii_case(database)
                || !identity.table().eq_ignore_ascii_case(table)
            {
                return Err(CatalogResolutionError::failed(format!(
                    "frozen relation `{}` does not match `{catalog}.{database}.{table}`",
                    identity.fqn()
                )));
            }
            return Ok(resolved);
        }
        // Only the SQL lookup is local. Its token still names the original
        // Provider binding, including the exact frozen snapshot and lease.
        novarocks_sql::planning::catalog::project_query_local_overlay_name(
            resolved, database, table,
        )
        .map_err(CatalogResolutionError::failed)
    }

    fn metadata_table_def(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
    ) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
        if metadata_table_type == novarocks_sql::planning::catalog::MetadataTableKind::Partitions {
            return Err(CatalogResolutionError::failed(
                "invariant: $partitions is a SQL view over $files",
            ));
        }
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => {
                let local = self
                    .service
                    .local()
                    .read()
                    .expect("catalog service local read lock");
                resolve_local_catalog_table_typed(&local, database, table)
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
                Ok(self
                    .bindings
                    .binding(token)
                    .map_err(CatalogResolutionError::failed)?
                    .resolved
                    .clone())
            }
        }
    }

    /// Resolve an ordinary relation without erasing whether it is absent or
    /// whether its catalog generation failed. SQL completion maps only
    /// `Missing` into a `CatalogRelationFact::missing` response.
    pub fn resolve_table_for_analysis_typed(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
        self.resolve_table_for_analysis_once(catalog, database, table)
    }

    /// Resolve an Iceberg metadata relation with the same typed absence
    /// contract as ordinary catalog lookup.
    pub fn resolve_metadata_table_typed(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
    ) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
        self.metadata_table_def(catalog, database, table, metadata_table_type)
    }

    pub(crate) async fn resolve_table_for_analysis_async(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
        if self.query_overlay(catalog, database, table).is_some() {
            return self.resolve_table_for_analysis_once(catalog, database, table);
        }
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => {
                self.resolve_table_for_analysis_once(catalog, database, table)
            }
            Some(catalog) => {
                let observation = self.require_catalog_admission(catalog)?;
                let key = QueryTableBindingKey::analysis_lookup(catalog, database, table);
                let token = if self.bindings.resolved_lookup(&key).is_some() {
                    self.bind_for_sql(key, |_| unreachable!("completed binding must not reload"))?
                } else {
                    let observed = self
                        .loader
                        .prepare_strict_base_table(catalog, database, table)
                        .await;
                    self.bind_for_sql(key, |binding| {
                        observed.and_then(|facts| facts.create(binding))
                    })?
                };
                self.verify_catalog_admission(catalog, observation.as_ref())?;
                Ok(self
                    .bindings
                    .binding(token)
                    .map_err(CatalogResolutionError::failed)?
                    .resolved
                    .clone())
            }
        }
    }

    pub(crate) async fn resolve_metadata_table_async(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        kind: novarocks_sql::planning::catalog::MetadataTableKind,
    ) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
        if kind == novarocks_sql::planning::catalog::MetadataTableKind::Partitions {
            return Err(CatalogResolutionError::failed(
                "invariant: $partitions is a SQL view over $files",
            ));
        }
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => {
                self.metadata_table_def(catalog, database, table, kind)
            }
            Some(catalog) => {
                let observation = self.require_catalog_admission(catalog)?;
                let key = QueryTableBindingKey::metadata(catalog, database, table, kind);
                let token = if self.bindings.resolved_lookup(&key).is_some() {
                    self.bind_for_sql(key, |_| unreachable!("completed binding must not reload"))?
                } else {
                    let observed = self
                        .loader
                        .prepare_metadata_table(catalog, database, table, kind)
                        .await;
                    self.bind_for_sql(key, |binding| {
                        observed.and_then(|facts| facts.create(binding))
                    })?
                };
                self.verify_catalog_admission(catalog, observation.as_ref())?;
                Ok(self
                    .bindings
                    .binding(token)
                    .map_err(CatalogResolutionError::failed)?
                    .resolved
                    .clone())
            }
        }
    }
}

/// A CPU-created lookup suspension. No Provider future is polled while this
/// value is built or completed; its exact lease is used by the named I/O gate.
pub(crate) enum OriginalCatalogLookup {
    Fact(novarocks_sql::compiler::CatalogRelationFact),
    Metadata(Box<OriginalCatalogMetadataLookup>),
}
pub(crate) struct OriginalCatalogMetadataLookup {
    need: novarocks_sql::compiler::CatalogRelationNeed,
    key: QueryTableBindingKey,
    observation: Option<novarocks_catalog_application::CatalogRuntimeObservation>,
    pub(crate) lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    pub(crate) namespace: String,
    pub(crate) physical_table: String,
    pub(crate) resolution: novarocks_spi::connector::ConnectorTableResolution,
    snapshot: Option<i64>,
    metadata_kind: Option<novarocks_sql::planning::catalog::MetadataTableKind>,
}
impl CatalogServiceMaterializer<'_> {
    fn original_catalog_admission(
        &self,
        catalog: &str,
    ) -> Result<
        Option<novarocks_catalog_application::CatalogRuntimeObservation>,
        crate::mv::domain::application::MvApplicationError,
    > {
        use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
        let Some(application) = self.catalog_application else {
            return Ok(None);
        };
        let id =
            novarocks_spi::connector::ConnectorInstanceId::parse(catalog).map_err(|error| {
                MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, error.to_string())
            })?;
        application
            .admit_catalog(&id)
            .require_ready(&id)
            .map(Some)
            .map_err(|error| {
                let cause = error.source_failure().cloned().unwrap_or_else(|| {
                    novarocks_failure_contract::SharedFailureCause::new(error.clone())
                });
                MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string())
                    .with_original_failure(cause)
            })
    }

    /// Original SQL need, original binding store, exact catalog admission.
    /// Call only in the pure CPU turn; the returned Metadata value is a named
    /// request rather than a callback that may enter a live catalog.
    pub(crate) fn begin_original_catalog_lookup(
        &self,
        need: &novarocks_sql::compiler::CatalogRelationNeed,
        controls: &dyn ConnectorControlResolver,
    ) -> Result<OriginalCatalogLookup, crate::mv::domain::application::MvApplicationError> {
        use crate::query_execution::mv_assembly::refresh_preparation::io_gate::{
            contract_error, provider_error,
        };
        use novarocks_sql::compiler::CatalogLookupTarget;
        let relation = need.relation();
        let catalog = relation.catalog.as_str();
        let metadata_kind = match need.target() {
            CatalogLookupTarget::Table { .. } => None,
            CatalogLookupTarget::IcebergMetadata { kind } => Some(kind),
        };
        if catalog == "default_catalog" {
            let resolved = match metadata_kind {
                None => self.resolve_table_for_analysis_once(
                    Some(catalog),
                    &relation.namespace,
                    &relation.table,
                ),
                Some(kind) => self.metadata_table_def(
                    Some(catalog),
                    &relation.namespace,
                    &relation.table,
                    kind,
                ),
            };
            return original_catalog_fact(need, resolved).map(OriginalCatalogLookup::Fact);
        }
        let observation = self.original_catalog_admission(catalog)?;
        if metadata_kind.is_none()
            && let Some(overlay) =
                self.query_overlay(Some(catalog), &relation.namespace, &relation.table)
        {
            let resolved =
                self.resolve_query_local_overlay(overlay, &relation.namespace, &relation.table);
            if self.original_catalog_admission(catalog)? != observation {
                return Err(contract_error(
                    "catalog attachment generation changed while acquiring its planning lease",
                ));
            }
            return original_catalog_fact(need, resolved).map(OriginalCatalogLookup::Fact);
        }
        let key = match metadata_kind {
            None => {
                QueryTableBindingKey::analysis_lookup(catalog, &relation.namespace, &relation.table)
            }
            Some(kind) => {
                QueryTableBindingKey::metadata(catalog, &relation.namespace, &relation.table, kind)
            }
        };
        if let Some(error) = self
            .original_resolution_errors
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
        {
            return Err(error);
        }
        if self.bindings.resolved_lookup(&key).is_some() {
            let token = self.bind_for_sql(key, |_| {
                unreachable!("memoized original lookup must not reload")
            });
            let resolved = token.and_then(|token| {
                self.bindings
                    .binding(token)
                    .map(|binding| binding.resolved.clone())
                    .map_err(CatalogResolutionError::failed)
            });
            if self.original_catalog_admission(catalog)? != observation {
                return Err(contract_error(
                    "catalog attachment generation changed while acquiring its planning lease",
                ));
            }
            return original_catalog_fact(need, resolved).map(OriginalCatalogLookup::Fact);
        }
        let lease =
            crate::connector::metadata_binding_typed(controls, catalog).map_err(provider_error)?;
        let (physical_table, resolution, snapshot) = match metadata_kind {
            None => {
                let (table, snapshot) =
                    crate::catalog_application::query_bindings::parse_time_travel_overlay_identity(
                        &relation.table,
                    )
                    .map(|(table, snapshot)| (table, Some(snapshot)))
                    .unwrap_or((&relation.table, None));
                (
                    table.to_string(),
                    novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                    snapshot,
                )
            }
            Some(kind) => (
                format!(
                    "{}${}",
                    relation.table,
                    metadata_table_alias_suffix(kind)
                        .map_err(|e| contract_error(e.into_message()))?
                ),
                novarocks_spi::connector::ConnectorTableResolution::ProviderReadAlias,
                None,
            ),
        };
        Ok(OriginalCatalogLookup::Metadata(Box::new(
            OriginalCatalogMetadataLookup {
                need: need.clone(),
                key,
                observation,
                lease,
                namespace: relation.namespace.clone(),
                physical_table,
                resolution,
                snapshot,
                metadata_kind,
            },
        )))
    }

    /// Consume that exact observation on the CPU. The original projection and
    /// memoization authors install the binding once; Provider failures retain
    /// their real cause alongside the same memoized failure.
    pub(crate) fn finish_original_catalog_lookup(
        &self,
        lookup: Box<OriginalCatalogMetadataLookup>,
        observed: Result<
            novarocks_spi::connector::ConnectorTableMetadata,
            novarocks_spi::connector::ConnectorError,
        >,
    ) -> Result<
        novarocks_sql::compiler::CatalogRelationFact,
        crate::mv::domain::application::MvApplicationError,
    > {
        use crate::query_execution::mv_assembly::refresh_preparation::io_gate::contract_error;
        let lookup = *lookup;
        let relation = lookup.need.relation();
        let materialization = match observed {
            Ok(metadata) => crate::catalog_application::query_catalog::connector_table_materialization_from_metadata(
                metadata, lookup.lease,
            ).map_err(CatalogResolutionError::failed),
            Err(error) if error.kind() == novarocks_spi::connector::ConnectorErrorKind::NotFound => {
                let missing = CatalogResolutionError::missing(format!("unknown table: {}.{}", relation.namespace, relation.table));
                let cached = self.bind_for_sql(lookup.key, |_| Err(missing)).and_then(|_| unreachable!());
                return original_catalog_fact(&lookup.need, cached);
            }
            Err(error) => {
                let error = crate::query_execution::mv_assembly::refresh_preparation::io_gate::provider_error(error);
                // Keep the original typed Provider failure. The reader stops;
                // a repeated need observes this same cause rather than I/O.
                let retired = self.original_resolution_errors.lock().unwrap()
                    .insert(lookup.key.clone(), error.clone());
                drop(retired);
                let _ = self.bind_for_sql(lookup.key, |_| Err(CatalogResolutionError::failed(error.message())));
                return Err(error);
            }
        };
        let token = self
            .bind_for_sql(lookup.key, |binding| {
                let mut materialization = materialization?;
                if let Some(snapshot) = lookup.snapshot {
                    materialization.read_selector =
                        novarocks_spi::connector::ConnectorReadSelector::SnapshotId(snapshot);
                }
                PreparedQueryTableBinding {
                    materialization,
                    catalog: relation.catalog.clone(),
                    namespace: relation.namespace.clone(),
                    table: relation.table.clone(),
                    metadata_kind: lookup.metadata_kind,
                }
                .create(binding)
            })
            .map_err(|error| contract_error(error.into_message()))?;
        if self.original_catalog_admission(&relation.catalog)? != lookup.observation {
            return Err(contract_error(
                "catalog attachment generation changed while acquiring its planning lease",
            ));
        }
        let resolved = self
            .bindings
            .binding(token)
            .map_err(contract_error)?
            .resolved
            .clone();
        novarocks_sql::compiler::CatalogRelationFact::resolved(&lookup.need, resolved)
            .map_err(|error| contract_error(error.to_string()))
    }
}

fn original_catalog_fact(
    need: &novarocks_sql::compiler::CatalogRelationNeed,
    resolved: CatalogResolutionResult<ResolvedAnalyzerTable>,
) -> Result<
    novarocks_sql::compiler::CatalogRelationFact,
    crate::mv::domain::application::MvApplicationError,
> {
    use crate::query_execution::mv_assembly::refresh_preparation::io_gate::contract_error;
    use novarocks_sql::compiler::CatalogRelationFact;
    match resolved {
        Ok(table) => CatalogRelationFact::resolved(need, table)
            .map_err(|error| contract_error(error.to_string())),
        Err(CatalogResolutionError::Missing { reason }) => {
            CatalogRelationFact::missing(need, reason)
                .map_err(|error| contract_error(error.to_string()))
        }
        Err(error) => Err(contract_error(error.into_message())),
    }
}

fn resolve_local_catalog_table_typed(
    local: &novarocks_sql::planning::catalog::PlannerMemoryCatalog,
    database: &str,
    table: &str,
) -> CatalogResolutionResult<ResolvedAnalyzerTable> {
    let database_exists = local
        .database_exists(database)
        .map_err(CatalogResolutionError::failed)?;
    if !database_exists {
        return Err(CatalogResolutionError::missing(format!(
            "unknown database: {database}"
        )));
    }
    let normalized_table = novarocks_types::naming::normalize_identifier(table)
        .map_err(CatalogResolutionError::failed)?;
    if !local
        .table_names_in_database(database)
        .into_iter()
        .any(|name| name == normalized_table)
    {
        return Err(CatalogResolutionError::missing(format!(
            "unknown table: {table}"
        )));
    }
    novarocks_sql::planning::catalog::resolve_local_catalog_table(local, database, table)
        .map_err(CatalogResolutionError::failed)
}

fn project_binding_for_sql(
    binding_id: SqlTableBindingId,
    binding: QueryTableBinding,
) -> Result<QueryTableBinding, String> {
    binding.validate_sql_scan_binding(binding_id)?;
    Ok(binding)
}

impl PlannerTableProvider for CatalogServiceMaterializer<'_> {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.resolve_table_for_analysis_typed(catalog, database, table)
            .map_err(CatalogResolutionError::into_message)
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
        self.resolve_metadata_table_typed(catalog, database, table, metadata_table_type)
            .map_err(CatalogResolutionError::into_message)
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
    catalog_application: Option<&'a dyn novarocks_catalog_application::CatalogApplicationPort>,
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
pub fn build_catalog_service_provider_with_query_local_overlays<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    _lookup_mode: TableLookupMode,
    overlays: Vec<QueryLocalTableOverlay>,
    catalog_application: Option<&'a dyn novarocks_catalog_application::CatalogApplicationPort>,
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

pub fn build_catalog_service_provider_with_bindings_and_query_local_overlays<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    bindings: Arc<QueryTableBindingStore>,
    overlays: Vec<QueryLocalTableOverlay>,
    catalog_application: Option<&'a dyn novarocks_catalog_application::CatalogApplicationPort>,
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
pub fn iceberg_table_binding_loader<'a>(
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
    ) -> CatalogResolutionResult<QueryTableBinding> {
        let (base_table, snapshot_id) =
            crate::catalog_application::query_bindings::parse_time_travel_overlay_identity(table)
                .map(|(base_table, snapshot_id)| (base_table, Some(snapshot_id)))
                .unwrap_or((table, None));
        let mut materialization = load_connector_table_materialization_with_lease_typed(
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
        .map_err(CatalogResolutionError::failed)
    }

    fn load_metadata_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
        binding_id: SqlTableBindingId,
    ) -> CatalogResolutionResult<QueryTableBinding> {
        let alias = format!(
            "{table}${}",
            metadata_table_alias_suffix(metadata_table_type)?
        );
        let materialization = load_connector_table_alias_materialization_with_lease_typed(
            self.controls,
            self.connector_context.clone(),
            catalog,
            namespace,
            &alias,
        )?;
        metadata_query_binding_from_materialization(
            materialization,
            catalog,
            namespace,
            table,
            metadata_table_type,
            binding_id,
        )
    }

    fn prepare_strict_base_table<'a>(
        &'a self,
        catalog: &'a str,
        namespace: &'a str,
        table: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = CatalogResolutionResult<PreparedQueryTableBinding>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let (base_table, snapshot_id) =
                crate::catalog_application::query_bindings::parse_time_travel_overlay_identity(
                    table,
                )
                .map(|(base, snapshot)| (base, Some(snapshot)))
                .unwrap_or((table, None));
            let mut materialization = crate::catalog_application::query_catalog::load_connector_table_materialization_with_resolution_async(
                self.controls, self.connector_context.clone(), catalog, namespace, base_table,
                novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
            ).await?;
            if let Some(snapshot) = snapshot_id {
                materialization.read_selector =
                    novarocks_spi::connector::ConnectorReadSelector::SnapshotId(snapshot);
            }
            Ok(PreparedQueryTableBinding {
                materialization,
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
                metadata_kind: None,
            })
        })
    }

    fn prepare_metadata_table<'a>(
        &'a self,
        catalog: &'a str,
        namespace: &'a str,
        table: &'a str,
        kind: novarocks_sql::planning::catalog::MetadataTableKind,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = CatalogResolutionResult<PreparedQueryTableBinding>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let alias = format!("{table}${}", metadata_table_alias_suffix(kind)?);
            let materialization = crate::catalog_application::query_catalog::load_connector_table_materialization_with_resolution_async(
                self.controls, self.connector_context.clone(), catalog, namespace, &alias,
                novarocks_spi::connector::ConnectorTableResolution::ProviderReadAlias,
            ).await?;
            Ok(PreparedQueryTableBinding {
                materialization,
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
                metadata_kind: Some(kind),
            })
        })
    }
}

fn metadata_query_binding_from_materialization(
    materialization: ConnectorQueryTableMaterialization,
    catalog: &str,
    namespace: &str,
    table: &str,
    metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
    binding_id: SqlTableBindingId,
) -> CatalogResolutionResult<QueryTableBinding> {
    // The columns SQL analyzes must be the ones the typed reader produces.
    // The connector's own alias materialization still reports the retired
    // schema — `committed_at` as a bare `Int64`, `summary` as `Utf8` —
    // while both the frozen contract and the typed reader say
    // `TIMESTAMP WITH TIME ZONE` and `MAP`. Analyzing the retired shape
    // renders a raw epoch integer, and ordering by it fails at the
    // exchange with a schema mismatch.
    let columns = frozen_metadata_columns(metadata_table_type)?.unwrap_or(materialization.columns);
    Ok(QueryTableBinding {
        resolved: novarocks_sql::planning::catalog::resolved_metadata_table(
            catalog,
            namespace,
            table,
            metadata_table_type,
            columns,
            materialization.row_lineage_metadata_columns,
            binding_id,
        ),
        statistics_pin: materialization.statistics_pin.clone(),
        admission: QueryTableBindingAdmission::Exact(materialization.planning_lease.clone()),
        source_metadata: None,
        scan_materialization: Some(QueryScanMaterialization {
            table: materialization.read_table,
            selector: materialization.read_selector,
            mv_partition_selection: None,
            planning_lease: materialization.planning_lease,
        }),
        mv_target_read: None,
        write_target_admission: None,
        frozen_cohort_read: None,
        frozen_snapshot_materializations: BTreeMap::new(),
        prepared_source_reads: BTreeMap::new(),
    })
}

/// The frozen column contract for one metadata relation, when it is fully
/// determined by the relation kind alone.
///
/// `$files` and `$entries` are not: their `partition`,
/// `lower_bounds` and `upper_bounds` columns are derived from the table's own
/// partition spec and schema, and the contract entry point omits them rather
/// than guess. Those facts live in the connector and no typed column binding
/// carries a type today, so those two keep resolving through the connector's
/// alias materialization until the derived types can cross the control plane.
/// Returning `None` for them is that staged state, stated rather than hidden.
fn frozen_metadata_columns(
    kind: novarocks_sql::planning::catalog::MetadataTableKind,
) -> CatalogResolutionResult<Option<Vec<novarocks_types::schema::ColumnDef>>> {
    use novarocks_sql::planning::catalog::MetadataTableKind;

    Ok(match kind {
        MetadataTableKind::Partitions => {
            return Err(CatalogResolutionError::failed(
                "invariant: $partitions is a SQL view over $files",
            ));
        }
        MetadataTableKind::Files | MetadataTableKind::Entries => None,
        MetadataTableKind::Snapshots
        | MetadataTableKind::History
        | MetadataTableKind::Refs
        | MetadataTableKind::Manifests => Some(
            novarocks_sql::planning::catalog::metadata_table_schema(kind)
                .into_iter()
                .map(|column| novarocks_types::schema::ColumnDef {
                    name: column.name,
                    data_type: column.data_type,
                    nullable: column.nullable,
                    write_default: None,
                    logical_type: column.logical_type,
                })
                .collect(),
        ),
    })
}

/// Connector read-alias suffix for one metadata relation.
///
/// The suffix is the only thing that tells the connector which system relation
/// to materialize, so it must stay exactly the connector's own vocabulary. The
/// match is exhaustive on purpose: a new relation must fail to compile here
/// rather than resolve to some other relation's schema.
pub(crate) fn metadata_table_alias_suffix(
    kind: novarocks_sql::planning::catalog::MetadataTableKind,
) -> CatalogResolutionResult<&'static str> {
    use novarocks_sql::planning::catalog::MetadataTableKind;

    Ok(match kind {
        MetadataTableKind::Snapshots => "SNAPSHOTS",
        MetadataTableKind::History => "HISTORY",
        MetadataTableKind::Refs => "REFS",
        MetadataTableKind::Files => "FILES",
        MetadataTableKind::Manifests => "MANIFESTS",
        MetadataTableKind::Partitions => {
            return Err(CatalogResolutionError::failed(
                "invariant: $partitions is a SQL view over $files",
            ));
        }
        // Was `LOGICAL_ICEBERG_METADATA`. The alias is retired; `$entries` is
        // the only spelling, and it maps to the wire's ENTRIES worker type.
        MetadataTableKind::Entries => "ENTRIES",
    })
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
        let mut allocator = SqlTableBindingAllocator::try_new_for_test(
            NonZeroU64::new(scope).expect("non-zero scope"),
        )
        .expect("test binding allocator");
        for _ in 1..ordinal.get() {
            allocator.allocate().expect("non-zero test binding ordinal");
        }
        allocator.allocate().expect("non-zero test binding ordinal")
    }

    #[test]
    fn partitions_completion_loads_only_one_files_binding_and_preserves_its_token() {
        use novarocks_sql::compiler::{
            CatalogLookupTarget, CatalogRelationFact, DEFAULT_COMPLETION_LIMITS,
            SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent, SqlCompileProgress,
            SqlCompiler, SqlFactBatch, SqlFinalPlanCompileRequest, SqlNeedBatch,
            SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput, StatisticsFact,
            builtin_sql_function_catalog, noop_constant_evaluator,
        };
        use novarocks_sql::planning::catalog::MetadataTableKind;
        struct FilesLoader {
            calls: Arc<AtomicUsize>,
            tokens: Arc<Mutex<Vec<SqlTableBindingId>>>,
            partitioned: bool,
        }
        impl QueryTableBindingLoader for FilesLoader {
            fn load_strict_base_table(
                &self,
                _: &str,
                _: &str,
                _: &str,
                _: SqlTableBindingId,
            ) -> CatalogResolutionResult<QueryTableBinding> {
                panic!("a metadata read must not resolve the ordinary table or CTE")
            }
            fn load_metadata_table(
                &self,
                catalog: &str,
                db: &str,
                table: &str,
                kind: MetadataTableKind,
                token: SqlTableBindingId,
            ) -> CatalogResolutionResult<QueryTableBinding> {
                assert_eq!(kind, MetadataTableKind::Files);
                assert_eq!((catalog, db, table), ("ice", "db", "orders"));
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.tokens.lock().unwrap().push(token);
                let mut columns = novarocks_sql::planning::catalog::metadata_table_schema(kind)
                    .into_iter()
                    .map(|column| novarocks_types::schema::ColumnDef {
                        name: column.name,
                        data_type: column.data_type,
                        nullable: column.nullable,
                        write_default: None,
                        logical_type: column.logical_type,
                    })
                    .collect::<Vec<_>>();
                if self.partitioned {
                    columns.push(novarocks_types::schema::ColumnDef {
                        name: "partition".into(),
                        data_type: arrow::datatypes::DataType::Struct(
                            vec![arrow::datatypes::Field::new(
                                "p",
                                arrow::datatypes::DataType::Int32,
                                true,
                            )]
                            .into(),
                        ),
                        nullable: true,
                        write_default: None,
                        logical_type: None,
                    });
                }
                let resolved = novarocks_sql::planning::catalog::resolved_metadata_table(
                    catalog,
                    db,
                    table,
                    kind,
                    columns,
                    Vec::new(),
                    token,
                );
                // Local supplies only the fixture's capability-free binding
                // container. Keep the exact Metadata source and token, as the
                // production metadata loader does; no runtime is opened here.
                let mut binding = QueryTableBinding::local(resolved.clone(), token);
                binding.resolved = resolved;
                Ok(binding)
            }
        }
        for partitioned in [true, false] {
            for sql in [
                "SELECT * FROM orders$partitions",
                "SELECT p.* FROM DB.ORDERS$partitions p",
                "SELECT * FROM ICE.DB.ORDERS$partitions",
                "WITH orders AS (SELECT 7 AS content) SELECT p.* FROM orders$partitions p",
            ] {
                let service =
                    crate::catalog_application::query_catalog::new_query_catalog_service();
                let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
                let calls = Arc::new(AtomicUsize::new(0));
                let tokens = Arc::new(Mutex::new(Vec::new()));
                let catalog = CatalogServiceMaterializer::new(
                    Some("ice"),
                    &service,
                    bindings.clone(),
                    Box::new(FilesLoader {
                        calls: calls.clone(),
                        tokens: tokens.clone(),
                        partitioned,
                    }),
                );
                let control = SqlCompileControl::unbounded();
                let request = SqlFinalPlanCompileRequest::new(
                    novarocks_physical_plan::PlanVersionId::try_new([71; 16]).unwrap(),
                    SqlStatementInput::sql(sql),
                    SqlCompileIntent::Query,
                    SqlSessionContext {
                        sql_semantics: Default::default(),
                        current_catalog: Some("ice".into()),
                        current_database: "db".into(),
                        optimizer_settings: SessionOptimizerSettings {
                            enable_materialized_view_rewrite: Some(false),
                            ..SessionOptimizerSettings::default()
                        },
                    },
                    SqlPlanningEnvironment::Distributed,
                    builtin_sql_function_catalog().snapshot(),
                    noop_constant_evaluator(),
                    crate::application::test_constant_policy(),
                    control.clone(),
                    novarocks_physical_plan::PipelineDopDomain {
                        min: 1,
                        max: 8,
                        requires_power_of_two: true,
                    },
                    novarocks_physical_plan::ScanReadBudget {
                        max_batch_rows: 1024,
                        max_batch_bytes: 1 << 20,
                    },
                    DEFAULT_COMPLETION_LIMITS,
                );
                let mut progress =
                    SqlCompiler::start(request.try_into_completion().unwrap(), &control).unwrap();
                loop {
                    let SqlCompileProgress::Incomplete(compilation) = progress else {
                        panic!("expected actual provider read need")
                    };
                    let facts = match compilation.needs() {
                        SqlNeedBatch::CatalogRelations(needs) => SqlFactBatch::CatalogRelations(needs.iter().map(|need| {
                            let CatalogLookupTarget::IcebergMetadata { kind } = need.target() else { panic!("only Files facts") };
                            assert_eq!(kind, MetadataTableKind::Files);
                            let relation = need.relation();
                            let resolved = catalog.resolve_metadata_table_typed(Some(&relation.catalog), &relation.namespace, &relation.table, kind).unwrap();
                            CatalogRelationFact::resolved(need, resolved).unwrap()
                        }).collect::<Vec<_>>().into_boxed_slice()),
                        SqlNeedBatch::Statistics(needs) => SqlFactBatch::Statistics(needs.iter().map(|need| StatisticsFact::try_new(need, need.metrics().to_vec(), novarocks_sql::planning::dml::DmlStatisticsEvidence::Missing {
                            binding: need.binding(), label: "ice.db.orders$files".into(), reason: "no statistics in metadata binding test".into(),
                        }).unwrap()).collect::<Vec<_>>().into_boxed_slice()),
                        SqlNeedBatch::ProviderReads(needs) => {
                            assert_eq!(needs.len(), 1, "{sql}");
                            assert_eq!(calls.load(Ordering::SeqCst), 1, "{sql}");
                            assert_eq!(bindings.captured_bindings().len(), 1, "{sql}");
                            let token = {
                                let tokens = tokens.lock().unwrap();
                                assert_eq!(tokens.len(), 1);
                                tokens[0]
                            };
                            assert_eq!(needs[0].binding(), token);
                            assert!(matches!(needs[0].relation(), novarocks_sql::compiler::ProviderReadRelationNeed::Metadata { kind: MetadataTableKind::Files, .. }));
                            let first = catalog.get_iceberg_metadata_table(None, "db", "orders", MetadataTableKind::Files).unwrap();
                            let second = catalog.get_iceberg_metadata_table(Some("ice"), "db", "orders", MetadataTableKind::Files).unwrap();
                            assert_eq!(novarocks_sql::planning::catalog::table_binding_id(&first), token);
                            assert_eq!(novarocks_sql::planning::catalog::table_binding_id(&second), token);
                            assert_eq!(calls.load(Ordering::SeqCst), 1);
                            break;
                        }
                        other => panic!("unexpected metadata completion round {other:?}"),
                    };
                    progress = SqlCompiler::finish(compilation, facts, &control).unwrap();
                }
                let error = catalog
                    .resolve_metadata_table_typed(
                        None,
                        "db",
                        "orders",
                        MetadataTableKind::Partitions,
                    )
                    .unwrap_err();
                assert!(matches!(error, CatalogResolutionError::Failed { .. }));
                assert!(
                    error
                        .message()
                        .contains("invariant: $partitions is a SQL view over $files")
                );
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[test]
    fn partitions_cannot_be_materialized_as_a_provider_alias() {
        use novarocks_sql::planning::catalog::MetadataTableKind;
        assert!(
            metadata_table_alias_suffix(MetadataTableKind::Partitions)
                .unwrap_err()
                .message()
                .contains("invariant:")
        );
        assert!(
            frozen_metadata_columns(MetadataTableKind::Partitions)
                .unwrap_err()
                .message()
                .contains("invariant:")
        );
        assert_eq!(
            metadata_table_alias_suffix(MetadataTableKind::Files).unwrap(),
            "FILES"
        );
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
            source_metadata: None,
            scan_materialization: None,
            write_target_admission: None,
            mv_target_read: None,
            frozen_cohort_read: None,
            frozen_snapshot_materializations: BTreeMap::new(),
            prepared_source_reads: BTreeMap::new(),
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
        ) -> CatalogResolutionResult<QueryTableBinding> {
            Ok(local_binding(_binding))
        }

        fn load_metadata_table(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
            _metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
            _binding: SqlTableBindingId,
        ) -> CatalogResolutionResult<QueryTableBinding> {
            Err(CatalogResolutionError::failed(
                "metadata is not part of this overlay fixture",
            ))
        }
    }

    #[derive(Clone, Copy)]
    enum TypedLoaderFailure {
        Missing,
        Failed,
    }

    struct TypedFailingLoader(TypedLoaderFailure);

    impl QueryTableBindingLoader for TypedFailingLoader {
        fn load_strict_base_table(
            &self,
            _catalog: &str,
            namespace: &str,
            table: &str,
            _binding: SqlTableBindingId,
        ) -> CatalogResolutionResult<QueryTableBinding> {
            match self.0 {
                TypedLoaderFailure::Missing => Err(CatalogResolutionError::missing(format!(
                    "unknown table: {namespace}.{table}"
                ))),
                TypedLoaderFailure::Failed => {
                    Err(CatalogResolutionError::failed("catalog transport failed"))
                }
            }
        }

        fn load_metadata_table(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
            _metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
            _binding: SqlTableBindingId,
        ) -> CatalogResolutionResult<QueryTableBinding> {
            Err(CatalogResolutionError::failed(
                "metadata is not part of this fixture",
            ))
        }
    }

    struct UnavailableCatalogApplication;

    struct ChangingCatalogApplication {
        admissions: AtomicUsize,
    }

    impl ChangingCatalogApplication {
        fn observation(
            generation: u64,
        ) -> novarocks_catalog_application::CatalogRuntimeObservation {
            novarocks_catalog_application::CatalogRuntimeObservation {
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

    impl novarocks_catalog_application::CatalogApplicationPort for ChangingCatalogApplication {
        fn create_catalog(
            &self,
            _command: novarocks_catalog_application::CatalogCreateCommand,
        ) -> Result<
            novarocks_catalog_application::CatalogRuntimeObservation,
            novarocks_catalog_application::CatalogApplicationError,
        > {
            unreachable!("create is not part of this fixture")
        }

        fn drop_catalog(
            &self,
            _command: novarocks_catalog_application::CatalogDropCommand,
        ) -> Result<(), novarocks_catalog_application::CatalogApplicationError> {
            unreachable!("drop is not part of this fixture")
        }

        fn admit_catalog(
            &self,
            _instance_id: &novarocks_spi::connector::ConnectorInstanceId,
        ) -> novarocks_catalog_application::CatalogAdmission {
            let attempt = self.admissions.fetch_add(1, Ordering::SeqCst);
            novarocks_catalog_application::CatalogAdmission::Ready(Self::observation(
                if attempt == 0 { 1 } else { 2 },
            ))
        }
    }

    impl novarocks_catalog_application::CatalogApplicationPort for UnavailableCatalogApplication {
        fn create_catalog(
            &self,
            _command: novarocks_catalog_application::CatalogCreateCommand,
        ) -> Result<
            novarocks_catalog_application::CatalogRuntimeObservation,
            novarocks_catalog_application::CatalogApplicationError,
        > {
            Err(novarocks_catalog_application::CatalogApplicationError::new(
                novarocks_catalog_application::CatalogApplicationErrorKind::Unavailable,
                "projection is stale",
            ))
        }

        fn drop_catalog(
            &self,
            _command: novarocks_catalog_application::CatalogDropCommand,
        ) -> Result<(), novarocks_catalog_application::CatalogApplicationError> {
            Err(novarocks_catalog_application::CatalogApplicationError::new(
                novarocks_catalog_application::CatalogApplicationErrorKind::Unavailable,
                "projection is stale",
            ))
        }

        fn admit_catalog(
            &self,
            _instance_id: &novarocks_spi::connector::ConnectorInstanceId,
        ) -> novarocks_catalog_application::CatalogAdmission {
            novarocks_catalog_application::CatalogAdmission::Unavailable {
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
    fn typed_resolution_distinguishes_external_missing_from_failure() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let missing_materializer = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            Arc::new(QueryTableBindingStore::try_new().expect("missing binding store")),
            Box::new(TypedFailingLoader(TypedLoaderFailure::Missing)),
        );
        let missing = missing_materializer
            .resolve_table_for_analysis_typed(None, "db", "orders")
            .expect_err("missing table must remain a typed absence");
        let repeated_missing = missing_materializer
            .resolve_table_for_analysis_typed(None, "db", "orders")
            .expect_err("memoized missing table must retain its typed absence");
        let failed = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            Arc::new(QueryTableBindingStore::try_new().expect("failure binding store")),
            Box::new(TypedFailingLoader(TypedLoaderFailure::Failed)),
        )
        .resolve_table_for_analysis_typed(None, "db", "orders")
        .expect_err("catalog failure must interrupt resolution");

        assert!(matches!(missing, CatalogResolutionError::Missing { .. }));
        assert_eq!(missing.message(), "unknown table: db.orders");
        assert!(matches!(
            repeated_missing,
            CatalogResolutionError::Missing { .. }
        ));
        assert!(matches!(failed, CatalogResolutionError::Failed { .. }));
        assert_eq!(failed.message(), "catalog transport failed");
    }

    #[test]
    fn typed_local_resolution_preserves_missing_and_invalid_name_failure() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let materializer = CatalogServiceMaterializer::new(
            Some("default_catalog"),
            &service,
            Arc::new(QueryTableBindingStore::try_new().expect("binding store")),
            Box::new(OverlayLoader),
        );

        let missing = materializer
            .resolve_table_for_analysis_typed(None, "default", "orders")
            .expect_err("absent local table must remain a typed absence");
        let invalid = materializer
            .resolve_table_for_analysis_typed(None, "default", "bad-name")
            .expect_err("invalid local name must remain a hard failure");

        assert_eq!(
            missing,
            CatalogResolutionError::Missing {
                reason: "unknown table: orders".to_string(),
            }
        );
        assert!(matches!(invalid, CatalogResolutionError::Failed { .. }));
        assert_eq!(invalid.message(), "unsupported identifier `bad-name`");
    }

    #[test]
    fn legacy_catalog_provider_stringifies_typed_missing_at_the_edge() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let materializer = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            Arc::new(QueryTableBindingStore::try_new().expect("binding store")),
            Box::new(TypedFailingLoader(TypedLoaderFailure::Missing)),
        );

        let error = materializer
            .resolve_table_for_analysis(None, "db", "orders")
            .expect_err("legacy provider still reports a string error");

        assert_eq!(error, "unknown table: db.orders");
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
        let local = service.local().read().expect("catalog read");
        assert!(
            novarocks_sql::planning::catalog::local_catalog_table(&local, "db", "__nr_cow_orders")
                .is_err()
        );
    }
    fn original_schema_need(sql: &str) -> novarocks_sql::compiler::SqlCapturedCatalogPreparation {
        let statements = novarocks_parser::parse(sql).unwrap();
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("actual SQL query fixture");
        };
        novarocks_sql::compiler::SqlCapturedCatalogPreparation::try_new(
            query.clone(),
            Some("ice"),
            "db",
            novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap()
    }

    #[test]
    fn original_mv_catalog_fact_preserves_actual_provider_cause_and_one_lookup() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let registry = crate::connector::FixtureConnectorRegistry::new();
        crate::connector::scan_model::register_planned_files_fixture(
            &registry,
            "ice",
            Vec::new(),
            None,
        );
        let controls = crate::connector::FixtureControlResolver::new(registry);
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
        let materializer = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            Arc::clone(&bindings),
            Box::new(OverlayLoader),
        );
        let preparation = original_schema_need("SELECT * FROM ice.db.orders");
        let [need] = preparation.needs() else {
            panic!("one actual relation need");
        };
        let OriginalCatalogLookup::Metadata(lookup) = materializer
            .begin_original_catalog_lookup(need, &controls)
            .unwrap()
        else {
            panic!("the original lookup must request metadata exactly once");
        };
        let original = ConnectorError::from_domain(
            ConnectorErrorKind::ResourceExhausted,
            novarocks_workload_control::WorkError::Capacity("original catalog observation"),
        );
        let fact = original.failure_fact();
        let cause = std::error::Error::source(&original)
            .unwrap()
            .downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
            .unwrap()
            .clone();
        let failed = materializer
            .finish_original_catalog_lookup(lookup, Err(original))
            .unwrap_err();
        assert_eq!(failed.original_failure(), Some(&cause));
        assert_eq!(cause.failure_fact(), fact);
        assert_eq!(fact.class(), FailureClass::Resource);
        assert!(matches!(
            bindings.resolved_lookup(&QueryTableBindingKey::analysis_lookup(
                "ice", "db", "orders"
            )),
            Some(Err(_))
        ));
        let repeated = match materializer.begin_original_catalog_lookup(need, &controls) {
            Err(error) => error,
            Ok(_) => panic!("failed original metadata may not be loaded again"),
        };
        assert_eq!(repeated.original_failure(), Some(&cause));
    }

    #[test]
    fn original_mv_catalog_keeps_canonical_frozen_relations_separate_from_local_names() {
        let controls = crate::connector::FixtureControlResolver::new(
            crate::connector::FixtureConnectorRegistry::new(),
        );
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
        let creations = Arc::new(AtomicUsize::new(0));
        let overlays = ["ice", "other"]
            .into_iter()
            .map(|catalog| {
                let creations = creations.clone();
                QueryLocalTableOverlay::for_catalog(
                    catalog,
                    "db",
                    "orders",
                    QueryTableBindingKey::snapshot(catalog, "db", "orders", 7),
                    move |binding| {
                        creations.fetch_add(1, Ordering::Relaxed);
                        let mut facts = frozen_overlay_binding(binding);
                        facts.resolved = test_connector_read_materialization(
                            catalog,
                            "orders",
                            binding,
                            novarocks_spi::connector::ConnectorReadSelector::SnapshotId(7),
                        );
                        Ok(facts)
                    },
                )
            })
            .collect();
        let materializer = CatalogServiceMaterializer::new_with_query_local_overlays(
            Some("ice"),
            &service,
            bindings.clone(),
            Box::new(OverlayLoader),
            overlays,
        );
        // Real SQL SchemaOnly prepass -> original application lookup -> exact
        // captured catalog; an empty resolver cannot supply a fallback lease.
        let preparation =
            original_schema_need("SELECT 1 FROM ice.db.orders a JOIN other.db.orders b ON 1 = 1");
        let facts = preparation
            .needs()
            .iter()
            .map(|need| {
                let OriginalCatalogLookup::Fact(fact) = materializer
                    .begin_original_catalog_lookup(need, &controls)
                    .unwrap()
                else {
                    panic!("an exact frozen relation must not reload live metadata");
                };
                fact
            })
            .collect::<Vec<_>>();
        let catalog = preparation.capture(&facts).unwrap();
        let mut tokens = Vec::new();
        for name in ["ice", "other"] {
            let resolved = catalog
                .provider()
                .resolve_table_for_analysis(Some(name), "db", "orders")
                .unwrap();
            assert_eq!(
                novarocks_sql::planning::catalog::materialization_identity_facts(&resolved).fqn(),
                format!("{name}.db.orders"),
            );
            let token = novarocks_sql::planning::catalog::table_binding_id(&resolved);
            assert_eq!(
                novarocks_sql::planning::catalog::frozen_input_snapshot_id(&resolved),
                Some(7),
            );
            assert_eq!(
                novarocks_sql::planning::catalog::table_binding_id(
                    &bindings.binding(token).unwrap().resolved
                ),
                token,
            );
            tokens.push(token);
        }
        assert_ne!(tokens[0], tokens[1]);
        assert_eq!(creations.load(Ordering::Relaxed), 2);
        let repeated = materializer
            .resolve_table_for_analysis(None, "DB", "ORDERS")
            .unwrap();
        assert_eq!(
            novarocks_sql::planning::catalog::table_binding_id(&repeated),
            tokens[0]
        );
        assert_eq!(creations.load(Ordering::Relaxed), 2);
        assert!(
            materializer
                .resolve_table_for_analysis(Some("default_catalog"), "db", "orders")
                .is_err()
        );
        assert!(
            catalog
                .provider()
                .resolve_table_for_analysis(Some("foreign"), "db", "orders")
                .is_err()
        );
    }

    #[test]
    fn original_mv_captured_catalog_consumes_the_exact_local_overlay_without_a_loader() {
        let controls = crate::connector::FixtureControlResolver::new(
            crate::connector::FixtureConnectorRegistry::new(),
        );
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
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
        let preparation = original_schema_need("SELECT * FROM default_catalog.db.__nr_cow_orders");
        let [need] = preparation.needs() else {
            panic!("one actual overlay relation need");
        };
        let OriginalCatalogLookup::Fact(fact) = materializer
            .begin_original_catalog_lookup(need, &controls)
            .unwrap()
        else {
            panic!("a frozen local overlay must not issue a provider request");
        };
        let catalog = preparation.capture(&[fact]).unwrap();
        let resolved = catalog
            .provider()
            .resolve_table_for_analysis(Some("default_catalog"), "db", "__nr_cow_orders")
            .unwrap();
        let token = novarocks_sql::planning::catalog::table_binding_id(&resolved);
        assert!(token.belongs_to(bindings.scope()));
        assert_eq!(
            novarocks_sql::planning::catalog::materialization_identity_facts(&resolved).fqn(),
            "default_catalog.db.__nr_cow_orders"
        );
        let physical = bindings.binding(token).unwrap();
        assert_eq!(
            novarocks_sql::planning::catalog::materialization_identity_facts(&physical.resolved)
                .fqn(),
            "ice.db.__nr_cow_orders"
        );
        assert_eq!(
            novarocks_sql::planning::catalog::table_binding_id(&physical.resolved),
            token
        );
        assert_eq!(
            novarocks_sql::planning::catalog::frozen_input_snapshot_id(&resolved),
            Some(7)
        );
        assert_eq!(
            novarocks_sql::planning::catalog::frozen_input_snapshot_id(&physical.resolved),
            Some(7)
        );
        let repeated = materializer
            .resolve_table_for_analysis(Some("default_catalog"), "db", "__nr_cow_orders")
            .unwrap();
        assert_eq!(
            novarocks_sql::planning::catalog::table_binding_id(&repeated),
            token
        );
        assert_eq!(
            novarocks_sql::planning::catalog::materialization_identity_facts(&repeated).fqn(),
            "default_catalog.db.__nr_cow_orders"
        );
        let mixed_case = materializer
            .resolve_table_for_analysis(Some("default_catalog"), "DB", "__NR_Cow_Orders")
            .expect("the original case-insensitive overlay lookup remains supported");
        assert_eq!(
            novarocks_sql::planning::catalog::table_binding_id(&mixed_case),
            token
        );
        assert_eq!(
            novarocks_sql::planning::catalog::materialization_identity_facts(&mixed_case).fqn(),
            "default_catalog.db.__nr_cow_orders"
        );
        assert_eq!(
            novarocks_sql::planning::catalog::frozen_input_snapshot_id(&mixed_case),
            Some(7)
        );
        let mixed_preparation =
            original_schema_need("SELECT * FROM DEFAULT_CATALOG.DB.__NR_COW_ORDERS");
        let [mixed_need] = mixed_preparation.needs() else {
            panic!("one normalized original overlay need");
        };
        let OriginalCatalogLookup::Fact(mixed_fact) = materializer
            .begin_original_catalog_lookup(mixed_need, &controls)
            .unwrap()
        else {
            panic!("the normalized overlay must not issue a provider request");
        };
        mixed_preparation.capture(&[mixed_fact]).unwrap();
        assert!(bindings.scan_materialization(token).unwrap().is_none());
        let local = service.local().read().unwrap();
        assert!(
            novarocks_sql::planning::catalog::local_catalog_table(&local, "db", "__nr_cow_orders")
                .is_err()
        );
    }
}

#[cfg(test)]
mod async_observation_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MissingLoader {
        gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        calls: Arc<AtomicUsize>,
    }
    impl QueryTableBindingLoader for MissingLoader {
        fn load_strict_base_table(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: SqlTableBindingId,
        ) -> CatalogResolutionResult<QueryTableBinding> {
            panic!("async resolution must not call the joining loader")
        }
        fn load_metadata_table(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: novarocks_sql::planning::catalog::MetadataTableKind,
            _: SqlTableBindingId,
        ) -> CatalogResolutionResult<QueryTableBinding> {
            panic!("async resolution must not call the joining loader")
        }
        fn prepare_strict_base_table<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: &'a str,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = CatalogResolutionResult<PreparedQueryTableBinding>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let gate = self.gate.lock().unwrap().take();
                if let Some(gate) = gate {
                    gate.await.expect("original observation released");
                }
                Err(CatalogResolutionError::missing("unknown table: db.orders"))
            })
        }
    }

    #[tokio::test]
    async fn async_materialization_waits_without_a_binding_lock_and_memoizes_original_absence() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let (release, gate) = tokio::sync::oneshot::channel();
        let materializer = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            Arc::clone(&bindings),
            Box::new(MissingLoader {
                gate: Mutex::new(Some(gate)),
                calls: Arc::clone(&calls),
            }),
        );
        let key = QueryTableBindingKey::analysis_lookup("ice", "db", "orders");
        let mut waiting =
            Box::pin(materializer.resolve_table_for_analysis_async(None, "db", "orders"));
        assert!(futures::poll!(&mut waiting).is_pending());
        // This real original-store observation would deadlock if its metadata
        // mutex were retained by the suspended provider operation.
        assert!(bindings.resolved_lookup(&key).is_none());
        release.send(()).unwrap();
        let error = waiting.await.unwrap_err();
        assert!(matches!(error, CatalogResolutionError::Missing { .. }));
        let repeated = materializer
            .resolve_table_for_analysis_async(None, "db", "orders")
            .await
            .unwrap_err();
        assert_eq!(error, repeated);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(bindings.resolved_lookup(&key), Some(Err(_))));
    }
}
