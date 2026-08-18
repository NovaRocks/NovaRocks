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

//! Application-owned query-local table bindings.
//!
//! A binding is deliberately more than a catalog table.  It captures the
//! exact connector control lease, table handle, incarnation and statistics
//! data version selected during admission.  SQL receives only the opaque
//! `SqlTableBindingId`; preparation and statistics must validate that token
//! against this store rather than acquiring a current connector generation.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::connector::backend::ResolvedTableStatisticsPin;
use arrow::datatypes::SchemaRef;
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorReadSelector, ConnectorTableHandle,
    ConnectorWritePreparation,
};
use novarocks_sql::binding::{SqlTableBindingAllocator, SqlTableBindingId, SqlTableBindingScopeId};
use novarocks_sql::planning::catalog::{
    self, MetadataTableKind as SqlMetadataTableKind, ResolvedAnalyzerTable,
};

static NEXT_BINDING_SCOPE: AtomicU64 = AtomicU64::new(1);

/// Canonical request-local lookup identity.  Names are normalized before the
/// binding is inserted, so a resolve failure is memoized just like success.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct QueryTableBindingKey {
    catalog: String,
    namespace: String,
    table: String,
    selector: QueryTableBindingSelector,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum QueryTableBindingSelector {
    StrictBaseTable,
    /// A terminal writer target. This remains separate from a read binding
    /// for the same physical table because the writer's frozen physical
    /// schema may include hidden lineage or MV state columns that a scan does
    /// not expose.
    /// One provider-signed terminal writer target. Multiple physical sink
    /// shapes for the same table (for example MOR change streams) must retain
    /// distinct bindings, rather than allowing one shape to stand in for
    /// another during SQL sink projection.
    WriteTarget([u8; 32]),
    Snapshot(i64),
    TimestampMillis(i64),
    Metadata(SqlMetadataTableKind),
    /// One frozen materialized-view target.  The target UUID distinguishes a
    /// recreated table at the same name, while the snapshot keeps target-state
    /// and target-locator scans on the exact refresh baseline.
    MvTarget {
        target_table_uuid: String,
        frozen_snapshot_id: Option<i64>,
    },
}

impl QueryTableBindingKey {
    /// Resolve the synthetic time-travel analyzer identity to the canonical
    /// physical table and snapshot selector before it reaches the request
    /// local memo.  This overlay is intentionally local to the binding
    /// store; it must never register a synthetic table in the global catalog.
    pub fn analysis_lookup(catalog: &str, namespace: &str, table: &str) -> Self {
        if let Some((base_table, snapshot_id)) = parse_time_travel_overlay_identity(table) {
            return Self::snapshot(catalog, namespace, base_table, snapshot_id);
        }
        Self::strict_base(catalog, namespace, table)
    }
    pub fn strict_base(catalog: &str, namespace: &str, table: &str) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::StrictBaseTable,
        )
    }

    /// Reserve an exact terminal writer target.  A write must never reuse a
    /// same-name read binding: those bindings carry different SQL facts while
    /// both remain valid for their independently frozen application roles.
    pub fn write_target(
        catalog: &str,
        namespace: &str,
        table: &str,
        preparation_digest: [u8; 32],
    ) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::WriteTarget(preparation_digest),
        )
    }

    pub fn snapshot(catalog: &str, namespace: &str, table: &str, snapshot_id: i64) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::Snapshot(snapshot_id),
        )
    }

    pub fn timestamp_millis(
        catalog: &str,
        namespace: &str,
        table: &str,
        timestamp_millis: i64,
    ) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::TimestampMillis(timestamp_millis),
        )
    }

    pub fn metadata(
        catalog: &str,
        namespace: &str,
        table: &str,
        kind: SqlMetadataTableKind,
    ) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::Metadata(kind),
        )
    }

    /// Identity for a materialized-view refresh target captured during
    /// admission.  This is deliberately distinct from a normal base-table or
    /// time-travel key: both target-state and target-locator scans must reuse
    /// this same frozen materialization, never a later target generation.
    pub fn mv_target(
        catalog: &str,
        namespace: &str,
        table: &str,
        target_table_uuid: &str,
        frozen_snapshot_id: Option<i64>,
    ) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::MvTarget {
                target_table_uuid: target_table_uuid.to_ascii_lowercase(),
                frozen_snapshot_id,
            },
        )
    }

    fn new(
        catalog: &str,
        namespace: &str,
        table: &str,
        selector: QueryTableBindingSelector,
    ) -> Self {
        Self {
            catalog: catalog.to_ascii_lowercase(),
            namespace: namespace.to_ascii_lowercase(),
            table: table.to_ascii_lowercase(),
            selector,
        }
    }
}

pub fn parse_time_travel_overlay_identity(table: &str) -> Option<(&str, i64)> {
    let encoded = table.strip_prefix("__sqlx1_tt_")?;
    let (base_table, snapshot_id) = encoded.rsplit_once('_')?;
    (!base_table.is_empty())
        .then(|| snapshot_id.parse::<i64>().ok())
        .flatten()
        .map(|snapshot_id| (base_table, snapshot_id))
}

/// One successful application materialization.  Opaque connector authority
/// stays here; neither the SQL scan vocabulary nor SQL catalog facts contain
/// a provider table, files, cloud properties, or serialized metadata.
#[derive(Clone)]
pub enum QueryTableBindingAdmission {
    /// Local SQL tables do not own a connector generation and cannot be used
    /// as a connector read or write admission source.
    Local,
    /// A connector-owned table admission retains the exact generation that
    /// admitted its read materialization or terminal write target.
    Exact(ConnectorControlPlanningLease),
}

impl QueryTableBindingAdmission {
    pub fn exact_planning_lease(&self) -> Result<ConnectorControlPlanningLease, String> {
        match self {
            Self::Exact(lease) => Ok(lease.clone()),
            Self::Local => Err("query binding has no connector planning lease".to_string()),
        }
    }
}

#[derive(Clone)]
pub struct QueryTableBinding {
    pub resolved: ResolvedAnalyzerTable,
    pub statistics_pin: Option<ResolvedTableStatisticsPin>,
    pub admission: QueryTableBindingAdmission,
    /// Provider facts required by scan preparation.  This is deliberately
    /// application-owned and paired with the same token as `resolved`; it is
    /// never embedded in a SQL logical or distributed plan.
    pub scan_materialization: Option<QueryScanMaterialization>,
    /// MV target SQL facts plus the two admitted opaque read authorities. A
    /// target-state scan may choose the pre-filtered read, while its locator
    /// always uses the full read; neither lane receives provider file facts.
    pub mv_target_read: Option<MvTargetReadAdmission>,
    /// SQL-owned write facts projected at connector admission.  The opaque
    /// handle is retained solely for the provider to rehydrate its execution
    /// carrier after SQL planning; Core never parses it or serializes provider
    /// metadata while preparing a terminal write.
    pub write_target_admission: Option<QueryWriteTargetAdmission>,
    /// Exact file sets captured for the snapshot selectors emitted by one
    /// admitted logical plan.  A binding can serve both sides of an IMV
    /// from/to comparison, so the primary materialization alone is not
    /// sufficient to recover a historical scan at preparation time.
    ///
    /// The map is query-local and shares the binding's planning lease; it is
    /// never populated by a later catalog lookup.
    pub frozen_snapshot_materializations: BTreeMap<i64, QueryScanMaterialization>,
    /// Provider-sealed snapshot-window admissions retained for SQL delta
    /// scans. Core never decodes their handles or reconstructs provider
    /// physical facts; preparation reuses the exact sealed scan through this
    /// binding's request-local token.
    pub admitted_change_scans: BTreeMap<(i64, i64), novarocks_spi::connector::ConnectorScan>,
}

/// One Provider-signed terminal write preparation retained beside the SQL
/// binding token.  Field identity, input shape and opaque table authority are
/// all sealed by the provider; SQL may project its Arrow layout and field
/// tokens but must not reconstruct table-format metadata.
#[derive(Clone)]
pub struct QueryWriteTargetAdmission {
    pub preparation: ConnectorWritePreparation,
}

/// Exact provider scan facts retained after admission.  The concrete Iceberg
/// representation is temporary only at this application boundary while SQL
/// callers are migrated to `SqlScanSource`; preparation must obtain it by the
/// request-local binding token rather than from a planner table.
#[derive(Clone)]
pub struct QueryScanMaterialization {
    /// Provider-neutral scan authority.  The handle is opaque to Core; the
    /// schema is the single projection-ordinal authority and the lease keeps
    /// the exact control generation alive until native preparation finishes.
    pub table: ConnectorTableHandle,
    pub schema: SchemaRef,
    pub selector: ConnectorReadSelector,
    pub statistics_pin: Option<ResolvedTableStatisticsPin>,
    pub planning_lease: ConnectorControlPlanningLease,
}

#[derive(Clone)]
pub struct MvTargetReadAdmission {
    pub full: QueryScanMaterialization,
    pub affected_partitions: QueryScanMaterialization,
    pub target_table_uuid: String,
    pub frozen_snapshot_id: Option<i64>,
}

impl std::fmt::Debug for QueryScanMaterialization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorRead")
            .field("owner", self.table.owner())
            .field("selector", &self.selector)
            .finish_non_exhaustive()
    }
}

impl QueryTableBinding {
    pub fn local(resolved: ResolvedAnalyzerTable, binding: SqlTableBindingId) -> Self {
        Self {
            resolved: catalog::attach_binding_to_local_materialization(resolved, binding),
            statistics_pin: None,
            admission: QueryTableBindingAdmission::Local,
            scan_materialization: None,
            mv_target_read: None,
            write_target_admission: None,
            frozen_snapshot_materializations: BTreeMap::new(),
            admitted_change_scans: BTreeMap::new(),
        }
    }

    /// Verify that the application materializer paired this table with the
    /// token it reserved.  Concrete provider scans are never accepted here:
    /// they must already live in `scan_materialization` before SQL receives
    /// the table facts.
    pub fn validate_sql_scan_binding(&self, binding: SqlTableBindingId) -> Result<(), String> {
        catalog::validate_materialization_binding(&self.resolved, binding)
    }
}

/// Build the complete owner-side admission retained by delta-preparation
/// tests. The SQL scan itself comes from a sealed SQL fixture; this helper
/// only pairs its opaque request-local token with one provider-sealed change
/// window and never exposes a planner table constructor.
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub fn admitted_change_window_binding_for_test(
    binding: SqlTableBindingId,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
    scan: novarocks_spi::connector::ConnectorScan,
) -> QueryTableBinding {
    let resolved = catalog::materialize_connector_read_table(
        novarocks_sql::planning::catalog::ConnectorReadTableFacts {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: Vec::new(),
            schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            binding,
            selector: ConnectorReadSelector::Current,
            planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
        },
    )
    .expect("test catalog facts materialize")
    .into_resolved_table();
    QueryTableBinding {
        resolved,
        statistics_pin: None,
        admission: QueryTableBindingAdmission::Local,
        scan_materialization: None,
        mv_target_read: None,
        write_target_admission: None,
        frozen_snapshot_materializations: BTreeMap::new(),
        admitted_change_scans: BTreeMap::from([((from_snapshot_id, to_snapshot_id), scan)]),
    }
}

struct StoredBinding {
    id: SqlTableBindingId,
}

/// Exact application authority paired with one compiler request.
pub struct QueryTableBindingStore {
    allocator: Mutex<SqlTableBindingAllocator>,
    entries: Mutex<HashMap<QueryTableBindingKey, Result<StoredBinding, String>>>,
    by_id: Mutex<HashMap<SqlTableBindingId, Arc<QueryTableBinding>>>,
}

impl QueryTableBindingStore {
    /// Allocate one fresh process-local scope.  Scope exhaustion is explicit
    /// rather than silently reusing a token from another query.
    pub fn try_new() -> Result<Self, String> {
        let raw_scope = NEXT_BINDING_SCOPE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| "SQL table binding scope space is exhausted".to_string())?;
        let scope = NonZeroU64::new(raw_scope)
            .ok_or_else(|| "SQL table binding scope space is exhausted".to_string())?;
        Ok(Self {
            allocator: Mutex::new(SqlTableBindingAllocator::try_new(scope)?),
            entries: Mutex::new(HashMap::new()),
            by_id: Mutex::new(HashMap::new()),
        })
    }

    /// Construct a deterministic request-local store for owner-side unit
    /// fixtures.  Production admission must always use `try_new`, which
    /// allocates a process-unique scope.  Tests use this only alongside
    /// `test_sql_scan_source`, whose token has the same fixed scope.
    #[cfg(any(test, feature = "query-execution-contract-test-support"))]
    pub fn try_new_with_scope_for_test(scope: NonZeroU64) -> Self {
        Self {
            allocator: Mutex::new(
                SqlTableBindingAllocator::try_new(scope)
                    .expect("test binding scope must construct SQL allocator"),
            ),
            entries: Mutex::new(HashMap::new()),
            by_id: Mutex::new(HashMap::new()),
        }
    }

    /// Stable, redacted identity material for one admitted binding set.
    ///
    /// This is used to bind application-side prepared artifacts (for example
    /// CTAS) to the exact catalog/statistics/control generation used during
    /// compilation. Opaque provider bytes are hashed rather than embedded so
    /// the digest cannot become a provider payload carrier.
    pub fn stable_digest_material(&self) -> Vec<u8> {
        use sha2::{Digest, Sha256};

        let mut material = Vec::new();
        material.extend_from_slice(&self.scope().get().get().to_be_bytes());
        for (binding_id, binding) in self.captured_bindings() {
            material.extend_from_slice(&binding_id.ordinal().get().to_be_bytes());
            let identity = catalog::materialization_identity_facts(&binding.resolved).fqn();
            material.extend_from_slice(&(identity.len() as u64).to_be_bytes());
            material.extend_from_slice(identity.as_bytes());
            if let Some(pin) = &binding.statistics_pin {
                material.extend_from_slice(pin.table.owner().as_str().as_bytes());
                material.push(0);
                material.extend_from_slice(Sha256::digest(pin.table.payload()).as_slice());
                material.extend_from_slice(Sha256::digest(pin.data_version.as_bytes()).as_slice());
            }
            if let QueryTableBindingAdmission::Exact(lease) = &binding.admission {
                let descriptor = lease.binding().descriptor();
                material.extend_from_slice(descriptor.provider_id.as_str().as_bytes());
                material.push(0);
                material.extend_from_slice(descriptor.instance_id.as_str().as_bytes());
                material.push(0);
                material.extend_from_slice(&lease.binding().incarnation().to_bytes());
            }
            material.push(0xff);
        }
        material
    }

    pub fn scope(&self) -> SqlTableBindingScopeId {
        self.allocator
            .lock()
            .expect("query table binding allocator lock")
            .scope()
    }

    /// Memoize both success and failure.  The supplied load closure executes
    /// at most once for a canonical key in this request.
    pub fn resolve_or_insert(
        &self,
        key: QueryTableBindingKey,
        load: impl FnOnce() -> Result<QueryTableBinding, String>,
    ) -> Result<SqlTableBindingId, String> {
        self.resolve_or_insert_with_id(key, |_| load())
    }

    /// Reserve the request-local token before projecting provider facts into a
    /// SQL table.  The loader cannot observe any other request's token, and
    /// the provisional token is inserted only when materialization succeeds.
    ///
    /// This is the admission seam used by the `SqlScanSource` cutover: the
    /// application loader receives the exact token that the resulting SQL
    /// table will carry, while the concrete scan authority remains in this
    /// store. Failed loads remain memoized and never publish their token.
    pub fn resolve_or_insert_with_id(
        &self,
        key: QueryTableBindingKey,
        load: impl FnOnce(SqlTableBindingId) -> Result<QueryTableBinding, String>,
    ) -> Result<SqlTableBindingId, String> {
        let mut entries = self.entries.lock().expect("query table binding lock");
        if let Some(entry) = entries.get(&key) {
            return entry.as_ref().map(|stored| stored.id).map_err(Clone::clone);
        }

        let result = self.allocate_id().and_then(|id| {
            load(id).map(|binding| {
                let binding = Arc::new(binding);
                self.by_id
                    .lock()
                    .expect("query table binding by-id lock")
                    .insert(id, Arc::clone(&binding));
                StoredBinding { id }
            })
        });
        let response = result
            .as_ref()
            .map(|stored| stored.id)
            .map_err(Clone::clone);
        entries.insert(key, result);
        response
    }

    pub fn binding(&self, id: SqlTableBindingId) -> Result<Arc<QueryTableBinding>, String> {
        if !id.belongs_to(self.scope()) {
            return Err("SQL table binding token belongs to a different request".to_string());
        }
        self.by_id
            .lock()
            .expect("query table binding by-id lock")
            .get(&id)
            .cloned()
            .ok_or_else(|| "SQL table binding token is missing from this request".to_string())
    }

    pub fn statistics_pin(
        &self,
        id: SqlTableBindingId,
    ) -> Result<Option<ResolvedTableStatisticsPin>, String> {
        Ok(self.binding(id)?.statistics_pin.clone())
    }

    pub fn exact_planning_lease(
        &self,
        id: SqlTableBindingId,
    ) -> Result<ConnectorControlPlanningLease, String> {
        self.binding(id)?.admission.exact_planning_lease()
    }

    /// Recover provider scan facts only through the exact request-local token.
    /// A missing materialization is a submission-time contract failure, not a
    /// reason to resolve a current table or connector generation.
    pub fn scan_materialization(
        &self,
        id: SqlTableBindingId,
    ) -> Result<Option<QueryScanMaterialization>, String> {
        Ok(self.binding(id)?.scan_materialization.clone())
    }

    /// Recover the exact file set admitted for one frozen snapshot selector.
    /// A selector without an admitted entry is a hard submission failure: a
    /// preparation-time fallback to the current materialization would silently
    /// turn an IMV `From` scan into a `To` scan.
    pub fn frozen_snapshot_materialization(
        &self,
        id: SqlTableBindingId,
        snapshot_id: i64,
    ) -> Result<QueryScanMaterialization, String> {
        let binding = self.binding(id)?;
        binding
            .frozen_snapshot_materializations
            .get(&snapshot_id)
            .cloned()
            .ok_or_else(|| {
            format!(
                "SQL frozen snapshot {snapshot_id} has no admitted connector materialization for its request-local binding"
            )
            })
    }

    /// Return the immutable bindings captured during admission.  The caller
    /// may project them into compiler input, but must not use this view to
    /// acquire a newer connector generation.
    pub fn captured_bindings(&self) -> Vec<(SqlTableBindingId, Arc<QueryTableBinding>)> {
        let mut bindings: Vec<_> = self
            .by_id
            .lock()
            .expect("query table binding by-id lock")
            .iter()
            .map(|(id, binding)| (*id, Arc::clone(binding)))
            .collect();
        bindings.sort_by_key(|(id, _)| id.ordinal().get());
        bindings
    }

    /// Lookup the exact resolution retained for the old physical scan facts
    /// while production callers are moved to `SqlScanSource`.  The result is
    /// still retrieved from this one token store; this helper never acquires a
    /// current connector generation.
    pub fn strict_base_binding(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Option<Arc<QueryTableBinding>> {
        self.binding_for_key(&QueryTableBindingKey::strict_base(
            catalog, namespace, table,
        ))
    }

    /// Return the explicitly admitted Iceberg writer binding for one target.
    /// Read and write bindings for the same physical table are intentionally
    /// distinct: the writer token owns its physical output schema and exact
    /// lease, while scans retain their own selector and materialization.
    pub fn admitted_iceberg_write_binding_id(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<SqlTableBindingId, String> {
        let matches = self
            .captured_bindings()
            .into_iter()
            .filter(|(_, binding)| {
                catalog::materialization_identity_facts(&binding.resolved)
                    .matches(catalog, namespace, table)
                    && binding.write_target_admission.is_some()
            })
            .collect::<Vec<_>>();
        let [binding] = matches.as_slice() else {
            return Err(format!(
                "SQL write target {catalog}.{namespace}.{table} does not have exactly one admitted Iceberg provider preparation"
            ));
        };
        let binding = &binding.1;
        Ok(catalog::table_binding_id(&binding.resolved))
    }

    /// Return the unique Provider-signed preparation admitted for a terminal
    /// write target.  Callers that need more than one shape must use their
    /// explicit preparation instead of allowing target lookup to choose one.
    pub fn admitted_iceberg_write_preparation(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<ConnectorWritePreparation, String> {
        let matches = self
            .captured_bindings()
            .into_iter()
            .filter(|(_, binding)| {
                catalog::materialization_identity_facts(&binding.resolved)
                    .matches(catalog, namespace, table)
                    && binding.write_target_admission.is_some()
            })
            .collect::<Vec<_>>();
        let [(_, binding)] = matches.as_slice() else {
            return Err(format!(
                "SQL write target {catalog}.{namespace}.{table} does not have exactly one admitted Iceberg provider preparation"
            ));
        };
        binding
            .write_target_admission
            .as_ref()
            .map(|admission| admission.preparation.clone())
            .ok_or_else(|| {
                format!(
                    "SQL write target {catalog}.{namespace}.{table} is missing admitted Iceberg provider facts"
                )
            })
    }

    /// Return one explicitly admitted writer binding for its sealed
    /// preparation. This is required when a single terminal operation has
    /// multiple writer shapes for the same physical target.
    pub fn admitted_iceberg_write_binding_id_for_preparation(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        preparation: &ConnectorWritePreparation,
    ) -> Result<SqlTableBindingId, String> {
        let key =
            QueryTableBindingKey::write_target(catalog, namespace, table, preparation.digest());
        let Some(binding) = self.binding_for_key(&key) else {
            return Err(format!(
                "SQL write target {catalog}.{namespace}.{table} was not admitted into this query binding store"
            ));
        };
        match binding.write_target_admission.as_ref() {
            Some(_) => Ok(catalog::table_binding_id(&binding.resolved)),
            _ => Err(format!(
                "SQL write target {catalog}.{namespace}.{table} is missing admitted Iceberg provider facts"
            )),
        }
    }

    /// Return the one admission-frozen MV target binding.  The UUID and
    /// snapshot are part of the lookup key so a recreated target or a later
    /// refresh baseline can never reuse an earlier request's authority.
    pub fn mv_target_binding_id(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        target_table_uuid: &str,
        frozen_snapshot_id: Option<i64>,
    ) -> Option<SqlTableBindingId> {
        let key = QueryTableBindingKey::mv_target(
            catalog,
            namespace,
            table,
            target_table_uuid,
            frozen_snapshot_id,
        );
        self.entries
            .lock()
            .expect("query table binding lock")
            .get(&key)
            .and_then(|entry| entry.as_ref().ok().map(|stored| stored.id))
    }

    fn binding_for_key(&self, key: &QueryTableBindingKey) -> Option<Arc<QueryTableBinding>> {
        let id = self
            .entries
            .lock()
            .expect("query table binding lock")
            .get(key)
            .and_then(|entry| entry.as_ref().ok().map(|stored| stored.id))?;
        self.binding(id).ok()
    }

    #[cfg(any(test, feature = "query-execution-contract-test-support"))]
    pub fn insert_strict_base_binding_for_test(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        binding: QueryTableBinding,
    ) {
        let key = QueryTableBindingKey::strict_base(catalog, namespace, table);
        self.resolve_or_insert(key, || Ok(binding))
            .expect("test binding insertion must allocate a token");
    }

    fn allocate_id(&self) -> Result<SqlTableBindingId, String> {
        self.allocator
            .lock()
            .expect("query table binding allocator lock")
            .allocate()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{QueryTableBinding, QueryTableBindingKey, QueryTableBindingStore};

    fn local_binding() -> QueryTableBinding {
        let mut allocator = novarocks_sql::binding::SqlTableBindingAllocator::try_new(
            NonZeroU64::new(1).expect("test scope"),
        )
        .expect("test allocator");
        local_binding_for(allocator.allocate().expect("test binding"))
    }

    fn local_binding_for(binding: novarocks_sql::binding::SqlTableBindingId) -> QueryTableBinding {
        let resolved = novarocks_sql::planning::catalog::materialize_connector_read_table(
            novarocks_sql::planning::catalog::ConnectorReadTableFacts {
                catalog: "default_catalog".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
                columns: Vec::new(),
                iceberg_row_lineage_metadata_columns: Vec::new(),
                schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                binding,
                selector: novarocks_spi::connector::ConnectorReadSelector::Current,
                planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
            },
        )
        .expect("test catalog facts materialize")
        .into_resolved_table();
        QueryTableBinding::local(resolved, binding)
    }

    #[test]
    fn spi5b_local_binding_cannot_supply_a_connector_planning_lease() {
        let error = match local_binding().admission.exact_planning_lease() {
            Ok(_) => panic!("local SQL bindings cannot admit a connector read"),
            Err(error) => error,
        };

        assert_eq!(error, "query binding has no connector planning lease");
    }

    #[test]
    fn sqlx2_binding_store_memoizes_failure_once_per_request() {
        let store = QueryTableBindingStore::try_new().expect("store");
        let attempts = AtomicUsize::new(0);
        let key = QueryTableBindingKey::strict_base("ICE", "DB", "ORDERS");

        let first = store.resolve_or_insert(key.clone(), || {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err("missing table".to_string())
        });
        let second = store.resolve_or_insert(key, || {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err("must not load twice".to_string())
        });

        assert_eq!(first.unwrap_err(), "missing table");
        assert_eq!(second.unwrap_err(), "missing table");
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn sqlx2_binding_store_rejects_cross_request_tokens_before_submission() {
        let first = QueryTableBindingStore::try_new().expect("first store");
        let second = QueryTableBindingStore::try_new().expect("second store");
        let token = first
            .resolve_or_insert_with_id(
                QueryTableBindingKey::strict_base("ice", "db", "orders"),
                |binding| Ok(local_binding_for(binding)),
            )
            .expect("first request binding");

        assert!(second.binding(token).is_err());
    }

    #[test]
    fn sqlx2_binding_store_reuses_one_exact_binding_only_within_the_request() {
        let first = QueryTableBindingStore::try_new().expect("first store");
        let second = QueryTableBindingStore::try_new().expect("second store");
        let key = QueryTableBindingKey::strict_base("ice", "db", "orders");

        let first_token = first
            .resolve_or_insert(key.clone(), || Ok(local_binding()))
            .expect("first token");
        let repeated_token = first
            .resolve_or_insert(key.clone(), || Err("must not reload".to_string()))
            .expect("repeated token");
        let second_token = second
            .resolve_or_insert(key, || Ok(local_binding()))
            .expect("second token");

        assert_eq!(first_token, repeated_token);
        assert_ne!(first_token, second_token);
        assert_eq!(
            novarocks_sql::planning::catalog::materialization_identity_facts(
                &first.binding(first_token).expect("exact binding").resolved,
            )
            .fqn(),
            "default_catalog.db.orders"
        );
        assert!(second.binding(first_token).is_err());
    }

    #[test]
    fn sqlx2_binding_digest_is_stable_per_request_and_scoped_across_requests() {
        let first = QueryTableBindingStore::try_new().expect("first store");
        let first_key = QueryTableBindingKey::strict_base("ice", "db", "orders");
        first
            .resolve_or_insert(first_key, || Ok(local_binding()))
            .expect("first binding");

        let first_digest = first.stable_digest_material();
        assert_eq!(first_digest, first.stable_digest_material());

        let second = QueryTableBindingStore::try_new().expect("second store");
        let second_key = QueryTableBindingKey::strict_base("ice", "db", "orders");
        second
            .resolve_or_insert(second_key, || Ok(local_binding()))
            .expect("second binding");
        assert_ne!(first_digest, second.stable_digest_material());
    }

    #[test]
    fn sqlx2_binding_loader_receives_the_token_published_by_the_store() {
        let store = QueryTableBindingStore::try_new().expect("store");
        let key = QueryTableBindingKey::strict_base("ice", "db", "orders");
        let observed = std::sync::Mutex::new(None);

        let token = store
            .resolve_or_insert_with_id(key, |id| {
                *observed.lock().expect("observed token lock") = Some(id);
                Ok(local_binding_for(id))
            })
            .expect("binding token");

        assert_eq!(
            *observed.lock().expect("observed token lock"),
            Some(token),
            "the SQL projection must carry the exact token published by admission"
        );
        let binding = store.binding(token).expect("binding stored");
        binding
            .validate_sql_scan_binding(token)
            .expect("materialization carries the published token");
    }

    #[test]
    fn sqlx2_binding_metadata_alias_uses_a_distinct_request_local_token() {
        let store = QueryTableBindingStore::try_new().expect("store");
        let base = store
            .resolve_or_insert(
                QueryTableBindingKey::strict_base("ice", "db", "orders"),
                || Ok(local_binding()),
            )
            .expect("base token");
        let metadata_key = QueryTableBindingKey::metadata(
            "ice",
            "db",
            "orders",
            novarocks_sql::planning::catalog::MetadataTableKind::Snapshots,
        );
        let metadata = store
            .resolve_or_insert(metadata_key.clone(), || Ok(local_binding()))
            .expect("metadata token");
        let repeated = store
            .resolve_or_insert(metadata_key, || {
                Err("must not reload metadata alias".to_string())
            })
            .expect("memoized metadata token");

        assert_ne!(base, metadata);
        assert_eq!(metadata, repeated);
    }

    #[test]
    fn sqlx2_binding_writer_target_is_distinct_from_same_name_scan() {
        let store = QueryTableBindingStore::try_new().expect("store");
        let scan = store
            .resolve_or_insert(
                QueryTableBindingKey::strict_base("ice", "db", "orders"),
                || Ok(local_binding()),
            )
            .expect("scan token");
        let writer_key = QueryTableBindingKey::write_target("ice", "db", "orders", [1; 32]);
        let writer = store
            .resolve_or_insert(writer_key.clone(), || Ok(local_binding()))
            .expect("writer token");
        let repeated = store
            .resolve_or_insert(writer_key, || {
                Err("must not rematerialize writer".to_string())
            })
            .expect("memoized writer token");

        assert_ne!(scan, writer);
        assert_eq!(writer, repeated);
    }

    #[test]
    fn sqlx2_binding_mv_target_reuses_one_frozen_target_only_within_the_request() {
        let first = QueryTableBindingStore::try_new().expect("first store");
        let second = QueryTableBindingStore::try_new().expect("second store");
        let key = QueryTableBindingKey::mv_target(
            "ice",
            "analytics",
            "orders_mv",
            "target-uuid-a",
            Some(42),
        );

        let first_token = first
            .resolve_or_insert(key.clone(), || Ok(local_binding()))
            .expect("first MV target token");
        let repeated_token = first
            .resolve_or_insert(key.clone(), || {
                Err("must not rematerialize target".to_string())
            })
            .expect("memoized MV target token");
        let second_token = second
            .resolve_or_insert(key, || Ok(local_binding()))
            .expect("second MV target token");

        assert_eq!(first_token, repeated_token);
        assert_ne!(first_token, second_token);
        assert_eq!(
            first.mv_target_binding_id("ICE", "ANALYTICS", "ORDERS_MV", "TARGET-UUID-A", Some(42),),
            Some(first_token),
        );
        assert!(second.binding(first_token).is_err());
    }

    #[test]
    fn sqlx2_binding_mv_target_keeps_uuid_and_snapshot_in_the_identity() {
        let store = QueryTableBindingStore::try_new().expect("store");
        let first = store
            .resolve_or_insert(
                QueryTableBindingKey::mv_target(
                    "ice",
                    "analytics",
                    "orders_mv",
                    "target-uuid-a",
                    Some(42),
                ),
                || Ok(local_binding()),
            )
            .expect("first target");
        let recreated = store
            .resolve_or_insert(
                QueryTableBindingKey::mv_target(
                    "ice",
                    "analytics",
                    "orders_mv",
                    "target-uuid-b",
                    Some(42),
                ),
                || Ok(local_binding()),
            )
            .expect("recreated target");
        let later_snapshot = store
            .resolve_or_insert(
                QueryTableBindingKey::mv_target(
                    "ice",
                    "analytics",
                    "orders_mv",
                    "target-uuid-a",
                    Some(43),
                ),
                || Ok(local_binding()),
            )
            .expect("later snapshot target");

        assert_ne!(first, recreated);
        assert_ne!(first, later_snapshot);
    }
}
