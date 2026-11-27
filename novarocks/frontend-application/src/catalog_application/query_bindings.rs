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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::query_catalog::{CatalogResolutionError, CatalogResolutionResult};
use crate::connector::backend::ResolvedTableStatisticsPin;
use novarocks_query_application::api::ExactBindingReceiptStore;
use novarocks_spi::connector::read_stack::{
    ConnectorPreparedSourceRead, ConnectorReadSourceProjection, ConnectorReadSourceSelection,
};
use novarocks_spi::connector::{
    ConnectorChangeWindowFacts, ConnectorControlPlanningPin, ConnectorReadSelector,
    ConnectorTableHandle, ConnectorTableMetadata,
};
use novarocks_sql::binding::{SqlTableBindingAllocator, SqlTableBindingId, SqlTableBindingScopeId};
use novarocks_sql::planning::catalog::{
    self, MetadataTableKind as SqlMetadataTableKind, ResolvedAnalyzerTable,
};

/// Canonical request-local binding identity. The name is normalized before
/// any binding or typed object outcome is retained.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct QueryTableBindingKey {
    catalog: String,
    namespace: String,
    table: String,
    selector: QueryTableBindingSelector,
}

/// Which admission-frozen input a scan occurrence reads.
///
/// There is deliberately no timestamp here. A timestamp names a moment, not an
/// input, and turning one into an input is admission's job: it is the only
/// place that holds the snapshot log the answer comes from. By the time a read
/// is selected the moment has already become a snapshot, so nothing downstream
/// can ask a provider to resolve a timestamp again - it has nothing to resolve
/// it from, and a provider that tried would be re-deciding a pin this query
/// already froze, against a table that may have moved since.
///
/// Change windows and metadata relations are selected through their own
/// admitted carriers, which hold different provider material; they are not
/// variants of this.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueryFrozenReadInput {
    /// The table as admission froze it for this request.
    Current,
    /// One historical snapshot admission captured for this request.
    Snapshot(i64),
    /// Exactly the files a provider pinned for one cohort.
    PinnedFileSet,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum QueryTableBindingSelector {
    StrictBaseTable,
    /// One logical write target of the query's write session. It remains
    /// separate from a read binding for the same physical table because the
    /// writer's frozen physical schema may include hidden lineage or MV state
    /// columns that a scan does not expose. Within a session the ordinal is
    /// dense and unique, so it is what tells two branches of the same physical
    /// table apart.
    SessionWriteTarget(u32),
    Snapshot(i64),
    Metadata(SqlMetadataTableKind),
    QualifiedMetadata {
        kind: SqlMetadataTableKind,
        binding: novarocks_spi::connector::ConnectorProviderBindingKey,
    },
    /// One frozen materialized-view target.  The target UUID distinguishes a
    /// recreated table at the same name, while the snapshot keeps target-state
    /// and target-locator scans on the exact refresh baseline.
    MvTarget {
        target_table_uuid: String,
        frozen_snapshot_id: Option<i64>,
    },
}

impl QueryTableBindingKey {
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
    pub fn session_write_target(
        catalog: &str,
        namespace: &str,
        table: &str,
        ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    ) -> Self {
        Self::new(
            catalog,
            namespace,
            table,
            QueryTableBindingSelector::SessionWriteTarget(ordinal.get()),
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

    pub(super) fn metadata_with_qualification(
        catalog: &str,
        namespace: &str,
        table: &str,
        kind: SqlMetadataTableKind,
        binding: Option<&novarocks_spi::connector::ConnectorProviderBindingKey>,
    ) -> Self {
        match binding {
            Some(binding) => Self::new(
                catalog,
                namespace,
                table,
                QueryTableBindingSelector::QualifiedMetadata {
                    kind,
                    binding: binding.clone(),
                },
            ),
            None => Self::metadata(catalog, namespace, table, kind),
        }
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

#[derive(Clone)]
pub enum QueryTableBindingAdmission {
    /// Local SQL tables do not own a connector generation and cannot be used
    /// as a connector read or write admission source.
    Local,
    /// A connector-owned table admission retains the exact generation that
    /// admitted its read materialization or terminal write target.
    Exact(ConnectorControlPlanningPin),
    /// The ordinary Current data read of a table observed by this statement.
    OriginalTableObservation(ConnectorControlPlanningPin, Option<String>),
    /// One statement's signed mutation target scan and admitted base.
    RowMutationMatch(
        ConnectorControlPlanningPin,
        Option<i64>,
        novarocks_spi::connector::ConnectorRowMutationPreparationIdentity,
        [u8; 32],
    ),
    /// An ordinary Current source sharing the admitted mutation table and base.
    RowMutationCurrent(
        ConnectorControlPlanningPin,
        novarocks_spi::connector::ConnectorRowMutationCurrent,
    ),
    /// A provider-frozen read has no independent table-selector materialization.
    /// Its source facts remain opaque in the provider request, while this
    /// retained lease fences the synthetic SQL relation to one exact control
    /// generation.
    FrozenRead(ConnectorControlPlanningPin),
}

impl QueryTableBindingAdmission {
    pub fn exact_planning_pin(&self) -> Result<ConnectorControlPlanningPin, String> {
        match self {
            Self::Exact(lease)
            | Self::OriginalTableObservation(lease, _)
            | Self::FrozenRead(lease)
            | Self::RowMutationMatch(lease, ..)
            | Self::RowMutationCurrent(lease, _) => Ok(lease.clone()),
            Self::Local => Err("query binding has no connector planning pin".to_string()),
        }
    }
}

/// A relation this request admitted as a provider-frozen cohort rather than
/// as a version of a table.
///
/// The cohort *is* the read. There is no name and no version a later lookup
/// could recover it from -- the provider minted it for this request and will
/// not mint it again -- so it is admitted here, beside the binding that names
/// it, rather than reconstructed when the read is frozen.
#[derive(Clone, Debug)]
pub enum QueryFrozenCohortRead {
    PinnedFileSet(crate::query_execution::cohort_read::QueryPinnedFileSetRead),
}

impl QueryFrozenCohortRead {
    /// The generation this cohort was frozen against.
    pub fn planning_pin(&self) -> &ConnectorControlPlanningPin {
        match self {
            Self::PinnedFileSet(read) => &read.planning_pin,
        }
    }

    /// Which admitted input this cohort answers for.
    pub const fn frozen_read_input(&self) -> QueryFrozenReadInput {
        match self {
            Self::PinnedFileSet(_) => QueryFrozenReadInput::PinnedFileSet,
        }
    }
}

#[derive(Clone)]
pub struct QueryTableBinding {
    pub resolved: ResolvedAnalyzerTable,
    pub statistics_pin: Option<ResolvedTableStatisticsPin>,
    pub admission: QueryTableBindingAdmission,
    /// The exact provider metadata that SQL analysis used for a connector
    /// base relation. Derived selectors and scan preparation consume its
    /// original provider provenance through the paired planning pin.
    pub source_metadata: Option<ConnectorTableMetadata>,
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
    /// The provider-frozen cohort this binding names, when it names one
    /// rather than a version of a table.
    pub frozen_cohort_read: Option<QueryFrozenCohortRead>,
    /// Exact file sets captured for the snapshot selectors emitted by one
    /// admitted logical plan.  A binding can serve both sides of an IMV
    /// from/to comparison, so the primary materialization alone is not
    /// sufficient to recover a historical scan at preparation time.
    ///
    /// The map is query-local and shares the binding's planning pin; it is
    /// never populated by a later catalog lookup.
    pub frozen_snapshot_materializations: BTreeMap<i64, QueryScanMaterialization>,
    /// Completed source preparations selected for this binding. A nonempty
    /// inventory forbids generic reopening for every data/window selector.
    pub prepared_source_reads: BTreeMap<QueryPreparedSourceSelector, Arc<QueryPreparedSourceRead>>,
}

/// Exact query-local selector; the receipt remains the actual Provider authority.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum QueryPreparedSourceSelector {
    Snapshot(i64),
    Delta {
        from_exclusive: i64,
        to_inclusive: i64,
    },
}

#[derive(Clone)]
pub struct QueryPreparedSourceRead {
    pub(crate) receipt: ConnectorPreparedSourceRead,
    pub(crate) projection: ConnectorReadSourceProjection,
    pub(crate) planning: ConnectorControlPlanningPin,
}

fn prepared_source_selector(
    selection: ConnectorReadSourceSelection,
    projection: ConnectorReadSourceProjection,
    window_facts: Option<&ConnectorChangeWindowFacts>,
) -> Result<QueryPreparedSourceSelector, String> {
    use ConnectorReadSourceProjection as P;
    use ConnectorReadSourceSelection as S;
    if projection != P::To && !window_facts.is_some_and(|facts| facts.permits(projection)) {
        return Err("prepared source does not authorize a From or Delta projection".into());
    }
    match (selection, projection) {
        (S::Snapshot(id) | S::CurrentAtSnapshot(id), P::To) => {
            Ok(QueryPreparedSourceSelector::Snapshot(id))
        }
        (S::ChangeWindow(window), P::To) => Ok(QueryPreparedSourceSelector::Snapshot(
            window.to_snapshot_id(),
        )),
        (S::ChangeWindow(window), P::From) => Ok(QueryPreparedSourceSelector::Snapshot(
            window.from_snapshot_id(),
        )),
        (S::ChangeWindow(window), P::Delta) => Ok(QueryPreparedSourceSelector::Delta {
            from_exclusive: window.from_snapshot_id(),
            to_inclusive: window.to_snapshot_id(),
        }),
        _ => Err("snapshot preparation cannot supply a From or Delta projection".into()),
    }
}

/// One admitted terminal write target retained beside the SQL binding token.
/// Field identity and input shape are sealed by the provider; SQL may project
/// the Arrow layout and field tokens but must not reconstruct table-format
/// metadata.
#[derive(Clone)]
pub struct QueryWriteTargetAdmission {
    /// What SQL is allowed to see: the Arrow layout and field tokens of this
    /// target's input rows, as the write session sealed them for the handle
    /// the plan's writer carries.
    pub input: novarocks_spi::connector::ConnectorWriteInputShape,
    pub layout: novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference,
}

/// Exact provider scan facts retained after admission.  The concrete Iceberg
/// representation is temporary only at this application boundary while SQL
/// callers are migrated to `SqlScanSource`; preparation must obtain it by the
/// request-local binding token rather than from a planner table.
#[derive(Clone)]
pub struct QueryScanMaterialization {
    /// Provider-neutral scan authority. The handle is opaque to Core; the
    /// lease keeps the exact control generation alive until native preparation.
    pub table: ConnectorTableHandle,
    pub selector: ConnectorReadSelector,
    /// Provider-owned exact file selection for an affected MV target read.
    pub mv_partition_selection:
        Option<novarocks_spi::connector::read_stack::ConnectorMvTargetPartitionSelection>,
    pub planning_pin: ConnectorControlPlanningPin,
}

#[derive(Clone)]
pub struct MvTargetReadAdmission {
    pub full: QueryScanMaterialization,
    pub affected_partitions: QueryScanMaterialization,
    pub target_table_uuid: String,
    pub target_object_id: novarocks_spi::connector::ConnectorTableObjectId,
    pub frozen_snapshot_id: Option<i64>,
    pub original_source: Option<novarocks_spi::connector::ConnectorProcessLocalPayload>,
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
            source_metadata: None,
            scan_materialization: None,
            mv_target_read: None,
            write_target_admission: None,
            frozen_cohort_read: None,
            frozen_snapshot_materializations: BTreeMap::new(),
            prepared_source_reads: BTreeMap::new(),
        }
    }

    /// Install only a projection of the same original To metadata/generation.
    pub fn install_prepared_source_read(
        &mut self,
        receipt: ConnectorPreparedSourceRead,
        projection: ConnectorReadSourceProjection,
        planning: ConnectorControlPlanningPin,
    ) -> Result<(), String> {
        let admitted = self.admission.exact_planning_pin()?;
        receipt
            .request()
            .observed_source_facts()
            .validate_owner(&novarocks_spi::connector::ConnectorProviderBindingKey {
                instance_id: planning.binding().descriptor().instance_id.clone(),
                incarnation: planning.binding().incarnation(),
            })
            .map_err(|error| error.to_string())?;
        let metadata = self
            .source_metadata
            .as_ref()
            .ok_or("prepared source binding has no original To metadata")?;
        if !Arc::ptr_eq(admitted.binding(), planning.binding())
            || receipt.binding().descriptor() != planning.binding().descriptor()
            || receipt.binding().catalog_handle()
                != planning
                    .binding()
                    .catalog_handle()
                    .map_err(|e| e.to_string())?
            || metadata.table != receipt.to_metadata().table
            || metadata.identity != receipt.to_metadata().identity
            || metadata.version != receipt.to_metadata().version
        {
            return Err(
                "prepared source differs from its query binding's original To materialization"
                    .into(),
            );
        }
        let to_snapshot = receipt.request().selection().to_snapshot_id();
        let primary = self
            .scan_materialization
            .as_ref()
            .ok_or("prepared source has no primary To materialization")?;
        if primary.selector != ConnectorReadSelector::SnapshotId(to_snapshot)
            || primary.table != receipt.to_metadata().table
            || !Arc::ptr_eq(primary.planning_pin.binding(), planning.binding())
        {
            return Err(
                "prepared source primary materialization is not its original Snapshot(To)".into(),
            );
        }
        if self.prepared_source_reads.values().any(|original| {
            original.receipt.request().object_id() != receipt.request().object_id()
                || original.receipt.request().to_revision() != receipt.request().to_revision()
                || original.receipt.request().observed_table() != receipt.request().observed_table()
        }) {
            return Err(
                "query binding cannot mix prepared source objects or original To pins".into(),
            );
        }
        let key = prepared_source_selector(
            receipt.request().selection(),
            projection,
            receipt.window_facts(),
        )?;
        if self.prepared_source_reads.contains_key(&key) {
            return Err("prepared source selector was installed twice".into());
        }
        self.prepared_source_reads.insert(
            key,
            Arc::new(QueryPreparedSourceRead {
                receipt,
                projection,
                planning,
            }),
        );
        Ok(())
    }

    pub(crate) fn prepared_source_read(
        &self,
        input: QueryFrozenReadInput,
    ) -> Result<Option<Arc<QueryPreparedSourceRead>>, String> {
        if self.prepared_source_reads.is_empty() {
            return Ok(None);
        }
        let id = match input {
            QueryFrozenReadInput::Current => match self
                .scan_materialization
                .as_ref()
                .map(|value| value.selector)
            {
                Some(ConnectorReadSelector::SnapshotId(id)) => id,
                _ => {
                    return Err(
                        "prepared source Current has no primary Snapshot(To) materialization"
                            .into(),
                    );
                }
            },
            QueryFrozenReadInput::Snapshot(id) => id,
            _ => return Err("prepared source data lookup cannot use a cohort selector".into()),
        };
        self.prepared_source_reads
            .get(&QueryPreparedSourceSelector::Snapshot(id))
            .cloned()
            .map(Some)
            .ok_or_else(|| format!("prepared source snapshot {id} has no exact original receipt"))
    }

    pub(crate) fn prepared_change_window(
        &self,
        from_exclusive: i64,
        to_inclusive: i64,
    ) -> Result<Option<Arc<QueryPreparedSourceRead>>, String> {
        // Ordinary explicit delta TVFs keep their original typed preparation.
        // Once this binding has prepared facts, no missing window may reopen.
        if self.prepared_source_reads.is_empty() {
            return Ok(None);
        }
        self.prepared_source_reads.get(&QueryPreparedSourceSelector::Delta { from_exclusive, to_inclusive }).cloned().map(Some)
            .ok_or_else(|| format!("prepared source window ({from_exclusive}, {to_inclusive}] has no exact original receipt"))
    }

    /// Verify that the application materializer paired this table with the
    /// token it reserved.  Concrete provider scans are never accepted here:
    /// they must already live in `scan_materialization` before SQL receives
    /// the table facts.
    pub fn validate_sql_scan_binding(&self, binding: SqlTableBindingId) -> Result<(), String> {
        catalog::validate_materialization_binding(&self.resolved, binding)
    }
}

struct StoredBinding {
    id: SqlTableBindingId,
}

/// A relation request is distinct from the canonical scan input it may select.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct QueryRelationRequestKey {
    relation: novarocks_types::naming::TableIdentity,
    selection: Option<novarocks_sql::planning::catalog::SqlTimeTravelSelection>,
}
impl QueryRelationRequestKey {
    pub(crate) fn from_need(
        need: &novarocks_sql::compiler::CatalogRelationNeed,
    ) -> Result<Self, novarocks_sql::compiler::SqlCompileError> {
        let selection = need
            .version()
            .map(|version| {
                novarocks_sql::planning::catalog::parse_time_travel_selection(
                    version,
                    &need.relation().fqn(),
                )
            })
            .transpose()?;
        Ok(Self {
            relation: need.relation().clone(),
            selection,
        })
    }
}

#[derive(Clone)]
pub(crate) enum MemoizedQueryRelation {
    Table {
        binding: SqlTableBindingId,
        selection: Option<novarocks_sql::planning::catalog::SqlTimeTravelSnapshotBindingFacts>,
    },
    View(crate::catalog_application::query_catalog::relation::ConnectorQueryViewMaterialization),
    SelectionRefused(Arc<novarocks_sql::compiler::CatalogRelationFact>),
}

/// Exact application authority paired with one compiler request.
pub struct QueryTableBindingStore {
    allocator: Mutex<SqlTableBindingAllocator>,
    exact_binding_receipts: ExactBindingReceiptStore,
    entries: Mutex<HashMap<QueryTableBindingKey, CatalogResolutionResult<StoredBinding>>>,
    by_id: Mutex<HashMap<SqlTableBindingId, Arc<QueryTableBinding>>>,
    qualifications: Mutex<super::quoted_metadata_qualification::CatalogQualificationBook>,
    pub(super) observed_relations:
        tokio::sync::Mutex<super::query_catalog::relation::StatementRelationObservations>,
    catalog_observations_sealed: AtomicBool,
    local_views: Mutex<
        Option<
            Result<
                novarocks_query_application::view::LocalViewSnapshot,
                novarocks_sql::compiler::SqlCompileError,
            >,
        >,
    >,
    pub(crate) relations: Mutex<
        HashMap<
            QueryRelationRequestKey,
            Result<
                MemoizedQueryRelation,
                crate::catalog_application::query_catalog::CatalogResolutionError,
            >,
        >,
    >,
    /// Once a statement enters a topology-only replan, an unknown lookup must
    /// not materialize a newer catalog/connector `Current` state. Existing
    /// request-local bindings remain readable and keep their exact leases.
    #[cfg(test)]
    sealed_for_topology_replan: AtomicBool,
}

pub(crate) struct FrozenStatementViews {
    local: novarocks_query_application::view::LocalViewSnapshot,
    external: Vec<(
        QueryTableBindingKey,
        super::query_catalog::relation::ConnectorQueryViewMaterialization,
    )>,
}

impl QueryTableBindingStore {
    /// Allocate one fresh process-local scope.  Scope exhaustion is explicit
    /// rather than silently reusing a token from another query.
    pub fn try_new() -> Result<Self, String> {
        let allocator = SqlTableBindingAllocator::new_unique()?;
        let exact_binding_receipts = ExactBindingReceiptStore::new(&allocator);
        Ok(Self {
            allocator: Mutex::new(allocator),
            exact_binding_receipts,
            entries: Mutex::new(HashMap::new()),
            by_id: Mutex::new(HashMap::new()),
            relations: Mutex::new(HashMap::new()),
            qualifications: Mutex::new(Default::default()),
            observed_relations: tokio::sync::Mutex::new(Default::default()),
            catalog_observations_sealed: AtomicBool::new(false),
            local_views: Mutex::new(None),
            #[cfg(test)]
            sealed_for_topology_replan: AtomicBool::new(false),
        })
    }

    /// Exercise the topology-replan rule that a sealed request reuses its
    /// admitted bindings and rejects new catalog or connector state.
    #[cfg(test)]
    pub fn seal_for_topology_replan(&self) {
        // Serialize with `resolve_or_insert_with_id`, which keeps `entries`
        // locked across the one permitted first-load callback.
        let _entries = self.entries.lock().expect("query table binding lock");
        self.exact_binding_receipts.seal();
        self.sealed_for_topology_replan
            .store(true, Ordering::Release);
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
            if let QueryTableBindingAdmission::Exact(lease)
            | QueryTableBindingAdmission::OriginalTableObservation(lease, _)
            | QueryTableBindingAdmission::FrozenRead(lease)
            | QueryTableBindingAdmission::RowMutationMatch(lease, ..)
            | QueryTableBindingAdmission::RowMutationCurrent(lease, _) = &binding.admission
            {
                let descriptor = lease.binding().descriptor();
                material.extend_from_slice(descriptor.provider_id.as_str().as_bytes());
                material.push(0);
                material.extend_from_slice(descriptor.instance_id.as_str().as_bytes());
                material.push(0);
                // Prepared-artifact identity is fenced by the FE-local
                // control runtime, never by the BE legacy execution key.
                material.extend_from_slice(&lease.control_runtime_id().to_bytes());
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

    /// Observe only the SQL prepass's demand list, once per catalog in this
    /// binding scope. The compiler receives provider and binding identities,
    /// while the original runtime observation remains application-owned.
    pub(crate) fn observe_catalog_qualifications(
        &self,
        candidates: &[novarocks_spi::connector::ConnectorInstanceId],
        catalogs: Option<&dyn novarocks_catalog_application::CatalogApplicationPort>,
    ) -> Result<
        novarocks_sql::compiler::CatalogQualifications,
        novarocks_sql::compiler::SqlCompileError,
    > {
        use super::quoted_metadata_qualification::CatalogQualificationObservation;
        use novarocks_sql::compiler::{
            CatalogQualification, CatalogQualifications, SqlCompileError,
        };

        let mut result = CatalogQualifications::default();
        if candidates.is_empty() {
            return Ok(result);
        }
        let mut original = self.qualifications.lock().map_err(|_| {
            SqlCompileError::Compilation(
                "catalog qualification observation lock is poisoned".into(),
            )
        })?;
        for instance in candidates {
            if !original.contains(instance) {
                self.require_catalog_observation_allowed()
                    .map_err(SqlCompileError::Compilation)?;
            }
            let observation = original
                .observe(instance, catalogs)
                .map_err(super::query_catalog::catalog_application_resolution_error)
                .map_err(super::query_catalog::CatalogResolutionError::into_compile)?;
            let qualification = match observation {
                CatalogQualificationObservation::Absent => CatalogQualification::Absent,
                CatalogQualificationObservation::Unavailable => CatalogQualification::Unavailable,
                CatalogQualificationObservation::Ready(read) => read.sql_qualification(),
            };
            result.record(instance.clone(), qualification)?;
        }
        Ok(result)
    }

    pub(super) fn qualified_catalog_read(
        &self,
        binding: &novarocks_spi::connector::ConnectorProviderBindingKey,
    ) -> super::query_catalog::CatalogResolutionResult<
        super::quoted_metadata_qualification::QualifiedCatalogRead,
    > {
        self.qualifications
            .lock()
            .map_err(|_| {
                super::query_catalog::CatalogResolutionError::failed(
                    "catalog qualification observation lock is poisoned",
                )
            })?
            .selected_read(binding)
            .cloned()
            .map_err(super::query_catalog::catalog_application_resolution_error)
    }

    pub(crate) fn capture_local_views(
        &self,
        capture: impl FnOnce() -> Result<novarocks_query_application::view::LocalViewSnapshot, String>,
    ) -> Result<
        novarocks_query_application::view::LocalViewSnapshot,
        novarocks_sql::compiler::SqlCompileError,
    > {
        let mut original = self
            .local_views
            .lock()
            .expect("statement local view snapshot lock");
        original
            .get_or_insert_with(|| {
                capture().map_err(novarocks_sql::compiler::SqlCompileError::Compilation)
            })
            .clone()
    }

    pub(crate) fn require_catalog_observation_allowed(&self) -> Result<(), String> {
        if self.catalog_observations_sealed.load(Ordering::Acquire) {
            return Err("statement catalog observation is sealed".into());
        }
        if self.exact_binding_receipts.sealed_view().is_some() {
            return Err("topology replan attempted to observe a relation absent from its admitted catalog facts".into());
        }
        Ok(())
    }

    /// Close catalog capture after the owner's final round. An already
    /// admitted write session can still register its write-target binding.
    pub(crate) async fn seal_catalog_observations(&self) {
        let _observations = self.observed_relations.lock().await;
        self.catalog_observations_sealed
            .store(true, Ordering::Release);
    }

    /// Observe a completed binding without starting another provider load.
    /// This is not a token mint or a permission to materialize a new relation.
    pub(crate) fn resolved_lookup(
        &self,
        key: &QueryTableBindingKey,
    ) -> Option<CatalogResolutionResult<SqlTableBindingId>> {
        self.entries
            .lock()
            .expect("query table binding lock")
            .get(key)
            .map(|entry| entry.as_ref().map(|stored| stored.id).map_err(Clone::clone))
    }

    pub(crate) async fn rewrite_view_definitions(
        &self,
        requested: &[novarocks_types::naming::TableIdentity],
    ) -> Vec<Arc<novarocks_sql::planning::catalog::SqlViewDefinition>> {
        let mut entries = self.observed_relations.lock().await.view_definitions();
        if let Some(Ok(local)) = self
            .local_views
            .lock()
            .expect("original local view snapshot")
            .as_ref()
        {
            entries.extend(
                requested
                    .iter()
                    .filter_map(|relation| local.get(relation).cloned()),
            );
        }
        entries
    }

    pub(crate) async fn freeze_statement_views(&self) -> Result<FrozenStatementViews, String> {
        let local = self
            .local_views
            .lock()
            .expect("original local view snapshot")
            .as_ref()
            .ok_or("MV analysis has no original local view snapshot")?
            .as_ref()
            .map_err(ToString::to_string)?
            .clone();
        Ok(FrozenStatementViews {
            local,
            external: self.observed_relations.lock().await.frozen_views(),
        })
    }

    pub(crate) async fn retain_statement_views(&self, views: &FrozenStatementViews) {
        self.local_views
            .lock()
            .expect("original local view snapshot")
            .get_or_insert_with(|| Ok(views.local.clone()));
        self.observed_relations
            .lock()
            .await
            .retain_frozen_views(&views.external);
    }

    /// Reuse a completed binding or a typed object outcome.
    #[cfg(test)]
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
    /// store. An object failure is memoized without publishing its token.
    pub fn resolve_or_insert_with_id(
        &self,
        key: QueryTableBindingKey,
        load: impl FnOnce(SqlTableBindingId) -> Result<QueryTableBinding, String>,
    ) -> Result<SqlTableBindingId, String> {
        self.resolve_catalog_binding(key, |id| load(id).map_err(CatalogResolutionError::failed))
            .map_err(CatalogResolutionError::into_message)
    }

    pub(super) fn resolve_catalog_binding(
        &self,
        key: QueryTableBindingKey,
        load: impl FnOnce(SqlTableBindingId) -> CatalogResolutionResult<QueryTableBinding>,
    ) -> CatalogResolutionResult<SqlTableBindingId> {
        let mut entries = self.entries.lock().expect("query table binding lock");
        if let Some(entry) = entries.get(&key) {
            return entry.as_ref().map(|stored| stored.id).map_err(Clone::clone);
        }
        #[cfg(test)]
        if self.sealed_for_topology_replan.load(Ordering::Acquire) {
            return Err(CatalogResolutionError::failed(
                "topology replan attempted to materialize a binding absent from the first admitted semantic snapshot",
            ));
        }

        let result = self
            .allocate_id()
            .map_err(CatalogResolutionError::failed)
            .and_then(|id| {
                load(id).and_then(|binding| {
                    if let Some(materialization) = &binding.scan_materialization {
                        let identity = catalog::materialization_identity_facts(&binding.resolved);
                        self.exact_binding_receipts
                            .register_connector_binding(
                                id,
                                [identity.catalog(), identity.namespace(), identity.table()],
                                &materialization.planning_pin,
                                &materialization.table,
                                materialization.selector,
                            )
                            .map_err(CatalogResolutionError::failed)?;
                    } else if let QueryTableBindingAdmission::FrozenRead(lease) = &binding.admission
                    {
                        let identity = catalog::materialization_identity_facts(&binding.resolved);
                        self.exact_binding_receipts
                            .register_frozen_connector_binding(
                                id,
                                [identity.catalog(), identity.namespace(), identity.table()],
                                lease,
                            )
                            .map_err(CatalogResolutionError::failed)?;
                    }
                    let binding = Arc::new(binding);
                    self.by_id
                        .lock()
                        .expect("query table binding by-id lock")
                        .insert(id, Arc::clone(&binding));
                    Ok(StoredBinding { id })
                })
            });
        let response = result
            .as_ref()
            .map(|stored| stored.id)
            .map_err(Clone::clone);
        if result
            .as_ref()
            .err()
            .is_none_or(CatalogResolutionError::is_object_outcome)
        {
            entries.insert(key, result);
        }
        response
    }

    /// Install the original target observation before completing ordinary Current reads.
    pub(crate) fn register_row_mutation_current(
        &self,
        original: &novarocks_spi::connector::ConnectorTableMetadata,
        planning_pin: &ConnectorControlPlanningPin,
        preparation: &novarocks_spi::connector::ConnectorRowMutationPreparation,
    ) -> Result<(), String> {
        let source = preparation
            .current_source(original)
            .map_err(|error| error.to_string())?;
        let control = planning_pin.binding();
        if control.descriptor().instance_id != preparation.owner().instance_id
            || control.incarnation() != preparation.owner().incarnation
        {
            return Err("row mutation Current read has another planning generation".into());
        }
        let identity = source.identity();
        let catalog = identity.instance_id.as_str();
        let namespace = identity.namespace.as_ref();
        let table = identity.table.as_ref();
        let key = QueryTableBindingKey::strict_base(catalog, namespace, table);
        self.resolve_or_insert_with_id(key, |id| {
            let mut materialization =
                super::query_catalog::connector_table_materialization_from_metadata(
                    original.clone(),
                    planning_pin.clone(),
                )?;
            materialization.statistics_pin = control
                .statistics()
                .map(|statistics| {
                    statistics
                        .row_mutation_statistics_version(&source)
                        .map(
                            |data_version| crate::connector::backend::ResolvedTableStatisticsPin {
                                table: preparation.match_source().clone(),
                                data_version,
                            },
                        )
                })
                .transpose()
                .map_err(|error| error.to_string())?;
            let mut binding =
                super::query_materializer::connector_query_binding_from_materialization(
                    materialization,
                    catalog,
                    namespace,
                    table,
                    id,
                )?;
            binding.admission = QueryTableBindingAdmission::RowMutationCurrent(
                planning_pin.clone(),
                source.clone(),
            );
            Ok(binding)
        })?;
        Ok(())
    }

    /// The one original rewritten target, already installed during catalog
    /// completion. The returned token carries no relation or predicate facts.
    pub(crate) fn row_mutation_match_binding(&self) -> Result<Option<SqlTableBindingId>, String> {
        let bindings = self.by_id.lock().expect("query table binding by-id lock");
        let mut matches = bindings.iter().filter_map(|(id, binding)| {
            matches!(
                binding.admission,
                QueryTableBindingAdmission::RowMutationMatch(..)
            )
            .then_some(*id)
        });
        let target = matches.next();
        if matches.next().is_some() {
            return Err("statement admitted more than one row mutation match target".into());
        }
        Ok(target)
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

    /// Recover provider scan facts only through the exact request-local token.
    /// A missing materialization is a submission-time contract failure, not a
    /// reason to resolve a current table or connector generation.
    #[cfg(test)]
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
    /// The admission-frozen input one scan occurrence reads.
    ///
    /// Selecting an input is a value-only lookup: it names which frozen read
    /// to use and carries no capability of its own.
    pub fn frozen_read_input(
        &self,
        id: SqlTableBindingId,
        input: QueryFrozenReadInput,
    ) -> Result<QueryScanMaterialization, String> {
        let binding = self.binding(id)?;
        match input {
            QueryFrozenReadInput::Current => binding.scan_materialization.clone().ok_or_else(|| {
                "SQL scan has no admitted connector materialization for its request-local binding"
                    .to_string()
            }),
            QueryFrozenReadInput::Snapshot(snapshot_id) => binding
                .frozen_snapshot_materializations
                .get(&snapshot_id)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "SQL frozen snapshot {snapshot_id} has no admitted connector materialization for its request-local binding"
                    )
                }),
            // A cohort is not a materialization of a relation: it names files
            // or a group directly, so there is nothing here to resolve it to.
            // Its carrier is read through `frozen_cohort_read` instead.
            QueryFrozenReadInput::PinnedFileSet => Err(
                "a provider-frozen cohort is read through its admitted carrier, not through a relation materialization"
                    .to_string(),
            ),
        }
    }

    /// The provider-frozen cohort admitted for this binding.
    pub fn frozen_cohort_read(
        &self,
        id: SqlTableBindingId,
        input: QueryFrozenReadInput,
    ) -> Result<QueryFrozenCohortRead, String> {
        let binding = self.binding(id)?;
        let cohort = binding.frozen_cohort_read.clone().ok_or_else(|| {
            "SQL frozen cohort read has no admitted carrier for its request-local binding"
                .to_string()
        })?;
        if cohort.frozen_read_input() != input {
            return Err(
                "SQL frozen cohort read names a different relation family than its admitted carrier"
                    .to_string(),
            );
        }
        Ok(cohort)
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

    /// Return the one admission-frozen MV target binding.  The UUID and
    /// snapshot are part of the lookup key so a recreated target or a later
    /// refresh baseline can never reuse an earlier request's authority.
    #[cfg(test)]
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

    #[cfg(test)]
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

    use super::{
        ConnectorReadSourceProjection, ConnectorReadSourceSelection, QueryPreparedSourceSelector,
        QueryTableBinding, QueryTableBindingKey, QueryTableBindingStore, prepared_source_selector,
    };

    #[test]
    fn local_view_capture_keeps_the_original_statement_snapshot_and_failure() {
        use novarocks_query_application::view::{QueryViewService, ViewService};

        let store = QueryTableBindingStore::try_new().unwrap();
        let service = QueryViewService::new();
        let captures = AtomicUsize::new(0);
        store
            .capture_local_views(|| {
                captures.fetch_add(1, Ordering::Relaxed);
                service.catalog_snapshot()
            })
            .unwrap();
        store
            .capture_local_views(|| {
                captures.fetch_add(1, Ordering::Relaxed);
                Err("another observation must not replace the original snapshot".into())
            })
            .unwrap();
        assert_eq!(captures.load(Ordering::Relaxed), 1);

        let failed = QueryTableBindingStore::try_new().unwrap();
        let original = failed
            .capture_local_views(|| Err("original registry failure".into()))
            .unwrap_err();
        let repeated = failed
            .capture_local_views(|| service.catalog_snapshot())
            .unwrap_err();
        assert_eq!(original.to_string(), repeated.to_string());
        assert!(repeated.to_string().contains("original registry failure"));
    }

    fn local_binding() -> QueryTableBinding {
        let mut allocator = novarocks_sql::binding::SqlTableBindingAllocator::try_new_for_test(
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
    fn prepared_source_selectors_keep_exact_endpoints_and_original_admission() {
        use ConnectorReadSourceProjection as P;
        use ConnectorReadSourceSelection as S;
        use novarocks_spi::connector::read_stack::ConnectorReadChangeWindow;
        use novarocks_spi::connector::{
            ConnectorChangeWindowFacts as A, ConnectorChangeWindowNegativeWeight,
            ConnectorChangeWindowNetZeroBasis, ConnectorChangeWindowNotIncrementalReason,
            ConnectorChangeWindowPartitionImpact as I, ConnectorChangeWindowVolume,
        };
        let window = S::ChangeWindow(ConnectorReadChangeWindow::new(7, 19));
        let difference = A::Difference {
            negative_weight: ConnectorChangeWindowNegativeWeight::Absent,
            partition_impact: I::Unpartitioned,
            volume: ConnectorChangeWindowVolume {
                source_event_rows_upper_bound: 1,
                source_data_file_bytes: 128,
            },
        };
        let net_zero = A::NetZero(ConnectorChangeWindowNetZeroBasis::ReplaceOnly);
        for selection in [S::Snapshot(19), S::CurrentAtSnapshot(19)] {
            assert_eq!(
                prepared_source_selector(selection, P::To, None).unwrap(),
                QueryPreparedSourceSelector::Snapshot(19)
            );
            assert!(prepared_source_selector(selection, P::From, None).is_err());
            assert!(prepared_source_selector(selection, P::Delta, None).is_err());
        }
        assert_eq!(
            prepared_source_selector(window, P::To, Some(&difference)).unwrap(),
            QueryPreparedSourceSelector::Snapshot(19)
        );
        assert_eq!(
            prepared_source_selector(window, P::From, Some(&difference)).unwrap(),
            QueryPreparedSourceSelector::Snapshot(7)
        );
        assert_eq!(
            prepared_source_selector(window, P::Delta, Some(&difference)).unwrap(),
            QueryPreparedSourceSelector::Delta {
                from_exclusive: 7,
                to_inclusive: 19
            }
        );
        assert_eq!(
            prepared_source_selector(window, P::To, Some(&net_zero)).unwrap(),
            QueryPreparedSourceSelector::Snapshot(19)
        );
        let full = A::NotIncremental(
            ConnectorChangeWindowNotIncrementalReason::SnapshotAncestryBroken {
                from_snapshot_id: 7,
            },
        );
        assert!(prepared_source_selector(window, P::From, Some(&net_zero)).is_err());
        assert_eq!(
            prepared_source_selector(window, P::Delta, Some(&net_zero)).unwrap(),
            QueryPreparedSourceSelector::Delta {
                from_exclusive: 7,
                to_inclusive: 19
            }
        );
        assert!(prepared_source_selector(window, P::From, Some(&full)).is_err());
        assert!(prepared_source_selector(window, P::Delta, Some(&full)).is_err());
        assert!(prepared_source_selector(S::Snapshot(19), P::Delta, Some(&net_zero)).is_err());
    }

    #[test]
    fn spi5b_local_binding_cannot_supply_a_connector_planning_pin() {
        let error = match local_binding().admission.exact_planning_pin() {
            Ok(_) => panic!("local SQL bindings cannot admit a connector read"),
            Err(error) => error,
        };

        assert_eq!(error, "query binding has no connector planning pin");
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
    fn topology_replan_seal_reuses_known_bindings_and_rejects_new_materialization() {
        let store = QueryTableBindingStore::try_new().expect("store");
        let known_key = QueryTableBindingKey::strict_base("ice", "db", "orders");
        let known = store
            .resolve_or_insert(known_key.clone(), || Ok(local_binding()))
            .expect("first admission");
        store.seal_for_topology_replan();

        let reused = store
            .resolve_or_insert(known_key, || {
                Err("must not reload admitted binding".to_string())
            })
            .expect("topology replan reuses first admission");
        assert_eq!(reused, known);

        let loads = AtomicUsize::new(0);
        let error = store
            .resolve_or_insert(
                QueryTableBindingKey::strict_base("ice", "db", "new_current"),
                || {
                    loads.fetch_add(1, Ordering::Relaxed);
                    Ok(local_binding())
                },
            )
            .expect_err("topology replan must not read an unknown current binding");
        assert!(error.contains("absent from the first admitted semantic snapshot"));
        assert_eq!(loads.load(Ordering::Relaxed), 0);
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
        let writer_key = QueryTableBindingKey::session_write_target(
            "ice",
            "db",
            "orders",
            novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0).expect("ordinal"),
        );
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

#[cfg(test)]
#[path = "query_bindings/tests.rs"]
mod observation_tests;
