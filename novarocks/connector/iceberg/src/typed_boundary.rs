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

//! The Iceberg side of the role-facing typed connector read boundary.
//!
//! This module adapts the concrete Iceberg control model in [`crate::typed_read`]
//! onto the coordinator-side provider read-runtime traits. Role code sees
//! opaque SPI handles through `ReadRuntimeAdapter`; concrete Iceberg values
//! remain in this module and the dedicated codec.
//!
//! Three properties are load-bearing here:
//!
//! * **the snapshot is pinned once.** `get_table_handle` resolves a reference,
//!   a snapshot id, or the current snapshot exactly once and freezes the answer
//!   into the handle. Nothing downstream -- pushdown, split enumeration, or a
//!   worker -- resolves it again or falls back to a later snapshot.
//! * **no secret crosses the boundary.** Only table properties this file can
//!   prove are non-secret reach the handle; a worker resolves object-store
//!   credentials from its own authorized configuration.
//! * **nothing is guessed.** An unknown system-relation suffix is absence, a
//!   missing manifest fact is an error, and a split that fails wire validation
//!   is an error rather than a silently dropped unit of work.
//!
//! It is additive: the existing [`crate::metadata`] control path is
//! untouched and keeps its own resolution rules.
// Design: ADR-0123 (docs/adr/ADR-0123-task-update-watermark-retry-delivery.md)

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use novarocks_spi::connector::read_stack::adapter::{
    ProviderReadColumnBinding, ProviderReadFilterApplication, ProviderReadLimitApplication,
    ProviderReadRuntime, ProviderReadSplitSource, ProviderReadSystemTablePlan,
};
use novarocks_spi::connector::read_stack::{
    Assignment, Bound, ConnectorExpression, ConnectorMvPartitionValue,
    ConnectorMvTargetPartitionSelection, ConnectorReadAttemptAccessMint,
    ConnectorReadAttemptAccessReacquirer, ConnectorReadAttemptAccessSealer,
    ConnectorReadAttemptAccessSource, ConnectorReadAttemptRuntime, ConnectorReadChangeWindow,
    ConnectorReadMetadataRequest, ConnectorReadMetadataVersion, ConnectorReadPublicSchema,
    ConnectorReadRelationVersion, ConnectorReadRequestControl, ConnectorReadRequestControlFactory,
    ConnectorReadStaticFacts, ConnectorReadTableHandle, ConnectorSession, ConnectorSplitBatch,
    ConnectorSplitSource, ConnectorTableHandle as _, ConnectorValue, ConnectorValueType,
    Constraint, Domain, DynamicFilterSnapshot, OrderedAssignments, Range, SchemaTableName,
    SplitWeight, SystemTableUnitHint, TupleDomain, ValueSet,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorOperationControl,
    ConnectorPinnedFileSet, ConnectorRequestContext, ConnectorTableObjectId,
    MvExactPartitionTransform, ProviderBindingEpoch, REWRITE_POSITION_DELETES_KIND,
};

use crate::file_pruning::file_may_satisfy_physical_predicates;
#[cfg(test)]
use crate::iceberg::spec::DataFileFormat;
use crate::iceberg::spec::{
    Datum, FormatVersion, Literal, ManifestFile, NestedField, PartitionSpec, PrimitiveLiteral,
    PrimitiveType, Schema, SchemaRef, Struct, StructType, TableMetadata, Transform, Type,
};
use crate::iceberg::table::Table;
use crate::loaded_table::{IcebergAttemptTableAccess, IcebergPhysicalTable};
use crate::metadata_context::IcebergMetadataContext;
use crate::read_model::{IcebergReadFile, IcebergReadSnapshot};
use crate::ref_snapshot::resolve_branch_head_snapshot_id;
use crate::scan_model::{
    IcebergDataFileInfo, IcebergPartitionFieldValue, IcebergPartitionValue,
    IcebergPhysicalPredicate, IcebergPhysicalPredicateDomain, IcebergPhysicalPredicateOp,
    IcebergPhysicalPredicateValue,
};
use crate::schema_facts::row_lineage_enabled;
use crate::typed_read::codec::public_read_schema;
use crate::typed_read::column_handle::{corrupt, invalid, unsupported};
use crate::typed_read::publish_read_schema_with_metadata;
#[cfg(test)]
use crate::typed_read::read_static_facts::decode_sha256_hex;
use crate::typed_read::read_static_facts::iceberg_final_static_facts;
use crate::typed_read::{
    ALWAYS_BOUND_METADATA_COLUMNS, HiveTransactionHandle, ICEBERG_CHANGE_OP_COLUMN,
    IcebergChangeSplit, IcebergChangeWindowEndpoints, IcebergChangeWindowHandle,
    IcebergChangeWindowHandleParams, IcebergChangeWindowSplitSource, IcebergColumnHandle,
    IcebergDeleteFile, IcebergDeleteFileContent, IcebergDeleteFileParams, IcebergFileFormat,
    IcebergMetadataColumn, IcebergPinnedDataFileSet, IcebergPlannedDataFile,
    IcebergRewriteArtifactContentId, IcebergRewritePositionDeleteFilesHandle,
    IcebergRewritePositionDeleteFilesSplit, IcebergRewritePositionDeleteFilesSplitParams,
    IcebergSplit, IcebergSplitSource, IcebergSplitSourceOptions, IcebergSystemTableReference,
    IcebergTableExecuteHandle, IcebergTableExecuteHandleParams, IcebergTableExecuteProcedureHandle,
    IcebergTableHandle, IcebergTableHandleParams, REWRITE_POSITION_DELETE_OUTPUT_COLUMNS,
    ROW_LINEAGE_METADATA_COLUMNS, SystemManifestSplit, SystemManifestSplitSource,
    SystemManifestSplitSourceParams, SystemRelationSplit, TrinoManifestFile, bounds_row_type,
    change_op_column_handle, derived_row_type_json, partition_row_type_with_metadata,
    plan_change_window_splits, system_relation_columns_with_metadata,
};
#[cfg(test)]
use novarocks_spi::connector::read_stack::{
    ConnectorReadArtifactCoverage, ConnectorReadDistribution,
};

/// The Iceberg table property carrying the default name mapping.
const NAME_MAPPING_PROPERTY: &str = "schema.name-mapping.default";

/// Table properties this boundary is willing to put on a worker-visible handle.
///
/// Every entry is a split-planning knob defined by the Iceberg table-format
/// specification, so none of them can name credential material. Anything else
/// -- a catalog client option, an object-store setting, a vendor extension this
/// process cannot classify -- is dropped rather than carried: a property whose
/// non-secret status cannot be *proven* has no business on the wire, and the
/// reader resolves its own authorized access separately.
const READER_VISIBLE_TABLE_PROPERTIES: [&str; 4] = [
    "read.split.metadata-target-size",
    "read.split.open-file-cost",
    "read.split.planning-lookback",
    "read.split.target-size",
];

/// One Iceberg system relation reachable as `<table>$<suffix>`.
///
/// Every relation is assigned through the runtime provider split source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergSystemRelation {
    Files,
    Entries,
    Snapshots,
    History,
    Refs,
    Manifests,
}

impl IcebergSystemRelation {
    /// The internal planning representation.  Unlike `worker_plan`, this
    /// never crosses the wire codec boundary.
    const fn runtime_worker_plan(
        self,
    ) -> (
        crate::typed_read::IcebergSystemTableType,
        SystemTableUnitHint,
    ) {
        match self {
            Self::Files => (
                crate::typed_read::IcebergSystemTableType::Files,
                SystemTableUnitHint::ManyUnits,
            ),
            Self::Entries => (
                crate::typed_read::IcebergSystemTableType::Entries,
                SystemTableUnitHint::ManyUnits,
            ),
            Self::Snapshots => (
                crate::typed_read::IcebergSystemTableType::Snapshots,
                SystemTableUnitHint::SingleUnit,
            ),
            Self::History => (
                crate::typed_read::IcebergSystemTableType::History,
                SystemTableUnitHint::SingleUnit,
            ),
            Self::Refs => (
                crate::typed_read::IcebergSystemTableType::Refs,
                SystemTableUnitHint::SingleUnit,
            ),
            Self::Manifests => (
                crate::typed_read::IcebergSystemTableType::Manifests,
                SystemTableUnitHint::SingleUnit,
            ),
        }
    }
}

type RequestPinnedPhysicalTables = Arc<Mutex<HashMap<SchemaTableName, IcebergPhysicalTable>>>;
type FrozenCowSource = Arc<crate::typed_read::frozen_source::IcebergCowPendingReadSource>;
type FrozenCowSources = Arc<Mutex<BTreeMap<[u8; 32], FrozenCowSource>>>;
use crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::{
    IcebergObservedSourceReadFacts, IcebergPreparedSourceRead, PreparedRelations,
};
use novarocks_spi::connector::read_stack::{
    ConnectorReadSourcePreparationRequest, ConnectorReadSourceProjection,
    ConnectorReadSourceSelection,
};
/// FE-only provenance. The wire relation keeps its original snapshot/domain;
/// this receipt selects the original frozen access author for fresh proofs.
#[derive(Clone)]
pub(crate) struct PreparedSourceAccess {
    pub(crate) source: Arc<IcebergPreparedSourceRead>,
    pub(crate) projection: ConnectorReadSourceProjection,
}
type PreparedSourceAccesses = Arc<Mutex<Vec<PreparedSourceAccess>>>;

/// The coordinator-side Iceberg adapter for one catalog instance generation.
///
/// It is constructed per transaction, exactly like Trino's per-transaction
/// `ConnectorMetadata`: the transaction marker it carries is stamped onto every
/// handle it mints, so a handle can never outlive the transaction that framed
/// it.
#[derive(Clone)]
pub struct IcebergTypedBoundary {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    catalog_handle: novarocks_spi::connector::CatalogHandle,
    transaction: HiveTransactionHandle,
    runtime: Arc<IcebergMetadataContext>,
    /// Query-attempt-local control capability. The installed control template
    /// has none because a vended metadata response may be materialized only
    /// through the attempt that owns its lease contribution.
    request_context: Option<ConnectorRequestContext>,
    /// A request-local physical-table view shared by typed metadata freezing
    /// and the later lazy split source.  It keeps the request-bound FileIO
    /// capability process-local while ensuring split enumeration does not
    /// reopen REST metadata after the attempt's credential manifest is sealed.
    request_pinned_physical_tables: Option<RequestPinnedPhysicalTables>,
    split_source_options: IcebergSplitSourceOptions,
    /// Exact COW observations adopted by this request's actual generation.
    /// Keyed by original source identity, never a SQL name or projection set.
    frozen_cow_sources: Option<FrozenCowSources>,
    prepared_source_accesses: Option<PreparedSourceAccesses>,
}

impl IcebergTypedBoundary {
    fn check_request_active(&self) -> Result<(), ConnectorError> {
        if let Some(request) = &self.request_context {
            ConnectorOperationControl::check_active(request)?;
        }
        Ok(())
    }

    fn complete_sdk_read<T>(
        &self,
        result: Result<Result<T, String>, String>,
    ) -> Result<T, ConnectorError> {
        self.check_request_active()?;
        result.map_err(unavailable)?.map_err(unavailable)
    }

    /// The composition-root entry point.
    ///
    /// `runtime` is the same control generation the existing
    /// [`crate::metadata::IcebergMetadata`] holds, so both paths
    /// observe one catalog client and one physical-table cache.
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        catalog_handle: novarocks_spi::connector::CatalogHandle,
        transaction: HiveTransactionHandle,
        runtime: Arc<IcebergMetadataContext>,
    ) -> Self {
        Self {
            descriptor,
            incarnation,
            catalog_handle,
            transaction,
            runtime,
            request_context: None,
            request_pinned_physical_tables: None,
            split_source_options: IcebergSplitSourceOptions::default(),
            frozen_cow_sources: None,
            prepared_source_accesses: None,
        }
    }

    /// Rebind this immutable generation template to one admitted request. The
    /// context is a process-local capability, never a session property, table
    /// handle, codec payload, or wire value.
    fn for_request(&self, request_context: ConnectorRequestContext) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            incarnation: self.incarnation,
            catalog_handle: self.catalog_handle.clone(),
            transaction: self.transaction,
            runtime: Arc::clone(&self.runtime),
            request_context: Some(request_context),
            request_pinned_physical_tables: Some(Arc::new(Mutex::new(HashMap::new()))),
            split_source_options: self.split_source_options,
            frozen_cow_sources: Some(Arc::new(Mutex::new(BTreeMap::new()))),
            prepared_source_accesses: Some(Arc::new(Mutex::new(Vec::new()))),
        }
    }

    fn for_request_with_pinned_table(
        &self,
        request_context: ConnectorRequestContext,
        name: SchemaTableName,
        table: IcebergPhysicalTable,
    ) -> Self {
        let tables = Arc::new(Mutex::new(HashMap::from([(name, table)])));
        Self {
            descriptor: self.descriptor.clone(),
            incarnation: self.incarnation,
            catalog_handle: self.catalog_handle.clone(),
            transaction: self.transaction,
            runtime: Arc::clone(&self.runtime),
            request_context: Some(request_context),
            request_pinned_physical_tables: Some(tables),
            split_source_options: self.split_source_options,
            frozen_cow_sources: Some(Arc::new(Mutex::new(BTreeMap::new()))),
            prepared_source_accesses: Some(Arc::new(Mutex::new(Vec::new()))),
        }
    }

    pub(crate) fn prepared_source_request_context(
        &self,
    ) -> Result<ConnectorRequestContext, ConnectorError> {
        self.check_request_active()?;
        self.request_context
            .clone()
            .ok_or_else(|| invalid("prepared source requires its actual request"))
    }

    pub(crate) fn prepared_source_runtime(&self) -> Arc<IcebergMetadataContext> {
        Arc::clone(&self.runtime)
    }

    fn prepared_source_access(
        &self,
        relation: &crate::typed_read::IcebergRuntimeRelation,
    ) -> Result<Option<PreparedSourceAccess>, ConnectorError> {
        let Some(sources) = &self.prepared_source_accesses else {
            return Ok(None);
        };
        let sources = sources
            .lock()
            .map_err(|_| corrupt("prepared source inventory poisoned"))?;
        let name = match relation {
            crate::typed_read::IcebergRuntimeRelation::Table(table) => table.schema_table_name(),
            crate::typed_read::IcebergRuntimeRelation::ChangeWindow(window) => {
                window.schema_table_name()
            }
            _ => return Ok(None),
        };
        let mut claimed = false;
        for receipt in sources.iter() {
            if receipt.source.request.name() != name {
                continue;
            }
            claimed = true;
            let matches = match (receipt.projection, relation) {
                (
                    ConnectorReadSourceProjection::To,
                    crate::typed_read::IcebergRuntimeRelation::Table(table),
                ) => same_source_table(table, &receipt.source.relations.to),
                (
                    ConnectorReadSourceProjection::From,
                    crate::typed_read::IcebergRuntimeRelation::Table(table),
                ) => receipt
                    .source
                    .relations
                    .from
                    .as_ref()
                    .is_some_and(|expected| same_source_table(table, expected)),
                (
                    ConnectorReadSourceProjection::Delta,
                    crate::typed_read::IcebergRuntimeRelation::ChangeWindow(window),
                ) => receipt
                    .source
                    .relations
                    .delta
                    .as_ref()
                    .is_some_and(|expected| window == expected),
                _ => false,
            };
            if matches {
                return Ok(Some(receipt.clone()));
            }
        }
        if claimed {
            return Err(invalid(
                "prepared source relation differs from its original prepared inventory",
            ));
        }
        Ok(None)
    }
    fn adopted_cow_source(
        &self,
        handle: &IcebergTableHandle,
    ) -> Result<Option<FrozenCowSource>, ConnectorError> {
        let Some(proof) = handle.frozen_cow_source() else {
            return Ok(None);
        };
        let sources = self
            .frozen_cow_sources
            .as_ref()
            .ok_or_else(|| corrupt("Iceberg COW source is not bound to a read request"))?;
        let source = sources
            .lock()
            .expect("request Iceberg COW source lock")
            .get(proof.source_digest())
            .cloned()
            .ok_or_else(|| corrupt("Iceberg COW handle has no adopted original source"))?;
        if source.signed_base != *proof.signed_base()
            || Some(source.pinned.version_ordinal()) != handle.snapshot_id()
            || source.read_file.path != proof.data_file_path()
            || !handle
                .read_domain()
                .is_some_and(|domain| domain == source.read_file.read_domain())
        {
            return Err(corrupt(
                "Iceberg COW handle differs from its adopted original source",
            ));
        }
        Ok(Some(source))
    }

    pub(crate) fn pinned_table_for_attempt(
        &self,
        name: &SchemaTableName,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        self.request_pinned_physical_tables
            .as_ref()
            .ok_or_else(|| invalid("Iceberg attempt access requires request-bound table state"))?
            .lock()
            .expect("request-pinned Iceberg table cache lock")
            .get(name)
            .cloned()
            .ok_or_else(|| {
                invalid("Iceberg attempt access was sealed before the exact table was frozen")
            })
    }

    /// Session knobs that change how files are cut, never what they contain.
    #[must_use]
    pub const fn with_split_source_options(mut self, options: IcebergSplitSourceOptions) -> Self {
        self.split_source_options = options;
        self
    }

    pub const fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub const fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }

    /// Build the provider-only bridge retained by an exact role installation.
    /// Roles receive its SPI trait objects, never this adapter or its concrete
    /// Iceberg payload accessors.
    pub fn read_runtime_adapter(
        self: Arc<Self>,
    ) -> novarocks_spi::connector::read_stack::adapter::ReadRuntimeAdapter<Self> {
        novarocks_spi::connector::read_stack::adapter::ReadRuntimeAdapter::new(self)
    }

    /// Load one relation, distinguishing absence from a control-plane failure.
    ///
    /// The cache is invalidated first because this is the boundary's single
    /// observation point for catalog truth: another engine can advance a REST
    /// or Hadoop table between statements, and a cached physical table would
    /// otherwise pin a snapshot that is already superseded before the statement
    /// even starts.
    fn load_relation(
        &self,
        name: &SchemaTableName,
    ) -> Result<Option<IcebergPhysicalTable>, ConnectorError> {
        self.runtime
            .control_state()
            .invalidate_table(name.schema_name(), name.table_name());
        self.load_pinned_relation(name).map(Some).or_else(|error| {
            if error.kind() == ConnectorErrorKind::NotFound {
                Ok(None)
            } else {
                Err(error)
            }
        })
    }

    /// Load one relation whose snapshot is already pinned.
    ///
    /// Deliberately does not invalidate: re-observing catalog truth here could
    /// only replace the metadata the pinned snapshot was chosen from, and the
    /// pinned id is passed explicitly to every walk below.
    fn load_pinned_relation(
        &self,
        name: &SchemaTableName,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        match (&self.request_context, &self.request_pinned_physical_tables) {
            (Some(context), Some(tables)) => {
                if let Some(table) = tables
                    .lock()
                    .expect("request-pinned Iceberg table cache lock")
                    .get(name)
                    .cloned()
                {
                    return Ok(table);
                }
                let table = self
                    .runtime
                    .load_table_classified_for_request(
                        name.schema_name(),
                        name.table_name(),
                        context,
                    )
                    .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
                tables
                    .lock()
                    .expect("request-pinned Iceberg table cache lock")
                    .insert(name.clone(), table.clone());
                Ok(table)
            }
            (Some(_), None) => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "request-bound Iceberg typed control has no request-pinned table cache",
            )),
            (None, None) => self
                .runtime
                .load_table_classified(name.schema_name(), name.table_name())
                .map_err(|(kind, message)| ConnectorError::new(kind, message)),
            (None, Some(_)) => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "unbound Iceberg typed control unexpectedly retained a request-pinned table cache",
            )),
        }
    }

    async fn load_relation_async(
        &self,
        name: &SchemaTableName,
    ) -> Result<Option<IcebergPhysicalTable>, ConnectorError> {
        self.runtime
            .control_state()
            .invalidate_table(name.schema_name(), name.table_name());
        self.load_pinned_relation_async(name)
            .await
            .map(Some)
            .or_else(|error| {
                if error.kind() == ConnectorErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(error)
                }
            })
    }
    async fn load_mv_target_relation_async(
        &self,
        name: &SchemaTableName,
    ) -> Result<Option<IcebergPhysicalTable>, ConnectorError> {
        let (Some(context), Some(tables)) =
            (&self.request_context, &self.request_pinned_physical_tables)
        else {
            return Err(invalid(
                "frozen MV target reads require a request-bound table cache",
            ));
        };
        self.check_request_active()?;
        let cached = {
            tables
                .lock()
                .expect("request-pinned Iceberg table cache lock")
                .get(name)
                .cloned()
        };
        if let Some(table) = cached {
            return Ok(Some(table));
        }
        let table = match self
            .runtime
            .observe_table_classified_for_request_with_snapshots_async(
                name.schema_name(),
                name.table_name(),
                context,
                crate::catalog::SnapshotLoading::Refs,
            )
            .await
        {
            Ok(table) => table,
            Err((ConnectorErrorKind::NotFound, _)) => return Ok(None),
            Err((kind, message)) => return Err(ConnectorError::new(kind, message)),
        };
        let mut unused = Some(table);
        let table = {
            let mut tables = tables
                .lock()
                .expect("request-pinned Iceberg table cache lock");
            tables
                .entry(name.clone())
                .or_insert_with(|| unused.take().expect("completed table"))
                .clone()
        };
        drop(unused);
        Ok(Some(table))
    }

    async fn load_pinned_relation_async(
        &self,
        name: &SchemaTableName,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        match (&self.request_context, &self.request_pinned_physical_tables) {
            (Some(context), Some(tables)) => {
                let cached = {
                    tables
                        .lock()
                        .expect("request-pinned Iceberg table cache lock")
                        .get(name)
                        .cloned()
                };
                if let Some(table) = cached {
                    return Ok(table);
                }
                let table = self
                    .runtime
                    .load_table_classified_for_request_async(
                        name.schema_name(),
                        name.table_name(),
                        context,
                    )
                    .await
                    .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
                // Keep the first completed observation; retire an unused racing
                // SDK view only after leaving the original cache lock.
                let mut unpublished = Some(table);
                let selected = {
                    let mut original = tables
                        .lock()
                        .expect("request-pinned Iceberg table cache lock");
                    match original.entry(name.clone()) {
                        std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
                        std::collections::hash_map::Entry::Vacant(entry) => entry
                            .insert(unpublished.take().expect("one completed table"))
                            .clone(),
                    }
                };
                drop(unpublished);
                Ok(selected)
            }
            (Some(_), None) => Err(invalid(
                "request-bound Iceberg typed control has no request-pinned table cache",
            )),
            (None, None) => self
                .runtime
                .load_table_classified_async(name.schema_name(), name.table_name())
                .await
                .map_err(|(kind, message)| ConnectorError::new(kind, message)),
            (None, Some(_)) => Err(invalid(
                "unbound Iceberg typed control unexpectedly retained a request-pinned table cache",
            )),
        }
    }

    /// MV target reads use a fresh refs observation, retained by this request
    /// for column binding, split planning and access sealing. Never publish a
    /// partial observation into the complete-metadata planning cache.
    fn load_mv_target_relation(
        &self,
        name: &SchemaTableName,
    ) -> Result<Option<IcebergPhysicalTable>, ConnectorError> {
        let (Some(context), Some(tables)) =
            (&self.request_context, &self.request_pinned_physical_tables)
        else {
            return Err(invalid(
                "frozen MV target reads require a request-bound table cache",
            ));
        };
        self.check_request_active()?;
        if let Some(table) = tables
            .lock()
            .expect("request-pinned Iceberg table cache lock")
            .get(name)
            .cloned()
        {
            return Ok(Some(table));
        }
        let table = match self
            .runtime
            .observe_table_classified_for_request_with_snapshots(
                name.schema_name(),
                name.table_name(),
                context,
                crate::catalog::SnapshotLoading::Refs,
            ) {
            Ok(table) => table,
            Err((ConnectorErrorKind::NotFound, _)) => return Ok(None),
            Err((kind, message)) => return Err(ConnectorError::new(kind, message)),
        };
        let table = tables
            .lock()
            .expect("request-pinned Iceberg table cache lock")
            .entry(name.clone())
            .or_insert(table)
            .clone();
        Ok(Some(table))
    }

    /// The visible files of one change-window endpoint.
    ///
    /// No predicate is applied: a change window is a difference of two endpoint
    /// row sets, and pruning one endpoint's files without pruning the other's
    /// identically would turn a survived file into a spurious add or removal.
    async fn change_window_endpoint_files(
        &self,
        table: &Table,
        domain: Arc<crate::delete_semantics::ReadDomain>,
    ) -> Result<Vec<IcebergPlannedDataFile>, ConnectorError> {
        self.check_request_active()?;
        let projection = ChangeWindowEndpointProjection::new(table.metadata(), &domain)?;
        let table = table.clone();
        let control = self.request_context.clone();
        let result = plan_pinned_snapshot(table, domain, control.as_ref()).await;
        self.check_request_active()?;
        let read_snapshot = result.map_err(unavailable)?;
        projection.plan(read_snapshot.files, self.request_context.as_ref())
    }

    /// Whether this relation's rows carry Iceberg v3 row lineage.
    ///
    /// The answer is a table property, which the handle deliberately does not
    /// carry: only the four split-planning knobs travel on it. It is read back
    /// from the pinned metadata the handle was frozen against, so the column
    /// set a scan is offered describes the very snapshot it will read.
    fn relation_has_row_lineage(
        &self,
        handle: &IcebergTableHandle,
    ) -> Result<bool, ConnectorError> {
        // This creator consumes the exact already-observed request fact.
        // A missing pin must not acquire network capability on a CPU worker.
        let physical = self.pinned_table_for_attempt(handle.schema_table_name())?;
        Ok(row_lineage_enabled(physical.table.metadata()))
    }

    /// The pinned metadata one system relation reference describes.
    ///
    /// The reference names the base relation and the exact metadata file it was
    /// frozen from, so the load is verified against both before its schema is
    /// used: a location that has been reused by another table would otherwise
    /// answer with some other relation's columns.
    fn system_relation_metadata(
        &self,
        reference: &IcebergSystemTableReference,
    ) -> Result<TableMetadata, ConnectorError> {
        let physical = self.pinned_table_for_attempt(reference.schema_table_name())?;
        let metadata = physical.table.metadata();
        reference.verify_loaded_metadata(metadata)?;
        Ok(metadata.clone())
    }

    /// The manifest-list entries of the snapshot a `$files` reference pins.
    async fn pinned_snapshot_manifests(
        &self,
        reference: &IcebergSystemTableReference,
    ) -> Result<Vec<TrinoManifestFile>, ConnectorError> {
        let snapshot_id = reference.snapshot_id().ok_or_else(|| {
            corrupt("iceberg $files reference carries no pinned snapshot to walk")
        })?;
        self.check_request_active()?;
        let physical = self
            .load_pinned_relation_async(reference.schema_table_name())
            .await?;
        let table = physical.table.clone();
        let control = self.request_context.clone();
        let result = (async move {
            pinned_snapshot_manifest_list(&table, snapshot_id, control.as_ref()).await
        })
        .await;
        self.check_request_active()?;
        let entries = result.map_err(unavailable)?;
        entries
            .iter()
            .map(TrinoManifestFile::from_manifest_file)
            .collect()
    }

    /// Enumerate one frozen change window as the difference of its endpoints.
    async fn change_window_split_source(
        &self,
        handle: &IcebergChangeWindowHandle,
    ) -> Result<IcebergChangeWindowSplitSource, ConnectorError> {
        let physical = self
            .load_pinned_relation_async(handle.schema_table_name())
            .await?;
        let partition_types = change_window_partition_types(handle.to_read_domain())?;
        let from_visible = self
            .change_window_endpoint_files(&physical.table, handle.from_read_domain().clone())
            .await?;
        let to_visible = self
            .change_window_endpoint_files(&physical.table, handle.to_read_domain().clone())
            .await?;
        let plan = plan_change_window_splits(
            handle,
            IcebergChangeWindowEndpoints {
                from_visible: &from_visible,
                to_visible: &to_visible,
            },
            &partition_types,
            self.split_source_options,
        )?;
        Ok(IcebergChangeWindowSplitSource::new(plan))
    }
}

/// Frozen interpretation prepared before the original complete endpoint walk.
/// Both legacy ordinary planning and prepared execution use this same author.
struct ChangeWindowEndpointProjection {
    schema: Schema,
    integers: crate::scalar_integer_domain::ScalarIntegerDomains,
    specs: BTreeMap<i32, PartitionSpec>,
}
impl ChangeWindowEndpointProjection {
    fn new(
        metadata: &TableMetadata,
        domain: &crate::delete_semantics::ReadDomain,
    ) -> Result<Self, ConnectorError> {
        let schema = domain
            .endpoint()
            .schema()
            .map_err(|e| corrupt(e.to_string()))?;
        let integers = crate::scalar_integer_domain::of_schema(
            &schema,
            &crate::scalar_integer_domain::metadata_declarations(metadata)?,
        )?;
        let specs = domain
            .endpoint()
            .partition_spec_jsons()
            .iter()
            .map(|(id, json)| {
                serde_json::from_str::<PartitionSpec>(json)
                    .map(|spec| (*id, spec))
                    .map_err(|error| {
                        corrupt(format!("decode pinned endpoint partition spec: {error}"))
                    })
            })
            .collect::<Result<BTreeMap<_, _>, ConnectorError>>()?;
        Ok(Self {
            schema,
            integers,
            specs,
        })
    }
    fn plan(
        self,
        files: Vec<IcebergReadFile>,
        control: Option<&ConnectorRequestContext>,
    ) -> Result<Vec<IcebergPlannedDataFile>, ConnectorError> {
        files
            .into_iter()
            .map(|read_file| {
                if let Some(control) = control {
                    control.check_active()?;
                }
                let spec = read_file
                    .partition_spec_id
                    .map(|id| {
                        self.specs.get(&id).ok_or_else(|| {
                            corrupt("iceberg change-window file names an absent partition spec")
                        })
                    })
                    .transpose()?;
                validate_scalar_integer_manifest_facts(
                    &self.integers,
                    read_file.manifest.as_ref(),
                    spec,
                    read_file.partition_values.as_ref(),
                )?;
                planned_data_file(read_file, &self.schema, spec, &BTreeSet::new())
            })
            .collect()
    }
}

/// The partition spelling a rewrite split carries for an unpartitioned file.
///
/// The split contract requires a non-empty partition JSON, and an
/// unpartitioned data file has no partition struct to spell. The empty object
/// is the spec's own encoding of "no partition fields", so it states that
/// rather than inventing a placeholder value.
const UNPARTITIONED_REWRITE_PARTITION_JSON: &str = "{}";

/// Restate one frozen delete artifact as the typed split's delete file.
///
/// Every fact is carried from the artifact the group named. A V2 Parquet
/// position-delete file has no addressed content range, so re-encoding it would
/// mean reading a whole file to find one data file's rows; the split contract
/// refuses that, and this refuses it earlier with the artifact's own path.
fn rewrite_position_delete_file(
    delete: &crate::scan_model::IcebergDeleteFileInfo,
) -> Result<IcebergDeleteFile, ConnectorError> {
    if delete.file_content != crate::scan_model::IcebergDeleteFileContent::Position
        || delete.file_format != crate::scan_model::IcebergDeleteFileFormat::Puffin
    {
        return Err(invalid(format!(
            "iceberg rewrite position delete artifact {} is not a puffin deletion vector",
            delete.path
        )));
    }
    IcebergDeleteFile::try_new(IcebergDeleteFileParams {
        content: IcebergDeleteFileContent::PositionDeletes,
        path: delete.path.clone(),
        format: IcebergFileFormat::Puffin,
        record_count: delete
            .record_count
            .ok_or_else(|| corrupt("rewrite delete has no manifest record count"))?,
        file_size_in_bytes: delete
            .length
            .ok_or_else(|| corrupt("rewrite delete has no file size"))?,
        partition_spec_id: delete
            .partition_spec_id
            .ok_or_else(|| corrupt("rewrite delete has no partition spec"))?,
        partition_data_json: delete
            .partition_data_json
            .clone()
            .ok_or_else(|| corrupt("rewrite delete has no typed partition tuple"))?,
        equality_field_ids: Vec::new(),
        row_position_lower_bound: None,
        row_position_upper_bound: None,
        data_sequence_number: delete
            .sequence_number
            .ok_or_else(|| corrupt("rewrite delete has no data sequence"))?,
        content_offset: delete.content_offset,
        content_size_in_bytes: delete.content_size_in_bytes,
        referenced_data_file: delete.referenced_data_file.clone(),
        decryption_data: None,
    })
}

/// Project complete facts without a second manifest walk or path-based join.
fn planned_data_file(
    read_file: IcebergReadFile,
    schema: &Schema,
    partition_spec: Option<&PartitionSpec>,
    dynamic_filter_columns: &BTreeSet<IcebergColumnHandle>,
) -> Result<IcebergPlannedDataFile, ConnectorError> {
    let data_facts = read_file.manifest.as_ref();
    Ok(IcebergPlannedDataFile {
        file_format: IcebergFileFormat::from_data_file_format(data_facts.file_format)?,
        split_offsets: data_facts.split_offsets.clone(),
        key_metadata: data_facts.key_metadata.clone(),
        // File statistics are reconstructed at the pinned-manifest boundary
        // from field IDs and the frozen current schema.  Only declared dynamic
        // filter columns are materialized: carrying every file metric to each
        // BE would turn an optimization hint into unbounded split payload.
        file_statistics_domain: manifest_statistics_domain(
            schema,
            partition_spec,
            &read_file,
            data_facts,
            dynamic_filter_columns,
        )?,
        decryption_data: None,
        read_file,
    })
}

impl std::fmt::Debug for IcebergTypedBoundary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergTypedBoundary")
            .field("instance_id", &self.descriptor.instance_id)
            .field("runtime", &self.runtime)
            .field("split_source_options", &self.split_source_options)
            .finish()
    }
}

/// Connector-owned constructor for the query-scoped FE typed-read services.
/// The registry retains the generation template, while every request receives
/// an adapter bound to its own process-local storage authority.
#[derive(Clone)]
pub struct IcebergTypedRequestControlFactory {
    template: Arc<IcebergTypedBoundary>,
}

impl IcebergTypedRequestControlFactory {
    pub fn new(template: Arc<IcebergTypedBoundary>) -> Self {
        Self { template }
    }
}

/// Fixed first-party headers of the actual serial FE read program. Metadata
/// files are awaited one at a time by build_read_snapshot_in_domain; one
/// additional slot covers its actual nested credential/authorization phase.
/// SDK graphs and retained variable payload are admitted where they form.
pub fn read_io_geometry()
-> Result<novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry, ConnectorError> {
    let io = std::num::NonZeroUsize::new(2).unwrap();
    let headers = std::mem::size_of::<IcebergTypedBoundary>()
        .checked_add(std::mem::size_of::<IcebergTypedRequestControlFactory>())
        .and_then(|n| n.checked_add(std::mem::size_of::<ConnectorReadRequestControl>()))
        .and_then(|n| {
            n.checked_add(std::mem::size_of::<
                novarocks_spi::connector::read_stack::ConnectorReadRelationPreparation,
            >())
        })
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "read Source header geometry overflow",
            )
        })?;
    let metadata = std::num::NonZeroU64::new(u64::try_from(headers).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            "read Source header geometry overflow",
        )
    })?)
    .unwrap();
    Ok(
        novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry::new(
            io, metadata, metadata,
        ),
    )
}

impl ConnectorReadRequestControlFactory for IcebergTypedRequestControlFactory {
    fn planning_io_geometry(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry, ConnectorError>
    {
        read_io_geometry()
    }

    fn visit_planning_dependencies(
        &self,
        visit: &mut dyn FnMut(
            &novarocks_spi::connector::SharedDependency,
        ) -> Result<(), ConnectorError>,
    ) -> Result<(), ConnectorError> {
        // Reuse the exact generation's original REST/FS/Thrift route author.
        for dependency in self.template.runtime.read_planning_initial_dependencies()? {
            visit(&dependency)?;
        }
        Ok(())
    }

    fn for_planning(
        &self,
        request: &novarocks_spi::connector::ConnectorPlanningContext,
    ) -> Result<ConnectorReadRequestControl, ConnectorError> {
        let boundary = Arc::new(self.template.for_request(request.request().clone()));
        Ok(iceberg_request_control(
            Arc::clone(&self.template),
            boundary,
        ))
    }
}

struct IcebergAttemptAccessSealer {
    template: Arc<IcebergTypedBoundary>,
    boundary: Arc<IcebergTypedBoundary>,
    adapter: Arc<
        novarocks_spi::connector::read_stack::adapter::ReadRuntimeAdapter<IcebergTypedBoundary>,
    >,
}

impl ConnectorReadAttemptAccessSealer for IcebergAttemptAccessSealer {
    fn seal(
        &self,
        frozen: &ConnectorReadTableHandle,
        mint: ConnectorReadAttemptAccessMint,
    ) -> Result<ConnectorReadAttemptAccessSource, ConnectorError> {
        let relation = self.adapter.table(frozen)?;
        let name = relation.schema_table_name().clone();
        let frozen_cow_source = match &relation {
            crate::typed_read::IcebergRuntimeRelation::Table(table) => {
                self.boundary.adopted_cow_source(table)?
            }
            _ => None,
        };
        let prepared = self.boundary.prepared_source_access(relation)?;
        let access = match (prepared, frozen_cow_source) {
            (Some(receipt), None) => IcebergAttemptAccessRecipe::PreparedSource(receipt),
            (Some(_), Some(_)) => {
                return Err(invalid("read relation has two source provenance authors"));
            }
            (None, Some(source)) => IcebergAttemptAccessRecipe::Cow(source),
            (None, None) => IcebergAttemptAccessRecipe::Original(
                IcebergAttemptTableAccess::freeze(self.boundary.pinned_table_for_attempt(&name)?),
            ),
        };
        Ok(mint.seal(Arc::new(IcebergAttemptAccessReacquirer {
            template: Arc::clone(&self.template),
            name,
            access,
        })))
    }
}

// A COW attempt borrows the ONE original access recipe through its source.
// Ordinary attempts keep the original owned recipe and allocation behavior.
enum IcebergAttemptAccessRecipe {
    Original(IcebergAttemptTableAccess),
    PreparedSource(PreparedSourceAccess),
    Cow(Arc<crate::typed_read::frozen_source::IcebergCowPendingReadSource>),
}

impl IcebergAttemptAccessRecipe {
    fn access(&self) -> &IcebergAttemptTableAccess {
        match self {
            Self::Original(access) => access,
            Self::Cow(source) => source.access.as_ref(),
            Self::PreparedSource(receipt) => receipt.source.access.as_ref(),
        }
    }
}

struct IcebergAttemptAccessReacquirer {
    template: Arc<IcebergTypedBoundary>,
    name: SchemaTableName,
    access: IcebergAttemptAccessRecipe,
}

/// One frozen table view under one catalog generation.
///
/// This key deliberately contains no request identity: the surrounding
/// `ConnectorRequestScope` is minted for exactly one admitted attempt. The
/// frozen metadata location prevents two time-separated table views with the
/// same SQL name from sharing one request-local reacquisition result.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct AttemptReacquiredTableKey {
    catalog: novarocks_spi::connector::CatalogHandle,
    table: SchemaTableName,
    metadata_identity: String,
}

/// Provider-private capability cache for one admitted attempt.
///
/// Attempt initialization opens sources serially. Keeping the cache in the
/// shared request scope makes repeated frozen scans reuse one current
/// capability without allowing that capability to cross an attempt boundary.
#[derive(Default)]
struct AttemptReacquiredTableCache {
    tables: Mutex<BTreeMap<AttemptReacquiredTableKey, IcebergPhysicalTable>>,
}

impl AttemptReacquiredTableCache {
    async fn get_or_reacquire(
        &self,
        key: AttemptReacquiredTableKey,
        reacquire: impl std::future::Future<Output = Result<IcebergPhysicalTable, ConnectorError>>,
    ) -> Result<IcebergPhysicalTable, ConnectorError> {
        if let Some(table) = self
            .tables
            .lock()
            .expect("attempt Iceberg table cache lock")
            .get(&key)
            .cloned()
        {
            return Ok(table);
        }
        let table = reacquire.await?;
        let mut unused = Some(table);
        let table = {
            let mut original = self
                .tables
                .lock()
                .expect("attempt Iceberg table cache lock");
            original
                .entry(key)
                .or_insert_with(|| unused.take().expect("completed access"))
                .clone()
        };
        drop(unused);
        Ok(table)
    }
}

impl ConnectorReadAttemptAccessReacquirer for IcebergAttemptAccessReacquirer {
    fn requires_metadata_split_preparation(&self) -> bool {
        matches!(self.access, IcebergAttemptAccessRecipe::PreparedSource(_))
    }

    fn for_attempt<'a>(
        &'a self,
        request: &'a novarocks_spi::connector::ConnectorAttemptContext,
    ) -> futures::future::BoxFuture<'a, Result<ConnectorReadAttemptRuntime, ConnectorError>> {
        Box::pin(async move {
            let request = request.request();
            // The enclosing SPI source checks this before dispatching us. Repeat
            // it here because a cache hit must retain the same cancellation and
            // deadline boundary as a provider reacquisition.
            if request.is_cancelled() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Cancelled,
                    "reacquire Iceberg table access was cancelled",
                ));
            }
            if Instant::now() >= request.deadline() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::DeadlineExceeded,
                    "reacquire Iceberg table access deadline elapsed",
                ));
            }
            let cache = request
                .request_scope_extension_or_insert_with(AttemptReacquiredTableCache::default);
            let key = AttemptReacquiredTableKey {
                catalog: self.template.catalog_handle.clone(),
                table: self.name.clone(),
                metadata_identity: self.access.access().request_cache_identity(),
            };
            let physical = cache
                .get_or_reacquire(key, async {
                    self.template
                        .runtime
                        .reacquire_table_access_for_request_async(
                            self.name.schema_name(),
                            self.name.table_name(),
                            self.access.access(),
                            request,
                        )
                        .await
                        .map_err(|(kind, message)| ConnectorError::new(kind, message))
                })
                .await?;
            let boundary = Arc::new(self.template.for_request_with_pinned_table(
                request.clone(),
                self.name.clone(),
                physical,
            ));
            if let IcebergAttemptAccessRecipe::Cow(source) = &self.access {
                use sha2::Digest;
                let digest: [u8; 32] =
                    sha2::Sha256::digest(source.original_source.payload()).into();
                boundary
                    .frozen_cow_sources
                    .as_ref()
                    .ok_or_else(|| corrupt("Iceberg COW retry lost its request source owner"))?
                    .lock()
                    .expect("request Iceberg COW source lock")
                    .insert(digest, source.clone());
            }
            if let IcebergAttemptAccessRecipe::PreparedSource(receipt) = &self.access {
                boundary
                    .prepared_source_accesses
                    .as_ref()
                    .ok_or_else(|| corrupt("prepared source retry lost its request inventory"))?
                    .lock()
                    .map_err(|_| corrupt("prepared source inventory poisoned"))?
                    .push(receipt.clone());
            }
            let control = iceberg_request_control(Arc::clone(&self.template), boundary);
            Ok(ConnectorReadAttemptRuntime::new(control.splits()))
        })
    }
}

fn iceberg_request_control(
    template: Arc<IcebergTypedBoundary>,
    boundary: Arc<IcebergTypedBoundary>,
) -> ConnectorReadRequestControl {
    let adapter = Arc::new(Arc::clone(&boundary).read_runtime_adapter());
    let sealer = Arc::new(IcebergAttemptAccessSealer {
        template,
        boundary,
        adapter: Arc::clone(&adapter),
    });
    ConnectorReadRequestControl::provider_reacquire_attempt_access(
        Arc::clone(&adapter)
            as Arc<dyn novarocks_spi::connector::read_stack::ConnectorReadMetadata>,
        adapter as Arc<dyn novarocks_spi::connector::read_stack::ConnectorReadSplitManager>,
        sealer,
    )
}

/// The provider type family is entirely Iceberg-owned.  The generic SPI
/// adapter stores these values behind opaque handles; it is intentionally the
/// codec, rather than an FE or BE role, that can recover them again.
impl ProviderReadRuntime for IcebergTypedBoundary {
    type Table = crate::typed_read::IcebergRuntimeRelation;
    type Column = IcebergColumnHandle;
    type Transaction = HiveTransactionHandle;
    type Split = crate::typed_read::IcebergReadSplit;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn catalog_handle(&self) -> &novarocks_spi::connector::CatalogHandle {
        &self.catalog_handle
    }

    fn transaction(&self) -> Self::Transaction {
        self.transaction
    }
}

/// Completed facts for one exact original relation request. Fields are private.
pub struct IcebergPreparedRelation {
    partition_snapshot: Option<crate::read_model::IcebergReadSnapshot>,
    physical: IcebergPhysicalTable,
    // Original opaque selection/content survives all completed SDK facts.
    request: novarocks_spi::connector::read_stack::ConnectorReadRelationPreparationRequest,
}

impl novarocks_spi::connector::read_stack::adapter::ProviderReadMetadata for IcebergTypedBoundary {
    type PreparedSourceRead = Arc<IcebergPreparedSourceRead>;
    type PreparedRelation = IcebergPreparedRelation;
    fn prepare_source_read<'a>(
        &'a self,
        _session: &'a ConnectorSession,
        request: &'a ConnectorReadSourcePreparationRequest,
        cpu: &'a Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
        original: &'a novarocks_spi::connector::ConnectorOriginalResultScope,
        owner: &'a mut Option<novarocks_spi::connector::ConnectorProcessLocalPayload>,
    ) -> futures::future::BoxFuture<
        'a,
        Result<
            novarocks_spi::connector::read_stack::adapter::ProviderPreparedSourceRead<
                Self::PreparedSourceRead,
            >,
            ConnectorError,
        >,
    > {
        Box::pin(async move {
            let context = self
                .request_context
                .as_ref()
                .ok_or_else(|| invalid("source preparation requires its original request"))?;
            self.check_request_active()?;
            if owner.is_some() {
                return Err(invalid("source preparation owner slot is occupied"));
            }
            let source = crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal::prepared_source(
                self.descriptor.clone(), self.incarnation, self.catalog_handle.clone(),
                Arc::clone(&self.runtime), request.clone(), context.clone(), original.clone(),
            );
            *owner = Some(novarocks_spi::connector::ConnectorProcessLocalPayload::new(
                novarocks_spi::connector::ConnectorProviderBindingKey {
                    instance_id: self.descriptor.instance_id.clone(),
                    incarnation: self.incarnation,
                },
                Arc::clone(&source),
            ));
            source.drive_prepared_source(cpu).await?;
            source.take_prepared_source()
        })
    }

    fn project_prepared_source_read(
        &self,
        _session: &ConnectorSession,
        request: &ConnectorReadSourcePreparationRequest,
        source: &Self::PreparedSourceRead,
        projection: ConnectorReadSourceProjection,
    ) -> Result<Self::Table, ConnectorError> {
        let owner = novarocks_spi::connector::ConnectorProviderBindingKey {
            instance_id: self.descriptor.instance_id.clone(),
            incarnation: self.incarnation,
        };
        if source.owner != owner
            || source.catalog != self.catalog_handle
            || request.name() != source.request.name()
            || request.object_id() != source.request.object_id()
            || request.selection() != source.request.selection()
            || request.from_revision() != source.request.from_revision()
            || request.to_revision() != source.request.to_revision()
            || request.observed_table() != source.request.observed_table()
        {
            return Err(invalid(
                "prepared source belongs to another exact read binding or pin",
            ));
        }
        request.observed_source_facts().validate_owner(&owner)?;
        let supplied = request
            .observed_source_facts()
            .value::<IcebergObservedSourceReadFacts>(self.descriptor.instance_id.as_str())?;
        if supplied.access.request_cache_identity() != source.access.request_cache_identity()
            || supplied.snapshots
                != source
                    .request
                    .observed_source_facts()
                    .value::<IcebergObservedSourceReadFacts>(self.descriptor.instance_id.as_str())?
                    .snapshots
        {
            return Err(invalid(
                "prepared source access frame differs from its original observation",
            ));
        }
        let context = self
            .request_context
            .as_ref()
            .ok_or_else(|| invalid("source projection requires a request binding"))?;
        self.check_request_active()?;
        let relation = match projection {
            ConnectorReadSourceProjection::To => {
                crate::typed_read::IcebergRuntimeRelation::Table(source.relations.to.clone())
            }
            ConnectorReadSourceProjection::From => {
                crate::typed_read::IcebergRuntimeRelation::Table(
                    source
                        .relations
                        .from
                        .clone()
                        .ok_or_else(|| invalid("prepared decision has no From relation"))?,
                )
            }
            ConnectorReadSourceProjection::Delta => {
                crate::typed_read::IcebergRuntimeRelation::ChangeWindow(
                    source
                        .relations
                        .delta
                        .clone()
                        .ok_or_else(|| invalid("prepared decision has no Delta relation"))?,
                )
            }
        };
        let physical = source.access.reacquired_request_scoped(
            self.runtime
                .resources()
                .planning_binding()
                .for_request(context.clone()),
            self.runtime
                .resources()
                .catalog_runtime()
                .iceberg_runtime()
                .clone(),
        )?;
        let tables = self
            .request_pinned_physical_tables
            .as_ref()
            .ok_or_else(|| invalid("source projection has no request inventory"))?;
        let mut tables = tables
            .lock()
            .map_err(|_| corrupt("request physical inventory poisoned"))?;
        if let Some(previous) = tables.get(request.name()) {
            if crate::delete_semantics::canonical_metadata_json(previous.table.metadata())
                .map_err(|error| corrupt(error.to_string()))?
                != crate::delete_semantics::canonical_metadata_json(physical.table.metadata())
                    .map_err(|error| corrupt(error.to_string()))?
            {
                return Err(invalid(
                    "request already contains a different frozen table frame",
                ));
            }
        } else {
            tables.insert(request.name().clone(), physical);
        }
        drop(tables);
        self.prepared_source_accesses
            .as_ref()
            .ok_or_else(|| invalid("source projection has no provenance inventory"))?
            .lock()
            .map_err(|_| corrupt("prepared source inventory poisoned"))?
            .push(PreparedSourceAccess {
                source: Arc::clone(source),
                projection,
            });
        Ok(relation)
    }

    fn prepare_relation<'a>(
        &'a self,
        _session: &'a ConnectorSession,
        request: &'a novarocks_spi::connector::read_stack::ConnectorReadRelationPreparationRequest,
    ) -> futures::future::BoxFuture<'a, Result<Option<Self::PreparedRelation>, ConnectorError>>
    {
        Box::pin(async move {
            use novarocks_spi::connector::read_stack::ConnectorReadRelationPreparationKind as K;
            self.check_request_active()?;
            let name = request.name();
            let physical = match request.kind() {
                K::Metadata {
                    request: metadata_request,
                } => {
                    let Some((base_table, relation)) = system_relation_of(name.table_name()) else {
                        return Ok(None);
                    };
                    let (system_table_type, _) = relation.runtime_worker_plan();
                    if let Some(metadata_request) = metadata_request {
                        if metadata_request.kind().as_str() != system_table_type.suffix() {
                            return Err(invalid(format!(
                                "iceberg metadata request kind {} does not match relation {}",
                                metadata_request.kind().as_str(),
                                system_table_type.suffix()
                            )));
                        }
                        match metadata_request.version() {
                            ConnectorReadMetadataVersion::Current => {}
                            ConnectorReadMetadataVersion::SnapshotId(snapshot_id) => {
                                return Err(unsupported(format!(
                                    "iceberg metadata snapshot request {snapshot_id} is unsupported because this read handle cannot prove the matching immutable metadata-file identity"
                                )));
                            }
                            ConnectorReadMetadataVersion::TimestampMillis(timestamp_millis) => {
                                return Err(unsupported(format!(
                                    "iceberg metadata timestamp request {timestamp_millis} is unsupported because this read handle cannot prove the matching immutable metadata-file identity"
                                )));
                            }
                        }
                    }
                    let base_name = SchemaTableName::try_new(name.schema_name(), &base_table)?;
                    self.load_relation_async(&base_name).await?
                }
                K::MvTarget { .. } | K::MvTargetPartition { .. } => {
                    if system_relation_of(name.table_name()).is_some() {
                        return Ok(None);
                    }
                    self.load_mv_target_relation_async(name).await?
                }
                K::TableExecute { procedure } => {
                    let novarocks_spi::connector::read_stack::ConnectorReadTableExecuteProcedure::RewritePositionDeleteFiles(group)=procedure;
                    group.content().validate_owner(
                        &novarocks_spi::connector::ConnectorProviderBindingKey {
                            instance_id: self.descriptor.instance_id.clone(),
                            incarnation: self.incarnation,
                        },
                    )?;
                    if system_relation_of(name.table_name()).is_some() {
                        return Ok(None);
                    }
                    self.load_relation_async(name).await?
                }
                _ => {
                    if system_relation_of(name.table_name()).is_some() {
                        return Ok(None);
                    }
                    self.load_relation_async(name).await?
                }
            };
            let Some(physical) = physical else {
                return Ok(None);
            };
            let partition_snapshot = if let K::MvTargetPartition { selection } = request.kind() {
                validate_mv_target_partition_selection(physical.table.metadata(), selection)?;
                // Preserve the original pure schema/selection preflight before physical I/O.
                projection_schema_for_pinned_snapshot(
                    physical.table.metadata(),
                    selection.snapshot_id(),
                )?;
                if selection.keys().is_empty() {
                    None
                } else {
                    self.check_request_active()?;
                    let snapshot = crate::read_snapshot::build_read_snapshot_at_with_control(
                        &physical.table,
                        selection.snapshot_id(),
                        self.request_context
                            .as_ref()
                            .map(|c| c as &dyn ConnectorOperationControl),
                    )
                    .await;
                    self.check_request_active()?;
                    Some(snapshot.map_err(unavailable)?)
                }
            } else {
                None
            };
            Ok(Some(IcebergPreparedRelation {
                request: request.clone(),
                physical,
                partition_snapshot,
            }))
        })
    }
    fn create_prepared_relation(
        &self,
        prepared: Self::PreparedRelation,
    ) -> Result<
        Option<
            novarocks_spi::connector::read_stack::adapter::ProviderReadOpenedRelation<Self::Table>,
        >,
        ConnectorError,
    > {
        use novarocks_spi::connector::read_stack::ConnectorReadRelationPreparationKind as K;
        self.check_request_active()?;
        let IcebergPreparedRelation {
            request,
            physical,
            partition_snapshot,
        } = prepared;
        let name = request.name();
        let table = match request.kind() {
            K::Table { version, reference } => Some(
                crate::typed_read::IcebergRuntimeRelation::Table(table_handle_for_version(
                    name,
                    physical.table.metadata(),
                    *version,
                    reference.as_deref(),
                )?),
            ),
            K::PinnedFileSet { pinned } => {
                let metadata = physical.table.metadata();
                let snapshot_id = pinned.version_ordinal();
                if metadata.snapshot_by_id(snapshot_id).is_none() {
                    return Err(corrupt(format!(
                        "iceberg relation {}.{} no longer holds snapshot {snapshot_id}, which a pinned read was frozen at",
                        name.schema_name(),
                        name.table_name()
                    )));
                }
                let files = IcebergPinnedDataFileSet::try_new(pinned.files())?;
                if files.len() != pinned.files().len() {
                    return Err(invalid(
                        "iceberg pinned read was offered the same data file more than once",
                    ));
                }
                // A pinned file set freezes row visibility, not a spelling of stable
                // Iceberg fields. When the current schema has exactly the same field
                // identities as the pinned snapshot, project the frozen files through
                // that current schema so a refresh rebind (for example `region` ->
                // `area`) and the connector bindings name the same field. Any
                // structural evolution remains on the historical schema and therefore
                // cannot be silently reinterpreted by this compatibility path.
                let schema = projection_schema_for_pinned_snapshot(metadata, snapshot_id)?;
                Some(crate::typed_read::IcebergRuntimeRelation::Table(
                    pinned_table_handle_with_schema(
                        name,
                        metadata,
                        Some(snapshot_id),
                        schema,
                        Some(files),
                    )?,
                ))
            }
            K::MvTarget {
                object_id,
                snapshot_id,
            } => {
                let metadata = physical.table.metadata();
                validate_mv_target_pin(metadata, object_id, *snapshot_id)?;
                let version = (*snapshot_id).map_or(
                    ConnectorReadRelationVersion::Current,
                    ConnectorReadRelationVersion::SnapshotId,
                );
                Some(crate::typed_read::IcebergRuntimeRelation::Table(
                    table_handle_for_version(name, metadata, version, None)?,
                ))
            }
            K::MvTargetPartition { selection } => {
                let metadata = physical.table.metadata();
                validate_mv_target_partition_selection(metadata, selection)?;
                let schema =
                    projection_schema_for_pinned_snapshot(metadata, selection.snapshot_id())?;
                let selected = if selection.keys().is_empty() {
                    Some(IcebergPinnedDataFileSet::try_new(Vec::<String>::new())?)
                } else {
                    let snapshot = partition_snapshot.as_ref().ok_or_else(|| {
                        corrupt("MV target partition observation has no completed snapshot")
                    })?;
                    select_mv_target_files(metadata, snapshot, selection)
                };
                Some(crate::typed_read::IcebergRuntimeRelation::Table(
                    pinned_table_handle_with_schema(
                        name,
                        metadata,
                        Some(selection.snapshot_id()),
                        schema,
                        selected,
                    )?,
                ))
            }
            K::Delta { window } => {
                pinned_change_window_handle(name, physical.table.metadata(), *window)?
                    .map(crate::typed_read::IcebergRuntimeRelation::ChangeWindow)
            }
            K::TableExecute { procedure } => {
                let novarocks_spi::connector::read_stack::ConnectorReadTableExecuteProcedure::RewritePositionDeleteFiles(group)=procedure;

                let metadata = physical.table.metadata();
                let content = group
                    .content()
                    .value::<crate::distributed_rewrite::FrozenRewritePlanContent>(
                        self.descriptor.instance_id.as_str(),
                    )?
                    .clone();
                content.validate_identity(group.operation_id(), group.artifact_digest_hex())?;
                content.group(group.group_digest_hex())?;
                let snapshot_id =
                    validate_rewrite_position_delete_artifact(name, metadata, &content)?;
                let table_handle = pinned_table_handle(name, metadata, Some(snapshot_id))?;
                let procedure_handle =
                    IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(
                        IcebergRewritePositionDeleteFilesHandle::try_new(
                            table_handle.clone(),
                            IcebergRewriteArtifactContentId::try_new(
                                group.operation_id(),
                                group.artifact_digest_hex(),
                            )?,
                            group.group_digest_hex(),
                        )?
                        .with_content(content),
                    );
                Some(crate::typed_read::IcebergRuntimeRelation::TableExecute(
                    IcebergTableExecuteHandle::try_new(IcebergTableExecuteHandleParams {
                        schema_table_name: name.clone(),
                        procedure_id: procedure_handle.procedure_id(),
                        table_location: table_handle.table_location().to_string(),
                        procedure_handle: Some(procedure_handle),
                    })?,
                ))
            }
            K::Metadata { .. } => {
                let (base_table, relation) =
                    system_relation_of(name.table_name()).ok_or_else(|| {
                        corrupt("prepared metadata relation lost its original suffix")
                    })?;
                let (system_table_type, distribution) = relation.runtime_worker_plan();
                let base_name = SchemaTableName::try_new(name.schema_name(), &base_table)?;
                let metadata = physical.table.metadata();
                let reference = IcebergSystemTableReference::try_new(
            crate::typed_read::IcebergSystemTableReferenceParams {
                schema_table_name: base_name,
                system_table_type,
                metadata_file_location: physical
                    .table
                    .metadata_location()
                    .ok_or_else(|| {
                        corrupt(format!(
                            "iceberg relation {}.{base_table} has no metadata file location",
                            name.schema_name()
                        ))
                    })?
                    .to_string(),
                table_uuid: metadata.uuid().to_string(),
                snapshot_id: metadata.current_snapshot_id(),
            },
        )?;
                return Ok(Some(
                    novarocks_spi::connector::read_stack::adapter::ProviderReadOpenedRelation::new(
                        crate::typed_read::IcebergRuntimeRelation::SystemTable(reference),
                        Some(distribution),
                    ),
                ));
            }
        };
        Ok(table.map(|table| {
            novarocks_spi::connector::read_stack::adapter::ProviderReadOpenedRelation::new(
                table, None,
            )
        }))
    }

    type FrozenSource = Arc<crate::typed_read::frozen_source::IcebergCowPendingReadSource>;

    fn adopt_frozen_source(
        &self,
        _session: &ConnectorSession,
        owner: &novarocks_spi::connector::ConnectorProviderBindingKey,
        source: &Self::FrozenSource,
    ) -> Result<crate::typed_read::IcebergRuntimeRelation, ConnectorError> {
        use sha2::Digest;
        self.check_request_active()?;
        if owner != &source.owner
            || owner.instance_id != self.descriptor.instance_id
            || owner.incarnation != self.incarnation
        {
            return Err(invalid(
                "Iceberg COW source belongs to a different provider incarnation",
            ));
        }
        let sources = self.frozen_cow_sources.as_ref().ok_or_else(|| {
            invalid("Iceberg COW source adoption requires an admitted read request")
        })?;
        let snapshot = source.pinned.version_ordinal();
        let schema = pinned_schema(source.metadata.as_ref(), Some(snapshot))?;
        let name = SchemaTableName::try_new(source.pinned.namespace(), source.pinned.table())?;
        let files = IcebergPinnedDataFileSet::try_new(source.pinned.files())?;
        let mut columns = Vec::new();
        for metadata in ALWAYS_BOUND_METADATA_COLUMNS
            .into_iter()
            .chain(ROW_LINEAGE_METADATA_COLUMNS)
        {
            let Ok(signed) = source
                .signed_source
                .scan_schema()
                .field_with_name(metadata.column_name())
            else {
                if ALWAYS_BOUND_METADATA_COLUMNS.contains(&metadata) {
                    return Err(corrupt(
                        "Iceberg COW signed source lost an identity metadata field",
                    ));
                }
                continue;
            };
            let column = pseudo_column_with_nullable(metadata, signed.is_nullable())?;
            let authored =
                crate::typed_read::schema_binding::metadata_target_field(&column, metadata)?;
            if authored.data_type() != signed.data_type() {
                return Err(corrupt(
                    "Iceberg COW signed metadata type differs from the original field author",
                ));
            }
            columns.push(column);
        }
        let digest: [u8; 32] = sha2::Sha256::digest(source.original_source.payload()).into();
        let proof = crate::typed_read::cow_source_proof::IcebergCowSourceProof::try_new(
            digest,
            source.signed_base,
            source.read_file.path.clone(),
            columns,
        )?;
        let table = pinned_table_handle_with_schema_and_domain(
            &name,
            source.metadata.as_ref(),
            Some(snapshot),
            schema,
            Some(files),
            || Ok(Some(Arc::clone(source.read_file.read_domain()))),
        )?
        .with_frozen_cow_source(Some(proof))?;
        // No SDK load, manifest walk, current-name rebind, or fresh observation.
        sources
            .lock()
            .expect("request Iceberg COW source lock")
            .insert(digest, source.clone());
        Ok(crate::typed_read::IcebergRuntimeRelation::Table(table))
    }

    fn get_table_handle(
        &self,
        _session: &ConnectorSession,
        name: &SchemaTableName,
        version: novarocks_spi::connector::read_stack::ConnectorReadRelationVersion,
        reference: Option<&str>,
    ) -> Result<Option<crate::typed_read::IcebergRuntimeRelation>, ConnectorError> {
        if system_relation_of(name.table_name()).is_some() {
            return Ok(None);
        }
        let Some(physical) = self.load_relation(name)? else {
            return Ok(None);
        };
        Ok(Some(crate::typed_read::IcebergRuntimeRelation::Table(
            table_handle_for_version(name, physical.table.metadata(), version, reference)?,
        )))
    }

    fn get_pinned_file_set_handle(
        &self,
        _session: &ConnectorSession,
        name: &SchemaTableName,
        pinned: &ConnectorPinnedFileSet,
    ) -> Result<Option<crate::typed_read::IcebergRuntimeRelation>, ConnectorError> {
        if system_relation_of(name.table_name()).is_some() {
            return Ok(None);
        }
        let Some(physical) = self.load_relation(name)? else {
            return Ok(None);
        };
        let metadata = physical.table.metadata();
        let snapshot_id = pinned.version_ordinal();
        if metadata.snapshot_by_id(snapshot_id).is_none() {
            return Err(corrupt(format!(
                "iceberg relation {}.{} no longer holds snapshot {snapshot_id}, which a pinned read was frozen at",
                name.schema_name(),
                name.table_name()
            )));
        }
        let files = IcebergPinnedDataFileSet::try_new(pinned.files())?;
        if files.len() != pinned.files().len() {
            return Err(invalid(
                "iceberg pinned read was offered the same data file more than once",
            ));
        }
        // A pinned file set freezes row visibility, not a spelling of stable
        // Iceberg fields. When the current schema has exactly the same field
        // identities as the pinned snapshot, project the frozen files through
        // that current schema so a refresh rebind (for example `region` ->
        // `area`) and the connector bindings name the same field. Any
        // structural evolution remains on the historical schema and therefore
        // cannot be silently reinterpreted by this compatibility path.
        let schema = projection_schema_for_pinned_snapshot(metadata, snapshot_id)?;
        Ok(Some(crate::typed_read::IcebergRuntimeRelation::Table(
            pinned_table_handle_with_schema(
                name,
                metadata,
                Some(snapshot_id),
                schema,
                Some(files),
            )?,
        )))
    }

    fn get_mv_target_handle(
        &self,
        _session: &ConnectorSession,
        name: &SchemaTableName,
        object_id: &ConnectorTableObjectId,
        snapshot_id: Option<i64>,
    ) -> Result<Option<crate::typed_read::IcebergRuntimeRelation>, ConnectorError> {
        if system_relation_of(name.table_name()).is_some() {
            return Ok(None);
        }
        let Some(physical) = self.load_mv_target_relation(name)? else {
            return Ok(None);
        };
        let metadata = physical.table.metadata();
        validate_mv_target_pin(metadata, object_id, snapshot_id)?;
        let version = snapshot_id.map_or(
            ConnectorReadRelationVersion::Current,
            ConnectorReadRelationVersion::SnapshotId,
        );
        Ok(Some(crate::typed_read::IcebergRuntimeRelation::Table(
            table_handle_for_version(name, metadata, version, None)?,
        )))
    }

    fn get_mv_target_partition_handle(
        &self,
        _session: &ConnectorSession,
        name: &SchemaTableName,
        selection: &ConnectorMvTargetPartitionSelection,
    ) -> Result<Option<crate::typed_read::IcebergRuntimeRelation>, ConnectorError> {
        if system_relation_of(name.table_name()).is_some() {
            return Ok(None);
        }
        let Some(physical) = self.load_mv_target_relation(name)? else {
            return Ok(None);
        };
        let metadata = physical.table.metadata();
        validate_mv_target_partition_selection(metadata, selection)?;
        let schema = projection_schema_for_pinned_snapshot(metadata, selection.snapshot_id())?;
        let selected = if selection.keys().is_empty() {
            Some(IcebergPinnedDataFileSet::try_new(Vec::<String>::new())?)
        } else {
            self.check_request_active()?;
            let table = physical.table.clone();
            let snapshot_id = selection.snapshot_id();
            let control = self.request_context.clone();
            let result = self
                .runtime
                .resources()
                .catalog_runtime()
                .block_on(async move {
                    crate::read_snapshot::build_read_snapshot_at_with_control(
                        &table,
                        snapshot_id,
                        control
                            .as_ref()
                            .map(|control| control as &dyn ConnectorOperationControl),
                    )
                    .await
                });
            let snapshot = self.complete_sdk_read(result)?;
            select_mv_target_files(metadata, &snapshot, selection)
        };
        Ok(Some(crate::typed_read::IcebergRuntimeRelation::Table(
            pinned_table_handle_with_schema(
                name,
                metadata,
                Some(selection.snapshot_id()),
                schema,
                selected,
            )?,
        )))
    }

    fn get_column_bindings(
        &self,
        _session: &ConnectorSession,
        table: &crate::typed_read::IcebergRuntimeRelation,
    ) -> Result<Vec<ProviderReadColumnBinding<IcebergColumnHandle>>, ConnectorError> {
        let columns: Vec<(String, IcebergColumnHandle, bool)> = match table {
            crate::typed_read::IcebergRuntimeRelation::ChangeWindow(handle) => {
                let mut columns = Vec::with_capacity(handle.columns().len() + 1);
                for column in handle.columns() {
                    let name = column.base_column_identity().name();
                    if name == ICEBERG_CHANGE_OP_COLUMN {
                        return Err(unsupported(format!(
                            "iceberg relation {}.{} has a field named {ICEBERG_CHANGE_OP_COLUMN}, which a change window reserves for its sign",
                            handle.schema_table_name().schema_name(),
                            handle.schema_table_name().table_name()
                        )));
                    }
                    let hidden = ROW_LINEAGE_METADATA_COLUMNS
                        .iter()
                        .any(|metadata| metadata.field_id() == column.base_field_id());
                    columns.push((name.to_string(), column.clone(), hidden));
                }
                columns.push((
                    ICEBERG_CHANGE_OP_COLUMN.to_string(),
                    change_op_column_handle()?,
                    false,
                ));
                columns
            }
            crate::typed_read::IcebergRuntimeRelation::SystemTable(reference) => {
                let metadata = self.system_relation_metadata(reference)?;
                system_relation_columns_with_metadata(reference.system_table_type(), &metadata)?
                    .into_iter()
                    .map(|column| {
                        (
                            column.base_column_identity().name().to_string(),
                            column,
                            false,
                        )
                    })
                    .collect()
            }
            crate::typed_read::IcebergRuntimeRelation::TableExecute(handle) => {
                let mut columns = Vec::new();
                match handle.procedure_handle() {
                    Some(IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(_)) => {
                        for (name, metadata) in REWRITE_POSITION_DELETE_OUTPUT_COLUMNS {
                            columns.push((
                                name.to_string(),
                                rewrite_position_delete_pseudo_column(name, metadata)?,
                                true,
                            ));
                        }
                    }
                    Some(IcebergTableExecuteProcedureHandle::Optimize(_)) | None => {
                        return Err(unsupported(
                            "an iceberg optimize procedure reads the data relation's own columns",
                        ));
                    }
                }
                columns
            }
            crate::typed_read::IcebergRuntimeRelation::Table(handle) => {
                let schema = handle.parse_table_schema()?;
                let mut columns = Vec::new();
                for field in schema.as_struct().fields() {
                    columns.push((
                        field.name.to_string(),
                        IcebergColumnHandle::base_column(field.as_ref())?
                            .with_scalar_integer_domain(
                                handle.scalar_integer_domains().get(&field.id).copied(),
                            )?,
                        false,
                    ));
                }
                if let Some(source) = handle.frozen_cow_source() {
                    for column in source.metadata_columns() {
                        columns.push((
                            column.base_column_identity().name().to_string(),
                            column.clone(),
                            true,
                        ));
                    }
                } else {
                    for (name, column) in
                        metadata_pseudo_columns(self.relation_has_row_lineage(handle)?)?
                    {
                        columns.push((name.to_string(), column, true));
                    }
                }
                columns
            }
            crate::typed_read::IcebergRuntimeRelation::TableFunction(_) => {
                return Err(unsupported(
                    "an iceberg table-function relation has no data table handle",
                ));
            }
            crate::typed_read::IcebergRuntimeRelation::MergeTable(_) => {
                return Err(unsupported(
                    "an iceberg merge relation has no data table handle",
                ));
            }
        };
        Ok(columns
            .into_iter()
            .map(|(name, column, hidden)| ProviderReadColumnBinding::new(name, column, hidden))
            .collect())
    }

    fn final_static_facts(
        &self,
        _session: &ConnectorSession,
        table: &crate::typed_read::IcebergRuntimeRelation,
    ) -> Result<ConnectorReadStaticFacts<Self::Column>, ConnectorError> {
        iceberg_final_static_facts(table)
    }

    fn read_public_schema(
        &self,
        _session: &ConnectorSession,
        table: &crate::typed_read::IcebergRuntimeRelation,
        columns: &[&IcebergColumnHandle],
        creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorReadPublicSchema,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        match table {
            crate::typed_read::IcebergRuntimeRelation::SystemTable(reference) => {
                creation.work(1)?;
                let metadata = self.system_relation_metadata(reference)?;
                creation.work(1)?;
                publish_read_schema_with_metadata(
                    reference.system_table_type(),
                    columns,
                    &metadata,
                    creation,
                )
            }
            relation => public_read_schema(relation, columns, creation),
        }
    }

    fn apply_filter(
        &self,
        _session: &ConnectorSession,
        table: &crate::typed_read::IcebergRuntimeRelation,
        constraint: &Constraint<IcebergColumnHandle>,
    ) -> Result<Option<ProviderReadFilterApplication<Self::Table, Self::Column>>, ConnectorError>
    {
        let crate::typed_read::IcebergRuntimeRelation::Table(handle) = table else {
            return Ok(None);
        };
        if !handle.accepts_pushdown() {
            return Ok(None);
        }
        let applied = handle.apply_filter(constraint)?;
        if applied.handle() == handle {
            return Ok(None);
        }
        let remaining_expression = applied.remaining_expression().cloned();
        let remaining_constraint = Constraint::try_new(
            applied.remaining_filter().clone(),
            remaining_expression
                .clone()
                .unwrap_or_else(ConnectorExpression::constant_true),
            constraint.assignments().clone(),
        )?;
        Ok(Some(ProviderReadFilterApplication::new(
            crate::typed_read::IcebergRuntimeRelation::Table(applied.into_handle()),
            remaining_constraint,
            remaining_expression,
        )))
    }

    fn apply_projection(
        &self,
        _session: &ConnectorSession,
        table: &crate::typed_read::IcebergRuntimeRelation,
        assignments: &[Assignment<IcebergColumnHandle>],
    ) -> Result<Option<Self::Table>, ConnectorError> {
        let crate::typed_read::IcebergRuntimeRelation::Table(handle) = table else {
            return Ok(None);
        };
        let applied =
            handle.apply_projection(&OrderedAssignments::try_new(assignments.to_vec())?)?;
        if applied.handle() == handle {
            return Ok(None);
        }
        Ok(Some(crate::typed_read::IcebergRuntimeRelation::Table(
            applied.into_handle(),
        )))
    }

    fn apply_limit(
        &self,
        _session: &ConnectorSession,
        table: &crate::typed_read::IcebergRuntimeRelation,
        limit: u64,
    ) -> Result<Option<ProviderReadLimitApplication<Self::Table>>, ConnectorError> {
        let crate::typed_read::IcebergRuntimeRelation::Table(handle) = table else {
            return Ok(None);
        };
        if !handle.accepts_pushdown() {
            return Ok(None);
        }
        let applied = handle.apply_limit(limit)?;
        if applied.handle() == handle {
            return Ok(None);
        }
        let guaranteed = applied.limit_guaranteed();
        Ok(Some(ProviderReadLimitApplication::new(
            crate::typed_read::IcebergRuntimeRelation::Table(applied.into_handle()),
            guaranteed,
        )))
    }

    fn get_system_table_plan(
        &self,
        _session: &ConnectorSession,
        name: &SchemaTableName,
    ) -> Result<Option<ProviderReadSystemTablePlan<Self::Table>>, ConnectorError> {
        let Some((base_table, relation)) = system_relation_of(name.table_name()) else {
            return Ok(None);
        };
        let (system_table_type, distribution) = relation.runtime_worker_plan();
        let base_name = SchemaTableName::try_new(name.schema_name(), &base_table)?;
        let Some(physical) = self.load_relation(&base_name)? else {
            return Ok(None);
        };
        let metadata = physical.table.metadata();
        let reference = IcebergSystemTableReference::try_new(
            crate::typed_read::IcebergSystemTableReferenceParams {
                schema_table_name: base_name,
                system_table_type,
                metadata_file_location: physical
                    .table
                    .metadata_location()
                    .ok_or_else(|| {
                        corrupt(format!(
                            "iceberg relation {}.{base_table} has no metadata file location",
                            name.schema_name()
                        ))
                    })?
                    .to_string(),
                table_uuid: metadata.uuid().to_string(),
                snapshot_id: metadata.current_snapshot_id(),
            },
        )?;
        Ok(Some(ProviderReadSystemTablePlan::new(
            crate::typed_read::IcebergRuntimeRelation::SystemTable(reference),
            distribution,
        )))
    }

    fn get_system_table_plan_for_request(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        request: &ConnectorReadMetadataRequest,
    ) -> Result<Option<ProviderReadSystemTablePlan<Self::Table>>, ConnectorError> {
        let Some((_, relation)) = system_relation_of(name.table_name()) else {
            return Ok(None);
        };
        let (system_table_type, _) = relation.runtime_worker_plan();
        if request.kind().as_str() != system_table_type.suffix() {
            return Err(invalid(format!(
                "iceberg metadata request kind {} does not match relation {}",
                request.kind().as_str(),
                system_table_type.suffix()
            )));
        }
        match request.version() {
            ConnectorReadMetadataVersion::Current => self.get_system_table_plan(session, name),
            ConnectorReadMetadataVersion::SnapshotId(snapshot_id) => Err(unsupported(format!(
                "iceberg metadata snapshot request {snapshot_id} is unsupported because this read handle cannot prove the matching immutable metadata-file identity"
            ))),
            ConnectorReadMetadataVersion::TimestampMillis(timestamp_millis) => {
                Err(unsupported(format!(
                    "iceberg metadata timestamp request {timestamp_millis} is unsupported because this read handle cannot prove the matching immutable metadata-file identity"
                )))
            }
        }
    }

    fn get_change_window_plan(
        &self,
        _session: &ConnectorSession,
        name: &SchemaTableName,
        window: novarocks_spi::connector::read_stack::ConnectorReadChangeWindow,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        if system_relation_of(name.table_name()).is_some() {
            return Ok(None);
        }
        let Some(physical) = self.load_relation(name)? else {
            return Ok(None);
        };
        Ok(
            pinned_change_window_handle(name, physical.table.metadata(), window)?
                .map(crate::typed_read::IcebergRuntimeRelation::ChangeWindow),
        )
    }

    fn get_table_execute_plan(
        &self,
        _session: &ConnectorSession,
        name: &SchemaTableName,
        procedure: novarocks_spi::connector::read_stack::ConnectorReadTableExecuteProcedure,
    ) -> Result<Option<Self::Table>, ConnectorError> {
        let novarocks_spi::connector::read_stack::ConnectorReadTableExecuteProcedure::RewritePositionDeleteFiles(group) = procedure;
        group
            .content()
            .validate_owner(&novarocks_spi::connector::ConnectorProviderBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.incarnation,
            })?;
        // Reuse the existing frozen-artifact construction, but retain the
        // resulting concrete handle instead of encoding it into a carrier.
        if system_relation_of(name.table_name()).is_some() {
            return Ok(None);
        }
        let Some(physical) = self.load_relation(name)? else {
            return Ok(None);
        };
        let metadata = physical.table.metadata();
        let content = group
            .content()
            .value::<crate::distributed_rewrite::FrozenRewritePlanContent>(
                self.descriptor.instance_id.as_str(),
            )?
            .clone();
        content.validate_identity(group.operation_id(), group.artifact_digest_hex())?;
        content.group(group.group_digest_hex())?;
        let snapshot_id = validate_rewrite_position_delete_artifact(name, metadata, &content)?;
        let table_handle = pinned_table_handle(name, metadata, Some(snapshot_id))?;
        let procedure_handle = IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(
            IcebergRewritePositionDeleteFilesHandle::try_new(
                table_handle.clone(),
                IcebergRewriteArtifactContentId::try_new(
                    group.operation_id(),
                    group.artifact_digest_hex(),
                )?,
                group.group_digest_hex(),
            )?
            .with_content(content),
        );
        Ok(Some(
            crate::typed_read::IcebergRuntimeRelation::TableExecute(
                IcebergTableExecuteHandle::try_new(IcebergTableExecuteHandleParams {
                    schema_table_name: name.clone(),
                    procedure_id: procedure_handle.procedure_id(),
                    table_location: table_handle.table_location().to_string(),
                    procedure_handle: Some(procedure_handle),
                })?,
            ),
        ))
    }
}

/// Check every identity fence the frozen rewrite artifact carries before the
/// TableExecute relation can admit split dispatch or any writer side effect.
fn validate_rewrite_position_delete_artifact(
    name: &SchemaTableName,
    metadata: &TableMetadata,
    artifact: &crate::distributed_rewrite::IcebergFrozenRewriteArtifactV1,
) -> Result<i64, ConnectorError> {
    let snapshot_id = artifact.base_snapshot_id.ok_or_else(|| {
        invalid(format!(
            "iceberg rewrite-position artifact for {}.{} has no base snapshot",
            name.schema_name(),
            name.table_name()
        ))
    })?;
    if artifact.operation_kind != REWRITE_POSITION_DELETES_KIND
        || artifact.namespace != name.schema_name()
        || artifact.table != name.table_name()
        || artifact.target_ref != "main"
        || metadata.uuid().to_string() != artifact.table_uuid
        || metadata.current_snapshot_id() != Some(snapshot_id)
        || metadata.current_schema_id() != artifact.schema_id
        || metadata.default_partition_spec_id() != artifact.default_spec_id
    {
        return Err(invalid(format!(
            "iceberg rewrite-position artifact does not match the frozen relation {}.{}",
            name.schema_name(),
            name.table_name()
        )));
    }
    if metadata.snapshot_by_id(snapshot_id).is_none() {
        return Err(invalid(format!(
            "iceberg rewrite-position artifact base snapshot {snapshot_id} is unavailable for {}.{}",
            name.schema_name(),
            name.table_name()
        )));
    }
    Ok(snapshot_id)
}

/// Preserve the original no-read case before starting a fresh observation.
/// Only an exact successful None predicate proves emptiness; neither a policy
/// label nor an unavailable observation is a substitute for that proof.
fn known_empty_prepared_table_source(
    request: &novarocks_spi::connector::read_stack::adapter::ProviderReadSplitOpenRequest<
        IcebergTypedBoundary,
    >,
    options: IcebergSplitSourceOptions,
) -> Result<Option<Box<dyn ProviderReadSplitSource<IcebergTypedBoundary>>>, ConnectorError> {
    let crate::typed_read::IcebergRuntimeRelation::Table(handle) = request.table.as_ref() else {
        return Ok(None);
    };
    if handle.pinned_data_files().is_some() {
        return Ok(None);
    }
    let predicate = handle
        .effective_predicate()
        .and_then(|predicate| predicate.intersect(request.constraint.summary()));
    if predicate
        .as_ref()
        .is_ok_and(|predicate| !predicate.is_none())
    {
        return Ok(None);
    }
    // Keep the old Table order: assignments/projection, schema, then the
    // selected predicate refusal or known-empty source. An erroneous predicate
    // cannot hide an earlier projection/type refusal.
    let enumeration = if request.columns.is_empty() {
        handle.clone()
    } else {
        handle
            .apply_projection(&OrderedAssignments::try_new(request.columns.clone())?)?
            .into_handle()
    };
    handle.parse_table_schema()?;
    let predicate = predicate?;
    if !predicate.is_none() {
        return Ok(None);
    }
    let wait = if request.dynamic_filter_columns.is_empty() {
        std::time::Duration::ZERO
    } else {
        std::time::Duration::from_secs(1)
    };
    Ok(Some(Box::new(
        IcebergRuntimeSplitSource::new(IcebergSplitSource::try_new(
            &enumeration,
            Vec::new(),
            options,
        )?)
        .with_initial_dynamic_filter_wait(wait),
    )))
}

impl novarocks_spi::connector::read_stack::adapter::ProviderReadSplitManager
    for IcebergTypedBoundary
{
    fn begin_get_splits(
        self: Arc<Self>,
        request: novarocks_spi::connector::read_stack::adapter::ProviderReadSplitOpenRequest<Self>,
        original_owner: &mut Option<novarocks_spi::connector::ConnectorProcessLocalPayload>,
    ) -> Result<
        futures::future::BoxFuture<
            'static,
            Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError>,
        >,
        ConnectorError,
    > {
        if original_owner.is_some() {
            return Err(invalid("split opening owner is occupied"));
        }
        let receipt = self.prepared_source_access(request.table.as_ref())?;
        let Some(receipt) = receipt else {
            return Ok(Box::pin(async move {
                self.get_splits(
                    &request.session,
                    &request.table,
                    &request.columns,
                    &request.dynamic_filter_columns,
                    &request.constraint,
                )
                .await
            }));
        };
        let preparation = request
            .preparation
            .clone()
            .ok_or_else(|| invalid("prepared source has no original split preparation packet"))?;
        self.check_request_active()?;
        crate::catalog_control::statistics::scoped_cpu::scoped_prepared_split::validate_prepared_split_request(
            self.as_ref(), &request, &receipt,
        )?;
        if receipt.projection == ConnectorReadSourceProjection::Delta
            && matches!(
                receipt.source.admission,
                Some(novarocks_spi::connector::ConnectorChangeWindowAdmission::MetadataOnly)
            )
        {
            let crate::typed_read::IcebergRuntimeRelation::ChangeWindow(handle) =
                request.table.as_ref()
            else {
                return Err(invalid("MetadataOnly Delta lost its exact typed window"));
            };
            let outcome = crate::typed_read::IcebergChangeWindowPlan::try_plan(
                handle.clone(),
                crate::typed_read::IcebergEndpointVisibility::Proven,
                Vec::new(),
            )?;
            let crate::typed_read::IcebergChangeWindowPlanOutcome::Incremental(plan) = outcome
            else {
                return Err(invalid(
                    "proven MetadataOnly Delta did not form an empty plan",
                ));
            };
            // Exact original receipt is the proof of zero contribution. No
            // fresh loader, CPU job, Source owner or retention guard is created.
            return Ok(Box::pin(async move {
                Ok(Box::new(IcebergRuntimeSplitSource::new(
                    IcebergChangeWindowSplitSource::new(plan),
                )) as Box<dyn ProviderReadSplitSource<Self>>)
            }));
        }
        if let Some(empty) = known_empty_prepared_table_source(&request, self.split_source_options)?
        {
            return Ok(Box::pin(async move { Ok(empty) }));
        }
        let source = crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal::prepared_split_source(
            Arc::clone(&self), request, receipt,
        )?;
        *original_owner = Some(novarocks_spi::connector::ConnectorProcessLocalPayload::new(
            novarocks_spi::connector::ConnectorProviderBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.incarnation,
            },
            Arc::clone(&source),
        ));
        Ok(Box::pin(async move {
            source.drive_prepared_split(preparation.cpu()).await?;
            source.take_prepared_split()
        }))
    }

    fn get_splits<'a>(
        &'a self,
        _session: &'a ConnectorSession,
        table: &'a Self::Table,
        columns: &'a [Assignment<IcebergColumnHandle>],
        dynamic_filter_columns: &'a BTreeSet<IcebergColumnHandle>,
        constraint: &'a Constraint<IcebergColumnHandle>,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError>,
    > {
        Box::pin(async move {
            if self.prepared_source_access(table)?.is_some() {
                return Err(invalid(
                    "prepared source must open through its original closed split source",
                ));
            }
            let result: Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError> = match table
            {
                crate::typed_read::IcebergRuntimeRelation::ChangeWindow(handle) => Ok(Box::new(
                    IcebergRuntimeSplitSource::new(self.change_window_split_source(handle).await?),
                )),
                crate::typed_read::IcebergRuntimeRelation::SystemTable(reference) => {
                    self.system_relation_runtime_split_source(reference).await
                }
                crate::typed_read::IcebergRuntimeRelation::TableExecute(handle) => {
                    self.table_execute_runtime_split_source(handle).await
                }
                crate::typed_read::IcebergRuntimeRelation::Table(handle) => {
                    let enumeration_handle = if columns.is_empty() {
                        handle.clone()
                    } else {
                        handle
                            .apply_projection(&OrderedAssignments::try_new(columns.to_vec())?)?
                            .into_handle()
                    };
                    let files = match handle.snapshot_id() {
                        None => Vec::new(),
                        Some(snapshot_id) => {
                            self.planned_files_runtime(
                                handle,
                                snapshot_id,
                                constraint,
                                dynamic_filter_columns,
                            )
                            .await?
                        }
                    };
                    let initial_dynamic_filter_wait = if !dynamic_filter_columns.is_empty() {
                        std::time::Duration::from_secs(1)
                    } else {
                        std::time::Duration::default()
                    };
                    Ok(Box::new(
                        IcebergRuntimeSplitSource::new(IcebergSplitSource::try_new(
                            &enumeration_handle,
                            files,
                            self.split_source_options,
                        )?)
                        .with_initial_dynamic_filter_wait(initial_dynamic_filter_wait),
                    ))
                }
                crate::typed_read::IcebergRuntimeRelation::TableFunction(_) => Err(unsupported(
                    "iceberg table-function split enumeration is not implemented",
                )),
                crate::typed_read::IcebergRuntimeRelation::MergeTable(_) => Err(unsupported(
                    "iceberg merge split enumeration is not implemented",
                )),
            };
            result
        })
    }
}

impl IcebergTypedBoundary {
    pub(crate) fn plan_prepared_source_endpoint(
        &self,
        request: &novarocks_spi::connector::read_stack::adapter::ProviderReadSplitOpenRequest<Self>,
        domain: &Arc<crate::delete_semantics::ReadDomain>,
        snapshot: crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::ScopedReadSnapshot,
    ) -> Result<crate::catalog_control::statistics::scoped_cpu::scoped_prepared_split::PreparedSplitEndpoint, ConnectorError>{
        let crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::ScopedReadSnapshot {
            files,
            retention_guard,
            ..
        } = snapshot;
        let files = match request.table.as_ref() {
            crate::typed_read::IcebergRuntimeRelation::ChangeWindow(_) => {
                let frozen = self.pinned_table_for_attempt(request.table.schema_table_name())?;
                ChangeWindowEndpointProjection::new(frozen.table.metadata(), domain)?
                    .plan(files, self.request_context.as_ref())?
            }
            crate::typed_read::IcebergRuntimeRelation::Table(handle) => {
                if handle.read_domain() != Some(domain) {
                    return Err(invalid(
                        "prepared Table endpoint differs from its frozen domain",
                    ));
                }
                self.planned_files_from_observation(
                    handle,
                    domain.endpoint().snapshot_id(),
                    files,
                    &request.constraint,
                    &request.dynamic_filter_columns,
                )?
            }
            _ => return Err(invalid("prepared source has an unsupported relation kind")),
        };
        Ok(crate::catalog_control::statistics::scoped_cpu::scoped_prepared_split::PreparedSplitEndpoint {
            files, retention: retention_guard,
        })
    }

    pub(crate) fn finish_prepared_source_splits(
        &self,
        request: novarocks_spi::connector::read_stack::adapter::ProviderReadSplitOpenRequest<Self>,
        from: Option<crate::catalog_control::statistics::scoped_cpu::scoped_prepared_split::PreparedSplitEndpoint>,
        selected: crate::catalog_control::statistics::scoped_cpu::scoped_prepared_split::PreparedSplitEndpoint,
    ) -> Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError> {
        use crate::catalog_control::statistics::scoped_cpu::scoped_prepared_split::PreparedSplitEndpoint;
        self.check_request_active()?;
        let PreparedSplitEndpoint { files, retention } = selected;
        match request.table.as_ref() {
            crate::typed_read::IcebergRuntimeRelation::ChangeWindow(handle) => {
                let PreparedSplitEndpoint {
                    files: from_files,
                    retention: from_retention,
                } = from.ok_or_else(|| invalid("prepared Delta has no original From endpoint"))?;
                let plan = plan_change_window_splits(
                    handle,
                    IcebergChangeWindowEndpoints {
                        from_visible: &from_files,
                        to_visible: &files,
                    },
                    &change_window_partition_types(handle.to_read_domain())?,
                    self.split_source_options,
                )?;
                Ok(Box::new(
                    IcebergRuntimeSplitSource::new(IcebergChangeWindowSplitSource::new(plan))
                        .with_source_retention([Some(from_retention), Some(retention)]),
                ))
            }
            crate::typed_read::IcebergRuntimeRelation::Table(handle) => {
                if from.is_some() {
                    return Err(invalid("prepared Table received two endpoint projections"));
                }
                let enumeration_handle = if request.columns.is_empty() {
                    handle.clone()
                } else {
                    handle
                        .apply_projection(&OrderedAssignments::try_new(request.columns)?)?
                        .into_handle()
                };
                let wait = if request.dynamic_filter_columns.is_empty() {
                    std::time::Duration::ZERO
                } else {
                    std::time::Duration::from_secs(1)
                };
                Ok(Box::new(
                    IcebergRuntimeSplitSource::new(IcebergSplitSource::try_new(
                        &enumeration_handle,
                        files,
                        self.split_source_options,
                    )?)
                    .with_initial_dynamic_filter_wait(wait)
                    .with_source_retention([None, Some(retention)]),
                ))
            }
            _ => Err(invalid("prepared source has an unsupported relation kind")),
        }
    }

    async fn planned_files_runtime(
        &self,
        handle: &IcebergTableHandle,
        snapshot_id: i64,
        constraint: &Constraint<IcebergColumnHandle>,
        dynamic_filter_columns: &BTreeSet<IcebergColumnHandle>,
    ) -> Result<Vec<IcebergPlannedDataFile>, ConnectorError> {
        let schema = handle.parse_table_schema()?;
        if let Some(source) = self.adopted_cow_source(handle)? {
            self.check_request_active()?;
            let read_file = source.read_file.as_ref().clone();
            let partition_spec = read_file
                .partition_spec_id
                .map(|id| handle.parse_partition_spec(id))
                .transpose()?;
            validate_scalar_integer_manifest_facts(
                handle.scalar_integer_domains(),
                read_file.manifest.as_ref(),
                partition_spec.as_ref(),
                read_file.partition_values.as_ref(),
            )?;
            let planned = planned_data_file(
                read_file,
                &schema,
                partition_spec.as_ref(),
                dynamic_filter_columns,
            )?;
            self.check_request_active()?;
            return Ok(vec![planned]);
        }
        if handle.pinned_data_files().is_none()
            && handle
                .effective_predicate()?
                .intersect(constraint.summary())?
                .is_none()
        {
            return Ok(Vec::new());
        }
        let physical = self
            .load_pinned_relation_async(handle.schema_table_name())
            .await?;
        self.check_request_active()?;
        let table = physical.table.clone();
        let control = self.request_context.clone();
        let domain = handle
            .read_domain()
            .cloned()
            .ok_or_else(|| corrupt("snapshot handle has no read domain"))?;
        let result =
            (async move { plan_pinned_snapshot(table, domain, control.as_ref()).await }).await;
        self.check_request_active()?;
        let read_snapshot = result.map_err(unavailable)?;
        self.planned_files_from_observation(
            handle,
            snapshot_id,
            read_snapshot.files,
            constraint,
            dynamic_filter_columns,
        )
    }

    fn planned_files_from_observation(
        &self,
        handle: &IcebergTableHandle,
        snapshot_id: i64,
        read_files: Vec<IcebergReadFile>,
        constraint: &Constraint<IcebergColumnHandle>,
        dynamic_filter_columns: &BTreeSet<IcebergColumnHandle>,
    ) -> Result<Vec<IcebergPlannedDataFile>, ConnectorError> {
        let schema = handle.parse_table_schema()?;
        let pinned = handle.pinned_data_files();
        let predicates = if pinned.is_some() {
            Vec::new()
        } else {
            let predicate = handle
                .effective_predicate()?
                .intersect(constraint.summary())?;
            if predicate.is_none() {
                return Ok(Vec::new());
            }
            physical_predicates(&predicate, &schema)
        };
        let mut planned = Vec::with_capacity(read_files.len());
        let mut pinned_seen = 0_usize;
        for read_file in read_files {
            self.check_request_active()?;
            if let Some(pinned) = pinned {
                if !pinned.contains(&read_file.path) {
                    continue;
                }
                pinned_seen += 1;
            }
            let partition_spec = read_file
                .partition_spec_id
                .map(|spec_id| handle.parse_partition_spec(spec_id))
                .transpose()?;
            let data_facts = read_file.manifest.as_ref();
            // Validate supplied narrow-domain facts before they can prove that
            // a file is prunable. Missing bounds remain conservative.
            validate_scalar_integer_manifest_facts(
                handle.scalar_integer_domains(),
                data_facts,
                partition_spec.as_ref(),
                read_file.partition_values.as_ref(),
            )?;
            if pinned.is_none()
                && !predicates.is_empty()
                && !file_may_satisfy_physical_predicates(
                    &pruning_view(handle, &schema, &read_file)?,
                    &predicates,
                )
            {
                continue;
            }
            planned.push(planned_data_file(
                read_file,
                &schema,
                partition_spec.as_ref(),
                dynamic_filter_columns,
            )?);
        }
        if let Some(pinned) = pinned
            && pinned_seen != pinned.len()
        {
            return Err(corrupt(format!(
                "iceberg pinned read of {}.{} names {} data files but snapshot {snapshot_id} holds only {pinned_seen} of them",
                handle.schema_table_name().schema_name(),
                handle.schema_table_name().table_name(),
                pinned.len(),
            )));
        }
        Ok(planned)
    }
}

/// The transport-neutral split source used by the generic SPI adapter.
///
/// It retains the established Iceberg enumeration lifecycle, but returns the
/// concrete split enum directly. Encoding happens only at the role's wire
/// egress through `IcebergConnectorReadWireAdapter`.
#[derive(Debug)]
struct IcebergRuntimeSplitSource<S> {
    inner: S,
    closed: bool,
    initial_dynamic_filter_wait: std::time::Duration,
    _source_retention: [Option<novarocks_spi::connector::ConnectorPayloadRetentionGuard>; 2],
}

impl<S> IcebergRuntimeSplitSource<S> {
    const fn new(inner: S) -> Self {
        Self {
            inner,
            closed: false,
            initial_dynamic_filter_wait: std::time::Duration::ZERO,
            _source_retention: [None, None],
        }
    }

    fn with_source_retention(
        mut self,
        retention: [Option<novarocks_spi::connector::ConnectorPayloadRetentionGuard>; 2],
    ) -> Self {
        self._source_retention = retention;
        self
    }

    fn with_initial_dynamic_filter_wait(mut self, wait: std::time::Duration) -> Self {
        self.initial_dynamic_filter_wait = wait;
        self
    }
}

impl<S> ProviderReadSplitSource<IcebergTypedBoundary> for IcebergRuntimeSplitSource<S>
where
    S: ConnectorSplitSource<Column = IcebergColumnHandle> + Send,
    S::Split: IntoIcebergRuntimeSplit,
{
    fn profile_snapshot(&self) -> novarocks_spi::connector::read_stack::SplitSourceProfile {
        self.inner.profile_snapshot()
    }

    fn initial_dynamic_filter_wait_request(&self) -> std::time::Duration {
        self.initial_dynamic_filter_wait
    }

    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &DynamicFilterSnapshot<IcebergColumnHandle>,
    ) -> Result<ConnectorSplitBatch<crate::typed_read::IcebergReadSplit>, ConnectorError> {
        if self.closed {
            return Ok(ConnectorSplitBatch::finished());
        }
        let batch = self.inner.next_batch(max_size, dynamic_filter)?;
        let no_more_splits = batch.no_more_splits();
        Ok(ConnectorSplitBatch::new(
            batch
                .into_splits()
                .into_iter()
                .map(IntoIcebergRuntimeSplit::into_runtime_split)
                .collect(),
            no_more_splits,
        ))
    }

    fn is_finished(&self) -> bool {
        self.closed || self.inner.is_finished()
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        self.inner.close()
    }
}

trait IntoIcebergRuntimeSplit {
    fn into_runtime_split(self) -> crate::typed_read::IcebergReadSplit;
}

impl IntoIcebergRuntimeSplit for IcebergSplit {
    fn into_runtime_split(self) -> crate::typed_read::IcebergReadSplit {
        crate::typed_read::IcebergReadSplit::Data(self)
    }
}

impl IntoIcebergRuntimeSplit for IcebergChangeSplit {
    fn into_runtime_split(self) -> crate::typed_read::IcebergReadSplit {
        crate::typed_read::IcebergReadSplit::ChangeWindow(self)
    }
}

impl IntoIcebergRuntimeSplit for SystemManifestSplit {
    fn into_runtime_split(self) -> crate::typed_read::IcebergReadSplit {
        crate::typed_read::IcebergReadSplit::SystemManifest(self)
    }
}

struct RuntimeSystemRelationSource {
    split: Option<SystemRelationSplit>,
}

impl ProviderReadSplitSource<IcebergTypedBoundary> for RuntimeSystemRelationSource {
    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &DynamicFilterSnapshot<IcebergColumnHandle>,
    ) -> Result<ConnectorSplitBatch<crate::typed_read::IcebergReadSplit>, ConnectorError> {
        if max_size == 0 {
            return Err(invalid("connector split batch size must be positive"));
        }
        if dynamic_filter.current_predicate().is_none() {
            self.split = None;
        }
        Ok(ConnectorSplitBatch::new(
            self.split
                .take()
                .into_iter()
                .map(crate::typed_read::IcebergReadSplit::SystemRelation)
                .collect(),
            true,
        ))
    }

    fn is_finished(&self) -> bool {
        self.split.is_none()
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.split = None;
        Ok(())
    }
}

impl IcebergTypedBoundary {
    async fn system_relation_runtime_split_source(
        &self,
        reference: &IcebergSystemTableReference,
    ) -> Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError> {
        if matches!(
            reference.system_table_type(),
            crate::typed_read::IcebergSystemTableType::Snapshots
                | crate::typed_read::IcebergSystemTableType::History
                | crate::typed_read::IcebergSystemTableType::Refs
                | crate::typed_read::IcebergSystemTableType::Manifests
        ) {
            return Ok(Box::new(RuntimeSystemRelationSource {
                split: Some(SystemRelationSplit::try_new(reference.clone())?),
            }));
        }
        if reference.snapshot_id().is_none() {
            return Ok(Box::new(RuntimeSystemRelationSource { split: None }));
        }
        let physical = self
            .load_pinned_relation_async(reference.schema_table_name())
            .await?;
        reference.verify_loaded_metadata(physical.table.metadata())?;
        let metadata = physical.table.metadata().clone();
        let specs = partition_specs_of(&metadata);
        let table_schema = metadata.current_schema().as_ref().clone();
        let mut partition_spec_jsons = BTreeMap::new();
        for spec in &specs {
            partition_spec_jsons.insert(
                spec.spec_id(),
                serde_json::to_string(spec).map_err(|error| {
                    corrupt(format!(
                        "iceberg partition spec is not serializable: {error}"
                    ))
                })?,
            );
        }
        let source = SystemManifestSplitSource::try_new(SystemManifestSplitSourceParams {
            table_has_variant_history: crate::manifest::table_has_variant_history(&metadata),
            manifests: self.pinned_snapshot_manifests(reference).await?,
            table_schema_json: serde_json::to_string(&table_schema).map_err(|error| {
                corrupt(format!("iceberg table schema is not serializable: {error}"))
            })?,
            metadata_table_schema_json:
                crate::typed_read::system_page_source::system_relation_schema_json_with_metadata(
                    reference.system_table_type(),
                    &metadata,
                )?,
            partition_spec_jsons,
            partition_column_type_json: partition_row_type_with_metadata(&metadata)?
                .as_ref()
                .map(derived_row_type_json)
                .transpose()?,
            partition_slot_field_ids: crate::partition_row::UnifiedPartitionRow::of(
                &table_schema,
                &specs,
            )?
            .field_ids(),
            bounds_column_type_json: bounds_row_type(&table_schema)?
                .as_ref()
                .map(derived_row_type_json)
                .transpose()?,
            encryption_key_id: None,
            reference: reference.clone(),
        })?;
        Ok(Box::new(IcebergRuntimeSplitSource::new(source)))
    }

    async fn table_execute_runtime_split_source(
        &self,
        handle: &IcebergTableExecuteHandle,
    ) -> Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError> {
        let rewrite = match handle.procedure_handle() {
            Some(IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(rewrite)) => {
                rewrite
            }
            Some(IcebergTableExecuteProcedureHandle::Optimize(_)) | None => {
                return Err(unsupported(
                    "an iceberg optimize procedure reads data splits, not a table-execute relation",
                ));
            }
        };
        let table_handle = rewrite.table_handle();
        let snapshot_id = table_handle.snapshot_id().ok_or_else(|| {
            corrupt("an iceberg rewrite position delete relation carries no pinned snapshot")
        })?;
        let request_context = self.request_context.as_ref().ok_or_else(|| {
            invalid("Iceberg rewrite planning requires the admitted request control")
        })?;
        let physical = self
            .load_pinned_relation_async(table_handle.schema_table_name())
            .await?;
        let (data_file, selected) =
            crate::distributed_rewrite::plan_rewrite_position_delete_splits_async(
                &physical.table,
                snapshot_id,
                rewrite.content()?.as_ref(),
                rewrite.group_digest_hex(),
                request_context,
            )
            .await?;
        let deletes = selected
            .iter()
            .map(rewrite_position_delete_file)
            .collect::<Result<Vec<_>, _>>()?;
        let split = IcebergRewritePositionDeleteFilesSplit::try_new(
            IcebergRewritePositionDeleteFilesSplitParams {
                data_file_path: data_file.path.clone(),
                data_file_size: data_file.size,
                partition_spec_id: data_file.partition_spec_id.ok_or_else(|| {
                    corrupt(format!(
                        "iceberg rewrite position delete data file {} records no partition spec",
                        data_file.path
                    ))
                })?,
                partition_data_json: data_file
                    .partition_key
                    .clone()
                    .unwrap_or_else(|| UNPARTITIONED_REWRITE_PARTITION_JSON.to_string()),
                selected_position_deletes: deletes,
                split_weight: SplitWeight::STANDARD,
            },
        )?;
        Ok(Box::new(OneRuntimeSplitSource::new(
            crate::typed_read::IcebergReadSplit::RewritePositionDeleteFiles(split),
        )))
    }
}

struct OneRuntimeSplitSource {
    split: Option<crate::typed_read::IcebergReadSplit>,
    closed: bool,
}

impl OneRuntimeSplitSource {
    const fn new(split: crate::typed_read::IcebergReadSplit) -> Self {
        Self {
            split: Some(split),
            closed: false,
        }
    }
}

impl ProviderReadSplitSource<IcebergTypedBoundary> for OneRuntimeSplitSource {
    fn next_batch(
        &mut self,
        _max_size: usize,
        _dynamic_filter: &DynamicFilterSnapshot<IcebergColumnHandle>,
    ) -> Result<ConnectorSplitBatch<crate::typed_read::IcebergReadSplit>, ConnectorError> {
        if self.closed {
            return Ok(ConnectorSplitBatch::finished());
        }
        let split = self.split.take().into_iter().collect();
        Ok(ConnectorSplitBatch::new(split, true))
    }

    fn is_finished(&self) -> bool {
        self.closed || self.split.is_none()
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        self.split = None;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Snapshot pinning and handle construction
// ---------------------------------------------------------------------------

/// Validate the persisted target identity and ordered partition contract
/// against one provider metadata generation before any file can be excluded.
fn validate_mv_target_partition_selection(
    metadata: &TableMetadata,
    selection: &ConnectorMvTargetPartitionSelection,
) -> Result<(), ConnectorError> {
    validate_mv_target_pin(
        metadata,
        selection.object_id(),
        Some(selection.snapshot_id()),
    )?;
    let spec_id = metadata.default_partition_spec_id();
    if selection.partition_spec_version().as_ref()
        != crate::storage_inspector::exact_partition_spec_version(spec_id).as_ref()
    {
        return Err(invalid(
            "MV target selection disagrees with the current partition spec",
        ));
    }
    let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
        corrupt(format!(
            "Iceberg table metadata does not carry default partition spec {spec_id}"
        ))
    })?;
    if selection.partition_fields().len() != spec.fields().len() {
        return Err(invalid(
            "MV target partition field count disagrees with Iceberg metadata",
        ));
    }
    for (provided, actual) in selection.partition_fields().iter().zip(spec.fields()) {
        if provided.partition_field_id().as_ref() != actual.field_id.to_be_bytes()
            || provided.source_target_field_id().as_ref() != actual.source_id.to_be_bytes()
            || !mv_target_transform_matches(provided.transform(), &actual.transform)
        {
            return Err(invalid(
                "MV target partition field disagrees with Iceberg metadata",
            ));
        }
    }
    Ok(())
}

fn validate_mv_target_pin(
    metadata: &TableMetadata,
    object_id: &ConnectorTableObjectId,
    snapshot_id: Option<i64>,
) -> Result<(), ConnectorError> {
    if object_id.as_bytes().as_ref() != metadata.uuid().to_string().as_bytes() {
        return Err(invalid(
            "MV target read names a different Iceberg table object",
        ));
    }
    match snapshot_id {
        Some(snapshot_id) if metadata.snapshot_by_id(snapshot_id).is_none() => {
            Err(not_found(format!(
                "frozen MV target snapshot {snapshot_id} is not present in this refs observation"
            )))
        }
        None if metadata.current_snapshot_id().is_some() => {
            Err(invalid("empty frozen MV target now has a snapshot"))
        }
        _ => Ok(()),
    }
}

fn mv_target_transform_matches(expected: &MvExactPartitionTransform, actual: &Transform) -> bool {
    matches!(
        (expected, actual),
        (MvExactPartitionTransform::Identity, Transform::Identity)
            | (MvExactPartitionTransform::Year, Transform::Year)
            | (MvExactPartitionTransform::Month, Transform::Month)
            | (MvExactPartitionTransform::Day, Transform::Day)
            | (MvExactPartitionTransform::Hour, Transform::Hour)
            | (MvExactPartitionTransform::Void, Transform::Void)
    ) || matches!(
        (expected, actual),
        (
            MvExactPartitionTransform::Bucket { num_buckets: left },
            Transform::Bucket(right)
        ) if left == right
    ) || matches!(
        (expected, actual),
        (MvExactPartitionTransform::Truncate { width: left }, Transform::Truncate(right))
            if left == right
    )
}

/// A `None` result is an unrestricted scan of the same pinned snapshot. A
/// historical/unknown file spec or an uncomparable value invalidates the
/// entire selection, including candidates already found in earlier files.
fn select_mv_target_files(
    metadata: &TableMetadata,
    snapshot: &IcebergReadSnapshot,
    selection: &ConnectorMvTargetPartitionSelection,
) -> Option<IcebergPinnedDataFileSet> {
    if snapshot.snapshot_id != Some(selection.snapshot_id()) {
        return None;
    }
    let current_spec_id = metadata.default_partition_spec_id();
    let mut selected = Vec::new();
    for file in &snapshot.files {
        if file.partition_spec_id != Some(current_spec_id) {
            return None;
        }
        let values = file.partition_values.as_ref()?.fields();
        if values.len() != selection.partition_fields().len() {
            return None;
        }
        let mut comparable = Vec::with_capacity(values.len());
        for value in values {
            comparable.push(mv_target_partition_value(value.as_ref())?);
        }
        if selection.keys().iter().any(|key| key == &comparable) {
            selected.push(file.path.as_str());
            if selected.len() > crate::typed_read::table_handle::MAX_PINNED_DATA_FILES {
                return None;
            }
        }
    }
    IcebergPinnedDataFileSet::try_new(selected).ok()
}

/// The same primitive spelling as Iceberg's change-window partition impact.
/// Values outside that vocabulary cannot justify excluding a target file.
fn mv_target_partition_value(value: Option<&Literal>) -> Option<ConnectorMvPartitionValue> {
    let Some(value) = value else {
        return Some(ConnectorMvPartitionValue::Null);
    };
    let Literal::Primitive(value) = value else {
        return None;
    };
    let string = match value {
        PrimitiveLiteral::Boolean(value) => value.to_string(),
        PrimitiveLiteral::Int(value) => value.to_string(),
        PrimitiveLiteral::Long(value) => value.to_string(),
        PrimitiveLiteral::Float(value) => value.0.to_string(),
        PrimitiveLiteral::Double(value) => value.0.to_string(),
        PrimitiveLiteral::String(value) => value.clone(),
        PrimitiveLiteral::Binary(_)
        | PrimitiveLiteral::Int128(_)
        | PrimitiveLiteral::UInt128(_)
        | PrimitiveLiteral::AboveMax
        | PrimitiveLiteral::BelowMin => return None,
    };
    Some(ConnectorMvPartitionValue::String(Arc::from(string)))
}

/// Resolve the requested version to exactly one snapshot, once.
///
/// A branch or tag is resolved here, by the connector, from the same metadata
/// the rest of the handle is frozen from. The engine never sees a reference
/// name again, so no later stage can resolve it to a different snapshot.
fn pin_snapshot(
    metadata: &TableMetadata,
    version: ConnectorReadRelationVersion,
    reference: Option<&str>,
) -> Result<Option<i64>, ConnectorError> {
    match version {
        ConnectorReadRelationVersion::Current => {
            if reference.is_some() {
                return Err(invalid(
                    "iceberg current-version read must not name a reference",
                ));
            }
            Ok(metadata.current_snapshot_id())
        }
        ConnectorReadRelationVersion::SnapshotId(snapshot_id) => {
            if reference.is_some() {
                return Err(invalid(
                    "iceberg snapshot-id read must not also name a reference",
                ));
            }
            metadata
                .snapshot_by_id(snapshot_id)
                .map(|_| Some(snapshot_id))
                .ok_or_else(|| not_found(format!("iceberg snapshot {snapshot_id} does not exist")))
        }
        ConnectorReadRelationVersion::Reference => {
            let reference = reference
                .ok_or_else(|| invalid("iceberg reference read requires a branch or tag name"))?;
            // `None` is an unborn branch: it exists and reads zero rows, which
            // is not the same as falling back to the current snapshot.
            resolve_branch_head_snapshot_id(metadata, reference).map_err(not_found)
        }
    }
}

/// The schema the pinned snapshot was written under.
///
/// Schema evolution after the snapshot must not retype a frozen read, so the
/// snapshot's own schema is used -- the same one the manifest walk resolves.
fn pinned_schema(
    metadata: &TableMetadata,
    snapshot_id: Option<i64>,
) -> Result<SchemaRef, ConnectorError> {
    match snapshot_id {
        Some(snapshot_id) => metadata
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| not_found(format!("iceberg snapshot {snapshot_id} does not exist")))?
            .schema(metadata)
            .map_err(|error| {
                corrupt(format!(
                    "iceberg snapshot {snapshot_id} names a schema the table metadata does not carry: {error}"
                ))
            }),
        // A relation with no snapshot reads zero rows; its current schema is
        // the only schema that has ever existed.
        None => Ok(metadata.current_schema().clone()),
    }
}

/// Freeze one worker-visible DATA relation handle.
fn pinned_table_handle(
    name: &SchemaTableName,
    metadata: &TableMetadata,
    snapshot_id: Option<i64>,
) -> Result<IcebergTableHandle, ConnectorError> {
    pinned_table_handle_with_files(name, metadata, snapshot_id, None)
}

fn table_handle_for_version(
    name: &SchemaTableName,
    metadata: &TableMetadata,
    version: ConnectorReadRelationVersion,
    reference: Option<&str>,
) -> Result<IcebergTableHandle, ConnectorError> {
    let snapshot_id = pin_snapshot(metadata, version, reference)?;
    let schema = match version {
        // A current read pins the current snapshot for row visibility but
        // projects it through the table's current schema. Iceberg field IDs
        // make metadata-only add/rename evolution readable without a new
        // snapshot.
        ConnectorReadRelationVersion::Current => metadata.current_schema().clone(),
        ConnectorReadRelationVersion::SnapshotId(_) | ConnectorReadRelationVersion::Reference => {
            projection_schema_for_pinned_snapshot(
                metadata,
                snapshot_id.ok_or_else(|| corrupt("pinned table read has no snapshot ID"))?,
            )?
        }
    };
    pinned_table_handle_with_schema(name, metadata, snapshot_id, schema, None)
}

/// Select the names a frozen snapshot projects without changing its field
/// identity. A rename preserves the complete field-ID tree, so current SQL
/// names remain safe for a historical read. Any structural change keeps the
/// snapshot's own schema rather than guessing a correspondence.
fn projection_schema_for_pinned_snapshot(
    metadata: &TableMetadata,
    snapshot_id: i64,
) -> Result<SchemaRef, ConnectorError> {
    let snapshot_schema = pinned_schema(metadata, Some(snapshot_id))?;
    Ok(
        if struct_types_share_field_identities(
            snapshot_schema.as_struct(),
            metadata.current_schema().as_struct(),
        ) {
            metadata.current_schema().clone()
        } else {
            snapshot_schema
        },
    )
}

fn pinned_table_handle_with_files(
    name: &SchemaTableName,
    metadata: &TableMetadata,
    snapshot_id: Option<i64>,
    pinned_data_files: Option<IcebergPinnedDataFileSet>,
) -> Result<IcebergTableHandle, ConnectorError> {
    let schema = pinned_schema(metadata, snapshot_id)?;
    pinned_table_handle_with_schema(name, metadata, snapshot_id, schema, pinned_data_files)
}

fn pinned_table_handle_with_schema(
    name: &SchemaTableName,
    metadata: &TableMetadata,
    snapshot_id: Option<i64>,
    schema: SchemaRef,
    pinned_data_files: Option<IcebergPinnedDataFileSet>,
) -> Result<IcebergTableHandle, ConnectorError> {
    // Preserve the original domain-mint position after the original serde work.
    pinned_table_handle_with_schema_and_domain(
        name,
        metadata,
        snapshot_id,
        schema.clone(),
        pinned_data_files,
        || {
            snapshot_id
                .map(|id| crate::read_snapshot::mint_read_domain(metadata, id, &schema))
                .transpose()
                .map_err(corrupt)
        },
    )
}

fn pinned_table_handle_with_schema_and_domain(
    name: &SchemaTableName,
    metadata: &TableMetadata,
    snapshot_id: Option<i64>,
    schema: SchemaRef,
    pinned_data_files: Option<IcebergPinnedDataFileSet>,
    read_domain: impl FnOnce()
        -> Result<Option<Arc<crate::delete_semantics::ReadDomain>>, ConnectorError>,
) -> Result<IcebergTableHandle, ConnectorError> {
    let mut partition_spec_jsons = BTreeMap::new();
    for spec in metadata.partition_specs_iter() {
        let json = serde_json::to_string(spec.as_ref()).map_err(|error| {
            corrupt(format!(
                "iceberg partition spec {} cannot be encoded: {error}",
                spec.spec_id()
            ))
        })?;
        partition_spec_jsons.insert(spec.spec_id(), json);
    }
    let spec_id = metadata.default_partition_spec_id();
    if !partition_spec_jsons.contains_key(&spec_id) {
        return Err(corrupt(format!(
            "iceberg table metadata names default partition spec {spec_id} but does not carry it"
        )));
    }
    let table_schema_json = serde_json::to_string(schema.as_ref())
        .map_err(|error| corrupt(format!("iceberg table schema cannot be encoded: {error}")))?;

    let document =
        crate::schema_mapping::nominal_declaration::TableNominalDocument::from_metadata(metadata)?;
    let read_domain = read_domain()?;
    IcebergTableHandle::try_new(IcebergTableHandleParams {
        schema_table_name: name.clone(),
        snapshot_id,
        read_domain,
        table_schema_json,
        spec_id: Some(spec_id),
        partition_spec_jsons,
        format_version: metadata.format_version() as i32,
        // Pushdown has not been offered yet; both predicates start unrestricted
        // rather than at some assumed default.
        unenforced_predicate: TupleDomain::all(),
        enforced_predicate: TupleDomain::all(),
        limit: None,
        projected_columns: BTreeSet::new(),
        name_mapping_json: metadata.properties().get(NAME_MAPPING_PROPERTY).cloned(),
        table_location: metadata.location().to_string(),
        storage_properties: reader_visible_storage_properties(metadata.properties()),
        pinned_data_files,
    })?
    .with_scalar_integer_domains(crate::scalar_integer_domain::of_schema(
        &schema,
        &document.integers,
    )?)?
    .with_nominal_declarations(
        crate::schema_mapping::nominal_declaration::restrict_to_schema(
            &schema,
            &document.nominals,
        )?,
    )
}

/// Freeze one worker-visible change-window relation handle.
///
/// The two answers this returns are deliberately different questions.
/// `Ok(None)` is a fact about the *relation*: a v1 table has no delete files at
/// all, so it can never express a row that stopped being visible, and this
/// connector exposes no change window over one. Every error below is a fact
/// about *this window*, and each is raised now rather than becoming a
/// difference that quietly means something else:
///
/// * an endpoint that is not in the metadata cannot be differenced at all;
/// * `to` must descend from `from`, because the two must be points on one
///   history for their difference to be the window's changes rather than the
///   distance between two unrelated branches;
/// * the handle carries exactly one schema, so the endpoints must agree on the
///   field identities it describes.
///
/// Both endpoints are pinned here, exactly as [`pinned_table_handle`] pins one
/// snapshot: nothing downstream resolves either of them again.
fn pinned_change_window_handle(
    name: &SchemaTableName,
    metadata: &TableMetadata,
    window: ConnectorReadChangeWindow,
) -> Result<Option<IcebergChangeWindowHandle>, ConnectorError> {
    pinned_change_window_handle_with_fallback(name, metadata, window, metadata.current_schema_id())
}

fn pinned_change_window_handle_with_fallback(
    name: &SchemaTableName,
    metadata: &TableMetadata,
    window: ConnectorReadChangeWindow,
    fallback_schema_id: i32,
) -> Result<Option<IcebergChangeWindowHandle>, ConnectorError> {
    match metadata.format_version() {
        FormatVersion::V2 | FormatVersion::V3 => {}
        FormatVersion::V1 => return Ok(None),
    }

    let from = window.from_snapshot_id();
    let to = window.to_snapshot_id();
    for snapshot_id in [from, to] {
        if metadata.snapshot_by_id(snapshot_id).is_none() {
            return Err(not_found(format!(
                "iceberg snapshot {snapshot_id} does not exist"
            )));
        }
    }
    if !snapshot_descends_from(metadata, to, from) {
        return Err(unsupported(format!(
            "iceberg snapshot {to} does not descend from snapshot {from}, so the two are not endpoints of one change window"
        )));
    }

    let from_schema = crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::frozen_snapshot_schema(metadata, from, fallback_schema_id)?;
    let to_schema = crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::frozen_snapshot_schema(metadata, to, fallback_schema_id)?;
    if !struct_types_share_field_identities(from_schema.as_struct(), to_schema.as_struct()) {
        // A rename preserves every field ID, so one frozen schema still
        // describes both endpoints. Anything else does not, and the single
        // `table_schema_json` this handle carries would misdescribe one side.
        return Err(unsupported(format!(
            "iceberg snapshots {from} and {to} do not share field identities, so one change-window schema cannot describe both"
        )));
    }

    let document =
        crate::schema_mapping::nominal_declaration::TableNominalDocument::from_metadata(metadata)?;
    let domains = &document.integers;
    let from_domains = crate::scalar_integer_domain::of_schema(&from_schema, domains)?;
    let to_domains = crate::scalar_integer_domain::of_schema(&to_schema, domains)?;
    if from_domains != to_domains {
        return Err(unsupported(
            "Iceberg change-window endpoints have different declared scalar integer domains",
        ));
    }
    change_window_handle_with_frozen_schema(name, metadata, window, to_schema).map(Some)
}

/// Static endpoint identities from the original frozen metadata, not evidence
/// of a physical read. MetadataOnly has no executable From projection and its
/// empty Delta never starts endpoint discovery.
fn change_window_handle_with_frozen_schema(
    name: &SchemaTableName,
    metadata: &TableMetadata,
    window: ConnectorReadChangeWindow,
    to_schema: SchemaRef,
) -> Result<IcebergChangeWindowHandle, ConnectorError> {
    let from = window.from_snapshot_id();
    let to = window.to_snapshot_id();
    let document =
        crate::schema_mapping::nominal_declaration::TableNominalDocument::from_metadata(metadata)?;
    let to_domains = crate::scalar_integer_domain::of_schema(&to_schema, &document.integers)?;
    let columns = change_window_columns(to_schema.as_ref(), row_lineage_enabled(metadata))?
        .into_iter()
        .map(|column| {
            let domain = to_domains.get(&column.base_field_id()).copied();
            column.with_scalar_integer_domain(domain)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let table_schema_json = serde_json::to_string(to_schema.as_ref())
        .map_err(|error| corrupt(format!("iceberg table schema cannot be encoded: {error}")))?;

    // Every spec, not the default one: a window spans two snapshots, so files
    // written under an older spec can appear on either side of the difference
    // and each split must be decodable from the handle alone.
    let mut partition_spec_jsons = BTreeMap::new();
    for spec in metadata.partition_specs_iter() {
        let json = serde_json::to_string(spec.as_ref()).map_err(|error| {
            corrupt(format!(
                "iceberg partition spec {} cannot be encoded: {error}",
                spec.spec_id()
            ))
        })?;
        partition_spec_jsons.insert(spec.spec_id(), json);
    }

    IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
        schema_table_name: name.clone(),
        table_schema_json,
        columns,
        name_mapping_json: metadata.properties().get(NAME_MAPPING_PROPERTY).cloned(),
        from_snapshot_id_exclusive: from,
        to_snapshot_id_inclusive: to,
        from_read_domain: crate::read_snapshot::mint_read_domain(metadata, from, &to_schema)
            .map_err(corrupt)?,
        to_read_domain: crate::read_snapshot::mint_read_domain(metadata, to, &to_schema)
            .map_err(corrupt)?,
        partition_spec_jsons,
    })?
    .with_nominal_declarations(
        crate::schema_mapping::nominal_declaration::restrict_to_schema(
            &to_schema,
            &document.nominals,
        )?,
    )
}

fn same_source_table(actual: &IcebergTableHandle, expected: &IcebergTableHandle) -> bool {
    actual.schema_table_name() == expected.schema_table_name()
        && actual.snapshot_id() == expected.snapshot_id()
        && actual.read_domain() == expected.read_domain()
}

pub(crate) fn frozen_prepared_source_relations(
    name: &SchemaTableName,
    metadata: &TableMetadata,
    selection: ConnectorReadSourceSelection,
    to_schema: SchemaRef,
    fallback_schema_id: i32,
    admission: Option<&novarocks_spi::connector::ConnectorChangeWindowAdmission>,
) -> Result<PreparedRelations, ConnectorError> {
    let to = match selection {
        ConnectorReadSourceSelection::Snapshot(to) => to,
        ConnectorReadSourceSelection::ChangeWindow(window) => window.to_snapshot_id(),
    };
    if matches!(
        admission,
        Some(novarocks_spi::connector::ConnectorChangeWindowAdmission::Incremental { .. })
    ) {
        let ConnectorReadSourceSelection::ChangeWindow(window) = selection else {
            return Err(invalid(
                "snapshot source cannot have an incremental decision",
            ));
        };
        let delta =
            pinned_change_window_handle_with_fallback(name, metadata, window, fallback_schema_id)?
                .ok_or_else(|| unsupported("incremental decision has no typed Delta relation"))?;
        let to = pinned_table_handle_with_schema_and_domain(
            name,
            metadata,
            Some(to),
            to_schema.clone(),
            None,
            || Ok(Some(Arc::clone(delta.to_read_domain()))),
        )?;
        let from = pinned_table_handle_with_schema_and_domain(
            name,
            metadata,
            Some(window.from_snapshot_id()),
            to_schema,
            None,
            || Ok(Some(Arc::clone(delta.from_read_domain()))),
        )?;
        Ok(PreparedRelations {
            to,
            from: Some(from),
            delta: Some(delta),
        })
    } else if matches!(
        admission,
        Some(novarocks_spi::connector::ConnectorChangeWindowAdmission::MetadataOnly)
    ) {
        let ConnectorReadSourceSelection::ChangeWindow(window) = selection else {
            return Err(invalid(
                "snapshot source cannot have a MetadataOnly window decision",
            ));
        };
        // A real MetadataOnly preparation proved this exact original lineage.
        // Reject contradictory private facts rather than inventing a From pin.
        if !matches!(
            metadata.format_version(),
            FormatVersion::V2 | FormatVersion::V3
        ) || metadata.snapshot_by_id(window.from_snapshot_id()).is_none()
            || metadata.snapshot_by_id(to).is_none()
            || !snapshot_descends_from(metadata, to, window.from_snapshot_id())
        {
            return Err(invalid(
                "MetadataOnly source has no exact original endpoint lineage",
            ));
        }
        let delta =
            change_window_handle_with_frozen_schema(name, metadata, window, to_schema.clone())?;
        let to = pinned_table_handle_with_schema_and_domain(
            name,
            metadata,
            Some(to),
            to_schema,
            None,
            || Ok(Some(Arc::clone(delta.to_read_domain()))),
        )?;
        Ok(PreparedRelations {
            to,
            from: None,
            delta: Some(delta),
        })
    } else {
        Ok(PreparedRelations {
            to: pinned_table_handle_with_schema(name, metadata, Some(to), to_schema, None)?,
            from: None,
            delta: None,
        })
    }
}

/// Whether `descendant` reaches `ancestor` by following parent pointers.
///
/// The walk is bounded by the number of snapshots the metadata carries, so a
/// corrupted parent cycle ends the search instead of hanging the coordinator.
fn snapshot_descends_from(metadata: &TableMetadata, descendant: i64, ancestor: i64) -> bool {
    let mut cursor = Some(descendant);
    for _ in 0..=metadata.snapshots().len() {
        let Some(snapshot_id) = cursor else {
            return false;
        };
        if snapshot_id == ancestor {
            return true;
        }
        cursor = metadata
            .snapshot_by_id(snapshot_id)
            .and_then(|snapshot| snapshot.parent_snapshot_id());
    }
    false
}

/// Whether two schemas describe the same fields, ignoring their names.
///
/// Field IDs and types are what a read binds to, so a pure rename leaves a
/// frozen read valid; anything else changes what a column *is*.
fn struct_types_share_field_identities(previous: &StructType, next: &StructType) -> bool {
    let previous = previous.fields();
    let next = next.fields();
    previous.len() == next.len()
        && previous.iter().zip(next.iter()).all(|(previous, next)| {
            previous.id == next.id
                && previous.required == next.required
                && types_share_field_identities(&previous.field_type, &next.field_type)
        })
}

fn types_share_field_identities(previous: &Type, next: &Type) -> bool {
    match (previous, next) {
        (Type::Primitive(previous), Type::Primitive(next)) => previous == next,
        (Type::Variant(previous), Type::Variant(next)) => previous == next,
        (Type::Struct(previous), Type::Struct(next)) => {
            struct_types_share_field_identities(previous, next)
        }
        (Type::List(previous), Type::List(next)) => {
            previous.element_field.id == next.element_field.id
                && previous.element_field.required == next.element_field.required
                && types_share_field_identities(
                    &previous.element_field.field_type,
                    &next.element_field.field_type,
                )
        }
        (Type::Map(previous), Type::Map(next)) => {
            previous.key_field.id == next.key_field.id
                && previous.value_field.id == next.value_field.id
                && previous.value_field.required == next.value_field.required
                && types_share_field_identities(
                    &previous.key_field.field_type,
                    &next.key_field.field_type,
                )
                && types_share_field_identities(
                    &previous.value_field.field_type,
                    &next.value_field.field_type,
                )
        }
        (
            Type::Primitive(_) | Type::Variant(_) | Type::Struct(_) | Type::List(_) | Type::Map(_),
            _,
        ) => false,
    }
}

/// The partition type of every spec the relation carries, keyed by spec id.
///
/// Enumeration needs these to encode a data file's frozen partition values;
/// the change-window handle itself carries no spec, so they are resolved from
/// the same metadata the endpoints were pinned from.
fn change_window_partition_types(
    domain: &crate::delete_semantics::ReadDomain,
) -> Result<BTreeMap<i32, Type>, ConnectorError> {
    domain
        .endpoint()
        .partition_type_jsons()
        .keys()
        .map(|id| {
            domain
                .endpoint()
                .partition_type(*id)
                .map(|ty| (*id, Type::Struct(ty)))
                .map_err(|e| corrupt(e.to_string()))
        })
        .collect()
}

/// The columns a change-window relation exposes.
///
/// They are the frozen relation's own fields, its frozen row-lineage metadata
/// columns when present, and `__change_op`. The sign is visible rather than
/// hidden because it is the point of the relation: a change row without it says
/// a row differs between the endpoints but not in which direction. It is also
/// the one column no file can supply -- the split variant is its only source --
/// so it is appended, never bound to a table field.
fn change_window_columns(
    schema: &Schema,
    row_lineage: bool,
) -> Result<Vec<IcebergColumnHandle>, ConnectorError> {
    let fields = schema.as_struct().fields();
    let mut columns = Vec::with_capacity(
        fields.len() + usize::from(row_lineage) * ROW_LINEAGE_METADATA_COLUMNS.len(),
    );
    for field in fields {
        columns.push(IcebergColumnHandle::base_column(field.as_ref())?);
    }
    if row_lineage {
        for metadata in ROW_LINEAGE_METADATA_COLUMNS {
            columns.push(pseudo_column(metadata)?);
        }
    }
    Ok(columns)
}

/// Keep only table properties provably safe to hand a worker.
fn reader_visible_storage_properties(
    properties: &HashMap<String, String>,
) -> BTreeMap<String, String> {
    READER_VISIBLE_TABLE_PROPERTIES
        .iter()
        .filter_map(|key| {
            properties
                .get(*key)
                .map(|value| ((*key).to_string(), value.clone()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Column bindings
// ---------------------------------------------------------------------------

/// The hidden Iceberg metadata columns of one data relation.
///
/// Each one is addressable by name but never part of `SELECT *`. The name is
/// the engine's own spelling -- `_file`, `_pos`, `_row_id`,
/// `_last_updated_sequence_number` -- because that is what a query writes and
/// what the frontend asks this relation to bind. Both the name and the declared
/// type come from [`IcebergMetadataColumn`], which is also what the reader binds
/// against, so a published identity and a readable one cannot drift apart.
///
/// `row_lineage` decides membership, not nullability: a table without row
/// lineage has no `first_row_id` to build `_row_id` from, so those two columns
/// are absent rather than bound and empty.
fn metadata_pseudo_columns(
    row_lineage: bool,
) -> Result<Vec<(&'static str, IcebergColumnHandle)>, ConnectorError> {
    let mut columns = Vec::with_capacity(
        ALWAYS_BOUND_METADATA_COLUMNS.len() + ROW_LINEAGE_METADATA_COLUMNS.len(),
    );
    for metadata in ALWAYS_BOUND_METADATA_COLUMNS {
        columns.push((metadata.column_name(), pseudo_column(metadata)?));
    }
    if row_lineage {
        for metadata in ROW_LINEAGE_METADATA_COLUMNS {
            columns.push((metadata.column_name(), pseudo_column(metadata)?));
        }
    }
    Ok(columns)
}

/// A metadata column's NULL contract is the column's own, the same one the
/// table metadata states to SQL: a frozen read publishes this handle's field,
/// and it must be exactly the type the scan was planned with.
fn pseudo_column(metadata: IcebergMetadataColumn) -> Result<IcebergColumnHandle, ConnectorError> {
    pseudo_column_with_nullable(metadata, metadata.nullable())
}

fn pseudo_column_with_nullable(
    metadata: IcebergMetadataColumn,
    nullable: bool,
) -> Result<IcebergColumnHandle, ConnectorError> {
    IcebergColumnHandle::base_column(&NestedField::new(
        metadata.field_id(),
        metadata.column_name(),
        Type::Primitive(metadata.declared_type()),
        !nullable,
    ))
}

/// A `REWRITE_POSITION_DELETE_FILES` result retains the reserved metadata
/// field IDs while exposing its procedure-specific output names on the wire.
fn rewrite_position_delete_pseudo_column(
    name: &str,
    metadata: IcebergMetadataColumn,
) -> Result<IcebergColumnHandle, ConnectorError> {
    crate::typed_read::table_execute::rewrite_position_delete_pseudo_column(name, metadata)
}

// ---------------------------------------------------------------------------
// System relation naming
// ---------------------------------------------------------------------------

/// Split `<table>$<suffix>` into its base relation and system relation.
///
/// Returns `None` for a name with no `$` and for an unknown suffix; both are
/// "this is not one of my system relations", never a guess.
fn system_relation_of(table_name: &str) -> Option<(String, IcebergSystemRelation)> {
    let (base_table, suffix) = table_name.rsplit_once('$')?;
    if base_table.is_empty() {
        return None;
    }
    let relation = match suffix.trim().to_ascii_uppercase().as_str() {
        "FILES" => IcebergSystemRelation::Files,
        "ENTRIES" => IcebergSystemRelation::Entries,
        "SNAPSHOTS" => IcebergSystemRelation::Snapshots,
        "HISTORY" => IcebergSystemRelation::History,
        "REFS" => IcebergSystemRelation::Refs,
        "MANIFESTS" => IcebergSystemRelation::Manifests,
        _ => return None,
    };
    Some((base_table.to_string(), relation))
}

// ---------------------------------------------------------------------------
// Manifest facts for split production
// ---------------------------------------------------------------------------

use crate::read_model::IcebergDataFileMetadata as DataFileManifestFacts;

async fn plan_pinned_snapshot(
    table: Table,
    domain: Arc<crate::delete_semantics::ReadDomain>,
    control: Option<&ConnectorRequestContext>,
) -> Result<IcebergReadSnapshot, String> {
    let operation_control = control.map(|control| control as &dyn ConnectorOperationControl);
    crate::read_snapshot::build_read_snapshot_in_domain(&table, domain, operation_control).await
}

/// Every partition spec a table has ever had, in the order the metadata holds
/// them.
///
/// A metadata relation's `partition` column is the union across specs, so the
/// whole set is handed over rather than the default spec alone.
fn partition_specs_of(metadata: &TableMetadata) -> Vec<PartitionSpec> {
    metadata
        .partition_specs_iter()
        .map(|spec| spec.as_ref().clone())
        .collect()
}

/// The manifest-list entries of one pinned snapshot.
///
/// Only the list is read: the manifests themselves are what the `$files` splits
/// distribute, so opening them here would do on the coordinator the very work
/// the splits exist to spread.
async fn pinned_snapshot_manifest_list(
    table: &Table,
    snapshot_id: i64,
    control: Option<&ConnectorRequestContext>,
) -> Result<Vec<ManifestFile>, String> {
    check_manifest_control(control.map(|control| control as &dyn ConnectorOperationControl))?;
    let metadata = table.metadata();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("iceberg snapshot {snapshot_id} is absent from table metadata"))?;
    let manifest_list = crate::manifest::load_manifest_list(snapshot, table.file_io(), metadata)
        .await
        .map_err(|error| format!("load manifest list: {error}"))?;
    check_manifest_control(control.map(|control| control as &dyn ConnectorOperationControl))?;
    Ok(manifest_list.entries().to_vec())
}

fn check_manifest_control(control: Option<&dyn ConnectorOperationControl>) -> Result<(), String> {
    if let Some(control) = control {
        control.check_active().map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// Reconstruct the file-local dynamic-filter domain from one pinned manifest
/// entry.  Manifest metrics are keyed by Iceberg field ID, while the read
/// stack is keyed by frozen column handles; binding through the current schema
/// is what makes a rename or a reordered schema unable to retarget a bound.
///
/// An absent, unsupported, or non-total metric is deliberately omitted.  That
/// widens the domain to `ALL` and keeps the file.  In contrast, mutually
/// impossible metrics are corruption: accepting them as a pruning proof could
/// silently discard rows, so the query must fail rather than guess.
/// Narrow logical declarations constrain every supplied physical INT fact,
/// including facts used to discard a file before any rows are materialized.
fn validate_scalar_integer_manifest_facts(
    declarations: &crate::scalar_integer_domain::ScalarIntegerDomains,
    facts: &DataFileManifestFacts,
    partition_spec: Option<&PartitionSpec>,
    partition_values: Option<&Struct>,
) -> Result<(), ConnectorError> {
    for (field_id, declaration) in declarations {
        for bound in [
            facts.lower_bounds.get(field_id),
            facts.upper_bounds.get(field_id),
        ]
        .into_iter()
        .flatten()
        {
            let PrimitiveLiteral::Int(value) = bound.literal() else {
                return Err(corrupt(
                    "iceberg scalar integer metric requires an INT32 carrier",
                ));
            };
            declaration.value(*value)?;
        }
        if let (Some(spec), Some(values)) = (partition_spec, partition_values) {
            for (index, field) in spec.fields().iter().enumerate() {
                if field.source_id != *field_id || field.transform != Transform::Identity {
                    continue;
                }
                let value = values.fields().get(index).ok_or_else(|| {
                    corrupt("iceberg scalar integer identity partition value is missing")
                })?;
                if let Some(value) = value {
                    let Literal::Primitive(PrimitiveLiteral::Int(value)) = value else {
                        return Err(corrupt(
                            "iceberg scalar integer identity partition requires an INT32 carrier",
                        ));
                    };
                    declaration.value(*value)?;
                }
            }
        }
    }
    Ok(())
}

fn manifest_statistics_domain(
    schema: &Schema,
    partition_spec: Option<&PartitionSpec>,
    read_file: &IcebergReadFile,
    facts: &DataFileManifestFacts,
    dynamic_filter_columns: &BTreeSet<IcebergColumnHandle>,
) -> Result<TupleDomain<IcebergColumnHandle>, ConnectorError> {
    let mut domains = BTreeMap::new();
    for column in dynamic_filter_columns {
        // Manifest metrics and partition constants describe whole top-level
        // fields.  A nested path has no independently addressable metric.
        if !column.is_base_column() {
            continue;
        }
        let Some(field) = schema.field_by_id(column.base_field_id()) else {
            continue;
        };
        let Type::Primitive(primitive) = field.field_type.as_ref() else {
            continue;
        };
        let Some(value_type) = connector_value_type(primitive) else {
            continue;
        };

        let metric_domain = manifest_metric_domain(
            column.base_field_id(),
            primitive,
            value_type,
            read_file.record_count,
            facts,
        )?;
        let partition_domain = identity_partition_domain(
            column.base_field_id(),
            primitive,
            value_type,
            partition_spec,
            read_file.partition_values.as_ref(),
        )?;
        let domain = match (metric_domain, partition_domain) {
            (Some(metrics), Some(partition)) => {
                let intersection = metrics.intersect(&partition)?;
                if intersection.is_none() {
                    return Err(corrupt(format!(
                        "iceberg data file {} has contradictory metric and identity-partition facts for field id {}",
                        read_file.path,
                        column.base_field_id()
                    )));
                }
                intersection
            }
            (Some(domain), None) | (None, Some(domain)) => domain,
            (None, None) => Domain::all(value_type),
        };
        let domain = match column.scalar_integer_domain() {
            Some(declaration) => declaration.domain(&domain)?,
            None => domain,
        };
        if !domain.is_all() {
            domains.insert(column.clone(), domain);
        }
    }
    TupleDomain::with_column_domains(domains)
}

/// A domain from a data-file metric. `None` means the metric cannot safely
/// prove anything and must be treated as `ALL` by the caller.
fn manifest_metric_domain(
    field_id: i32,
    primitive: &PrimitiveType,
    value_type: ConnectorValueType,
    file_record_count: Option<i64>,
    facts: &DataFileManifestFacts,
) -> Result<Option<Domain>, ConnectorError> {
    let value_count = facts.value_counts.get(&field_id).copied();
    let null_count = facts.null_value_counts.get(&field_id).copied().unwrap_or(0);
    let nan_count = facts.nan_value_counts.get(&field_id).copied().unwrap_or(0);

    if let Some(record_count) = file_record_count {
        let record_count = u64::try_from(record_count).map_err(|_| {
            corrupt("iceberg data file record count is negative while decoding manifest metrics")
        })?;
        if value_count.is_some_and(|count| count > record_count) {
            return Err(corrupt(format!(
                "iceberg manifest value count for field id {field_id} exceeds the data-file record count"
            )));
        }
    }
    if let Some(value_count) = value_count
        && (null_count > value_count
            || nan_count > value_count
            || null_count + nan_count > value_count)
    {
        return Err(corrupt(format!(
            "iceberg manifest null or NaN count for field id {field_id} exceeds its value count"
        )));
    }

    // A complete null count is a stronger proof than bounds, including the
    // all-null case where writers correctly omit both bounds.
    if value_count.is_some_and(|count| count == null_count) {
        return Ok(Some(Domain::only_null(value_type)));
    }
    // NaN deliberately has no position in ConnectorValue ordering.  A metric
    // carrying it can still be read, but cannot be used to eliminate a file.
    if nan_count > 0 {
        return Ok(None);
    }

    let (Some(lower), Some(upper)) = (
        facts.lower_bounds.get(&field_id),
        facts.upper_bounds.get(&field_id),
    ) else {
        return Ok(None);
    };
    let (Some(lower), Some(upper)) = (
        datum_as_connector_value(lower, primitive),
        datum_as_connector_value(upper, primitive),
    ) else {
        return Ok(None);
    };
    let Some(ordering) = lower.try_compare_same_type(&upper) else {
        return Ok(None);
    };
    if ordering.is_gt() {
        return Err(corrupt(format!(
            "iceberg manifest lower bound is above upper bound for field id {field_id}"
        )));
    }
    // Boolean only admits an equality range. A false-to-true metric is sound
    // but not expressible in the shared algebra, so it remains fail-open.
    if !value_type.is_orderable() && ordering.is_ne() {
        return Ok(None);
    }
    let range = Range::try_new(value_type, Bound::Inclusive(lower), Bound::Inclusive(upper))
        .map_err(|error| {
            corrupt(format!(
                "iceberg manifest bounds for field id {field_id} cannot form a typed range: {error}"
            ))
        })?;
    Ok(Some(Domain::new(
        ValueSet::of_ranges(value_type, vec![range])?,
        // A missing value count cannot prove that nulls are absent.
        value_count.is_none_or(|_| null_count > 0),
    )))
}

/// A singleton domain from an identity partition value.  As with manifest
/// metrics, a missing or unsupported value is not an error: it simply cannot
/// prune this file.
fn identity_partition_domain(
    field_id: i32,
    primitive: &PrimitiveType,
    value_type: ConnectorValueType,
    partition_spec: Option<&PartitionSpec>,
    partition_values: Option<&Struct>,
) -> Result<Option<Domain>, ConnectorError> {
    let (Some(spec), Some(values)) = (partition_spec, partition_values) else {
        return Ok(None);
    };
    let Some((index, _)) =
        spec.fields().iter().enumerate().find(|(_, field)| {
            field.source_id == field_id && field.transform == Transform::Identity
        })
    else {
        return Ok(None);
    };
    let Some(value) = values.fields().get(index) else {
        return Ok(None);
    };
    let Some(value) = value.as_ref() else {
        return Ok(Some(Domain::only_null(value_type)));
    };
    let Literal::Primitive(value) = value else {
        return Ok(None);
    };
    let Some(value) = primitive_literal_as_connector_value(value, primitive) else {
        return Ok(None);
    };
    Ok(Some(Domain::single_value(value)?))
}

fn connector_value_type(primitive: &PrimitiveType) -> Option<ConnectorValueType> {
    Some(match primitive {
        PrimitiveType::Boolean => ConnectorValueType::Boolean,
        PrimitiveType::Int => ConnectorValueType::Integer,
        PrimitiveType::Long => ConnectorValueType::BigInt,
        PrimitiveType::Float => ConnectorValueType::Real,
        PrimitiveType::Double => ConnectorValueType::Double,
        PrimitiveType::Decimal { precision, scale }
            if *precision <= 38 && *scale <= *precision && *scale <= u32::from(i8::MAX as u8) =>
        {
            ConnectorValueType::Decimal {
                precision: u8::try_from(*precision).ok()?,
                scale: i8::try_from(*scale).ok()?,
            }
        }
        PrimitiveType::Date => ConnectorValueType::Date,
        PrimitiveType::Time => ConnectorValueType::TimeMicros,
        PrimitiveType::Timestamp => ConnectorValueType::TimestampMicros,
        PrimitiveType::Timestamptz => ConnectorValueType::TimestampTzMicros,
        PrimitiveType::TimestampNs => ConnectorValueType::TimestampNanos,
        PrimitiveType::TimestamptzNs => ConnectorValueType::TimestampTzNanos,
        PrimitiveType::String => ConnectorValueType::Varchar,
        PrimitiveType::Uuid => ConnectorValueType::Uuid,
        PrimitiveType::Fixed(length) if *length <= u64::from(u32::MAX) => {
            ConnectorValueType::Fixed {
                length: u32::try_from(*length).ok()?,
            }
        }
        PrimitiveType::Binary => ConnectorValueType::Varbinary,
        PrimitiveType::Decimal { .. } | PrimitiveType::Fixed(_) => {
            return None;
        }
    })
}

/// Decode a metric under the current schema type, rather than trusting the
/// historical type that happened to be attached while its manifest was read.
/// This makes legal Iceberg type promotion explicit and rejects a metric that
/// cannot be interpreted under the scan's frozen schema.
fn datum_as_connector_value(datum: &Datum, primitive: &PrimitiveType) -> Option<ConnectorValue> {
    let bytes = datum.to_bytes().ok()?;
    let decoded = Datum::try_from_bytes(bytes.as_ref(), primitive.clone()).ok()?;
    primitive_literal_as_connector_value(decoded.literal(), primitive)
}

fn primitive_literal_as_connector_value(
    literal: &PrimitiveLiteral,
    primitive: &PrimitiveType,
) -> Option<ConnectorValue> {
    match (primitive, literal) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(value)) => {
            Some(ConnectorValue::Boolean(*value))
        }
        (PrimitiveType::Int, PrimitiveLiteral::Int(value)) => Some(ConnectorValue::Integer(*value)),
        (PrimitiveType::Long, PrimitiveLiteral::Long(value)) => {
            Some(ConnectorValue::BigInt(*value))
        }
        (PrimitiveType::Float, PrimitiveLiteral::Float(value)) if !value.is_nan() => {
            Some(ConnectorValue::Real(value.0))
        }
        (PrimitiveType::Double, PrimitiveLiteral::Double(value)) if !value.is_nan() => {
            Some(ConnectorValue::Double(value.0))
        }
        (PrimitiveType::Decimal { precision, scale }, PrimitiveLiteral::Int128(value)) => {
            ConnectorValue::try_decimal(
                *value,
                u8::try_from(*precision).ok()?,
                i8::try_from(*scale).ok()?,
            )
            .ok()
        }
        (PrimitiveType::Date, PrimitiveLiteral::Int(value)) => Some(ConnectorValue::Date(*value)),
        (PrimitiveType::Time, PrimitiveLiteral::Long(value)) => {
            Some(ConnectorValue::TimeMicros(*value))
        }
        (PrimitiveType::Timestamp, PrimitiveLiteral::Long(value)) => {
            Some(ConnectorValue::TimestampMicros(*value))
        }
        (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(value)) => {
            Some(ConnectorValue::TimestampTzMicros(*value))
        }
        (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(value)) => {
            Some(ConnectorValue::TimestampNanos(*value))
        }
        (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(value)) => {
            Some(ConnectorValue::TimestampTzNanos(*value))
        }
        (PrimitiveType::String, PrimitiveLiteral::String(value)) => {
            Some(ConnectorValue::Varchar(value.clone().into()))
        }
        (PrimitiveType::Uuid, PrimitiveLiteral::UInt128(value)) => {
            Some(ConnectorValue::Uuid(value.to_be_bytes()))
        }
        (PrimitiveType::Fixed(expected), PrimitiveLiteral::Binary(value))
            if usize::try_from(*expected).ok() == Some(value.len()) =>
        {
            Some(ConnectorValue::Fixed(value.clone().into()))
        }
        (PrimitiveType::Binary, PrimitiveLiteral::Binary(value)) => {
            Some(ConnectorValue::Varbinary(value.clone().into()))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Static file pruning
// ---------------------------------------------------------------------------

/// The manifest-only view `file_pruning` judges.
///
/// Only the facts pruning reads are filled in: identity partition values and
/// column statistics. Everything else stays absent so no consumer can mistake
/// this for a planning record.
fn pruning_view(
    handle: &IcebergTableHandle,
    schema: &Schema,
    read_file: &IcebergReadFile,
) -> Result<IcebergDataFileInfo, ConnectorError> {
    let partition_values = match (
        read_file.partition_spec_id,
        read_file.partition_values.as_ref(),
    ) {
        (Some(spec_id), Some(values)) => {
            let spec = handle.parse_partition_spec(spec_id)?;
            let mut decoded = Vec::with_capacity(spec.fields().len());
            for (index, field) in spec.fields().iter().enumerate() {
                let source_column = schema
                    .field_by_id(field.source_id)
                    .map(|source| source.name.clone())
                    .unwrap_or_else(|| format!("#{}", field.source_id));
                decoded.push(IcebergPartitionFieldValue {
                    source_column,
                    field_name: field.name.clone(),
                    transform: partition_transform_name(&field.transform),
                    value: values
                        .fields()
                        .get(index)
                        .and_then(|literal| literal.as_ref())
                        .and_then(partition_value_of),
                });
            }
            decoded
        }
        // Without both a spec and its values, identity-partition pruning cannot
        // judge anything; statistics pruning still applies.
        _ => Vec::new(),
    };

    Ok(IcebergDataFileInfo {
        path: read_file.path.clone(),
        size: read_file.size,
        row_count: read_file.record_count,
        column_stats: read_file.column_stats.clone(),
        partition_spec_id: read_file.partition_spec_id,
        partition_key: read_file.partition_key.clone(),
        partition_values,
        // Nothing below is read by pruning. Each stays absent so this value
        // cannot be mistaken for a planning record of the file.
        first_row_id: None,
        data_sequence_number: None,
        ivm_change_op: None,
        included_positions: None,
        delete_files: Vec::new(),
        manifest_path: None,
    })
}

fn partition_transform_name(transform: &crate::iceberg::spec::Transform) -> String {
    match transform {
        crate::iceberg::spec::Transform::Identity => "identity".to_string(),
        other => format!("{other:?}").to_ascii_lowercase(),
    }
}

fn partition_value_of(literal: &Literal) -> Option<IcebergPartitionValue> {
    let Literal::Primitive(value) = literal else {
        return None;
    };
    match value {
        PrimitiveLiteral::Boolean(value) => Some(IcebergPartitionValue::Boolean(*value)),
        PrimitiveLiteral::Int(value) => Some(IcebergPartitionValue::Int32(*value)),
        PrimitiveLiteral::Long(value) => Some(IcebergPartitionValue::Int64(*value)),
        PrimitiveLiteral::Float(value) => Some(IcebergPartitionValue::Float(value.0)),
        PrimitiveLiteral::Double(value) => Some(IcebergPartitionValue::Double(value.0)),
        PrimitiveLiteral::String(value) => Some(IcebergPartitionValue::String(value.clone())),
        PrimitiveLiteral::Binary(value) => Some(IcebergPartitionValue::Binary(value.clone())),
        // A decimal, a sentinel, or an unsigned 128-bit literal has no
        // comparable projection here, and "cannot judge" must keep the file.
        PrimitiveLiteral::Int128(_)
        | PrimitiveLiteral::UInt128(_)
        | PrimitiveLiteral::AboveMax
        | PrimitiveLiteral::BelowMin => None,
    }
}

/// Project a frozen predicate onto the physical predicates `file_pruning` reads.
///
/// Pruning must be sound, so every column this cannot express contributes
/// nothing and the file is kept. Two conditions in particular bar a column:
/// a nested or non-primitive column has no manifest statistics of its own, and
/// a domain that admits NULL cannot be judged from bounds -- a NULL partition
/// value would satisfy it while comparing as no value at all.
fn physical_predicates(
    predicate: &TupleDomain<IcebergColumnHandle>,
    schema: &Schema,
) -> Vec<IcebergPhysicalPredicate> {
    let Some(domains) = predicate.domains() else {
        return Vec::new();
    };
    let mut predicates = Vec::new();
    for (column, domain) in domains {
        if !column.is_base_column() || domain.null_allowed() {
            continue;
        }
        let Some(field) = schema.field_by_id(column.base_field_id()) else {
            continue;
        };
        if !matches!(field.field_type.as_ref(), Type::Primitive(_)) {
            continue;
        }
        for physical_domain in physical_domains(domain) {
            predicates.push(IcebergPhysicalPredicate {
                field_id: column.base_field_id(),
                column: field.name.clone(),
                domain: physical_domain,
            });
        }
    }
    predicates
}

/// The conjunction of physical domains one column domain implies.
fn physical_domains(domain: &Domain) -> Vec<IcebergPhysicalPredicateDomain> {
    if let Some(values) = domain.values().discrete_values() {
        let mut discrete = Vec::with_capacity(values.len());
        for value in values {
            let Some(value) = physical_value(value) else {
                return Vec::new();
            };
            discrete.push(value);
        }
        if discrete.is_empty() {
            return Vec::new();
        }
        return vec![IcebergPhysicalPredicateDomain::DiscreteSet { values: discrete }];
    }

    let ranges = domain.values().ranges();
    // A union of two or more ranges is not a conjunction of bounds, so nothing
    // sound can be emitted from it without widening it into something weaker.
    let [range] = ranges else {
        return Vec::new();
    };
    let mut result = Vec::with_capacity(2);
    for (bound, inclusive_op, exclusive_op) in [
        (
            range.low(),
            IcebergPhysicalPredicateOp::Ge,
            IcebergPhysicalPredicateOp::Gt,
        ),
        (
            range.high(),
            IcebergPhysicalPredicateOp::Le,
            IcebergPhysicalPredicateOp::Lt,
        ),
    ] {
        let (op, value) = match bound {
            Bound::Unbounded => continue,
            Bound::Inclusive(value) => (inclusive_op, value),
            Bound::Exclusive(value) => (exclusive_op, value),
        };
        let Some(value) = physical_value(value) else {
            continue;
        };
        result.push(IcebergPhysicalPredicateDomain::Range { op, value });
    }
    result
}

/// Project one typed value onto the literal domain manifest pruning compares in.
///
/// ADR-0018 limits that domain to boolean, int32, int64 and date; every other
/// type is "cannot judge", which keeps the file.
fn physical_value(value: &ConnectorValue) -> Option<IcebergPhysicalPredicateValue> {
    match value {
        ConnectorValue::Boolean(value) => Some(IcebergPhysicalPredicateValue::Boolean(*value)),
        ConnectorValue::Integer(value) => Some(IcebergPhysicalPredicateValue::Int32(*value)),
        ConnectorValue::BigInt(value) => Some(IcebergPhysicalPredicateValue::Int64(*value)),
        ConnectorValue::Date(value) => Some(IcebergPhysicalPredicateValue::Date32(*value)),
        ConnectorValue::TinyInt(value) => {
            Some(IcebergPhysicalPredicateValue::Int32(i32::from(*value)))
        }
        ConnectorValue::SmallInt(value) => {
            Some(IcebergPhysicalPredicateValue::Int32(i32::from(*value)))
        }
        ConnectorValue::Real(_)
        | ConnectorValue::Double(_)
        | ConnectorValue::Decimal { .. }
        | ConnectorValue::TimeMicros(_)
        | ConnectorValue::TimestampMicros(_)
        | ConnectorValue::TimestampMillis(_)
        | ConnectorValue::TimestampTzMicros(_)
        | ConnectorValue::TimestampNanos(_)
        | ConnectorValue::TimestampTzNanos(_)
        | ConnectorValue::Varchar(_)
        | ConnectorValue::Varbinary(_)
        | ConnectorValue::Uuid(_)
        | ConnectorValue::Fixed(_) => None,
    }
}

fn not_found(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::NotFound, message)
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message)
}

#[cfg(test)]
mod mv_target_selection_tests {
    use super::*;
    use bytes::Bytes;

    fn fixture() -> (
        TableMetadata,
        ConnectorMvTargetPartitionSelection,
        IcebergReadSnapshot,
    ) {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                ))])
                .build()
                .expect("schema"),
        );
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(7)
            .add_partition_field("region", "region_part", Transform::Identity)
            .expect("partition field")
            .build()
            .expect("partition spec");
        let builder = crate::iceberg::spec::TableMetadataBuilder::new(
            schema.as_ref().clone(),
            spec,
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "file:///mv-target-selection".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder");
        let snapshot = crate::iceberg::spec::Snapshot::builder()
            .with_snapshot_id(41)
            .with_parent_snapshot_id(None)
            .with_sequence_number(1)
            .with_timestamp_ms(1_700_000_000_000)
            .with_manifest_list("file:///mv-target-selection/metadata/snap-41.avro".to_string())
            .with_summary(crate::iceberg::spec::Summary {
                operation: crate::iceberg::spec::Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();
        let metadata = builder
            .add_snapshot(snapshot)
            .expect("snapshot")
            .build()
            .expect("metadata")
            .metadata;
        let spec = metadata
            .partition_spec_by_id(metadata.default_partition_spec_id())
            .expect("default spec");
        let field = &spec.fields()[0];
        let selection = ConnectorMvTargetPartitionSelection::try_new(
            novarocks_spi::connector::ConnectorTableObjectId::try_new(Bytes::from(
                metadata.uuid().to_string(),
            ))
            .expect("object id"),
            crate::storage_inspector::exact_partition_spec_version(spec.spec_id()),
            vec![
                novarocks_spi::connector::MvExactPartitionField::try_new(
                    Bytes::copy_from_slice(&field.field_id.to_be_bytes()),
                    Bytes::copy_from_slice(&field.source_id.to_be_bytes()),
                    MvExactPartitionTransform::Identity,
                )
                .expect("field"),
            ],
            vec![vec![ConnectorMvPartitionValue::String(Arc::from("emea"))]],
            41,
        )
        .expect("selection");
        let domain = crate::read_snapshot::mint_read_domain(&metadata, 41, &schema).unwrap();
        let index = crate::delete_semantics::DeleteCandidateIndex::try_new(
            domain,
            crate::delete_semantics::DeleteObservation::from_manifests([]).unwrap(),
        )
        .unwrap();
        let file = |path: &str, value: Option<Literal>, spec_id: i32| {
            let values = Struct::from_iter([value]);
            let data = crate::delete_semantics::DataFileFact::try_new(
                path,
                crate::delete_semantics::DataSequenceNumber::try_new(1).unwrap(),
                crate::delete_semantics::TypedPartition::bind(spec, &schema, &values).unwrap(),
                1,
                crate::delete_semantics::FileMetrics::default(),
            )
            .unwrap();
            IcebergReadFile {
                path: path.to_string(),
                size: 1,
                record_count: Some(1),
                column_stats: None,
                partition_spec_id: Some(spec_id),
                partition_key: None,
                partition_values: Some(values),
                manifest_path: None,
                first_row_id: None,
                data_sequence_number: Some(1),
                manifest: Arc::new(DataFileManifestFacts {
                    file_format: DataFileFormat::Parquet,
                    split_offsets: Vec::new(),
                    key_metadata: Vec::new(),
                    value_counts: HashMap::new(),
                    null_value_counts: HashMap::new(),
                    nan_value_counts: HashMap::new(),
                    lower_bounds: HashMap::new(),
                    upper_bounds: HashMap::new(),
                }),
                deletes: index
                    .for_data(&data)
                    .unwrap()
                    .load_view(crate::delete_semantics::StatisticsPolicy::Disabled),
            }
        };
        let snapshot = IcebergReadSnapshot {
            snapshot_id: Some(41),
            files: vec![
                file(
                    "file:///emea.parquet",
                    Some(Literal::string("emea")),
                    spec.spec_id(),
                ),
                file(
                    "file:///apac.parquet",
                    Some(Literal::string("apac")),
                    spec.spec_id(),
                ),
            ],
        };
        (metadata, selection, snapshot)
    }

    #[test]
    fn exact_target_partition_selection_pins_only_matching_files() {
        let (metadata, selection, snapshot) = fixture();
        validate_mv_target_partition_selection(&metadata, &selection).expect("exact target");
        let pinned = select_mv_target_files(&metadata, &snapshot, &selection).expect("pinned");
        assert_eq!(pinned.len(), 1);
        assert!(pinned.contains("file:///emea.parquet"));
        assert!(!pinned.contains("file:///apac.parquet"));
    }

    #[test]
    fn frozen_target_requires_exact_object_snapshot_and_empty_state() {
        let (metadata, selection, _) = fixture();
        assert_eq!(metadata.current_snapshot_id(), None);
        validate_mv_target_pin(&metadata, selection.object_id(), None).unwrap();
        validate_mv_target_pin(&metadata, selection.object_id(), Some(41)).unwrap();
        let missing =
            validate_mv_target_pin(&metadata, selection.object_id(), Some(42)).unwrap_err();
        assert_eq!(missing.kind(), ConnectorErrorKind::NotFound);
        assert!(
            missing
                .to_string()
                .contains("not present in this refs observation")
        );
        let other = ConnectorTableObjectId::try_new(Bytes::from_static(b"other-object")).unwrap();
        assert_eq!(
            validate_mv_target_pin(&metadata, &other, Some(41))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        let metadata = metadata
            .into_builder(None)
            .set_ref(
                "main",
                crate::iceberg::spec::SnapshotReference::new(
                    41,
                    crate::iceberg::spec::SnapshotRetention::branch(None, None, None),
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert_eq!(metadata.current_snapshot_id(), Some(41));
        assert_eq!(
            validate_mv_target_pin(&metadata, selection.object_id(), None)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        validate_mv_target_pin(&metadata, selection.object_id(), Some(41)).unwrap();
    }

    #[test]
    fn target_partition_selection_rejects_different_object_spec_or_field() {
        let (metadata, selection, _) = fixture();
        let make = |object_id, spec_version, fields| {
            ConnectorMvTargetPartitionSelection::try_new(
                object_id,
                spec_version,
                fields,
                selection.keys().to_vec(),
                selection.snapshot_id(),
            )
            .expect("selection shape")
        };
        let other_object = make(
            novarocks_spi::connector::ConnectorTableObjectId::try_new(Bytes::from_static(
                b"different-object",
            ))
            .expect("other object"),
            selection.partition_spec_version().clone(),
            selection.partition_fields().to_vec(),
        );
        assert!(validate_mv_target_partition_selection(&metadata, &other_object).is_err());

        let other_spec = make(
            selection.object_id().clone(),
            Bytes::copy_from_slice(&999_i32.to_le_bytes()),
            selection.partition_fields().to_vec(),
        );
        assert!(validate_mv_target_partition_selection(&metadata, &other_spec).is_err());

        let other_field = make(
            selection.object_id().clone(),
            selection.partition_spec_version().clone(),
            vec![
                novarocks_spi::connector::MvExactPartitionField::try_new(
                    Bytes::copy_from_slice(&999_i32.to_be_bytes()),
                    selection.partition_fields()[0]
                        .source_target_field_id()
                        .clone(),
                    MvExactPartitionTransform::Identity,
                )
                .expect("other field"),
            ],
        );
        assert!(validate_mv_target_partition_selection(&metadata, &other_field).is_err());
    }

    #[test]
    fn empty_known_target_partition_selection_pins_no_files() {
        let (metadata, selection, snapshot) = fixture();
        let empty = ConnectorMvTargetPartitionSelection::try_new(
            selection.object_id().clone(),
            selection.partition_spec_version().clone(),
            selection.partition_fields().to_vec(),
            Vec::new(),
            selection.snapshot_id(),
        )
        .expect("empty selection");
        let pinned = select_mv_target_files(&metadata, &snapshot, &empty).expect("pinned empty");
        assert!(pinned.is_empty());
    }

    #[test]
    fn target_partition_selection_abandons_oversized_file_set() {
        let (metadata, selection, mut snapshot) = fixture();
        let seed = snapshot.files.remove(0);
        snapshot.files = (0..=crate::typed_read::table_handle::MAX_PINNED_DATA_FILES)
            .map(|ordinal| {
                let mut file = seed.clone();
                file.path = format!("file:///emea-{ordinal}.parquet");
                file
            })
            .collect();
        assert!(select_mv_target_files(&metadata, &snapshot, &selection).is_none());
    }

    #[test]
    fn historical_or_uncomparable_file_abandons_entire_target_selection() {
        let (metadata, selection, mut snapshot) = fixture();
        snapshot.files[1].partition_spec_id = Some(metadata.default_partition_spec_id() - 1);
        assert!(select_mv_target_files(&metadata, &snapshot, &selection).is_none());

        snapshot.files[1].partition_spec_id = Some(metadata.default_partition_spec_id());
        snapshot.files[1].partition_values = Some(Struct::from_iter([Some(Literal::Primitive(
            PrimitiveLiteral::Binary(vec![1, 2, 3]),
        ))]));
        assert!(select_mv_target_files(&metadata, &snapshot, &selection).is_none());
    }
}

#[cfg(test)]
pub(crate) mod final_static_facts_tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use super::*;

    pub(crate) fn table_handle() -> IcebergTableHandle {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        IcebergTableHandle::try_new(IcebergTableHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "orders").expect("table name"),
            snapshot_id: Some(41),
            read_domain: Some(crate::delete_semantics::test_read_domain(&schema, &[], 41)),
            table_schema_json: serde_json::to_string(&schema).expect("schema json"),
            spec_id: None,
            partition_spec_jsons: BTreeMap::new(),
            format_version: 2,
            unenforced_predicate: TupleDomain::all(),
            enforced_predicate: TupleDomain::all(),
            limit: None,
            projected_columns: BTreeSet::new(),
            name_mapping_json: None,
            table_location: "s3://warehouse/db/orders".to_string(),
            storage_properties: BTreeMap::new(),
            pinned_data_files: None,
        })
        .expect("table handle")
    }

    #[test]
    fn table_static_facts_keep_source_identity_separate_from_selection() {
        let base = table_handle();
        let limited = base.apply_limit(9).expect("limit").into_handle();

        let base =
            iceberg_final_static_facts(&crate::typed_read::IcebergRuntimeRelation::Table(base))
                .expect("base facts");
        let limited =
            iceberg_final_static_facts(&crate::typed_read::IcebergRuntimeRelation::Table(limited))
                .expect("limited facts");

        assert_eq!(base.input_version(), limited.input_version());
        assert_ne!(base.selection_digest(), limited.selection_digest());
        assert_eq!(
            base.artifact_coverage(),
            &ConnectorReadArtifactCoverage::NoArtifactInputs
        );
    }

    #[test]
    fn system_table_static_facts_publish_the_frozen_metadata_identity() {
        let reference = IcebergSystemTableReference::try_new(
            crate::typed_read::IcebergSystemTableReferenceParams {
                schema_table_name: SchemaTableName::try_new("db", "orders").expect("table"),
                system_table_type: crate::typed_read::IcebergSystemTableType::Snapshots,
                metadata_file_location: "s3://warehouse/db/orders/metadata/v1.json".to_string(),
                table_uuid: "6ba7b810-9dad-11d1-80b4-00c04fd430c8".to_string(),
                snapshot_id: Some(41),
            },
        )
        .expect("reference");

        let facts = iceberg_final_static_facts(
            &crate::typed_read::IcebergRuntimeRelation::SystemTable(reference),
        )
        .expect("system facts");

        assert!(!facts.input_version().as_bytes().is_empty());
        assert_ne!(facts.selection_digest(), [0; 32]);
        assert!(matches!(
            facts.properties().distribution(),
            ConnectorReadDistribution::Unconstrained
        ));
    }

    #[test]
    fn malformed_artifact_digest_cannot_be_coverage_evidence() {
        assert!(decode_sha256_hex("not-a-sha256").is_err());
        assert_eq!(
            decode_sha256_hex("0000000000000000000000000000000000000000000000000000000000000001")
                .expect("digest"),
            [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 1,
            ]
        );
    }
}

#[cfg(test)]
mod public_read_schema_tests {
    use std::sync::Arc;

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::read_stack::ConnectorReadMetadata;
    use novarocks_spi::connector::read_stack::adapter::ReadRuntimeAdapter;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, ConnectorProviderId,
    };

    use super::*;

    fn with_creation<T>(
        run: impl FnOnce(
            &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> T,
    ) -> T {
        let mut request = |_: std::alloc::Layout| Ok(());
        let mut work = |_: usize| Ok(());
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        run(&mut creation)
    }
    fn public_read_schema(
        relation: &crate::typed_read::IcebergRuntimeRelation,
        columns: &[IcebergColumnHandle],
    ) -> Result<
        ConnectorReadPublicSchema,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        let columns: Vec<_> = columns.iter().collect();
        with_creation(|creation| {
            crate::typed_read::codec::public_read_schema(relation, &columns, creation)
        })
    }
    fn installed_adapter(
        runtime: &tokio::runtime::Runtime,
        warehouse: &tempfile::TempDir,
    ) -> ReadRuntimeAdapter<IcebergTypedBoundary> {
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let resources =
            crate::resources::IcebergMetadataResources::new(binding, runtime.handle().clone());
        let metadata_context = Arc::new(
            IcebergMetadataContext::try_new(
                crate::catalog_control::IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("metadata context"),
        );
        let catalog_id = ConnectorInstanceId::parse("ice").expect("catalog ID");
        Arc::new(IcebergTypedBoundary::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(crate::PROVIDER_ID).expect("provider ID"),
                instance_id: catalog_id.clone(),
            },
            ProviderBindingEpoch::new(),
            CatalogHandle::new(catalog_id, CatalogVersion::from_bytes([1; 32])),
            HiveTransactionHandle::new(true, [2; 16]),
            metadata_context,
        ))
        .read_runtime_adapter()
    }

    fn session() -> ConnectorSession {
        ConnectorSession::try_new("q", "u", "UTC", "en_US", std::time::SystemTime::UNIX_EPOCH)
            .expect("session")
    }

    /// What the installed metadata publishes through the type-erased boundary
    /// is exactly the provider author's schema for the same frozen read.
    #[test]
    fn installed_read_metadata_publishes_the_provider_authored_schema() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let adapter = installed_adapter(&runtime, &warehouse);
        let handle = final_static_facts_tests::table_handle();
        let schema = handle.parse_table_schema().expect("schema");
        let columns = vec![IcebergColumnHandle::base_column_of(&schema, 1).expect("id"); 2];
        let relation = crate::typed_read::IcebergRuntimeRelation::Table(handle);
        let expected = public_read_schema(&relation, &columns).expect("provider schema");

        let table = adapter.wrap_table(relation);
        let columns = columns
            .into_iter()
            .map(|column| adapter.wrap_column(column))
            .collect::<Vec<_>>();
        let published = with_creation(|creation| {
            ConnectorReadMetadata::read_public_schema(
                &adapter,
                &session(),
                &table,
                &columns,
                creation,
            )
        })
        .expect("published schema");
        assert_eq!(published, expected);
        assert!(
            published
                .schema()
                .fields()
                .iter()
                .all(|field| field.metadata().is_empty())
        );
        let facts = published.source_schema_facts().unwrap();
        assert_eq!(facts.field_fact(&[0]).unwrap().field_id(), Some(1));
        assert_eq!(facts.field_fact(&[1]).unwrap().field_id(), Some(1));
    }

    /// A hidden metadata column is planned with the field the table metadata
    /// states to SQL, and its frozen read publishes the field of the handle
    /// the read binding mints. Both are this provider's statement about the
    /// same column, and the package law compares them exactly: logical type,
    /// NULL contract and Arrow data type.
    #[test]
    fn metadata_pseudo_columns_publish_the_type_the_table_states_to_sql() {
        let relation = crate::typed_read::IcebergRuntimeRelation::Table(
            final_static_facts_tests::table_handle(),
        );
        let (names, columns): (Vec<_>, Vec<_>) = metadata_pseudo_columns(true)
            .expect("metadata columns")
            .into_iter()
            .map(|(name, column)| (name.to_string(), column))
            .unzip();
        let published = public_read_schema(&relation, &columns).expect("public schema");
        let planned = crate::metadata::metadata_arrow_fields(&names).expect("table fields");

        assert_eq!(published.schema().fields().len(), planned.len());
        for (ordinal, (public, sql)) in published.schema().fields().iter().zip(&planned).enumerate()
        {
            let sql_logical =
                novarocks_type_contract::field_logical_type(sql).expect("SQL logical type");
            assert_eq!(public.name(), sql.name());
            assert_eq!(
                (
                    published.logical_types()[ordinal],
                    public.is_nullable(),
                    public.data_type()
                ),
                (sql_logical, sql.is_nullable(), sql.data_type()),
                "metadata column {} publishes a different type than the table states",
                sql.name()
            );
        }
    }

    #[test]
    fn installed_read_metadata_refuses_a_relation_without_a_field_author() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let adapter = installed_adapter(&runtime, &warehouse);
        let base = final_static_facts_tests::table_handle();
        let schema = base.parse_table_schema().expect("schema");
        let column = IcebergColumnHandle::base_column_of(&schema, 1).expect("id");
        let function = crate::typed_read::TableChangesFunctionHandle::try_new(
            crate::typed_read::TableChangesFunctionHandleParams {
                schema_table_name: base.schema_table_name().clone(),
                table_schema_json: base.table_schema_json().into(),
                columns: vec![column.clone()],
                name_mapping_json: None,
                start_snapshot_id: 40,
                end_snapshot_id: 41,
            },
        )
        .expect("table function");
        let table = adapter.wrap_table(crate::typed_read::IcebergRuntimeRelation::TableFunction(
            function,
        ));
        let error = with_creation(|creation| {
            ConnectorReadMetadata::read_public_schema(
                &adapter,
                &session(),
                &table,
                &[adapter.wrap_column(column)],
                creation,
            )
        })
        .expect_err("a table-function relation has no public read schema");
        assert!(
            matches!(error,novarocks_connector_contract::PureProviderCompileError::Provider(cause) if cause.kind()==ConnectorErrorKind::Unsupported)
        );
    }
}

#[cfg(test)]
mod attempt_access_tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Weak};
    use std::time::{Duration, Instant};

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, ConnectorProviderId,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };

    use super::*;

    #[derive(Debug)]
    struct AttemptScopeMarker(u8);

    struct TrackedAttemptResources {
        stop: novarocks_spi::connector::ConnectorStopOwner,
    }

    impl TrackedAttemptResources {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                stop: novarocks_spi::connector::ConnectorStopOwner::new(),
            })
        }
    }

    fn request_context(resources: &Arc<TrackedAttemptResources>) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            resources.stop.view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("attempt context")
    }

    fn physical_table(
        runtime: &crate::iceberg::Runtime,
        binding: crate::access_binding::IcebergReadBinding,
        metadata_location: &str,
    ) -> IcebergPhysicalTable {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "s3://warehouse/data/table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = crate::iceberg::table::Table::builder()
            .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).expect("identifier"))
            .file_io(crate::fs_io::build_file_io_for_location(
                "s3://warehouse/data/table",
                binding,
            ))
            .metadata(metadata)
            .metadata_location(metadata_location.to_string())
            .runtime(runtime.clone())
            .build()
            .expect("table");
        IcebergPhysicalTable::new(table)
    }

    #[test]
    fn mv_target_refs_observation_stays_request_local_and_historical_reads_use_all() {
        use novarocks_spi::connector::read_stack::adapter::ProviderReadMetadata;
        let executor = tokio::runtime::Runtime::new().unwrap();
        let warehouse = tempfile::tempdir().unwrap();
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .unwrap();
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(executor.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(executor.handle().clone())),
        );
        let original = IcebergMetadataContext::try_new(
            crate::catalog_control::IcebergCatalogControlState::new(configuration),
            crate::resources::IcebergMetadataResources::new(binding, executor.handle().clone()),
        )
        .unwrap();
        let incarnation = ProviderBindingEpoch::new();
        let table = executor.block_on(
            crate::catalog::admission_test_support::create_static_fixture_table(
                &original,
                incarnation,
                crate::catalog::CatalogNamespaceName::new("db"),
                crate::iceberg::TableCreation::builder()
                    .name("t".to_string())
                    .schema(
                        Schema::builder()
                            .with_fields(vec![Arc::new(NestedField::required(
                                1,
                                "id",
                                Type::Primitive(PrimitiveType::Long),
                            ))])
                            .build()
                            .unwrap(),
                    )
                    .format_version(FormatVersion::V2)
                    .build(),
            ),
        );
        let loads = Arc::new(Mutex::new(Vec::new()));
        let owner = crate::catalog::admission_test_support::all_admitted_with_snapshot_trace(
            original.novarocks_catalog().clone(),
            loads.clone(),
        );
        let context = Arc::new(IcebergMetadataContext::with_catalog_for_test(
            original.control_state().clone(),
            original.resources().clone(),
            owner,
        ));
        let catalog_id = ConnectorInstanceId::parse("ice").unwrap();
        let template = IcebergTypedBoundary::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: catalog_id.clone(),
            },
            incarnation,
            CatalogHandle::new(catalog_id, CatalogVersion::from_bytes([1; 32])),
            HiveTransactionHandle::new(true, [2; 16]),
            context.clone(),
        );
        let name = SchemaTableName::try_new("db", "t").unwrap();
        let object = ConnectorTableObjectId::try_new(bytes::Bytes::from(
            table.metadata().uuid().to_string(),
        ))
        .unwrap();
        let session =
            ConnectorSession::try_new("q", "u", "UTC", "en_US", std::time::SystemTime::UNIX_EPOCH)
                .unwrap();
        let resources = TrackedAttemptResources::new();
        let boundary = template.for_request(request_context(&resources));
        let relation = boundary
            .get_mv_target_handle(&session, &name, &object, None)
            .unwrap()
            .unwrap();
        let crate::typed_read::IcebergRuntimeRelation::Table(handle) = &relation else {
            panic!("table")
        };
        assert_eq!(handle.snapshot_id(), None);
        assert_eq!(
            handle.parse_table_schema().unwrap(),
            *table.metadata().current_schema().as_ref()
        );
        boundary.get_column_bindings(&session, &relation).unwrap();
        let pinned = boundary.load_pinned_relation(&name).unwrap();
        let sealed = boundary.pinned_table_for_attempt(&name).unwrap();
        assert_eq!(pinned.table.metadata().uuid(), table.metadata().uuid());
        assert_eq!(sealed.table.metadata_location(), table.metadata_location());
        assert_eq!(
            *loads.lock().unwrap(),
            vec![crate::catalog::SnapshotLoading::Refs]
        );
        assert!(
            context
                .control_state()
                .physical_table_cache()
                .get("db", "t")
                .unwrap()
                .is_none()
        );
        let wrong =
            ConnectorTableObjectId::try_new(bytes::Bytes::from_static(b"replacement")).unwrap();
        assert_eq!(
            boundary
                .get_mv_target_handle(&session, &name, &wrong, None)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(
            boundary
                .get_mv_target_handle(&session, &name, &object, Some(41))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::NotFound
        );
        let historical_resources = TrackedAttemptResources::new();
        let historical = template.for_request(request_context(&historical_resources));
        assert!(
            historical
                .get_table_handle(
                    &session,
                    &name,
                    ConnectorReadRelationVersion::SnapshotId(41),
                    None
                )
                .is_err()
        );
        assert_eq!(
            *loads.lock().unwrap(),
            vec![
                crate::catalog::SnapshotLoading::Refs,
                crate::catalog::SnapshotLoading::All
            ]
        );

        let async_resources = TrackedAttemptResources::new();
        let async_context = request_context(&async_resources);
        let async_boundary = template.for_request(async_context.clone());
        use novarocks_spi::connector::read_stack::{
            ConnectorReadRelationPreparationKind as K, ConnectorReadRelationPreparationRequest as R,
        };
        let table_request = R::new(
            name.clone(),
            K::Table {
                version: ConnectorReadRelationVersion::Current,
                reference: None,
            },
        );
        let prepared = executor
            .block_on(async_boundary.prepare_relation(&session, &table_request))
            .unwrap()
            .unwrap();
        let observed_loads = loads.lock().unwrap().clone();
        assert_eq!(
            observed_loads.last(),
            Some(&crate::catalog::SnapshotLoading::All)
        );
        let metadata_request = R::new(
            SchemaTableName::try_new("db", "t$snapshots").unwrap(),
            K::Metadata { request: None },
        );
        let metadata_facts = executor
            .block_on(async_boundary.prepare_relation(&session, &metadata_request))
            .unwrap()
            .unwrap();
        let mv_request = R::new(
            name.clone(),
            K::MvTarget {
                object_id: object.clone(),
                snapshot_id: None,
            },
        );
        let mv_facts = executor
            .block_on(async_boundary.prepare_relation(&session, &mv_request))
            .unwrap()
            .unwrap();
        let delta_request = R::new(
            name.clone(),
            K::Delta {
                window: novarocks_spi::connector::read_stack::ConnectorReadChangeWindow::new(
                    40, 41,
                ),
            },
        );
        let delta_facts = executor
            .block_on(async_boundary.prepare_relation(&session, &delta_request))
            .unwrap()
            .unwrap();
        // The pure creator must consume the completed original metadata,
        // not resolve the name or reload a schema after this cut.
        std::fs::remove_dir_all(warehouse.path()).unwrap();
        let (metadata_relation, hint) = async_boundary
            .create_prepared_relation(metadata_facts)
            .unwrap()
            .unwrap()
            .into_parts();
        assert!(matches!(
            metadata_relation,
            crate::typed_read::IcebergRuntimeRelation::SystemTable(_)
        ));
        assert!(hint.is_some());
        let (mv_relation, hint) = async_boundary
            .create_prepared_relation(mv_facts)
            .unwrap()
            .unwrap()
            .into_parts();
        assert!(matches!(
            mv_relation,
            crate::typed_read::IcebergRuntimeRelation::Table(_)
        ));
        assert!(hint.is_none());
        // The empty real V2 table has neither endpoint: retain the original
        // NotFound classification rather than replacing Delta with Unsupported.
        let error = async_boundary
            .create_prepared_relation(delta_facts)
            .err()
            .unwrap();
        assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
        let relation = async_boundary
            .create_prepared_relation(prepared)
            .unwrap()
            .unwrap()
            .into_parts()
            .0;
        let crate::typed_read::IcebergRuntimeRelation::Table(handle) = relation else {
            panic!("table")
        };
        assert_eq!(
            handle.parse_table_schema().unwrap(),
            *table.metadata().current_schema().as_ref()
        );
        assert_eq!(*loads.lock().unwrap(), observed_loads);
        // A pure creator must refuse absent original observations; these
        // checks must not invoke the catalog even when the warehouse is gone.
        let missing_resources = TrackedAttemptResources::new();
        let missing = template.for_request(request_context(&missing_resources));
        let absent = missing.relation_has_row_lineage(&handle).unwrap_err();
        assert_eq!(absent.kind(), ConnectorErrorKind::InvalidRequest);
        let crate::typed_read::IcebergRuntimeRelation::SystemTable(reference) = &metadata_relation
        else {
            panic!("prepared system relation");
        };
        let absent = missing.system_relation_metadata(reference).unwrap_err();
        assert_eq!(absent.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(*loads.lock().unwrap(), observed_loads);
        // A fresh typed control for the SAME request must still borrow that
        // request's original frozen cache entry, even after warehouse removal.
        let replay_boundary = template.for_request(async_context);
        let replay = executor
            .block_on(replay_boundary.prepare_relation(&session, &table_request))
            .unwrap()
            .unwrap();
        replay_boundary
            .create_prepared_relation(replay)
            .unwrap()
            .unwrap();
        assert_eq!(*loads.lock().unwrap(), observed_loads);
    }

    #[test]
    fn attempt_boundary_uses_the_new_request_scope_and_exact_pinned_table() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let resources =
            crate::resources::IcebergMetadataResources::new(binding, runtime.handle().clone());
        let metadata_context = Arc::new(
            IcebergMetadataContext::try_new(
                crate::catalog_control::IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("metadata context"),
        );
        let catalog_id = ConnectorInstanceId::parse("ice").expect("catalog ID");
        let template = IcebergTypedBoundary::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                instance_id: catalog_id.clone(),
            },
            ProviderBindingEpoch::new(),
            CatalogHandle::new(catalog_id, CatalogVersion::from_bytes([1; 32])),
            HiveTransactionHandle::new(true, [2; 16]),
            Arc::clone(&metadata_context),
        );
        let name = SchemaTableName::try_new("db", "t").expect("table name");
        let old_resources = TrackedAttemptResources::new();
        let old_resources_weak: Weak<TrackedAttemptResources> = Arc::downgrade(&old_resources);
        let old_attempt = request_context(&old_resources);
        let old_marker =
            old_attempt.request_scope_extension_or_insert_with(|| AttemptScopeMarker(3));
        let old_marker_weak = Arc::downgrade(&old_marker);
        let old_binding = template
            .runtime
            .resources()
            .planning_binding()
            .for_request(old_attempt.clone());
        let physical = physical_table(
            template
                .runtime
                .resources()
                .catalog_runtime()
                .iceberg_runtime(),
            old_binding,
            "s3://warehouse/data/table/metadata/v1.json",
        );
        let old_file_io = physical.table.file_io() as *const _;
        let frozen_uuid = physical.table.metadata().uuid();
        let frozen_snapshot = physical.table.metadata().current_snapshot_id();
        let access = IcebergAttemptTableAccess::freeze(physical.clone());
        old_resources.stop.request_stop();

        let new_resources = TrackedAttemptResources::new();
        let attempt = request_context(&new_resources);
        let marker = attempt.request_scope_extension_or_insert_with(|| AttemptScopeMarker(7));
        let cache =
            attempt.request_scope_extension_or_insert_with(AttemptReacquiredTableCache::default);
        let key = AttemptReacquiredTableKey {
            catalog: template.catalog_handle.clone(),
            table: name.clone(),
            metadata_identity: access.request_cache_identity(),
        };
        let calls = AtomicUsize::new(0);
        let _first_cached = runtime
            .block_on(cache.get_or_reacquire(key.clone(), async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(physical.clone())
            }))
            .expect("first frozen table is reacquired");
        let second_cached = runtime
            .block_on(cache.get_or_reacquire(key, async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(physical.clone())
            }))
            .expect("identical frozen table reuses request capability");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "one attempt must reacquire an identical frozen table once"
        );
        assert_eq!(
            second_cached.table.metadata_location(),
            Some("s3://warehouse/data/table/metadata/v1.json"),
            "the cached capability stays pinned to the frozen metadata view"
        );
        let wrong_identity = metadata_context
            .reacquire_table_access_for_request("db", "other", &access, &attempt)
            .expect_err("static recipe must remain bound to its exact table identity");
        assert_eq!(wrong_identity.0, ConnectorErrorKind::InvalidRequest);
        let rebound = metadata_context
            .reacquire_table_access_for_request("db", "t", &access, &attempt)
            .expect("static access must rebind under the new request");
        assert_ne!(rebound.table.file_io() as *const _, old_file_io);
        assert_eq!(rebound.table.metadata().uuid(), frozen_uuid);
        assert_eq!(
            rebound.table.metadata().current_snapshot_id(),
            frozen_snapshot
        );
        assert_eq!(
            rebound.table.metadata_location(),
            Some("s3://warehouse/data/table/metadata/v1.json")
        );
        let boundary = template.for_request_with_pinned_table(attempt, name.clone(), rebound);

        let rebound_marker = boundary
            .request_context
            .as_ref()
            .expect("request-bound boundary")
            .request_scope_extension_or_insert_with(|| AttemptScopeMarker(9));
        assert!(Arc::ptr_eq(&marker, &rebound_marker));
        assert_eq!(rebound_marker.0, 7);
        let pinned = boundary
            .load_pinned_relation(&name)
            .expect("split planning reads the preinstalled physical table");
        assert_eq!(
            pinned.table.metadata_location(),
            Some("s3://warehouse/data/table/metadata/v1.json")
        );
        assert_eq!(
            pinned.table.metadata().uuid(),
            physical.table.metadata().uuid()
        );

        drop(pinned);
        drop(boundary);
        drop(second_cached);
        drop(_first_cached);
        drop(cache);
        drop(physical);
        drop(old_attempt);
        drop(old_marker);
        drop(old_resources);
        assert!(old_marker_weak.upgrade().is_none());
        assert!(old_resources_weak.upgrade().is_none());
    }
}

#[cfg(test)]
mod manifest_statistics_tests {
    use super::*;

    fn facts_for_int_bounds(lower: i32, upper: i32) -> DataFileManifestFacts {
        DataFileManifestFacts {
            file_format: DataFileFormat::Parquet,
            split_offsets: Vec::new(),
            key_metadata: Vec::new(),
            value_counts: HashMap::from([(1, 10)]),
            null_value_counts: HashMap::from([(1, 0)]),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::from([(1, Datum::int(lower))]),
            upper_bounds: HashMap::from([(1, Datum::int(upper))]),
        }
    }

    #[test]
    fn manifest_metric_reconstructs_a_non_null_current_schema_range() {
        let domain = manifest_metric_domain(
            1,
            &PrimitiveType::Int,
            ConnectorValueType::Integer,
            Some(10),
            &facts_for_int_bounds(10, 20),
        )
        .expect("valid metrics")
        .expect("metrics are usable");

        assert!(!domain.null_allowed());
        assert_eq!(
            domain.values().ranges(),
            &[Range::try_new(
                ConnectorValueType::Integer,
                Bound::Inclusive(ConnectorValue::Integer(10)),
                Bound::Inclusive(ConnectorValue::Integer(20)),
            )
            .expect("range")]
        );
    }

    #[test]
    fn manifest_nan_or_missing_bounds_do_not_claim_a_pruning_proof() {
        let mut nan = facts_for_int_bounds(10, 20);
        nan.nan_value_counts.insert(1, 1);
        assert!(
            manifest_metric_domain(
                1,
                &PrimitiveType::Int,
                ConnectorValueType::Integer,
                Some(10),
                &nan,
            )
            .expect("NaN is fail-open")
            .is_none()
        );

        let mut missing = facts_for_int_bounds(10, 20);
        missing.upper_bounds.clear();
        assert!(
            manifest_metric_domain(
                1,
                &PrimitiveType::Int,
                ConnectorValueType::Integer,
                Some(10),
                &missing,
            )
            .expect("missing bound is fail-open")
            .is_none()
        );
    }

    #[test]
    fn contradictory_manifest_bounds_fail_the_query() {
        let error = manifest_metric_domain(
            1,
            &PrimitiveType::Int,
            ConnectorValueType::Integer,
            Some(10),
            &facts_for_int_bounds(20, 10),
        )
        .expect_err("inverted bounds are corruption");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }
    #[test]
    fn scalar_integer_change_window_freezes_the_current_stable_id_domain_on_both_endpoints() {
        use crate::scalar_integer_domain::ScalarIntegerDomain;
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                17,
                "tiny",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let mut builder = crate::iceberg::spec::TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://change-domain".to_string(),
            FormatVersion::V2,
            HashMap::from([(
                crate::schema_mapping::nominal_declaration::PROPERTY.to_string(),
                r#"{"version":1,"fields":[{"id":1,"domain":"tinyint"}]}"#.to_string(),
            )]),
        )
        .unwrap();
        for (id, parent, sequence) in [(41, None, 1), (42, Some(41), 2)] {
            let snapshot = crate::iceberg::spec::Snapshot::builder()
                .with_snapshot_id(id)
                .with_parent_snapshot_id(parent)
                .with_sequence_number(sequence)
                .with_timestamp_ms(1_700_000_000_000 + sequence)
                .with_manifest_list(format!("memory://change-domain/snap-{id}.avro"))
                .with_summary(crate::iceberg::spec::Summary {
                    operation: crate::iceberg::spec::Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build();
            builder = builder.add_snapshot(snapshot).unwrap();
        }
        let metadata = builder.build().unwrap().metadata;
        let window = ConnectorReadChangeWindow::new(41, 42);
        let handle = pinned_change_window_handle(
            &SchemaTableName::try_new("db", "tiny").unwrap(),
            &metadata,
            window,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            handle.columns()[0].scalar_integer_domain(),
            Some(ScalarIntegerDomain::Int8)
        );
        let decoded = IcebergChangeWindowHandle::from_proto(&handle.to_proto()).unwrap();
        assert_eq!(decoded, handle);
        assert_eq!(
            decoded.columns()[0].scalar_integer_domain(),
            Some(ScalarIntegerDomain::Int8)
        );
    }

    #[test]
    fn scalar_integer_manifest_bounds_reject_corruption_before_pruning() {
        use crate::scalar_integer_domain::ScalarIntegerDomain;
        let declarations = BTreeMap::from([(1, ScalarIntegerDomain::Int8)]);
        validate_scalar_integer_manifest_facts(
            &declarations,
            &facts_for_int_bounds(-128, 127),
            None,
            None,
        )
        .expect("valid narrow bounds");
        for (lower, upper) in [(-129, 0), (0, 128)] {
            let error = validate_scalar_integer_manifest_facts(
                &declarations,
                &facts_for_int_bounds(lower, upper),
                None,
                None,
            )
            .expect_err(
                "out-of-domain supplied bound is corrupt, even if a predicate would prune it",
            );
            assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        }
        let mut missing = facts_for_int_bounds(-128, 127);
        missing.lower_bounds.clear();
        missing.upper_bounds.clear();
        validate_scalar_integer_manifest_facts(&declarations, &missing, None, None)
            .expect("absent bounds are unknown, not corrupt");
        let mut wrong_carrier = missing;
        wrong_carrier.lower_bounds.insert(1, Datum::long(0));
        assert_eq!(
            validate_scalar_integer_manifest_facts(&declarations, &wrong_carrier, None, None,)
                .expect_err("wrong metric carrier")
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn scalar_integer_identity_partition_facts_are_checked_before_pruning() {
        use crate::scalar_integer_domain::ScalarIntegerDomain;
        let declarations = BTreeMap::from([(1, ScalarIntegerDomain::Int8)]);
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::optional(
                    1,
                    "tiny",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .unwrap(),
        );
        let spec = PartitionSpec::builder(schema)
            .with_spec_id(4)
            .add_partition_field("tiny", "tiny_part", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let mut facts = facts_for_int_bounds(-128, 127);
        facts.lower_bounds.clear();
        facts.upper_bounds.clear();
        for value in [
            None,
            Some(Literal::Primitive(PrimitiveLiteral::Int(-128))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(127))),
        ] {
            validate_scalar_integer_manifest_facts(
                &declarations,
                &facts,
                Some(&spec),
                Some(&Struct::from_iter([value])),
            )
            .expect("NULL and valid exact partition constants");
        }
        for value in [
            Literal::Primitive(PrimitiveLiteral::Int(128)),
            Literal::Primitive(PrimitiveLiteral::Long(0)),
        ] {
            assert_eq!(
                validate_scalar_integer_manifest_facts(
                    &declarations,
                    &facts,
                    Some(&spec),
                    Some(&Struct::from_iter([Some(value)]))
                )
                .unwrap_err()
                .kind(),
                ConnectorErrorKind::CorruptData
            );
        }
    }
}

#[cfg(test)]
mod historical_window_schema_tests {
    use super::*;
    use crate::iceberg::spec::{Operation, Snapshot, SortOrder, Summary, TableMetadataBuilder};

    #[test]
    fn equal_fabricated_change_window_endpoints_are_validated_before_empty_window() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "key",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory:///unadmitted-window".to_owned(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let error = pinned_change_window_handle(
            &SchemaTableName::try_new("db", "orders").unwrap(),
            &metadata,
            ConnectorReadChangeWindow::new(0, 0),
        )
        .unwrap_err();
        assert_eq!(error.message(), "iceberg snapshot 0 does not exist");
    }

    #[test]
    fn later_promotion_does_not_rebind_historical_window_admission_or_execution() {
        let schema = |primitive| {
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "key",
                    Type::Primitive(primitive),
                ))])
                .build()
                .unwrap()
        };
        let metadata = TableMetadataBuilder::new(
            schema(PrimitiveType::Int),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory:///window".to_owned(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let timestamp = metadata.last_updated_ms();
        let snapshot = |id, parent| {
            Snapshot::builder()
                .with_snapshot_id(id)
                .with_parent_snapshot_id(parent)
                .with_sequence_number(id)
                .with_timestamp_ms(timestamp)
                .with_manifest_list(format!("memory:///window-{id}.avro"))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .with_schema_id(0)
                .build()
        };
        let metadata = metadata
            .into_builder(None)
            .add_snapshot(snapshot(1, None))
            .unwrap()
            .add_snapshot(snapshot(2, Some(1)))
            .unwrap()
            .add_current_schema(schema(PrimitiveType::Long))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert_eq!(
            metadata
                .current_schema()
                .field_by_id(1)
                .unwrap()
                .field_type
                .as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
        let admitted = crate::change_planning::resolved_window_schema(&metadata, 2).unwrap();
        let handle = pinned_change_window_handle(
            &SchemaTableName::try_new("db", "t").unwrap(),
            &metadata,
            ConnectorReadChangeWindow::new(1, 2),
        )
        .unwrap()
        .unwrap();
        assert_eq!(admitted.as_ref(), &handle.parse_table_schema().unwrap());
        assert_eq!(
            admitted.field_by_id(1).unwrap().field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Int)
        );
        assert_eq!(
            handle.from_read_domain().endpoint().schema().unwrap(),
            *admitted
        );
        assert_eq!(
            handle.to_read_domain().endpoint().schema().unwrap(),
            *admitted
        );
    }
}

#[cfg(test)]
mod system_work_unit_tests {
    use super::*;

    fn reference(kind: crate::typed_read::IcebergSystemTableType) -> IcebergSystemTableReference {
        IcebergSystemTableReference::try_new(crate::typed_read::IcebergSystemTableReferenceParams {
            schema_table_name: SchemaTableName::try_new("db", "t").unwrap(),
            system_table_type: kind,
            metadata_file_location: "s3://warehouse/db/t/metadata/v1.json".into(),
            table_uuid: "9d1f4c1e-6a1f-4a0b-9c3a-0f2b6d5e7a11".into(),
            snapshot_id: Some(11),
        })
        .unwrap()
    }

    #[test]
    fn singleton_source_emits_exactly_one_frozen_unit_then_eos() {
        use crate::typed_read::IcebergSystemTableType as Kind;
        let filter = DynamicFilterSnapshot::all_complete();
        for kind in [Kind::Snapshots, Kind::History, Kind::Refs, Kind::Manifests] {
            let reference = reference(kind);
            let mut source = RuntimeSystemRelationSource {
                split: Some(SystemRelationSplit::try_new(reference.clone()).unwrap()),
            };
            assert!(!source.is_finished());
            assert!(source.next_batch(0, &filter).is_err());
            let batch = source.next_batch(9, &filter).unwrap();
            assert!(batch.no_more_splits());
            let splits = batch.into_splits();
            assert_eq!(splits.len(), 1);
            let crate::typed_read::IcebergReadSplit::SystemRelation(split) = &splits[0] else {
                panic!("relation unit")
            };
            assert_eq!(split.reference(), &reference);
            assert!(source.is_finished());
            assert!(
                source
                    .next_batch(1, &filter)
                    .unwrap()
                    .into_splits()
                    .is_empty()
            );
            source.close().unwrap();
            source.close().unwrap();
        }
    }

    #[test]
    fn closed_singleton_and_empty_manifest_sources_emit_no_units() {
        let filter = DynamicFilterSnapshot::all_complete();
        let mut source = RuntimeSystemRelationSource {
            split: Some(
                SystemRelationSplit::try_new(reference(
                    crate::typed_read::IcebergSystemTableType::Snapshots,
                ))
                .unwrap(),
            ),
        };
        source.close().unwrap();
        assert!(
            source
                .next_batch(1, &filter)
                .unwrap()
                .into_splits()
                .is_empty()
        );
        let mut empty = RuntimeSystemRelationSource { split: None };
        assert!(empty.is_finished());
        assert!(
            empty
                .next_batch(1, &filter)
                .unwrap()
                .into_splits()
                .is_empty()
        );
    }
}

#[cfg(test)]
mod prepared_source_relation_tests {
    use super::*;
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{CatalogHandle, ConnectorExactSemanticRevision};
    use novarocks_spi::connector::{
        ConnectorChangeWindowAdmission, ConnectorChangeWindowFullRebuildReason,
        ConnectorChangeWindowPartitionImpact,
    };

    fn metadata() -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let mut builder = crate::iceberg::spec::TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://prepared-source".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap();
        for (id, parent, seq) in [(41, None, 1), (42, Some(41), 2)] {
            builder = builder
                .add_snapshot(
                    crate::iceberg::spec::Snapshot::builder()
                        .with_snapshot_id(id)
                        .with_parent_snapshot_id(parent)
                        .with_sequence_number(seq)
                        .with_timestamp_ms(1_700_000_000_000 + seq)
                        .with_manifest_list(format!("memory://prepared-source/{id}.avro"))
                        .with_summary(crate::iceberg::spec::Summary {
                            operation: crate::iceberg::spec::Operation::Append,
                            additional_properties: HashMap::new(),
                        })
                        .build(),
                )
                .unwrap();
        }
        builder.build().unwrap().metadata
    }

    /// Immutable owner-local matcher fixture, without CPU/IO grants.
    fn inventory_fixture() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        Arc<IcebergTypedBoundary>,
        Arc<IcebergPreparedSourceRead>,
    ) {
        let executor = tokio::runtime::Runtime::new().unwrap();
        let warehouse = tempfile::tempdir().unwrap();
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".into(),
                warehouse.path().display().to_string(),
            )],
        )
        .unwrap();
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(executor.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(executor.handle().clone())),
        );
        let resources = crate::resources::IcebergMetadataResources::new(
            binding.clone(),
            executor.handle().clone(),
        );
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                crate::catalog_control::IcebergCatalogControlState::new(configuration),
                resources,
            )
            .unwrap(),
        );
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse(crate::PROVIDER_ID)
                .unwrap(),
            instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("ice").unwrap(),
        };
        let incarnation = ProviderBindingEpoch::from_bytes([0x67; 16]);
        let catalog = CatalogHandle::new(
            descriptor.instance_id.clone(),
            novarocks_spi::connector::CatalogVersion::from_bytes([0x45; 32]),
        );
        let context = ConnectorRequestContext::try_new(
            Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap();
        let mut value = serde_json::to_value(metadata()).unwrap();
        value["location"] = serde_json::json!(warehouse.path().display().to_string());
        let metadata: TableMetadata = serde_json::from_value(value).unwrap();
        let name = SchemaTableName::try_new("db", "t").unwrap();
        let physical = IcebergPhysicalTable::new(
            crate::iceberg::table::Table::builder()
                .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
                .metadata(Arc::new(metadata.clone()))
                .file_io(crate::fs_io::build_file_io_for_location(
                    metadata.location(),
                    binding,
                ))
                .runtime(
                    runtime
                        .resources()
                        .catalog_runtime()
                        .iceberg_runtime()
                        .clone(),
                )
                .build()
                .unwrap(),
        );
        let observed = crate::metadata::with_pure_schema_creation(&context, |creation| {
            crate::metadata::build_table_metadata(
                crate::metadata::TableMetadataBuildInput {
                    descriptor: &descriptor,
                    incarnation,
                    request: novarocks_spi::connector::ConnectorTableRequest {
                        table: novarocks_spi::connector::ConnectorTableIdentity {
                            instance_id: descriptor.instance_id.clone(),
                            namespace: "db".into(),
                            table: "t".into(),
                        },
                        resolution:
                            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                        context: context.clone(),
                    },
                    loaded: physical.clone(),
                    table_name: "t".into(),
                    metadata_table_type: None,
                    snapshots: crate::catalog::SnapshotLoading::All,
                    projection: crate::metadata::TableMetadataProjection::SnapshotTo(42),
                },
                creation,
            )
            .map_err(crate::metadata::metadata_creation_error)
        })
        .unwrap();
        let object = novarocks_spi::connector::ConnectorTableObjectId::try_new(bytes::Bytes::from(
            metadata.uuid().to_string(),
        ))
        .unwrap();
        let revision = |id| {
            ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                descriptor.provider_id.clone(),
                &object,
                Some(id),
            )
            .unwrap()
        };
        let request = ConnectorReadSourcePreparationRequest::change_window(
            name.clone(),
            observed.table.clone(),
            observed.source_read_facts().unwrap().clone(),
            object.clone(),
            ConnectorReadChangeWindow::new(41, 42),
            revision(41),
            revision(42),
        );
        let admission = ConnectorChangeWindowAdmission::Incremental {
            has_inserts: true,
            has_deletes: false,
            partition_impact: ConnectorChangeWindowPartitionImpact::Unpartitioned,
        };
        let relations = frozen_prepared_source_relations(
            &name,
            &metadata,
            request.selection(),
            metadata.current_schema().clone(),
            metadata.current_schema_id(),
            Some(&admission),
        )
        .unwrap();
        let source = Arc::new(IcebergPreparedSourceRead {
            owner: novarocks_spi::connector::ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            catalog: catalog.clone(),
            request,
            access: Arc::new(IcebergAttemptTableAccess::freeze(physical)),
            relations,
            admission: Some(admission),
            to_metadata: Arc::new(observed),
        });
        let boundary = Arc::new(
            IcebergTypedBoundary::new(
                descriptor,
                incarnation,
                catalog,
                HiveTransactionHandle::new(true, [0x72; 16]),
                runtime,
            )
            .for_request(context),
        );
        (executor, warehouse, boundary, source)
    }
    fn install_inventory(boundary: &IcebergTypedBoundary, source: &Arc<IcebergPreparedSourceRead>) {
        *boundary
            .prepared_source_accesses
            .as_ref()
            .unwrap()
            .lock()
            .unwrap() = [
            ConnectorReadSourceProjection::To,
            ConnectorReadSourceProjection::From,
            ConnectorReadSourceProjection::Delta,
        ]
        .into_iter()
        .map(|projection| PreparedSourceAccess {
            source: source.clone(),
            projection,
        })
        .collect();
    }

    #[test]
    fn prepared_inventory_checks_all_occurrence_projections_before_refusal() {
        let (_executor, _warehouse, boundary, source) = inventory_fixture();
        install_inventory(&boundary, &source);
        let from = crate::typed_read::IcebergRuntimeRelation::Table(
            source.relations.from.as_ref().unwrap().clone(),
        );
        let found = boundary.prepared_source_access(&from).unwrap().unwrap();
        assert_eq!(found.projection, ConnectorReadSourceProjection::From);
        assert!(Arc::ptr_eq(&found.source, &source));
        let delta = crate::typed_read::IcebergRuntimeRelation::ChangeWindow(
            source.relations.delta.as_ref().unwrap().clone(),
        );
        assert_eq!(
            boundary
                .prepared_source_access(&delta)
                .unwrap()
                .unwrap()
                .projection,
            ConnectorReadSourceProjection::Delta
        );
        let filtered = source
            .relations
            .to
            .apply_filter(&Constraint::of_summary(TupleDomain::all()))
            .unwrap()
            .into_handle();
        let limited = filtered.apply_limit(1).unwrap().into_handle();
        let column =
            IcebergColumnHandle::base_column_of(&limited.parse_table_schema().unwrap(), 1).unwrap();
        let assignment = Assignment::try_new("id", column, ConnectorValueType::BigInt).unwrap();
        let projected = limited
            .apply_projection(&OrderedAssignments::try_new(vec![assignment]).unwrap())
            .unwrap()
            .into_handle();
        assert_eq!(
            boundary
                .prepared_source_access(&crate::typed_read::IcebergRuntimeRelation::Table(
                    projected
                ))
                .unwrap()
                .unwrap()
                .projection,
            ConnectorReadSourceProjection::To
        );
    }

    #[test]
    fn same_name_unknown_domain_and_window_cannot_fall_back_to_original_split_opening() {
        use novarocks_spi::connector::read_stack::adapter::{
            ProviderReadSplitManager, ProviderReadSplitOpenRequest,
        };
        let (_executor, _warehouse, boundary, source) = inventory_fixture();
        install_inventory(&boundary, &source);
        let mut table = source.relations.to.to_proto();
        table.read_domain.as_mut().unwrap().observation_id[0] ^= 1;
        let table = crate::typed_read::IcebergRuntimeRelation::Table(
            IcebergTableHandle::from_proto(&table).unwrap(),
        );
        let mut window = source.relations.delta.as_ref().unwrap().to_proto();
        window.from_snapshot_id_exclusive = window.to_snapshot_id_inclusive;
        window.from_read_domain = window.to_read_domain.clone();
        let window = crate::typed_read::IcebergRuntimeRelation::ChangeWindow(
            IcebergChangeWindowHandle::from_proto(&window).unwrap(),
        );
        for relation in [table, window] {
            let error = boundary.prepared_source_access(&relation).err().unwrap();
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            assert_eq!(
                error.message(),
                "prepared source relation differs from its original prepared inventory"
            );
            let mut owner = None;
            // Synchronous refusal cannot yield a foreign F or install/take Source.
            let result = Arc::clone(&boundary).begin_get_splits(
                ProviderReadSplitOpenRequest {
                    session: ConnectorSession::try_new(
                        "q",
                        "u",
                        "UTC",
                        "en_US",
                        std::time::SystemTime::UNIX_EPOCH,
                    )
                    .unwrap(),
                    table: Arc::new(relation),
                    columns: Vec::new(),
                    dynamic_filter_columns: BTreeSet::new(),
                    constraint: Constraint::of_summary(TupleDomain::all()),
                    preparation: None,
                },
                &mut owner,
            );
            let error = result.err().expect("no ordinary foreign future may escape");
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            assert_eq!(
                error.message(),
                "prepared source relation differs from its original prepared inventory"
            );
            assert!(owner.is_none());
        }
    }

    #[test]
    fn empty_or_different_name_inventory_preserves_ordinary_none() {
        let (_executor, _warehouse, boundary, source) = inventory_fixture();
        let same = crate::typed_read::IcebergRuntimeRelation::Table(source.relations.to.clone());
        assert!(boundary.prepared_source_access(&same).unwrap().is_none());
        let mut no_inventory = boundary.as_ref().clone();
        no_inventory.prepared_source_accesses = None;
        assert!(
            no_inventory
                .prepared_source_access(&same)
                .unwrap()
                .is_none()
        );
        install_inventory(&boundary, &source);
        let mut table = source.relations.to.to_proto();
        table.schema_table_name.as_mut().unwrap().table_name = "ordinary_other".into();
        let other = crate::typed_read::IcebergRuntimeRelation::Table(
            IcebergTableHandle::from_proto(&table).unwrap(),
        );
        assert!(boundary.prepared_source_access(&other).unwrap().is_none());
    }
    #[test]
    fn prepared_incremental_relations_share_the_original_endpoint_domains() {
        let metadata = metadata();
        let relations = frozen_prepared_source_relations(
            &SchemaTableName::try_new("db", "t").unwrap(),
            &metadata,
            ConnectorReadSourceSelection::ChangeWindow(ConnectorReadChangeWindow::new(41, 42)),
            metadata.current_schema().clone(),
            metadata.current_schema_id(),
            Some(&ConnectorChangeWindowAdmission::Incremental {
                has_inserts: true,
                has_deletes: false,
                partition_impact: ConnectorChangeWindowPartitionImpact::Unpartitioned,
            }),
        )
        .unwrap();
        let delta = relations.delta.unwrap();
        let from = relations.from.unwrap();
        assert_eq!(from.snapshot_id(), Some(41));
        assert_eq!(relations.to.snapshot_id(), Some(42));
        assert_eq!(from.read_domain(), Some(delta.from_read_domain()));
        assert_eq!(relations.to.read_domain(), Some(delta.to_read_domain()));
        let decoded = IcebergChangeWindowHandle::from_proto(&delta.to_proto()).unwrap();
        assert_eq!(decoded, delta);
        assert!(same_source_table(
            &relations.to.apply_limit(1).unwrap().into_handle(),
            &relations.to
        ));
    }

    #[test]
    fn prepared_snapshot_and_full_rebuild_do_not_manufacture_from_or_delta_relations() {
        let metadata = metadata();
        for admission in [
            None,
            Some(ConnectorChangeWindowAdmission::FullRebuild(
                ConnectorChangeWindowFullRebuildReason::LineageBroken {
                    from_snapshot_id: 999,
                },
            )),
        ] {
            let selection = if admission.is_none() {
                ConnectorReadSourceSelection::Snapshot(42)
            } else {
                ConnectorReadSourceSelection::ChangeWindow(ConnectorReadChangeWindow::new(999, 42))
            };
            let relations = frozen_prepared_source_relations(
                &SchemaTableName::try_new("db", "t").unwrap(),
                &metadata,
                selection,
                metadata.current_schema().clone(),
                metadata.current_schema_id(),
                admission.as_ref(),
            )
            .unwrap();
            assert_eq!(relations.to.snapshot_id(), Some(42));
            assert!(relations.from.is_none());
            assert!(relations.delta.is_none());
        }
    }

    #[test]
    fn prepared_metadata_only_has_exact_empty_delta_without_executable_from() {
        let metadata = metadata();
        for from in [41, 42] {
            let relations = frozen_prepared_source_relations(
                &SchemaTableName::try_new("db", "t").unwrap(),
                &metadata,
                ConnectorReadSourceSelection::ChangeWindow(ConnectorReadChangeWindow::new(
                    from, 42,
                )),
                metadata.current_schema().clone(),
                metadata.current_schema_id(),
                Some(&ConnectorChangeWindowAdmission::MetadataOnly),
            )
            .unwrap();
            assert!(relations.from.is_none());
            let delta = relations.delta.unwrap();
            assert_eq!(delta.from_snapshot_id_exclusive(), from);
            assert_eq!(delta.to_snapshot_id_inclusive(), 42);
            assert_eq!(relations.to.read_domain(), Some(delta.to_read_domain()));
            assert_eq!(
                IcebergChangeWindowHandle::from_proto(&delta.to_proto()).unwrap(),
                delta
            );
            let outcome = crate::typed_read::IcebergChangeWindowPlan::try_plan(
                delta,
                crate::typed_read::IcebergEndpointVisibility::Proven,
                Vec::new(),
            )
            .unwrap();
            let crate::typed_read::IcebergChangeWindowPlanOutcome::Incremental(plan) = outcome
            else {
                panic!("closed zero contribution")
            };
            assert!(plan.into_splits().is_empty());
        }
        assert!(
            frozen_prepared_source_relations(
                &SchemaTableName::try_new("db", "t").unwrap(),
                &metadata,
                ConnectorReadSourceSelection::ChangeWindow(ConnectorReadChangeWindow::new(999, 42)),
                metadata.current_schema().clone(),
                metadata.current_schema_id(),
                Some(&ConnectorChangeWindowAdmission::MetadataOnly),
            )
            .is_err()
        );
    }

    #[test]
    fn metadata_only_delta_rejects_another_exact_window_before_empty_opening() {
        use novarocks_spi::connector::read_stack::adapter::{
            ProviderReadSplitManager, ProviderReadSplitOpenRequest,
        };
        let (_executor, _warehouse, boundary, mut source) = inventory_fixture();
        let metadata = source.access.metadata().clone();
        let relations = frozen_prepared_source_relations(
            source.request.name(),
            &metadata,
            source.request.selection(),
            metadata.current_schema().clone(),
            metadata.current_schema_id(),
            Some(&ConnectorChangeWindowAdmission::MetadataOnly),
        )
        .unwrap();
        let original = Arc::get_mut(&mut source).unwrap();
        original.relations = relations;
        original.admission = Some(ConnectorChangeWindowAdmission::MetadataOnly);
        install_inventory(&boundary, &source);
        let mut foreign = source.relations.delta.as_ref().unwrap().to_proto();
        foreign.from_snapshot_id_exclusive = foreign.to_snapshot_id_inclusive;
        foreign.from_read_domain = foreign.to_read_domain.clone();
        let relation = crate::typed_read::IcebergRuntimeRelation::ChangeWindow(
            IcebergChangeWindowHandle::from_proto(&foreign).unwrap(),
        );
        let mut owner = None;
        let result = Arc::clone(&boundary).begin_get_splits(
            ProviderReadSplitOpenRequest {
                session: ConnectorSession::try_new(
                    "q",
                    "u",
                    "UTC",
                    "en_US",
                    std::time::SystemTime::UNIX_EPOCH,
                )
                .unwrap(),
                table: Arc::new(relation),
                columns: Vec::new(),
                dynamic_filter_columns: BTreeSet::new(),
                constraint: Constraint::of_summary(TupleDomain::all()),
                preparation: None,
            },
            &mut owner,
        );
        let error = result
            .err()
            .expect("foreign window cannot claim the empty decision");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            error.message(),
            "prepared source relation differs from its original prepared inventory"
        );
        assert!(owner.is_none());
    }

    #[test]
    fn prepared_snapshot_missing_schema_id_uses_only_the_original_pin_fallback() {
        let metadata = metadata();
        let schema = crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::frozen_snapshot_schema(
            &metadata, 42, metadata.current_schema_id(),
        ).unwrap();
        assert_eq!(schema.as_ref(), metadata.current_schema().as_ref());
        assert!(crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::frozen_snapshot_schema(&metadata, 42, i32::MAX).is_err());
        assert!(crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::frozen_snapshot_schema(&metadata, 999, metadata.current_schema_id()).is_err());
    }
}

#[cfg(test)]
mod known_empty_prepared_source_tests {
    use super::*;
    use novarocks_spi::connector::read_stack::adapter::ProviderReadSplitOpenRequest;

    fn request(
        summary: TupleDomain<IcebergColumnHandle>,
    ) -> ProviderReadSplitOpenRequest<IcebergTypedBoundary> {
        ProviderReadSplitOpenRequest {
            session: ConnectorSession::try_new(
                "q",
                "u",
                "UTC",
                "en_US",
                std::time::SystemTime::UNIX_EPOCH,
            )
            .unwrap(),
            table: Arc::new(crate::typed_read::IcebergRuntimeRelation::Table(
                super::final_static_facts_tests::table_handle(),
            )),
            columns: Vec::new(),
            dynamic_filter_columns: BTreeSet::new(),
            constraint: Constraint::of_summary(summary),
            preparation: None,
        }
    }
    #[test]
    fn exact_none_uses_empty_source_but_nonempty_still_requires_the_original_open() {
        let request = request(TupleDomain::none());
        let mut source =
            known_empty_prepared_table_source(&request, IcebergSplitSourceOptions::default())
                .unwrap()
                .expect("exact None has no read");
        let batch = source
            .next_batch(1, &DynamicFilterSnapshot::all_complete())
            .unwrap();
        assert!(batch.no_more_splits());
        assert!(batch.into_splits().is_empty());
        source.close().unwrap();
        assert!(
            known_empty_prepared_table_source(
                &self::request(TupleDomain::all()),
                IcebergSplitSourceOptions::default()
            )
            .unwrap()
            .is_none()
        );
    }
    #[test]
    fn known_empty_does_not_hide_the_original_projection_refusal() {
        let mut request = request(TupleDomain::none());
        let crate::typed_read::IcebergRuntimeRelation::Table(handle) = request.table.as_ref()
        else {
            unreachable!();
        };
        let column =
            IcebergColumnHandle::base_column_of(&handle.parse_table_schema().unwrap(), 1).unwrap();
        let assignment = Assignment::try_new("same", column, ConnectorValueType::BigInt).unwrap();
        request.columns = vec![assignment.clone(), assignment];
        let Err(error) =
            known_empty_prepared_table_source(&request, IcebergSplitSourceOptions::default())
        else {
            panic!("known-empty read skipped original projection validation");
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            error.message(),
            "connector assignment variables must be unique"
        );
    }
}
