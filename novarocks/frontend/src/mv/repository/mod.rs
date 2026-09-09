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

pub mod catalog;
pub mod codec;
pub mod key;
mod operation;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use novarocks_state_store_api::{
    Direction, Key, KeyRange, Precondition, RangeRequest, ReadTransaction, StateRecord, StateStore,
    StateStoreError, StateStoreErrorKind, WriteTransaction,
};
use uuid::Uuid;

use crate::mv::domain::dependency::model::MvDependencyObjectRef;
use crate::mv::domain::persistence::definition::StoredMvDefinition;
use crate::mv::domain::persistence::dependency::{CreateMvDependencyRequest, StoredMvDependency};
use crate::mv::domain::repository::{
    DeleteMvProjectionRequest, LoadedMvProjection, MvProjectionRequest, MvProjectionVersion,
    MvPublishedProjection, MvRepository, MvRepositoryError, MvRepositoryErrorKind, MvTarget,
    MvTargetLookup, ReplaceMvProjectionRequest,
};
use crate::state_store::StateStoreRunPolicy;
use crate::state_store::metrics::{StateStoreConsumer, StateStoreMetrics};

use self::codec::{
    DecodedMvRecord, MvRecordKind, MvSequence, decode_projection, decode_record, encode_projection,
    encode_record,
};
use self::key::{
    accelerator_prefix, dependency_by_downstream_key, dependency_by_downstream_prefix,
    dependency_by_upstream_catalog_prefixes, dependency_by_upstream_key,
    dependency_by_upstream_prefix, projection_by_id_key, projection_prefix, sequence_key,
    target_lookup_catalog_prefix, target_lookup_key,
};

/// StateStore owner for the single current MV Accelerator family.
pub struct StateStoreMvRepository {
    store: Arc<dyn StateStore>,
    run_policy: StateStoreRunPolicy,
    runner_metrics: StateStoreMetrics,
}

impl StateStoreMvRepository {
    /// Opens the repository against one store instance under one application
    /// run policy.
    ///
    /// The policy is supplied rather than read from the store: how many
    /// attempts an MV write is worth and how long the caller will wait are
    /// application decisions, and a provider no longer reports an attempt
    /// ceiling for anyone to borrow.
    pub async fn open(
        store: Arc<dyn StateStore>,
        run_policy: StateStoreRunPolicy,
    ) -> Result<Arc<Self>, MvRepositoryError> {
        Ok(Arc::new(Self {
            runner_metrics: StateStoreMetrics::new(StateStoreConsumer::MV_ACCELERATOR),
            store,
            run_policy,
        }))
    }

    async fn scan_prefix(&self, prefix: Key) -> Result<Vec<StateRecord>, MvRepositoryError> {
        scan_read_prefix(
            self.store.as_ref(),
            prefix,
            self.store.limits().max_page_size,
        )
        .await
        .map_err(operation::state_store_error)
    }

    async fn read_record(&self, key: &Key) -> Result<Option<StateRecord>, MvRepositoryError> {
        let mut transaction = self
            .store
            .begin_read()
            .await
            .map_err(operation::state_store_error)?;
        let record = transaction
            .get(key)
            .await
            .map_err(operation::state_store_error)?;
        transaction
            .abort()
            .await
            .map_err(operation::state_store_error)?;
        Ok(record)
    }
}

#[async_trait::async_trait]
impl MvRepository for StateStoreMvRepository {
    async fn create_projection(
        &self,
        operation_id: Uuid,
        projection: MvProjectionRequest,
    ) -> Result<LoadedMvProjection, MvRepositoryError> {
        validate_projection_request(&projection)?;
        let dependencies = deduplicate_dependencies(0, &projection.dependencies)?;
        let store = Arc::clone(&self.store);
        let mv_id = operation::run(
            store.as_ref(),
            &self.runner_metrics,
            self.run_policy,
            "create MV Accelerator projection",
            move |transaction| {
                let projection = projection.clone();
                let dependencies = dependencies.clone();
                Box::pin(async move {
                    let sequence_key = sequence_key().map_err(invalid_state_store)?;
                    let (next_id, sequence_precondition) =
                        match transaction.get(&sequence_key).await? {
                            Some(record) => {
                                let decoded: DecodedMvRecord<MvSequence> =
                                    decode_record(&sequence_key, &record.value)
                                        .map_err(invalid_state_store)?;
                                let next_id =
                                    decoded.value.last_allocated_id.checked_add(1).ok_or_else(
                                        || invalid_state_store("MV ID sequence exhausted"),
                                    )?;
                                (next_id, Precondition::Version(record.version))
                            }
                            None => (1, Precondition::Absent),
                        };
                    let definition = definition_from_request(next_id, &projection);
                    let root_key = projection_by_id_key(next_id).map_err(invalid_state_store)?;
                    if transaction.get(&root_key).await?.is_some() {
                        return Err(conflict_state_store("MV Accelerator projection ID exists"));
                    }
                    let target = definition_target(&definition).map_err(invalid_state_store)?;
                    let target_key = target_key(&target).map_err(invalid_state_store)?;
                    if transaction.get(&target_key).await?.is_some() {
                        return Err(conflict_state_store("MV Accelerator target already exists"));
                    }
                    put_sequence(
                        transaction,
                        operation_id,
                        MvSequence {
                            last_allocated_id: next_id,
                        },
                        sequence_precondition,
                    )
                    .await?;
                    put_projection(transaction, operation_id, &definition, Precondition::Absent)
                        .await?;
                    put_target_lookup(
                        transaction,
                        operation_id,
                        target_key,
                        next_id,
                        Precondition::Absent,
                    )
                    .await?;
                    for dependency in dependencies_for_mv(next_id, &dependencies) {
                        put_dependency_pair(transaction, operation_id, &dependency).await?;
                    }
                    Ok(next_id)
                })
            },
        )
        .await?;
        self.load_by_id(mv_id)
            .await?
            .ok_or_else(|| corruption("created MV Accelerator projection is missing"))
    }

    async fn replace_projection(
        &self,
        operation_id: Uuid,
        request: ReplaceMvProjectionRequest,
    ) -> Result<LoadedMvProjection, MvRepositoryError> {
        if request.mv_id <= 0 {
            return Err(invalid("MV projection ID must be positive"));
        }
        validate_projection_request(&request.projection)?;
        let mv_id = request.mv_id;
        let desired_dependencies =
            deduplicate_dependencies(request.mv_id, &request.projection.dependencies)?;
        let expected_version = request.expected_version.store_version().clone();
        let page_size = self.store.limits().max_page_size;
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            self.run_policy,
            "replace MV Accelerator projection",
            move |transaction| {
                let request = request.clone();
                let expected_version = expected_version.clone();
                let desired_dependencies = desired_dependencies.clone();
                Box::pin(async move {
                    let root_key =
                        projection_by_id_key(request.mv_id).map_err(invalid_state_store)?;
                    let root_record = transaction
                        .get(&root_key)
                        .await?
                        .ok_or_else(|| conflict_state_store("MV projection changed before CAS"))?;
                    if root_record.version != expected_version {
                        return Err(conflict_state_store("MV projection changed before CAS"));
                    }
                    let current = decode_projection(&root_key, &root_record.value)
                        .map_err(invalid_state_store)?
                        .value;
                    let next = definition_from_request(request.mv_id, &request.projection);
                    replace_target_index(transaction, operation_id, &current, &next).await?;
                    replace_dependency_indexes(
                        transaction,
                        operation_id,
                        request.mv_id,
                        &desired_dependencies,
                        page_size,
                    )
                    .await?;
                    put_projection(
                        transaction,
                        operation_id,
                        &next,
                        Precondition::Version(root_record.version),
                    )
                    .await?;
                    Ok(())
                })
            },
        )
        .await?;
        self.load_by_id(mv_id)
            .await?
            .ok_or_else(|| corruption("replaced MV Accelerator projection is missing"))
    }

    async fn load_by_id(
        &self,
        mv_id: i64,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        if mv_id <= 0 {
            return Err(invalid("MV projection ID must be positive"));
        }
        let key = projection_by_id_key(mv_id).map_err(corruption)?;
        self.read_record(&key)
            .await?
            .map(|record| loaded_projection(&key, record))
            .transpose()
    }

    async fn find_by_target(
        &self,
        target: &MvTarget,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        let key = target_key(target).map_err(corruption)?;
        let Some(lookup_record) = self.read_record(&key).await? else {
            return Ok(None);
        };
        let lookup: DecodedMvRecord<MvTargetLookup> =
            decode_record(&key, &lookup_record.value).map_err(corruption)?;
        let loaded = self.load_by_id(lookup.value.mv_id).await?.ok_or_else(|| {
            corruption("MV Accelerator target lookup references a missing projection")
        })?;
        if definition_target(&loaded.definition).map_err(corruption)? != *target {
            return Err(corruption(
                "MV Accelerator target lookup does not match its projection",
            ));
        }
        Ok(Some(loaded))
    }

    async fn list_projections(&self) -> Result<Vec<LoadedMvProjection>, MvRepositoryError> {
        let records = self
            .scan_prefix(projection_prefix().map_err(corruption)?)
            .await?;
        records
            .into_iter()
            .map(|record| {
                let key = record.key.clone();
                loaded_projection(&key, record)
            })
            .collect()
    }

    /// A delete stamps no record, so it carries no operation provenance; the
    /// runner owns the attempt identity this write is retried under.
    async fn delete_projection(
        &self,
        _operation_id: Uuid,
        request: DeleteMvProjectionRequest,
    ) -> Result<bool, MvRepositoryError> {
        if request.mv_id <= 0 {
            return Err(invalid("MV projection ID must be positive"));
        }
        let expected_version = request.expected_version.store_version().clone();
        let page_size = self.store.limits().max_page_size;
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            self.run_policy,
            "delete MV Accelerator projection",
            move |transaction| {
                let request = request.clone();
                let expected_version = expected_version.clone();
                Box::pin(async move {
                    let root_key =
                        projection_by_id_key(request.mv_id).map_err(invalid_state_store)?;
                    let Some(root_record) = transaction.get(&root_key).await? else {
                        return Ok(false);
                    };
                    if root_record.version != expected_version {
                        return Err(conflict_state_store("MV projection changed before delete"));
                    }
                    let definition = decode_projection(&root_key, &root_record.value)
                        .map_err(invalid_state_store)?
                        .value;
                    if definition.source_revision != request.expected_source_revision {
                        return Err(conflict_state_store(
                            "MV source revision changed before delete",
                        ));
                    }
                    delete_indexes_for_projection(
                        transaction,
                        &definition,
                        request.mv_id,
                        page_size,
                    )
                    .await?;
                    transaction
                        .delete(root_key, Precondition::Version(root_record.version))
                        .await?;
                    Ok(true)
                })
            },
        )
        .await
    }

    async fn wipe_accelerator(&self, _operation_id: Uuid) -> Result<(), MvRepositoryError> {
        let records = self
            .scan_prefix(accelerator_prefix().map_err(corruption)?)
            .await?;
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            self.run_policy,
            "wipe MV Accelerator family",
            move |transaction| {
                let records = records.clone();
                Box::pin(async move {
                    for record in &records {
                        let current = transaction.get(&record.key).await?.ok_or_else(|| {
                            conflict_state_store("MV Accelerator changed before wipe")
                        })?;
                        if current.version != record.version {
                            return Err(conflict_state_store("MV Accelerator changed before wipe"));
                        }
                        transaction
                            .delete(
                                record.key.clone(),
                                Precondition::Version(record.version.clone()),
                            )
                            .await?;
                    }
                    Ok(())
                })
            },
        )
        .await
    }

    async fn list_dependencies_by_downstream(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        let records = self
            .scan_prefix(dependency_by_downstream_prefix(mv_id).map_err(corruption)?)
            .await?;
        decode_dependencies(records)
    }

    async fn list_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        let records = self
            .scan_prefix(dependency_by_upstream_prefix(upstream).map_err(corruption)?)
            .await?;
        decode_dependencies(records)
    }

    async fn wipe_projection_by_target(
        &self,
        operation_id: Uuid,
        target: &MvTarget,
    ) -> Result<bool, MvRepositoryError> {
        let Some(loaded) = self.find_by_target(target).await? else {
            return Ok(false);
        };
        self.delete_projection(
            operation_id,
            DeleteMvProjectionRequest {
                mv_id: loaded.definition.mv_id,
                expected_version: loaded.version,
                expected_source_revision: loaded.definition.source_revision,
            },
        )
        .await
    }

    async fn ensure_no_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        let dependencies = self.list_downstream_dependencies(upstream).await?;
        if dependencies.is_empty() {
            Ok(())
        } else {
            Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                format!(
                    "{} has downstream materialized views: {}",
                    upstream.display_name(),
                    dependencies
                        .iter()
                        .map(|dependency| dependency.downstream_mv_id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ))
        }
    }
}

async fn scan_transaction_range(
    transaction: &mut dyn ReadTransaction,
    range: KeyRange,
    page_size: usize,
) -> Result<Vec<StateRecord>, StateStoreError> {
    let mut continuation = None;
    let mut records = Vec::new();
    loop {
        let page = transaction
            .range(&RangeRequest {
                range: range.clone(),
                direction: Direction::Forward,
                page_size,
                continuation: continuation.clone(),
            })
            .await?;
        continuation = page.continuation;
        records.extend(page.records);
        if continuation.is_none() {
            return Ok(records);
        }
    }
}

async fn scan_read_prefix(
    store: &dyn StateStore,
    prefix: Key,
    page_size: usize,
) -> Result<Vec<StateRecord>, StateStoreError> {
    let range = KeyRange::for_prefix(prefix)?;
    let mut transaction = store.begin_read().await?;
    let records = scan_transaction_range(transaction.as_mut(), range, page_size).await?;
    transaction.abort().await?;
    Ok(records)
}

async fn scan_write_prefix(
    transaction: &mut dyn WriteTransaction,
    prefix: Key,
    page_size: usize,
) -> Result<Vec<StateRecord>, StateStoreError> {
    let range = KeyRange::for_prefix(prefix)?;
    scan_transaction_range(transaction, range, page_size).await
}

/// How the current Accelerator names a catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvCatalogReference {
    /// A current MV writes its projection into this catalog.
    Target,
    /// A current MV reads an upstream object out of this catalog.
    UpstreamDependency,
}

impl MvCatalogReference {
    pub(crate) const fn describe(self) -> &'static str {
        match self {
            Self::Target => "a materialized view target",
            Self::UpstreamDependency => "materialized view upstream dependencies",
        }
    }
}

/// Reads whether the current Accelerator still names `catalog` as an MV target
/// or upstream dependency.  Only current keys are consulted; historical MV
/// families are intentionally not compatibility data.
///
/// This is a plain read path, and deliberately so.  The same two prefixes used
/// to be scanned inside the catalog attachment delete transaction, which read
/// like a cross-family serializability fence between `DROP CATALOG` and MV DDL.
/// It never was one: the family scanned here is a rebuildable Accelerator that
/// may legitimately be wiped in whole, so the "guarantee" evaporated exactly
/// when the cache was cleared, and the parties that actually race — concurrent
/// MV DDL and an external catalog desired-state controller — were never
/// participants in that transaction anyway.  Keeping the scan transactional
/// only advertised a tool that does not exist.  The caller therefore treats
/// this as an observation, not a fence; see
/// `CatalogAttachmentRepository::observe_materialized_view_references`.
pub(crate) async fn observe_catalog_references(
    store: &dyn StateStore,
    catalog: &str,
    page_size: usize,
) -> Result<Option<MvCatalogReference>, StateStoreError> {
    if !scan_read_prefix(
        store,
        target_lookup_catalog_prefix(catalog).map_err(invalid_state_store)?,
        page_size,
    )
    .await?
    .is_empty()
    {
        return Ok(Some(MvCatalogReference::Target));
    }
    for prefix in dependency_by_upstream_catalog_prefixes(catalog).map_err(invalid_state_store)? {
        if !scan_read_prefix(store, prefix, page_size).await?.is_empty() {
            return Ok(Some(MvCatalogReference::UpstreamDependency));
        }
    }
    Ok(None)
}

fn loaded_projection(
    key: &Key,
    record: StateRecord,
) -> Result<LoadedMvProjection, MvRepositoryError> {
    let definition = decode_projection(key, &record.value)
        .map_err(corruption)?
        .value;
    Ok(LoadedMvProjection {
        definition,
        version: MvProjectionVersion::from_store(record.version),
    })
}

fn validate_projection_request(request: &MvProjectionRequest) -> Result<(), MvRepositoryError> {
    request
        .definition
        .query_definition
        .validate()
        .map_err(|error| invalid(format!("invalid persisted MV query definition: {error}")))?;
    if !request
        .definition
        .storage_engine
        .eq_ignore_ascii_case("iceberg")
    {
        return Err(invalid(
            "MV Accelerator accepts only lake-backed Iceberg projections",
        ));
    }
    if request
        .definition
        .target_catalog
        .as_deref()
        .is_none_or(str::is_empty)
        || request
            .definition
            .target_namespace
            .as_deref()
            .is_none_or(str::is_empty)
        || request
            .definition
            .target_table
            .as_deref()
            .is_none_or(str::is_empty)
    {
        return Err(invalid(
            "MV Accelerator projection requires an exact target",
        ));
    }
    if request.source_revision.descriptor_content_hash.is_empty() {
        return Err(invalid(
            "MV source revision descriptor hash must not be empty",
        ));
    }
    if request
        .source_revision
        .current_target_snapshot_id
        .is_some_and(|snapshot_id| snapshot_id < 0)
    {
        return Err(invalid("MV source revision snapshot must not be negative"));
    }
    if request.definition.created_at_ms < 0 {
        return Err(invalid("MV projection creation time must not be negative"));
    }
    if request.refresh.interval_ms.is_some_and(|value| value <= 0)
        || request
            .refresh
            .max_staleness_ms
            .is_some_and(|value| value < 0)
    {
        return Err(invalid("MV refresh configuration has an invalid duration"));
    }
    if request.refresh.interval_ms.is_some() && !request.refresh.policy.accepts_interval() {
        return Err(invalid(
            "MV refresh interval is only valid for ASYNC_INTERVAL",
        ));
    }
    match &request.publication {
        MvPublishedProjection::NeverPublished => {
            if request.source_revision.current_target_snapshot_id.is_some() {
                // A bootstrap target snapshot is allowed by the lake contract;
                // it is a source revision and not an invented waterline.
            }
        }
        MvPublishedProjection::Published(waterline) => {
            if waterline.last_refresh_ms < 0
                || waterline.last_refresh_rows < 0
                || waterline.last_refreshed_iceberg_snapshot_id < 0
                || request.source_revision.current_target_snapshot_id
                    != Some(waterline.last_refreshed_iceberg_snapshot_id)
            {
                return Err(invalid(
                    "published MV waterline does not match its source revision",
                ));
            }
            if waterline.base_snapshots.keys().collect::<BTreeSet<_>>()
                != waterline
                    .base_table_object_ids
                    .keys()
                    .collect::<BTreeSet<_>>()
            {
                return Err(invalid(
                    "published MV base snapshots and object identities differ",
                ));
            }
            if waterline
                .base_snapshots
                .values()
                .any(|snapshot_id| *snapshot_id < 0)
            {
                return Err(invalid("published MV base snapshot must not be negative"));
            }
        }
    }
    Ok(())
}

fn definition_from_request(mv_id: i64, request: &MvProjectionRequest) -> StoredMvDefinition {
    let (
        last_refresh_ms,
        last_refresh_rows,
        last_refresh_snapshots,
        last_refresh_table_object_ids,
        last_refreshed_iceberg_snapshot_id,
    ) = match &request.publication {
        MvPublishedProjection::NeverPublished => {
            (None, None, BTreeMap::new(), BTreeMap::new(), None)
        }
        MvPublishedProjection::Published(waterline) => (
            Some(waterline.last_refresh_ms),
            Some(waterline.last_refresh_rows),
            waterline.base_snapshots.clone(),
            waterline.base_table_object_ids.clone(),
            Some(waterline.last_refreshed_iceberg_snapshot_id),
        ),
    };
    StoredMvDefinition {
        mv_id,
        query_definition: request.definition.query_definition.clone(),
        base_table_refs: request.definition.base_table_refs.clone(),
        primary_key_columns: request.definition.primary_key_columns.clone(),
        storage_engine: request.definition.storage_engine.clone(),
        target_catalog: request.definition.target_catalog.clone(),
        target_namespace: request.definition.target_namespace.clone(),
        target_table: request.definition.target_table.clone(),
        schema_contract: request.definition.schema_contract.clone(),
        partition_spec: request.definition.partition_spec.clone(),
        last_refresh_ms,
        last_refresh_rows,
        last_refresh_snapshots,
        last_refresh_table_object_ids,
        last_refreshed_iceberg_snapshot_id,
        refresh_policy: request.refresh.policy.clone(),
        refresh_paused: request.refresh.paused,
        refresh_interval_ms: request.refresh.interval_ms,
        max_staleness_ms: request.refresh.max_staleness_ms,
        created_at_ms: request.definition.created_at_ms,
        source_revision: request.source_revision.clone(),
    }
}

fn definition_target(definition: &StoredMvDefinition) -> Result<MvTarget, String> {
    Ok(MvTarget {
        catalog: Some(
            definition
                .target_catalog
                .clone()
                .ok_or_else(|| "MV projection target catalog is missing".to_string())?,
        ),
        database: definition
            .target_namespace
            .clone()
            .ok_or_else(|| "MV projection target namespace is missing".to_string())?,
        name: definition
            .target_table
            .clone()
            .ok_or_else(|| "MV projection target table is missing".to_string())?,
    })
}

fn target_key(target: &MvTarget) -> Result<Key, String> {
    target_lookup_key(
        target.catalog.as_deref().unwrap_or_default(),
        &target.database,
        &target.name,
    )
}

fn deduplicate_dependencies(
    mv_id: i64,
    requests: &[CreateMvDependencyRequest],
) -> Result<Vec<CreateMvDependencyRequest>, MvRepositoryError> {
    let mut unique = BTreeMap::new();
    for request in requests {
        if request.created_at_ms < 0 {
            return Err(invalid("MV dependency creation time must not be negative"));
        }
        let key = dependency_by_downstream_key(mv_id.max(1), &request.upstream).map_err(invalid)?;
        if let Some(existing) = unique.insert(key, request.clone())
            && existing != *request
        {
            return Err(invalid("duplicate MV dependency has conflicting facts"));
        }
    }
    Ok(unique.into_values().collect())
}

fn dependencies_for_mv(
    mv_id: i64,
    requests: &[CreateMvDependencyRequest],
) -> Vec<StoredMvDependency> {
    requests
        .iter()
        .map(|request| StoredMvDependency {
            downstream_mv_id: mv_id,
            upstream: request.upstream.clone(),
            created_at_ms: request.created_at_ms,
        })
        .collect()
}

async fn put_sequence(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    sequence: MvSequence,
    precondition: Precondition,
) -> Result<(), StateStoreError> {
    let key = sequence_key().map_err(invalid_state_store)?;
    let value = encode_record(MvRecordKind::Sequence, operation_id, &sequence)
        .map_err(invalid_state_store)?;
    transaction.put(key, value, precondition).await
}

async fn put_projection(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    definition: &StoredMvDefinition,
    precondition: Precondition,
) -> Result<(), StateStoreError> {
    let key = projection_by_id_key(definition.mv_id).map_err(invalid_state_store)?;
    let value = encode_projection(operation_id, definition).map_err(invalid_state_store)?;
    transaction.put(key, value, precondition).await
}

async fn put_target_lookup(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    key: Key,
    mv_id: i64,
    precondition: Precondition,
) -> Result<(), StateStoreError> {
    let value = encode_record(
        MvRecordKind::TargetLookup,
        operation_id,
        &MvTargetLookup { mv_id },
    )
    .map_err(invalid_state_store)?;
    transaction.put(key, value, precondition).await
}

async fn put_dependency_pair(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    dependency: &StoredMvDependency,
) -> Result<(), StateStoreError> {
    let downstream =
        dependency_by_downstream_key(dependency.downstream_mv_id, &dependency.upstream)
            .map_err(invalid_state_store)?;
    let upstream = dependency_by_upstream_key(&dependency.upstream, dependency.downstream_mv_id)
        .map_err(invalid_state_store)?;
    if transaction.get(&downstream).await?.is_some() || transaction.get(&upstream).await?.is_some()
    {
        return Err(conflict_state_store("MV dependency index already exists"));
    }
    let value = encode_record(MvRecordKind::Dependency, operation_id, dependency)
        .map_err(invalid_state_store)?;
    transaction
        .put(downstream, value.clone(), Precondition::Absent)
        .await?;
    transaction.put(upstream, value, Precondition::Absent).await
}

async fn replace_target_index(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    current: &StoredMvDefinition,
    next: &StoredMvDefinition,
) -> Result<(), StateStoreError> {
    let current_key = target_key(&definition_target(current).map_err(invalid_state_store)?)
        .map_err(invalid_state_store)?;
    let next_key = target_key(&definition_target(next).map_err(invalid_state_store)?)
        .map_err(invalid_state_store)?;
    let current_record = transaction
        .get(&current_key)
        .await?
        .ok_or_else(|| invalid_state_store("MV target lookup is missing"))?;
    let lookup: DecodedMvRecord<MvTargetLookup> =
        decode_record(&current_key, &current_record.value).map_err(invalid_state_store)?;
    if lookup.value.mv_id != current.mv_id {
        return Err(invalid_state_store(
            "MV target lookup references a different projection",
        ));
    }
    if current_key == next_key {
        put_target_lookup(
            transaction,
            operation_id,
            next_key,
            next.mv_id,
            Precondition::Version(current_record.version),
        )
        .await
    } else {
        if transaction.get(&next_key).await?.is_some() {
            return Err(conflict_state_store("replacement MV target already exists"));
        }
        transaction
            .delete(current_key, Precondition::Version(current_record.version))
            .await?;
        put_target_lookup(
            transaction,
            operation_id,
            next_key,
            next.mv_id,
            Precondition::Absent,
        )
        .await
    }
}

async fn replace_dependency_indexes(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    mv_id: i64,
    desired: &[CreateMvDependencyRequest],
    page_size: usize,
) -> Result<(), StateStoreError> {
    let records = scan_write_prefix(
        transaction,
        dependency_by_downstream_prefix(mv_id).map_err(invalid_state_store)?,
        page_size,
    )
    .await?;
    let mut current = BTreeMap::new();
    for downstream in records {
        let dependency: DecodedMvRecord<StoredMvDependency> =
            decode_record(&downstream.key, &downstream.value).map_err(invalid_state_store)?;
        let upstream_key = dependency_by_upstream_key(&dependency.value.upstream, mv_id)
            .map_err(invalid_state_store)?;
        let upstream = transaction
            .get(&upstream_key)
            .await?
            .ok_or_else(|| invalid_state_store("MV dependency index is asymmetric"))?;
        current.insert(downstream.key.clone(), (downstream, upstream_key, upstream));
    }
    let desired = dependencies_for_mv(mv_id, desired)
        .into_iter()
        .map(|dependency| {
            let key = dependency_by_downstream_key(mv_id, &dependency.upstream)?;
            Ok((key, dependency))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()
        .map_err(invalid_state_store)?;
    for (key, (downstream, upstream_key, upstream)) in current {
        if let Some(dependency) = desired.get(&key) {
            let value = encode_record(MvRecordKind::Dependency, operation_id, dependency)
                .map_err(invalid_state_store)?;
            transaction
                .put(
                    key,
                    value.clone(),
                    Precondition::Version(downstream.version),
                )
                .await?;
            transaction
                .put(upstream_key, value, Precondition::Version(upstream.version))
                .await?;
        } else {
            transaction
                .delete(key, Precondition::Version(downstream.version))
                .await?;
            transaction
                .delete(upstream_key, Precondition::Version(upstream.version))
                .await?;
        }
    }
    for (key, dependency) in desired {
        if transaction.get(&key).await?.is_none() {
            put_dependency_pair(transaction, operation_id, &dependency).await?;
        }
    }
    Ok(())
}

async fn delete_indexes_for_projection(
    transaction: &mut dyn WriteTransaction,
    definition: &StoredMvDefinition,
    mv_id: i64,
    page_size: usize,
) -> Result<(), StateStoreError> {
    let target_key = target_key(&definition_target(definition).map_err(invalid_state_store)?)
        .map_err(invalid_state_store)?;
    let target = transaction
        .get(&target_key)
        .await?
        .ok_or_else(|| invalid_state_store("MV target lookup is missing"))?;
    let lookup: DecodedMvRecord<MvTargetLookup> =
        decode_record(&target_key, &target.value).map_err(invalid_state_store)?;
    if lookup.value.mv_id != mv_id {
        return Err(invalid_state_store(
            "MV target lookup references a different projection",
        ));
    }
    transaction
        .delete(target_key, Precondition::Version(target.version))
        .await?;
    let downstream = scan_write_prefix(
        transaction,
        dependency_by_downstream_prefix(mv_id).map_err(invalid_state_store)?,
        page_size,
    )
    .await?;
    for record in downstream {
        let dependency: DecodedMvRecord<StoredMvDependency> =
            decode_record(&record.key, &record.value).map_err(invalid_state_store)?;
        let upstream_key = dependency_by_upstream_key(&dependency.value.upstream, mv_id)
            .map_err(invalid_state_store)?;
        let upstream = transaction
            .get(&upstream_key)
            .await?
            .ok_or_else(|| invalid_state_store("MV dependency index is asymmetric"))?;
        transaction
            .delete(record.key, Precondition::Version(record.version))
            .await?;
        transaction
            .delete(upstream_key, Precondition::Version(upstream.version))
            .await?;
    }
    Ok(())
}

fn decode_dependencies(
    records: Vec<StateRecord>,
) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
    let mut dependencies = records
        .into_iter()
        .map(|record| {
            decode_record::<StoredMvDependency>(&record.key, &record.value)
                .map(|decoded| decoded.value)
                .map_err(corruption)
        })
        .collect::<Result<Vec<_>, _>>()?;
    dependencies.sort_by(|left, right| {
        left.upstream
            .display_name()
            .cmp(&right.upstream.display_name())
    });
    Ok(dependencies)
}

fn invalid(message: impl Into<String>) -> MvRepositoryError {
    MvRepositoryError::new(MvRepositoryErrorKind::InvalidRequest, message)
}

fn corruption(message: impl Into<String>) -> MvRepositoryError {
    MvRepositoryError::new(MvRepositoryErrorKind::Corruption, message)
}

fn invalid_state_store(_message: impl Into<String>) -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "MV Accelerator StateStore key or record is invalid",
    )
}

fn conflict_state_store(_message: impl Into<String>) -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::PreconditionFailed,
        "MV Accelerator StateStore transaction precondition failed",
    )
}
