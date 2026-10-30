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
pub mod keyed;
mod operation;

#[cfg(test)]
#[path = "tests_definition.rs"]
mod tests_definition;
#[cfg(test)]
#[path = "tests_dependency.rs"]
mod tests_dependency;
#[cfg(test)]
mod tests_inventory;
#[cfg(test)]
#[path = "tests_port.rs"]
mod tests_port;

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_state_store_api::{
    Direction, Key, KeyRange, Precondition, RangeRequest, ReadTransaction, StateRecord, StateStore,
    StateStoreError, StateStoreErrorKind, WriteTransaction,
};
use uuid::Uuid;

use crate::dependency::MvDependencyObjectRef;
use crate::persistence::dependency::StoredMvDependency;
use crate::product::MvTarget;
use crate::repository::{
    MvDependencyReadBound, MvProjectionInventoryBound, MvProjectionInventoryBuilder,
    MvProjectionInventoryEntry, MvRepositoryError, MvRepositoryErrorKind, MvTargetLookup,
};
use crate::repository_metrics::MvRepositoryMetrics;
use novarocks_state_store_runtime::StateStoreRunPolicy;

use self::codec::{
    DecodedMvRecord, MvRecordKind, MvSequence, decode_dependency_with_budget, decode_record,
    encode_record,
};
use self::key::{
    dependency_by_downstream_key, dependency_by_downstream_prefix,
    dependency_by_upstream_catalog_prefixes, dependency_by_upstream_key,
    dependency_by_upstream_prefix, projection_by_id_key, projection_prefix, sequence_key,
    target_lookup_catalog_prefix, target_lookup_key,
};

/// StateStore owner for the single current MV Accelerator family.
pub struct StateStoreMvRepository {
    store: Arc<dyn StateStore>,
    run_policy: StateStoreRunPolicy,
    runner_metrics: MvRepositoryMetrics,
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
            runner_metrics: MvRepositoryMetrics::default(),
            store,
            run_policy,
        }))
    }
}

async fn scan_transaction_range<T: ReadAccess + ?Sized>(
    transaction: &mut T,
    range: KeyRange,
    page_size: usize,
) -> Result<Vec<StateRecord>, T::Error> {
    let mut continuation = None;
    let mut records = Vec::new();
    loop {
        let page = transaction
            .read_range(&RangeRequest {
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

/// How the current Accelerator names a catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvCatalogReference {
    /// A current MV writes its projection into this catalog.
    Target,
    /// A current MV reads an upstream object out of this catalog.
    UpstreamDependency,
}

impl MvCatalogReference {
    pub const fn describe(self) -> &'static str {
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
pub async fn observe_catalog_references(
    store: &dyn StateStore,
    catalog: &str,
    page_size: usize,
) -> Result<Option<MvCatalogReference>, StateStoreError> {
    observe_catalog_references_with(&LegacyCatalogRead { store, page_size }, catalog).await
}

/// Borrow the exact installed execution owner. Unsupported original providers
/// refuse; this never dispatches through the legacy transaction API.
pub async fn observe_catalog_references_in(
    store: &dyn StateStore,
    catalog: &str,
    page_size: usize,
    execution: &novarocks_state_store_api::StateStoreExecutionContext,
) -> Result<Option<MvCatalogReference>, MvRepositoryError> {
    observe_catalog_references_with(
        &OriginalCatalogRead {
            store,
            page_size,
            execution,
        },
        catalog,
    )
    .await
}

#[async_trait::async_trait]
trait CatalogRead: Sync {
    type Error: Send;
    fn invalid_prefix(message: String) -> Self::Error;
    async fn scan(&self, prefix: Key) -> Result<Vec<StateRecord>, Self::Error>;
}
struct LegacyCatalogRead<'a> {
    store: &'a dyn StateStore,
    page_size: usize,
}
#[async_trait::async_trait]
impl CatalogRead for LegacyCatalogRead<'_> {
    type Error = StateStoreError;
    fn invalid_prefix(message: String) -> Self::Error {
        invalid_state_store(message)
    }
    async fn scan(&self, prefix: Key) -> Result<Vec<StateRecord>, Self::Error> {
        scan_read_prefix(self.store, prefix, self.page_size).await
    }
}
struct OriginalCatalogRead<'a> {
    store: &'a dyn StateStore,
    page_size: usize,
    execution: &'a novarocks_state_store_api::StateStoreExecutionContext,
}
#[async_trait::async_trait]
impl CatalogRead for OriginalCatalogRead<'_> {
    type Error = MvRepositoryError;
    fn invalid_prefix(message: String) -> Self::Error {
        operation::state_store_error(invalid_state_store(message))
    }
    async fn scan(&self, prefix: Key) -> Result<Vec<StateRecord>, Self::Error> {
        let range = KeyRange::for_prefix(prefix).map_err(operation::state_store_error)?;
        let mut transaction = ReadSession::Original(
            self.store
                .begin_original_read_in(self.execution)
                .await
                .map_err(operation::original_failure)?,
        );
        let result = scan_transaction_range(&mut transaction, range, self.page_size).await;
        let close = transaction.read_close().await;
        settle_read(result, close)
    }
}
async fn observe_catalog_references_with<T: CatalogRead>(
    read: &T,
    catalog: &str,
) -> Result<Option<MvCatalogReference>, T::Error> {
    if !read
        .scan(target_lookup_catalog_prefix(catalog).map_err(T::invalid_prefix)?)
        .await?
        .is_empty()
    {
        return Ok(Some(MvCatalogReference::Target));
    }
    for prefix in dependency_by_upstream_catalog_prefixes(catalog).map_err(T::invalid_prefix)? {
        if !read.scan(prefix).await?.is_empty() {
            return Ok(Some(MvCatalogReference::UpstreamDependency));
        }
    }
    Ok(None)
}

/// One record per provider call, one continuation, no retained raw stream.
struct BoundedRecordRange {
    range: KeyRange,
    continuation: Option<novarocks_state_store_api::ContinuationToken>,
    previous: Option<Key>,
    exhausted: bool,
}

impl BoundedRecordRange {
    fn new(prefix: Key) -> Result<Self, MvRepositoryError> {
        Ok(Self {
            range: KeyRange::for_prefix(prefix).map_err(operation::state_store_error)?,
            continuation: None,
            previous: None,
            exhausted: false,
        })
    }

    async fn next<T: ReadAccess + ?Sized>(
        &mut self,
        transaction: &mut T,
        bound: MvProjectionInventoryBound,
    ) -> Result<Option<StateRecord>, MvRepositoryError> {
        if self.exhausted {
            return Ok(None);
        }
        let request = RangeRequest {
            range: self.range.clone(),
            direction: Direction::Forward,
            page_size: 1,
            continuation: self.continuation.clone(),
        };
        let page = transaction.read_range(&request).await.map_err(T::project)?;
        if page.records.len() > 1 || (page.records.is_empty() && page.continuation.is_some()) {
            return Err(corruption(
                "MV inventory provider violated the exact page bound",
            ));
        }
        if page
            .continuation
            .as_ref()
            .is_some_and(|token| token.as_bytes().len() > bound.continuation_token_bytes)
        {
            return Err(invalid(
                "MV inventory exceeds its continuation token byte bound",
            ));
        }
        if page.continuation.is_some() && page.continuation == self.continuation {
            return Err(corruption("MV inventory continuation made no progress"));
        }
        let cursor_bytes = self
            .previous
            .as_ref()
            .map_or(0, |key| key.as_bytes().len())
            .saturating_add(
                self.continuation
                    .as_ref()
                    .map_or(0, |token| token.as_bytes().len()),
            );
        let Some(record) = page.records.into_iter().next() else {
            self.exhausted = true;
            return Ok(None);
        };
        // Account old cursor, provider page and exact cursor copies together.
        let raw_peak = record
            .value
            .as_bytes()
            .len()
            .saturating_add(record.key.as_bytes().len().saturating_mul(2))
            .saturating_add(
                page.continuation
                    .as_ref()
                    .map_or(0, |token| token.as_bytes().len())
                    .saturating_mul(2),
            )
            .saturating_add(cursor_bytes);
        if raw_peak > bound.raw_page_bytes {
            return Err(invalid("MV inventory exceeds its raw page byte bound"));
        }
        if let Some(token) = &page.continuation
            && token
                .resume_after(&request)
                .map_err(operation::state_store_error)?
                != record.key
        {
            return Err(corruption(
                "MV inventory continuation differs from its last page key",
            ));
        }
        self.exhausted = page.continuation.is_none();
        self.continuation = page
            .continuation
            .map(|token| {
                novarocks_state_store_api::ContinuationToken::try_from(
                    bytes::Bytes::copy_from_slice(token.as_bytes()),
                )
            })
            .transpose()
            .map_err(operation::state_store_error)?;
        check_inventory_raw_record(&record, bound)?;
        if record.key < self.range.start
            || record.key >= self.range.end
            || self
                .previous
                .as_ref()
                .is_some_and(|previous| previous >= &record.key)
        {
            return Err(corruption(
                "MV inventory page key is out of range or made no progress",
            ));
        }
        self.previous = Some(
            Key::try_from(bytes::Bytes::copy_from_slice(record.key.as_bytes()))
                .map_err(operation::state_store_error)?,
        );
        Ok(Some(record))
    }
}

fn check_inventory_raw_record(
    record: &StateRecord,
    bound: MvProjectionInventoryBound,
) -> Result<(), MvRepositoryError> {
    if record
        .key
        .as_bytes()
        .len()
        .checked_add(record.value.as_bytes().len())
        .is_none_or(|bytes| bytes > bound.raw_page_bytes)
    {
        return Err(MvRepositoryError::new(
            MvRepositoryErrorKind::InvalidRequest,
            "MV inventory exceeds its raw page byte bound",
        ));
    }
    Ok(())
}

fn target_key(target: &MvTarget) -> Result<Key, String> {
    target_lookup_key(
        target.catalog().unwrap_or_default(),
        target.namespace(),
        target.name(),
    )
}

fn dependencies_for_mv(mv_id: i64, requests: &[StoredMvDependency]) -> Vec<StoredMvDependency> {
    requests
        .iter()
        .cloned()
        .map(|mut dependency| {
            dependency.downstream_mv_id = mv_id;
            dependency
        })
        .collect()
}

async fn put_sequence<T: WriteAccess + ?Sized>(
    transaction: &mut T,
    operation_id: Uuid,
    sequence: MvSequence,
    precondition: Precondition,
) -> Result<(), T::Error> {
    let key = sequence_key().map_err(|error| T::from_store(invalid_state_store(error)))?;
    let value = encode_record(MvRecordKind::Sequence, operation_id, &sequence)
        .map_err(|error| T::from_store(invalid_state_store(error)))?;
    transaction.write_put(key, value, precondition).await
}

async fn put_target_lookup<T: WriteAccess + ?Sized>(
    transaction: &mut T,
    operation_id: Uuid,
    key: Key,
    mv_id: i64,
    precondition: Precondition,
) -> Result<(), T::Error> {
    let value = encode_record(
        MvRecordKind::TargetLookup,
        operation_id,
        &MvTargetLookup { mv_id },
    )
    .map_err(|error| T::from_store(invalid_state_store(error)))?;
    transaction.write_put(key, value, precondition).await
}

async fn put_dependency_pair<T: WriteAccess + ?Sized>(
    transaction: &mut T,
    operation_id: Uuid,
    dependency: &StoredMvDependency,
) -> Result<(), T::Error> {
    let downstream = dependency_by_downstream_key(
        dependency.downstream_mv_id,
        &dependency.upstream,
        dependency.occurrence_id,
    )
    .map_err(|error| T::from_store(invalid_state_store(error)))?;
    let upstream = dependency_by_upstream_key(
        &dependency.upstream,
        dependency.downstream_mv_id,
        dependency.occurrence_id,
    )
    .map_err(|error| T::from_store(invalid_state_store(error)))?;
    if transaction.read_get(&downstream).await?.is_some()
        || transaction.read_get(&upstream).await?.is_some()
    {
        return Err(T::from_store(conflict_state_store(
            "MV dependency index already exists",
        )));
    }
    let value = encode_record(MvRecordKind::Dependency, operation_id, dependency)
        .map_err(|error| T::from_store(invalid_state_store(error)))?;
    transaction
        .write_put(downstream, value.clone(), Precondition::Absent)
        .await?;
    transaction
        .write_put(upstream, value, Precondition::Absent)
        .await
}

// A keyed root declares the complete occurrence vocabulary for its indexes.
// The caller authenticates that root's version in this same transaction.
// Point reads preserve the write protocol: a continuation freezes mutations.
async fn dependency_pairs_for_projection<T: WriteAccess + ?Sized>(
    transaction: &mut T,
    mv_id: i64,
    declared: &[StoredMvDependency],
) -> Result<BTreeMap<Key, (StateRecord, Key, StateRecord)>, T::Error> {
    let mut current = BTreeMap::new();
    for expected in declared {
        if expected.downstream_mv_id != mv_id {
            return Err(T::from_store(invalid_state_store(
                "MV dependency belongs to another projection",
            )));
        }
        let downstream_key =
            dependency_by_downstream_key(mv_id, &expected.upstream, expected.occurrence_id)
                .map_err(|error| T::from_store(invalid_state_store(error)))?;
        let upstream_key =
            dependency_by_upstream_key(&expected.upstream, mv_id, expected.occurrence_id)
                .map_err(|error| T::from_store(invalid_state_store(error)))?;
        let downstream = transaction
            .read_get(&downstream_key)
            .await?
            .ok_or_else(|| {
                T::from_store(invalid_state_store("MV dependency index is asymmetric"))
            })?;
        let upstream = transaction.read_get(&upstream_key).await?.ok_or_else(|| {
            T::from_store(invalid_state_store("MV dependency index is asymmetric"))
        })?;
        let downstream_value: DecodedMvRecord<StoredMvDependency> =
            decode_record(&downstream.key, &downstream.value)
                .map_err(|error| T::from_store(invalid_state_store(error)))?;
        let upstream_value: DecodedMvRecord<StoredMvDependency> =
            decode_record(&upstream.key, &upstream.value)
                .map_err(|error| T::from_store(invalid_state_store(error)))?;
        if &downstream_value.value != expected || &upstream_value.value != expected {
            return Err(T::from_store(invalid_state_store(
                "MV dependency index differs from its projection",
            )));
        }
        if current
            .insert(downstream_key, (downstream, upstream_key, upstream))
            .is_some()
        {
            return Err(T::from_store(invalid_state_store(
                "MV dependency occurrence is duplicated",
            )));
        }
    }
    Ok(current)
}

async fn replace_dependency_indexes<T: WriteAccess + ?Sized>(
    transaction: &mut T,
    operation_id: Uuid,
    mv_id: i64,
    previous: &[StoredMvDependency],
    desired: &[StoredMvDependency],
) -> Result<(), T::Error> {
    let current = dependency_pairs_for_projection(transaction, mv_id, previous).await?;
    let desired = dependencies_for_mv(mv_id, desired)
        .into_iter()
        .map(|dependency| {
            let key = dependency_by_downstream_key(
                mv_id,
                &dependency.upstream,
                dependency.occurrence_id,
            )?;
            Ok((key, dependency))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()
        .map_err(|error| T::from_store(invalid_state_store(error)))?;
    for (key, (downstream, upstream_key, upstream)) in current {
        if let Some(dependency) = desired.get(&key) {
            let value = encode_record(MvRecordKind::Dependency, operation_id, dependency)
                .map_err(|error| T::from_store(invalid_state_store(error)))?;
            transaction
                .write_put(
                    key,
                    value.clone(),
                    Precondition::Version(downstream.version),
                )
                .await?;
            transaction
                .write_put(upstream_key, value, Precondition::Version(upstream.version))
                .await?;
        } else {
            transaction
                .write_delete(key, Precondition::Version(downstream.version))
                .await?;
            transaction
                .write_delete(upstream_key, Precondition::Version(upstream.version))
                .await?;
        }
    }
    for (key, dependency) in desired {
        if transaction.read_get(&key).await?.is_none() {
            put_dependency_pair(transaction, operation_id, &dependency).await?;
        }
    }
    Ok(())
}

async fn delete_indexes_for_target<T: WriteAccess + ?Sized>(
    transaction: &mut T,
    projection: &crate::persistence::keyed_documents::StoredKeyedMvProjection,
) -> Result<(), T::Error> {
    let mv_id = projection.mv_id;
    let target_key = target_key(&projection.facts.target())
        .map_err(|error| T::from_store(invalid_state_store(error)))?;
    let target = transaction
        .read_get(&target_key)
        .await?
        .ok_or_else(|| T::from_store(invalid_state_store("MV target lookup is missing")))?;
    let lookup: DecodedMvRecord<MvTargetLookup> = decode_record(&target_key, &target.value)
        .map_err(|error| T::from_store(invalid_state_store(error)))?;
    if lookup.value.mv_id != mv_id {
        return Err(T::from_store(invalid_state_store(
            "MV target lookup references a different projection",
        )));
    }
    let declared = keyed::keyed_dependencies(mv_id, &projection.facts);
    let current = dependency_pairs_for_projection(transaction, mv_id, &declared).await?;
    transaction
        .write_delete(target_key, Precondition::Version(target.version))
        .await?;
    for (key, (downstream, upstream_key, upstream)) in current {
        transaction
            .write_delete(key, Precondition::Version(downstream.version))
            .await?;
        transaction
            .write_delete(upstream_key, Precondition::Version(upstream.version))
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
            let dependency = decode_record::<StoredMvDependency>(&record.key, &record.value)
                .map_err(corruption)?
                .value;
            let downstream_key = dependency_by_downstream_key(
                dependency.downstream_mv_id,
                &dependency.upstream,
                dependency.occurrence_id,
            )
            .map_err(corruption)?;
            let upstream_key = dependency_by_upstream_key(
                &dependency.upstream,
                dependency.downstream_mv_id,
                dependency.occurrence_id,
            )
            .map_err(corruption)?;
            if record.key != downstream_key && record.key != upstream_key {
                return Err(corruption(
                    "MV dependency key differs from its canonical occurrence",
                ));
            }
            Ok(dependency)
        })
        .collect::<Result<Vec<_>, _>>()?;
    dependencies.sort_by(|left, right| {
        left.upstream
            .display_name()
            .cmp(&right.upstream.display_name())
            .then(left.occurrence_id.cmp(&right.occurrence_id))
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

impl StateStoreMvRepository {
    pub async fn list_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.read_repository()
            .list_downstream_dependencies(upstream)
            .await
    }

    pub async fn ensure_no_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        self.read_repository()
            .ensure_no_downstream_dependencies(upstream)
            .await
    }
}
impl StateStoreMvRepository {
    #[cfg(any(test, novarocks_dev_controls))]
    pub async fn wipe_accelerator(&self, operation_id: Uuid) -> Result<(), MvRepositoryError> {
        self.write_repository().wipe_accelerator(operation_id).await
    }
}

#[cfg(any(test, novarocks_dev_controls))]
use self::key::accelerator_prefix;

/// A request-local borrower. The caller retains the original physical owner
/// through actual exit; this view neither mints nor owns that capability.
pub struct StateStoreMvRepositoryExecution<'a> {
    repository: &'a StateStoreMvRepository,
    execution: &'a dyn novarocks_state_store_api::StateStoreExecutionRequests,
}
impl StateStoreMvRepository {
    pub fn in_execution<'a>(
        &'a self,
        execution: &'a novarocks_state_store_api::StateStoreExecutionContext,
    ) -> StateStoreMvRepositoryExecution<'a> {
        self.in_requests(execution)
    }
    pub fn in_requests<'a>(
        &'a self,
        execution: &'a dyn novarocks_state_store_api::StateStoreExecutionRequests,
    ) -> StateStoreMvRepositoryExecution<'a> {
        StateStoreMvRepositoryExecution {
            repository: self,
            execution,
        }
    }
    fn read_repository(&self) -> ReadRepository<'_> {
        ReadRepository {
            store: self.store.as_ref(),
            execution: None,
        }
    }
}
impl StateStoreMvRepositoryExecution<'_> {
    fn read_repository(&self) -> ReadRepository<'_> {
        ReadRepository {
            store: self.repository.store.as_ref(),
            execution: Some(self.execution),
        }
    }
    pub async fn list_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.read_repository()
            .list_downstream_dependencies(upstream)
            .await
    }
    pub async fn ensure_no_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        self.read_repository()
            .ensure_no_downstream_dependencies(upstream)
            .await
    }
    pub async fn find_keyed_by_target(
        &self,
        target: &MvTarget,
        budget: crate::persistence::validation::PersistenceDecodeBudget,
    ) -> Result<Option<keyed::LoadedKeyedMvProjection>, MvRepositoryError> {
        self.read_repository()
            .find_keyed_by_target(target, budget)
            .await
    }
    pub async fn list_keyed_projections(
        &self,
    ) -> Result<Vec<keyed::LoadedKeyedMvProjection>, MvRepositoryError> {
        self.read_repository().list_keyed_projections().await
    }
    pub async fn list_keyed_projection_inventory(
        &self,
        bound: MvProjectionInventoryBound,
    ) -> Result<Vec<MvProjectionInventoryEntry>, MvRepositoryError> {
        self.read_repository()
            .list_keyed_projection_inventory(bound)
            .await
    }
    pub async fn find_keyed_by_target_bounded(
        &self,
        target: &MvTarget,
        bound: MvProjectionInventoryBound,
    ) -> Result<Option<keyed::LoadedKeyedMvProjection>, MvRepositoryError> {
        self.read_repository()
            .find_keyed_by_target_bounded(target, bound)
            .await
    }
    pub async fn list_keyed_dependencies_by_downstream_bounded(
        &self,
        loaded: &keyed::LoadedKeyedMvProjection,
        bound: MvDependencyReadBound,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.read_repository()
            .list_keyed_dependencies_by_downstream_bounded(loaded, bound)
            .await
    }
    pub async fn find_keyed_by_target_object(
        &self,
        object: &novarocks_spi::connector::ConnectorTableObjectId,
    ) -> Result<Option<keyed::LoadedKeyedMvProjection>, MvRepositoryError> {
        self.read_repository()
            .find_keyed_by_target_object(object)
            .await
    }
    pub async fn list_keyed_dependencies_by_downstream(
        &self,
        loaded: &keyed::LoadedKeyedMvProjection,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.read_repository()
            .list_keyed_dependencies_by_downstream(loaded)
            .await
    }
}
struct ReadRepository<'a> {
    store: &'a dyn StateStore,
    execution: Option<&'a dyn novarocks_state_store_api::StateStoreExecutionRequests>,
}
impl ReadRepository<'_> {
    async fn begin_read(&self) -> Result<ReadSession, MvRepositoryError> {
        match self.execution {
            Some(requests) => {
                let execution = requests
                    .read_transaction_context()
                    .map_err(operation::original_failure)?;
                self.store
                    .begin_original_read_in(&execution)
                    .await
                    .map(ReadSession::Original)
                    .map_err(operation::original_failure)
            }
            None => self
                .store
                .begin_read()
                .await
                .map(ReadSession::Legacy)
                .map_err(operation::state_store_error),
        }
    }
    async fn scan_prefix(&self, prefix: Key) -> Result<Vec<StateRecord>, MvRepositoryError> {
        let range = KeyRange::for_prefix(prefix).map_err(operation::state_store_error)?;
        let mut transaction = self.begin_read().await?;
        let result =
            scan_transaction_range(&mut transaction, range, self.store.limits().max_page_size)
                .await;
        let close = transaction.read_close().await;
        settle_read(result, close)
    }
    async fn classified_dependencies(
        &self,
        prefix: Key,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        let mut transaction = self.begin_read().await?;
        let result = async {
            let page_size = self.store.limits().max_page_size;
            let rows = scan_transaction_range(
                &mut transaction,
                KeyRange::for_prefix(prefix.clone()).map_err(operation::state_store_error)?,
                page_size,
            )
            .await?;
            let roots = scan_transaction_range(
                &mut transaction,
                KeyRange::for_prefix(projection_prefix().map_err(corruption)?)
                    .map_err(operation::state_store_error)?,
                page_size,
            )
            .await?;
            // A failed or partial inventory is not evidence that an occurrence is external.
            let inventory = roots
                .into_iter()
                .map(|record| {
                    codec::decode_keyed_projection(
                        &record.key,
                        &record.value,
                        crate::persistence::validation::PersistenceDecodeBudget::default(),
                    )
                    .map(|decoded| decoded.value)
                    .map_err(corruption)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut dependencies = decode_dependencies(rows)?;
            let mut expected = Vec::new();
            for root in &inventory {
                for dependency in keyed::keyed_dependencies(root.mv_id, &root.facts) {
                    let downstream_key = dependency_by_downstream_key(
                        root.mv_id,
                        &dependency.upstream,
                        dependency.occurrence_id,
                    )
                    .map_err(corruption)?;
                    let upstream_key = dependency_by_upstream_key(
                        &dependency.upstream,
                        root.mv_id,
                        dependency.occurrence_id,
                    )
                    .map_err(corruption)?;
                    if downstream_key.as_bytes().starts_with(prefix.as_bytes())
                        || upstream_key.as_bytes().starts_with(prefix.as_bytes())
                    {
                        expected.push(dependency);
                    }
                }
            }
            if dependencies.len() != expected.len()
                || dependencies
                    .iter()
                    .any(|dependency| !expected.contains(dependency))
            {
                return Err(corruption(
                    "MV dependency index is incomplete or differs from its canonical occurrences",
                ));
            }
            let inventory = inventory
                .into_iter()
                .map(|root| MvProjectionInventoryEntry {
                    mv_id: root.mv_id,
                    target: root.facts.target(),
                    object_id: root.facts.source_revision().target_object_id.clone(),
                })
                .collect::<Vec<_>>();
            crate::bounded_dependencies::classify(&mut dependencies, &inventory);
            Ok(dependencies)
        }
        .await;
        let close = transaction.read_close().await;
        settle_read(result, close)
    }
    async fn read_record(&self, key: &Key) -> Result<Option<StateRecord>, MvRepositoryError> {
        let mut transaction = self.begin_read().await?;
        let result = transaction.read_get(key).await;
        let close = transaction.read_close().await;
        settle_read(result, close)
    }
    pub async fn list_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.classified_dependencies(dependency_by_upstream_prefix(upstream).map_err(corruption)?)
            .await
    }
    pub async fn ensure_no_downstream_dependencies(
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
    pub async fn find_keyed_by_target(
        &self,
        target: &MvTarget,
        budget: crate::persistence::validation::PersistenceDecodeBudget,
    ) -> Result<Option<keyed::LoadedKeyedMvProjection>, MvRepositoryError> {
        let key = target_key(target).map_err(corruption)?;
        let Some(lookup) = self.read_record(&key).await? else {
            return Ok(None);
        };
        let lookup: DecodedMvRecord<MvTargetLookup> =
            decode_record(&key, &lookup.value).map_err(corruption)?;
        let key = projection_by_id_key(lookup.value.mv_id).map_err(corruption)?;
        let row = self
            .read_record(&key)
            .await?
            .ok_or_else(|| corruption("keyed MV lookup has no projection"))?;
        let decoded =
            codec::decode_keyed_projection(&key, &row.value, budget).map_err(corruption)?;
        if decoded.value.facts.target() != *target {
            return Err(corruption("keyed MV lookup disagrees with its projection"));
        }
        Ok(Some(keyed::LoadedKeyedMvProjection {
            projection: decoded.value,
            version: crate::repository::MvProjectionVersion::from_store(row.version),
        }))
    }
    pub async fn list_keyed_projections(
        &self,
    ) -> Result<Vec<keyed::LoadedKeyedMvProjection>, MvRepositoryError> {
        let mut rows = Vec::new();
        for row in self
            .scan_prefix(projection_prefix().map_err(corruption)?)
            .await?
        {
            let value = codec::decode_keyed_projection(
                &row.key,
                &row.value,
                crate::persistence::validation::PersistenceDecodeBudget::default(),
            )
            .map_err(corruption)?
            .value;
            if row.key != projection_by_id_key(value.mv_id).map_err(corruption)? {
                return Err(corruption(
                    "keyed projection key differs from root identity",
                ));
            }
            rows.push(keyed::LoadedKeyedMvProjection {
                projection: value,
                version: crate::repository::MvProjectionVersion::from_store(row.version),
            });
        }
        Ok(rows)
    }
    pub async fn list_keyed_projection_inventory(
        &self,
        bound: MvProjectionInventoryBound,
    ) -> Result<Vec<MvProjectionInventoryEntry>, MvRepositoryError> {
        let mut tx = self.begin_read().await?;
        let result = keyed::bounded_keyed_inventory(&mut tx, bound).await;
        let close = tx.read_close().await;
        match (result, close) {
            (Err(e), _) => Err(e),
            (_, Err(e)) => Err(e),
            (Ok(v), Ok(())) => Ok(v),
        }
    }
    pub async fn find_keyed_by_target_bounded(
        &self,
        target: &MvTarget,
        bound: MvProjectionInventoryBound,
    ) -> Result<Option<keyed::LoadedKeyedMvProjection>, MvRepositoryError> {
        MvProjectionInventoryBuilder::new(bound)?;
        let index = target_key(target).map_err(corruption)?;
        let Some(row) = self.read_record(&index).await? else {
            return Ok(None);
        };
        check_inventory_raw_record(&row, bound)?;
        let lookup: DecodedMvRecord<MvTargetLookup> =
            decode_record(&index, &row.value).map_err(corruption)?;
        let root = projection_by_id_key(lookup.value.mv_id).map_err(corruption)?;
        let row = self
            .read_record(&root)
            .await?
            .ok_or_else(|| corruption("keyed target lookup has no root"))?;
        check_inventory_raw_record(&row, bound)?;
        let facts = codec::decode_keyed_projection(&root, &row.value, bound.decode)
            .map_err(corruption)?
            .value;
        if facts.facts.target() != *target {
            return Err(corruption("keyed lookup disagrees with root"));
        }
        Ok(Some(keyed::LoadedKeyedMvProjection {
            projection: facts,
            version: crate::repository::MvProjectionVersion::from_store(row.version),
        }))
    }
    pub async fn list_keyed_dependencies_by_downstream_bounded(
        &self,
        loaded: &keyed::LoadedKeyedMvProjection,
        bound: MvDependencyReadBound,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        use crate::bounded_dependencies::{
            DependencyCollector, classify, validate_canonical_parts,
        };
        let mut tx = self.begin_read().await?;
        let result = async {
            let root = projection_by_id_key(loaded.projection.mv_id).map_err(corruption)?;
            let row = tx
                .read_get(&root)
                .await?
                .ok_or_else(|| corruption("keyed dependency root disappeared"))?;
            if row.version != *loaded.version.store_version() {
                return Err(MvRepositoryError::new(
                    MvRepositoryErrorKind::Conflict,
                    "keyed dependency root changed",
                ));
            }
            check_inventory_raw_record(&row, bound.inventory)?;
            let projection =
                codec::decode_keyed_projection(&root, &row.value, bound.inventory.decode)
                    .map_err(corruption)?
                    .value;
            drop(row);
            let mut collector = DependencyCollector::new(bound)?;
            let mut range = BoundedRecordRange::new(
                dependency_by_downstream_prefix(projection.mv_id).map_err(corruption)?,
            )?;
            while let Some(row) = range.next(&mut tx, bound.inventory).await? {
                let dependency = decode_dependency_with_budget(
                    &row.key,
                    &row.value,
                    bound.inventory.decode.max_working_set_bytes,
                )
                .map_err(corruption)?
                .value;
                if row.key
                    != dependency_by_downstream_key(
                        dependency.downstream_mv_id,
                        &dependency.upstream,
                        dependency.occurrence_id,
                    )
                    .map_err(corruption)?
                {
                    return Err(corruption(
                        "keyed dependency index key differs from occurrence",
                    ));
                }
                collector.push(dependency)?;
            }
            let mut dependencies = collector.finish();
            validate_canonical_parts(
                projection.mv_id,
                projection.facts.definition(),
                &dependencies,
            )?;
            drop(projection);
            let inventory = keyed::bounded_keyed_inventory(&mut tx, bound.inventory).await?;
            classify(&mut dependencies, &inventory);
            Ok(dependencies)
        }
        .await;
        let close = tx.read_close().await;
        match (result, close) {
            (Err(e), _) => Err(e),
            (_, Err(e)) => Err(e),
            (Ok(v), Ok(())) => Ok(v),
        }
    }
    pub async fn find_keyed_by_target_object(
        &self,
        object: &novarocks_spi::connector::ConnectorTableObjectId,
    ) -> Result<Option<keyed::LoadedKeyedMvProjection>, MvRepositoryError> {
        let mut found = None;
        for row in self.list_keyed_projections().await? {
            if &row.projection.facts.source_revision().target_object_id == object {
                if found.is_some() {
                    return Err(corruption(
                        "target object has more than one keyed projection",
                    ));
                }
                found = Some(row);
            }
        }
        Ok(found)
    }
    pub async fn list_keyed_dependencies_by_downstream(
        &self,
        loaded: &keyed::LoadedKeyedMvProjection,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        let current = self
            .find_keyed_by_target(
                &loaded.projection.facts.target(),
                crate::persistence::validation::PersistenceDecodeBudget::default(),
            )
            .await?;
        if current.as_ref().map(|v| &v.version) != Some(&loaded.version) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                "keyed dependency root changed",
            ));
        }
        let mut dependencies = Vec::new();
        for row in self
            .scan_prefix(
                dependency_by_downstream_prefix(loaded.projection.mv_id).map_err(corruption)?,
            )
            .await?
        {
            let dependency: DecodedMvRecord<StoredMvDependency> =
                decode_record(&row.key, &row.value).map_err(corruption)?;
            if row.key
                != dependency_by_downstream_key(
                    dependency.value.downstream_mv_id,
                    &dependency.value.upstream,
                    dependency.value.occurrence_id,
                )
                .map_err(corruption)?
            {
                return Err(corruption(
                    "keyed dependency key differs from canonical occurrence",
                ));
            }
            dependencies.push(dependency.value);
        }
        crate::bounded_dependencies::validate_canonical_parts(
            loaded.projection.mv_id,
            loaded.projection.facts.definition(),
            &dependencies,
        )?;
        let inventory = self
            .list_keyed_projections()
            .await?
            .into_iter()
            .map(|p| MvProjectionInventoryEntry {
                mv_id: p.projection.mv_id,
                target: p.projection.facts.target(),
                object_id: p
                    .projection
                    .facts
                    .source_revision()
                    .target_object_id
                    .clone(),
            })
            .collect::<Vec<_>>();
        crate::bounded_dependencies::classify(&mut dependencies, &inventory);
        Ok(dependencies)
    }
}

fn settle_read<T>(
    result: Result<T, MvRepositoryError>,
    close: Result<(), MvRepositoryError>,
) -> Result<T, MvRepositoryError> {
    match (result, close) {
        (Err(error), _) => Err(error),
        (_, Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

#[async_trait::async_trait]
trait ReadAccess: Send {
    type Error: Send;
    fn project(error: Self::Error) -> MvRepositoryError;
    async fn read_get(&mut self, key: &Key) -> Result<Option<StateRecord>, Self::Error>;
    async fn read_range(
        &mut self,
        request: &RangeRequest,
    ) -> Result<novarocks_state_store_api::RangePage, Self::Error>;
}
#[async_trait::async_trait]
impl<T: ReadTransaction + ?Sized> ReadAccess for T {
    type Error = StateStoreError;
    fn project(error: Self::Error) -> MvRepositoryError {
        operation::state_store_error(error)
    }
    async fn read_get(&mut self, key: &Key) -> Result<Option<StateRecord>, Self::Error> {
        self.get(key).await
    }
    async fn read_range(
        &mut self,
        request: &RangeRequest,
    ) -> Result<novarocks_state_store_api::RangePage, Self::Error> {
        self.range(request).await
    }
}
enum ReadSession {
    Legacy(Box<dyn ReadTransaction>),
    Original(Box<dyn novarocks_state_store_api::OriginalReadTransaction>),
}
impl ReadSession {
    async fn read_close(self) -> Result<(), MvRepositoryError> {
        match self {
            Self::Legacy(transaction) => transaction
                .abort()
                .await
                .map_err(operation::state_store_error),
            Self::Original(mut transaction) => transaction
                .close_in()
                .await
                .map_err(operation::original_failure),
        }
    }
}
#[async_trait::async_trait]
impl ReadAccess for ReadSession {
    type Error = MvRepositoryError;
    fn project(error: Self::Error) -> MvRepositoryError {
        error
    }
    async fn read_get(&mut self, key: &Key) -> Result<Option<StateRecord>, Self::Error> {
        match self {
            Self::Legacy(transaction) => transaction
                .get(key)
                .await
                .map_err(operation::state_store_error),
            Self::Original(transaction) => transaction
                .get_in(key)
                .await
                .map_err(operation::original_failure),
        }
    }
    async fn read_range(
        &mut self,
        request: &RangeRequest,
    ) -> Result<novarocks_state_store_api::RangePage, Self::Error> {
        match self {
            Self::Legacy(transaction) => transaction
                .range(request)
                .await
                .map_err(operation::state_store_error),
            Self::Original(transaction) => transaction
                .range_in(request)
                .await
                .map_err(operation::original_failure),
        }
    }
}

#[cfg(test)]
mod tests_original_read;

impl StateStoreMvRepository {
    fn write_repository(&self) -> WriteRepository<'_> {
        WriteRepository {
            repository: self,
            execution: None,
        }
    }
}
impl StateStoreMvRepositoryExecution<'_> {
    fn write_repository(&self) -> WriteRepository<'_> {
        WriteRepository {
            repository: self.repository,
            execution: Some(self.execution),
        }
    }
    pub async fn install_keyed_projection(
        &self,
        operation_id: Uuid,
        facts: crate::persistence::keyed_documents::MvKeyedDocumentProjection,
        expected: Option<keyed::LoadedKeyedMvProjection>,
    ) -> Result<keyed::LoadedKeyedMvProjection, MvRepositoryError> {
        self.write_repository()
            .install_keyed_projection(operation_id, facts, expected)
            .await
    }
    pub async fn delete_keyed_projection(
        &self,
        operation_id: Uuid,
        expected: keyed::LoadedKeyedMvProjection,
    ) -> Result<bool, MvRepositoryError> {
        self.write_repository()
            .delete_keyed_projection(operation_id, expected)
            .await
    }
    #[cfg(any(test, novarocks_dev_controls))]
    pub async fn wipe_accelerator(&self, operation_id: Uuid) -> Result<(), MvRepositoryError> {
        self.write_repository().wipe_accelerator(operation_id).await
    }
}
struct WriteRepository<'a> {
    repository: &'a StateStoreMvRepository,
    execution: Option<&'a dyn novarocks_state_store_api::StateStoreExecutionRequests>,
}
impl WriteRepository<'_> {
    fn read_repository(&self) -> ReadRepository<'_> {
        ReadRepository {
            store: self.repository.store.as_ref(),
            execution: self.execution,
        }
    }
    async fn install_keyed_projection(
        &self,
        operation_id: Uuid,
        facts: crate::persistence::keyed_documents::MvKeyedDocumentProjection,
        expected: Option<keyed::LoadedKeyedMvProjection>,
    ) -> Result<keyed::LoadedKeyedMvProjection, MvRepositoryError> {
        let target = facts.target();
        if expected
            .as_ref()
            .is_some_and(|loaded| loaded.projection.facts.target() != target)
        {
            return Err(invalid("keyed projection replacement target differs"));
        }
        self.run(InstallProjection {
            operation_id,
            facts,
            expected,
        })
        .await?;
        self.read_repository()
            .find_keyed_by_target(
                &target,
                crate::persistence::validation::PersistenceDecodeBudget::default(),
            )
            .await?
            .ok_or_else(|| corruption("installed keyed projection is missing"))
    }
    async fn delete_keyed_projection(
        &self,
        operation_id: Uuid,
        expected: keyed::LoadedKeyedMvProjection,
    ) -> Result<bool, MvRepositoryError> {
        self.run(DeleteProjection {
            operation_id,
            expected,
        })
        .await
    }
    #[cfg(any(test, novarocks_dev_controls))]
    async fn wipe_accelerator(&self, _operation_id: Uuid) -> Result<(), MvRepositoryError> {
        let records = self
            .read_repository()
            .scan_prefix(accelerator_prefix().map_err(corruption)?)
            .await?;
        self.run(WipeProjection { records }).await
    }
    async fn run<C: WriteCommand>(&self, command: C) -> Result<C::Output, MvRepositoryError> {
        let repository = self.repository;
        match self.execution {
            Some(requests) => {
                let execution = requests
                    .write_operation_context()
                    .map_err(operation::original_failure)?;
                operation::run_in(
                    repository.store.as_ref(),
                    &repository.runner_metrics,
                    repository.run_policy,
                    C::PURPOSE,
                    &execution,
                    move |transaction| {
                        let command = command.clone();
                        Box::pin(async move {
                            command.apply(&mut OriginalWriteAccess(transaction)).await
                        })
                    },
                )
                .await
            }
            None => {
                operation::run(
                    repository.store.as_ref(),
                    &repository.runner_metrics,
                    repository.run_policy,
                    C::PURPOSE,
                    move |transaction| {
                        let command = command.clone();
                        Box::pin(async move { command.apply(transaction).await })
                    },
                )
                .await
            }
        }
    }
}
// Only these actual repository operations implement this private retry input.
// It preserves one domain algorithm and the existing runner's attempt policy.
#[async_trait::async_trait]
trait WriteCommand: Clone + Send + Sync + 'static {
    type Output: Send + 'static;
    const PURPOSE: &'static str;
    async fn apply<T: WriteAccess + ?Sized>(
        self,
        transaction: &mut T,
    ) -> Result<Self::Output, T::Error>;
}
#[derive(Clone)]
struct InstallProjection {
    operation_id: Uuid,
    facts: crate::persistence::keyed_documents::MvKeyedDocumentProjection,
    expected: Option<keyed::LoadedKeyedMvProjection>,
}
#[async_trait::async_trait]
impl WriteCommand for InstallProjection {
    type Output = ();
    const PURPOSE: &'static str = "install keyed MV projection";
    async fn apply<T: WriteAccess + ?Sized>(self, transaction: &mut T) -> Result<(), T::Error> {
        let Self {
            operation_id,
            facts,
            expected,
        } = self;

        let target = facts.target();
        let index =
            target_key(&target).map_err(|error| T::from_store(invalid_state_store(error)))?;
        let (id, condition, previous) = match expected {
            Some(expected) => {
                let root = projection_by_id_key(expected.projection.mv_id)
                    .map_err(|error| T::from_store(invalid_state_store(error)))?;
                let record = transaction.read_get(&root).await?.ok_or_else(|| {
                    T::from_store(conflict_state_store("keyed projection vanished before CAS"))
                })?;
                if record.version != *expected.version.store_version() {
                    return Err(T::from_store(conflict_state_store(
                        "keyed projection changed before CAS",
                    )));
                }
                let stored = codec::decode_keyed_projection(
                    &root,
                    &record.value,
                    crate::persistence::validation::PersistenceDecodeBudget::default(),
                )
                .map_err(|error| T::from_store(invalid_state_store(error)))?
                .value;
                if stored.mv_id != expected.projection.mv_id || stored.facts.target() != target {
                    return Err(T::from_store(conflict_state_store(
                        "keyed projection root identity differs",
                    )));
                }
                let previous = keyed::keyed_dependencies(stored.mv_id, &stored.facts);
                let current = transaction.read_get(&index).await?.ok_or_else(|| {
                    T::from_store(conflict_state_store("keyed target index vanished"))
                })?;
                let lookup: DecodedMvRecord<MvTargetLookup> = decode_record(&index, &current.value)
                    .map_err(|error| T::from_store(invalid_state_store(error)))?;
                if lookup.value.mv_id != expected.projection.mv_id {
                    return Err(T::from_store(conflict_state_store(
                        "keyed target index changed",
                    )));
                }
                (
                    expected.projection.mv_id,
                    Precondition::Version(record.version),
                    previous,
                )
            }
            None => {
                if transaction.read_get(&index).await?.is_some() {
                    return Err(T::from_store(conflict_state_store(
                        "MV target exists before keyed create",
                    )));
                }
                let sequence =
                    sequence_key().map_err(|error| T::from_store(invalid_state_store(error)))?;
                let (id, condition) = match transaction.read_get(&sequence).await? {
                    Some(row) => {
                        let old: DecodedMvRecord<MvSequence> = decode_record(&sequence, &row.value)
                            .map_err(|error| T::from_store(invalid_state_store(error)))?;
                        (
                            old.value.last_allocated_id.checked_add(1).ok_or_else(|| {
                                T::from_store(invalid_state_store("MV ID sequence exhausted"))
                            })?,
                            Precondition::Version(row.version),
                        )
                    }
                    None => (1, Precondition::Absent),
                };
                put_sequence(
                    transaction,
                    operation_id,
                    MvSequence {
                        last_allocated_id: id,
                    },
                    condition,
                )
                .await?;
                put_target_lookup(transaction, operation_id, index, id, Precondition::Absent)
                    .await?;
                (id, Precondition::Absent, Vec::new())
            }
        };
        let dependencies = keyed::keyed_dependencies(id, &facts);
        replace_dependency_indexes(transaction, operation_id, id, &previous, &dependencies).await?;
        let root =
            projection_by_id_key(id).map_err(|error| T::from_store(invalid_state_store(error)))?;
        let value = codec::encode_keyed_projection(
            operation_id,
            &crate::persistence::keyed_documents::StoredKeyedMvProjection { mv_id: id, facts },
        )
        .map_err(|error| T::from_store(invalid_state_store(error)))?;
        transaction.write_put(root, value, condition).await?;
        Ok(())
    }
}
#[derive(Clone)]
struct DeleteProjection {
    operation_id: Uuid,
    expected: keyed::LoadedKeyedMvProjection,
}
#[async_trait::async_trait]
impl WriteCommand for DeleteProjection {
    type Output = bool;
    const PURPOSE: &'static str = "delete keyed MV projection";
    async fn apply<T: WriteAccess + ?Sized>(self, transaction: &mut T) -> Result<bool, T::Error> {
        let Self {
            operation_id,
            expected,
        } = self;

        let key = projection_by_id_key(expected.projection.mv_id)
            .map_err(|error| T::from_store(invalid_state_store(error)))?;
        let Some(row) = transaction.read_get(&key).await? else {
            return Ok(false);
        };
        if row.version != *expected.version.store_version() {
            return Err(T::from_store(conflict_state_store(
                "keyed MV root changed before delete",
            )));
        }
        let current = codec::decode_keyed_projection(
            &key,
            &row.value,
            crate::persistence::validation::PersistenceDecodeBudget::default(),
        )
        .map_err(|error| T::from_store(invalid_state_store(error)))?
        .value;
        if current.facts.source_revision() != expected.projection.facts.source_revision() {
            return Err(T::from_store(conflict_state_store(
                "keyed MV source changed before delete",
            )));
        }
        delete_indexes_for_target(transaction, &current).await?;
        transaction
            .write_delete(key, Precondition::Version(row.version))
            .await?;
        let _ = operation_id;
        Ok(true)
    }
}
#[cfg(any(test, novarocks_dev_controls))]
#[derive(Clone)]
struct WipeProjection {
    records: Vec<StateRecord>,
}
#[cfg(any(test, novarocks_dev_controls))]
#[async_trait::async_trait]
impl WriteCommand for WipeProjection {
    type Output = ();
    const PURPOSE: &'static str = "wipe MV Accelerator family";
    async fn apply<T: WriteAccess + ?Sized>(self, transaction: &mut T) -> Result<(), T::Error> {
        let Self { records } = self;

        for record in &records {
            let current = transaction.read_get(&record.key).await?.ok_or_else(|| {
                T::from_store(conflict_state_store("MV Accelerator changed before wipe"))
            })?;
            if current.version != record.version {
                return Err(T::from_store(conflict_state_store(
                    "MV Accelerator changed before wipe",
                )));
            }
            transaction
                .write_delete(
                    record.key.clone(),
                    Precondition::Version(record.version.clone()),
                )
                .await?;
        }
        Ok(())
    }
}
#[async_trait::async_trait]
trait WriteAccess: ReadAccess {
    fn from_store(error: StateStoreError) -> Self::Error;
    async fn write_put(
        &mut self,
        key: Key,
        value: novarocks_state_store_api::Value,
        precondition: Precondition,
    ) -> Result<(), Self::Error>;
    async fn write_delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), Self::Error>;
}
#[async_trait::async_trait]
impl<T: WriteTransaction + ?Sized> WriteAccess for T {
    fn from_store(error: StateStoreError) -> Self::Error {
        error
    }
    async fn write_put(
        &mut self,
        key: Key,
        value: novarocks_state_store_api::Value,
        precondition: Precondition,
    ) -> Result<(), Self::Error> {
        self.put(key, value, precondition).await
    }
    async fn write_delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), Self::Error> {
        self.delete(key, precondition).await
    }
}
struct OriginalWriteAccess<'a>(&'a mut dyn novarocks_state_store_api::OriginalWriteTransaction);
#[async_trait::async_trait]
impl ReadAccess for OriginalWriteAccess<'_> {
    type Error = novarocks_state_store_api::StateStoreExecutionFailure;
    fn project(error: Self::Error) -> MvRepositoryError {
        operation::original_failure(error)
    }
    async fn read_get(&mut self, key: &Key) -> Result<Option<StateRecord>, Self::Error> {
        self.0.get_in(key).await
    }
    async fn read_range(
        &mut self,
        request: &RangeRequest,
    ) -> Result<novarocks_state_store_api::RangePage, Self::Error> {
        self.0.range_in(request).await
    }
}
#[async_trait::async_trait]
impl WriteAccess for OriginalWriteAccess<'_> {
    fn from_store(error: StateStoreError) -> Self::Error {
        error.into_original_failure()
    }
    async fn write_put(
        &mut self,
        key: Key,
        value: novarocks_state_store_api::Value,
        precondition: Precondition,
    ) -> Result<(), Self::Error> {
        self.0.put_in(key, value, precondition).await
    }
    async fn write_delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), Self::Error> {
        self.0.delete_in(key, precondition).await
    }
}
