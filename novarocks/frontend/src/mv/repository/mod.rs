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
use std::future::Future;
use std::sync::Arc;

use novarocks::mv::dependency::model::MvDependencyObjectRef;
use novarocks::mv::persistence::definition::{
    StoredMvDefinition, StoredMvRefreshPolicy, UpdateMvRefreshMetadataRequest,
};
use novarocks::mv::persistence::dependency::{CreateMvDependencyRequest, StoredMvDependency};
use novarocks::mv::persistence::partition::{
    MvPartitionRefreshStatus, RecordFailedMvPartitionStatesRequest,
    ReplaceMvPartitionStatesRequest, StoredMvPartitionState, UpdateMvPartitionContractRequest,
};
use novarocks::mv::persistence::refresh::{
    BeginIcebergMvRefreshRequest, FrontendMvRefreshAction, FrontendMvRefreshActionPhase,
    FrontendMvRefreshActionState, FrontendMvRefreshLedger, MvRefreshFinalizeRequest,
    MvRefreshLifecycleOwner, MvRefreshState, RecordPublishCommitRequest,
    RecordStagingCommitRequest, RefreshCommitMarker, RefreshExternalOutcome, StoredMvRefresh,
    UpdateStarRocksMvRefreshSummaryRequest,
};
use novarocks::mv::repository::{
    CreateMvRepositoryRequest, CreateMvRepositoryWithIdRequest,
    FinalizeMvRefreshWithPartitionsRequest, MvRepository, MvRepositoryAvailability,
    MvRepositoryError, MvRepositoryErrorKind, MvTarget, MvTargetLookup, RebuildMvRepositoryRequest,
    RecordExternalCommitAndFinalizeRequest,
};
use novarocks_spi::state_store::{
    Direction, Key, KeyRange, Precondition, RangeRequest, StateRecord, StateStore, WriteTransaction,
};
use novarocks_state_store::metrics::StateStoreMetrics;
use uuid::Uuid;

use self::codec::{
    DecodedMvRecord, MvRecordKind, MvSequence, decode_definition, decode_record, encode_definition,
    encode_record,
};
use self::key::{
    definition_by_id_key, definition_prefix, dependency_by_downstream_key,
    dependency_by_downstream_prefix, dependency_by_upstream_key, dependency_by_upstream_prefix,
    mv_prefix, partition_by_mv_key, partition_by_mv_prefix, refresh_by_id_key, refresh_prefix,
    sequence_key, target_lookup_key,
};

/// The sole MV StateStore repository. It keeps provider transactions private
/// and exposes only the provider-neutral core MV port.
pub struct StateStoreMvRepository {
    store: Arc<dyn StateStore>,
    runtime: tokio::runtime::Handle,
    runner_metrics: StateStoreMetrics,
}

pub use novarocks::mv::repository::BeginFrontendMvRefreshIntentRequest;

impl StateStoreMvRepository {
    pub async fn open(
        store: Arc<dyn StateStore>,
        runtime: tokio::runtime::Handle,
    ) -> Result<Arc<Self>, MvRepositoryError> {
        let repository = Arc::new(Self {
            runner_metrics: StateStoreMetrics::new(
                novarocks_spi::state_store::StateStoreProviderId::new("frontend-mv"),
            ),
            store,
            runtime,
        });
        repository.validate_open_state().await?;
        Ok(repository)
    }

    fn blocking<T>(
        &self,
        future: impl Future<Output = Result<T, MvRepositoryError>>,
    ) -> Result<T, MvRepositoryError> {
        match tokio::runtime::Handle::try_current() {
            Ok(handle)
                if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread =>
            {
                Err(MvRepositoryError::new(
                    MvRepositoryErrorKind::InvalidRequest,
                    "MV repository synchronous commands cannot run on a current-thread Tokio runtime",
                ))
            }
            Ok(_) => tokio::task::block_in_place(|| self.runtime.block_on(future)),
            Err(_) => self.runtime.block_on(future),
        }
    }

    async fn validate_open_state(&self) -> Result<(), MvRepositoryError> {
        let records = self.scan_prefix(mv_prefix().map_err(corruption)?).await?;
        let mut definitions = BTreeMap::new();
        let mut target_records = BTreeMap::new();
        let mut downstream = BTreeMap::new();
        let mut upstream = BTreeMap::new();
        let mut refreshes = Vec::new();
        let mut partitions = Vec::new();
        for record in records {
            match key::decode_key(&record.key).map_err(corruption)?.kind {
                key::MvKeyKind::Sequence => {
                    let sequence: DecodedMvRecord<MvSequence> =
                        decode_record(&record.key, &record.value).map_err(corruption)?;
                    if sequence.value.last_allocated_id < 0 || sequence.value.last_refresh_id < 0 {
                        return Err(corruption("MV sequence must not be negative"));
                    }
                }
                key::MvKeyKind::Definition => {
                    let definition =
                        decode_definition(&record.key, &record.value).map_err(corruption)?;
                    if definition_by_id_key(definition.value.mv_id).map_err(corruption)?
                        != record.key
                    {
                        return Err(corruption("MV definition key does not match its stored ID"));
                    }
                    definitions.insert(definition.value.mv_id, definition.value);
                }
                key::MvKeyKind::TargetLookup => {
                    let lookup: DecodedMvRecord<MvTargetLookup> =
                        decode_record(&record.key, &record.value).map_err(corruption)?;
                    target_records.insert(record.key, lookup.value);
                }
                key::MvKeyKind::DependencyDownstream => {
                    let dependency: DecodedMvRecord<StoredMvDependency> =
                        decode_record(&record.key, &record.value).map_err(corruption)?;
                    if dependency_by_downstream_key(
                        dependency.value.downstream_mv_id,
                        &dependency.value.upstream,
                    )
                    .map_err(corruption)?
                        != record.key
                    {
                        return Err(corruption(
                            "MV downstream dependency key does not match its record",
                        ));
                    }
                    downstream.insert(record.key, dependency.value);
                }
                key::MvKeyKind::DependencyUpstream => {
                    let dependency: DecodedMvRecord<StoredMvDependency> =
                        decode_record(&record.key, &record.value).map_err(corruption)?;
                    if dependency_by_upstream_key(
                        &dependency.value.upstream,
                        dependency.value.downstream_mv_id,
                    )
                    .map_err(corruption)?
                        != record.key
                    {
                        return Err(corruption(
                            "MV upstream dependency key does not match its record",
                        ));
                    }
                    upstream.insert(record.key, dependency.value);
                }
                key::MvKeyKind::Refresh => {
                    let refresh: DecodedMvRecord<StoredMvRefresh> =
                        decode_record(&record.key, &record.value).map_err(corruption)?;
                    if refresh_by_id_key(refresh.value.refresh_id).map_err(corruption)?
                        != record.key
                    {
                        return Err(corruption("MV refresh key does not match its stored ID"));
                    }
                    refreshes.push(refresh.value);
                }
                key::MvKeyKind::Partition => {
                    let partition: DecodedMvRecord<StoredMvPartitionState> =
                        decode_record(&record.key, &record.value).map_err(corruption)?;
                    if partition_by_mv_key(partition.value.mv_id, &partition.value.partition_key)
                        .map_err(corruption)?
                        != record.key
                    {
                        return Err(corruption(
                            "MV partition key does not match its stored identity",
                        ));
                    }
                    partitions.push(partition.value);
                }
            }
        }
        for (key, lookup) in &target_records {
            let definition = definitions
                .get(&lookup.mv_id)
                .ok_or_else(|| corruption("MV target lookup references a missing definition"))?;
            let target = definition_target(definition)?.ok_or_else(|| {
                corruption("MV target lookup references a definition without a target")
            })?;
            if target_lookup_key(
                &target.catalog.unwrap_or_default(),
                &target.database,
                &target.name,
            )
            .map_err(corruption)?
                != *key
            {
                return Err(corruption(
                    "MV target lookup key does not match its definition target",
                ));
            }
        }
        for definition in definitions.values() {
            let Some(target) = definition_target(definition)? else {
                continue;
            };
            let target_key = target_lookup_key(
                &target.catalog.unwrap_or_default(),
                &target.database,
                &target.name,
            )
            .map_err(corruption)?;
            if target_records.get(&target_key).map(|lookup| lookup.mv_id) != Some(definition.mv_id)
            {
                return Err(corruption(
                    "MV definition target has no matching target lookup record",
                ));
            }
        }
        for refresh in refreshes {
            match refresh.lifecycle_owner {
                MvRefreshLifecycleOwner::LegacyCore if refresh.frontend_ledger.is_some() => {
                    return Err(corruption(
                        "legacy MV refresh unexpectedly carries a frontend ledger",
                    ));
                }
                MvRefreshLifecycleOwner::FrontendCurrent => {
                    let ledger = refresh
                        .frontend_ledger
                        .as_ref()
                        .ok_or_else(|| corruption("frontend-owned MV refresh has no v3 ledger"))?;
                    ledger.validate().map_err(corruption)?;
                }
                MvRefreshLifecycleOwner::LegacyCore => {}
            }
            let definition = definitions
                .get(&refresh.mv_id)
                .ok_or_else(|| corruption("MV refresh references a missing definition"))?;
            if refresh.state == MvRefreshState::Finalized
                && definition.active_refresh_id == Some(refresh.refresh_id)
            {
                return Err(corruption(
                    "finalized MV refresh remains active on its definition",
                ));
            }
            if definition.active_refresh_id == Some(refresh.refresh_id)
                && !definition.refresh_in_progress
            {
                return Err(corruption("active MV refresh is not marked in progress"));
            }
        }
        for partition in partitions {
            if !definitions.contains_key(&partition.mv_id) {
                return Err(corruption(
                    "MV partition state references a missing definition",
                ));
            }
        }
        for (key, dependency) in &downstream {
            let peer =
                dependency_by_upstream_key(&dependency.upstream, dependency.downstream_mv_id)
                    .map_err(corruption)?;
            if upstream.get(&peer) != Some(dependency) {
                return Err(corruption(format!(
                    "MV dependency index {key:?} has no symmetric upstream record"
                )));
            }
        }
        for (key, dependency) in &upstream {
            let peer =
                dependency_by_downstream_key(dependency.downstream_mv_id, &dependency.upstream)
                    .map_err(corruption)?;
            if downstream.get(&peer) != Some(dependency) {
                return Err(corruption(format!(
                    "MV dependency index {key:?} has no symmetric downstream record"
                )));
            }
        }
        Ok(())
    }

    async fn scan_prefix(&self, prefix: Key) -> Result<Vec<StateRecord>, MvRepositoryError> {
        let range = KeyRange::for_prefix(prefix).map_err(operation::state_store_error)?;
        let mut continuation = None;
        let mut records = Vec::new();
        loop {
            let mut transaction = self
                .store
                .begin_read()
                .await
                .map_err(operation::state_store_error)?;
            let page = transaction
                .range(&RangeRequest {
                    range: range.clone(),
                    direction: Direction::Forward,
                    page_size: self.store.limits().max_page_size,
                    continuation: continuation.clone(),
                })
                .await
                .map_err(operation::state_store_error)?;
            transaction
                .abort()
                .await
                .map_err(operation::state_store_error)?;
            continuation = page.continuation;
            records.extend(page.records);
            if continuation.is_none() {
                return Ok(records);
            }
        }
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

    async fn require_definition_async(&self, mv_id: i64) -> Result<(), MvRepositoryError> {
        if self.load_by_id_async(mv_id).await?.is_some() {
            Ok(())
        } else {
            Err(MvRepositoryError::new(
                MvRepositoryErrorKind::NotFound,
                format!("mv definition {mv_id} not found"),
            ))
        }
    }

    async fn require_refresh_async(&self, refresh_id: i64) -> Result<(), MvRepositoryError> {
        if self.load_refresh_async(refresh_id).await?.is_some() {
            Ok(())
        } else {
            Err(MvRepositoryError::new(
                MvRepositoryErrorKind::NotFound,
                format!("mv refresh {refresh_id} not found"),
            ))
        }
    }

    async fn create_async(
        &self,
        operation_id: Uuid,
        request: CreateMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.create_with_optional_id_async(operation_id, None, request)
            .await
    }

    async fn create_with_optional_id_async(
        &self,
        operation_id: Uuid,
        explicit_id: Option<i64>,
        request: CreateMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        validate_create_request(&request)?;
        prevalidate_create_operation(operation_id, explicit_id, &request)?;
        let recovery_request = request.clone();
        let store = Arc::clone(&self.store);
        let metrics = &self.runner_metrics;
        let outcome =
            operation::run_raw(
                store.as_ref(),
                metrics,
                operation_id,
                "create materialized view",
                move |transaction| {
                    let request = request.clone();
                    Box::pin(async move {
                        let sequence_key = sequence_key().map_err(invalid_state_store)?;
                        let sequence_record = transaction.get(&sequence_key).await?;
                        let sequence = match &sequence_record {
                            Some(record) => {
                                decode_record::<MvSequence>(&sequence_key, &record.value)
                                    .map_err(invalid_state_store)?
                                    .value
                            }
                            None => MvSequence {
                                last_allocated_id: 0,
                                last_refresh_id: 0,
                            },
                        };
                        let last = sequence.last_allocated_id;
                        let mv_id =
                            match explicit_id {
                                Some(value) => value,
                                None => last.checked_add(1).filter(|value| *value > 0).ok_or_else(
                                    || invalid_state_store("MV definition ID sequence overflow"),
                                )?,
                            };
                        if mv_id <= 0 {
                            return Err(invalid_state_store("MV definition ID must be positive"));
                        }
                        let definition_key =
                            definition_by_id_key(mv_id).map_err(invalid_state_store)?;
                        if transaction.get(&definition_key).await?.is_some() {
                            return Err(conflict_state_store(format!(
                                "mv definition {mv_id} already exists"
                            )));
                        }
                        let definition = definition_from_request(mv_id, &request);
                        let sequence_value = encode_record(
                            MvRecordKind::Sequence,
                            operation_id,
                            &MvSequence {
                                last_allocated_id: last.max(mv_id),
                                last_refresh_id: sequence.last_refresh_id,
                            },
                        )
                        .map_err(invalid_state_store)?;
                        let definition_value = encode_definition(operation_id, &definition)
                            .map_err(invalid_state_store)?;
                        if mv_id > last {
                            transaction
                                .put(
                                    sequence_key,
                                    sequence_value,
                                    sequence_record
                                        .map(|record| Precondition::Version(record.version))
                                        .unwrap_or(Precondition::Absent),
                                )
                                .await?;
                        }
                        transaction
                            .put(definition_key, definition_value, Precondition::Absent)
                            .await?;
                        if let Some(target) = definition_target(&definition).map_err(|_| {
                            invalid_state_store("MV definition has an invalid target")
                        })? {
                            let target_key = target_lookup_key(
                                &target.catalog.unwrap_or_default(),
                                &target.database,
                                &target.name,
                            )
                            .map_err(invalid_state_store)?;
                            let target_value = encode_record(
                                MvRecordKind::TargetLookup,
                                operation_id,
                                &MvTargetLookup { mv_id },
                            )
                            .map_err(invalid_state_store)?;
                            transaction
                                .put(target_key, target_value, Precondition::Absent)
                                .await?;
                        }
                        for dependency in deduplicate_dependencies(mv_id, &request.dependencies)
                            .map_err(invalid_state_store)?
                        {
                            put_dependency(
                                transaction,
                                operation_id,
                                &dependency,
                                Precondition::Absent,
                            )
                            .await?;
                        }
                        Ok(definition)
                    })
                },
            )
            .await;
        match outcome {
            Ok(value) => Ok(value),
            Err(novarocks_state_store::RunFailure::CommitUnknown {
                transaction_id,
                error,
            }) => {
                let original = MvRepositoryError::new(
                    MvRepositoryErrorKind::CommitUnknown,
                    format!("MV CREATE commit outcome is unknown: {error}"),
                );
                match operation::resolve_commit(self.store.as_ref(), &transaction_id).await? {
                    novarocks_spi::state_store::CommitResolution::Committed(_) => self
                        .recover_create(operation_id, &recovery_request, original)
                        .await
                        .map_err(|recovery| {
                            if recovery.kind() == MvRepositoryErrorKind::CommitUnknown {
                                corruption(
                                    "MV CREATE committed but its authoritative records are missing",
                                )
                            } else {
                                recovery
                            }
                        }),
                    novarocks_spi::state_store::CommitResolution::NotCommitted => {
                        Box::pin(self.create_with_optional_id_async(
                            operation_id,
                            explicit_id,
                            recovery_request,
                        ))
                        .await
                    }
                    novarocks_spi::state_store::CommitResolution::Unresolved => {
                        self.recover_create(operation_id, &recovery_request, original)
                            .await
                    }
                }
            }
            Err(error) => Err(operation::run_failure(error)),
        }
    }

    async fn recover_create(
        &self,
        operation_id: Uuid,
        request: &CreateMvRepositoryRequest,
        original: MvRepositoryError,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        // A durable, exact operation-ID match is authoritative even if the provider
        // cannot yet resolve its commit ledger.
        for definition in self.list_definitions_async().await? {
            let key = definition_by_id_key(definition.mv_id).map_err(corruption)?;
            let Some(record) = self.read_record(&key).await? else {
                continue;
            };
            let matches_definition = decode_definition(&key, &record.value)
                .map(|decoded| decoded.operation_id == operation_id)
                .unwrap_or(false)
                && definition_matches_request(&definition, request);
            if !matches_definition {
                continue;
            }
            if let Some(target) = definition_target(&definition)? {
                let target_key = target_lookup_key(
                    &target.catalog.unwrap_or_default(),
                    &target.database,
                    &target.name,
                )
                .map_err(corruption)?;
                let Some(target_record) = self.read_record(&target_key).await? else {
                    continue;
                };
                let target_matches =
                    decode_record::<MvTargetLookup>(&target_key, &target_record.value)
                        .map(|decoded| {
                            decoded.operation_id == operation_id
                                && decoded.value.mv_id == definition.mv_id
                        })
                        .unwrap_or(false);
                if !target_matches {
                    continue;
                }
            }
            let expected_dependencies =
                deduplicate_dependencies(definition.mv_id, &request.dependencies)
                    .map_err(corruption)?;
            let actual_dependencies = self
                .list_dependencies_downstream_async(definition.mv_id)
                .await?;
            if actual_dependencies.len() != expected_dependencies.len() {
                continue;
            }
            let mut dependencies_match = true;
            for expected in &expected_dependencies {
                let downstream_key =
                    dependency_by_downstream_key(expected.downstream_mv_id, &expected.upstream)
                        .map_err(corruption)?;
                let upstream_key =
                    dependency_by_upstream_key(&expected.upstream, expected.downstream_mv_id)
                        .map_err(corruption)?;
                for key in [&downstream_key, &upstream_key] {
                    let Some(record) = self.read_record(key).await? else {
                        dependencies_match = false;
                        break;
                    };
                    let matches = decode_record::<StoredMvDependency>(key, &record.value)
                        .map(|decoded| {
                            decoded.operation_id == operation_id && decoded.value == *expected
                        })
                        .unwrap_or(false);
                    if !matches {
                        dependencies_match = false;
                        break;
                    }
                }
                if !dependencies_match {
                    break;
                }
            }
            if dependencies_match {
                return Ok(definition);
            }
        }
        Err(original)
    }

    async fn reserve_definition_id_async(&self, mv_id: i64) -> Result<(), MvRepositoryError> {
        if mv_id <= 0 {
            return Err(invalid("mv definition id must be positive"));
        }
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "reserve materialized view ID",
            move |transaction| {
                Box::pin(async move {
                    let definition_key =
                        definition_by_id_key(mv_id).map_err(invalid_state_store)?;
                    if transaction.get(&definition_key).await?.is_some() {
                        return Err(conflict_state_store(format!(
                            "mv definition {mv_id} already exists"
                        )));
                    }
                    let key = sequence_key().map_err(invalid_state_store)?;
                    let current = transaction.get(&key).await?;
                    let sequence = match &current {
                        Some(record) => {
                            decode_record::<MvSequence>(&key, &record.value)
                                .map_err(invalid_state_store)?
                                .value
                        }
                        None => MvSequence {
                            last_allocated_id: 0,
                            last_refresh_id: 0,
                        },
                    };
                    let last = sequence.last_allocated_id;
                    if last < mv_id {
                        let value = encode_record(
                            MvRecordKind::Sequence,
                            operation_id,
                            &MvSequence {
                                last_allocated_id: mv_id,
                                last_refresh_id: sequence.last_refresh_id,
                            },
                        )
                        .map_err(invalid_state_store)?;
                        transaction
                            .put(
                                key,
                                value,
                                current
                                    .map(|record| Precondition::Version(record.version))
                                    .unwrap_or(Precondition::Absent),
                            )
                            .await?;
                    }
                    Ok(())
                })
            },
        )
        .await
    }

    async fn load_by_id_async(
        &self,
        mv_id: i64,
    ) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        if mv_id <= 0 {
            return Err(invalid("mv definition id must be positive"));
        }
        let key = definition_by_id_key(mv_id).map_err(invalid)?;
        self.read_record(&key)
            .await?
            .map(|record| {
                decode_definition(&key, &record.value)
                    .map(|decoded| decoded.value)
                    .map_err(corruption)
            })
            .transpose()
    }

    async fn list_definitions_async(&self) -> Result<Vec<StoredMvDefinition>, MvRepositoryError> {
        let mut definitions = self
            .scan_prefix(definition_prefix().map_err(corruption)?)
            .await?
            .into_iter()
            .map(|record| {
                decode_definition(&record.key, &record.value)
                    .map(|decoded| decoded.value)
                    .map_err(corruption)
            })
            .collect::<Result<Vec<_>, _>>()?;
        definitions.sort_by_key(|definition| definition.mv_id);
        Ok(definitions)
    }

    async fn find_by_target_async(
        &self,
        target: &MvTarget,
    ) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        let key = target_lookup_key(
            &target.catalog.clone().unwrap_or_default(),
            &target.database,
            &target.name,
        )
        .map_err(invalid)?;
        let Some(record) = self.read_record(&key).await? else {
            return Ok(None);
        };
        let lookup: DecodedMvRecord<MvTargetLookup> =
            decode_record(&key, &record.value).map_err(corruption)?;
        let definition = self
            .load_by_id_async(lookup.value.mv_id)
            .await?
            .ok_or_else(|| corruption("MV target lookup references a missing definition"))?;
        let definition_target = definition_target(&definition)?.ok_or_else(|| {
            corruption("MV target lookup references a definition without a target")
        })?;
        let definition_key = target_lookup_key(
            &definition_target.catalog.unwrap_or_default(),
            &definition_target.database,
            &definition_target.name,
        )
        .map_err(corruption)?;
        if definition_key != key {
            return Err(corruption("MV target lookup does not match its definition"));
        }
        Ok(Some(definition))
    }

    async fn list_dependencies_downstream_async(
        &self,
        mv_id: i64,
    ) -> Result<Vec<(StateRecord, StoredMvDependency)>, MvRepositoryError> {
        let mut dependencies = self
            .scan_prefix(dependency_by_downstream_prefix(mv_id).map_err(corruption)?)
            .await?
            .into_iter()
            .map(|record| {
                let dependency: DecodedMvRecord<StoredMvDependency> =
                    decode_record(&record.key, &record.value).map_err(corruption)?;
                Ok((record, dependency.value))
            })
            .collect::<Result<Vec<_>, MvRepositoryError>>()?;
        dependencies.sort_by(|left, right| {
            dependency_sort_key(&left.1).cmp(&dependency_sort_key(&right.1))
        });
        Ok(dependencies)
    }

    async fn list_dependencies_upstream_async(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        let mut dependencies = self
            .scan_prefix(dependency_by_upstream_prefix(upstream).map_err(corruption)?)
            .await?
            .into_iter()
            .map(|record| {
                decode_record::<StoredMvDependency>(&record.key, &record.value)
                    .map(|decoded| decoded.value)
                    .map_err(corruption)
            })
            .collect::<Result<Vec<_>, _>>()?;
        dependencies.sort_by_key(|dependency| dependency.downstream_mv_id);
        Ok(dependencies)
    }

    async fn replace_dependencies_async(
        &self,
        mv_id: i64,
        requests: Vec<CreateMvDependencyRequest>,
    ) -> Result<(), MvRepositoryError> {
        if mv_id <= 0 {
            return Err(invalid("mv definition id must be positive"));
        }
        let operation_id = Uuid::now_v7();
        let page_size = self.store.limits().max_page_size;
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "replace materialized view dependencies",
            move |transaction| {
                let requests = requests.clone();
                Box::pin(async move {
                    let prefix =
                        dependency_by_downstream_prefix(mv_id).map_err(invalid_state_store)?;
                    let existing = range_transaction(transaction, prefix, page_size).await?;
                    let desired =
                        deduplicate_dependencies(mv_id, &requests).map_err(invalid_state_store)?;
                    let desired_by_key = desired
                        .into_iter()
                        .map(|dependency| {
                            dependency_by_downstream_key(mv_id, &dependency.upstream)
                                .map(|key| (key, dependency))
                                .map_err(invalid_state_store)
                        })
                        .collect::<Result<BTreeMap<_, _>, _>>()?;
                    for record in existing {
                        let dependency: DecodedMvRecord<StoredMvDependency> =
                            decode_record(&record.key, &record.value)
                                .map_err(invalid_state_store)?;
                        let upstream_key =
                            dependency_by_upstream_key(&dependency.value.upstream, mv_id)
                                .map_err(invalid_state_store)?;
                        if let Some(replacement) = desired_by_key.get(&record.key) {
                            let upstream =
                                transaction.get(&upstream_key).await?.ok_or_else(|| {
                                    invalid_state_store("MV dependency index is asymmetric")
                                })?;
                            let payload =
                                encode_record(MvRecordKind::Dependency, operation_id, replacement)
                                    .map_err(invalid_state_store)?;
                            transaction
                                .put(
                                    record.key,
                                    payload.clone(),
                                    Precondition::Version(record.version),
                                )
                                .await?;
                            transaction
                                .put(
                                    upstream_key,
                                    payload,
                                    Precondition::Version(upstream.version),
                                )
                                .await?;
                        } else {
                            transaction
                                .delete(record.key, Precondition::Version(record.version))
                                .await?;
                            let upstream =
                                transaction.get(&upstream_key).await?.ok_or_else(|| {
                                    invalid_state_store("MV dependency index is asymmetric")
                                })?;
                            transaction
                                .delete(upstream_key, Precondition::Version(upstream.version))
                                .await?;
                        }
                    }
                    for (key, dependency) in desired_by_key {
                        if transaction.get(&key).await?.is_none() {
                            put_dependency(
                                transaction,
                                operation_id,
                                &dependency,
                                Precondition::Absent,
                            )
                            .await?;
                        }
                    }
                    Ok(())
                })
            },
        )
        .await
    }

    async fn delete_dependencies_async(&self, mv_id: i64) -> Result<(), MvRepositoryError> {
        self.replace_dependencies_async(mv_id, Vec::new()).await
    }

    async fn drop_by_id_async(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        if mv_id <= 0 {
            return Err(invalid("mv definition id must be positive"));
        }
        let operation_id = Uuid::now_v7();
        let existing_partitions = self.list_partition_state_records_async(mv_id).await?;
        let dependency_records = self
            .scan_prefix(dependency_by_downstream_prefix(mv_id).map_err(corruption)?)
            .await?;
        let mut existing_dependencies = Vec::with_capacity(dependency_records.len());
        for downstream in dependency_records {
            let dependency: DecodedMvRecord<StoredMvDependency> =
                decode_record(&downstream.key, &downstream.value).map_err(corruption)?;
            let upstream_key = dependency_by_upstream_key(&dependency.value.upstream, mv_id)
                .map_err(corruption)?;
            let upstream = self
                .read_record(&upstream_key)
                .await?
                .ok_or_else(|| corruption("MV dependency index is asymmetric before MV drop"))?;
            existing_dependencies.push((downstream, upstream));
        }
        let existing_refreshes = self
            .scan_prefix(refresh_prefix().map_err(corruption)?)
            .await?
            .into_iter()
            .filter_map(|record| {
                decode_record::<StoredMvRefresh>(&record.key, &record.value)
                    .map(|refresh| (refresh.value.mv_id == mv_id).then_some(record))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(corruption)?;
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "drop materialized view definition",
            move |transaction| {
                let existing_partitions = existing_partitions.clone();
                let existing_dependencies = existing_dependencies.clone();
                let existing_refreshes = existing_refreshes.clone();
                Box::pin(async move {
                    let definition_key =
                        definition_by_id_key(mv_id).map_err(invalid_state_store)?;
                    let Some(record) = transaction.get(&definition_key).await? else {
                        return Ok(false);
                    };
                    let definition = decode_definition(&definition_key, &record.value)
                        .map_err(invalid_state_store)?
                        .value;
                    if definition.refresh_in_progress || definition.active_refresh_id.is_some() {
                        return Err(conflict_state_store(format!(
                            "mv definition {mv_id} has refresh in progress"
                        )));
                    }
                    if let Some(target) = definition_target(&definition)
                        .map_err(|_| invalid_state_store("MV definition has an invalid target"))?
                    {
                        let target_key = target_lookup_key(
                            &target.catalog.unwrap_or_default(),
                            &target.database,
                            &target.name,
                        )
                        .map_err(invalid_state_store)?;
                        let target_record =
                            transaction.get(&target_key).await?.ok_or_else(|| {
                                invalid_state_store("MV definition target lookup is missing")
                            })?;
                        transaction
                            .delete(target_key, Precondition::Version(target_record.version))
                            .await?;
                    }
                    for (dependency_record, upstream_record) in &existing_dependencies {
                        let current_dependency = transaction
                            .get(&dependency_record.key)
                            .await?
                            .ok_or_else(|| {
                                conflict_state_store("MV dependency changed before drop")
                            })?;
                        let current_upstream = transaction
                            .get(&upstream_record.key)
                            .await?
                            .ok_or_else(|| {
                                conflict_state_store("MV dependency changed before drop")
                            })?;
                        if current_dependency.version != dependency_record.version
                            || current_upstream.version != upstream_record.version
                        {
                            return Err(conflict_state_store("MV dependency changed before drop"));
                        }
                        transaction
                            .delete(
                                dependency_record.key.clone(),
                                Precondition::Version(dependency_record.version.clone()),
                            )
                            .await?;
                        transaction
                            .delete(
                                upstream_record.key.clone(),
                                Precondition::Version(upstream_record.version.clone()),
                            )
                            .await?;
                    }
                    for refresh_record in &existing_refreshes {
                        let current_refresh =
                            transaction.get(&refresh_record.key).await?.ok_or_else(|| {
                                conflict_state_store("MV refresh changed before drop")
                            })?;
                        if current_refresh.version != refresh_record.version {
                            return Err(conflict_state_store("MV refresh changed before drop"));
                        }
                        transaction
                            .delete(
                                refresh_record.key.clone(),
                                Precondition::Version(refresh_record.version.clone()),
                            )
                            .await?;
                    }
                    delete_partition_states_transaction(transaction, &existing_partitions).await?;
                    transaction
                        .delete(definition_key, Precondition::Version(record.version))
                        .await?;
                    Ok(true)
                })
            },
        )
        .await
    }

    async fn update_partition_contract_async(
        &self,
        request: UpdateMvPartitionContractRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.require_definition_async(request.mv_id).await?;
        let operation_id = Uuid::now_v7();
        let existing_partitions = self
            .list_partition_state_records_async(request.mv_id)
            .await?;
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "update MV partition contract",
            move |transaction| {
                let request = request.clone();
                let existing_partitions = existing_partitions.clone();
                Box::pin(async move {
                    let (definition_record, mut definition) =
                        load_definition_transaction(transaction, request.mv_id).await?;
                    let schema = definition.schema_contract.as_mut().ok_or_else(|| {
                        invalid_state_store("MV definition has no schema contract")
                    })?;
                    schema.target.partition = Some(request.partition_spec.clone());
                    definition.partition_spec = Some(request.partition_spec);
                    definition.partition_state_complete = false;
                    put_definition_transaction(
                        transaction,
                        operation_id,
                        &definition,
                        Precondition::Version(definition_record.version),
                    )
                    .await?;
                    delete_partition_states_transaction(transaction, &existing_partitions).await?;
                    Ok(definition)
                })
            },
        )
        .await
    }

    async fn set_rebuilt_refresh_watermark_async(
        &self,
        mv_id: i64,
        base_snapshots: BTreeMap<String, i64>,
        base_table_uuids: BTreeMap<String, String>,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.require_definition_async(mv_id).await?;
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "set rebuilt MV refresh watermark",
            move |transaction| {
                let base_snapshots = base_snapshots.clone();
                let base_table_uuids = base_table_uuids.clone();
                Box::pin(async move {
                    let (record, mut definition) =
                        load_definition_transaction(transaction, mv_id).await?;
                    definition.last_refresh_snapshots = base_snapshots;
                    definition.last_refresh_table_uuids = base_table_uuids;
                    put_definition_transaction(
                        transaction,
                        operation_id,
                        &definition,
                        Precondition::Version(record.version),
                    )
                    .await?;
                    Ok(definition)
                })
            },
        )
        .await
    }

    async fn update_refresh_metadata_async(
        &self,
        request: UpdateMvRefreshMetadataRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        validate_refresh_metadata_request(&request)?;
        self.require_definition_async(request.mv_id).await?;
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "update MV refresh metadata",
            move |transaction| {
                let request = request.clone();
                Box::pin(async move {
                    let (record, mut definition) =
                        load_definition_transaction(transaction, request.mv_id).await?;
                    definition.refresh_policy = request.refresh_policy;
                    definition.refresh_paused = request.refresh_paused;
                    definition.refresh_interval_ms = request.refresh_interval_ms;
                    definition.max_staleness_ms = request.max_staleness_ms;
                    definition.last_scheduler_error = request.last_scheduler_error;
                    definition.next_refresh_after_ms = request.next_refresh_after_ms;
                    put_definition_transaction(
                        transaction,
                        operation_id,
                        &definition,
                        Precondition::Version(record.version),
                    )
                    .await?;
                    Ok(definition)
                })
            },
        )
        .await
    }

    async fn begin_refresh_intent_async(
        &self,
        mv_id: i64,
        target_snapshots: BTreeMap<String, i64>,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        self.require_definition_async(mv_id).await?;
        let operation_id = Uuid::now_v7();
        let page_size = self.store.limits().max_page_size;
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "begin MV refresh",
            move |transaction| {
                let target_snapshots = target_snapshots.clone();
                Box::pin(async move {
                    let (definition_record, mut definition) =
                        load_definition_transaction(transaction, mv_id).await?;
                    if definition.refresh_in_progress || definition.active_refresh_id.is_some() {
                        return Err(conflict_state_store(format!(
                            "mv definition {mv_id} already has refresh in progress"
                        )));
                    }
                    let refresh_id =
                        allocate_refresh_id(transaction, operation_id, page_size).await?;
                    definition.refresh_in_progress = true;
                    definition.active_refresh_id = Some(refresh_id);
                    definition.refresh_target_snapshots = target_snapshots.clone();
                    let refresh = new_refresh(refresh_id, mv_id, None, target_snapshots);
                    put_definition_transaction(
                        transaction,
                        operation_id,
                        &definition,
                        Precondition::Version(definition_record.version),
                    )
                    .await?;
                    put_refresh_transaction(
                        transaction,
                        operation_id,
                        &refresh,
                        Precondition::Absent,
                    )
                    .await?;
                    Ok(refresh)
                })
            },
        )
        .await
    }

    async fn begin_iceberg_refresh_intent_async(
        &self,
        request: BeginIcebergMvRefreshRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        validate_iceberg_refresh_request(&request)?;
        self.require_definition_async(request.mv_id).await?;
        let operation_id = Uuid::now_v7();
        let page_size = self.store.limits().max_page_size;
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "begin Iceberg MV refresh",
            move |transaction| {
                let request = request.clone();
                Box::pin(async move {
                    let (definition_record, mut definition) =
                        load_definition_transaction(transaction, request.mv_id).await?;
                    if definition.refresh_in_progress || definition.active_refresh_id.is_some() {
                        return Err(conflict_state_store(format!(
                            "mv definition {} already has refresh in progress",
                            request.mv_id
                        )));
                    }
                    let refresh_id =
                        allocate_refresh_id(transaction, operation_id, page_size).await?;
                    definition.refresh_in_progress = true;
                    definition.active_refresh_id = Some(refresh_id);
                    definition.refresh_target_snapshots = request.base_snapshots.clone();
                    let refresh = StoredMvRefresh {
                        refresh_id,
                        mv_id: request.mv_id,
                        operation_id: request.operation_id,
                        state: MvRefreshState::IntentCreated,
                        target_catalog: Some(request.target_catalog),
                        target_namespace: Some(request.target_namespace),
                        target_table: Some(request.target_table),
                        staging_branch: Some(request.staging_branch),
                        expected_main_snapshot_id: request.expected_main_snapshot_id,
                        staging_snapshot_id: None,
                        published_snapshot_id: None,
                        target_snapshots: request.base_snapshots,
                        base_table_uuids: BTreeMap::new(),
                        rows: None,
                        marker: Some(RefreshCommitMarker {
                            refresh_id,
                            mv_id: request.mv_id,
                            token: request.marker_token,
                        }),
                        external_outcome: None,
                        lifecycle_owner: MvRefreshLifecycleOwner::LegacyCore,
                        frontend_ledger: None,
                    };
                    put_definition_transaction(
                        transaction,
                        operation_id,
                        &definition,
                        Precondition::Version(definition_record.version),
                    )
                    .await?;
                    put_refresh_transaction(
                        transaction,
                        operation_id,
                        &refresh,
                        Precondition::Absent,
                    )
                    .await?;
                    Ok(refresh)
                })
            },
        )
        .await
    }

    pub async fn begin_frontend_refresh_intent_async(
        &self,
        mut request: BeginFrontendMvRefreshIntentRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        validate_frontend_refresh_request(&request)?;
        if request.prepare_external_actions {
            request.ledger.actions = frontend_prepared_actions(&request.ledger);
        }
        request.ledger.validate().map_err(invalid)?;
        self.require_definition_async(request.mv_id).await?;
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "begin frontend-owned MV refresh",
            move |transaction| {
                let request = request.clone();
                Box::pin(async move {
                    let (definition_record, mut definition) =
                        load_definition_transaction(transaction, request.mv_id).await?;
                    if definition.refresh_in_progress || definition.active_refresh_id.is_some() {
                        return Err(conflict_state_store(format!(
                            "mv definition {} already has refresh in progress",
                            request.mv_id
                        )));
                    }
                    let refresh_id = request.refresh_id;
                    definition.refresh_in_progress = true;
                    definition.active_refresh_id = Some(refresh_id);
                    definition.refresh_target_snapshots = request.base_snapshots.clone();
                    let refresh = StoredMvRefresh {
                        refresh_id,
                        mv_id: request.mv_id,
                        operation_id: None,
                        state: MvRefreshState::IntentCreated,
                        target_catalog: Some(request.target_catalog),
                        target_namespace: Some(request.target_namespace),
                        target_table: Some(request.target_table),
                        staging_branch: Some(request.staging_branch),
                        expected_main_snapshot_id: request.expected_main_snapshot_id,
                        staging_snapshot_id: None,
                        published_snapshot_id: None,
                        target_snapshots: request.base_snapshots,
                        base_table_uuids: BTreeMap::new(),
                        rows: None,
                        marker: Some(RefreshCommitMarker {
                            refresh_id,
                            mv_id: request.mv_id,
                            token: request.marker_token,
                        }),
                        external_outcome: None,
                        lifecycle_owner: MvRefreshLifecycleOwner::FrontendCurrent,
                        frontend_ledger: Some(request.ledger),
                    };
                    put_definition_transaction(
                        transaction,
                        operation_id,
                        &definition,
                        Precondition::Version(definition_record.version),
                    )
                    .await?;
                    put_refresh_transaction(
                        transaction,
                        operation_id,
                        &refresh,
                        Precondition::Absent,
                    )
                    .await?;
                    Ok(refresh)
                })
            },
        )
        .await
    }

    pub async fn reserve_frontend_refresh_id_async(&self) -> Result<i64, MvRepositoryError> {
        let operation_id = Uuid::now_v7();
        let page_size = self.store.limits().max_page_size;
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "reserve frontend-owned MV refresh ID",
            move |transaction| {
                Box::pin(
                    async move { allocate_refresh_id(transaction, operation_id, page_size).await },
                )
            },
        )
        .await
    }

    pub async fn record_frontend_refresh_action_async(
        &self,
        refresh_id: i64,
        action: FrontendMvRefreshAction,
    ) -> Result<(), MvRepositoryError> {
        validate_frontend_refresh_action(&action)?;
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "record frontend-owned MV refresh action",
            move |transaction| {
                let action = action.clone();
                Box::pin(async move {
                    let (record, mut refresh) =
                        load_refresh_transaction(transaction, refresh_id).await?;
                    if refresh.lifecycle_owner != MvRefreshLifecycleOwner::FrontendCurrent {
                        return Err(conflict_state_store(format!(
                            "mv refresh {refresh_id} is not frontend-owned"
                        )));
                    }
                    let ledger = refresh.frontend_ledger.as_mut().ok_or_else(|| {
                        invalid_state_store("frontend-owned MV refresh has no v3 ledger")
                    })?;
                    let expected_operation_id = frontend_action_operation_id(ledger, &action.phase);
                    if action.operation_id.as_slice() != expected_operation_id {
                        return Err(conflict_state_store(format!(
                            "mv refresh {refresh_id} action does not use its preallocated operation ID"
                        )));
                    }
                    let Some(existing_index) = ledger
                        .actions
                        .iter()
                        .position(|existing| existing.phase == action.phase)
                    else {
                        return Err(invalid_state_store(format!(
                            "frontend-owned mv refresh {refresh_id} is missing a prepared {:?} action",
                            action.phase
                        )));
                    };
                    let existing = &ledger.actions[existing_index];
                    if existing == &action {
                        return Ok(());
                    }
                    if ledger.actions.iter().any(|existing| {
                        existing.state == FrontendMvRefreshActionState::CommitUnknown
                    }) {
                        return Err(conflict_state_store(format!(
                            "mv refresh {refresh_id} has a commit-unknown action and cannot advance"
                        )));
                    }
                    if existing.state != FrontendMvRefreshActionState::Prepared {
                        return Err(conflict_state_store(format!(
                            "mv refresh {refresh_id} action {:?} conflicts with persisted outcome",
                            action.phase
                        )));
                    }
                    if action.state == FrontendMvRefreshActionState::Prepared {
                        return Err(conflict_state_store(format!(
                            "mv refresh {refresh_id} action {:?} cannot replace its prepared intent",
                            action.phase
                        )));
                    }
                    ensure_frontend_action_prerequisites(ledger, &action)
                        .map_err(invalid_state_store)?;
                    ledger.actions[existing_index] = action.clone();
                    match (&action.phase, &action.state) {
                        (_, FrontendMvRefreshActionState::CommitUnknown)
                            if refresh.state != MvRefreshState::PublishCommitted =>
                        {
                            refresh.state = MvRefreshState::CommitUnknown;
                        }
                        (_, FrontendMvRefreshActionState::CommitUnknown) => {
                            // Main is already known published. A later cleanup uncertainty must
                            // retain that truth and force a visible finalization failure instead
                            // of reopening the publication decision.
                            ledger.cleanup_pending = true;
                        }
                        (
                            FrontendMvRefreshActionPhase::Write,
                            FrontendMvRefreshActionState::KnownCommitted,
                        ) => refresh.state = MvRefreshState::StagingCommitted,
                        (
                            FrontendMvRefreshActionPhase::Publication,
                            FrontendMvRefreshActionState::KnownCommitted,
                        ) => {
                            refresh.state = MvRefreshState::PublishCommitted;
                            // The provider-neutral committed version carries
                            // the optional typed snapshot fact needed by the
                            // repository's atomic finalize check.  Its opaque
                            // payload remains only in the v3 ledger.
                            refresh.published_snapshot_id = action
                                .committed_version
                                .as_ref()
                                .and_then(|version| version.snapshot_id);
                            refresh.external_outcome = Some(RefreshExternalOutcome {
                                target_snapshot_id: refresh.published_snapshot_id,
                                commit_id: "frontend-v3-publication".to_string(),
                            });
                            ledger.cleanup_pending = true;
                        }
                        (
                            FrontendMvRefreshActionPhase::StagingDrop,
                            FrontendMvRefreshActionState::KnownCommitted,
                        ) => ledger.cleanup_pending = false,
                        _ => {}
                    }
                    ledger.validate().map_err(invalid_state_store)?;
                    put_refresh_transaction(
                        transaction,
                        operation_id,
                        &refresh,
                        Precondition::Version(record.version),
                    )
                    .await
                })
            },
        )
        .await
    }

    async fn record_staging_commit_async(
        &self,
        request: RecordStagingCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        self.require_refresh_async(request.refresh_id).await?;
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "record MV staging commit",
            move |transaction| {
                let request = request.clone();
                Box::pin(async move {
                    let (record, mut refresh) =
                        load_refresh_transaction(transaction, request.refresh_id).await?;
                    if refresh.state == MvRefreshState::StagingCommitted {
                        if refresh.staging_snapshot_id == Some(request.staging_snapshot_id)
                            && refresh.rows == Some(request.rows)
                            && refresh.base_table_uuids == request.base_table_uuids
                        {
                            return Ok(());
                        }
                        return Err(conflict_state_store(format!(
                            "mv refresh {} staging commit differs from recorded value",
                            request.refresh_id
                        )));
                    }
                    expect_refresh_state(&refresh, MvRefreshState::IntentCreated)?;
                    refresh.state = MvRefreshState::StagingCommitted;
                    refresh.staging_snapshot_id = Some(request.staging_snapshot_id);
                    refresh.rows = Some(request.rows);
                    refresh.base_table_uuids = request.base_table_uuids;
                    put_refresh_transaction(
                        transaction,
                        operation_id,
                        &refresh,
                        Precondition::Version(record.version),
                    )
                    .await
                })
            },
        )
        .await
    }

    async fn record_publish_commit_async(
        &self,
        request: RecordPublishCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        self.require_refresh_async(request.refresh_id).await?;
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "record MV publish commit",
            move |transaction| {
                let request = request.clone();
                Box::pin(async move {
                    let (record, mut refresh) =
                        load_refresh_transaction(transaction, request.refresh_id).await?;
                    if refresh.state == MvRefreshState::PublishCommitted {
                        if refresh.published_snapshot_id == Some(request.published_snapshot_id)
                            && persisted_publish_target_snapshot(&refresh)
                                == Some(request.published_snapshot_id)
                        {
                            return Ok(());
                        }
                        return Err(conflict_state_store(format!(
                            "mv refresh {} publish commit differs from recorded value",
                            request.refresh_id
                        )));
                    }
                    expect_refresh_state(&refresh, MvRefreshState::StagingCommitted)?;
                    refresh.state = MvRefreshState::PublishCommitted;
                    refresh.published_snapshot_id = Some(request.published_snapshot_id);
                    refresh.external_outcome = Some(RefreshExternalOutcome {
                        target_snapshot_id: Some(request.published_snapshot_id),
                        commit_id: format!("iceberg-snapshot-{}", request.published_snapshot_id),
                    });
                    put_refresh_transaction(
                        transaction,
                        operation_id,
                        &refresh,
                        Precondition::Version(record.version),
                    )
                    .await
                })
            },
        )
        .await
    }

    async fn record_external_commit_outcome_async(
        &self,
        refresh_id: i64,
        outcome: RefreshExternalOutcome,
    ) -> Result<(), MvRepositoryError> {
        self.require_refresh_async(refresh_id).await?;
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "record MV external commit",
            move |transaction| {
                let outcome = outcome.clone();
                Box::pin(async move {
                    let (record, mut refresh) =
                        load_refresh_transaction(transaction, refresh_id).await?;
                    expect_refresh_state(&refresh, MvRefreshState::IntentCreated)?;
                    refresh.state = MvRefreshState::PublishCommitted;
                    refresh.published_snapshot_id = outcome.target_snapshot_id;
                    refresh.external_outcome = Some(outcome);
                    put_refresh_transaction(
                        transaction,
                        operation_id,
                        &refresh,
                        Precondition::Version(record.version),
                    )
                    .await
                })
            },
        )
        .await
    }

    async fn mark_refresh_commit_unknown_async(
        &self,
        refresh_id: i64,
    ) -> Result<(), MvRepositoryError> {
        self.require_refresh_async(refresh_id).await?;
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "mark MV refresh commit unknown",
            move |transaction| {
                Box::pin(async move {
                    let (record, mut refresh) =
                        load_refresh_transaction(transaction, refresh_id).await?;
                    if matches!(
                        refresh.state,
                        MvRefreshState::Finalized | MvRefreshState::Aborted
                    ) {
                        return Ok(());
                    }
                    refresh.state = MvRefreshState::CommitUnknown;
                    put_refresh_transaction(
                        transaction,
                        operation_id,
                        &refresh,
                        Precondition::Version(record.version),
                    )
                    .await
                })
            },
        )
        .await
    }

    async fn finalize_refresh_async(
        &self,
        request: MvRefreshFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        self.require_refresh_async(request.refresh_id).await?;
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "finalize MV refresh",
            move |transaction| {
                let request = request.clone();
                Box::pin(async move {
                    finalize_refresh_transaction(transaction, operation_id, request).await
                })
            },
        )
        .await
    }

    async fn finalize_frontend_refresh_without_external_actions_async(
        &self,
        request: MvRefreshFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        self.require_refresh_async(request.refresh_id).await?;
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "finalize frontend MV refresh without external actions",
            move |transaction| {
                let request = request.clone();
                Box::pin(async move {
                    finalize_frontend_refresh_without_external_actions_transaction(
                        transaction,
                        operation_id,
                        request,
                    )
                    .await
                })
            },
        )
        .await
    }

    async fn finalize_refresh_with_partitions_async(
        &self,
        request: FinalizeMvRefreshWithPartitionsRequest,
    ) -> Result<(), MvRepositoryError> {
        self.require_refresh_async(request.refresh.refresh_id)
            .await?;
        if let Some(partitions) = &request.partitions {
            self.require_definition_async(partitions.mv_id).await?;
        }
        let operation_id = Uuid::now_v7();
        let existing_partitions = match request.partitions.as_ref() {
            Some(partitions) => {
                self.list_partition_state_records_async(partitions.mv_id)
                    .await?
            }
            None => Vec::new(),
        };
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "finalize MV refresh with partitions",
            move |transaction| {
                let request = request.clone();
                let existing_partitions = existing_partitions.clone();
                Box::pin(async move {
                    finalize_refresh_transaction(transaction, operation_id, request.refresh)
                        .await?;
                    if let Some(partitions) = request.partitions {
                        replace_partition_states_transaction(
                            transaction,
                            operation_id,
                            partitions,
                            &existing_partitions,
                        )
                        .await?;
                    }
                    Ok(())
                })
            },
        )
        .await
    }

    async fn record_external_commit_and_finalize_async(
        &self,
        request: RecordExternalCommitAndFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        self.require_refresh_async(request.refresh_id).await?;
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "record MV external commit and finalize",
            move |transaction| {
                let request = request.clone();
                Box::pin(async move {
                    let (record, mut refresh) =
                        load_refresh_transaction(transaction, request.refresh_id).await?;
                    expect_refresh_state(&refresh, MvRefreshState::IntentCreated)?;
                    refresh.state = MvRefreshState::PublishCommitted;
                    refresh.published_snapshot_id = request.external_outcome.target_snapshot_id;
                    refresh.external_outcome = Some(request.external_outcome);
                    put_refresh_transaction(
                        transaction,
                        operation_id,
                        &refresh,
                        Precondition::Version(record.version),
                    )
                    .await?;
                    finalize_refresh_transaction(transaction, operation_id, request.finalize).await
                })
            },
        )
        .await
    }

    async fn clear_refresh_progress_async(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "clear MV refresh progress",
            move |transaction| {
                Box::pin(async move {
                    let definition_key =
                        definition_by_id_key(mv_id).map_err(invalid_state_store)?;
                    let Some(definition_record) = transaction.get(&definition_key).await? else {
                        return Ok(false);
                    };
                    let mut definition =
                        decode_definition(&definition_key, &definition_record.value)
                            .map_err(invalid_state_store)?
                            .value;
                    if !definition.refresh_in_progress && definition.active_refresh_id.is_none() {
                        return Ok(true);
                    }
                    if let Some(refresh_id) = definition.active_refresh_id {
                        let (refresh_record, mut refresh) =
                            load_refresh_transaction(transaction, refresh_id).await?;
                        if refresh.state == MvRefreshState::CommitUnknown {
                            return Err(conflict_state_store(format!(
                                "mv definition {} active refresh {} is commit-unknown",
                                definition.mv_id, refresh_id
                            )));
                        }
                        if !matches!(
                            refresh.state,
                            MvRefreshState::Finalized | MvRefreshState::Aborted
                        ) {
                            refresh.state = MvRefreshState::Aborted;
                            put_refresh_transaction(
                                transaction,
                                operation_id,
                                &refresh,
                                Precondition::Version(refresh_record.version),
                            )
                            .await?;
                        }
                    }
                    definition.refresh_in_progress = false;
                    definition.active_refresh_id = None;
                    definition.refresh_target_snapshots.clear();
                    put_definition_transaction(
                        transaction,
                        operation_id,
                        &definition,
                        Precondition::Version(definition_record.version),
                    )
                    .await?;
                    Ok(true)
                })
            },
        )
        .await
    }

    async fn load_refresh_async(
        &self,
        refresh_id: i64,
    ) -> Result<Option<StoredMvRefresh>, MvRepositoryError> {
        if refresh_id <= 0 {
            return Err(invalid("mv refresh id must be positive"));
        }
        let key = refresh_by_id_key(refresh_id).map_err(invalid)?;
        self.read_record(&key)
            .await?
            .map(|record| {
                decode_record(&key, &record.value)
                    .map(|decoded: DecodedMvRecord<StoredMvRefresh>| decoded.value)
                    .map_err(corruption)
            })
            .transpose()
    }

    async fn list_refreshes_async(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        let mut refreshes = self
            .scan_prefix(refresh_prefix().map_err(corruption)?)
            .await?
            .into_iter()
            .map(|record| {
                decode_record(&record.key, &record.value)
                    .map(|decoded: DecodedMvRecord<StoredMvRefresh>| decoded.value)
                    .map_err(corruption)
            })
            .collect::<Result<Vec<_>, _>>()?;
        refreshes.sort_by_key(|refresh| refresh.refresh_id);
        Ok(refreshes)
    }

    async fn list_unfinished_refreshes_async(
        &self,
    ) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        let mut refreshes = self.list_refreshes_async().await?;
        refreshes.retain(|refresh| {
            refresh.lifecycle_owner == MvRefreshLifecycleOwner::LegacyCore
                && !matches!(
                    refresh.state,
                    MvRefreshState::Finalized | MvRefreshState::Aborted
                )
        });
        refreshes.sort_by_key(|refresh| refresh.refresh_id);
        Ok(refreshes)
    }

    async fn update_starrocks_refresh_summary_if_present_async(
        &self,
        request: UpdateStarRocksMvRefreshSummaryRequest,
    ) -> Result<bool, MvRepositoryError> {
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "update StarRocks MV refresh summary",
            move |transaction| {
                let request = request.clone();
                Box::pin(async move {
                    let key = definition_by_id_key(request.mv_id).map_err(invalid_state_store)?;
                    let Some(record) = transaction.get(&key).await? else {
                        return Ok(false);
                    };
                    let mut definition = decode_definition(&key, &record.value)
                        .map_err(invalid_state_store)?
                        .value;
                    if let Some(refresh_id) = definition.active_refresh_id {
                        let (refresh_record, mut refresh) =
                            load_refresh_transaction(transaction, refresh_id).await?;
                        if refresh.state == MvRefreshState::CommitUnknown {
                            return Err(conflict_state_store(format!(
                                "mv definition {} active refresh {} is commit-unknown",
                                definition.mv_id, refresh_id
                            )));
                        }
                        refresh.state = MvRefreshState::Finalized;
                        put_refresh_transaction(
                            transaction,
                            operation_id,
                            &refresh,
                            Precondition::Version(refresh_record.version),
                        )
                        .await?;
                    }
                    definition.last_refresh_ms = Some(request.last_refresh_ms);
                    definition.last_refresh_rows = Some(request.last_refresh_rows);
                    definition.last_refresh_snapshots = request.base_snapshots;
                    definition.last_refresh_table_uuids = request.base_table_uuids;
                    definition.refresh_in_progress = false;
                    definition.active_refresh_id = None;
                    definition.refresh_target_snapshots.clear();
                    put_definition_transaction(
                        transaction,
                        operation_id,
                        &definition,
                        Precondition::Version(record.version),
                    )
                    .await?;
                    Ok(true)
                })
            },
        )
        .await
    }

    async fn replace_partition_states_async(
        &self,
        request: ReplaceMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        self.require_definition_async(request.mv_id).await?;
        let operation_id = Uuid::now_v7();
        let existing_partitions = self
            .list_partition_state_records_async(request.mv_id)
            .await?;
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "replace MV partition states",
            move |transaction| {
                let request = request.clone();
                let existing_partitions = existing_partitions.clone();
                Box::pin(async move {
                    replace_partition_states_transaction(
                        transaction,
                        operation_id,
                        request,
                        &existing_partitions,
                    )
                    .await
                })
            },
        )
        .await
    }

    async fn record_failed_partition_states_async(
        &self,
        request: RecordFailedMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        self.require_definition_async(request.mv_id).await?;
        let operation_id = Uuid::now_v7();
        let existing_partitions = self
            .list_partition_state_records_async(request.mv_id)
            .await?;
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "record failed MV partition states",
            move |transaction| {
                let request = request.clone();
                let existing_partitions = existing_partitions.clone();
                Box::pin(async move {
                    record_failed_partition_states_transaction(
                        transaction,
                        operation_id,
                        request,
                        &existing_partitions,
                    )
                    .await
                })
            },
        )
        .await
    }

    async fn clear_partition_states_async(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        let operation_id = Uuid::now_v7();
        let existing_partitions = self.list_partition_state_records_async(mv_id).await?;
        let store = Arc::clone(&self.store);
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "clear MV partition states",
            move |transaction| {
                let existing_partitions = existing_partitions.clone();
                Box::pin(async move {
                    let key = definition_by_id_key(mv_id).map_err(invalid_state_store)?;
                    let Some(record) = transaction.get(&key).await? else {
                        return Ok(false);
                    };
                    let mut definition = decode_definition(&key, &record.value)
                        .map_err(invalid_state_store)?
                        .value;
                    delete_partition_states_transaction(transaction, &existing_partitions).await?;
                    if definition.partition_state_complete {
                        definition.partition_state_complete = false;
                        put_definition_transaction(
                            transaction,
                            operation_id,
                            &definition,
                            Precondition::Version(record.version),
                        )
                        .await?;
                    }
                    Ok(true)
                })
            },
        )
        .await
    }

    async fn list_partition_states_async(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StoredMvPartitionState>, MvRepositoryError> {
        let mut states = self
            .list_partition_state_records_async(mv_id)
            .await?
            .into_iter()
            .map(|record| {
                decode_record(&record.key, &record.value)
                    .map(|decoded: DecodedMvRecord<StoredMvPartitionState>| decoded.value)
                    .map_err(corruption)
            })
            .collect::<Result<Vec<_>, _>>()?;
        states.sort_by(|left, right| left.partition_key.cmp(&right.partition_key));
        Ok(states)
    }

    async fn list_partition_state_records_async(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StateRecord>, MvRepositoryError> {
        if mv_id <= 0 {
            return Err(invalid("mv definition id must be positive"));
        }
        self.scan_prefix(partition_by_mv_prefix(mv_id).map_err(invalid)?)
            .await
    }

    async fn adopt_target_compaction_snapshot_async(
        &self,
        target: &MvTarget,
        expected_snapshot_id: i64,
        adopted_snapshot_id: i64,
    ) -> Result<bool, MvRepositoryError> {
        let operation_id = Uuid::now_v7();
        let store = Arc::clone(&self.store);
        let target = target.clone();
        operation::run(
            store.as_ref(),
            &self.runner_metrics,
            operation_id,
            "adopt MV target compaction snapshot",
            move |transaction| {
                let target = target.clone();
                Box::pin(async move {
                    let target_key = target_lookup_key(
                        &target.catalog.clone().unwrap_or_default(),
                        &target.database,
                        &target.name,
                    )
                    .map_err(invalid_state_store)?;
                    let Some(lookup_record) = transaction.get(&target_key).await? else {
                        return Ok(false);
                    };
                    let lookup: DecodedMvRecord<MvTargetLookup> =
                        decode_record(&target_key, &lookup_record.value)
                            .map_err(invalid_state_store)?;
                    let (definition_record, mut definition) =
                        load_definition_transaction(transaction, lookup.value.mv_id).await?;
                    if definition_target(&definition)
                        .map_err(|_| invalid_state_store("invalid MV target"))?
                        .as_ref()
                        != Some(&target)
                    {
                        return Err(invalid_state_store(
                            "MV target lookup does not match its definition",
                        ));
                    }
                    if definition.refresh_in_progress
                        || definition.active_refresh_id.is_some()
                        || definition.last_refreshed_iceberg_snapshot_id
                            != Some(expected_snapshot_id)
                    {
                        return Ok(false);
                    }
                    if definition.last_refreshed_iceberg_snapshot_id == Some(adopted_snapshot_id) {
                        return Ok(true);
                    }
                    definition.last_refreshed_iceberg_snapshot_id = Some(adopted_snapshot_id);
                    put_definition_transaction(
                        transaction,
                        operation_id,
                        &definition,
                        Precondition::Version(definition_record.version),
                    )
                    .await?;
                    Ok(true)
                })
            },
        )
        .await
    }
}

impl MvRepository for StateStoreMvRepository {
    fn availability(&self) -> MvRepositoryAvailability {
        MvRepositoryAvailability::Available
    }
    fn create(
        &self,
        operation_id: Uuid,
        request: CreateMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.blocking(self.create_async(operation_id, request))
    }
    fn create_with_id(
        &self,
        operation_id: Uuid,
        request: CreateMvRepositoryWithIdRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.blocking(self.create_with_optional_id_async(
            operation_id,
            Some(request.mv_id),
            request.create,
        ))
    }
    fn rebuild(
        &self,
        operation_id: Uuid,
        request: RebuildMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.blocking(self.create_async(operation_id, request.create))
    }
    fn reserve_definition_id(&self, mv_id: i64) -> Result<(), MvRepositoryError> {
        self.blocking(self.reserve_definition_id_async(mv_id))
    }
    fn load_by_id(&self, mv_id: i64) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        self.blocking(self.load_by_id_async(mv_id))
    }
    fn find_by_target(
        &self,
        target: &MvTarget,
    ) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        self.blocking(self.find_by_target_async(target))
    }
    fn list_definitions(&self) -> Result<Vec<StoredMvDefinition>, MvRepositoryError> {
        self.blocking(self.list_definitions_async())
    }
    fn drop_by_id(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        self.blocking(self.drop_by_id_async(mv_id))
    }
    fn drop_by_target(&self, target: &MvTarget) -> Result<bool, MvRepositoryError> {
        match self.find_by_target(target)? {
            Some(definition) => self.drop_by_id(definition.mv_id),
            None => Ok(false),
        }
    }
    fn replace_dependencies_for_mv(
        &self,
        mv_id: i64,
        dependencies: Vec<CreateMvDependencyRequest>,
    ) -> Result<(), MvRepositoryError> {
        self.blocking(self.replace_dependencies_async(mv_id, dependencies))
    }
    fn delete_dependencies_for_mv(&self, mv_id: i64) -> Result<(), MvRepositoryError> {
        self.blocking(self.delete_dependencies_async(mv_id))
    }
    fn ensure_no_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        let dependencies = self.blocking(self.list_dependencies_upstream_async(upstream))?;
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
    fn list_dependencies_by_downstream(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.blocking(async {
            Ok(self
                .list_dependencies_downstream_async(mv_id)
                .await?
                .into_iter()
                .map(|(_, dependency)| dependency)
                .collect())
        })
    }
    fn list_downstream_dependencies(
        &self,
        upstream: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        self.blocking(self.list_dependencies_upstream_async(upstream))
    }
    fn set_rebuilt_refresh_watermark(
        &self,
        mv_id: i64,
        base_snapshots: BTreeMap<String, i64>,
        base_table_uuids: BTreeMap<String, String>,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.blocking(self.set_rebuilt_refresh_watermark_async(
            mv_id,
            base_snapshots,
            base_table_uuids,
        ))
    }
    fn update_refresh_metadata(
        &self,
        request: UpdateMvRefreshMetadataRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.blocking(self.update_refresh_metadata_async(request))
    }
    fn update_partition_contract(
        &self,
        request: UpdateMvPartitionContractRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.blocking(self.update_partition_contract_async(request))
    }
    fn begin_refresh_intent(
        &self,
        mv_id: i64,
        target_snapshots: BTreeMap<String, i64>,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        self.blocking(self.begin_refresh_intent_async(mv_id, target_snapshots))
    }
    fn begin_iceberg_refresh_intent(
        &self,
        request: BeginIcebergMvRefreshRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        self.blocking(self.begin_iceberg_refresh_intent_async(request))
    }
    fn begin_frontend_refresh_intent(
        &self,
        request: BeginFrontendMvRefreshIntentRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        self.blocking(self.begin_frontend_refresh_intent_async(request))
    }
    fn reserve_frontend_refresh_id(&self) -> Result<i64, MvRepositoryError> {
        self.blocking(self.reserve_frontend_refresh_id_async())
    }
    fn record_frontend_refresh_action(
        &self,
        refresh_id: i64,
        action: FrontendMvRefreshAction,
    ) -> Result<(), MvRepositoryError> {
        self.blocking(self.record_frontend_refresh_action_async(refresh_id, action))
    }
    fn record_staging_commit(
        &self,
        request: RecordStagingCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        self.blocking(self.record_staging_commit_async(request))
    }
    fn record_publish_commit(
        &self,
        request: RecordPublishCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        self.blocking(self.record_publish_commit_async(request))
    }
    fn mark_refresh_commit_unknown(&self, refresh_id: i64) -> Result<(), MvRepositoryError> {
        self.blocking(self.mark_refresh_commit_unknown_async(refresh_id))
    }
    fn record_external_commit_outcome(
        &self,
        refresh_id: i64,
        outcome: RefreshExternalOutcome,
    ) -> Result<(), MvRepositoryError> {
        self.blocking(self.record_external_commit_outcome_async(refresh_id, outcome))
    }
    fn finalize_refresh(&self, request: MvRefreshFinalizeRequest) -> Result<(), MvRepositoryError> {
        self.blocking(self.finalize_refresh_async(request))
    }
    fn finalize_frontend_refresh_without_external_actions(
        &self,
        request: MvRefreshFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        self.blocking(self.finalize_frontend_refresh_without_external_actions_async(request))
    }
    fn finalize_refresh_with_partitions(
        &self,
        request: FinalizeMvRefreshWithPartitionsRequest,
    ) -> Result<(), MvRepositoryError> {
        self.blocking(self.finalize_refresh_with_partitions_async(request))
    }
    fn record_external_commit_and_finalize(
        &self,
        request: RecordExternalCommitAndFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        self.blocking(self.record_external_commit_and_finalize_async(request))
    }
    fn clear_refresh_progress(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        self.blocking(self.clear_refresh_progress_async(mv_id))
    }
    fn load_refresh(&self, refresh_id: i64) -> Result<Option<StoredMvRefresh>, MvRepositoryError> {
        self.blocking(self.load_refresh_async(refresh_id))
    }
    fn list_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        self.blocking(self.list_refreshes_async())
    }
    fn list_unfinished_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        self.blocking(self.list_unfinished_refreshes_async())
    }
    fn list_unfinished_branch_staged_iceberg_refreshes(
        &self,
    ) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        self.blocking(async {
            Ok(self
                .list_unfinished_refreshes_async()
                .await?
                .into_iter()
                .filter(|refresh| {
                    refresh.lifecycle_owner == MvRefreshLifecycleOwner::LegacyCore
                        && refresh.target_catalog.is_some()
                        && refresh.target_namespace.is_some()
                        && refresh.target_table.is_some()
                        && refresh.staging_branch.is_some()
                        && refresh.marker.is_some()
                })
                .collect())
        })
    }
    fn update_starrocks_refresh_summary_if_present(
        &self,
        request: UpdateStarRocksMvRefreshSummaryRequest,
    ) -> Result<bool, MvRepositoryError> {
        self.blocking(self.update_starrocks_refresh_summary_if_present_async(request))
    }
    fn replace_partition_states(
        &self,
        request: ReplaceMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        self.blocking(self.replace_partition_states_async(request))
    }
    fn record_failed_partition_states(
        &self,
        request: RecordFailedMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        self.blocking(self.record_failed_partition_states_async(request))
    }
    fn clear_partition_states(&self, mv_id: i64) -> Result<bool, MvRepositoryError> {
        self.blocking(self.clear_partition_states_async(mv_id))
    }
    fn list_partition_states(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StoredMvPartitionState>, MvRepositoryError> {
        self.blocking(self.list_partition_states_async(mv_id))
    }
    fn adopt_target_compaction_snapshot(
        &self,
        target: &MvTarget,
        expected_snapshot_id: i64,
        adopted_snapshot_id: i64,
    ) -> Result<bool, MvRepositoryError> {
        self.blocking(self.adopt_target_compaction_snapshot_async(
            target,
            expected_snapshot_id,
            adopted_snapshot_id,
        ))
    }
}

fn definition_from_request(mv_id: i64, request: &CreateMvRepositoryRequest) -> StoredMvDefinition {
    StoredMvDefinition {
        mv_id,
        select_sql: request.definition.select_sql.clone(),
        base_table_refs: request.definition.base_table_refs.clone(),
        primary_key_columns: request.definition.primary_key_columns.clone(),
        storage_engine: request.definition.storage_engine.clone(),
        target_catalog: request.definition.target_catalog.clone(),
        target_namespace: request.definition.target_namespace.clone(),
        target_table: request.definition.target_table.clone(),
        schema_contract: request.definition.schema_contract.clone(),
        partition_spec: request.definition.partition_spec.clone(),
        partition_state_complete: false,
        last_refresh_ms: None,
        last_refresh_rows: None,
        last_refresh_snapshots: BTreeMap::new(),
        last_refresh_table_uuids: BTreeMap::new(),
        last_refreshed_iceberg_snapshot_id: None,
        refresh_in_progress: false,
        active_refresh_id: None,
        refresh_target_snapshots: BTreeMap::new(),
        refresh_policy: request.refresh.policy.clone(),
        refresh_paused: request.refresh.paused,
        refresh_interval_ms: request.refresh.interval_ms,
        max_staleness_ms: request.refresh.max_staleness_ms,
        last_scheduler_error: None,
        next_refresh_after_ms: request.refresh.next_refresh_after_ms,
        created_at_ms: request.definition.created_at_ms,
    }
}

fn definition_matches_request(
    definition: &StoredMvDefinition,
    request: &CreateMvRepositoryRequest,
) -> bool {
    definition.select_sql == request.definition.select_sql
        && definition.base_table_refs == request.definition.base_table_refs
        && definition.primary_key_columns == request.definition.primary_key_columns
        && definition.storage_engine == request.definition.storage_engine
        && definition.target_catalog == request.definition.target_catalog
        && definition.target_namespace == request.definition.target_namespace
        && definition.target_table == request.definition.target_table
        && definition.schema_contract == request.definition.schema_contract
        && definition.partition_spec == request.definition.partition_spec
        && definition.created_at_ms == request.definition.created_at_ms
        && definition.refresh_policy == request.refresh.policy
        && definition.refresh_paused == request.refresh.paused
        && definition.refresh_interval_ms == request.refresh.interval_ms
        && definition.max_staleness_ms == request.refresh.max_staleness_ms
        && definition.next_refresh_after_ms == request.refresh.next_refresh_after_ms
}

fn validate_create_request(request: &CreateMvRepositoryRequest) -> Result<(), MvRepositoryError> {
    let target_fields = [
        &request.definition.target_catalog,
        &request.definition.target_namespace,
        &request.definition.target_table,
    ];
    if target_fields.iter().any(|field| field.is_some())
        && target_fields.iter().any(|field| field.is_none())
    {
        return Err(invalid(
            "MV definition target catalog, namespace, and table must be set together",
        ));
    }
    Ok(())
}

/// Validate every possible CREATE key and envelope before the first write
/// transaction. IDs are fixed-width keys and Avro longs are bounded by the
/// explicit maximum, so `i64::MAX` is the conservative encoding probe for an
/// automatically allocated ID as well as an explicit ID.
fn prevalidate_create_operation(
    operation_id: Uuid,
    explicit_id: Option<i64>,
    request: &CreateMvRepositoryRequest,
) -> Result<(), MvRepositoryError> {
    let mv_id = explicit_id.unwrap_or(i64::MAX);
    if mv_id <= 0 {
        return Err(invalid("MV definition ID must be positive"));
    }
    let definition = definition_from_request(mv_id, request);
    let _ = sequence_key().map_err(invalid)?;
    let _ = encode_record(
        MvRecordKind::Sequence,
        operation_id,
        &MvSequence {
            last_allocated_id: mv_id,
            last_refresh_id: 0,
        },
    )
    .map_err(invalid)?;
    let _ = definition_by_id_key(mv_id).map_err(invalid)?;
    let _ = encode_definition(operation_id, &definition).map_err(invalid)?;
    if let Some(target) = definition_target(&definition)? {
        let _ = target_lookup_key(
            &target.catalog.unwrap_or_default(),
            &target.database,
            &target.name,
        )
        .map_err(invalid)?;
        let _ = encode_record(
            MvRecordKind::TargetLookup,
            operation_id,
            &MvTargetLookup { mv_id },
        )
        .map_err(invalid)?;
    }
    for dependency in deduplicate_dependencies(mv_id, &request.dependencies).map_err(invalid)? {
        let _ = dependency_by_downstream_key(mv_id, &dependency.upstream).map_err(invalid)?;
        let _ = dependency_by_upstream_key(&dependency.upstream, mv_id).map_err(invalid)?;
        let _ =
            encode_record(MvRecordKind::Dependency, operation_id, &dependency).map_err(invalid)?;
    }
    Ok(())
}

fn definition_target(
    definition: &StoredMvDefinition,
) -> Result<Option<MvTarget>, MvRepositoryError> {
    match (
        &definition.target_catalog,
        &definition.target_namespace,
        &definition.target_table,
    ) {
        (None, None, None) => Ok(None),
        (Some(catalog), Some(database), Some(name)) => Ok(Some(MvTarget {
            catalog: Some(catalog.clone()),
            database: database.clone(),
            name: name.clone(),
        })),
        _ => Err(corruption("MV definition has a partial target identity")),
    }
}

fn deduplicate_dependencies(
    mv_id: i64,
    requests: &[CreateMvDependencyRequest],
) -> Result<Vec<StoredMvDependency>, String> {
    let mut seen = BTreeSet::new();
    let mut dependencies = Vec::new();
    for request in requests {
        let key = dependency_by_downstream_key(mv_id, &request.upstream)?;
        if seen.insert(key) {
            dependencies.push(StoredMvDependency {
                downstream_mv_id: mv_id,
                upstream: request.upstream.clone(),
                created_at_ms: request.created_at_ms,
            });
        }
    }
    Ok(dependencies)
}

async fn put_dependency(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    dependency: &StoredMvDependency,
    precondition: Precondition,
) -> Result<(), novarocks_spi::state_store::StateStoreError> {
    let payload = encode_record(MvRecordKind::Dependency, operation_id, dependency)
        .map_err(invalid_state_store)?;
    let downstream =
        dependency_by_downstream_key(dependency.downstream_mv_id, &dependency.upstream)
            .map_err(invalid_state_store)?;
    let upstream = dependency_by_upstream_key(&dependency.upstream, dependency.downstream_mv_id)
        .map_err(invalid_state_store)?;
    transaction
        .put(downstream, payload.clone(), precondition.clone())
        .await?;
    transaction.put(upstream, payload, precondition).await
}

async fn range_transaction(
    transaction: &mut dyn WriteTransaction,
    prefix: Key,
    page_size: usize,
) -> Result<Vec<StateRecord>, novarocks_spi::state_store::StateStoreError> {
    let range = KeyRange::for_prefix(prefix)?;
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

async fn load_definition_transaction(
    transaction: &mut dyn WriteTransaction,
    mv_id: i64,
) -> Result<(StateRecord, StoredMvDefinition), novarocks_spi::state_store::StateStoreError> {
    let key = definition_by_id_key(mv_id).map_err(invalid_state_store)?;
    let record = transaction
        .get(&key)
        .await?
        .ok_or_else(|| invalid_state_store(format!("mv definition {mv_id} not found")))?;
    let definition = decode_definition(&key, &record.value)
        .map_err(invalid_state_store)?
        .value;
    Ok((record, definition))
}

async fn load_refresh_transaction(
    transaction: &mut dyn WriteTransaction,
    refresh_id: i64,
) -> Result<(StateRecord, StoredMvRefresh), novarocks_spi::state_store::StateStoreError> {
    let key = refresh_by_id_key(refresh_id).map_err(invalid_state_store)?;
    let record = transaction
        .get(&key)
        .await?
        .ok_or_else(|| invalid_state_store(format!("mv refresh {refresh_id} not found")))?;
    let refresh = decode_record(&key, &record.value)
        .map_err(invalid_state_store)?
        .value;
    Ok((record, refresh))
}

async fn put_definition_transaction(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    definition: &StoredMvDefinition,
    precondition: Precondition,
) -> Result<(), novarocks_spi::state_store::StateStoreError> {
    transaction
        .put(
            definition_by_id_key(definition.mv_id).map_err(invalid_state_store)?,
            encode_definition(operation_id, definition).map_err(invalid_state_store)?,
            precondition,
        )
        .await
}

async fn put_refresh_transaction(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    refresh: &StoredMvRefresh,
    precondition: Precondition,
) -> Result<(), novarocks_spi::state_store::StateStoreError> {
    transaction
        .put(
            refresh_by_id_key(refresh.refresh_id).map_err(invalid_state_store)?,
            encode_record(MvRecordKind::Refresh, operation_id, refresh)
                .map_err(invalid_state_store)?,
            precondition,
        )
        .await
}

async fn put_partition_transaction(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    state: &StoredMvPartitionState,
    precondition: Precondition,
) -> Result<(), novarocks_spi::state_store::StateStoreError> {
    transaction
        .put(
            partition_by_mv_key(state.mv_id, &state.partition_key).map_err(invalid_state_store)?,
            encode_record(MvRecordKind::Partition, operation_id, state)
                .map_err(invalid_state_store)?,
            precondition,
        )
        .await
}

async fn allocate_refresh_id(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    page_size: usize,
) -> Result<i64, novarocks_spi::state_store::StateStoreError> {
    let sequence_key = sequence_key().map_err(invalid_state_store)?;
    let sequence_record = transaction.get(&sequence_key).await?;
    let sequence = match &sequence_record {
        Some(record) => {
            decode_record::<MvSequence>(&sequence_key, &record.value)
                .map_err(invalid_state_store)?
                .value
        }
        None => MvSequence {
            last_allocated_id: 0,
            last_refresh_id: 0,
        },
    };
    let records = range_transaction_with_page_size(
        transaction,
        refresh_prefix().map_err(invalid_state_store)?,
        page_size,
    )
    .await?;
    let existing_max = records
        .into_iter()
        .map(|record| {
            decode_record::<StoredMvRefresh>(&record.key, &record.value)
                .map(|decoded| decoded.value.refresh_id)
                .map_err(invalid_state_store)
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(0);
    let next = sequence
        .last_refresh_id
        .max(existing_max)
        .checked_add(1)
        .filter(|value| *value > 0)
        .ok_or_else(|| invalid_state_store("MV refresh ID sequence overflow"))?;
    let value = encode_record(
        MvRecordKind::Sequence,
        operation_id,
        &MvSequence {
            last_allocated_id: sequence.last_allocated_id,
            last_refresh_id: next,
        },
    )
    .map_err(invalid_state_store)?;
    transaction
        .put(
            sequence_key,
            value,
            sequence_record
                .map(|record| Precondition::Version(record.version))
                .unwrap_or(Precondition::Absent),
        )
        .await?;
    Ok(next)
}

async fn range_transaction_with_page_size(
    transaction: &mut dyn WriteTransaction,
    prefix: Key,
    page_size: usize,
) -> Result<Vec<StateRecord>, novarocks_spi::state_store::StateStoreError> {
    let range = KeyRange::for_prefix(prefix)?;
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

async fn delete_partition_states_transaction(
    transaction: &mut dyn WriteTransaction,
    records: &[StateRecord],
) -> Result<(), novarocks_spi::state_store::StateStoreError> {
    for record in records {
        let current = transaction
            .get(&record.key)
            .await?
            .ok_or_else(|| conflict_state_store("MV partition state changed before mutation"))?;
        if current.version != record.version {
            return Err(conflict_state_store(
                "MV partition state changed before mutation",
            ));
        }
        transaction
            .delete(
                record.key.clone(),
                Precondition::Version(record.version.clone()),
            )
            .await?;
    }
    Ok(())
}

async fn replace_partition_states_transaction(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    request: ReplaceMvPartitionStatesRequest,
    existing: &[StateRecord],
) -> Result<(), novarocks_spi::state_store::StateStoreError> {
    validate_partition_state_limit(request.max_entries)?;
    let (definition_record, mut definition) =
        load_definition_transaction(transaction, request.mv_id).await?;
    delete_partition_states_transaction(transaction, existing).await?;
    if request.partition_keys.len() > request.max_entries {
        definition.partition_state_complete = false;
        return put_definition_transaction(
            transaction,
            operation_id,
            &definition,
            Precondition::Version(definition_record.version),
        )
        .await;
    }
    for partition_key in request.partition_keys {
        let state = StoredMvPartitionState {
            mv_id: request.mv_id,
            partition_key,
            status: MvPartitionRefreshStatus::Fresh,
            last_refresh_ms: Some(request.last_refresh_ms),
            base_snapshots: request.base_snapshots.clone(),
            target_snapshot_id: request.target_snapshot_id,
            last_refresh_id: Some(request.last_refresh_id),
            failure_message: None,
        };
        put_partition_transaction(transaction, operation_id, &state, Precondition::Absent).await?;
    }
    definition.partition_state_complete = true;
    put_definition_transaction(
        transaction,
        operation_id,
        &definition,
        Precondition::Version(definition_record.version),
    )
    .await
}

async fn record_failed_partition_states_transaction(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    request: RecordFailedMvPartitionStatesRequest,
    existing: &[StateRecord],
) -> Result<(), novarocks_spi::state_store::StateStoreError> {
    validate_partition_state_limit(request.max_entries)?;
    let (definition_record, mut definition) =
        load_definition_transaction(transaction, request.mv_id).await?;
    delete_partition_states_transaction(transaction, existing).await?;
    if request.partition_keys.len() > request.max_entries {
        definition.partition_state_complete = false;
        return put_definition_transaction(
            transaction,
            operation_id,
            &definition,
            Precondition::Version(definition_record.version),
        )
        .await;
    }
    for partition_key in request.partition_keys {
        let state = StoredMvPartitionState {
            mv_id: request.mv_id,
            partition_key,
            status: MvPartitionRefreshStatus::Failed,
            last_refresh_ms: Some(request.last_refresh_ms),
            base_snapshots: request.base_snapshots.clone(),
            target_snapshot_id: request.target_snapshot_id,
            last_refresh_id: Some(request.last_refresh_id),
            failure_message: Some(request.failure_message.clone()),
        };
        put_partition_transaction(transaction, operation_id, &state, Precondition::Absent).await?;
    }
    definition.partition_state_complete = true;
    put_definition_transaction(
        transaction,
        operation_id,
        &definition,
        Precondition::Version(definition_record.version),
    )
    .await
}

async fn finalize_refresh_transaction(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    request: MvRefreshFinalizeRequest,
) -> Result<(), novarocks_spi::state_store::StateStoreError> {
    let (refresh_record, mut refresh) =
        load_refresh_transaction(transaction, request.refresh_id).await?;
    if refresh.state == MvRefreshState::Finalized {
        return Ok(());
    }
    expect_refresh_state(&refresh, MvRefreshState::PublishCommitted)?;
    let persisted_snapshot = persisted_publish_target_snapshot(&refresh);
    if persisted_snapshot != request.target_snapshot_id {
        return Err(conflict_state_store(format!(
            "mv refresh {} target snapshot is {:?}, expected published snapshot {:?}",
            request.refresh_id, request.target_snapshot_id, persisted_snapshot
        )));
    }
    let (definition_record, mut definition) =
        load_definition_transaction(transaction, refresh.mv_id).await?;
    if definition.active_refresh_id != Some(request.refresh_id) {
        return Err(conflict_state_store(format!(
            "mv definition {} active refresh is {:?}, expected {}",
            refresh.mv_id, definition.active_refresh_id, request.refresh_id
        )));
    }
    definition.last_refresh_rows = Some(request.rows);
    definition.last_refresh_snapshots = request.base_snapshots;
    definition.last_refresh_table_uuids = request.base_table_uuids;
    definition.last_refreshed_iceberg_snapshot_id = request.target_snapshot_id;
    definition.refresh_in_progress = false;
    definition.active_refresh_id = None;
    definition.refresh_target_snapshots.clear();
    refresh.state = MvRefreshState::Finalized;
    put_definition_transaction(
        transaction,
        operation_id,
        &definition,
        Precondition::Version(definition_record.version),
    )
    .await?;
    put_refresh_transaction(
        transaction,
        operation_id,
        &refresh,
        Precondition::Version(refresh_record.version),
    )
    .await
}

async fn finalize_frontend_refresh_without_external_actions_transaction(
    transaction: &mut dyn WriteTransaction,
    operation_id: Uuid,
    request: MvRefreshFinalizeRequest,
) -> Result<(), novarocks_spi::state_store::StateStoreError> {
    let (refresh_record, mut refresh) =
        load_refresh_transaction(transaction, request.refresh_id).await?;
    if refresh.state == MvRefreshState::Finalized {
        return Ok(());
    }
    if refresh.lifecycle_owner != MvRefreshLifecycleOwner::FrontendCurrent
        || refresh
            .frontend_ledger
            .as_ref()
            .is_none_or(|ledger| !ledger.actions.is_empty())
    {
        return Err(conflict_state_store(format!(
            "mv refresh {} is not a frontend no-external-action attempt",
            request.refresh_id
        )));
    }
    expect_refresh_state(&refresh, MvRefreshState::IntentCreated)?;
    let (definition_record, mut definition) =
        load_definition_transaction(transaction, refresh.mv_id).await?;
    if definition.active_refresh_id != Some(request.refresh_id) {
        return Err(conflict_state_store(format!(
            "mv definition {} active refresh is {:?}, expected {}",
            refresh.mv_id, definition.active_refresh_id, request.refresh_id
        )));
    }
    definition.last_refresh_rows = Some(request.rows);
    definition.last_refresh_snapshots = request.base_snapshots;
    definition.last_refresh_table_uuids = request.base_table_uuids;
    definition.last_refreshed_iceberg_snapshot_id = request.target_snapshot_id;
    definition.refresh_in_progress = false;
    definition.active_refresh_id = None;
    definition.refresh_target_snapshots.clear();
    refresh.state = MvRefreshState::Finalized;
    put_definition_transaction(
        transaction,
        operation_id,
        &definition,
        Precondition::Version(definition_record.version),
    )
    .await?;
    put_refresh_transaction(
        transaction,
        operation_id,
        &refresh,
        Precondition::Version(refresh_record.version),
    )
    .await
}

fn new_refresh(
    refresh_id: i64,
    mv_id: i64,
    operation_id: Option<i64>,
    target_snapshots: BTreeMap<String, i64>,
) -> StoredMvRefresh {
    StoredMvRefresh {
        refresh_id,
        mv_id,
        operation_id,
        state: MvRefreshState::IntentCreated,
        target_catalog: None,
        target_namespace: None,
        target_table: None,
        staging_branch: None,
        expected_main_snapshot_id: None,
        staging_snapshot_id: None,
        published_snapshot_id: None,
        target_snapshots,
        base_table_uuids: BTreeMap::new(),
        rows: None,
        marker: None,
        external_outcome: None,
        lifecycle_owner: MvRefreshLifecycleOwner::LegacyCore,
        frontend_ledger: None,
    }
}

fn expect_refresh_state(
    refresh: &StoredMvRefresh,
    expected: MvRefreshState,
) -> Result<(), novarocks_spi::state_store::StateStoreError> {
    if refresh.state == expected {
        return Ok(());
    }
    Err(conflict_state_store(format!(
        "mv refresh {} is {}, expected {}",
        refresh.refresh_id,
        refresh.state.as_str(),
        expected.as_str()
    )))
}

fn persisted_publish_target_snapshot(refresh: &StoredMvRefresh) -> Option<i64> {
    refresh.published_snapshot_id.or_else(|| {
        refresh
            .external_outcome
            .as_ref()
            .and_then(|outcome| outcome.target_snapshot_id)
    })
}

fn validate_iceberg_refresh_request(
    request: &BeginIcebergMvRefreshRequest,
) -> Result<(), MvRepositoryError> {
    if request.mv_id <= 0
        || request.target_catalog.is_empty()
        || request.target_namespace.is_empty()
        || request.target_table.is_empty()
        || request.staging_branch.is_empty()
        || request.marker_token.is_empty()
    {
        return Err(invalid(
            "Iceberg MV refresh request requires non-empty identifiers and a positive MV ID",
        ));
    }
    Ok(())
}

fn validate_frontend_refresh_request(
    request: &BeginFrontendMvRefreshIntentRequest,
) -> Result<(), MvRepositoryError> {
    if request.refresh_id <= 0
        || request.mv_id <= 0
        || request.target_catalog.is_empty()
        || request.target_namespace.is_empty()
        || request.target_table.is_empty()
        || request.staging_branch.is_empty()
        || request.marker_token.is_empty()
    {
        return Err(invalid(
            "frontend MV refresh request requires non-empty identifiers and positive refresh and MV IDs",
        ));
    }
    if !request.ledger.actions.is_empty() {
        return Err(invalid(
            "frontend MV refresh intent cannot contain completed external actions",
        ));
    }
    request.ledger.validate().map_err(invalid)
}

fn validate_frontend_refresh_action(
    action: &FrontendMvRefreshAction,
) -> Result<(), MvRepositoryError> {
    if action.operation_id.len() != 16 {
        return Err(invalid(
            "frontend MV refresh action operation ID must be a 16-byte UUID",
        ));
    }
    Ok(())
}

fn frontend_action_operation_id<'a>(
    ledger: &'a FrontendMvRefreshLedger,
    phase: &FrontendMvRefreshActionPhase,
) -> &'a [u8] {
    match phase {
        FrontendMvRefreshActionPhase::StagingCreate => &ledger.staging_create_operation_id,
        FrontendMvRefreshActionPhase::Write => &ledger.write_operation_id,
        FrontendMvRefreshActionPhase::Publication => &ledger.publication_operation_id,
        FrontendMvRefreshActionPhase::StagingDrop => &ledger.staging_drop_operation_id,
    }
}

fn frontend_prepared_actions(ledger: &FrontendMvRefreshLedger) -> Vec<FrontendMvRefreshAction> {
    [
        (
            FrontendMvRefreshActionPhase::StagingCreate,
            &ledger.staging_create_operation_id,
        ),
        (
            FrontendMvRefreshActionPhase::Write,
            &ledger.write_operation_id,
        ),
        (
            FrontendMvRefreshActionPhase::Publication,
            &ledger.publication_operation_id,
        ),
        (
            FrontendMvRefreshActionPhase::StagingDrop,
            &ledger.staging_drop_operation_id,
        ),
    ]
    .into_iter()
    .map(|(phase, operation_id)| FrontendMvRefreshAction {
        phase,
        state: FrontendMvRefreshActionState::Prepared,
        operation_id: operation_id.clone(),
        receipt: None,
        committed_version: None,
        external_evidence: None,
        provider_finalized: false,
    })
    .collect()
}

fn ensure_frontend_action_prerequisites(
    ledger: &FrontendMvRefreshLedger,
    action: &FrontendMvRefreshAction,
) -> Result<(), String> {
    let state_for = |phase| {
        ledger
            .actions
            .iter()
            .find(|existing| existing.phase == phase)
            .map(|existing| &existing.state)
    };
    match action.phase {
        FrontendMvRefreshActionPhase::StagingCreate => Ok(()),
        FrontendMvRefreshActionPhase::Write => {
            if state_for(FrontendMvRefreshActionPhase::StagingCreate)
                == Some(&FrontendMvRefreshActionState::KnownCommitted)
            {
                Ok(())
            } else {
                Err(
                    "frontend MV write cannot advance before staging creation is known committed"
                        .to_string(),
                )
            }
        }
        FrontendMvRefreshActionPhase::Publication => {
            if state_for(FrontendMvRefreshActionPhase::Write)
                == Some(&FrontendMvRefreshActionState::KnownCommitted)
            {
                Ok(())
            } else {
                Err(
                    "frontend MV publication cannot advance before write is known committed"
                        .to_string(),
                )
            }
        }
        FrontendMvRefreshActionPhase::StagingDrop => {
            if state_for(FrontendMvRefreshActionPhase::Publication)
                == Some(&FrontendMvRefreshActionState::KnownCommitted)
            {
                Ok(())
            } else {
                Err(
                    "frontend MV cleanup cannot advance before publication is known committed"
                        .to_string(),
                )
            }
        }
    }
}

fn validate_refresh_metadata_request(
    request: &UpdateMvRefreshMetadataRequest,
) -> Result<(), MvRepositoryError> {
    if matches!(request.refresh_policy, StoredMvRefreshPolicy::AsyncInterval) {
        if request.refresh_interval_ms.is_none_or(|value| value <= 0) {
            return Err(invalid(
                "ASYNC_INTERVAL refresh policy requires positive refresh_interval_ms",
            ));
        }
    } else if request.refresh_interval_ms.is_some() {
        return Err(invalid(format!(
            "{} refresh policy cannot set refresh_interval_ms",
            request.refresh_policy.as_sql_str()
        )));
    }
    if request.max_staleness_ms.is_some_and(|value| value <= 0) {
        return Err(invalid("max_staleness_ms must be positive when set"));
    }
    if request.next_refresh_after_ms.is_some_and(|value| value < 0) {
        return Err(invalid(
            "next_refresh_after_ms must be non-negative when set",
        ));
    }
    Ok(())
}

fn validate_partition_state_limit(
    max_entries: usize,
) -> Result<(), novarocks_spi::state_store::StateStoreError> {
    if max_entries == 0 {
        return Err(invalid_state_store(
            "mv partition state max_entries must be positive",
        ));
    }
    Ok(())
}

fn dependency_sort_key(
    dependency: &StoredMvDependency,
) -> (
    &Option<String>,
    &String,
    &String,
    &novarocks::mv::dependency::model::MvDependencyObjectType,
    &novarocks::mv::dependency::model::MvDependencyStorageEngine,
) {
    (
        &dependency.upstream.catalog,
        &dependency.upstream.database_or_namespace,
        &dependency.upstream.name,
        &dependency.upstream.object_type,
        &dependency.upstream.storage_engine,
    )
}
fn invalid(message: impl Into<String>) -> MvRepositoryError {
    MvRepositoryError::new(MvRepositoryErrorKind::InvalidRequest, message)
}
fn corruption(message: impl Into<String>) -> MvRepositoryError {
    MvRepositoryError::new(MvRepositoryErrorKind::Corruption, message)
}
fn invalid_state_store(_message: impl Into<String>) -> novarocks_spi::state_store::StateStoreError {
    novarocks_spi::state_store::StateStoreError::new(
        novarocks_spi::state_store::StateStoreErrorKind::InvalidRequest,
        "invalid MV StateStore request",
    )
}
fn conflict_state_store(
    _message: impl Into<String>,
) -> novarocks_spi::state_store::StateStoreError {
    novarocks_spi::state_store::StateStoreError::new(
        novarocks_spi::state_store::StateStoreErrorKind::Conflict,
        "MV StateStore transaction conflict",
    )
}
