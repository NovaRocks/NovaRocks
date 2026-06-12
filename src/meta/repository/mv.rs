use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::meta::keys::{NS_MV, normalize_lookup_name};
use crate::meta::repository::mv_contract::{MvPartitionContract, MvSchemaContract};
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_payload_for_kind, encode_record_payload, id_scopes,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaReadTxn, MetaRecord, MetaRecordKind,
    MetaRecordPut, MetaRevision, MetaWriteTxn,
};

const MV_DEFINITION_KIND: &str = "mv.definition";
const MV_TARGET_LOOKUP_KIND: &str = "mv.target_lookup";
const MV_REFRESH_KIND: &str = "mv.refresh";
const MV_PARTITION_STATE_KIND: &str = "mv.partition_state";
const MV_DEPENDENCY_KIND: &str = "mv.dependency";

#[derive(Default)]
pub struct MvMetaRepository;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvDefinition {
    pub mv_id: i64,
    pub select_sql: String,
    pub base_table_refs: Vec<String>,
    pub primary_key_columns: Vec<String>,
    pub storage_engine: String,
    pub target_catalog: Option<String>,
    pub target_namespace: Option<String>,
    pub target_table: Option<String>,
    #[serde(default)]
    pub schema_contract: Option<MvSchemaContract>,
    #[serde(default)]
    pub partition_spec: Option<MvPartitionContract>,
    #[serde(default)]
    pub partition_state_complete: bool,
    pub last_refresh_ms: Option<i64>,
    pub last_refresh_rows: Option<i64>,
    pub last_refresh_snapshots: BTreeMap<String, i64>,
    pub last_refresh_table_uuids: BTreeMap<String, String>,
    pub last_refreshed_iceberg_snapshot_id: Option<i64>,
    pub refresh_in_progress: bool,
    #[serde(default)]
    pub active_refresh_id: Option<i64>,
    pub refresh_target_snapshots: BTreeMap<String, i64>,
    #[serde(default)]
    pub refresh_policy: StoredMvRefreshPolicy,
    #[serde(default)]
    pub refresh_paused: bool,
    #[serde(default)]
    pub refresh_interval_ms: Option<i64>,
    #[serde(default)]
    pub max_staleness_ms: Option<i64>,
    #[serde(default)]
    pub last_scheduler_error: Option<String>,
    #[serde(default)]
    pub next_refresh_after_ms: Option<i64>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredMvDefinitionAvro {
    mv_id: i64,
    select_sql: String,
    base_table_refs: Vec<String>,
    primary_key_columns: Vec<String>,
    storage_engine: String,
    target_catalog: Option<String>,
    target_namespace: Option<String>,
    target_table: Option<String>,
    schema_contract: Option<String>,
    partition_spec: Option<String>,
    #[serde(default)]
    partition_state_complete: bool,
    last_refresh_ms: Option<i64>,
    last_refresh_rows: Option<i64>,
    last_refresh_snapshots: BTreeMap<String, i64>,
    last_refresh_table_uuids: BTreeMap<String, String>,
    last_refreshed_iceberg_snapshot_id: Option<i64>,
    refresh_in_progress: bool,
    active_refresh_id: Option<i64>,
    refresh_target_snapshots: BTreeMap<String, i64>,
    refresh_policy: StoredMvRefreshPolicy,
    refresh_paused: bool,
    refresh_interval_ms: Option<i64>,
    max_staleness_ms: Option<i64>,
    last_scheduler_error: Option<String>,
    next_refresh_after_ms: Option<i64>,
    created_at_ms: i64,
}

impl TryFrom<&StoredMvDefinition> for StoredMvDefinitionAvro {
    type Error = RepositoryError;

    fn try_from(value: &StoredMvDefinition) -> RepositoryResult<Self> {
        Ok(Self {
            mv_id: value.mv_id,
            select_sql: value.select_sql.clone(),
            base_table_refs: value.base_table_refs.clone(),
            primary_key_columns: value.primary_key_columns.clone(),
            storage_engine: value.storage_engine.clone(),
            target_catalog: value.target_catalog.clone(),
            target_namespace: value.target_namespace.clone(),
            target_table: value.target_table.clone(),
            schema_contract: value
                .schema_contract
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|err| {
                    RepositoryError::invalid(format!(
                        "failed to encode MV schema contract as JSON: {err}"
                    ))
                })?,
            partition_spec: value
                .partition_spec
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|err| {
                    RepositoryError::invalid(format!(
                        "failed to encode MV partition contract as JSON: {err}"
                    ))
                })?,
            partition_state_complete: value.partition_state_complete,
            last_refresh_ms: value.last_refresh_ms,
            last_refresh_rows: value.last_refresh_rows,
            last_refresh_snapshots: value.last_refresh_snapshots.clone(),
            last_refresh_table_uuids: value.last_refresh_table_uuids.clone(),
            last_refreshed_iceberg_snapshot_id: value.last_refreshed_iceberg_snapshot_id,
            refresh_in_progress: value.refresh_in_progress,
            active_refresh_id: value.active_refresh_id,
            refresh_target_snapshots: value.refresh_target_snapshots.clone(),
            refresh_policy: value.refresh_policy.clone(),
            refresh_paused: value.refresh_paused,
            refresh_interval_ms: value.refresh_interval_ms,
            max_staleness_ms: value.max_staleness_ms,
            last_scheduler_error: value.last_scheduler_error.clone(),
            next_refresh_after_ms: value.next_refresh_after_ms,
            created_at_ms: value.created_at_ms,
        })
    }
}

impl TryFrom<StoredMvDefinitionAvro> for StoredMvDefinition {
    type Error = RepositoryError;

    fn try_from(value: StoredMvDefinitionAvro) -> RepositoryResult<Self> {
        Ok(Self {
            mv_id: value.mv_id,
            select_sql: value.select_sql,
            base_table_refs: value.base_table_refs,
            primary_key_columns: value.primary_key_columns,
            storage_engine: value.storage_engine,
            target_catalog: value.target_catalog,
            target_namespace: value.target_namespace,
            target_table: value.target_table,
            schema_contract: value
                .schema_contract
                .as_deref()
                .map(serde_json::from_str::<MvSchemaContract>)
                .transpose()
                .map_err(|err| {
                    RepositoryError::invalid(format!(
                        "failed to decode MV schema contract JSON: {err}"
                    ))
                })?,
            partition_spec: value
                .partition_spec
                .as_deref()
                .map(serde_json::from_str::<MvPartitionContract>)
                .transpose()
                .map_err(|err| {
                    RepositoryError::invalid(format!(
                        "failed to decode MV partition contract JSON: {err}"
                    ))
                })?,
            partition_state_complete: value.partition_state_complete,
            last_refresh_ms: value.last_refresh_ms,
            last_refresh_rows: value.last_refresh_rows,
            last_refresh_snapshots: value.last_refresh_snapshots,
            last_refresh_table_uuids: value.last_refresh_table_uuids,
            last_refreshed_iceberg_snapshot_id: value.last_refreshed_iceberg_snapshot_id,
            refresh_in_progress: value.refresh_in_progress,
            active_refresh_id: value.active_refresh_id,
            refresh_target_snapshots: value.refresh_target_snapshots,
            refresh_policy: value.refresh_policy,
            refresh_paused: value.refresh_paused,
            refresh_interval_ms: value.refresh_interval_ms,
            max_staleness_ms: value.max_staleness_ms,
            last_scheduler_error: value.last_scheduler_error,
            next_refresh_after_ms: value.next_refresh_after_ms,
            created_at_ms: value.created_at_ms,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedMvDefinition {
    pub record_revision: MetaRevision,
    pub value: StoredMvDefinition,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMvDefinitionRequest {
    pub select_sql: String,
    pub base_table_refs: Vec<String>,
    pub primary_key_columns: Vec<String>,
    pub storage_engine: String,
    pub target_catalog: Option<String>,
    pub target_namespace: Option<String>,
    pub target_table: Option<String>,
    pub schema_contract: Option<MvSchemaContract>,
    pub partition_spec: Option<MvPartitionContract>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StoredMvRefreshPolicy {
    #[default]
    Manual,
    AsyncOnChange,
    AsyncInterval,
}

impl StoredMvRefreshPolicy {
    pub fn as_sql_str(&self) -> &'static str {
        match self {
            Self::Manual => "DEFERRED_MANUAL",
            Self::AsyncOnChange => "ASYNC_ON_CHANGE",
            Self::AsyncInterval => "ASYNC_INTERVAL",
        }
    }

    fn accepts_interval(&self) -> bool {
        matches!(self, Self::AsyncInterval)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateMvRefreshMetadataRequest {
    pub mv_id: i64,
    pub refresh_policy: StoredMvRefreshPolicy,
    pub refresh_paused: bool,
    pub refresh_interval_ms: Option<i64>,
    pub max_staleness_ms: Option<i64>,
    pub last_scheduler_error: Option<String>,
    pub next_refresh_after_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvPartitionRefreshStatus {
    Fresh,
    Refreshing,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvPartitionState {
    pub mv_id: i64,
    pub partition_key: String,
    pub status: MvPartitionRefreshStatus,
    pub last_refresh_ms: Option<i64>,
    pub base_snapshots: BTreeMap<String, i64>,
    pub target_snapshot_id: Option<i64>,
    pub last_refresh_id: Option<i64>,
    pub failure_message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaceMvPartitionStatesRequest {
    pub mv_id: i64,
    pub partition_keys: BTreeSet<String>,
    pub last_refresh_ms: i64,
    pub base_snapshots: BTreeMap<String, i64>,
    pub target_snapshot_id: Option<i64>,
    pub last_refresh_id: i64,
    pub max_entries: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordFailedMvPartitionStatesRequest {
    pub mv_id: i64,
    pub partition_keys: BTreeSet<String>,
    pub failure_message: String,
    pub last_refresh_ms: i64,
    pub base_snapshots: BTreeMap<String, i64>,
    pub target_snapshot_id: Option<i64>,
    pub last_refresh_id: i64,
    pub max_entries: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvDependencyObjectType {
    Table,
    MaterializedView,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvDependencyStorageEngine {
    StarRocks,
    Iceberg,
    ExternalTable,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MvDependencyObjectRef {
    pub catalog: Option<String>,
    pub database_or_namespace: String,
    pub name: String,
    pub object_type: MvDependencyObjectType,
    pub storage_engine: MvDependencyStorageEngine,
}

impl MvDependencyObjectRef {
    pub fn display_name(&self) -> String {
        let object = match self.catalog.as_deref() {
            Some(catalog) => format!("{catalog}.{}.{}", self.database_or_namespace, self.name),
            None => format!("{}.{}", self.database_or_namespace, self.name),
        };
        match self.object_type {
            MvDependencyObjectType::Table => object,
            MvDependencyObjectType::MaterializedView => format!("mv:{object}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvDependency {
    pub downstream_mv_id: i64,
    pub upstream: MvDependencyObjectRef,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMvDependencyRequest {
    pub upstream: MvDependencyObjectRef,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvTargetLookup {
    pub mv_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshExternalOutcome {
    pub target_snapshot_id: Option<i64>,
    pub commit_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshCommitMarker {
    pub refresh_id: i64,
    pub mv_id: i64,
    pub token: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvRefresh {
    pub refresh_id: i64,
    pub mv_id: i64,
    #[serde(default)]
    pub operation_id: Option<i64>,
    pub state: MvRefreshState,
    #[serde(default)]
    pub target_catalog: Option<String>,
    #[serde(default)]
    pub target_namespace: Option<String>,
    #[serde(default)]
    pub target_table: Option<String>,
    #[serde(default)]
    pub staging_branch: Option<String>,
    #[serde(default)]
    pub expected_main_snapshot_id: Option<i64>,
    #[serde(default)]
    pub staging_snapshot_id: Option<i64>,
    #[serde(default)]
    pub published_snapshot_id: Option<i64>,
    #[serde(default)]
    pub target_snapshots: BTreeMap<String, i64>,
    #[serde(default)]
    pub base_table_uuids: BTreeMap<String, String>,
    #[serde(default)]
    pub rows: Option<i64>,
    #[serde(default)]
    pub marker: Option<RefreshCommitMarker>,
    #[serde(default)]
    pub external_outcome: Option<RefreshExternalOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvRefreshState {
    IntentCreated,
    StagingCommitted,
    #[serde(alias = "EXTERNAL_COMMITTED")]
    PublishCommitted,
    Finalized,
    AbortRequested,
    Aborted,
    CommitUnknown,
}

impl MvRefreshState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IntentCreated => "INTENT_CREATED",
            Self::StagingCommitted => "STAGING_COMMITTED",
            Self::PublishCommitted => "PUBLISH_COMMITTED",
            Self::Finalized => "FINALIZED",
            Self::AbortRequested => "ABORT_REQUESTED",
            Self::Aborted => "ABORTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BeginIcebergMvRefreshRequest {
    pub mv_id: i64,
    pub operation_id: Option<i64>,
    pub target_catalog: String,
    pub target_namespace: String,
    pub target_table: String,
    pub staging_branch: String,
    pub expected_main_snapshot_id: Option<i64>,
    pub base_snapshots: BTreeMap<String, i64>,
    pub marker_token: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordStagingCommitRequest {
    pub refresh_id: i64,
    pub staging_snapshot_id: i64,
    pub rows: i64,
    pub base_table_uuids: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPublishCommitRequest {
    pub refresh_id: i64,
    pub published_snapshot_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshFinalizeRequest {
    pub refresh_id: i64,
    pub rows: i64,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_uuids: BTreeMap<String, String>,
    pub target_snapshot_id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateStarRocksMvRefreshSummaryRequest {
    pub mv_id: i64,
    pub last_refresh_ms: i64,
    pub last_refresh_rows: i64,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_uuids: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateMvPartitionContractRequest {
    pub mv_id: i64,
    pub partition_spec: MvPartitionContract,
}

impl MvMetaRepository {
    pub fn create_definition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateMvDefinitionRequest,
    ) -> RepositoryResult<StoredMvDefinition> {
        loop {
            let mv_id = txn.allocate_id(id_scopes::starrocks_table())?;
            if self.load_by_id(txn, mv_id)?.is_none() {
                return self.create_definition_with_id(txn, mv_id, req);
            }
        }
    }

    pub fn reserve_definition_id(
        &self,
        txn: &mut dyn MetaWriteTxn,
        mv_id: i64,
    ) -> RepositoryResult<()> {
        if mv_id <= 0 {
            return Err(RepositoryError::invalid(format!(
                "mv definition id must be positive, got {mv_id}"
            )));
        }
        if self.load_by_id(txn, mv_id)?.is_some() {
            return Err(RepositoryError::conflict(format!(
                "mv definition {mv_id} already exists"
            )));
        }
        loop {
            let reserved = txn.allocate_id(id_scopes::starrocks_table())?;
            if reserved >= mv_id {
                return Ok(());
            }
        }
    }

    pub fn create_definition_with_id(
        &self,
        txn: &mut dyn MetaWriteTxn,
        mv_id: i64,
        req: CreateMvDefinitionRequest,
    ) -> RepositoryResult<StoredMvDefinition> {
        if mv_id <= 0 {
            return Err(RepositoryError::invalid(format!(
                "mv definition id must be positive, got {mv_id}"
            )));
        }
        let definition = StoredMvDefinition {
            mv_id,
            select_sql: req.select_sql,
            base_table_refs: req.base_table_refs,
            primary_key_columns: req.primary_key_columns,
            storage_engine: req.storage_engine,
            target_catalog: req.target_catalog,
            target_namespace: req.target_namespace,
            target_table: req.target_table,
            schema_contract: req.schema_contract,
            partition_spec: req.partition_spec,
            partition_state_complete: false,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: BTreeMap::new(),
            last_refresh_table_uuids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: BTreeMap::new(),
            refresh_policy: StoredMvRefreshPolicy::Manual,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: req.created_at_ms,
        };

        txn.put(MetaRecordPut::new(
            key_by_id(mv_id)?,
            record_kind(MV_DEFINITION_KIND)?,
            ExpectedRevision::NotExists,
            encode_record_payload(
                MV_DEFINITION_KIND,
                &StoredMvDefinitionAvro::try_from(&definition)?,
            )?,
        ))?;

        if let (Some(catalog), Some(namespace), Some(table)) = (
            definition.target_catalog.as_deref(),
            definition.target_namespace.as_deref(),
            definition.target_table.as_deref(),
        ) {
            txn.put(MetaRecordPut::new(
                key_by_target(catalog, namespace, table)?,
                record_kind(MV_TARGET_LOOKUP_KIND)?,
                ExpectedRevision::NotExists,
                encode_record_payload(MV_TARGET_LOOKUP_KIND, &MvTargetLookup { mv_id })?,
            ))?;
        }

        Ok(definition)
    }

    pub fn update_refresh_metadata(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: UpdateMvRefreshMetadataRequest,
    ) -> RepositoryResult<StoredMvDefinition> {
        validate_refresh_metadata(&req)?;
        let mut definition = self.load_versioned_by_id(txn, req.mv_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("mv definition {} not found", req.mv_id))
        })?;
        definition.value.refresh_policy = req.refresh_policy;
        definition.value.refresh_paused = req.refresh_paused;
        definition.value.refresh_interval_ms = req.refresh_interval_ms;
        definition.value.max_staleness_ms = req.max_staleness_ms;
        definition.value.last_scheduler_error = req.last_scheduler_error;
        definition.value.next_refresh_after_ms = req.next_refresh_after_ms;
        put_definition(
            txn,
            &definition,
            ExpectedRevision::Exact(definition.record_revision.clone()),
        )?;
        Ok(definition.value)
    }

    pub fn update_partition_contract(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: UpdateMvPartitionContractRequest,
    ) -> RepositoryResult<StoredMvDefinition> {
        let mut definition = self.load_versioned_by_id(txn, req.mv_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("mv definition {} not found", req.mv_id))
        })?;
        let schema_contract = definition.value.schema_contract.as_mut().ok_or_else(|| {
            RepositoryError::invalid(format!(
                "mv definition {} is missing schema contract",
                req.mv_id
            ))
        })?;
        schema_contract.target.partition = Some(req.partition_spec.clone());
        definition.value.partition_spec = Some(req.partition_spec);
        definition.value.partition_state_complete = false;
        put_definition(
            txn,
            &definition,
            ExpectedRevision::Exact(definition.record_revision.clone()),
        )?;
        Ok(definition.value)
    }

    pub fn load_by_id(
        &self,
        txn: &dyn MetaReadTxn,
        mv_id: i64,
    ) -> RepositoryResult<Option<StoredMvDefinition>> {
        Ok(self
            .load_versioned_by_id(txn, mv_id)?
            .map(|versioned| versioned.value))
    }

    pub fn load_versioned_by_id(
        &self,
        txn: &dyn MetaReadTxn,
        mv_id: i64,
    ) -> RepositoryResult<Option<VersionedMvDefinition>> {
        txn.get(&key_by_id(mv_id)?)?
            .map(decode_definition_record)
            .transpose()
    }

    pub fn list_definitions(
        &self,
        txn: &dyn MetaReadTxn,
    ) -> RepositoryResult<Vec<StoredMvDefinition>> {
        txn.scan(&key_prefix_by_id()?, None)?
            .into_iter()
            .map(decode_definition_record)
            .map(|result| result.map(|versioned| versioned.value))
            .collect()
    }

    pub fn find_by_target(
        &self,
        txn: &dyn MetaReadTxn,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> RepositoryResult<Option<StoredMvDefinition>> {
        let Some(record) = txn.get(&key_by_target(catalog, namespace, table)?)? else {
            return Ok(None);
        };
        let lookup: MvTargetLookup = decode_record_payload(&record, MV_TARGET_LOOKUP_KIND)?;
        let definition =
            self.load_target_lookup_definition(txn, &lookup, catalog, namespace, table)?;
        Ok(Some(definition.value))
    }

    pub fn drop_by_target(
        &self,
        txn: &mut dyn MetaWriteTxn,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> RepositoryResult<bool> {
        let target_key = key_by_target(catalog, namespace, table)?;
        let Some(record) = txn.get(&target_key)? else {
            return Ok(false);
        };
        let lookup: MvTargetLookup = decode_record_payload(&record, MV_TARGET_LOOKUP_KIND)?;
        let definition =
            self.load_target_lookup_definition(txn, &lookup, catalog, namespace, table)?;
        if definition.value.refresh_in_progress || definition.value.active_refresh_id.is_some() {
            return Err(RepositoryError::conflict(format!(
                "mv definition {} has refresh in progress",
                definition.value.mv_id
            )));
        }

        self.delete_dependencies_for_mv(txn, lookup.mv_id)?;
        self.delete_partition_states_for_mv(txn, lookup.mv_id)?;
        txn.delete(&target_key, ExpectedRevision::Exact(record.revision))?;
        txn.delete(
            &key_by_id(lookup.mv_id)?,
            ExpectedRevision::Exact(definition.record_revision),
        )?;
        Ok(true)
    }

    pub fn drop_by_id(&self, txn: &mut dyn MetaWriteTxn, mv_id: i64) -> RepositoryResult<bool> {
        let Some(definition) = self.load_versioned_by_id(txn, mv_id)? else {
            return Ok(false);
        };
        if definition.value.refresh_in_progress || definition.value.active_refresh_id.is_some() {
            return Err(RepositoryError::conflict(format!(
                "mv definition {} has refresh in progress",
                definition.value.mv_id
            )));
        }
        if let (Some(catalog), Some(namespace), Some(table)) = (
            definition.value.target_catalog.as_deref(),
            definition.value.target_namespace.as_deref(),
            definition.value.target_table.as_deref(),
        ) {
            txn.delete(
                &key_by_target(catalog, namespace, table)?,
                ExpectedRevision::Any,
            )?;
        }
        self.delete_dependencies_for_mv(txn, mv_id)?;
        self.delete_partition_states_for_mv(txn, mv_id)?;
        txn.delete(
            &key_by_id(mv_id)?,
            ExpectedRevision::Exact(definition.record_revision),
        )?;
        Ok(true)
    }

    fn load_target_lookup_definition(
        &self,
        txn: &dyn MetaReadTxn,
        lookup: &MvTargetLookup,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> RepositoryResult<VersionedMvDefinition> {
        let definition = self
            .load_versioned_by_id(txn, lookup.mv_id)?
            .ok_or_else(|| {
                RepositoryError::provider(format!("mv definition {} not found", lookup.mv_id))
            })?;
        if !definition_target_matches(&definition.value, catalog, namespace, table) {
            return Err(RepositoryError::provider(format!(
                "mv target lookup {}/{}/{} points to definition {} with target {:?}.{:?}.{:?}",
                normalize_lookup_name(catalog),
                normalize_lookup_name(namespace),
                normalize_lookup_name(table),
                definition.value.mv_id,
                definition.value.target_catalog,
                definition.value.target_namespace,
                definition.value.target_table
            )));
        }
        Ok(definition)
    }

    pub fn begin_refresh_intent(
        &self,
        txn: &mut dyn MetaWriteTxn,
        mv_id: i64,
        target_snapshots: BTreeMap<String, i64>,
    ) -> RepositoryResult<StoredMvRefresh> {
        let mut definition = self.load_versioned_by_id(txn, mv_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("mv definition {mv_id} not found"))
        })?;
        if definition.value.refresh_in_progress {
            return Err(RepositoryError::conflict(format!(
                "mv definition {mv_id} already has refresh in progress"
            )));
        }

        let refresh_id = txn.allocate_id(id_scopes::refresh_id())?;
        definition.value.refresh_in_progress = true;
        definition.value.active_refresh_id = Some(refresh_id);
        definition.value.refresh_target_snapshots = target_snapshots.clone();
        put_definition(
            txn,
            &definition,
            ExpectedRevision::Exact(definition.record_revision.clone()),
        )?;

        let refresh = StoredMvRefresh {
            refresh_id,
            mv_id,
            operation_id: None,
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
        };
        put_refresh(txn, &refresh, ExpectedRevision::NotExists)?;
        Ok(refresh)
    }

    pub fn begin_iceberg_refresh_intent(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: BeginIcebergMvRefreshRequest,
    ) -> RepositoryResult<StoredMvRefresh> {
        let mut definition = self.load_versioned_by_id(txn, req.mv_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("mv definition {} not found", req.mv_id))
        })?;
        if definition.value.refresh_in_progress || definition.value.active_refresh_id.is_some() {
            return Err(RepositoryError::conflict(format!(
                "mv definition {} already has refresh in progress",
                req.mv_id
            )));
        }

        let refresh_id = txn.allocate_id(id_scopes::refresh_id())?;
        let marker = RefreshCommitMarker {
            refresh_id,
            mv_id: req.mv_id,
            token: req.marker_token,
        };
        definition.value.refresh_in_progress = true;
        definition.value.active_refresh_id = Some(refresh_id);
        definition.value.refresh_target_snapshots = req.base_snapshots.clone();
        put_definition(
            txn,
            &definition,
            ExpectedRevision::Exact(definition.record_revision.clone()),
        )?;

        let refresh = StoredMvRefresh {
            refresh_id,
            mv_id: req.mv_id,
            operation_id: req.operation_id,
            state: MvRefreshState::IntentCreated,
            target_catalog: Some(req.target_catalog),
            target_namespace: Some(req.target_namespace),
            target_table: Some(req.target_table),
            staging_branch: Some(req.staging_branch),
            expected_main_snapshot_id: req.expected_main_snapshot_id,
            staging_snapshot_id: None,
            published_snapshot_id: None,
            target_snapshots: req.base_snapshots,
            base_table_uuids: BTreeMap::new(),
            rows: None,
            marker: Some(marker),
            external_outcome: None,
        };
        put_refresh(txn, &refresh, ExpectedRevision::NotExists)?;
        Ok(refresh)
    }

    pub fn record_staging_commit(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: RecordStagingCommitRequest,
    ) -> RepositoryResult<()> {
        let mut refresh = load_versioned_refresh(txn, req.refresh_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("mv refresh {} not found", req.refresh_id))
        })?;
        if refresh.value.state == MvRefreshState::StagingCommitted {
            if refresh.value.staging_snapshot_id == Some(req.staging_snapshot_id)
                && refresh.value.rows == Some(req.rows)
                && refresh.value.base_table_uuids == req.base_table_uuids
            {
                return Ok(());
            }
            return Err(RepositoryError::conflict(format!(
                "mv refresh {} staging commit differs from recorded value",
                req.refresh_id
            )));
        }
        if refresh.value.state != MvRefreshState::IntentCreated {
            return Err(RepositoryError::conflict(format!(
                "mv refresh {} is {}, expected {}",
                req.refresh_id,
                refresh.value.state.as_str(),
                MvRefreshState::IntentCreated.as_str()
            )));
        }
        refresh.value.state = MvRefreshState::StagingCommitted;
        refresh.value.staging_snapshot_id = Some(req.staging_snapshot_id);
        refresh.value.rows = Some(req.rows);
        refresh.value.base_table_uuids = req.base_table_uuids;
        put_refresh(
            txn,
            &refresh.value,
            ExpectedRevision::Exact(refresh.record_revision),
        )
    }

    pub fn record_publish_commit(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: RecordPublishCommitRequest,
    ) -> RepositoryResult<()> {
        let mut refresh = load_versioned_refresh(txn, req.refresh_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("mv refresh {} not found", req.refresh_id))
        })?;
        if refresh.value.state == MvRefreshState::PublishCommitted {
            let outcome_snapshot_id = refresh
                .value
                .external_outcome
                .as_ref()
                .and_then(|outcome| outcome.target_snapshot_id);
            if refresh.value.published_snapshot_id == Some(req.published_snapshot_id)
                && outcome_snapshot_id == Some(req.published_snapshot_id)
            {
                return Ok(());
            }
            return Err(RepositoryError::conflict(format!(
                "mv refresh {} publish commit differs from recorded value",
                req.refresh_id
            )));
        }
        if refresh.value.state != MvRefreshState::StagingCommitted {
            return Err(RepositoryError::conflict(format!(
                "mv refresh {} is {}, expected {}",
                req.refresh_id,
                refresh.value.state.as_str(),
                MvRefreshState::StagingCommitted.as_str()
            )));
        }
        refresh.value.state = MvRefreshState::PublishCommitted;
        refresh.value.published_snapshot_id = Some(req.published_snapshot_id);
        refresh.value.external_outcome = Some(RefreshExternalOutcome {
            target_snapshot_id: Some(req.published_snapshot_id),
            commit_id: format!("iceberg-snapshot-{}", req.published_snapshot_id),
        });
        put_refresh(
            txn,
            &refresh.value,
            ExpectedRevision::Exact(refresh.record_revision),
        )
    }

    pub fn mark_refresh_commit_unknown(
        &self,
        txn: &mut dyn MetaWriteTxn,
        refresh_id: i64,
    ) -> RepositoryResult<()> {
        let mut refresh = load_versioned_refresh(txn, refresh_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("mv refresh {refresh_id} not found"))
        })?;
        if matches!(
            refresh.value.state,
            MvRefreshState::Finalized | MvRefreshState::Aborted
        ) {
            return Ok(());
        }
        refresh.value.state = MvRefreshState::CommitUnknown;
        put_refresh(
            txn,
            &refresh.value,
            ExpectedRevision::Exact(refresh.record_revision),
        )
    }

    pub fn load_refresh(
        &self,
        txn: &dyn MetaReadTxn,
        refresh_id: i64,
    ) -> RepositoryResult<Option<StoredMvRefresh>> {
        Ok(load_versioned_refresh(txn, refresh_id)?.map(|versioned| versioned.value))
    }

    pub fn list_unfinished_refreshes(
        &self,
        txn: &dyn MetaReadTxn,
    ) -> RepositoryResult<Vec<StoredMvRefresh>> {
        Ok(txn
            .scan(&key_prefix_refresh()?, None)?
            .into_iter()
            .map(decode_refresh_record)
            .collect::<RepositoryResult<Vec<_>>>()?
            .into_iter()
            .map(|versioned| versioned.value)
            .filter(|refresh| {
                !matches!(
                    refresh.state,
                    MvRefreshState::Finalized | MvRefreshState::Aborted
                )
            })
            .collect())
    }

    pub fn list_unfinished_branch_staged_iceberg_refreshes(
        &self,
        txn: &dyn MetaReadTxn,
    ) -> RepositoryResult<Vec<StoredMvRefresh>> {
        Ok(self
            .list_unfinished_refreshes(txn)?
            .into_iter()
            .filter(|refresh| {
                refresh.target_catalog.is_some()
                    && refresh.target_namespace.is_some()
                    && refresh.target_table.is_some()
                    && refresh.staging_branch.is_some()
                    && refresh.marker.is_some()
            })
            .collect())
    }

    pub fn record_external_commit_outcome(
        &self,
        txn: &mut dyn MetaWriteTxn,
        refresh_id: i64,
        outcome: RefreshExternalOutcome,
    ) -> RepositoryResult<()> {
        let mut refresh = load_versioned_refresh(txn, refresh_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("mv refresh {refresh_id} not found"))
        })?;
        if refresh.value.state != MvRefreshState::IntentCreated {
            return Err(RepositoryError::conflict(format!(
                "mv refresh {refresh_id} is {}, expected {}",
                refresh.value.state.as_str(),
                MvRefreshState::IntentCreated.as_str()
            )));
        }
        refresh.value.state = MvRefreshState::PublishCommitted;
        refresh.value.published_snapshot_id = outcome.target_snapshot_id;
        refresh.value.external_outcome = Some(outcome);
        put_refresh(
            txn,
            &refresh.value,
            ExpectedRevision::Exact(refresh.record_revision),
        )
    }

    pub fn finalize_refresh(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: MvRefreshFinalizeRequest,
    ) -> RepositoryResult<()> {
        let mut refresh = load_versioned_refresh(txn, req.refresh_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("mv refresh {} not found", req.refresh_id))
        })?;
        if refresh.value.state == MvRefreshState::Finalized {
            return Ok(());
        }
        if refresh.value.state != MvRefreshState::PublishCommitted {
            return Err(RepositoryError::conflict(format!(
                "mv refresh {} is {}, expected {}",
                req.refresh_id,
                refresh.value.state.as_str(),
                MvRefreshState::PublishCommitted.as_str()
            )));
        }
        let persisted_target_snapshot_id = persisted_publish_target_snapshot(&refresh.value);
        if persisted_target_snapshot_id != req.target_snapshot_id {
            return Err(RepositoryError::conflict(format!(
                "mv refresh {} target snapshot is {:?}, expected published snapshot {:?}",
                req.refresh_id, req.target_snapshot_id, persisted_target_snapshot_id
            )));
        }

        let mut definition = self
            .load_versioned_by_id(txn, refresh.value.mv_id)?
            .ok_or_else(|| {
                RepositoryError::not_found(format!(
                    "mv definition {} not found",
                    refresh.value.mv_id
                ))
            })?;
        if definition.value.active_refresh_id != Some(req.refresh_id) {
            return Err(RepositoryError::conflict(format!(
                "mv definition {} active refresh is {:?}, expected {}",
                refresh.value.mv_id, definition.value.active_refresh_id, req.refresh_id
            )));
        }

        definition.value.last_refresh_rows = Some(req.rows);
        definition.value.last_refresh_snapshots = req.base_snapshots;
        definition.value.last_refresh_table_uuids = req.base_table_uuids;
        definition.value.last_refreshed_iceberg_snapshot_id = req.target_snapshot_id;
        definition.value.refresh_in_progress = false;
        definition.value.active_refresh_id = None;
        definition.value.refresh_target_snapshots.clear();
        put_definition(
            txn,
            &definition,
            ExpectedRevision::Exact(definition.record_revision.clone()),
        )?;

        refresh.value.state = MvRefreshState::Finalized;
        put_refresh(
            txn,
            &refresh.value,
            ExpectedRevision::Exact(refresh.record_revision),
        )
    }

    pub fn update_starrocks_refresh_summary_if_present(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: UpdateStarRocksMvRefreshSummaryRequest,
    ) -> RepositoryResult<bool> {
        let Some(mut definition) = self.load_versioned_by_id(txn, req.mv_id)? else {
            return Ok(false);
        };
        if let Some(refresh_id) = definition.value.active_refresh_id
            && let Some(refresh) = load_versioned_refresh(txn, refresh_id)?
            && refresh.value.state == MvRefreshState::CommitUnknown
        {
            return Err(RepositoryError::conflict(format!(
                "mv definition {} active refresh {} is commit-unknown",
                definition.value.mv_id, refresh_id
            )));
        }
        definition.value.last_refresh_ms = Some(req.last_refresh_ms);
        definition.value.last_refresh_rows = Some(req.last_refresh_rows);
        definition.value.last_refresh_snapshots = req.base_snapshots;
        definition.value.last_refresh_table_uuids = req.base_table_uuids;
        definition.value.refresh_in_progress = false;
        if let Some(refresh_id) = definition.value.active_refresh_id.take()
            && let Some(mut refresh) = load_versioned_refresh(txn, refresh_id)?
        {
            refresh.value.state = MvRefreshState::Finalized;
            put_refresh(
                txn,
                &refresh.value,
                ExpectedRevision::Exact(refresh.record_revision),
            )?;
        }
        definition.value.refresh_target_snapshots.clear();
        put_definition(
            txn,
            &definition,
            ExpectedRevision::Exact(definition.record_revision.clone()),
        )?;
        Ok(true)
    }

    pub fn clear_refresh_progress(
        &self,
        txn: &mut dyn MetaWriteTxn,
        mv_id: i64,
    ) -> RepositoryResult<bool> {
        let Some(mut definition) = self.load_versioned_by_id(txn, mv_id)? else {
            return Ok(false);
        };
        if !definition.value.refresh_in_progress && definition.value.active_refresh_id.is_none() {
            return Ok(true);
        }
        if let Some(refresh_id) = definition.value.active_refresh_id
            && let Some(refresh) = load_versioned_refresh(txn, refresh_id)?
            && refresh.value.state == MvRefreshState::CommitUnknown
        {
            return Err(RepositoryError::conflict(format!(
                "mv definition {} active refresh {} is commit-unknown",
                definition.value.mv_id, refresh_id
            )));
        }
        if let Some(refresh_id) = definition.value.active_refresh_id.take()
            && let Some(mut refresh) = load_versioned_refresh(txn, refresh_id)?
            && !matches!(
                refresh.value.state,
                MvRefreshState::Finalized | MvRefreshState::Aborted
            )
        {
            refresh.value.state = MvRefreshState::Aborted;
            put_refresh(
                txn,
                &refresh.value,
                ExpectedRevision::Exact(refresh.record_revision),
            )?;
        }
        definition.value.refresh_in_progress = false;
        definition.value.refresh_target_snapshots.clear();
        put_definition(
            txn,
            &definition,
            ExpectedRevision::Exact(definition.record_revision.clone()),
        )?;
        Ok(true)
    }

    pub fn replace_partition_states(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: ReplaceMvPartitionStatesRequest,
    ) -> RepositoryResult<Vec<StoredMvPartitionState>> {
        validate_partition_state_limit(req.max_entries)?;
        let mut definition = self.load_versioned_by_id(txn, req.mv_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("mv definition {} not found", req.mv_id))
        })?;
        self.delete_partition_states_for_mv(txn, req.mv_id)?;
        if req.partition_keys.len() > req.max_entries {
            definition.value.partition_state_complete = false;
            put_definition(
                txn,
                &definition,
                ExpectedRevision::Exact(definition.record_revision.clone()),
            )?;
            return Ok(Vec::new());
        }

        let mut states = Vec::with_capacity(req.partition_keys.len());
        for partition_key in req.partition_keys {
            let state = StoredMvPartitionState {
                mv_id: req.mv_id,
                partition_key,
                status: MvPartitionRefreshStatus::Fresh,
                last_refresh_ms: Some(req.last_refresh_ms),
                base_snapshots: req.base_snapshots.clone(),
                target_snapshot_id: req.target_snapshot_id,
                last_refresh_id: Some(req.last_refresh_id),
                failure_message: None,
            };
            put_partition_state(txn, &state, ExpectedRevision::NotExists)?;
            states.push(state);
        }
        definition.value.partition_state_complete = true;
        put_definition(
            txn,
            &definition,
            ExpectedRevision::Exact(definition.record_revision.clone()),
        )?;
        Ok(states)
    }

    pub fn record_failed_partition_states(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: RecordFailedMvPartitionStatesRequest,
    ) -> RepositoryResult<Vec<StoredMvPartitionState>> {
        validate_partition_state_limit(req.max_entries)?;
        let mut definition = self.load_versioned_by_id(txn, req.mv_id)?.ok_or_else(|| {
            RepositoryError::not_found(format!("mv definition {} not found", req.mv_id))
        })?;
        self.delete_partition_states_for_mv(txn, req.mv_id)?;
        if req.partition_keys.len() > req.max_entries {
            definition.value.partition_state_complete = false;
            put_definition(
                txn,
                &definition,
                ExpectedRevision::Exact(definition.record_revision.clone()),
            )?;
            return Ok(Vec::new());
        }

        let mut states = Vec::with_capacity(req.partition_keys.len());
        for partition_key in req.partition_keys {
            let state = StoredMvPartitionState {
                mv_id: req.mv_id,
                partition_key,
                status: MvPartitionRefreshStatus::Failed,
                last_refresh_ms: Some(req.last_refresh_ms),
                base_snapshots: req.base_snapshots.clone(),
                target_snapshot_id: req.target_snapshot_id,
                last_refresh_id: Some(req.last_refresh_id),
                failure_message: Some(req.failure_message.clone()),
            };
            put_partition_state(txn, &state, ExpectedRevision::NotExists)?;
            states.push(state);
        }
        definition.value.partition_state_complete = true;
        put_definition(
            txn,
            &definition,
            ExpectedRevision::Exact(definition.record_revision.clone()),
        )?;
        Ok(states)
    }

    pub fn clear_partition_states(
        &self,
        txn: &mut dyn MetaWriteTxn,
        mv_id: i64,
    ) -> RepositoryResult<bool> {
        let Some(mut definition) = self.load_versioned_by_id(txn, mv_id)? else {
            return Ok(false);
        };
        self.delete_partition_states_for_mv(txn, mv_id)?;
        if definition.value.partition_state_complete {
            definition.value.partition_state_complete = false;
            put_definition(
                txn,
                &definition,
                ExpectedRevision::Exact(definition.record_revision.clone()),
            )?;
        }
        Ok(true)
    }

    pub fn list_partition_states(
        &self,
        txn: &dyn MetaReadTxn,
        mv_id: i64,
    ) -> RepositoryResult<Vec<StoredMvPartitionState>> {
        let mut states = txn
            .scan(&key_prefix_partition_state(mv_id)?, None)?
            .into_iter()
            .map(decode_partition_state_record)
            .collect::<RepositoryResult<Vec<_>>>()?;
        states.sort_by(|left, right| left.partition_key.cmp(&right.partition_key));
        Ok(states)
    }

    fn delete_partition_states_for_mv(
        &self,
        txn: &mut dyn MetaWriteTxn,
        mv_id: i64,
    ) -> RepositoryResult<()> {
        let existing = self.list_partition_states(txn, mv_id)?;
        for state in existing {
            txn.delete(
                &key_partition_state(state.mv_id, &state.partition_key)?,
                ExpectedRevision::Any,
            )?;
        }
        Ok(())
    }

    /// Adopt a new target snapshot produced by a pure compaction (OPTIMIZE /
    /// rewrite_data_files) of an iceberg MV's own storage table.
    ///
    /// A compaction REPLACE snapshot does not change the MV's logical contents
    /// — it only rewrites the physical data files — so the MV's recorded
    /// `last_refreshed_iceberg_snapshot_id` (which incremental refresh uses as
    /// its tamper-detection baseline via `validate_target_snapshot`) can safely
    /// be advanced to the new snapshot without re-running a refresh. Without
    /// this, the next incremental refresh would reject the table as "modified
    /// outside NovaRocks".
    ///
    /// Safety:
    /// * Only adopts when no refresh is in progress (same guard as `drop_*`),
    ///   so it never races a refresh that is mutating the same field.
    /// * Only adopts when the recorded snapshot equals `expected_base_snapshot_id`
    ///   (the snapshot the compaction was based on). If they differ, a refresh
    ///   advanced the baseline between the compaction and this call, and the new
    ///   compaction snapshot may not be a pure rewrite of the recorded state —
    ///   in that case we do NOT adopt (returns `false`) and leave the guard to
    ///   surface the discrepancy on the next refresh, rather than risk skipping
    ///   a real data change.
    ///
    /// Returns `Ok(true)` if the snapshot was adopted, `Ok(false)` if the MV
    /// definition was not found, has a refresh in progress, or its recorded
    /// baseline no longer matches `expected_base_snapshot_id`.
    pub fn adopt_target_compaction_snapshot(
        &self,
        txn: &mut dyn MetaWriteTxn,
        catalog: &str,
        namespace: &str,
        table: &str,
        expected_base_snapshot_id: i64,
        new_snapshot_id: i64,
    ) -> RepositoryResult<bool> {
        let target_key = key_by_target(catalog, namespace, table)?;
        let Some(record) = txn.get(&target_key)? else {
            return Ok(false);
        };
        let lookup: MvTargetLookup = decode_record_payload(&record, MV_TARGET_LOOKUP_KIND)?;
        let mut definition =
            self.load_target_lookup_definition(txn, &lookup, catalog, namespace, table)?;
        if definition.value.refresh_in_progress || definition.value.active_refresh_id.is_some() {
            return Ok(false);
        }
        if definition.value.last_refreshed_iceberg_snapshot_id != Some(expected_base_snapshot_id) {
            return Ok(false);
        }
        if definition.value.last_refreshed_iceberg_snapshot_id == Some(new_snapshot_id) {
            return Ok(true);
        }
        definition.value.last_refreshed_iceberg_snapshot_id = Some(new_snapshot_id);
        put_definition(
            txn,
            &definition,
            ExpectedRevision::Exact(definition.record_revision.clone()),
        )?;
        Ok(true)
    }

    pub fn replace_dependencies_for_mv(
        &self,
        txn: &mut dyn MetaWriteTxn,
        downstream_mv_id: i64,
        dependencies: Vec<CreateMvDependencyRequest>,
    ) -> RepositoryResult<Vec<StoredMvDependency>> {
        self.delete_dependencies_for_mv(txn, downstream_mv_id)?;

        let mut seen = BTreeSet::new();
        let mut stored = Vec::new();
        for req in dependencies {
            let key = dependency_object_key(&req.upstream)?;
            if !seen.insert(key) {
                continue;
            }
            let dependency = StoredMvDependency {
                downstream_mv_id,
                upstream: req.upstream,
                created_at_ms: req.created_at_ms,
            };
            put_dependency_indexes(txn, &dependency)?;
            stored.push(dependency);
        }
        Ok(stored)
    }

    pub fn delete_dependencies_for_mv(
        &self,
        txn: &mut dyn MetaWriteTxn,
        downstream_mv_id: i64,
    ) -> RepositoryResult<()> {
        let existing = self.list_dependencies_by_downstream(txn, downstream_mv_id)?;
        for dependency in existing {
            txn.delete(
                &key_dependency_by_downstream(dependency.downstream_mv_id, &dependency.upstream)?,
                ExpectedRevision::Any,
            )?;
            txn.delete(
                &key_dependency_by_upstream(&dependency.upstream, dependency.downstream_mv_id)?,
                ExpectedRevision::Any,
            )?;
        }
        Ok(())
    }

    pub fn ensure_no_downstream_dependencies(
        &self,
        txn: &dyn MetaReadTxn,
        upstream: &MvDependencyObjectRef,
    ) -> RepositoryResult<()> {
        let downstream = self.list_downstream_dependencies(txn, upstream)?;
        if downstream.is_empty() {
            return Ok(());
        }
        let mut ids = downstream
            .iter()
            .map(|dep| dep.downstream_mv_id)
            .collect::<Vec<i64>>();
        ids.sort();
        let ids_str = ids
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        Err(RepositoryError::conflict(format!(
            "{} has downstream materialized views: {ids_str}",
            upstream.display_name(),
        )))
    }

    pub fn list_dependencies_by_downstream(
        &self,
        txn: &dyn MetaReadTxn,
        downstream_mv_id: i64,
    ) -> RepositoryResult<Vec<StoredMvDependency>> {
        let mut dependencies = txn
            .scan(
                &key_prefix_dependency_by_downstream(downstream_mv_id)?,
                None,
            )?
            .into_iter()
            .map(decode_dependency_record)
            .collect::<RepositoryResult<Vec<_>>>()?;
        // Sort by an explicit tuple key so the API ordering does not silently
        // change if `MvDependencyObjectRef`'s field order is ever reshuffled.
        dependencies.sort_by(|left, right| {
            (
                &left.upstream.catalog,
                &left.upstream.database_or_namespace,
                &left.upstream.name,
                &left.upstream.object_type,
                &left.upstream.storage_engine,
            )
                .cmp(&(
                    &right.upstream.catalog,
                    &right.upstream.database_or_namespace,
                    &right.upstream.name,
                    &right.upstream.object_type,
                    &right.upstream.storage_engine,
                ))
        });
        Ok(dependencies)
    }

    pub fn list_downstream_dependencies(
        &self,
        txn: &dyn MetaReadTxn,
        upstream: &MvDependencyObjectRef,
    ) -> RepositoryResult<Vec<StoredMvDependency>> {
        let mut dependencies = txn
            .scan(&key_prefix_dependency_by_upstream(upstream)?, None)?
            .into_iter()
            .map(decode_dependency_record)
            .collect::<RepositoryResult<Vec<_>>>()?;
        dependencies.sort_by_key(|dep| dep.downstream_mv_id);
        Ok(dependencies)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VersionedMvRefresh {
    record_revision: MetaRevision,
    value: StoredMvRefresh,
}

fn decode_definition_record(record: MetaRecord) -> RepositoryResult<VersionedMvDefinition> {
    let value =
        decode_record_payload::<StoredMvDefinitionAvro>(&record, MV_DEFINITION_KIND)?.try_into()?;
    Ok(VersionedMvDefinition {
        record_revision: record.revision,
        value,
    })
}

fn load_versioned_refresh(
    txn: &dyn MetaReadTxn,
    refresh_id: i64,
) -> RepositoryResult<Option<VersionedMvRefresh>> {
    txn.get(&key_refresh(refresh_id)?)?
        .map(decode_refresh_record)
        .transpose()
}

fn decode_refresh_record(record: MetaRecord) -> RepositoryResult<VersionedMvRefresh> {
    let value = decode_record_payload(&record, MV_REFRESH_KIND)?;
    Ok(VersionedMvRefresh {
        record_revision: record.revision,
        value,
    })
}

fn put_definition(
    txn: &mut dyn MetaWriteTxn,
    definition: &VersionedMvDefinition,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_by_id(definition.value.mv_id)?,
        record_kind(MV_DEFINITION_KIND)?,
        expected,
        encode_record_payload(
            MV_DEFINITION_KIND,
            &StoredMvDefinitionAvro::try_from(&definition.value)?,
        )?,
    ))?;
    Ok(())
}

fn put_refresh(
    txn: &mut dyn MetaWriteTxn,
    refresh: &StoredMvRefresh,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_refresh(refresh.refresh_id)?,
        record_kind(MV_REFRESH_KIND)?,
        expected,
        encode_record_payload(MV_REFRESH_KIND, refresh)?,
    ))?;
    Ok(())
}

fn decode_partition_state_record(record: MetaRecord) -> RepositoryResult<StoredMvPartitionState> {
    decode_record_payload(&record, MV_PARTITION_STATE_KIND)
}

fn put_partition_state(
    txn: &mut dyn MetaWriteTxn,
    state: &StoredMvPartitionState,
    expected: ExpectedRevision,
) -> RepositoryResult<()> {
    txn.put(MetaRecordPut::new(
        key_partition_state(state.mv_id, &state.partition_key)?,
        record_kind(MV_PARTITION_STATE_KIND)?,
        expected,
        encode_record_payload(MV_PARTITION_STATE_KIND, state)?,
    ))?;
    Ok(())
}

fn persisted_publish_target_snapshot(refresh: &StoredMvRefresh) -> Option<i64> {
    refresh.published_snapshot_id.or_else(|| {
        refresh
            .external_outcome
            .as_ref()
            .and_then(|outcome| outcome.target_snapshot_id)
    })
}

fn decode_record_payload<T>(record: &MetaRecord, expected_kind: &str) -> RepositoryResult<T>
where
    T: for<'de> Deserialize<'de>,
{
    if record.kind.as_str() != expected_kind {
        return Err(RepositoryError::provider(format!(
            "metadata record {} has kind {}, expected {expected_kind}",
            record.key.canonical_path(),
            record.kind.as_str()
        )));
    }
    decode_payload_for_kind(expected_kind, &record.payload).map_err(|err| {
        RepositoryError::provider(format!(
            "failed to decode metadata record {} as {expected_kind}: {err}",
            record.key.canonical_path()
        ))
    })
}

fn record_kind(value: &str) -> RepositoryResult<MetaRecordKind> {
    Ok(MetaRecordKind::new(value)?)
}

fn definition_target_matches(
    definition: &StoredMvDefinition,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> bool {
    definition
        .target_catalog
        .as_deref()
        .map(normalize_lookup_name)
        == Some(normalize_lookup_name(catalog))
        && definition
            .target_namespace
            .as_deref()
            .map(normalize_lookup_name)
            == Some(normalize_lookup_name(namespace))
        && definition
            .target_table
            .as_deref()
            .map(normalize_lookup_name)
            == Some(normalize_lookup_name(table))
}

fn validate_refresh_metadata(req: &UpdateMvRefreshMetadataRequest) -> RepositoryResult<()> {
    if req.refresh_policy.accepts_interval() {
        match req.refresh_interval_ms {
            Some(value) if value > 0 => {}
            _ => {
                return Err(RepositoryError::invalid(
                    "ASYNC_INTERVAL refresh policy requires positive refresh_interval_ms",
                ));
            }
        }
    } else if req.refresh_interval_ms.is_some() {
        return Err(RepositoryError::invalid(format!(
            "{} refresh policy cannot set refresh_interval_ms",
            req.refresh_policy.as_sql_str()
        )));
    }

    if let Some(value) = req.max_staleness_ms
        && value <= 0
    {
        return Err(RepositoryError::invalid(
            "max_staleness_ms must be positive when set",
        ));
    }

    if let Some(value) = req.next_refresh_after_ms
        && value < 0
    {
        return Err(RepositoryError::invalid(
            "next_refresh_after_ms must be non-negative when set",
        ));
    }

    Ok(())
}

fn validate_partition_state_limit(max_entries: usize) -> RepositoryResult<()> {
    if max_entries == 0 {
        return Err(RepositoryError::invalid(
            "mv partition state max_entries must be positive",
        ));
    }
    Ok(())
}

fn key_by_id(mv_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MV,
        ["by-id".to_string(), mv_id.to_string()],
    )?)
}

fn key_prefix_by_id() -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(NS_MV, ["by-id"])?)
}

fn key_prefix_refresh() -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(NS_MV, ["refresh"])?)
}

fn key_by_target(catalog: &str, namespace: &str, table: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MV,
        [
            "by-target".to_string(),
            normalize_lookup_name(catalog),
            normalize_lookup_name(namespace),
            normalize_lookup_name(table),
        ],
    )?)
}

fn key_refresh(refresh_id: i64) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MV,
        ["refresh".to_string(), refresh_id.to_string()],
    )?)
}

fn key_partition_state(mv_id: i64, partition_key: &str) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MV,
        [
            "partition-state".to_string(),
            mv_id.to_string(),
            encode_key_segment(partition_key),
        ],
    )?)
}

fn key_prefix_partition_state(mv_id: i64) -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(
        NS_MV,
        ["partition-state".to_string(), mv_id.to_string()],
    )?)
}

fn encode_key_segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0F) as usize] as char);
            }
        }
    }
    out
}

/// Separator used to pack the dependency object identity into a single key
/// segment. Real SQL identifiers cannot contain this character; we reject
/// any value that does so the packed key encoding stays self-validating.
const DEPENDENCY_KEY_SEPARATOR: char = '|';

fn reject_dependency_separator(field: &str, value: &str) -> RepositoryResult<()> {
    if value.contains(DEPENDENCY_KEY_SEPARATOR) {
        return Err(RepositoryError::invalid(format!(
            "mv dependency field {field} must not contain '{DEPENDENCY_KEY_SEPARATOR}' \
             (got {value:?})"
        )));
    }
    Ok(())
}

fn dependency_object_key(object: &MvDependencyObjectRef) -> RepositoryResult<String> {
    if let Some(catalog) = object.catalog.as_deref() {
        reject_dependency_separator("catalog", catalog)?;
    }
    reject_dependency_separator("database_or_namespace", &object.database_or_namespace)?;
    reject_dependency_separator("name", &object.name)?;

    let catalog = object
        .catalog
        .as_deref()
        .map(normalize_lookup_name)
        .unwrap_or_else(|| "_".to_string());
    let object_type = match object.object_type {
        MvDependencyObjectType::Table => "table",
        MvDependencyObjectType::MaterializedView => "mv",
    };
    let storage_engine = match object.storage_engine {
        MvDependencyStorageEngine::StarRocks => "starrocks",
        MvDependencyStorageEngine::Iceberg => "iceberg",
        MvDependencyStorageEngine::ExternalTable => "external_table",
    };
    Ok(format!(
        "{storage_engine}|{object_type}|{}|{}|{}",
        catalog,
        normalize_lookup_name(&object.database_or_namespace),
        normalize_lookup_name(&object.name)
    ))
}

fn key_dependency_by_downstream(
    downstream_mv_id: i64,
    upstream: &MvDependencyObjectRef,
) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MV,
        [
            "dependency".to_string(),
            "by-downstream".to_string(),
            downstream_mv_id.to_string(),
            dependency_object_key(upstream)?,
        ],
    )?)
}

fn key_prefix_dependency_by_downstream(downstream_mv_id: i64) -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(
        NS_MV,
        [
            "dependency".to_string(),
            "by-downstream".to_string(),
            downstream_mv_id.to_string(),
        ],
    )?)
}

fn key_dependency_by_upstream(
    upstream: &MvDependencyObjectRef,
    downstream_mv_id: i64,
) -> RepositoryResult<MetaKey> {
    Ok(MetaKey::new(
        NS_MV,
        [
            "dependency".to_string(),
            "by-upstream".to_string(),
            dependency_object_key(upstream)?,
            downstream_mv_id.to_string(),
        ],
    )?)
}

fn key_prefix_dependency_by_upstream(
    upstream: &MvDependencyObjectRef,
) -> RepositoryResult<MetaKeyPrefix> {
    Ok(MetaKeyPrefix::new(
        NS_MV,
        [
            "dependency".to_string(),
            "by-upstream".to_string(),
            dependency_object_key(upstream)?,
        ],
    )?)
}

fn decode_dependency_record(record: MetaRecord) -> RepositoryResult<StoredMvDependency> {
    decode_record_payload(&record, MV_DEPENDENCY_KIND)
}

fn put_dependency_indexes(
    txn: &mut dyn MetaWriteTxn,
    dependency: &StoredMvDependency,
) -> RepositoryResult<()> {
    let payload = encode_record_payload(MV_DEPENDENCY_KIND, dependency)?;
    txn.put(MetaRecordPut::new(
        key_dependency_by_downstream(dependency.downstream_mv_id, &dependency.upstream)?,
        record_kind(MV_DEPENDENCY_KIND)?,
        ExpectedRevision::Any,
        payload.clone(),
    ))?;
    txn.put(MetaRecordPut::new(
        key_dependency_by_upstream(&dependency.upstream, dependency.downstream_mv_id)?,
        record_kind(MV_DEPENDENCY_KIND)?,
        ExpectedRevision::Any,
        payload,
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::SqliteMetaStoreProvider;
    use crate::meta::provider::MetaStoreProvider;

    /// Open a fresh in-memory-style SQLite provider backed by a temp directory.
    fn open_provider() -> (tempfile::TempDir, SqliteMetaStoreProvider) {
        let dir = tempfile::tempdir().expect("create tempdir for mv tests");
        let provider = SqliteMetaStoreProvider::open(dir.path().join("mv.sqlite"))
            .expect("open sqlite metadata provider for mv tests");
        (dir, provider)
    }

    /// Build a minimal `CreateMvDefinitionRequest` whose target triple is
    /// (`catalog`, `namespace`, `table`).  The target triple is required so
    /// `adopt_target_compaction_snapshot` can look up the definition.
    fn minimal_create_request(
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> CreateMvDefinitionRequest {
        CreateMvDefinitionRequest {
            select_sql: "SELECT 1".to_string(),
            base_table_refs: vec![],
            primary_key_columns: vec![],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some(catalog.to_string()),
            target_namespace: Some(namespace.to_string()),
            target_table: Some(table.to_string()),
            schema_contract: None,
            partition_spec: None,
            created_at_ms: 1_700_000_000_000,
        }
    }

    /// Patch a stored definition by loading it, applying `f`, then writing it
    /// back.  Used to seed `last_refreshed_iceberg_snapshot_id`, `refresh_in_progress`,
    /// and `active_refresh_id` without going through the full refresh state
    /// machine.
    fn patch_definition(
        provider: &SqliteMetaStoreProvider,
        mv_id: i64,
        f: impl FnOnce(&mut StoredMvDefinition),
    ) {
        let repo = MvMetaRepository;
        let mut txn = provider.begin_write("patch mv definition").unwrap();
        let mut versioned = repo
            .load_versioned_by_id(txn.as_ref(), mv_id)
            .unwrap()
            .expect("definition must exist to patch");
        f(&mut versioned.value);
        put_definition(
            txn.as_mut(),
            &versioned,
            ExpectedRevision::Exact(versioned.record_revision.clone()),
        )
        .unwrap();
        txn.commit().unwrap();
    }

    // ── positive path ──────────────────────────────────────────────────────

    #[test]
    fn adopt_compaction_snapshot_advances_recorded_id_on_clean_baseline() {
        let (_dir, provider) = open_provider();
        let repo = MvMetaRepository;

        // Create the MV definition.
        let mut txn = provider.begin_write("create mv").unwrap();
        let definition = repo
            .create_definition(txn.as_mut(), minimal_create_request("cat", "ns", "tbl"))
            .unwrap();
        txn.commit().unwrap();

        const BASE: i64 = 1000;
        const NEW: i64 = 2000;

        // Seed last_refreshed_iceberg_snapshot_id = Some(BASE); no refresh active.
        patch_definition(&provider, definition.mv_id, |d| {
            d.last_refreshed_iceberg_snapshot_id = Some(BASE);
        });

        // Call adopt.
        let mut txn = provider.begin_write("adopt").unwrap();
        let adopted = repo
            .adopt_target_compaction_snapshot(txn.as_mut(), "cat", "ns", "tbl", BASE, NEW)
            .unwrap();
        txn.commit().unwrap();

        assert!(adopted, "expected adopt to return true on a clean baseline");

        // Confirm the stored id was advanced to NEW.
        let read = provider.begin_read().unwrap();
        let stored = repo
            .load_by_id(read.as_ref(), definition.mv_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.last_refreshed_iceberg_snapshot_id,
            Some(NEW),
            "recorded snapshot id must be advanced to the new compaction snapshot"
        );
    }

    // ── negative gates ─────────────────────────────────────────────────────

    #[test]
    fn adopt_compaction_snapshot_skips_when_refresh_in_progress() {
        let (_dir, provider) = open_provider();
        let repo = MvMetaRepository;

        let mut txn = provider.begin_write("create mv").unwrap();
        let definition = repo
            .create_definition(txn.as_mut(), minimal_create_request("cat", "ns", "tbl2"))
            .unwrap();
        txn.commit().unwrap();

        const BASE: i64 = 1000;
        const NEW: i64 = 2000;

        // Seed: baseline recorded, refresh currently in progress.
        patch_definition(&provider, definition.mv_id, |d| {
            d.last_refreshed_iceberg_snapshot_id = Some(BASE);
            d.refresh_in_progress = true;
            d.active_refresh_id = Some(42);
        });

        let mut txn = provider.begin_write("adopt").unwrap();
        let adopted = repo
            .adopt_target_compaction_snapshot(txn.as_mut(), "cat", "ns", "tbl2", BASE, NEW)
            .unwrap();
        txn.commit().unwrap();

        assert!(
            !adopted,
            "expected adopt to return false when refresh is in progress"
        );

        // Recorded id must be unchanged.
        let read = provider.begin_read().unwrap();
        let stored = repo
            .load_by_id(read.as_ref(), definition.mv_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.last_refreshed_iceberg_snapshot_id,
            Some(BASE),
            "recorded snapshot id must not change when refresh is in progress"
        );
    }

    #[test]
    fn adopt_compaction_snapshot_skips_on_baseline_mismatch() {
        let (_dir, provider) = open_provider();
        let repo = MvMetaRepository;

        let mut txn = provider.begin_write("create mv").unwrap();
        let definition = repo
            .create_definition(txn.as_mut(), minimal_create_request("cat", "ns", "tbl3"))
            .unwrap();
        txn.commit().unwrap();

        const BASE: i64 = 1000;
        const OTHER: i64 = 999; // recorded id differs from expected_base
        const NEW: i64 = 2000;

        // Seed: recorded id is OTHER, not BASE.
        patch_definition(&provider, definition.mv_id, |d| {
            d.last_refreshed_iceberg_snapshot_id = Some(OTHER);
        });

        let mut txn = provider.begin_write("adopt").unwrap();
        let adopted = repo
            .adopt_target_compaction_snapshot(txn.as_mut(), "cat", "ns", "tbl3", BASE, NEW)
            .unwrap();
        txn.commit().unwrap();

        assert!(
            !adopted,
            "expected adopt to return false when baseline does not match"
        );

        // Recorded id must be unchanged.
        let read = provider.begin_read().unwrap();
        let stored = repo
            .load_by_id(read.as_ref(), definition.mv_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.last_refreshed_iceberg_snapshot_id,
            Some(OTHER),
            "recorded snapshot id must not change on baseline mismatch"
        );
    }

    #[test]
    fn adopt_compaction_snapshot_skips_when_no_target_record() {
        let (_dir, provider) = open_provider();
        let repo = MvMetaRepository;

        // No definition exists for this target triple at all.
        let mut txn = provider.begin_write("adopt").unwrap();
        let adopted = repo
            .adopt_target_compaction_snapshot(
                txn.as_mut(),
                "cat",
                "ns",
                "nonexistent_table",
                1000,
                2000,
            )
            .unwrap();
        txn.commit().unwrap();

        assert!(
            !adopted,
            "expected adopt to return false when no target record exists"
        );
    }
}
