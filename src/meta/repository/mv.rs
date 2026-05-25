use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::meta::keys::{NS_MV, normalize_lookup_name};
use crate::meta::repository::{
    RepositoryError, RepositoryResult, decode_json_payload, encode_json_payload, id_scopes,
};
use crate::meta::{
    ExpectedRevision, MetaKey, MetaKeyPrefix, MetaReadTxn, MetaRecord, MetaRecordKind,
    MetaRecordPut, MetaRevision, MetaWriteTxn,
};

const MV_DEFINITION_KIND: &str = "mv.definition";
const MV_TARGET_LOOKUP_KIND: &str = "mv.target_lookup";
const MV_REFRESH_KIND: &str = "mv.refresh";
const MV_DEPENDENCY_KIND: &str = "mv.dependency";
const MV_DEFINITION_SCHEMA_VERSION: i32 = 2;
const MV_TARGET_LOOKUP_SCHEMA_VERSION: i32 = 1;
const MV_REFRESH_SCHEMA_VERSION: i32 = 1;
const MV_DEPENDENCY_SCHEMA_VERSION: i32 = 1;

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
    pub schema_contract: Option<crate::meta::repository::mv_contract::MvSchemaContract>,
    #[serde(default)]
    pub partition_spec: Option<crate::meta::repository::mv_contract::MvPartitionContract>,
    pub last_refresh_ms: Option<i64>,
    pub last_refresh_rows: Option<i64>,
    pub last_refresh_snapshots: BTreeMap<String, i64>,
    pub last_refresh_table_uuids: BTreeMap<String, String>,
    pub last_refreshed_iceberg_snapshot_id: Option<i64>,
    pub refresh_in_progress: bool,
    #[serde(default)]
    pub active_refresh_id: Option<i64>,
    pub refresh_target_snapshots: BTreeMap<String, i64>,
    pub created_at_ms: i64,
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
    pub schema_contract: Option<crate::meta::repository::mv_contract::MvSchemaContract>,
    pub partition_spec: Option<crate::meta::repository::mv_contract::MvPartitionContract>,
    pub created_at_ms: i64,
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
    ManagedLake,
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
pub struct UpdateManagedMvRefreshSummaryRequest {
    pub mv_id: i64,
    pub last_refresh_ms: i64,
    pub last_refresh_rows: i64,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_uuids: BTreeMap<String, String>,
}

impl MvMetaRepository {
    pub fn create_definition(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: CreateMvDefinitionRequest,
    ) -> RepositoryResult<StoredMvDefinition> {
        loop {
            let mv_id = txn.allocate_id(id_scopes::managed_table())?;
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
            let reserved = txn.allocate_id(id_scopes::managed_table())?;
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
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: BTreeMap::new(),
            last_refresh_table_uuids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: BTreeMap::new(),
            created_at_ms: req.created_at_ms,
        };

        txn.put(MetaRecordPut::new(
            key_by_id(mv_id)?,
            record_kind(MV_DEFINITION_KIND)?,
            ExpectedRevision::NotExists,
            encode_json_payload(MV_DEFINITION_SCHEMA_VERSION, &definition)?,
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
                encode_json_payload(MV_TARGET_LOOKUP_SCHEMA_VERSION, &MvTargetLookup { mv_id })?,
            ))?;
        }

        Ok(definition)
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
        let lookup: MvTargetLookup = decode_record_payload(
            &record,
            MV_TARGET_LOOKUP_KIND,
            MV_TARGET_LOOKUP_SCHEMA_VERSION,
        )?;
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
        let lookup: MvTargetLookup = decode_record_payload(
            &record,
            MV_TARGET_LOOKUP_KIND,
            MV_TARGET_LOOKUP_SCHEMA_VERSION,
        )?;
        let definition =
            self.load_target_lookup_definition(txn, &lookup, catalog, namespace, table)?;
        if definition.value.refresh_in_progress || definition.value.active_refresh_id.is_some() {
            return Err(RepositoryError::conflict(format!(
                "mv definition {} has refresh in progress",
                definition.value.mv_id
            )));
        }

        self.delete_dependencies_for_mv(txn, lookup.mv_id)?;
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

    pub fn update_managed_refresh_summary_if_present(
        &self,
        txn: &mut dyn MetaWriteTxn,
        req: UpdateManagedMvRefreshSummaryRequest,
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
    let value = decode_record_payload(&record, MV_DEFINITION_KIND, MV_DEFINITION_SCHEMA_VERSION)?;
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
    let value = decode_record_payload(&record, MV_REFRESH_KIND, MV_REFRESH_SCHEMA_VERSION)?;
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
        encode_json_payload(MV_DEFINITION_SCHEMA_VERSION, &definition.value)?,
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
        encode_json_payload(MV_REFRESH_SCHEMA_VERSION, refresh)?,
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

fn decode_record_payload<T>(
    record: &MetaRecord,
    expected_kind: &str,
    expected_schema_version: i32,
) -> RepositoryResult<T>
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
    if record.payload.schema_version != expected_schema_version {
        return Err(RepositoryError::provider(format!(
            "metadata record {} has schema version {}, expected {expected_schema_version}",
            record.key.canonical_path(),
            record.payload.schema_version
        )));
    }
    decode_json_payload(&record.payload)
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
        MvDependencyStorageEngine::ManagedLake => "managed_lake",
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
    decode_record_payload(&record, MV_DEPENDENCY_KIND, MV_DEPENDENCY_SCHEMA_VERSION)
}

fn put_dependency_indexes(
    txn: &mut dyn MetaWriteTxn,
    dependency: &StoredMvDependency,
) -> RepositoryResult<()> {
    let payload = encode_json_payload(MV_DEPENDENCY_SCHEMA_VERSION, dependency)?;
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
}
