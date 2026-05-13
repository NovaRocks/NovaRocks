use std::collections::BTreeMap;

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
const MV_DEFINITION_SCHEMA_VERSION: i32 = 1;
const MV_TARGET_LOOKUP_SCHEMA_VERSION: i32 = 1;
const MV_REFRESH_SCHEMA_VERSION: i32 = 1;

#[derive(Default)]
pub struct MvMetaRepository;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvTargetApplyKey {
    pub column_name: String,
    pub field_id: i32,
    pub source: MvTargetApplyKeySource,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvTargetApplyKeySource {
    BaseRowId,
}

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
    pub target_apply_key: Option<MvTargetApplyKey>,
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
    pub target_apply_key: Option<MvTargetApplyKey>,
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
            target_apply_key: req.target_apply_key,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn stored_mv_definition(target_apply_key: Option<MvTargetApplyKey>) -> StoredMvDefinition {
        StoredMvDefinition {
            mv_id: 42,
            select_sql: "SELECT id FROM ice.ns.orders".to_string(),
            base_table_refs: vec!["ice.ns.orders".to_string()],
            primary_key_columns: Vec::new(),
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("mv".to_string()),
            target_table: Some("orders_mv".to_string()),
            target_apply_key,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: BTreeMap::new(),
            last_refresh_table_uuids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: BTreeMap::new(),
            created_at_ms: 1234,
        }
    }

    #[test]
    fn mv_target_apply_key_metadata_round_trips() {
        let apply_key = MvTargetApplyKey {
            column_name: "__nova_base_row_id".to_string(),
            field_id: 1001,
            source: MvTargetApplyKeySource::BaseRowId,
        };
        let definition = stored_mv_definition(Some(apply_key.clone()));

        let json = serde_json::to_string(&definition).expect("serialize mv definition");
        assert!(
            json.contains("\"source\":\"BASE_ROW_ID\""),
            "apply-key source must use the persisted SCREAMING_SNAKE_CASE contract: {json}"
        );
        let decoded: StoredMvDefinition =
            serde_json::from_str(&json).expect("deserialize mv definition");

        assert_eq!(decoded.target_apply_key, Some(apply_key));
    }

    #[test]
    fn mv_target_apply_key_defaults_to_none_for_old_records() {
        let mut json =
            serde_json::to_value(stored_mv_definition(None)).expect("serialize mv definition");
        json.as_object_mut()
            .expect("definition object")
            .remove("target_apply_key");

        let decoded: StoredMvDefinition =
            serde_json::from_value(json).expect("deserialize old mv definition");

        assert_eq!(decoded.target_apply_key, None);
    }
}
