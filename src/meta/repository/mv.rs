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
pub struct StoredMvDefinition {
    pub mv_id: i64,
    pub select_sql: String,
    pub base_table_refs: Vec<String>,
    pub primary_key_columns: Vec<String>,
    pub storage_engine: String,
    pub target_catalog: Option<String>,
    pub target_namespace: Option<String>,
    pub target_table: Option<String>,
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
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvTargetLookup {
    pub mv_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvRefresh {
    pub refresh_id: i64,
    pub mv_id: i64,
    pub state: MvRefreshState,
    pub target_snapshots: BTreeMap<String, i64>,
    pub external_outcome: Option<RefreshExternalOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvRefreshState {
    IntentCreated,
    ExternalCommitted,
    Finalized,
    AbortRequested,
    Aborted,
    CommitUnknown,
}

impl MvRefreshState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IntentCreated => "INTENT_CREATED",
            Self::ExternalCommitted => "EXTERNAL_COMMITTED",
            Self::Finalized => "FINALIZED",
            Self::AbortRequested => "ABORT_REQUESTED",
            Self::Aborted => "ABORTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshExternalOutcome {
    pub target_snapshot_id: Option<i64>,
    pub commit_id: String,
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
            target_snapshots,
            external_outcome: None,
        };
        put_refresh(txn, &refresh, ExpectedRevision::NotExists)?;
        Ok(refresh)
    }

    pub fn load_refresh(
        &self,
        txn: &dyn MetaReadTxn,
        refresh_id: i64,
    ) -> RepositoryResult<Option<StoredMvRefresh>> {
        Ok(load_versioned_refresh(txn, refresh_id)?.map(|versioned| versioned.value))
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
        refresh.value.state = MvRefreshState::ExternalCommitted;
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
        if refresh.value.state != MvRefreshState::ExternalCommitted {
            return Err(RepositoryError::conflict(format!(
                "mv refresh {} is {}, expected {}",
                req.refresh_id,
                refresh.value.state.as_str(),
                MvRefreshState::ExternalCommitted.as_str()
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
        .map(|record| {
            let value = decode_record_payload(&record, MV_REFRESH_KIND, MV_REFRESH_SCHEMA_VERSION)?;
            Ok(VersionedMvRefresh {
                record_revision: record.revision,
                value,
            })
        })
        .transpose()
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
