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

use std::collections::BTreeMap;
use std::io::Cursor;

use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value};
use bytes::Bytes;
use novarocks_spi::connector::ConnectorTableObjectId;
use novarocks_state_store_api::{Key, Value};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::common::persisted_query_definition::PersistedQueryDefinition;
use crate::mv::domain::persistence::definition::{
    MV_ACCELERATOR_PROJECTION_SUBJECT, MvAcceleratorSourceRevision, MvDesiredRefreshPolicy,
    StoredMvDefinition,
};
use crate::mv::domain::persistence::dependency::MV_ACCELERATOR_DEPENDENCY_SUBJECT;
use crate::mv::domain::persistence::schema::{MvPartitionContract, MvSchemaContract};
use crate::state_family::StateFamily;

use super::catalog::schema_catalog;
use super::key::{MvKeyKind, expected_record_kind};

const MAGIC: &[u8; 4] = b"NRMA";
/// Record version of the MV accelerator family.
///
/// Declared by the manifest, not here: a second literal could disagree with
/// the version the manifest publishes and nothing would catch it.
const ENVELOPE_VERSION: u8 = match StateFamily::MvAccelerator.record_version() {
    Some(version) => version,
    None => panic!("MV accelerator is a durable accelerator family"),
};
const HEADER_BYTES_BEFORE_FINGERPRINT: usize = 12;
const OPERATION_ID_BYTES: usize = 16;
const PAYLOAD_LENGTH_BYTES: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum MvRecordKind {
    Projection = 1,
    TargetLookup = 2,
    Dependency = 3,
    Sequence = 4,
}

impl MvRecordKind {
    fn from_byte(value: u8) -> Result<Self, String> {
        match value {
            1 => Ok(Self::Projection),
            2 => Ok(Self::TargetLookup),
            3 => Ok(Self::Dependency),
            4 => Ok(Self::Sequence),
            _ => Err(format!("unknown MV Accelerator record kind {value}")),
        }
    }

    fn subject(self) -> &'static str {
        match self {
            Self::Projection => MV_ACCELERATOR_PROJECTION_SUBJECT,
            Self::TargetLookup => "mv.accelerator_target_lookup",
            Self::Dependency => MV_ACCELERATOR_DEPENDENCY_SUBJECT,
            Self::Sequence => "mv.accelerator_sequence",
        }
    }

    fn matches_key(self, key_kind: MvKeyKind) -> bool {
        match self {
            Self::Projection => key_kind == MvKeyKind::Projection,
            Self::TargetLookup => key_kind == MvKeyKind::TargetLookup,
            Self::Dependency => matches!(
                key_kind,
                MvKeyKind::DependencyDownstream | MvKeyKind::DependencyUpstream
            ),
            Self::Sequence => key_kind == MvKeyKind::Sequence,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedMvRecord<T> {
    pub operation_id: Uuid,
    pub value: T,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MvSequence {
    pub last_allocated_id: i64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct StoredMvDefinitionAvro {
    mv_id: i64,
    query_definition: PersistedQueryDefinition,
    base_table_refs: Vec<String>,
    primary_key_columns: Vec<String>,
    storage_engine: String,
    target_catalog: Option<String>,
    target_namespace: Option<String>,
    target_table: Option<String>,
    schema_contract: Option<String>,
    partition_spec: Option<String>,
    last_refresh_ms: Option<i64>,
    last_refresh_rows: Option<i64>,
    last_refresh_snapshots: BTreeMap<String, i64>,
    last_refresh_table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
    last_refreshed_iceberg_snapshot_id: Option<i64>,
    refresh_policy: MvDesiredRefreshPolicy,
    refresh_paused: bool,
    refresh_interval_ms: Option<i64>,
    max_staleness_ms: Option<i64>,
    created_at_ms: i64,
    source_revision: MvAcceleratorSourceRevision,
}

impl TryFrom<&StoredMvDefinition> for StoredMvDefinitionAvro {
    type Error = String;

    fn try_from(value: &StoredMvDefinition) -> Result<Self, Self::Error> {
        Ok(Self {
            mv_id: value.mv_id,
            query_definition: value.query_definition.clone(),
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
                .map_err(|error| format!("encode MV schema contract failed: {error}"))?,
            partition_spec: value
                .partition_spec
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| format!("encode MV partition contract failed: {error}"))?,
            last_refresh_ms: value.last_refresh_ms,
            last_refresh_rows: value.last_refresh_rows,
            last_refresh_snapshots: value.last_refresh_snapshots.clone(),
            last_refresh_table_object_ids: value.last_refresh_table_object_ids.clone(),
            last_refreshed_iceberg_snapshot_id: value.last_refreshed_iceberg_snapshot_id,
            refresh_policy: value.refresh_policy.clone(),
            refresh_paused: value.refresh_paused,
            refresh_interval_ms: value.refresh_interval_ms,
            max_staleness_ms: value.max_staleness_ms,
            created_at_ms: value.created_at_ms,
            source_revision: value.source_revision.clone(),
        })
    }
}

impl TryFrom<StoredMvDefinitionAvro> for StoredMvDefinition {
    type Error = String;

    fn try_from(value: StoredMvDefinitionAvro) -> Result<Self, Self::Error> {
        Ok(Self {
            mv_id: value.mv_id,
            query_definition: value.query_definition,
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
                .map_err(|error| format!("decode MV schema contract failed: {error}"))?,
            partition_spec: value
                .partition_spec
                .as_deref()
                .map(serde_json::from_str::<MvPartitionContract>)
                .transpose()
                .map_err(|error| format!("decode MV partition contract failed: {error}"))?,
            last_refresh_ms: value.last_refresh_ms,
            last_refresh_rows: value.last_refresh_rows,
            last_refresh_snapshots: value.last_refresh_snapshots,
            last_refresh_table_object_ids: value.last_refresh_table_object_ids,
            last_refreshed_iceberg_snapshot_id: value.last_refreshed_iceberg_snapshot_id,
            refresh_policy: value.refresh_policy,
            refresh_paused: value.refresh_paused,
            refresh_interval_ms: value.refresh_interval_ms,
            max_staleness_ms: value.max_staleness_ms,
            created_at_ms: value.created_at_ms,
            source_revision: value.source_revision,
        })
    }
}

pub fn encode_projection(
    operation_id: Uuid,
    definition: &StoredMvDefinition,
) -> Result<Value, String> {
    definition
        .query_definition
        .validate()
        .map_err(|error| format!("invalid persisted MV query definition: {error}"))?;
    encode_record(
        MvRecordKind::Projection,
        operation_id,
        &StoredMvDefinitionAvro::try_from(definition)?,
    )
}

pub fn decode_projection(
    key: &Key,
    value: &Value,
) -> Result<DecodedMvRecord<StoredMvDefinition>, String> {
    let decoded: DecodedMvRecord<StoredMvDefinitionAvro> = decode_record(key, value)?;
    let value: StoredMvDefinition = decoded.value.try_into()?;
    value
        .query_definition
        .validate()
        .map_err(|error| format!("invalid persisted MV query definition: {error}"))?;
    Ok(DecodedMvRecord {
        operation_id: decoded.operation_id,
        value,
    })
}

pub fn encode_record<T>(kind: MvRecordKind, operation_id: Uuid, value: &T) -> Result<Value, String>
where
    T: Serialize,
{
    let catalog = schema_catalog()?;
    let entry = catalog.latest(kind.subject())?;
    let datum = to_value(value)
        .map_err(|error| format!("convert MV Accelerator record failed: {error}"))?;
    let payload = to_avro_datum(entry.schema(), datum).map_err(|error| {
        format!(
            "encode MV Accelerator Avro payload for {} schema {} failed: {error}",
            entry.subject(),
            entry.id()
        )
    })?;
    let fingerprint = entry.fingerprint().as_bytes();
    let fingerprint_len = u16::try_from(fingerprint.len())
        .map_err(|_| "MV Accelerator Avro fingerprint is too large".to_string())?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| "MV Accelerator Avro payload is too large".to_string())?;
    let mut envelope = Vec::with_capacity(
        HEADER_BYTES_BEFORE_FINGERPRINT
            + fingerprint.len()
            + OPERATION_ID_BYTES
            + PAYLOAD_LENGTH_BYTES
            + payload.len(),
    );
    envelope.extend_from_slice(MAGIC);
    envelope.push(ENVELOPE_VERSION);
    envelope.push(kind as u8);
    envelope.extend_from_slice(&entry.id().to_be_bytes());
    envelope.extend_from_slice(&fingerprint_len.to_be_bytes());
    envelope.extend_from_slice(fingerprint);
    envelope.extend_from_slice(operation_id.as_bytes());
    envelope.extend_from_slice(&payload_len.to_be_bytes());
    envelope.extend_from_slice(&payload);
    Value::try_from(Bytes::from(envelope))
        .map_err(|error| format!("encode MV Accelerator StateStore value failed: {error}"))
}

pub fn decode_record<T>(key: &Key, value: &Value) -> Result<DecodedMvRecord<T>, String>
where
    T: DeserializeOwned,
{
    let bytes = value.as_bytes();
    if bytes.len() < HEADER_BYTES_BEFORE_FINGERPRINT + OPERATION_ID_BYTES + PAYLOAD_LENGTH_BYTES {
        return Err("MV Accelerator envelope is truncated".to_string());
    }
    if &bytes[..4] != MAGIC {
        return Err("MV Accelerator envelope has invalid magic".to_string());
    }
    if bytes[4] != ENVELOPE_VERSION {
        return Err(format!(
            "unsupported MV Accelerator envelope version {}",
            bytes[4]
        ));
    }
    let kind = MvRecordKind::from_byte(bytes[5])?;
    let key_kind = expected_record_kind(key)?;
    if !kind.matches_key(key_kind) {
        return Err(format!(
            "MV Accelerator envelope record kind {:?} does not match key kind {:?}",
            kind, key_kind
        ));
    }
    let schema_id = i32::from_be_bytes(bytes[6..10].try_into().expect("fixed schema id slice"));
    let fingerprint_len = u16::from_be_bytes(
        bytes[10..12]
            .try_into()
            .expect("fixed fingerprint length slice"),
    ) as usize;
    let fingerprint_end = HEADER_BYTES_BEFORE_FINGERPRINT
        .checked_add(fingerprint_len)
        .ok_or_else(|| "MV Accelerator fingerprint length overflows".to_string())?;
    let operation_end = fingerprint_end
        .checked_add(OPERATION_ID_BYTES)
        .ok_or_else(|| "MV Accelerator operation ID length overflows".to_string())?;
    let payload_length_end = operation_end
        .checked_add(PAYLOAD_LENGTH_BYTES)
        .ok_or_else(|| "MV Accelerator payload length overflows".to_string())?;
    if payload_length_end > bytes.len() {
        return Err("MV Accelerator envelope is truncated before payload".to_string());
    }
    let fingerprint = std::str::from_utf8(&bytes[HEADER_BYTES_BEFORE_FINGERPRINT..fingerprint_end])
        .map_err(|_| "MV Accelerator fingerprint is not ASCII".to_string())?;
    if !fingerprint.is_ascii() {
        return Err("MV Accelerator fingerprint is not ASCII".to_string());
    }
    let operation_id = Uuid::from_slice(&bytes[fingerprint_end..operation_end])
        .map_err(|error| format!("MV Accelerator operation ID is invalid: {error}"))?;
    let payload_len = u32::from_be_bytes(
        bytes[operation_end..payload_length_end]
            .try_into()
            .expect("fixed payload length slice"),
    ) as usize;
    let payload_end = payload_length_end
        .checked_add(payload_len)
        .ok_or_else(|| "MV Accelerator payload length overflows".to_string())?;
    if payload_end != bytes.len() {
        return Err("MV Accelerator payload length does not match exact record size".to_string());
    }
    let catalog = schema_catalog()?;
    let writer = catalog.entry(kind.subject(), schema_id)?;
    if writer.fingerprint() != fingerprint {
        return Err(format!(
            "MV Accelerator Avro schema fingerprint mismatch for {} schema {}",
            kind.subject(),
            schema_id
        ));
    }
    let reader = catalog.latest(kind.subject())?;
    let payload = &bytes[payload_length_end..payload_end];
    let mut cursor = Cursor::new(payload);
    let datum =
        from_avro_datum(writer.schema(), &mut cursor, Some(reader.schema())).map_err(|error| {
            format!(
                "decode MV Accelerator Avro payload for {} writer {} reader {} failed: {error}",
                kind.subject(),
                writer.id(),
                reader.id()
            )
        })?;
    if cursor.position() != payload.len() as u64 {
        return Err("MV Accelerator Avro payload has trailing bytes".to_string());
    }
    let value = from_value(&datum)
        .map_err(|error| format!("materialize MV Accelerator Avro payload failed: {error}"))?;
    Ok(DecodedMvRecord {
        operation_id,
        value,
    })
}
