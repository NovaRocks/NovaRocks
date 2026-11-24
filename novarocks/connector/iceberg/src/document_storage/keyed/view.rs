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

use super::{
    envelope::{decode_marker, decode_slot, decode_version},
    keys::{DocumentKey, corrupt, is_legacy_key, parse_key, slot_key, version_key},
    record::decode_record,
};
use crate::iceberg::spec::TableMetadata;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorDocumentVersion, ConnectorError, ConnectorErrorKind,
    ConnectorManagedObjectMarker, ConnectorMarkerKey, ConnectorMarkerValue, ConnectorObservedSlot,
    ConnectorSlotName, ConnectorSnapshotRecord, DocumentIdentity, FullKeyedDocumentView,
    FullObservationToken, KeyedDocumentObservationRequest, KeyedDocumentReadError,
    KeyedDocumentRoot, KeyedDocumentView, KeyedSnapshotObservation, SnapshotFileTotals,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Resolve semantic links without observing any historical snapshot. All IDs in
/// the returned set have an available version; missing links and cycles fail.
pub(crate) fn reference_closure(
    versions: &BTreeMap<DocumentIdentity, ConnectorDocumentVersion>,
    roots: impl IntoIterator<Item = DocumentIdentity>,
) -> Result<BTreeSet<DocumentIdentity>, ConnectorError> {
    let mut live = BTreeSet::new();
    let mut active = BTreeSet::new();
    let mut pending: Vec<_> = roots.into_iter().map(|id| (id, false)).collect();
    while let Some((id, exit)) = pending.pop() {
        if exit {
            active.remove(&id);
            live.insert(id);
            continue;
        }
        if live.contains(&id) {
            continue;
        }
        if !active.insert(id) {
            return Err(corrupt(&version_key(id), "cyclic document references"));
        }
        let version = versions
            .get(&id)
            .ok_or_else(|| corrupt(&version_key(id), "missing referenced document version"))?;
        pending.push((id, true));
        for target in version.references().values().rev() {
            pending.push((*target, false));
        }
    }
    Ok(live)
}

pub(crate) fn parse_snapshot_records(
    properties: &HashMap<String, String>,
    detect_legacy: bool,
) -> Result<Vec<ConnectorSnapshotRecord>, KeyedDocumentReadError> {
    let mut entries: Vec<_> = properties.iter().collect();
    entries.sort_by_key(|(key, _)| *key);
    let mut records = Vec::new();
    for (key, value) in entries {
        // Legacy detection is defined on table properties and the current
        // summary. Historical summaries never become a current interpretation.
        if !detect_legacy && is_legacy_key(key) {
            continue;
        }
        if let Some(DocumentKey::Record { .. }) = parse_key(key, true)? {
            records.push(decode_record(key, value)?);
        }
    }
    Ok(records)
}

/// Pure table-property projection for commit preparation on its exact staged
/// baseline. It does not fabricate an observation request or load metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct KeyedProperties {
    pub slots: BTreeMap<ConnectorSlotName, ConnectorObservedSlot>,
    pub markers: BTreeMap<ConnectorMarkerKey, ConnectorMarkerValue>,
    pub versions: BTreeMap<DocumentIdentity, ConnectorDocumentVersion>,
    pub roots: BTreeMap<String, KeyedDocumentRoot>,
}

pub(crate) fn parse_properties(
    properties: &HashMap<String, String>,
) -> Result<KeyedProperties, KeyedDocumentReadError> {
    if let Some(key) = properties.keys().filter(|key| is_legacy_key(key)).min() {
        return Err(KeyedDocumentReadError::LegacyFormat { key: key.clone() });
    }
    let mut slots = BTreeMap::new();
    let mut markers = BTreeMap::new();
    let mut versions = BTreeMap::new();
    let mut roots = BTreeMap::new();
    let mut entries: Vec<_> = properties.iter().collect();
    entries.sort_by_key(|(key, _)| *key);
    for (key, value) in entries {
        match parse_key(key, false)? {
            Some(DocumentKey::Version(id)) => {
                versions.insert(id, decode_version(key, value)?);
            }
            Some(DocumentKey::Slot(_)) => {
                let (name, slot) = decode_slot(key, value)?;
                slots.insert(name, slot);
            }
            Some(DocumentKey::Marker(_)) => {
                let (name, marker) = decode_marker(key, value)?;
                markers.insert(name, marker);
            }
            Some(DocumentKey::Root(owner)) => {
                let identity = DocumentIdentity::parse_hex(value).map_err(|e| corrupt(key, e))?;
                roots.insert(
                    owner,
                    KeyedDocumentRoot {
                        identity,
                        dangling: false,
                    },
                );
            }
            Some(DocumentKey::Record { .. }) => unreachable!("parse_key validates location"),
            None => {}
        }
    }
    for root in roots.values_mut() {
        root.dangling = reference_closure(&versions, Some(root.identity)).is_err();
    }
    Ok(KeyedProperties {
        slots,
        markers,
        versions,
        roots,
    })
}

/// Validate current interpretation before granting a write or discovery result.
/// A root may dangle, while the current records must always resolve exactly.
pub(crate) fn management_marker(
    metadata: &TableMetadata,
) -> Result<Option<ConnectorManagedObjectMarker>, KeyedDocumentReadError> {
    let properties = parse_properties(metadata.properties())?;
    if let Some(snapshot) = metadata.current_snapshot() {
        for record in parse_snapshot_records(&snapshot.summary().additional_properties, true)? {
            reference_closure(&properties.versions, record.binding)?;
        }
    }
    let name = ConnectorSlotName {
        owner: "novarocks.storage".into(),
        name: "management".into(),
    };
    properties
        .slots
        .get(&name)
        .map(|slot| decode_management(&slot_key(&name), &slot.value.content))
        .transpose()
        .map_err(Into::into)
}

pub(crate) fn project_view(
    metadata: &TableMetadata,
    request: &KeyedDocumentObservationRequest,
    metadata_version: ConnectorCommittedVersion,
) -> Result<KeyedDocumentView, KeyedDocumentReadError> {
    request.validate()?;
    metadata_version.validate()?;
    let current_properties = metadata
        .current_snapshot()
        .map(|s| &s.summary().additional_properties);
    if let Some(key) = metadata
        .properties()
        .keys()
        .chain(current_properties.into_iter().flat_map(|p| p.keys()))
        .filter(|key| is_legacy_key(key))
        .min()
    {
        return Err(KeyedDocumentReadError::LegacyFormat { key: key.clone() });
    }
    if metadata_version.snapshot_id() != metadata.current_snapshot_id() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "keyed observation metadata version has a different current snapshot",
        )
        .into());
    }
    if request.object_id.as_bytes().as_ref() != metadata.uuid().to_string().as_bytes() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "keyed document observation belongs to a different physical table",
        )
        .into());
    }
    let KeyedProperties {
        slots,
        markers,
        versions,
        roots,
    } = parse_properties(metadata.properties())?;
    let current_records = metadata
        .current_snapshot()
        .map(|s| parse_snapshot_records(&s.summary().additional_properties, true))
        .transpose()?
        .unwrap_or_default();
    for record in &current_records {
        reference_closure(&versions, record.binding)
            .map_err(|e| corrupt(&super::keys::record_key(&record.owner, &record.name), e))?;
    }
    let totals = metadata
        .current_snapshot()
        .map(|snapshot| {
            let values = &snapshot.summary().additional_properties;
            let total = |name: &str| {
                values
                    .get(name)
                    .map(|value| value.parse::<u64>().map_err(|error| corrupt(name, error)))
                    .transpose()
            };
            Ok::<_, ConnectorError>(SnapshotFileTotals {
                data_files: total("total-data-files")?,
                delete_files: total("total-delete-files")?,
            })
        })
        .transpose()?
        .unwrap_or_default();
    let management_name = ConnectorSlotName {
        owner: "novarocks.storage".into(),
        name: "management".into(),
    };
    let management = slots
        .get(&management_name)
        .map(|slot| decode_management(&slot_key(&management_name), &slot.value.content))
        .transpose()?;
    Ok(KeyedDocumentView {
        owner: request.owner.clone(),
        table: request.target.clone(),
        object_id: request.object_id.clone(),
        metadata_version,
        current_snapshot: metadata.current_snapshot_id(),
        current_snapshot_file_totals: totals,
        before_first_data_publication: proves_initial_slot_history(metadata),
        slots,
        markers,
        versions,
        roots,
        current_records,
        management,
    })
}

fn proves_initial_slot_history(metadata: &TableMetadata) -> bool {
    use crate::commit::slot_change::SLOT_CHANGE_SNAPSHOT_SUMMARY_KEY;
    let mut sequence = metadata.last_sequence_number();
    let mut next = metadata.current_snapshot_id();
    while let Some(id) = next {
        let Some(snapshot) = metadata.snapshot_by_id(id) else {
            return false;
        };
        if sequence <= 0
            || snapshot.sequence_number() != sequence
            || snapshot
                .summary()
                .additional_properties
                .get(SLOT_CHANGE_SNAPSHOT_SUMMARY_KEY)
                .map(String::as_str)
                != Some("true")
        {
            return false;
        }
        sequence -= 1;
        next = snapshot.parent_snapshot_id();
    }
    sequence == 0
}

pub(crate) fn project_full_view(
    metadata: &TableMetadata,
    request: &KeyedDocumentObservationRequest,
    metadata_version: ConnectorCommittedVersion,
) -> Result<FullKeyedDocumentView, KeyedDocumentReadError> {
    let current = project_view(metadata, request, metadata_version.clone())?;
    let mut snapshots = Vec::new();
    let mut diagnostics = Vec::new();
    for snapshot in metadata.snapshots() {
        let snapshot_records = parse_snapshot_records(
            &snapshot.summary().additional_properties,
            snapshot.snapshot_id() == metadata.current_snapshot_id().unwrap_or(-1),
        )?;
        if Some(snapshot.snapshot_id()) != metadata.current_snapshot_id() {
            for record in &snapshot_records {
                if let Err(error) = reference_closure(&current.versions, record.binding) {
                    diagnostics.push(format!(
                        "history cannot be interpreted: snapshot {} record {}: {error}",
                        snapshot.snapshot_id(),
                        super::keys::record_key(&record.owner, &record.name)
                    ));
                }
            }
        }
        snapshots.push(KeyedSnapshotObservation {
            snapshot_id: snapshot.snapshot_id(),
            parent: snapshot.parent_snapshot_id(),
            records: snapshot_records,
            slot_storage_effect_digest: snapshot
                .summary()
                .additional_properties
                .get(crate::commit::slot_change::SLOT_STORAGE_EFFECT_SUMMARY_KEY)
                .and_then(|value| DocumentIdentity::parse_hex(value).ok())
                .map(DocumentIdentity::to_bytes),
        });
    }
    snapshots.sort_by_key(|s| s.snapshot_id);
    diagnostics.sort();
    let payload = serde_json::to_vec(metadata).map(Bytes::from).map_err(|e| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("serialize full document observation: {e}"),
        )
    })?;
    let token = FullObservationToken::try_new(
        request.owner.clone(),
        request.target.clone(),
        request.object_id.clone(),
        metadata_version,
        payload,
    )?;
    let refs = metadata
        .refs()
        .iter()
        .map(|(name, reference)| (name.clone(), reference.snapshot_id))
        .collect();
    Ok(FullKeyedDocumentView {
        current,
        snapshots,
        refs,
        diagnostics,
        token,
    })
}

pub(crate) fn decode_management(
    key: &str,
    mut content: &[u8],
) -> Result<ConnectorManagedObjectMarker, ConnectorError> {
    fn next<'a>(key: &str, content: &mut &'a [u8]) -> Result<&'a str, ConnectorError> {
        let length_bytes = content
            .get(..4)
            .ok_or_else(|| corrupt(key, "truncated management length"))?;
        let length = u32::from_be_bytes(length_bytes.try_into().expect("checked length")) as usize;
        *content = &content[4..];
        let value = content
            .get(..length)
            .ok_or_else(|| corrupt(key, "truncated management value"))?;
        *content = &content[length..];
        std::str::from_utf8(value).map_err(|e| corrupt(key, e))
    }
    let kind = next(key, &mut content)?;
    let owner = next(key, &mut content)?;
    let incarnation = next(key, &mut content)?;
    if !content.is_empty() {
        return Err(corrupt(key, "trailing management bytes"));
    }
    ConnectorManagedObjectMarker::try_new(kind, owner, incarnation).map_err(|e| corrupt(key, e))
}

/// Exact same-load metadata retained by a full token. Admission validates its
/// provider/table scope before use; this function never fetches a newer base.
pub(crate) fn metadata_from_token(
    token: &FullObservationToken,
) -> Result<TableMetadata, ConnectorError> {
    let metadata: TableMetadata =
        serde_json::from_slice(token.payload()).map_err(|e| corrupt("full observation", e))?;
    if token.object_id().as_bytes().as_ref() != metadata.uuid().to_string().as_bytes()
        || token.metadata_version().snapshot_id() != metadata.current_snapshot_id()
    {
        return Err(corrupt(
            "full observation",
            "physical object scope mismatch",
        ));
    }
    Ok(metadata)
}
