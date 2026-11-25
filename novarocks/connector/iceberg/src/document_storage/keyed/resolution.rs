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

//! Evidence classification never cleans, reissues or aborts external work.

use super::record::encode_record;
use novarocks_spi::connector::{
    CommitConclusion, CommitProof, ConnectorError, ConnectorErrorKind, FullKeyedDocumentView,
    KeyedDispatchRecord, KeyedDocumentView, KeyedSnapshotDispatch, KeyedSnapshotObservation,
    UnknownCommitEvidence, UnknownCommitIdentity,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Resolution {
    Conclusion(CommitConclusion),
    NeedFull,
}

/// A full observation may be newer than the initial refs observation. Keep any
/// positive fact from either read; a later expiry cannot revoke that proof.
pub(crate) fn resolve(
    refs: &KeyedDocumentView,
    full: Option<&FullKeyedDocumentView>,
    evidence: &UnknownCommitEvidence,
) -> Result<Resolution, ConnectorError> {
    if let Some(full) = full
        && (refs.owner != full.current.owner
            || refs.table != full.current.table
            || refs.object_id != full.current.object_id)
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "commit observations belong to different provider or table scopes",
        ));
    }
    if let UnknownCommitIdentity::SnapshotDispatch(dispatch) = &evidence.identity {
        if refs.object_id != dispatch.target {
            return Ok(Resolution::Conclusion(
                CommitConclusion::InvalidatedUndetermined,
            ));
        }
        let Some(full) = full else {
            return Ok(Resolution::NeedFull);
        };
        let snapshots = index_snapshots(&full.snapshots)?;
        return resolve_snapshot_dispatch(
            full.current.current_snapshot,
            &snapshots,
            evidence,
            dispatch,
        )
        .map(Resolution::Conclusion);
    }
    let combined_effect = match &evidence.identity {
        UnknownCommitIdentity::SlotStorageEffect { effect_digest, .. } => Some(effect_digest),
        _ => None,
    };
    if combined_effect.is_none()
        && let Some(proof) = immediate_proof(refs, evidence)
    {
        return Ok(committed(proof));
    }
    let Some(full) = full else {
        return Ok(
            if combined_effect.is_none() && refs.current_snapshot == evidence.asserted_parent {
                Resolution::Conclusion(CommitConclusion::StillUnknown)
            } else {
                Resolution::NeedFull
            },
        );
    };
    if combined_effect.is_none()
        && let Some(proof) = immediate_proof(&full.current, evidence)
    {
        return Ok(committed(proof));
    }
    let snapshots = index_snapshots(&full.snapshots)?;
    if let Some(digest) = combined_effect {
        // Main, SlotWriter and a parent edge do not independently identify
        // the frozen storage effect. Missing history leaves this unknown.
        return Ok(
            if snapshots
                .get(&evidence.preselected_snapshot)
                .is_some_and(|snapshot| {
                    snapshot.parent == evidence.asserted_parent
                        && snapshot.slot_storage_effect_digest.as_ref() == Some(digest)
                })
            {
                committed(CommitProof::Snapshot)
            } else {
                Resolution::Conclusion(CommitConclusion::StillUnknown)
            },
        );
    }
    if let Some(snapshot) = snapshots.get(&evidence.preselected_snapshot) {
        let matches = match &evidence.identity {
            UnknownCommitIdentity::RecordBytes { owner, name, bytes } => {
                has_record(snapshot, owner, name, bytes)?
            }
            _ => true,
        };
        if matches {
            return Ok(committed(CommitProof::Snapshot));
        }
    }
    if full
        .snapshots
        .iter()
        .any(|s| s.parent == Some(evidence.preselected_snapshot))
    {
        return Ok(committed(CommitProof::ParentPointer));
    }
    if let UnknownCommitIdentity::RecordBytes { owner, name, bytes } = &evidence.identity {
        for snapshot in &full.snapshots {
            if has_record(snapshot, owner, name, bytes)? {
                return Ok(committed(CommitProof::CarriedRecord));
            }
        }
    }
    if full.current.current_snapshot == evidence.asserted_parent {
        return Ok(Resolution::Conclusion(CommitConclusion::StillUnknown));
    }
    // A visible but mismatching selected snapshot is not negative evidence.
    // Snapshot absence by itself is also insufficient: follow the complete
    // main ancestry back to the asserted parent, or to the first snapshot.
    let appeared = snapshots.contains_key(&evidence.preselected_snapshot)
        || full
            .snapshots
            .iter()
            .any(|s| s.parent == Some(evidence.preselected_snapshot));
    if !appeared
        && has_negative_ancestry(
            &snapshots,
            full.current.current_snapshot,
            evidence.asserted_parent,
        )
    {
        return Ok(Resolution::Conclusion(CommitConclusion::NotCommitted(
            CommitProof::NegativeProof,
        )));
    }
    Ok(Resolution::Conclusion(
        CommitConclusion::InvalidatedUndetermined,
    ))
}

pub(crate) fn index_snapshots(
    entries: &[KeyedSnapshotObservation],
) -> Result<BTreeMap<i64, &KeyedSnapshotObservation>, ConnectorError> {
    let mut snapshots = BTreeMap::new();
    for snapshot in entries {
        if snapshots.insert(snapshot.snapshot_id, snapshot).is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "full commit observation repeats a snapshot ID",
            ));
        }
    }
    Ok(snapshots)
}

pub(crate) fn resolve_snapshot_dispatch(
    current: Option<i64>,
    snapshots: &BTreeMap<i64, &KeyedSnapshotObservation>,
    evidence: &UnknownCommitEvidence,
    dispatch: &KeyedSnapshotDispatch,
) -> Result<CommitConclusion, ConnectorError> {
    let selected = evidence.preselected_snapshot;
    let parent = evidence.asserted_parent;
    let delete = dispatch.delete_stage.as_ref();
    let receipt_parent = delete.map(|delete| delete.snapshot).or(parent);
    if let Some(snapshot) = snapshots.get(&selected)
        && snapshot.parent == receipt_parent
        && snapshot.managed_repartition_effect == dispatch.repartition_effect
        && has_dispatch_records(snapshot, &dispatch.records)?
    {
        return Ok(CommitConclusion::Committed(CommitProof::Snapshot));
    }
    if let Some(delete) = delete
        && let Some(snapshot) = snapshots.get(&delete.snapshot)
        && snapshot.parent == parent
        && snapshot.full_refresh_stage_witness.as_deref() == Some(delete.witness.as_str())
    {
        return Ok(CommitConclusion::Committed(CommitProof::Snapshot));
    }
    // A carried frame also needs the lineage to the selected append. A bare
    // pointer, or an equal app payload on a different branch, does not suffice.
    let mut observed_record = false;
    for snapshot in snapshots.values() {
        if has_dispatch_records(snapshot, &dispatch.records)? {
            observed_record = true;
            if dispatch.repartition_effect.is_none() && snapshot.parent == Some(selected) {
                return Ok(CommitConclusion::Committed(CommitProof::CarriedRecord));
            }
        }
    }
    if current == parent {
        return Ok(CommitConclusion::StillUnknown);
    }
    let appears = |id| {
        snapshots.contains_key(&id) || snapshots.values().any(|value| value.parent == Some(id))
    };
    if !observed_record
        && !appears(selected)
        && delete.is_none_or(|delete| !appears(delete.snapshot))
        && has_negative_ancestry(snapshots, current, parent)
    {
        return Ok(CommitConclusion::NotCommitted(CommitProof::NegativeProof));
    }
    Ok(CommitConclusion::InvalidatedUndetermined)
}

fn has_dispatch_records(
    snapshot: &KeyedSnapshotObservation,
    records: &[KeyedDispatchRecord],
) -> Result<bool, ConnectorError> {
    if records.is_empty() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "managed snapshot evidence has no record frame",
        ));
    }
    for record in records {
        if !has_record(snapshot, &record.owner, &record.name, &record.envelope)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn committed(proof: CommitProof) -> Resolution {
    Resolution::Conclusion(CommitConclusion::Committed(proof))
}
fn immediate_proof(
    view: &KeyedDocumentView,
    evidence: &UnknownCommitEvidence,
) -> Option<CommitProof> {
    if view.current_snapshot == Some(evidence.preselected_snapshot) {
        return Some(CommitProof::Main);
    }
    if let UnknownCommitIdentity::SlotWriter { slot } = &evidence.identity
        && view
            .slots
            .get(slot)
            .is_some_and(|s| s.writer == Some(evidence.preselected_snapshot))
    {
        return Some(CommitProof::SlotWriter);
    }
    None
}
fn has_record(
    snapshot: &KeyedSnapshotObservation,
    owner: &str,
    name: &str,
    bytes: &[u8],
) -> Result<bool, ConnectorError> {
    for record in &snapshot.records {
        if record.owner == owner
            && record.name == name
            && encode_record(record)?.as_bytes() == bytes
        {
            return Ok(true);
        }
    }
    Ok(false)
}
fn has_negative_ancestry(
    snapshots: &BTreeMap<i64, &KeyedSnapshotObservation>,
    mut current: Option<i64>,
    parent: Option<i64>,
) -> bool {
    if parent.is_some_and(|id| !snapshots.contains_key(&id)) {
        return false;
    }
    let mut visited = BTreeSet::new();
    while let Some(id) = current {
        if !visited.insert(id) {
            return false;
        }
        let Some(snapshot) = snapshots.get(&id) else {
            return false;
        };
        if snapshot.parent == parent {
            return true;
        }
        // Encountering an older chain end without the asserted parent is a
        // rollback or an incomplete relationship, never a complete refutation.
        current = snapshot.parent;
    }
    false
}

#[cfg(test)]
#[path = "resolution_tests.rs"]
mod tests;
