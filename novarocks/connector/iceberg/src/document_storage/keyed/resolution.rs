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
    KeyedDocumentView, KeyedSnapshotObservation, UnknownCommitEvidence, UnknownCommitIdentity,
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
    let mut snapshots = BTreeMap::new();
    for snapshot in &full.snapshots {
        if snapshots.insert(snapshot.snapshot_id, snapshot).is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "full commit observation repeats a snapshot ID",
            ));
        }
    }
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
