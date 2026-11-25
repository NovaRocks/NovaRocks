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

//! Same-dispatch facts for a full materialized view refresh.

use super::{
    model::{AttemptToken, BaseIdentity, FrozenRequest},
    staging::StagedView,
};
use crate::iceberg::{Error, ErrorKind, Result, TableUpdate, spec::Operation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const FULL_REFRESH_STAGE_SUMMARY_KEY: &str = "novarocks.mv.full-refresh-stage.v1";

// Design: ADR-0268 (docs/adr/ADR-0268-iceberg-mv-converges-from-the-original-commit.md)
/// The private delete witness binds every selected snapshot to one actual
/// attempt. It is evidence even when expiry has removed the append snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FullRefreshStageWitness {
    version: u8,
    table_uuid: Uuid,
    operation_id: [u8; 16],
    attempt_ordinal: u32,
    attempt_nonce: [u8; 16],
    asserted_parent: Option<i64>,
    delete_snapshot: i64,
    append_snapshot: i64,
}

impl FullRefreshStageWitness {
    fn new(
        uuid: Uuid,
        attempt: AttemptToken,
        parent: Option<i64>,
        delete_snapshot: i64,
        append_snapshot: i64,
    ) -> Self {
        Self {
            version: 1,
            table_uuid: uuid,
            operation_id: attempt.operation().to_bytes(),
            attempt_ordinal: attempt.ordinal(),
            attempt_nonce: attempt.nonce_bytes(),
            asserted_parent: parent,
            delete_snapshot,
            append_snapshot,
        }
    }

    pub(crate) fn prepare(view: &StagedView<'_>, delete: i64) -> Result<String> {
        if view.stage_key() != 0 {
            return Err(invalid(
                "full refresh delete is not the first snapshot stage",
            ));
        }
        let append = view.artifacts().stage_snapshot_id(1, view.metadata())?;
        let witness = Self::new(
            view.metadata().uuid(),
            view.artifacts().attempt_token(),
            view.round_parent(),
            delete,
            append,
        );
        serde_json::to_string(&witness).map_err(|error| invalid(error.to_string()))
    }

    pub(crate) fn verify(request: &FrozenRequest) -> Result<(i64, String)> {
        let BaseIdentity::Existing { uuid, parent, .. } = request.base() else {
            return Err(invalid("full refresh witness requires an existing table"));
        };
        let mut snapshots = request.updates().iter().filter_map(|update| match update {
            TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
            _ => None,
        });
        let (Some(delete), Some(append), None) =
            (snapshots.next(), snapshots.next(), snapshots.next())
        else {
            return Err(invalid(
                "full refresh witness requires exactly two selected snapshots",
            ));
        };
        if delete.summary().operation != Operation::Delete
            || delete.parent_snapshot_id() != *parent
            || append.summary().operation != Operation::Append
            || append.parent_snapshot_id() != Some(delete.snapshot_id())
            || request.ref_snapshot_after("main") != Some(append.snapshot_id())
        {
            return Err(invalid(
                "full refresh selection differs from the frozen dispatch",
            ));
        }
        let witness = Self::new(
            *uuid,
            request.artifacts().attempt(),
            *parent,
            delete.snapshot_id(),
            append.snapshot_id(),
        );
        let expected =
            serde_json::to_string(&witness).map_err(|error| invalid(error.to_string()))?;
        if delete
            .summary()
            .additional_properties
            .get(FULL_REFRESH_STAGE_SUMMARY_KEY)
            != Some(&expected)
        {
            return Err(invalid("full refresh delete lost its exact stage witness"));
        }
        Ok((delete.snapshot_id(), expected))
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

use crate::iceberg::spec::{Snapshot, TableMetadata};
use novarocks_spi::connector::{
    ConnectorTableObjectId, KeyedDispatchDeleteStage, KeyedDispatchRecord, KeyedSnapshotDispatch,
    UnknownCommitEvidence, UnknownCommitIdentity,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use super::write_stack::control::{
    ICEBERG_WRITE_SESSION_MARKER_PROPERTY, MANAGED_REPARTITION_EFFECT_SUMMARY_KEY,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KeyedDeleteSelection {
    snapshot: i64,
    witness: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KeyedSnapshotSelection {
    pub(crate) table_uuid: Uuid,
    pub(crate) parent: Option<i64>,
    pub(crate) snapshot: i64,
    pub(crate) delete: Option<KeyedDeleteSelection>,
    records: BTreeMap<String, [u8; 32]>,
    effect: Option<String>,
}

/// Read-only facts from one observed Iceberg snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum KeyedSnapshotObservation {
    Delete {
        snapshot: i64,
        parent: Option<i64>,
        witness: String,
    },
    Records {
        snapshot: i64,
        parent: Option<i64>,
        records: BTreeMap<String, [u8; 32]>,
        effect: Option<String>,
    },
}

impl KeyedSnapshotSelection {
    pub(crate) fn frozen(request: &FrozenRequest) -> Result<Option<Self>> {
        use crate::document_storage::keyed::keys::{DocumentKey, RECORD_PREFIX, parse_key};
        let BaseIdentity::Existing { uuid, parent, .. } = request.base() else {
            return Ok(None);
        };
        if request.target_ref() != "main" {
            return Ok(None);
        }
        let Some(snapshot) = request.ref_snapshot_after("main") else {
            return Ok(None);
        };
        let Some(selected) = request.updates().iter().find_map(|update| match update {
            TableUpdate::AddSnapshot { snapshot: value } if value.snapshot_id() == snapshot => {
                Some(value)
            }
            _ => None,
        }) else {
            return Ok(None);
        };
        let summary = &selected.summary().additional_properties;
        let has_write_session = summary.contains_key(ICEBERG_WRITE_SESSION_MARKER_PROPERTY);
        let mut records = BTreeMap::new();
        for (key, value) in summary
            .iter()
            .filter(|(key, _)| key.starts_with(RECORD_PREFIX))
        {
            let Some(DocumentKey::Record { name, .. }) =
                parse_key(key, true).map_err(|error| invalid(error.to_string()))?
            else {
                continue;
            };
            if has_write_session
                || (name == "retirement" && selected.summary().operation == Operation::Append)
            {
                records.insert(key.clone(), Sha256::digest(value).into());
            }
        }
        if records.is_empty() {
            return Ok(None);
        }
        let delete = if request.updates().iter().any(|update| {
            matches!(update, TableUpdate::AddSnapshot { snapshot } if
                snapshot.summary().additional_properties.contains_key(FULL_REFRESH_STAGE_SUMMARY_KEY))
        }) {
            let (snapshot, witness) = FullRefreshStageWitness::verify(request)?;
            Some(KeyedDeleteSelection { snapshot, witness })
        } else {
            None
        };
        // Older direct staging fixtures use an unadorned two-snapshot request.
        // They retain the original attribution instead of claiming this proof.
        if selected.parent_snapshot_id() != delete.as_ref().map(|stage| stage.snapshot).or(*parent)
        {
            return Ok(None);
        }
        Ok(Some(Self {
            table_uuid: *uuid,
            parent: *parent,
            snapshot,
            delete,
            records,
            effect: summary.get(MANAGED_REPARTITION_EFFECT_SUMMARY_KEY).cloned(),
        }))
    }

    pub(crate) fn to_evidence(
        &self,
        operation_id: [u8; 16],
        target: ConnectorTableObjectId,
        frames: &BTreeMap<String, String>,
    ) -> Result<UnknownCommitEvidence> {
        use crate::document_storage::keyed::keys::{DocumentKey, parse_key};
        let mut records = Vec::with_capacity(self.records.len());
        for (key, digest) in &self.records {
            let frame = frames
                .get(key)
                .ok_or_else(|| invalid("keyed dispatch lost its exact record frame"))?;
            let actual: [u8; 32] = Sha256::digest(frame).into();
            if &actual != digest {
                return Err(invalid(
                    "keyed dispatch record does not match the original frozen request",
                ));
            }
            let Some(DocumentKey::Record { owner, name }) =
                parse_key(key, true).map_err(|error| invalid(error.to_string()))?
            else {
                return Err(invalid("keyed dispatch record has no exact snapshot key"));
            };
            records.push(KeyedDispatchRecord {
                owner,
                name,
                envelope: bytes::Bytes::copy_from_slice(frame.as_bytes()),
            });
        }
        Ok(UnknownCommitEvidence {
            asserted_parent: self.parent,
            preselected_snapshot: self.snapshot,
            identity: UnknownCommitIdentity::SnapshotDispatch(Box::new(KeyedSnapshotDispatch {
                target,
                operation_id: novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
                    operation_id,
                ),
                delete_stage: self.delete.as_ref().map(|stage| KeyedDispatchDeleteStage {
                    snapshot: stage.snapshot,
                    witness: stage.witness.clone(),
                }),
                records,
                repartition_effect: self.effect.clone(),
            })),
        })
    }

    pub(crate) fn observe(&self, metadata: &TableMetadata) -> Option<KeyedSnapshotObservation> {
        if self.table_uuid != metadata.uuid() {
            return None;
        }
        if let Some(observed) = metadata
            .snapshot_by_id(self.snapshot)
            .and_then(|snapshot| self.observe_records(snapshot))
        {
            return Some(observed);
        }
        if let Some(delete) = &self.delete
            && let Some(snapshot) = metadata.snapshot_by_id(delete.snapshot)
            && let Some(witness) = snapshot
                .summary()
                .additional_properties
                .get(FULL_REFRESH_STAGE_SUMMARY_KEY)
        {
            let observed = KeyedSnapshotObservation::Delete {
                snapshot: snapshot.snapshot_id(),
                parent: snapshot.parent_snapshot_id(),
                witness: witness.clone(),
            };
            if self.matches(&observed) {
                return Some(observed);
            }
        }
        if self.effect.is_none() {
            for snapshot in metadata.snapshots() {
                if snapshot.parent_snapshot_id() == Some(self.snapshot)
                    && let Some(observed) = self.observe_records(snapshot)
                {
                    return Some(observed);
                }
            }
        }
        None
    }

    fn observe_records(&self, snapshot: &Snapshot) -> Option<KeyedSnapshotObservation> {
        let summary = &snapshot.summary().additional_properties;
        let mut records = BTreeMap::new();
        for key in self.records.keys() {
            records.insert(key.clone(), Sha256::digest(summary.get(key)?).into());
        }
        let observed = KeyedSnapshotObservation::Records {
            snapshot: snapshot.snapshot_id(),
            parent: snapshot.parent_snapshot_id(),
            records,
            effect: summary.get(MANAGED_REPARTITION_EFFECT_SUMMARY_KEY).cloned(),
        };
        self.matches(&observed).then_some(observed)
    }

    pub(crate) fn matches(&self, observed: &KeyedSnapshotObservation) -> bool {
        match observed {
            KeyedSnapshotObservation::Delete {
                snapshot,
                parent,
                witness,
            } => self.delete.as_ref().is_some_and(|delete| {
                *snapshot == delete.snapshot && *parent == self.parent && *witness == delete.witness
            }),
            KeyedSnapshotObservation::Records {
                snapshot,
                parent,
                records,
                effect,
            } => {
                let receipt_parent = self
                    .delete
                    .as_ref()
                    .map(|delete| delete.snapshot)
                    .or(self.parent);
                records == &self.records
                    && if *snapshot == self.snapshot {
                        *parent == receipt_parent && effect == &self.effect
                    } else {
                        self.effect.is_none() && *parent == Some(self.snapshot)
                    }
            }
        }
    }
}
