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

//! Main-conditioned, zero-file slot changes. No conflict may rebase this intent.
// Design: ADR-0180 (docs/adr/ADR-0180-inline-keyed-documents-and-snapshot-records.md)

use super::{
    model::{Dependency, OperationIntent, RequestShape},
    staging::{PreparedChange, Preparer, StagedView},
};
use crate::document_storage::keyed::{envelope, keys, record, view};
use crate::iceberg::spec::{Operation, TableMetadata};
use crate::iceberg::{Error, ErrorKind, Result, TableUpdate};
use async_trait::async_trait;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, KeyedDocumentReadError, KeyedSlotChange,
};
use std::collections::{BTreeSet, HashMap};

pub(crate) fn document_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        format!("keyed document commit: {error}"),
    )
    .with_source(error)
}
fn read_error(error: KeyedDocumentReadError) -> Error {
    match error {
        KeyedDocumentReadError::Invalid(error) => document_error(error),
        legacy @ KeyedDocumentReadError::LegacyFormat { .. } => {
            Error::new(ErrorKind::FeatureUnsupported, legacy.to_string()).with_source(legacy)
        }
    }
}

pub(crate) struct RecordCarry {
    pub summary: HashMap<String, String>,
    pub properties: HashMap<String, String>,
}
/// Reassert the exact parent's record and root closures. A stale root is
/// repaired only from a carried binding belonging to that root's owner.
pub(crate) fn carry_records_and_closure(metadata: &TableMetadata) -> Result<RecordCarry> {
    let properties = view::parse_properties(metadata.properties()).map_err(read_error)?;
    let records = metadata
        .current_snapshot()
        .map(|snapshot| {
            view::parse_snapshot_records(&snapshot.summary().additional_properties, true)
        })
        .transpose()
        .map_err(read_error)?
        .unwrap_or_default();
    let mut live = BTreeSet::new();
    let mut rewrites = HashMap::new();
    let mut summary = HashMap::new();
    for entry in &records {
        live.extend(
            view::reference_closure(&properties.versions, entry.binding).map_err(document_error)?,
        );
        summary.insert(
            keys::record_key(&entry.owner, &entry.name),
            record::encode_record(entry).map_err(document_error)?,
        );
    }
    for (owner, root) in &properties.roots {
        if !root.dangling {
            live.extend(
                view::reference_closure(&properties.versions, Some(root.identity))
                    .map_err(document_error)?,
            );
        } else {
            let candidates: BTreeSet<_> = records
                .iter()
                .filter(|record| &record.owner == owner)
                .filter_map(|record| record.binding)
                .collect();
            if candidates.len() > 1 {
                return Err(document_error(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!("ambiguous carried bindings for {}", keys::root_key(owner)),
                )));
            }
            if let Some(binding) = candidates.first() {
                rewrites.insert(keys::root_key(owner), binding.to_hex());
            }
        }
    }
    for id in live {
        let version = properties.versions.get(&id).ok_or_else(|| {
            document_error(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("missing document version {}", id.to_hex()),
            ))
        })?;
        rewrites.insert(
            keys::version_key(id),
            envelope::encode_version(version).map_err(document_error)?,
        );
    }
    Ok(RecordCarry {
        summary,
        properties: rewrites,
    })
}

pub(crate) struct SlotChangePreparer {
    pub change: KeyedSlotChange,
}
#[async_trait]
impl Preparer for SlotChangePreparer {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        intent: &OperationIntent,
    ) -> Result<PreparedChange> {
        self.change.validate().map_err(document_error)?;
        if intent.shape() != RequestShape::SnapshotProducing
            || intent.target_ref() != "main"
            || !intent.dependencies().contains(&Dependency::RefUnchanged)
            || !intent.changes().added.is_empty()
            || !intent.changes().removed.is_empty()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Slot change requires a ref-frozen, zero-file main intent",
            ));
        }
        if intent
            .summary()
            .keys()
            .any(|key| key.starts_with(keys::PREFIX) || keys::is_legacy_key(key))
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Slot change summary cannot introduce document records or legacy document state",
            ));
        }
        let metadata = view.metadata();
        let parent = metadata.current_snapshot();
        let manifests = if let Some(parent) = &parent {
            view.artifacts().check_active()?;
            let list =
                crate::manifest::load_manifest_list(parent, view.artifacts().file_io(), metadata)
                    .await?;
            view.artifacts().check_active()?;
            list.entries().to_vec()
        } else {
            vec![]
        };
        let snapshot_id = super::staging::new_snapshot_id(metadata);
        let carry = carry_records_and_closure(metadata)?;
        let mut rewrites = carry.properties;
        for (name, value) in &self.change.slots {
            rewrites.insert(
                keys::slot_key(name),
                envelope::encode_slot(name, value, Some(snapshot_id)).map_err(document_error)?,
            );
        }
        let removals: Vec<_> = self
            .change
            .markers_remove
            .iter()
            .map(keys::marker_key)
            .collect();
        let operation = if parent.is_some() {
            Operation::Replace
        } else {
            Operation::Append
        };
        let carried_record_bytes = carry
            .summary
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        let mut change = super::overwrite::prepare_snapshot_change(
            view,
            intent,
            snapshot_id,
            operation,
            manifests,
            carry.summary,
            false,
        )
        .await?;
        change
            .updates
            .push(TableUpdate::SetProperties { updates: rewrites });
        if !removals.is_empty() {
            change
                .updates
                .push(TableUpdate::RemoveProperties { removals });
        }
        for name in self.change.slots.keys() {
            tracing::info!(slot = %keys::slot_key(name), preselected_snapshot_id = snapshot_id, "Prepared ordered document slot change");
        }
        crate::document_storage::keyed::write::trace_updates(
            metadata.properties(),
            &change.updates,
            carried_record_bytes,
        );
        Ok(change)
    }
}

#[cfg(test)]
mod tests {
    use super::super::model::{
        FileChanges, IsolationLevel, OperationIntentParts, StartSnapshot, TableTarget,
    };
    use super::super::overwrite::preparer_tests::Fixture;
    use super::*;
    use crate::iceberg::spec::FormatVersion;
    use crate::iceberg::{NamespaceIdent, TableIdent};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorDocumentFormat, ConnectorDocumentVersion, ConnectorSlotName, ConnectorSlotValue,
        ConnectorSnapshotRecord, DocumentIdentity,
    };
    use std::collections::BTreeMap;
    fn intent(fixture: &Fixture, metadata: &TableMetadata) -> OperationIntent {
        intent_with_summary(fixture, metadata, BTreeMap::new())
    }
    fn intent_with_summary(
        fixture: &Fixture,
        metadata: &TableMetadata,
        summary: BTreeMap<String, String>,
    ) -> OperationIntent {
        OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: TableIdent::new(NamespaceIdent::new("db".into()), "t".into()),
                uuid: Some(metadata.uuid()),
            },
            target_ref: "main".into(),
            start: metadata.current_snapshot().map(|s| StartSnapshot {
                snapshot_id: s.snapshot_id(),
                sequence_number: s.sequence_number(),
            }),
            changes: FileChanges::default(),
            dependencies: vec![Dependency::RefUnchanged],
            isolation: IsolationLevel::Snapshot,
            shape: RequestShape::SnapshotProducing,
            summary,
            token: fixture.operation.token(),
        })
        .unwrap()
    }
    fn change() -> SlotChangePreparer {
        SlotChangePreparer {
            change: KeyedSlotChange {
                slots: BTreeMap::from([(
                    ConnectorSlotName::try_new("novarocks.mv", "configuration").unwrap(),
                    ConnectorSlotValue {
                        format: ConnectorDocumentFormat::try_new(
                            "novarocks.mv",
                            "configuration",
                            1,
                        )
                        .unwrap(),
                        content: Bytes::from_static(b"same"),
                    },
                )]),
                markers_remove: BTreeSet::new(),
            },
        }
    }
    #[tokio::test]
    async fn slot_summary_cannot_introduce_an_unowned_document_record() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let forged = intent_with_summary(
            &fixture,
            &base,
            BTreeMap::from([(
                "novarocks.doc.v2.record.novarocks.mv~publication".into(),
                "2.-.eA".into(),
            )]),
        );
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut engine = super::super::staging::StagingEngine::begin(
            super::super::staging::StagingBase::Existing {
                metadata: base,
                metadata_location: "file:///exact-base.json".into(),
            },
            &forged,
            &attempt,
        )
        .unwrap();
        assert!(engine.stage(&change()).await.is_err());
        assert!(engine.updates().is_empty());
        assert!(fixture.operation.artifacts().unwrap().is_empty());
    }

    #[tokio::test]
    async fn first_slot_snapshot_is_empty_append_and_same_value_still_advances_main() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let (first, request) = fixture
            .stage(base.clone(), &intent(&fixture, &base), &change())
            .await;
        let parent = first.current_snapshot().unwrap();
        assert_eq!(parent.summary().operation, Operation::Append);
        assert!(request.requirements().contains(
            &crate::iceberg::TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".into(),
                snapshot_id: None
            }
        ));
        assert_eq!(
            parent
                .summary()
                .additional_properties
                .get("total-data-files")
                .map(String::as_str),
            Some("0")
        );
        let (second, request) = fixture
            .stage(first.clone(), &intent(&fixture, &first), &change())
            .await;
        let current = second.current_snapshot().unwrap();
        assert_ne!(current.snapshot_id(), parent.snapshot_id());
        assert_eq!(current.parent_snapshot_id(), Some(parent.snapshot_id()));
        assert_eq!(current.summary().operation, Operation::Replace);
        assert_ne!(current.manifest_list(), parent.manifest_list());
        let snapshot_updates = request
            .updates()
            .iter()
            .filter(|u| matches!(u, TableUpdate::AddSnapshot { .. }))
            .count();
        assert_eq!(snapshot_updates, 1);
        assert!(request.updates().iter().all(|u| matches!(
            u,
            TableUpdate::AddSnapshot { .. }
                | TableUpdate::SetSnapshotRef { .. }
                | TableUpdate::SetProperties { .. }
        )));
        let parsed = view::parse_properties(second.properties()).unwrap();
        let name = ConnectorSlotName::try_new("novarocks.mv", "configuration").unwrap();
        assert_eq!(parsed.slots[&name].writer, Some(current.snapshot_id()));
        assert_eq!(parsed.slots[&name].value, change().change.slots[&name]);
    }
    #[tokio::test]
    async fn carry_preserves_record_bytes_and_repairs_dangling_root() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let (first, _) = fixture
            .stage(base.clone(), &intent(&fixture, &base), &change())
            .await;
        let version = ConnectorDocumentVersion::try_new(
            "novarocks.mv",
            "binding",
            ConnectorDocumentFormat::try_new("novarocks.mv", "binding", 1).unwrap(),
            vec![],
            Bytes::from_static(b"stable"),
        )
        .unwrap();
        let record = ConnectorSnapshotRecord {
            owner: "novarocks.mv".into(),
            name: "publication".into(),
            binding: Some(version.identity()),
            payload: Bytes::from_static(b"publication"),
        };
        let record_key = keys::record_key(&record.owner, &record.name);
        let frame = record::encode_record(&record).unwrap();
        let mut json = serde_json::to_value(&first).unwrap();
        json["properties"][keys::version_key(version.identity())] =
            envelope::encode_version(&version).unwrap().into();
        json["properties"][keys::root_key("novarocks.mv")] =
            DocumentIdentity::from_bytes([9; 32]).to_hex().into();
        for snapshot in json["snapshots"].as_array_mut().unwrap() {
            if snapshot["snapshot-id"].as_i64() == first.current_snapshot_id() {
                snapshot["summary"][&record_key] = frame.clone().into();
            }
        }
        let bound: TableMetadata = serde_json::from_value(json).unwrap();
        let (after, request) = fixture
            .stage(bound.clone(), &intent(&fixture, &bound), &change())
            .await;
        assert_eq!(
            after
                .current_snapshot()
                .unwrap()
                .summary()
                .additional_properties[&record_key],
            frame
        );
        assert_eq!(
            after.properties()[&keys::root_key("novarocks.mv")],
            version.identity().to_hex()
        );
        let rewrites = request
            .updates()
            .iter()
            .find_map(|u| match u {
                TableUpdate::SetProperties { updates } => Some(updates),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            rewrites[&keys::version_key(version.identity())],
            envelope::encode_version(&version).unwrap()
        );
    }

    #[tokio::test]
    async fn slot_change_reuses_nonempty_data_and_delete_manifests_with_one_new_list() {
        use crate::commit::model::ArtifactWriter;
        use crate::iceberg::spec::ManifestContentType;

        let fixture = Fixture::new();
        let base = fixture.metadata_with_data_and_delete_manifests().await;
        let reader = fixture.operation.begin_attempt().unwrap();
        let parent = base.current_snapshot().unwrap();
        let before = crate::manifest::load_manifest_list(parent, reader.file_io(), &base)
            .await
            .unwrap();
        assert_eq!(before.entries().len(), 2);
        assert!(
            before
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Data)
        );
        assert!(
            before
                .entries()
                .iter()
                .any(|m| m.content == ManifestContentType::Deletes)
        );

        let (after, request) = fixture
            .stage(base.clone(), &intent(&fixture, &base), &change())
            .await;
        let current = after.current_snapshot().unwrap();
        let carried = crate::manifest::load_manifest_list(current, reader.file_io(), &after)
            .await
            .unwrap();
        // ManifestFile equality includes path, content, file/row counters, sequence,
        // original added snapshot and first-row-ID fields. No manifest is rewritten.
        assert_eq!(carried.entries(), before.entries());
        assert_eq!(current.summary().operation, Operation::Replace);
        assert_eq!(current.parent_snapshot_id(), Some(parent.snapshot_id()));
        assert_eq!(current.row_range(), Some((base.next_row_id(), 0)));
        assert_ne!(current.manifest_list(), parent.manifest_list());
        assert_eq!(request.artifacts().attempt_owned().len(), 1);
        assert_eq!(
            request.artifacts().attempt_owned()[0].path(),
            current.manifest_list()
        );
        assert!(
            std::path::Path::new(current.manifest_list().strip_prefix("file://").unwrap()).exists()
        );
        for field in ["total-data-files", "total-delete-files", "total-records"] {
            assert_eq!(
                current.summary().additional_properties[field],
                parent.summary().additional_properties[field]
            );
        }
    }
}
