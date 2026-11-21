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

//! Bind ordinary storage and a slot replacement to the selected data snapshot.

use super::{
    FrozenRequest, IcebergWriteCommitSession, OperationIntent, Preparer, StagedView, format_error,
};
use crate::commit::model::{BaseIdentity, Dependency, RequestShape, canonical_json};
use crate::commit::overwrite::{OverwritePreparer, OverwriteSnapshotExtension};
use crate::document_storage::keyed::{envelope, keys};
use crate::iceberg::spec::TableMetadata;
use crate::iceberg::{Error, ErrorKind, TableUpdate};
use async_trait::async_trait;
use novarocks_spi::connector::{
    ConnectorDocumentPublicationSlotUpdate, ConnectorError, DocumentIdentity,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use super::super::{
    ICEBERG_WRITE_SESSION_MARKER_PROPERTY, MANAGED_REPARTITION_EFFECT_SUMMARY_KEY, corrupt,
    internal,
};

pub(crate) fn slot_matches(
    metadata: &TableMetadata,
    update: &ConnectorDocumentPublicationSlotUpdate,
) -> Result<bool, ConnectorError> {
    let key = keys::slot_key(update.name());
    let Some(value) = metadata.properties().get(&key) else {
        return Ok(false);
    };
    let (name, observed) = envelope::decode_slot(&key, value)?;
    Ok(observed.value.digest(&name, observed.writer) == update.expected_old())
}

#[derive(Clone)]
pub(super) struct Effect {
    session: String,
    publication: [u8; 16],
    uuid: uuid::Uuid,
    parent: Option<i64>,
    storage_updates: usize,
    partition_updates: usize,
    prefix_digest: [u8; 32],
    slot: Option<ConnectorDocumentPublicationSlotUpdate>,
}

fn digest(value: impl serde::Serialize, domain: &[u8]) -> Result<[u8; 32], ConnectorError> {
    let canonical = serde_json::to_value(value)
        .map(canonical_json)
        .and_then(|value| serde_json::to_vec(&value))
        .map_err(|error| internal(format!("encode managed repartition witness: {error}")))?;
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(canonical);
    Ok(hash.finalize().into())
}

fn prefix_digest(
    storage: &[TableUpdate],
    partitioning: &[TableUpdate],
) -> Result<[u8; 32], ConnectorError> {
    digest(
        (storage, partitioning),
        b"novarocks.iceberg.managed-repartition-prefix.v1\0",
    )
}

impl Effect {
    pub(super) fn freeze<'a>(
        handle: &'a IcebergWriteCommitSession,
        source: &TableMetadata,
    ) -> Result<Option<ManagedOverwrite<'a>>, ConnectorError> {
        let Some(repartition) = handle.repartition() else {
            return Ok(None);
        };
        let declaration = handle
            .document_publication()
            .ok_or_else(|| corrupt("managed repartition lacks its publication declaration"))?
            .declaration();
        if declaration.slot_update().is_none() && repartition.storage_updates().is_none() {
            return Ok(None);
        }
        let spec = repartition.prospective_metadata().default_partition_spec();
        let mut partition = Vec::new();
        if source.partition_spec_by_id(spec.spec_id()).is_none() {
            partition.push(TableUpdate::AddSpec {
                spec: spec.as_ref().clone().into_unbound(),
            });
        }
        partition.push(TableUpdate::SetDefaultSpec {
            spec_id: spec.spec_id(),
        });
        let storage = repartition.storage_updates().unwrap_or_default();
        Ok(Some(ManagedOverwrite {
            effect: Self {
                session: handle.session_id().to_string(),
                publication: declaration.publication_id().to_bytes(),
                uuid: source.uuid(),
                parent: handle.table().base_snapshot_id(),
                storage_updates: storage.len(),
                partition_updates: partition.len(),
                prefix_digest: prefix_digest(storage, &partition)?,
                slot: declaration.slot_update().cloned(),
            },
            prospective: repartition.prospective_metadata(),
        }))
    }

    fn summary_value(&self, snapshot: i64) -> Result<String, ConnectorError> {
        let slot = self.slot.as_ref().map(|update| {
            (
                &update.name().owner,
                &update.name().name,
                update.expected_old().to_bytes(),
                update
                    .value()
                    .digest(update.name(), Some(snapshot))
                    .to_bytes(),
            )
        });
        let digest = digest(
            (
                &self.session,
                self.publication,
                self.uuid,
                "main",
                self.parent,
                snapshot,
                self.prefix_digest,
                slot,
            ),
            b"novarocks.iceberg.managed-repartition-effect.v1\0",
        )?;
        Ok(DocumentIdentity::from_bytes(digest).to_hex())
    }

    fn slot_property(&self, snapshot: i64) -> Result<Option<(String, String)>, ConnectorError> {
        self.slot
            .as_ref()
            .map(|update| {
                Ok((
                    keys::slot_key(update.name()),
                    envelope::encode_slot(update.name(), update.value(), Some(snapshot))?,
                ))
            })
            .transpose()
    }

    fn extension(&self, snapshot: i64) -> Result<OverwriteSnapshotExtension, ConnectorError> {
        let update = self
            .slot_property(snapshot)?
            .map(|entry| TableUpdate::SetProperties {
                updates: HashMap::from([entry]),
            });
        Ok(OverwriteSnapshotExtension {
            summary: (
                MANAGED_REPARTITION_EFFECT_SUMMARY_KEY.into(),
                self.summary_value(snapshot)?,
            ),
            update,
        })
    }

    pub(super) fn verify_request(&self, request: &FrozenRequest) -> Result<(), ConnectorError> {
        if !matches!(request.base(), BaseIdentity::Existing { uuid, parent, .. }
            if *uuid == self.uuid && *parent == self.parent)
            || request.target_ref() != "main"
            || request.shape() != RequestShape::SnapshotProducing
        {
            return Err(corrupt(
                "managed repartition changed its exact publication base",
            ));
        }
        let updates = request.updates();
        let (storage, rest) = updates
            .split_at_checked(self.storage_updates)
            .ok_or_else(|| corrupt("managed repartition omitted its frozen storage prefix"))?;
        let (partition, rest) = rest
            .split_at_checked(self.partition_updates)
            .ok_or_else(|| corrupt("managed repartition omitted its frozen partition prefix"))?;
        if prefix_digest(storage, partition)? != self.prefix_digest {
            return Err(corrupt(
                "managed repartition changed its frozen property or partition updates",
            ));
        }
        let snapshot_id = request
            .ref_snapshot_after("main")
            .ok_or_else(|| corrupt("managed repartition has no selected snapshot"))?;
        let mut snapshots = rest.iter().filter_map(|update| match update {
            TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
            _ => None,
        });
        let snapshot = snapshots
            .next()
            .filter(|snapshot| {
                snapshot.snapshot_id() == snapshot_id
                    && snapshot.parent_snapshot_id() == self.parent
            })
            .ok_or_else(|| {
                corrupt("managed repartition changed its selected snapshot or parent")
            })?;
        let summary = &snapshot.summary().additional_properties;
        if snapshots.next().is_some()
            || summary.get(ICEBERG_WRITE_SESSION_MARKER_PROPERTY) != Some(&self.session)
            || summary.get(MANAGED_REPARTITION_EFFECT_SUMMARY_KEY)
                != Some(&self.summary_value(snapshot_id)?)
        {
            return Err(corrupt(
                "managed repartition lost its immutable effect witness",
            ));
        }
        if let Some((key, expected)) = self.slot_property(snapshot_id)? {
            let mut writes = rest.iter().filter_map(|update| match update {
                TableUpdate::SetProperties { updates } => updates.get(&key),
                _ => None,
            });
            if writes.next() != Some(&expected)
                || writes.next().is_some()
                || rest.iter().any(|update| {
                    matches!(update,
                    TableUpdate::RemoveProperties { removals } if removals.contains(&key))
                })
            {
                return Err(corrupt(
                    "managed repartition lost its exact selected-snapshot slot value",
                ));
            }
        }
        Ok(())
    }
}

pub(super) struct ManagedOverwrite<'a> {
    pub(super) effect: Effect,
    prospective: &'a TableMetadata,
}

#[async_trait]
impl Preparer for ManagedOverwrite<'_> {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        intent: &OperationIntent,
    ) -> crate::iceberg::Result<crate::commit::staging::PreparedChange> {
        let metadata = view.metadata();
        let parent = metadata
            .snapshot_for_ref("main")
            .map(|snapshot| snapshot.snapshot_id());
        if intent.target_ref() != "main"
            || !intent.dependencies().contains(&Dependency::RefUnchanged)
            || parent != self.effect.parent
            || intent.summary().get(ICEBERG_WRITE_SESSION_MARKER_PROPERTY)
                != Some(&self.effect.session)
        {
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                "managed overwrite lost its original main publication",
            ));
        }
        // Keyed publication preparation may update its private document records
        // in the same staged request. Ordinary writer properties must still
        // agree with the generation that already issued the writers.
        let ordinary = |key: &str| !key.starts_with(keys::PREFIX) && !keys::is_legacy_key(key);
        let expected = self.prospective.properties();
        let actual = metadata.properties();
        for (properties, other) in [(expected, actual), (actual, expected)] {
            for (key, value) in properties {
                view.artifacts().check_active()?;
                if ordinary(key) && other.get(key) != Some(value) {
                    return Err(Error::new(
                        ErrorKind::CatalogCommitConflicts,
                        "staged managed overwrite differs from the sealed writer settings",
                    ));
                }
            }
        }
        if metadata.location() != self.prospective.location()
            || metadata.format_version() != self.prospective.format_version()
            || metadata.current_schema() != self.prospective.current_schema()
            || metadata.default_partition_spec() != self.prospective.default_partition_spec()
        {
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                "staged managed overwrite differs from the sealed writer type or partition spec",
            ));
        }
        if let Some(update) = &self.effect.slot
            && !slot_matches(metadata, update).map_err(format_error)?
        {
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                "managed overwrite slot differs from the admitted value and writer",
            ));
        }
        OverwritePreparer
            .prepare_with_snapshot_extension(view, intent, |snapshot| {
                self.effect
                    .extension(snapshot)
                    .map(Some)
                    .map_err(format_error)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::model::{FileChanges, OperationIntentParts, TableTarget};
    use crate::commit::overwrite::preparer_tests::Fixture;
    use crate::commit::publication_descriptor::{AttributionPlan, PublicationDescriptor};
    use crate::commit::staging::{StagingBase, StagingEngine};
    use crate::iceberg::spec::{
        FormatVersion, Transform, UnboundPartitionField, UnboundPartitionSpecBuilder,
    };
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorDocumentFormat, ConnectorSlotName, ConnectorSlotValue,
    };
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn one_overwrite_writes_the_slot_and_immutable_repartition_witness() {
        let fixture = Fixture::new();
        let name = ConnectorSlotName::try_new("novarocks.test", "configuration").unwrap();
        let key = keys::slot_key(&name);
        let format = ConnectorDocumentFormat::try_new("novarocks.test", "config", 1).unwrap();
        let old = ConnectorSlotValue {
            format: format.clone(),
            content: Bytes::from_static(b"old"),
        };
        let desired = ConnectorSlotValue {
            format,
            content: Bytes::from_static(b"new"),
        };
        let slot = ConnectorDocumentPublicationSlotUpdate::try_new(
            name.clone(),
            old.digest(&name, None),
            desired.clone(),
        )
        .unwrap();
        let with_slot = |metadata: TableMetadata, writer| {
            metadata
                .into_builder(None)
                .set_properties(HashMap::from([(
                    key.clone(),
                    envelope::encode_slot(&name, &old, writer).unwrap(),
                )]))
                .unwrap()
                .build()
                .unwrap()
                .metadata
        };
        let base = with_slot(fixture.metadata(FormatVersion::V2), None);
        assert!(slot_matches(&base, &slot).unwrap());
        assert!(!slot_matches(&with_slot(base.clone(), Some(74)), &slot).unwrap());

        let storage = TableUpdate::SetProperties {
            updates: HashMap::from([("read.split.target-size".into(), "1024".into())]),
        };
        let unbound = UnboundPartitionSpecBuilder::new()
            .add_partition_fields([UnboundPartitionField {
                source_id: 1,
                field_id: None,
                name: "id".into(),
                transform: Transform::Identity,
            }])
            .unwrap()
            .build();
        let intended = storage
            .clone()
            .apply(base.clone().into_builder(None))
            .unwrap()
            .add_default_partition_spec(unbound.clone())
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let selected = intended.default_partition_spec();
        let normalized = [
            TableUpdate::AddSpec {
                spec: selected.as_ref().clone().into_unbound(),
            },
            TableUpdate::SetDefaultSpec {
                spec_id: selected.spec_id(),
            },
        ];
        let session = "original managed repartition test session".to_string();
        let effect = Effect {
            session: session.clone(),
            publication: [7; 16],
            uuid: base.uuid(),
            parent: None,
            storage_updates: 1,
            partition_updates: 2,
            prefix_digest: prefix_digest(std::slice::from_ref(&storage), &normalized).unwrap(),
            slot: Some(slot),
        };
        let overwrite = ManagedOverwrite {
            effect,
            prospective: &intended,
        };
        let intent = OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap(),
                uuid: Some(base.uuid()),
            },
            target_ref: "main".into(),
            start: None,
            changes: FileChanges::default(),
            dependencies: vec![Dependency::RefUnchanged],
            protocol: crate::commit::model::CommitProtocol::Legacy,
            shape: RequestShape::SnapshotProducing,
            summary: BTreeMap::from([(ICEBERG_WRITE_SESSION_MARKER_PROPERTY.into(), session)]),
            token: fixture.operation.token(),
        })
        .unwrap();
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata: base,
                metadata_location: "file://base.metadata.json".into(),
            },
            &intent,
            &attempt,
        )
        .unwrap();
        engine
            .stage_change(crate::commit::staging::PreparedChange {
                requirements: vec![],
                updates: vec![
                    storage,
                    TableUpdate::AddSpec { spec: unbound },
                    TableUpdate::SetDefaultSpec { spec_id: -1 },
                ],
            })
            .unwrap();
        engine.stage(&overwrite).await.unwrap();
        let metadata = engine.metadata().clone();
        let snapshot = metadata.current_snapshot().unwrap().snapshot_id();
        assert_eq!(metadata.snapshots().len(), 1);
        assert_eq!(metadata.default_partition_spec_id(), selected.spec_id());
        assert_eq!(metadata.properties()["read.split.target-size"], "1024");
        let (_, stored) = envelope::decode_slot(&key, &metadata.properties()[&key]).unwrap();
        assert_eq!(stored.value, desired);
        assert_eq!(stored.writer, Some(snapshot));

        let request = engine.freeze().unwrap();
        overwrite.effect.verify_request(&request).unwrap();
        let mut wrong_prefix = overwrite.effect.clone();
        wrong_prefix.prefix_digest[0] ^= 1;
        assert!(wrong_prefix.verify_request(&request).is_err());
        let descriptor = PublicationDescriptor::frozen(&request).unwrap();
        let plan = &descriptor.attribution;
        assert!(
            matches!(plan, AttributionPlan::ManagedRepartition { snapshot_id, parent: None, .. } if *snapshot_id == snapshot)
        );
        let observed = plan.observe_table_metadata(&metadata).unwrap();
        assert!(plan.matches_observation(&observed));
        let later = with_slot(metadata, Some(snapshot));
        assert!(plan.matches_observation(&plan.observe_table_metadata(&later).unwrap()));
        let mut wrong = plan.clone();
        let AttributionPlan::ManagedRepartition { effect, .. } = &mut wrong else {
            unreachable!()
        };
        effect.replace_range(.., "wrong");
        assert!(!wrong.matches_observation(&observed));
        let mut wrong_parent = plan.clone();
        let AttributionPlan::ManagedRepartition { parent, .. } = &mut wrong_parent else {
            unreachable!()
        };
        *parent = Some(snapshot);
        assert!(!wrong_parent.matches_observation(&observed));
    }
}
