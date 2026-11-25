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

//! An exact retirement reuses the original publication's manifest entries.

use super::{
    model::{CommitProtocol, OperationIntent, OperationToken, RequestShape},
    slot_change::{carry_records_and_closure, document_error},
    staging::{PreparedChange, Preparer, StagedView},
};
use crate::document_storage::keyed::{envelope, keys, record};
use crate::iceberg::{Error, ErrorKind, Result, TableUpdate, spec::Operation};
use async_trait::async_trait;
use novarocks_spi::connector::KeyedRetirement;

pub(crate) struct RetirementPreparer {
    pub(crate) retirement: KeyedRetirement,
}

#[async_trait]
impl Preparer for RetirementPreparer {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        intent: &OperationIntent,
    ) -> Result<PreparedChange> {
        let retirement = &self.retirement;
        if !matches!(intent.protocol(), CommitProtocol::MvRetirement { .. })
            || intent.shape() != RequestShape::SnapshotProducing
            || intent.target_ref() != "main"
            || intent.token() != OperationToken::from_mutation(retirement.operation_id)
            || !intent.changes().added.is_empty()
            || !intent.changes().removed.is_empty()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Retirement requires its original zero-file materialized view intent on main",
            ));
        }
        let metadata = view.metadata();
        let parent = metadata.current_snapshot().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Materialized view retirement has no parent",
            )
        })?;
        let mut carried = carry_records_and_closure(metadata)?;
        let retired = &retirement.retired_record;
        let retired_key = keys::record_key(&retired.owner, &retired.name);
        if carried.summary.remove(&retired_key)
            != Some(record::encode_record(retired).map_err(document_error)?)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Materialized view retirement no longer sees the exact admitted publication",
            ));
        }
        let witness = &retirement.record;
        carried.summary.insert(
            keys::record_key(&witness.owner, &witness.name),
            record::encode_record(witness).map_err(document_error)?,
        );
        let (marker, value) = &retirement.invalid_marker;
        carried.properties.insert(
            keys::marker_key(marker),
            envelope::encode_marker(marker, value).map_err(document_error)?,
        );

        view.artifacts().check_active()?;
        let list =
            crate::manifest::load_manifest_list(parent, view.artifacts().file_io(), metadata)
                .await?;
        view.artifacts().check_active()?;
        let summary_bytes = carried
            .summary
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        let mut change = super::overwrite::prepare_snapshot_change(
            view,
            intent,
            view.stage_snapshot_id()?,
            Operation::Append,
            list.entries().to_vec(),
            carried.summary,
            false,
        )
        .await?;
        change.updates.push(TableUpdate::SetProperties {
            updates: carried.properties,
        });
        crate::document_storage::keyed::write::trace_updates(
            metadata.properties(),
            &change.updates,
            summary_bytes,
        );
        Ok(change)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{
        model::{
            ArtifactWriter, FileChanges, MvRefreshDependencies, OperationIntentParts,
            StartSnapshot, TableTarget,
        },
        operation::{IcebergCommitOperation, OperationLimits},
        overwrite::preparer_tests::Fixture,
        staging::{StagingBase, StagingEngine},
    };
    use crate::iceberg::{TableIdent, TableRequirement, spec::TableMetadata};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorDocumentFormat, ConnectorDocumentVersion, ConnectorMarkerKey,
        ConnectorMarkerValue, ConnectorMutationOperationId, ConnectorSnapshotRecord,
        LakePublicationId,
    };
    use std::collections::BTreeMap;

    fn retired_parent(
        metadata: TableMetadata,
        operation_id: ConnectorMutationOperationId,
    ) -> (TableMetadata, KeyedRetirement) {
        let binding = ConnectorDocumentVersion::try_new(
            "novarocks.mv",
            "binding",
            ConnectorDocumentFormat::try_new("novarocks.mv", "binding", 1).unwrap(),
            vec![],
            Bytes::from_static(b"retained binding"),
        )
        .unwrap();
        let retired_record = ConnectorSnapshotRecord {
            owner: "novarocks.mv".into(),
            name: "publication".into(),
            binding: Some(binding.identity()),
            payload: Bytes::from_static(b"published"),
        };
        let record = ConnectorSnapshotRecord {
            owner: retired_record.owner.clone(),
            name: "retirement".into(),
            binding: None,
            payload: Bytes::copy_from_slice(&operation_id.to_bytes()),
        };
        let mut json = serde_json::to_value(&metadata).unwrap();
        json["properties"][keys::version_key(binding.identity())] =
            envelope::encode_version(&binding).unwrap().into();
        json["properties"][keys::root_key("novarocks.mv")] = binding.identity().to_hex().into();
        let current = metadata.current_snapshot_id().unwrap();
        let parent = json["snapshots"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|snapshot| snapshot["snapshot-id"] == current)
            .unwrap();
        parent["summary"][keys::record_key(&retired_record.owner, &retired_record.name)] =
            record::encode_record(&retired_record).unwrap().into();
        let invalid_marker = (
            ConnectorMarkerKey {
                owner: "novarocks.mv".into(),
                name: "invalid".into(),
                binding: "retired-p".into(),
            },
            ConnectorMarkerValue {
                format: ConnectorDocumentFormat::try_new("novarocks.mv", "marker", 1).unwrap(),
                content: Bytes::from_static(b"insufficient verification"),
            },
        );
        (
            serde_json::from_value(json).unwrap(),
            KeyedRetirement {
                operation_id,
                retired_publication: LakePublicationId::new_v7(),
                expected_root: binding.identity(),
                retired_record,
                record,
                invalid_marker,
            },
        )
    }

    fn retirement_intent(
        operation: &IcebergCommitOperation,
        parent: &TableMetadata,
        retirement: &KeyedRetirement,
    ) -> OperationIntent {
        let snapshot = parent.current_snapshot().unwrap();
        OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: TableIdent::from_strs(["db", "t"]).unwrap(),
                uuid: Some(parent.uuid()),
            },
            target_ref: "main".into(),
            start: Some(StartSnapshot {
                snapshot_id: snapshot.snapshot_id(),
                sequence_number: snapshot.sequence_number(),
            }),
            changes: FileChanges::default(),
            dependencies: Vec::new(),
            protocol: CommitProtocol::MvRetirement {
                admitted: MvRefreshDependencies {
                    roots: BTreeMap::from([(
                        retirement.retired_record.owner.clone(),
                        Some(retirement.expected_root),
                    )]),
                    slots: Default::default(),
                },
            },
            shape: RequestShape::SnapshotProducing,
            summary: Default::default(),
            token: operation.token(),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn retirement_appends_one_list_reuses_both_manifests_and_drops_publication_record() {
        let fixture = Fixture::new();
        let id = ConnectorMutationOperationId::from_bytes([21; 16]);
        let (metadata, retirement) =
            retired_parent(fixture.metadata_with_data_and_delete_manifests().await, id);
        let parent = metadata.current_snapshot().unwrap();
        let source = fixture.operation.begin_attempt().unwrap();
        let operation = IcebergCommitOperation::new(
            OperationToken::from_mutation(id),
            metadata.location(),
            source.read_binding(),
            fixture.operation.request_context().clone(),
            fixture.operation.runtime().clone(),
            OperationLimits::default(),
        )
        .unwrap();
        let attempt = operation.begin_attempt().unwrap();
        let intent = retirement_intent(&operation, &metadata, &retirement);
        let parent_list = crate::manifest::load_manifest_list(parent, attempt.file_io(), &metadata)
            .await
            .unwrap();
        let preparer = RetirementPreparer {
            retirement: retirement.clone(),
        };
        let mut staging = StagingEngine::begin(
            StagingBase::Existing {
                metadata: metadata.clone(),
                metadata_location: "file:///original-metadata.json".into(),
            },
            &intent,
            &attempt,
        )
        .unwrap();
        staging.stage(&preparer).await.unwrap();
        let after = staging.metadata().clone();
        let current = after.current_snapshot().unwrap();
        assert_eq!(current.parent_snapshot_id(), Some(parent.snapshot_id()));
        assert_eq!(current.summary().operation, Operation::Append);
        assert_ne!(current.manifest_list(), parent.manifest_list());
        let current_list = crate::manifest::load_manifest_list(current, attempt.file_io(), &after)
            .await
            .unwrap();
        assert_eq!(current_list.entries(), parent_list.entries());
        let facts =
            crate::document_storage::keyed::view::parse_properties(after.properties()).unwrap();
        assert_eq!(
            facts.roots["novarocks.mv"].identity,
            retirement.expected_root
        );
        assert!(!facts.roots["novarocks.mv"].dangling);
        assert_eq!(
            facts.markers[&retirement.invalid_marker.0],
            retirement.invalid_marker.1
        );
        let records = crate::document_storage::keyed::view::parse_snapshot_records(
            &current.summary().additional_properties,
            true,
        )
        .unwrap();
        assert_eq!(records, vec![retirement.record]);
        let frozen = staging.freeze().unwrap();
        assert!(
            frozen
                .requirements()
                .contains(&TableRequirement::RefSnapshotIdMatch {
                    r#ref: "main".into(),
                    snapshot_id: Some(parent.snapshot_id()),
                })
        );
        assert_eq!(operation.artifacts().unwrap().len(), 1);

        let verdict = crate::commit::dependency::validate(
            &intent,
            &after,
            after.current_snapshot_id(),
            &attempt,
        )
        .await
        .unwrap();
        let crate::commit::dependency::Verdict::ValidationConflict(failure) = verdict else {
            panic!("a retirement cannot move to a different main");
        };
        use novarocks_failure_contract::{
            FailureProjection, connector_reason::ICEBERG_COMMIT_VALIDATION_CONFLICT,
        };
        assert_eq!(
            failure.failure_fact().reason(),
            ICEBERG_COMMIT_VALIDATION_CONFLICT
        );
        assert_eq!(failure.message(), crate::commit::model::MV_MAIN_ADVANCED);
    }
}
