// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact frozen publication facts for provider-private recovery envelopes.
//!
//! These records grant no cleanup or I/O authority. Only the live operation
//! ledger can authorize deletion; decoded facts support read-only adjudication.

use super::model::{
    ArtifactOrigin, ArtifactWriteState, BaseIdentity, FrozenRequest, OperationAuthority,
    OperationToken, ReceivedPublication, RequestShape,
};
use super::operation::IcebergCommitOperation;
use crate::iceberg::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message.into())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Authority {
    Write,
    Mutation,
    WriteSession,
}
impl From<OperationAuthority> for Authority {
    fn from(authority: OperationAuthority) -> Self {
        match authority {
            OperationAuthority::Write => Self::Write,
            OperationAuthority::Mutation => Self::Mutation,
            OperationAuthority::WriteSession => Self::WriteSession,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Shape {
    SnapshotProducing,
    MetadataOnly,
    RefRetention,
    DocumentExpiry,
    Create,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Base {
    Existing {
        uuid: uuid::Uuid,
        parent_snapshot_id: Option<i64>,
        metadata_location: String,
    },
    Create {
        uuid: uuid::Uuid,
        location: String,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Class {
    SessionData,
    Operation,
    Attempt,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum State {
    Allocated,
    Writing,
    Written,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    ordinal: u32,
    nonce: [u8; 16],
}
impl From<super::model::AttemptToken> for Attempt {
    fn from(token: super::model::AttemptToken) -> Self {
        Self {
            ordinal: token.ordinal(),
            nonce: token.nonce_bytes(),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Object {
    path: String,
    class: Class,
    attempt: Option<Attempt>,
    state: State,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FrozenPublicationFacts {
    authority: Authority,
    operation_id: [u8; 16],
    attempt: Attempt,
    shape: Shape,
    namespace: Vec<String>,
    table: String,
    target_ref: String,
    base: Base,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    base_facts: Option<super::model::FrozenBaseFacts>,
    published_snapshot_id: Option<i64>,
    objects: Vec<Object>,
    attempt_owned: Vec<u32>,
    operation_references: Vec<u32>,
    session_references: Vec<u32>,
}

impl FrozenPublicationFacts {
    pub(crate) fn validate_create_target(
        &self,
        operation: OperationToken,
        identifier: &crate::iceberg::TableIdent,
        uuid: uuid::Uuid,
        location: &str,
        target_ref: &str,
        published_snapshot_id: Option<i64>,
    ) -> Result<()> {
        self.validate(operation)?;
        if &self.namespace != identifier.namespace.as_ref()
            || self.table != identifier.name
            || self.target_ref != target_ref
            || self.shape != Shape::Create
            || self.published_snapshot_id != published_snapshot_id
            || !matches!(&self.base, Base::Create { uuid: actual_uuid, location: actual_location }
                if *actual_uuid == uuid && actual_location == location)
        {
            return Err(invalid(
                "Recovery publication facts do not match the exact staged create target",
            ));
        }
        Ok(())
    }
    pub(crate) fn validate_no_session_data(&self) -> Result<()> {
        if !self.session_references.is_empty()
            || self
                .objects
                .iter()
                .any(|object| object.class == Class::SessionData)
        {
            return Err(invalid(
                "This publication cannot own session data artifacts",
            ));
        }
        Ok(())
    }
    pub(crate) fn validate_existing_target(
        &self,
        operation: OperationToken,
        identifier: &crate::iceberg::TableIdent,
        uuid: uuid::Uuid,
        target_ref: &str,
        shape: RequestShape,
    ) -> Result<()> {
        self.validate(operation)?;
        let expected_shape = match shape {
            RequestShape::SnapshotProducing => Shape::SnapshotProducing,
            RequestShape::MetadataOnly => Shape::MetadataOnly,
            RequestShape::RefRetention => Shape::RefRetention,
            RequestShape::DocumentExpiry => Shape::DocumentExpiry,
            RequestShape::Create => Shape::Create,
        };
        if &self.namespace != identifier.namespace.as_ref()
            || self.table != identifier.name
            || self.target_ref != target_ref
            || self.shape != expected_shape
            || !matches!(self.base, Base::Existing { uuid: actual, .. } if actual == uuid)
        {
            return Err(invalid(
                "Recovery publication facts do not match the exact existing target",
            ));
        }
        Ok(())
    }
    pub(crate) fn from_request(
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<Self> {
        if request.artifacts().attempt().operation() != operation.token() {
            return Err(invalid("Recovery request belongs to another operation"));
        }
        let base = match request.base() {
            BaseIdentity::Existing {
                uuid,
                parent,
                metadata_location,
            } => Base::Existing {
                uuid: *uuid,
                parent_snapshot_id: *parent,
                metadata_location: metadata_location.clone(),
            },
            BaseIdentity::Create { staged } => {
                if staged.operation() != operation.token() {
                    return Err(invalid(
                        "Recovery create identity belongs to another operation",
                    ));
                }
                Base::Create {
                    uuid: staged.initial_metadata().uuid(),
                    location: staged.initial_metadata().location().into(),
                }
            }
        };
        // Transitional envelope projection only; the live ledger remains the sole owner.
        let current = request.artifacts().attempt();
        let selected: BTreeSet<_> = request.artifacts().references().iter().collect();
        let mut index = BTreeMap::new();
        let mut objects = Vec::new();
        let mut attempt_owned = Vec::new();
        let mut operation_references = Vec::new();
        let mut session_references = Vec::new();
        for record in operation.artifacts()? {
            let object_index = u32::try_from(objects.len())
                .map_err(|_| invalid("Recovery object count overflow"))?;
            index.insert(record.object.clone(), object_index);
            let (class, attempt) = match record.origin {
                ArtifactOrigin::Received {
                    publishes: ReceivedPublication::ViaManifest,
                } => (Class::SessionData, None),
                ArtifactOrigin::Received {
                    publishes: ReceivedPublication::WithRequest,
                } => (Class::Operation, None),
                // REWRITE already carries a previous attempt's selected manifests.
                // The old envelope can express those only as operation references.
                ArtifactOrigin::Produced { attempt, .. }
                    if attempt != current && selected.contains(&record.object) =>
                {
                    (Class::Operation, None)
                }
                ArtifactOrigin::Produced { attempt, .. } => (Class::Attempt, Some(attempt.into())),
            };
            if class == Class::Attempt && attempt.as_ref() == Some(&Attempt::from(current)) {
                // The old validator requires the whole current attempt, including orphans.
                attempt_owned.push(object_index);
            }
            if selected.contains(&record.object) {
                match class {
                    Class::Operation => operation_references.push(object_index),
                    Class::SessionData => session_references.push(object_index),
                    Class::Attempt => {}
                }
            }
            objects.push(Object {
                path: record.object.path().into(),
                class,
                attempt,
                state: match record.write_state {
                    ArtifactWriteState::Allocated => State::Allocated,
                    ArtifactWriteState::Writing => State::Writing,
                    ArtifactWriteState::Written => State::Written,
                },
            });
        }
        if selected.iter().any(|object| !index.contains_key(*object)) {
            return Err(invalid(
                "Frozen publication reference is absent from recovery ledger",
            ));
        }
        let facts = Self {
            authority: operation.token().authority().into(),
            operation_id: operation.token().to_bytes(),
            attempt: request.artifacts().attempt().into(),
            shape: match request.shape() {
                RequestShape::SnapshotProducing => Shape::SnapshotProducing,
                RequestShape::MetadataOnly => Shape::MetadataOnly,
                RequestShape::RefRetention => Shape::RefRetention,
                RequestShape::DocumentExpiry => Shape::DocumentExpiry,
                RequestShape::Create => Shape::Create,
            },
            namespace: request.identifier().namespace.as_ref().to_vec(),
            table: request.identifier().name.clone(),
            target_ref: request.target_ref().into(),
            base,
            base_facts: request.base_facts().cloned(),
            published_snapshot_id: request.ref_snapshot_after(request.target_ref()),
            attempt_owned,
            operation_references,
            session_references,
            objects,
        };
        facts.validate(operation.token())?;
        Ok(facts)
    }

    pub(crate) fn validate(&self, operation: OperationToken) -> Result<()> {
        if self.authority != operation.authority().into()
            || self.operation_id != operation.to_bytes()
            || self.namespace.is_empty()
            || self.table.is_empty()
            || self.target_ref.is_empty()
        {
            return Err(invalid(
                "Recovery publication differs from its exact operation/target identity",
            ));
        }
        match (&self.base, self.shape) {
            (
                Base::Existing {
                    metadata_location, ..
                },
                Shape::SnapshotProducing
                | Shape::MetadataOnly
                | Shape::RefRetention
                | Shape::DocumentExpiry,
            ) if !metadata_location.is_empty() => {}
            (Base::Create { location, .. }, Shape::Create) if !location.is_empty() => {}
            _ => {
                return Err(invalid(
                    "Recovery publication shape disagrees with its authoritative base",
                ));
            }
        }
        if self.shape == Shape::SnapshotProducing && self.published_snapshot_id.is_none() {
            return Err(invalid(
                "Snapshot-producing recovery publication has no published snapshot",
            ));
        }
        let valid_nonce = |nonce: &[u8; 16]| uuid::Uuid::from_bytes(*nonce).get_version_num() == 7;
        if !valid_nonce(&self.attempt.nonce) {
            return Err(invalid(
                "Recovery publication has no actual UUIDv7 attempt nonce",
            ));
        }
        let mut attempts = BTreeMap::from([(self.attempt.ordinal, self.attempt.nonce)]);
        let mut previous = None;
        for object in &self.objects {
            super::write_stack::domain::validate_location("recovery object", &object.path)
                .map_err(|error| invalid(error.message()))?;
            if previous.is_some_and(|path| path >= object.path.as_str()) {
                return Err(invalid(
                    "Recovery ledger paths must be unique and strictly ordered",
                ));
            }
            previous = Some(object.path.as_str());
            match (object.class, &object.attempt) {
                (Class::Attempt, Some(attempt))
                    if attempt.ordinal <= self.attempt.ordinal && valid_nonce(&attempt.nonce) =>
                {
                    if attempts
                        .insert(attempt.ordinal, attempt.nonce)
                        .is_some_and(|old| old != attempt.nonce)
                    {
                        return Err(invalid("Recovery attempt ordinal has conflicting nonces"));
                    }
                }
                (Class::SessionData, None) if object.state == State::Written => {}
                (Class::Operation, None) => {}
                _ => {
                    return Err(invalid(
                        "Recovery object lifetime disagrees with its write/attempt facts",
                    ));
                }
            }
        }
        let mut referenced = BTreeSet::new();
        for (references, class) in [
            (&self.attempt_owned, Class::Attempt),
            (&self.operation_references, Class::Operation),
            (&self.session_references, Class::SessionData),
        ] {
            let mut previous = None;
            for index in references {
                let object = self
                    .objects
                    .get(*index as usize)
                    .ok_or_else(|| invalid("Recovery reference is outside its complete ledger"))?;
                if previous.is_some_and(|old| old >= *index)
                    || !referenced.insert(*index)
                    || object.class != class
                    || object.state != State::Written
                    || (class == Class::Attempt && object.attempt.as_ref() != Some(&self.attempt))
                {
                    return Err(invalid(
                        "Recovery reference disagrees with exact ownership/write facts",
                    ));
                }
                previous = Some(*index);
            }
        }
        let current: Vec<_> = self
            .objects
            .iter()
            .enumerate()
            .filter(|(_, object)| {
                object.class == Class::Attempt && object.attempt.as_ref() == Some(&self.attempt)
            })
            .map(|(i, _)| i as u32)
            .collect();
        if current != self.attempt_owned {
            return Err(invalid(
                "Recovery publication omits current attempt artifacts",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorMutationOperationId;

    fn retention_facts() -> (
        FrozenPublicationFacts,
        OperationToken,
        crate::iceberg::TableIdent,
    ) {
        let operation =
            OperationToken::from_mutation(ConnectorMutationOperationId::from_bytes([4; 16]));
        let attempt = super::super::model::AttemptToken::new(operation, 3);
        let identifier = crate::iceberg::TableIdent::new(
            crate::iceberg::NamespaceIdent::new("db".into()),
            "t".into(),
        );
        (
            FrozenPublicationFacts {
                authority: operation.authority().into(),
                operation_id: operation.to_bytes(),
                attempt: attempt.into(),
                shape: Shape::RefRetention,
                namespace: vec!["db".into()],
                table: "t".into(),
                target_ref: "main".into(),
                base: Base::Existing {
                    uuid: uuid::Uuid::nil(),
                    parent_snapshot_id: Some(17),
                    metadata_location: "s3://warehouse/db/t/metadata/v3.json".into(),
                },
                base_facts: Some(super::super::model::FrozenBaseFacts {
                    format_version: 3,
                    last_sequence_number: 8,
                    next_row_id: 120,
                }),
                published_snapshot_id: None,
                objects: vec![],
                attempt_owned: vec![],
                operation_references: vec![],
                session_references: vec![],
            },
            operation,
            identifier,
        )
    }

    #[test]
    fn ref_retention_round_trip_preserves_original_baseline_and_authority() {
        let (facts, operation, identifier) = retention_facts();
        let bytes = serde_json::to_vec(&facts).unwrap();
        let restored: FrozenPublicationFacts = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(facts, restored);
        restored
            .validate_existing_target(
                operation,
                &identifier,
                uuid::Uuid::nil(),
                "main",
                RequestShape::RefRetention,
            )
            .unwrap();
        restored.validate_no_session_data().unwrap();
        assert!(
            restored
                .validate_existing_target(
                    operation,
                    &identifier,
                    uuid::Uuid::nil(),
                    "main",
                    RequestShape::MetadataOnly
                )
                .is_err()
        );
        assert!(
            restored
                .validate(OperationToken::from_mutation(
                    ConnectorMutationOperationId::from_bytes([5; 16])
                ))
                .is_err()
        );
    }

    #[test]
    fn old_recovery_facts_without_baseline_remain_readable_without_inventing_facts() {
        let (facts, operation, _) = retention_facts();
        let mut value = serde_json::to_value(facts).unwrap();
        value["shape"] = serde_json::json!("metadata_only");
        value.as_object_mut().unwrap().remove("base_facts");
        let restored: FrozenPublicationFacts = serde_json::from_value(value).unwrap();
        restored.validate(operation).unwrap();
        assert!(restored.base_facts.is_none());
        assert!(
            serde_json::to_value(restored)
                .unwrap()
                .get("base_facts")
                .is_none()
        );
    }
}

#[cfg(test)]
mod document_expiry_recovery_tests {
    use super::*;
    use crate::commit::model::{
        Dependency, FileChanges, IsolationLevel, OperationIntent, OperationIntentParts, TableTarget,
    };
    use crate::commit::overwrite::preparer_tests::Fixture;
    use crate::commit::staging::{PreparedChange, StagingBase, StagingEngine};
    use crate::iceberg::spec::FormatVersion;
    use crate::iceberg::{TableIdent, TableUpdate};
    use novarocks_spi::connector::ConnectorWriteOperationId;

    #[tokio::test]
    async fn document_expiry_recovery_roundtrips_and_keeps_exact_identity() {
        let fixture = Fixture::new();
        let metadata = fixture.metadata(FormatVersion::V2);
        let identifier = TableIdent::from_strs(["db", "t"]).unwrap();
        let intent = OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: identifier.clone(),
                uuid: Some(metadata.uuid()),
            },
            target_ref: "main".into(),
            start: None,
            changes: FileChanges::default(),
            dependencies: vec![Dependency::NoRead],
            isolation: IsolationLevel::Snapshot,
            shape: RequestShape::DocumentExpiry,
            summary: Default::default(),
            token: fixture.operation.token(),
        })
        .unwrap();
        let attempt = fixture.operation.begin_attempt().unwrap();
        let location = format!(
            "file://{}/full.metadata.json",
            fixture.directory.path().display()
        );
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata: metadata.clone(),
                metadata_location: location.clone(),
            },
            &intent,
            &attempt,
        )
        .unwrap();
        engine
            .stage_change(PreparedChange {
                requirements: vec![],
                updates: vec![TableUpdate::RemoveProperties {
                    removals: vec![format!("novarocks.doc.v2.rev.{}", "0".repeat(64))],
                }],
            })
            .unwrap();
        let request = engine.freeze().unwrap();
        let facts = FrozenPublicationFacts::from_request(&request, &fixture.operation).unwrap();
        let encoded = serde_json::to_vec(&facts).unwrap();
        let decoded: FrozenPublicationFacts = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, facts);
        assert_eq!(decoded.shape, Shape::DocumentExpiry);
        assert_eq!(decoded.published_snapshot_id, None);
        assert!(matches!(
            &decoded.base,
            Base::Existing { uuid, parent_snapshot_id: None, metadata_location }
                if *uuid == metadata.uuid() && metadata_location == &location
        ));
        decoded.validate(fixture.operation.token()).unwrap();
        decoded.validate_no_session_data().unwrap();
        decoded
            .validate_existing_target(
                fixture.operation.token(),
                &identifier,
                metadata.uuid(),
                "main",
                RequestShape::DocumentExpiry,
            )
            .unwrap();
        assert!(
            decoded
                .validate_existing_target(
                    fixture.operation.token(),
                    &identifier,
                    metadata.uuid(),
                    "main",
                    RequestShape::SnapshotProducing,
                )
                .is_err()
        );
        assert!(
            decoded
                .validate_existing_target(
                    fixture.operation.token(),
                    &TableIdent::from_strs(["db", "other"]).unwrap(),
                    metadata.uuid(),
                    "main",
                    RequestShape::DocumentExpiry,
                )
                .is_err()
        );
        assert!(
            decoded
                .validate(OperationToken::from_write(
                    ConnectorWriteOperationId::from_bytes([9; 16],)
                ))
                .is_err()
        );
    }
}
