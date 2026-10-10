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
    ArtifactClass, ArtifactWriteState, BaseIdentity, FrozenRequest, ObjectIdentity,
    OperationAuthority, OperationToken, RequestShape,
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
        let mut index = BTreeMap::new();
        let mut objects = Vec::new();
        for record in operation.artifacts()? {
            index.insert(
                record.object.clone(),
                u32::try_from(objects.len())
                    .map_err(|_| invalid("Recovery object count overflow"))?,
            );
            objects.push(Object {
                path: record.object.path().into(),
                class: match record.class {
                    ArtifactClass::SessionData => Class::SessionData,
                    ArtifactClass::Operation => Class::Operation,
                    ArtifactClass::Attempt => Class::Attempt,
                    ArtifactClass::ExternalRegistered => {
                        return Err(invalid(
                            "External registered object cannot enter owned recovery facts",
                        ));
                    }
                },
                attempt: record.attempt.map(Attempt::from),
                state: match record.write_state {
                    ArtifactWriteState::Allocated => State::Allocated,
                    ArtifactWriteState::Writing => State::Writing,
                    ArtifactWriteState::Written => State::Written,
                },
            });
        }
        let references = |paths: &[ObjectIdentity]| -> Result<Vec<u32>> {
            paths
                .iter()
                .map(|path| {
                    index.get(path).copied().ok_or_else(|| {
                        invalid("Frozen publication reference is absent from recovery ledger")
                    })
                })
                .collect()
        };
        let facts = Self {
            authority: operation.token().authority().into(),
            operation_id: operation.token().to_bytes(),
            attempt: request.artifacts().attempt().into(),
            shape: match request.shape() {
                RequestShape::SnapshotProducing => Shape::SnapshotProducing,
                RequestShape::MetadataOnly => Shape::MetadataOnly,
                RequestShape::Create => Shape::Create,
            },
            namespace: request.identifier().namespace.as_ref().to_vec(),
            table: request.identifier().name.clone(),
            target_ref: request.target_ref().into(),
            base,
            published_snapshot_id: request.ref_snapshot_after(request.target_ref()),
            attempt_owned: references(request.artifacts().attempt_owned())?,
            operation_references: references(request.artifacts().operation_references())?,
            session_references: references(request.artifacts().session_references())?,
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
                Shape::SnapshotProducing | Shape::MetadataOnly,
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
