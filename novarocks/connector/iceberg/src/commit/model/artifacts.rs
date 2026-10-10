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

//! Ownership and cleanup use physical object identity, never manifest entry identity.

use std::collections::{BTreeMap, BTreeSet};

use super::{AttemptToken, ObjectIdentity, OperationToken, invalid};
use crate::iceberg::Result;
use crate::iceberg::io::FileIO;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactClass {
    ExternalRegistered,
    SessionData,
    Operation,
    Attempt,
}

/// Closed output format vocabulary; the format is explicit rather than inferred from a caller path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactKind {
    Manifest,
    ManifestList,
    Statistics,
    DeletionVector,
    Metadata,
}

impl ArtifactKind {
    pub const fn extension(self) -> &'static str {
        match self {
            Self::Manifest | Self::ManifestList => "avro",
            Self::Statistics | Self::DeletionVector => "puffin",
            Self::Metadata => "json",
        }
    }
}

/// An attempt-scoped output capability. Preparers cannot delete objects or replace the operation owner.
/// Allocations support only Operation and Attempt lifetimes; existing session files are adopted by the owner.
pub trait ArtifactWriter: Send + Sync {
    fn file_io(&self) -> &FileIO;
    fn allocate(&self, class: ArtifactClass, kind: ArtifactKind) -> Result<ObjectIdentity>;
    fn attempt_token(&self) -> AttemptToken;
    fn check_active(&self) -> Result<()>;
    fn snapshot_artifacts(&self, owned_references: &[ObjectIdentity]) -> Result<AttemptArtifacts>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactWriteState {
    Allocated,
    Writing,
    Written,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactRecord {
    pub object: ObjectIdentity,
    pub class: ArtifactClass,
    pub attempt: Option<AttemptToken>,
    pub write_state: ArtifactWriteState,
}

#[derive(Debug)]
pub struct ArtifactLedger {
    operation: OperationToken,
    records: BTreeMap<ObjectIdentity, ArtifactRecord>,
    registered_paths: BTreeSet<ObjectIdentity>,
}

impl ArtifactLedger {
    pub fn new(operation: OperationToken) -> Self {
        Self {
            operation,
            records: BTreeMap::new(),
            registered_paths: BTreeSet::new(),
        }
    }
    pub fn register(&mut self, record: ArtifactRecord) -> Result<()> {
        if record.object.path().is_empty() {
            return Err(invalid("Owned artifact path must not be empty"));
        }
        if record.class == ArtifactClass::ExternalRegistered {
            return Err(invalid(
                "External registered objects cannot enter the owned artifact ledger",
            ));
        }
        match (record.class, record.attempt) {
            (ArtifactClass::Attempt, Some(attempt)) if attempt.operation() == self.operation => {}
            (ArtifactClass::SessionData | ArtifactClass::Operation, None) => {}
            _ => {
                return Err(invalid(
                    "Artifact lifetime does not match its operation or attempt owner",
                ));
            }
        }
        if self.registered_paths.contains(&record.object) {
            return Err(invalid(format!(
                "Iceberg artifact path is already registered: {}",
                record.object.path()
            )));
        }
        self.registered_paths.insert(record.object.clone());
        self.records.insert(record.object.clone(), record);
        Ok(())
    }
    pub fn mark_writing(&mut self, object: &ObjectIdentity) -> Result<()> {
        let record = self
            .records
            .get_mut(object)
            .ok_or_else(|| invalid("Unregistered artifact cannot be written"))?;
        if record.write_state != ArtifactWriteState::Allocated {
            return Err(invalid(format!(
                "Iceberg artifact path cannot be written twice: {}",
                object.path()
            )));
        }
        record.write_state = ArtifactWriteState::Writing;
        Ok(())
    }
    pub fn mark_written(&mut self, object: &ObjectIdentity) -> Result<()> {
        let record = self
            .records
            .get_mut(object)
            .ok_or_else(|| invalid("Unregistered artifact cannot be completed"))?;
        if record.write_state != ArtifactWriteState::Writing {
            return Err(invalid(
                "Only an admitted in-flight artifact may finish writing",
            ));
        }
        record.write_state = ArtifactWriteState::Written;
        Ok(())
    }
    pub fn record(&self, object: &ObjectIdentity) -> Option<&ArtifactRecord> {
        self.records.get(object)
    }
    pub fn records(&self) -> impl Iterator<Item = &ArtifactRecord> {
        self.records.values()
    }
    pub fn selected(&self, scope: CleanupScope) -> Vec<ArtifactRecord> {
        self.records
            .values()
            .filter(|record| scope.includes(record))
            .cloned()
            .collect()
    }
    /// Call only after storage confirms removal (absence also confirms removal).
    pub fn forget_removed(&mut self, object: &ObjectIdentity) -> Option<ArtifactRecord> {
        self.records.remove(object)
    }
    pub fn snapshot_for_attempt(
        &self,
        attempt: AttemptToken,
        references: impl IntoIterator<Item = ObjectIdentity>,
    ) -> Result<AttemptArtifacts> {
        if attempt.operation() != self.operation {
            return Err(invalid("Attempt belongs to another operation"));
        }
        let mut operation_references = BTreeSet::new();
        let mut session_references = BTreeSet::new();
        for object in references {
            let record = self.records.get(&object).ok_or_else(|| {
                invalid("Frozen request references an unregistered owned artifact")
            })?;
            match record.class {
                ArtifactClass::Operation => {
                    operation_references.insert(object);
                }
                ArtifactClass::SessionData => {
                    session_references.insert(object);
                }
                _ => {
                    return Err(invalid(
                        "Only operation or session artifacts may be cross-attempt references",
                    ));
                }
            }
        }
        Ok(AttemptArtifacts {
            attempt,
            attempt_owned: self
                .records
                .values()
                .filter(|r| r.attempt == Some(attempt))
                .map(|r| r.object.clone())
                .collect(),
            operation_references: operation_references.into_iter().collect(),
            session_references: session_references.into_iter().collect(),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupScope {
    ThisAttempt(AttemptToken),
    AbandonedAttempts { committed: AttemptToken },
    EntireOperation,
}

impl CleanupScope {
    fn includes(self, record: &ArtifactRecord) -> bool {
        match self {
            Self::ThisAttempt(attempt) => {
                record.class == ArtifactClass::Attempt && record.attempt == Some(attempt)
            }
            Self::AbandonedAttempts { committed } => {
                record.class == ArtifactClass::Attempt
                    && record.attempt.is_some_and(|attempt| attempt != committed)
            }
            Self::EntireOperation => record.class != ArtifactClass::ExternalRegistered,
        }
    }
}

/// An immutable registration snapshot travels with the request, including owned references.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptArtifacts {
    attempt: AttemptToken,
    attempt_owned: Vec<ObjectIdentity>,
    operation_references: Vec<ObjectIdentity>,
    session_references: Vec<ObjectIdentity>,
}

impl AttemptArtifacts {
    pub fn empty(attempt: AttemptToken) -> Self {
        Self {
            attempt,
            attempt_owned: Vec::new(),
            operation_references: Vec::new(),
            session_references: Vec::new(),
        }
    }
    pub const fn attempt(&self) -> AttemptToken {
        self.attempt
    }
    pub fn attempt_owned(&self) -> &[ObjectIdentity] {
        &self.attempt_owned
    }
    pub fn operation_references(&self) -> &[ObjectIdentity] {
        &self.operation_references
    }
    pub fn session_references(&self) -> &[ObjectIdentity] {
        &self.session_references
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorWriteOperationId;

    fn object(path: &str) -> ObjectIdentity {
        ObjectIdentity::new(path).unwrap()
    }
    fn record(path: &str, class: ArtifactClass, attempt: Option<AttemptToken>) -> ArtifactRecord {
        ArtifactRecord {
            object: object(path),
            class,
            attempt,
            write_state: ArtifactWriteState::Allocated,
        }
    }

    #[test]
    fn external_objects_never_enter_owned_cleanup_and_paths_are_written_once() {
        let op = OperationToken::from_write(ConnectorWriteOperationId::from_bytes([1; 16]));
        let attempt = AttemptToken::new(op, 0);
        let mut ledger = ArtifactLedger::new(op);
        assert!(
            ledger
                .register(record(
                    "external.parquet",
                    ArtifactClass::ExternalRegistered,
                    None
                ))
                .is_err()
        );
        ledger
            .register(record(
                "manifest.avro",
                ArtifactClass::Attempt,
                Some(attempt),
            ))
            .unwrap();
        ledger.mark_writing(&object("manifest.avro")).unwrap();
        assert!(ledger.mark_writing(&object("manifest.avro")).is_err());
        ledger.mark_written(&object("manifest.avro")).unwrap();
        assert!(
            ledger
                .register(record("manifest.avro", ArtifactClass::Operation, None))
                .is_err()
        );
        assert!(ledger.mark_writing(&object("unregistered.avro")).is_err());
        assert_eq!(ledger.selected(CleanupScope::EntireOperation).len(), 1);
        ledger.forget_removed(&object("manifest.avro"));
        assert!(
            ledger
                .register(record(
                    "manifest.avro",
                    ArtifactClass::Attempt,
                    Some(attempt)
                ))
                .is_err()
        );
    }

    #[test]
    fn attempt_cleanup_preserves_session_and_operation_references() {
        let op = OperationToken::from_write(ConnectorWriteOperationId::from_bytes([1; 16]));
        let a = AttemptToken::new(op, 0);
        let b = AttemptToken::new(op, 1);
        let mut ledger = ArtifactLedger::new(op);
        for record in [
            record("a.avro", ArtifactClass::Attempt, Some(a)),
            record("b.avro", ArtifactClass::Attempt, Some(b)),
            record("draft.puffin", ArtifactClass::Operation, None),
            record("data.parquet", ArtifactClass::SessionData, None),
        ] {
            ledger.register(record).unwrap();
        }
        assert_eq!(
            ledger
                .selected(CleanupScope::ThisAttempt(a))
                .iter()
                .map(|r| r.object.path())
                .collect::<Vec<_>>(),
            ["a.avro"]
        );
        assert_eq!(
            ledger
                .selected(CleanupScope::AbandonedAttempts { committed: b })
                .len(),
            1
        );
        let snapshot = ledger
            .snapshot_for_attempt(b, [object("draft.puffin"), object("data.parquet")])
            .unwrap();
        assert_eq!(snapshot.attempt_owned(), [object("b.avro")]);
        assert_eq!(snapshot.operation_references(), [object("draft.puffin")]);
        assert_eq!(snapshot.session_references(), [object("data.parquet")]);
    }
}
