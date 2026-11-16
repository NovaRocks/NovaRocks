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

//! The operation owns physical objects through publication and actual cleanup.

use std::collections::{BTreeMap, BTreeSet};

use super::{AttemptToken, ObjectIdentity, OperationToken, invalid};
use super::{ManifestReuseBasis, ManifestRewriteReuse};
use crate::iceberg::Result;
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{ManifestFile, TableMetadata};

/// A received object's explicit publication contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceivedPublication {
    ViaManifest,
    WithRequest,
}

/// Format facts do not determine ownership or cleanup scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactKind {
    Manifest,
    ManifestList,
    Statistics,
    #[cfg(test)]
    DeletionVector,
    #[cfg(test)]
    Metadata,
}
impl ArtifactKind {
    pub const fn extension(self) -> &'static str {
        match self {
            Self::Manifest | Self::ManifestList => "avro",
            Self::Statistics => "puffin",
            #[cfg(test)]
            Self::DeletionVector => "puffin",
            #[cfg(test)]
            Self::Metadata => "json",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactOrigin {
    Received {
        publishes: ReceivedPublication,
    },
    Produced {
        attempt: AttemptToken,
        kind: ArtifactKind,
    },
}

/// Preparers can create and report facts; only their operation can retire or delete.
pub trait ArtifactWriter: Send + Sync {
    fn file_io(&self) -> &FileIO;
    fn allocate(&self, kind: ArtifactKind) -> Result<ObjectIdentity>;
    fn attempt_token(&self) -> AttemptToken;
    fn check_active(&self) -> Result<()>;
    #[cfg(not(test))]
    fn stage_snapshot_id(&self, stage: usize, metadata: &TableMetadata) -> Result<i64>;
    #[cfg(test)]
    fn stage_snapshot_id(&self, _stage: usize, metadata: &TableMetadata) -> Result<i64> {
        Ok(crate::commit::staging::new_snapshot_id(metadata))
    }
    #[cfg(not(test))]
    fn find_reusable_manifest(&self, basis: &ManifestReuseBasis) -> Result<Option<ManifestFile>>;
    #[cfg(test)]
    fn find_reusable_manifest(&self, _basis: &ManifestReuseBasis) -> Result<Option<ManifestFile>> {
        Ok(None)
    }
    #[cfg(not(test))]
    fn remember_reusable_manifest(
        &self,
        basis: ManifestReuseBasis,
        output: ManifestFile,
    ) -> Result<()>;
    #[cfg(test)]
    fn remember_reusable_manifest(
        &self,
        _basis: ManifestReuseBasis,
        _output: ManifestFile,
    ) -> Result<()> {
        Ok(())
    }
    #[cfg(not(test))]
    fn rewrite_reuse_backing_bytes(&self, stage: usize) -> Result<u64>;
    #[cfg(test)]
    fn rewrite_reuse_backing_bytes(&self, _stage: usize) -> Result<u64> {
        Ok(0)
    }
    #[cfg(not(test))]
    fn find_reusable_rewrite(
        &self,
        stage: usize,
        encoding: &[u8],
        latest: &[ManifestFile],
    ) -> Result<Option<ManifestRewriteReuse>>;
    #[cfg(test)]
    fn find_reusable_rewrite(
        &self,
        _stage: usize,
        _encoding: &[u8],
        _latest: &[ManifestFile],
    ) -> Result<Option<ManifestRewriteReuse>> {
        Ok(None)
    }
    #[cfg(not(test))]
    fn remember_reusable_rewrite(&self, stage: usize, result: ManifestRewriteReuse) -> Result<()>;
    #[cfg(test)]
    fn remember_reusable_rewrite(
        &self,
        _stage: usize,
        _result: ManifestRewriteReuse,
    ) -> Result<()> {
        Ok(())
    }
    fn record_manifest(
        &self,
        object: &ObjectIdentity,
        live_paths: &[String],
        replaces: &[String],
    ) -> Result<()>;
    fn record_manifest_list(&self, object: &ObjectIdentity, paths: &[String]) -> Result<()>;
    fn snapshot_artifacts(
        &self,
        lists: &[ObjectIdentity],
        statistics: &[ObjectIdentity],
    ) -> Result<AttemptArtifacts>;
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
    pub origin: ArtifactOrigin,
    pub write_state: ArtifactWriteState,
    pub retired: bool,
    pub covers: BTreeSet<ObjectIdentity>,
    pub replaces: Vec<String>,
    pub members: BTreeSet<ObjectIdentity>,
}
impl ArtifactRecord {
    pub fn received(object: ObjectIdentity, publishes: ReceivedPublication) -> Self {
        Self {
            object,
            origin: ArtifactOrigin::Received { publishes },
            write_state: ArtifactWriteState::Written,
            retired: false,
            covers: BTreeSet::new(),
            replaces: Vec::new(),
            members: BTreeSet::new(),
        }
    }
    pub fn produced(object: ObjectIdentity, attempt: AttemptToken, kind: ArtifactKind) -> Self {
        Self {
            object,
            origin: ArtifactOrigin::Produced { attempt, kind },
            write_state: ArtifactWriteState::Allocated,
            retired: false,
            covers: BTreeSet::new(),
            replaces: Vec::new(),
            members: BTreeSet::new(),
        }
    }
}

fn invariant(message: impl Into<String>) -> crate::iceberg::Error {
    let message = message.into();
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, message.clone()).with_source(
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Internal,
            message,
        ),
    )
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
        if let ArtifactOrigin::Produced { attempt, .. } = record.origin
            && attempt.operation() != self.operation
        {
            return Err(invariant("Produced artifact belongs to another operation"));
        }
        if !self.registered_paths.insert(record.object.clone()) {
            return Err(invariant(format!(
                "Iceberg artifact path is already registered: {}",
                record.object.path()
            )));
        }
        self.records.insert(record.object.clone(), record);
        Ok(())
    }
    pub fn check_writable(&self, object: &ObjectIdentity) -> Result<()> {
        let record = self
            .records
            .get(object)
            .ok_or_else(|| invariant("Unregistered artifact cannot be written"))?;
        if record.retired || !matches!(record.origin, ArtifactOrigin::Produced { .. }) {
            return Err(invariant(
                "Iceberg artifact is retired or received and cannot start a write",
            ));
        }
        Ok(())
    }
    pub fn mark_writing(&mut self, object: &ObjectIdentity) -> Result<()> {
        self.check_writable(object)?;
        let record = self.records.get_mut(object).expect("checked record");
        if record.write_state != ArtifactWriteState::Allocated {
            return Err(invariant("Iceberg artifact path cannot be written twice"));
        }
        record.write_state = ArtifactWriteState::Writing;
        Ok(())
    }
    pub fn mark_written(&mut self, object: &ObjectIdentity) -> Result<()> {
        let record = self
            .records
            .get_mut(object)
            .ok_or_else(|| invariant("Unregistered artifact cannot be completed"))?;
        if record.write_state != ArtifactWriteState::Writing {
            return Err(invariant(
                "Only an admitted in-flight artifact may finish writing",
            ));
        }
        // Retirement closes admission, but an already admitted writer must still report actual exit.
        record.write_state = ArtifactWriteState::Written;
        Ok(())
    }
    pub fn records(&self) -> impl Iterator<Item = &ArtifactRecord> {
        self.records.values()
    }
    pub fn selected(&self, _scope: CleanupScope) -> Vec<ArtifactRecord> {
        self.records.values().cloned().collect()
    }
    pub fn retire(&mut self, object: &ObjectIdentity) -> Result<()> {
        self.records
            .get_mut(object)
            .ok_or_else(|| invariant("Cannot retire an unregistered artifact"))?
            .retired = true;
        Ok(())
    }
    pub fn is_retired(&self, object: &ObjectIdentity) -> bool {
        self.records.get(object).is_none_or(|record| record.retired)
    }
    pub fn forget_removed(&mut self, object: &ObjectIdentity) -> Option<ArtifactRecord> {
        self.records.remove(object)
    }
    fn produced(&self, object: &ObjectIdentity, kind: ArtifactKind) -> Result<&ArtifactRecord> {
        let record = self
            .records
            .get(object)
            .ok_or_else(|| invariant("Selected artifact is absent from the operation ledger"))?;
        if record.retired
            || record.write_state != ArtifactWriteState::Written
            || !matches!(record.origin, ArtifactOrigin::Produced {kind: actual,..} if actual==kind)
        {
            return Err(invariant(format!(
                "Selected artifact is retired, incomplete or has another format: {}",
                object.path()
            )));
        }
        Ok(record)
    }
    pub fn check_reusable_manifest(&self, object: &ObjectIdentity) -> Result<()> {
        self.produced(object, ArtifactKind::Manifest).map(|_| ())
    }
    pub fn record_manifest(
        &mut self,
        object: &ObjectIdentity,
        live_paths: &[String],
        replaces: &[String],
    ) -> Result<()> {
        self.produced(object, ArtifactKind::Manifest)?;
        let mut covers = BTreeSet::new();
        for path in live_paths {
            if let Some(record) = self.records.get(&ObjectIdentity::new(path.clone())?)
                && matches!(
                    record.origin,
                    ArtifactOrigin::Received {
                        publishes: ReceivedPublication::ViaManifest
                    }
                )
            {
                covers.insert(record.object.clone());
            }
        }
        let record = self.records.get_mut(object).expect("checked manifest");
        record.covers = covers;
        record.replaces = replaces.to_vec();
        Ok(())
    }
    pub fn record_manifest_list(
        &mut self,
        object: &ObjectIdentity,
        paths: &[String],
    ) -> Result<()> {
        self.produced(object, ArtifactKind::ManifestList)?;
        let paths: BTreeSet<_> = paths.iter().map(String::as_str).collect();
        let mut members = BTreeSet::new();
        for path in &paths {
            let identity = ObjectIdentity::new(*path)?;
            if let Some(record) = self.records.get(&identity)
                && matches!(record.origin, ArtifactOrigin::Produced { .. })
            {
                let record = self.produced(&identity, ArtifactKind::Manifest)?;
                if record
                    .replaces
                    .iter()
                    .any(|path| paths.contains(path.as_str()))
                {
                    return Err(invariant(
                        "Manifest list contains a replacement together with its source",
                    ));
                }
                members.insert(record.object.clone());
            }
        }
        self.records.get_mut(object).expect("checked list").members = members;
        Ok(())
    }
    pub fn snapshot_for_request(
        &self,
        attempt: AttemptToken,
        lists: &[ObjectIdentity],
        statistics: &[ObjectIdentity],
    ) -> Result<AttemptArtifacts> {
        if attempt.operation() != self.operation {
            return Err(invariant("Attempt belongs to another operation"));
        }
        let mut references = BTreeSet::new();
        let mut covered = BTreeSet::new();
        for object in lists {
            if !self.records.contains_key(object) {
                continue;
            }
            let list = self.produced(object, ArtifactKind::ManifestList)?;
            references.insert(list.object.clone());
            for member in &list.members {
                let manifest = self.produced(member, ArtifactKind::Manifest)?;
                references.insert(manifest.object.clone());
                covered.extend(manifest.covers.iter().cloned());
            }
        }
        for object in statistics {
            if self.records.contains_key(object) {
                references.insert(
                    self.produced(object, ArtifactKind::Statistics)?
                        .object
                        .clone(),
                );
            }
        }
        for record in self.records.values() {
            if let ArtifactOrigin::Received { publishes } = record.origin {
                if record.retired
                    || (publishes == ReceivedPublication::ViaManifest
                        && !covered.contains(&record.object))
                {
                    return Err(invariant(format!(
                        "Received artifact is not covered by the frozen publication: {}",
                        record.object.path()
                    )));
                }
                references.insert(record.object.clone());
            }
        }
        Ok(AttemptArtifacts {
            attempt,
            references: references.into_iter().collect(),
        })
    }
    /// Published records are settled, while their immutable paths remain reserved forever.
    pub fn settle_published(&mut self, references: &[ObjectIdentity]) -> Vec<ArtifactRecord> {
        let references: BTreeSet<_> = references.iter().collect();
        self.records.retain(|object, record| {
            !references.contains(object)
                && !matches!(record.origin, ArtifactOrigin::Received { .. })
        });
        for record in self.records.values_mut() {
            record.retired = true;
        }
        self.records.values().cloned().collect()
    }
    #[cfg(test)]
    pub fn confirm_written_for_test(&mut self, object: &ObjectIdentity) -> Result<()> {
        self.mark_writing(object)?;
        self.mark_written(object)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupScope {
    EntireOperation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptArtifacts {
    attempt: AttemptToken,
    references: Vec<ObjectIdentity>,
}
impl AttemptArtifacts {
    pub fn empty(attempt: AttemptToken) -> Self {
        Self {
            attempt,
            references: Vec::new(),
        }
    }
    pub const fn attempt(&self) -> AttemptToken {
        self.attempt
    }
    pub fn references(&self) -> &[ObjectIdentity] {
        &self.references
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorWriteOperationId;

    fn object(path: &str) -> ObjectIdentity {
        ObjectIdentity::new(path).unwrap()
    }
    fn ledger() -> (ArtifactLedger, AttemptToken) {
        let token = OperationToken::from_write(ConnectorWriteOperationId::from_bytes([1; 16]));
        (ArtifactLedger::new(token), AttemptToken::new(token, 0))
    }
    fn produced(
        ledger: &mut ArtifactLedger,
        attempt: AttemptToken,
        path: &str,
        kind: ArtifactKind,
    ) -> ObjectIdentity {
        let object = object(path);
        ledger
            .register(ArtifactRecord::produced(object.clone(), attempt, kind))
            .unwrap();
        ledger.confirm_written_for_test(&object).unwrap();
        object
    }
    #[test]
    fn received_objects_cannot_be_written_and_paths_are_never_registered_twice() {
        let (mut ledger, attempt) = ledger();
        let data = object("data.parquet");
        ledger
            .register(ArtifactRecord::received(
                data.clone(),
                ReceivedPublication::ViaManifest,
            ))
            .unwrap();
        assert!(ledger.mark_writing(&data).is_err());
        let manifest = produced(
            &mut ledger,
            attempt,
            "manifest.avro",
            ArtifactKind::Manifest,
        );
        assert!(ledger.mark_writing(&manifest).is_err());
        ledger.forget_removed(&manifest);
        assert!(
            ledger
                .register(ArtifactRecord::produced(
                    manifest,
                    attempt,
                    ArtifactKind::Manifest
                ))
                .is_err()
        );
        assert!(ledger.mark_writing(&object("external.parquet")).is_err());
    }
    #[test]
    fn retirement_closes_admission_but_preserves_actual_writer_completion() {
        let (mut ledger, attempt) = ledger();
        let manifest = object("manifest.avro");
        ledger
            .register(ArtifactRecord::produced(
                manifest.clone(),
                attempt,
                ArtifactKind::Manifest,
            ))
            .unwrap();
        ledger.mark_writing(&manifest).unwrap();
        ledger.retire(&manifest).unwrap();
        assert!(ledger.check_writable(&manifest).is_err());
        ledger.mark_written(&manifest).unwrap();
        assert!(ledger.record_manifest(&manifest, &[], &[]).is_err());
    }
    #[test]
    fn frozen_references_include_all_snapshots_and_received_objects() {
        let (mut ledger, attempt) = ledger();
        let data = object("data.parquet");
        let provenance = object("provenance.json");
        ledger
            .register(ArtifactRecord::received(
                data.clone(),
                ReceivedPublication::ViaManifest,
            ))
            .unwrap();
        ledger
            .register(ArtifactRecord::received(
                provenance.clone(),
                ReceivedPublication::WithRequest,
            ))
            .unwrap();
        let manifest = produced(&mut ledger, attempt, "data.avro", ArtifactKind::Manifest);
        ledger
            .record_manifest(&manifest, &["data.parquet".into()], &[])
            .unwrap();
        let first = produced(
            &mut ledger,
            attempt,
            "first.avro",
            ArtifactKind::ManifestList,
        );
        ledger
            .record_manifest_list(&first, &["data.avro".into(), "external.avro".into()])
            .unwrap();
        let last = produced(
            &mut ledger,
            attempt,
            "last.avro",
            ArtifactKind::ManifestList,
        );
        ledger.record_manifest_list(&last, &[]).unwrap();
        let orphan = produced(&mut ledger, attempt, "orphan.avro", ArtifactKind::Manifest);
        let facts = ledger
            .snapshot_for_request(attempt, &[first.clone(), last.clone()], &[])
            .unwrap();
        let expected: BTreeSet<_> = [data, provenance, manifest, first, last]
            .into_iter()
            .collect();
        assert_eq!(
            facts.references().iter().cloned().collect::<BTreeSet<_>>(),
            expected
        );
        let remainder = ledger.settle_published(facts.references());
        assert_eq!(remainder.len(), 1);
        assert_eq!(remainder[0].object, orphan);
        assert!(remainder[0].retired);
        assert_eq!(ledger.selected(CleanupScope::EntireOperation), remainder);
    }
    #[test]
    fn uncovered_received_object_refuses_publication_and_replacements_cannot_coexist() {
        let (mut ledger, attempt) = ledger();
        let data = object("uncovered.parquet");
        ledger
            .register(ArtifactRecord::received(
                data,
                ReceivedPublication::ViaManifest,
            ))
            .unwrap();
        assert!(ledger.snapshot_for_request(attempt, &[], &[]).is_err());
        let manifest = produced(
            &mut ledger,
            attempt,
            "replacement.avro",
            ArtifactKind::Manifest,
        );
        ledger
            .record_manifest(&manifest, &[], &["source.avro".into()])
            .unwrap();
        let list = produced(
            &mut ledger,
            attempt,
            "list.avro",
            ArtifactKind::ManifestList,
        );
        assert!(
            ledger
                .record_manifest_list(&list, &["replacement.avro".into(), "source.avro".into()])
                .is_err()
        );
    }
}
