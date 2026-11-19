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
use std::sync::Arc;

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
}
impl ArtifactKind {
    pub const fn extension(self) -> &'static str {
        match self {
            Self::Manifest | Self::ManifestList => "avro",
            Self::Statistics => "puffin",
            #[cfg(test)]
            Self::DeletionVector => "puffin",
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
        added_paths: &[String],
        existing_paths: &[String],
        replaces: &[String],
    ) -> Result<()>;
    fn record_manifest_list(
        &self,
        object: &ObjectIdentity,
        snapshot_id: i64,
        paths: &[String],
    ) -> Result<()>;
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
    pub added_entries: usize,
    pub resolved_added_entries: usize,
    pub list_snapshot_id: Option<i64>,
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
            added_entries: 0,
            resolved_added_entries: 0,
            list_snapshot_id: None,
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
            added_entries: 0,
            resolved_added_entries: 0,
            list_snapshot_id: None,
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
    round_artifacts: BTreeMap<AttemptToken, Vec<ObjectIdentity>>,
    compact_received: Option<Arc<[ObjectIdentity]>>,
    written_stages: BTreeMap<i64, BTreeSet<ObjectIdentity>>,
}
impl ArtifactLedger {
    pub fn new(operation: OperationToken) -> Self {
        Self {
            operation,
            records: BTreeMap::new(),
            registered_paths: BTreeSet::new(),
            round_artifacts: BTreeMap::new(),
            compact_received: None,
            written_stages: BTreeMap::new(),
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
        if let ArtifactOrigin::Produced {
            attempt,
            kind: ArtifactKind::ManifestList | ArtifactKind::Statistics,
        } = record.origin
        {
            self.round_artifacts
                .entry(attempt)
                .or_default()
                .push(record.object.clone());
        }
        self.records.insert(record.object.clone(), record);
        Ok(())
    }
    pub(crate) fn retire_round(&mut self, attempt: AttemptToken) -> Result<()> {
        for object in self.round_artifacts.remove(&attempt).into_iter().flatten() {
            self.retire(&object)?;
        }
        Ok(())
    }

    pub(crate) fn seal_compact_stage(
        &mut self,
        snapshot_id: i64,
        manifests: &[ManifestFile],
        expected_entries: usize,
    ) -> Result<()> {
        let mut covered = BTreeSet::new();
        let mut originals = BTreeSet::new();
        let mut entries = 0usize;
        for descriptor in manifests {
            let (object, record) = self
                .records
                .get_key_value(descriptor.manifest_path.as_str())
                .ok_or_else(|| invariant("Missing newly written compact manifest"))?;
            self.produced(object, ArtifactKind::Manifest)?;
            if descriptor.added_snapshot_id != snapshot_id
                || record.added_entries != record.resolved_added_entries
                || !originals.insert(object.clone())
            {
                return Err(invariant(
                    "Compact manifest has unresolved or duplicate additions",
                ));
            }
            covered.extend(record.covers.iter().cloned());
            entries = entries
                .checked_add(record.added_entries)
                .ok_or_else(|| invariant("Compact manifest addition count overflow"))?;
        }
        if entries != expected_entries {
            return Err(invariant(
                "Compact manifests do not cover the accepted entry count",
            ));
        }
        // All compact stages are owned by this operation. The data stage seals the
        // immutable received set, and a format rewrite preserves that exact set.
        if let Some(received) = &self.compact_received {
            for object in received.iter() {
                let record = &self.records[object];
                if record.origin
                    == (ArtifactOrigin::Received {
                        publishes: ReceivedPublication::ViaManifest,
                    })
                    && !covered.contains(object)
                {
                    return Err(invariant("Reencoded manifests lost a received object"));
                }
            }
        } else {
            let mut received = Vec::new();
            for record in self.records.values() {
                if let ArtifactOrigin::Received { publishes } = record.origin {
                    if record.retired
                        || (publishes == ReceivedPublication::ViaManifest
                            && !covered.contains(&record.object))
                    {
                        return Err(invariant("Compact manifests omit a received object"));
                    }
                    received.push(record.object.clone());
                }
            }
            self.compact_received = Some(received.into());
        }
        self.written_stages.insert(snapshot_id, originals);
        Ok(())
    }

    fn check_compact_stage(
        &self,
        list: &ArtifactRecord,
        originals: &BTreeSet<ObjectIdentity>,
    ) -> Result<()> {
        let mut selected = BTreeSet::new();
        for member in &list.members {
            let output = self.produced(member, ArtifactKind::Manifest)?;
            if originals.contains(member) {
                selected.insert(member.path());
                continue;
            }
            if output.replaces.is_empty() {
                continue;
            }
            let mut expected = 0usize;
            let mut replaced_new = false;
            for source in &output.replaces {
                if let Some(original) = originals.get(source.as_str()) {
                    replaced_new = true;
                    if !selected.insert(original.path()) {
                        return Err(invariant(
                            "Compact manifest is covered by more than one list member",
                        ));
                    }
                    expected = expected
                        .checked_add(
                            self.produced(original, ArtifactKind::Manifest)?
                                .added_entries,
                        )
                        .ok_or_else(|| invariant("Merged manifest addition count overflow"))?;
                }
            }
            if replaced_new
                && (output.added_entries != expected || output.resolved_added_entries != expected)
            {
                return Err(invariant(
                    "Merged manifest does not preserve compact addition count",
                ));
            }
        }
        if selected.len() != originals.len() {
            return Err(invariant("Frozen list omits a compact manifest"));
        }
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
        added_paths: &[String],
        existing_paths: &[String],
        replaces: &[String],
    ) -> Result<()> {
        self.produced(object, ArtifactKind::Manifest)?;
        let mut covers = BTreeSet::new();
        let mut resolved = 0;
        for (path, added) in added_paths
            .iter()
            .map(|path| (path, true))
            .chain(existing_paths.iter().map(|path| (path, false)))
        {
            if let Some(record) = self.records.get(path.as_str())
                && matches!(
                    record.origin,
                    ArtifactOrigin::Received {
                        publishes: ReceivedPublication::ViaManifest
                    }
                )
            {
                covers.insert(record.object.clone());
                resolved += usize::from(added);
            }
        }
        let record = self.records.get_mut(object).expect("checked manifest");
        record.covers = covers;
        record.added_entries = added_paths.len();
        record.resolved_added_entries = resolved;
        record.replaces = replaces.to_vec();
        Ok(())
    }
    pub fn record_manifest_list(
        &mut self,
        object: &ObjectIdentity,
        snapshot_id: i64,
        paths: &[String],
    ) -> Result<()> {
        self.produced(object, ArtifactKind::ManifestList)?;
        let paths: BTreeSet<_> = paths.iter().map(String::as_str).collect();
        let mut members = BTreeSet::new();
        for path in &paths {
            if let Some((identity, record)) = self.records.get_key_value(*path)
                && matches!(record.origin, ArtifactOrigin::Produced { .. })
            {
                let record = self.produced(identity, ArtifactKind::Manifest)?;
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
        let record = self.records.get_mut(object).expect("checked list");
        record.members = members;
        record.list_snapshot_id = Some(snapshot_id);
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
        let mut observed_stages = BTreeSet::new();
        for object in lists {
            if !self.records.contains_key(object) {
                continue;
            }
            let list = self.produced(object, ArtifactKind::ManifestList)?;
            references.insert(list.object.clone());
            if let Some(id) = list.list_snapshot_id
                && let Some(originals) = self.written_stages.get(&id)
            {
                self.check_compact_stage(list, originals).inspect_err(|error| {
                    tracing::error!(operation = ?self.operation, snapshot = id, %error, "Compact manifest coverage check failed");
                })?;
                observed_stages.insert(id);
            }
            for member in &list.members {
                let manifest = self.produced(member, ArtifactKind::Manifest)?;
                references.insert(manifest.object.clone());
                if self.compact_received.is_none() {
                    covered.extend(manifest.covers.iter().cloned());
                }
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
        if let Some(received) = &self.compact_received {
            if observed_stages.len() != self.written_stages.len() {
                return Err(invariant("Frozen request omits a compact snapshot stage"));
            }
            return Ok(AttemptArtifacts {
                attempt,
                references: ArtifactReferences::new(Arc::clone(received), references),
            });
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
            references: ArtifactReferences::new(Arc::default(), references),
        })
    }
    /// Published records are settled, while their immutable paths remain reserved forever.
    pub fn settle_published(
        &mut self,
        references: BTreeSet<&ObjectIdentity>,
    ) -> Vec<ArtifactRecord> {
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

/// Exact published ownership. Compact publications share one immutable
/// received set across attempts and only freeze new metadata references.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArtifactReferences {
    received: Arc<[ObjectIdentity]>,
    prepared: Arc<[ObjectIdentity]>,
}

impl ArtifactReferences {
    fn new(received: Arc<[ObjectIdentity]>, prepared: BTreeSet<ObjectIdentity>) -> Self {
        Self {
            received,
            prepared: prepared.into_iter().collect::<Vec<_>>().into(),
        }
    }

    pub fn contains(&self, object: &ObjectIdentity) -> bool {
        self.received.binary_search(object).is_ok() || self.prepared.binary_search(object).is_ok()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.received.len() + self.prepared.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.received.is_empty() && self.prepared.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &ObjectIdentity> + Clone {
        self.into_iter()
    }
}

impl<'a> IntoIterator for &'a ArtifactReferences {
    type Item = &'a ObjectIdentity;
    type IntoIter = std::iter::Chain<
        std::slice::Iter<'a, ObjectIdentity>,
        std::slice::Iter<'a, ObjectIdentity>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.received.iter().chain(self.prepared.iter())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptArtifacts {
    attempt: AttemptToken,
    references: ArtifactReferences,
}
impl AttemptArtifacts {
    pub fn empty(attempt: AttemptToken) -> Self {
        Self {
            attempt,
            references: ArtifactReferences::default(),
        }
    }
    pub const fn attempt(&self) -> AttemptToken {
        self.attempt
    }
    pub fn references(&self) -> &ArtifactReferences {
        &self.references
    }
}

#[cfg(test)]
mod tests;
