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
use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;

use super::{AttemptToken, ObjectIdentity, OperationToken, invalid};
use super::{ManifestReuseBasis, ManifestRewriteReuse};
use crate::iceberg::Result;
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{ManifestFile, TableMetadata};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPayloadRetentionGuard,
};

use crate::commit::new_manifests::RegisteredPaths;

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
    fn registered_manifest_paths(&self, _snapshot_id: i64) -> Result<Option<Arc<RegisteredPaths>>> {
        Ok(None)
    }
    fn observe_registered_manifest(
        &self,
        _snapshot_id: i64,
        _replaces: &[String],
    ) -> Result<Option<RegisteredManifestObservation>> {
        Ok(None)
    }
    fn record_registered_manifest(
        &self,
        _object: &ObjectIdentity,
        _observation: RegisteredManifestObservation,
        _replaces: &[String],
    ) -> Result<()> {
        Err(invariant("Manifest writer does not own a registered stage"))
    }
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
    // Direct rolling outputs cover a consecutive slice of the immutable source
    // sequence. The same fact is retained when a V1 output is reencoded.
    registered_run: Option<Range<usize>>,
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
            registered_run: None,
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
            registered_run: None,
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

fn registration_size_error(message: impl Into<String>) -> crate::iceberg::Error {
    let message = message.into();
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, message.clone()).with_source(
        ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message),
    )
}

struct RegisteredStage {
    stage: usize,
    paths: Option<Arc<RegisteredPaths>>,
    snapshot: Option<i64>,
}

impl std::fmt::Debug for RegisteredStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredStage")
            .field("stage", &self.stage)
            .field("snapshot", &self.snapshot)
            .field("paths", &self.paths.as_ref().map(|paths| paths.len()))
            .finish()
    }
}

// Collect path evidence using source positions. The writer can borrow a
// DataFile path to check its identity before moving the file into the SDK.
pub struct RegisteredManifestObservation {
    paths: Arc<RegisteredPaths>,
    snapshot: i64,
    runs: Vec<Range<usize>>,
    replaced: bool,
    run_index: usize,
    cursor: Option<usize>,
    start: Option<usize>,
    added: usize,
    _run_backing: Option<ConnectorPayloadRetentionGuard>,
}

impl RegisteredManifestObservation {
    pub(crate) fn push_added(&mut self, actual: &str) -> Result<()> {
        let expected = match self.cursor {
            Some(ordinal) => ordinal,
            None if !self.replaced => self.paths.index(actual).ok_or_else(|| {
                invariant("Manifest added a file outside its Iceberg registration")
            })?,
            None => {
                return Err(invariant(
                    "Manifest replaced fewer Iceberg registered files than it added",
                ));
            }
        };
        if self.paths.get(expected) != Some(actual) {
            return Err(invariant(
                "Manifest changed the Iceberg registered file sequence",
            ));
        }
        let next = expected
            .checked_add(1)
            .ok_or_else(|| invariant("Iceberg registered file ordinal overflow"))?;
        self.start.get_or_insert(expected);
        self.added += 1;
        self.cursor = if self.replaced && next == self.runs[self.run_index].end {
            self.run_index += 1;
            self.runs.get(self.run_index).map(|run| run.start)
        } else {
            Some(next)
        };
        Ok(())
    }

    fn finish(&self) -> Result<Option<Range<usize>>> {
        if self.replaced && self.cursor.is_some() {
            return Err(invariant("Manifest omitted an Iceberg registered file"));
        }
        if self.replaced && self.runs.len() != 1 {
            // A merge can replace disjoint runs. The checked source order
            // remains in the manifest replacements and needs no new path set.
            return Ok(None);
        }
        self.start
            .map(|start| {
                start
                    .checked_add(self.added)
                    .map(|end| start..end)
                    .ok_or_else(|| invariant("Iceberg registered manifest count overflow"))
            })
            .transpose()
    }
}

#[derive(Debug)]
pub struct ArtifactLedger {
    operation: OperationToken,
    records: BTreeMap<ObjectIdentity, ArtifactRecord>,
    registered_paths: BTreeSet<ObjectIdentity>,
    round_artifacts: BTreeMap<AttemptToken, Vec<ObjectIdentity>>,
    compact_received: Option<Arc<[ObjectIdentity]>>,
    registration: Option<RegisteredStage>,
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
            registration: None,
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
        if self.registration.is_some() && matches!(record.origin, ArtifactOrigin::Received { .. }) {
            return Err(invariant(
                "Registered Iceberg files and adopted writer objects cannot mix",
            ));
        }
        if self
            .registration
            .as_ref()
            .and_then(|stage| stage.paths.as_ref())
            .is_some_and(|paths| paths.index(record.object.path()).is_some())
        {
            return Err(invariant(
                "An Iceberg artifact cannot replace a registered external file",
            ));
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

    pub(crate) fn install_registered_paths(
        &mut self,
        stage: usize,
        paths: Arc<RegisteredPaths>,
    ) -> Result<()> {
        if self.registration.is_some() || !self.records.is_empty() {
            return Err(invariant(
                "Iceberg registered stage requires a fresh artifact ledger",
            ));
        }
        self.registration = Some(RegisteredStage {
            stage,
            paths: Some(paths),
            snapshot: None,
        });
        Ok(())
    }

    /// Release preparation-only aliases after the original attempt loop exits.
    /// Recovery and cleanup continue to own the actual published artifacts.
    pub(crate) fn take_registered_paths(&mut self) -> Option<Arc<RegisteredPaths>> {
        self.registration.as_mut()?.paths.take()
    }

    pub(crate) fn registered_stage(&self) -> Option<usize> {
        self.registration.as_ref().map(|stage| stage.stage)
    }

    pub(crate) fn registered_manifest_paths(
        &self,
        stage: usize,
        snapshot: i64,
    ) -> Result<Option<&Arc<RegisteredPaths>>> {
        let Some(registration) = self
            .registration
            .as_ref()
            .filter(|value| value.stage == stage)
        else {
            return Ok(None);
        };
        if registration.snapshot.is_some_and(|id| id != snapshot) {
            return Err(invariant(
                "Iceberg registered manifest changed its snapshot",
            ));
        }
        registration
            .paths
            .as_ref()
            .map(Some)
            .ok_or_else(|| invariant("Iceberg registered preparation already exited"))
    }

    pub(crate) fn observe_registered_manifest(
        &self,
        stage: usize,
        snapshot: i64,
        replaces: &[String],
    ) -> Result<Option<RegisteredManifestObservation>> {
        let Some(paths) = self.registered_manifest_paths(stage, snapshot)? else {
            return Ok(None);
        };
        let runs = replaces
            .iter()
            .filter_map(|path| self.records.get(path.as_str()))
            .filter_map(|record| record.registered_run.as_ref());
        let count = runs.clone().count();
        let (mut expected, mut backing) = (Vec::new(), None);
        if count != 0 {
            let size = count
                .checked_mul(size_of::<Range<usize>>())
                .ok_or_else(|| {
                    registration_size_error("Iceberg registered manifest run extent overflow")
                })?;
            backing = Some(paths.reserve(size, "Iceberg registered manifest source runs")?);
            expected
                .try_reserve_exact(count)
                .map_err(|error| registration_size_error(error.to_string()))?;
            expected.extend(runs.cloned());
        }
        Ok(Some(RegisteredManifestObservation {
            paths: Arc::clone(paths),
            snapshot,
            cursor: expected.first().map(|run| run.start),
            runs: expected,
            replaced: !replaces.is_empty(),
            run_index: 0,
            start: None,
            added: 0,
            _run_backing: backing,
        }))
    }

    pub(crate) fn record_registered_manifest(
        &mut self,
        object: &ObjectIdentity,
        observation: RegisteredManifestObservation,
        replaces: &[String],
    ) -> Result<()> {
        self.produced(object, ArtifactKind::Manifest)?;
        let registration = self
            .registration
            .as_ref()
            .ok_or_else(|| invariant("Missing Iceberg registered manifest source"))?;
        if !registration
            .paths
            .as_ref()
            .is_some_and(|paths| Arc::ptr_eq(paths, &observation.paths))
            || registration
                .snapshot
                .is_some_and(|id| id != observation.snapshot)
        {
            return Err(invariant(
                "Iceberg registered manifest observation lost its source",
            ));
        }
        let range = observation.finish()?;
        let record = self.records.get_mut(object).expect("checked manifest");
        record.registered_run = range;
        record.added_entries = observation.added;
        record.resolved_added_entries = observation.added;
        record.replaces = replaces.to_vec();
        Ok(())
    }

    pub(crate) fn seal_registered_stage(
        &mut self,
        stage: usize,
        snapshot: i64,
        manifests: &[ManifestFile],
        expected_entries: usize,
    ) -> Result<()> {
        let registration = self
            .registration
            .as_ref()
            .filter(|registration| registration.stage == stage)
            .ok_or_else(|| invariant("Iceberg registered manifest stage lost its input"))?;
        let paths = registration
            .paths
            .as_ref()
            .ok_or_else(|| invariant("Iceberg registered preparation already exited"))?;
        if paths.len() != expected_entries || registration.snapshot.is_some_and(|id| id != snapshot)
        {
            return Err(invariant(
                "Iceberg registered manifest stage changed its admitted source",
            ));
        }
        let mut originals = BTreeSet::new();
        let mut next = 0usize;
        for descriptor in manifests {
            let (object, record) = self
                .records
                .get_key_value(descriptor.manifest_path.as_str())
                .ok_or_else(|| invariant("Missing newly written registered manifest"))?;
            self.produced(object, ArtifactKind::Manifest)?;
            if descriptor.added_snapshot_id != snapshot
                || descriptor.added_files_count.map(|count| count as usize)
                    != Some(record.added_entries)
                || !originals.insert(object.clone())
            {
                return Err(invariant(
                    "Iceberg registered manifest has unresolved or duplicate additions",
                ));
            }
            let run = record
                .registered_run
                .as_ref()
                .ok_or_else(|| invariant("Iceberg registered manifest has no source run"))?;
            if run.start != next || run.len() != record.added_entries {
                return Err(invariant(
                    "Iceberg registered manifests changed the admitted file sequence",
                ));
            }
            next = run.end;
        }
        if next != expected_entries {
            return Err(invariant(
                "Iceberg registered manifests do not cover the admitted file sequence",
            ));
        }
        self.registration
            .as_mut()
            .expect("checked registration")
            .snapshot = Some(snapshot);
        self.written_stages.insert(snapshot, originals);
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
        if observed_stages.len() != self.written_stages.len() {
            return Err(invariant("Frozen request omits a compact snapshot stage"));
        }
        if self
            .registration
            .as_ref()
            .is_some_and(|registration| registration.snapshot.is_none())
        {
            return Err(invariant(
                "Frozen request omits unsealed Iceberg registered files",
            ));
        }
        if let Some(received) = &self.compact_received {
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
