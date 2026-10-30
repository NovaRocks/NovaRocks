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

//! Foreground reclamation after one exact Applied expiration. This executor
//! never publishes metadata, changes a journal verdict, or queues detached work.

#[cfg(test)]
#[path = "reclamation/io_oracles.rs"]
mod io_oracles;
#[path = "reclamation/known_length.rs"]
mod known_length;
#[cfg(test)]
#[path = "reclamation/round_oracles.rs"]
mod round_oracles;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

use super::retention::{RetentionPlan, require_gc};
use crate::access_binding::IcebergReadBinding;
use crate::catalog::transaction::{CommitProof, MetadataPredecessorObservation};
use crate::catalog::{
    CatalogCommitBase, CatalogMetadataLoad, CatalogMetadataValidator, CatalogTableName,
};
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, DataFileFormat, FormatVersion, ManifestFile, ManifestList, ManifestStatus,
    Snapshot, TableMetadata,
};
use crate::metadata_context::IcebergMetadataContext;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorRequestContext,
};

const MARKER: &str = "novarocks.connector.maintenance.v2";

// This clock only measures freshness; it never grants or extends an I/O budget.
trait ReclamationClock: Send + Sync {
    fn now(&self) -> Instant;
}
struct MonotonicClock;
impl ReclamationClock for MonotonicClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

// Production always resolves the original request-bound FileIO/access. The
// private adapter also permits actual controlled object services in unit tests.
trait ReclamationStorage: Send + Sync {
    fn file_io(&self, location: &str, binding: IcebergReadBinding) -> FileIO;
    fn delete_access(
        &self,
        location: &str,
        binding: &IcebergReadBinding,
    ) -> Result<(crate::opendal::Operator, String), ConnectorError>;
}
struct AuthorizedStorage;
impl ReclamationStorage for AuthorizedStorage {
    fn file_io(&self, location: &str, binding: IcebergReadBinding) -> FileIO {
        crate::fs_io::build_file_io_for_location(location, binding)
    }
    fn delete_access(
        &self,
        location: &str,
        binding: &IcebergReadBinding,
    ) -> Result<(crate::opendal::Operator, String), ConnectorError> {
        let access =
            crate::fs_io::resolve_access_for_location(location, binding).map_err(unavailable)?;
        Ok((
            access.operator(),
            access.single_relative_path().map_err(corrupt)?.to_owned(),
        ))
    }
}

struct ReclamationPorts<'a> {
    catalog: &'a dyn crate::catalog::NovaRocksCatalog,
    planning_binding: &'a IcebergReadBinding,
    clock: &'a dyn ReclamationClock,
    storage: &'a dyn ReclamationStorage,
}

#[derive(Clone, Default)]
struct Reachability {
    paths: BTreeSet<String>,
    data: BTreeSet<String>,
    dv: BTreeMap<String, BTreeSet<String>>,
    puffin: BTreeSet<String>,
    lengths: BTreeMap<String, u64>,
    conflicting_lengths: bool,
}
impl Reachability {
    fn extend(&mut self, other: &Self) {
        self.paths.extend(other.paths.iter().cloned());
        self.data.extend(other.data.iter().cloned());
        self.puffin.extend(other.puffin.iter().cloned());
        self.conflicting_lengths |= other.conflicting_lengths;
        for (path, length) in &other.lengths {
            if self
                .lengths
                .insert(path.clone(), *length)
                .is_some_and(|previous| previous != *length)
            {
                self.conflicting_lengths = true;
            }
        }
        for (path, targets) in &other.dv {
            self.dv
                .entry(path.clone())
                .or_default()
                .extend(targets.iter().cloned());
        }
    }
}
/// Both interpretations come from the same complete physical decode. Deleted
/// entries remain collection candidates but cannot protect live content.
#[derive(Clone, Default)]
struct SnapshotFiles {
    live: Reachability,
    candidates: Reachability,
}
impl SnapshotFiles {
    fn extend(&mut self, other: &Self) {
        self.live.extend(&other.live);
        self.candidates.extend(&other.candidates);
    }
    fn structure(&mut self, path: &str, length: Option<u64>) {
        for facts in [&mut self.live, &mut self.candidates] {
            facts.paths.insert(path.to_owned());
            if let Some(length) = length {
                facts.lengths.insert(path.to_owned(), length);
            }
        }
    }
}
#[derive(Default)]
struct ScanCache {
    roots: BTreeMap<(i64, String), SnapshotFiles>,
    lists: HashMap<String, (FormatVersion, Vec<ManifestFile>)>,
    manifests: HashMap<String, (ManifestFile, SnapshotFiles)>,
    puffin: HashMap<String, BTreeSet<String>>,
    puffin_reads: usize,
    catalog_reads: usize,
    list_reads: usize,
    manifest_reads: usize,
}
impl ScanCache {
    async fn snapshot(
        &mut self,
        snapshot: &Snapshot,
        version: FormatVersion,
        io: &FileIO,
    ) -> Result<SnapshotFiles, ConnectorError> {
        self.root(
            snapshot.snapshot_id(),
            snapshot.manifest_list(),
            version,
            io,
        )
        .await
    }
    async fn root(
        &mut self,
        id: i64,
        list_path: &str,
        version: FormatVersion,
        io: &FileIO,
    ) -> Result<SnapshotFiles, ConnectorError> {
        let key = (id, list_path.to_owned());
        if let Some(complete) = self.roots.get(&key) {
            return Ok(complete.clone());
        }
        let descriptors = match self.lists.get(list_path) {
            Some((original_version, descriptors)) => {
                if *original_version != version {
                    return Err(corrupt(
                        "Immutable manifest-list path has contradictory format versions",
                    ));
                }
                descriptors.clone()
            }
            None => {
                let bytes = io
                    .new_input(list_path)
                    .map_err(iceberg_error)?
                    .read()
                    .await
                    .map_err(iceberg_error)?;
                let list =
                    ManifestList::parse_with_version(&bytes, version).map_err(iceberg_error)?;
                self.list_reads += 1;
                let descriptors = list.entries().to_vec();
                self.lists
                    .insert(list_path.to_owned(), (version, descriptors.clone()));
                descriptors
            }
        };
        let mut result = SnapshotFiles::default();
        result.structure(list_path, None);
        for descriptor in descriptors {
            let facts = match self.manifests.get(&descriptor.manifest_path) {
                Some((original, facts)) => {
                    // No HEAD/stat is needed for immutable read caches. Inline
                    // list facts must nevertheless agree for the same path.
                    if original != &descriptor {
                        return Err(corrupt(
                            "Immutable manifest path has contradictory inline descriptors",
                        ));
                    }
                    facts.clone()
                }
                None => {
                    let manifest = crate::manifest::load_manifest(&descriptor, io)
                        .await
                        .map_err(iceberg_error)?;
                    self.manifest_reads += 1;
                    let mut facts = SnapshotFiles::default();
                    facts.structure(
                        &descriptor.manifest_path,
                        Some(
                            u64::try_from(descriptor.manifest_length)
                                .map_err(|_| corrupt("Manifest inline length is negative"))?,
                        ),
                    );
                    for entry in manifest.entries() {
                        let mut file_facts = Reachability::default();
                        let file = entry.data_file();
                        let path = file.file_path().to_owned();
                        file_facts.paths.insert(path.clone());
                        let length = file.file_size_in_bytes();
                        if file_facts
                            .lengths
                            .insert(path.clone(), length)
                            .is_some_and(|previous| previous != length)
                        {
                            return Err(corrupt(
                                "Immutable file path has contradictory inline lengths",
                            ));
                        }
                        if file.content_type() == DataContentType::Data {
                            file_facts.data.insert(path.clone());
                        }
                        if file.file_format() == DataFileFormat::Puffin {
                            file_facts.puffin.insert(path.clone());
                            // Unknown or partially specified DV associations do
                            // not grant permission to delete the whole object.
                            let identity = crate::commit::model::EntryIdentity::try_from(file)
                                .map_err(iceberg_error)?;
                            let crate::commit::model::EntryIdentity::DeletionVector {
                                referenced_data_file,
                                ..
                            } = identity
                            else {
                                return Err(corrupt(
                                    "Unrecognized Puffin manifest entry cannot authorize reclamation",
                                ));
                            };
                            file_facts
                                .dv
                                .entry(path)
                                .or_default()
                                .insert(referenced_data_file);
                        }
                        facts.candidates.extend(&file_facts);
                        if entry.status != ManifestStatus::Deleted {
                            facts.live.extend(&file_facts);
                        }
                    }
                    if facts.live.conflicting_lengths || facts.candidates.conflicting_lengths {
                        return Err(corrupt(
                            "Immutable manifest entries have contradictory inline lengths",
                        ));
                    }
                    self.manifests.insert(
                        descriptor.manifest_path.clone(),
                        (descriptor.clone(), facts.clone()),
                    );
                    facts
                }
            };
            result.extend(&facts);
        }
        // Root coverage is published only after the entire scan succeeds.
        if result.live.conflicting_lengths || result.candidates.conflicting_lengths {
            return Err(corrupt(
                "Immutable snapshot paths have contradictory inline lengths",
            ));
        }
        self.roots.insert(key, result.clone());
        Ok(result)
    }
    async fn puffin_associations(
        &mut self,
        paths: &BTreeSet<String>,
        lengths: &BTreeMap<String, u64>,
        io: &FileIO,
    ) -> Result<BTreeMap<String, BTreeSet<String>>, ConnectorError> {
        let mut result = BTreeMap::new();
        for path in paths {
            if !self.puffin.contains_key(path) {
                let reader = crate::iceberg::puffin::PuffinReader::new(known_length::bind(
                    io.new_input(path).map_err(iceberg_error)?,
                    *lengths
                        .get(path)
                        .ok_or_else(|| corrupt("Puffin has no immutable inline length"))?,
                ))
                .await
                .map_err(iceberg_error)?;
                let metadata = reader.file_metadata().await.map_err(iceberg_error)?;
                let mut references = BTreeSet::new();
                for blob in metadata.blobs() {
                    match blob.blob_type() {
                        crate::iceberg::puffin::DELETION_VECTOR_V1 => {
                            let target = blob
                                .properties()
                                .get("referenced-data-file")
                                .filter(|value| !value.is_empty())
                                .ok_or_else(|| {
                                    corrupt("Puffin DV has no complete data-file association")
                                })?;
                            references.insert(target.clone());
                        }
                        crate::iceberg::puffin::APACHE_DATASKETCHES_THETA_V1 => {}
                        _ => {
                            return Err(corrupt(
                                "Unknown Puffin blob type prevents whole-object reclamation",
                            ));
                        }
                    }
                }
                self.puffin_reads += 1;
                self.puffin.insert(path.clone(), references);
            }
            result.insert(path.clone(), self.puffin[path].clone());
        }
        Ok(result)
    }
    async fn metadata(
        &mut self,
        metadata: &TableMetadata,
        io: &FileIO,
    ) -> Result<Reachability, ConnectorError> {
        let mut result = Reachability::default();
        for snapshot in metadata.snapshots() {
            result.extend(
                &self
                    .snapshot(snapshot.as_ref(), metadata.format_version(), io)
                    .await?
                    .live,
            );
        }
        for file in metadata.statistics_iter() {
            result.puffin.insert(file.statistics_path.clone());
            result.lengths.insert(
                file.statistics_path.clone(),
                u64::try_from(file.file_size_in_bytes)
                    .map_err(|_| corrupt("Statistics inline length is negative"))?,
            );
            result.paths.insert(file.statistics_path.clone());
        }
        for file in metadata.partition_statistics_iter() {
            result.paths.insert(file.statistics_path.clone());
        }
        if result.conflicting_lengths {
            return Err(corrupt(
                "Complete metadata paths have contradictory inline lengths",
            ));
        }
        Ok(result)
    }
}

struct FreshView {
    base: CatalogCommitBase,
    validator: Option<CatalogMetadataValidator>,
    requested: Instant,
}
async fn refresh(
    catalog: &dyn crate::catalog::NovaRocksCatalog,
    clock: &dyn ReclamationClock,
    target: &CatalogTableName,
    binding: &IcebergReadBinding,
    view: &mut Option<FreshView>,
    uuid: uuid::Uuid,
    cache: &mut ScanCache,
    io: &FileIO,
    protected: &mut Reachability,
) -> Result<(), ConnectorError> {
    let requested = clock.now();
    cache.catalog_reads += 1;
    let validator = view.as_ref().and_then(|view| view.validator.as_ref());
    let loaded = catalog
        .load_complete_metadata_for_read(target.clone(), binding.clone(), validator)
        .await?;
    // The successful HTTP/owner response itself must leave a usable window.
    // Slow root scanning is different: cache complete facts, then revalidate.
    if refresh_due(requested, clock.now(), catalog.evidence_policy().freshness) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "freshness_exhausted: complete metadata response exceeded the reclamation freshness window; no deletion authorized",
        ));
    }
    // A failed authorization/root scan must leave the previous qualification
    // unchanged. Only a matching 304 needs the retained baseline copied here.
    let mut candidate = match &loaded {
        CatalogMetadataLoad::NotModified => view.as_ref().map(|previous| FreshView {
            base: CatalogCommitBase {
                metadata: previous.base.metadata.clone(),
                metadata_location: previous.base.metadata_location.clone(),
            },
            validator: previous.validator.clone(),
            requested: previous.requested,
        }),
        CatalogMetadataLoad::Complete { .. } => None,
    };
    accept_complete_load(&mut candidate, loaded, requested)?;
    let current = candidate.as_ref().expect("complete metadata candidate");
    if current.base.metadata.uuid() != uuid {
        return Err(corrupt("Reclamation table UUID changed"));
    }
    require_gc(&current.base.metadata).map_err(iceberg_error)?;
    let roots = cache.metadata(&current.base.metadata, io).await?;
    protected.extend(&roots);
    *view = candidate;
    Ok(())
}

fn accept_complete_load(
    view: &mut Option<FreshView>,
    loaded: CatalogMetadataLoad,
    requested: Instant,
) -> Result<(), ConnectorError> {
    match loaded {
        CatalogMetadataLoad::Complete { base, validator } => {
            *view = Some(FreshView {
                base: *base,
                validator,
                requested,
            })
        }
        CatalogMetadataLoad::NotModified => {
            let previous = view
                .as_mut()
                .filter(|view| view.validator.is_some())
                .ok_or_else(|| corrupt("304 has no matching complete reclamation view"))?;
            previous.requested = requested;
        }
    }
    Ok(())
}

/// Locate marker introduction through actual metadata predecessors, never the
/// planning baseline. Once complete, callers keep these facts through future
/// metadata-log truncation and only refresh current authorization/roots.
async fn audit_predecessor(
    view: &FreshView,
    expected_marker: &str,
    proof: &CommitProof,
    requested_removals: &BTreeSet<String>,
    io: &FileIO,
    cache: &mut ScanCache,
) -> Result<(Reachability, Vec<String>), ConnectorError> {
    let uuid = view.base.metadata.uuid();
    let mut current = AuditCursor::from_metadata(&view.base.metadata);
    let mut location = view.base.metadata_location.clone();
    let mut seen = BTreeSet::new();
    loop {
        if !seen.insert(location.clone()) {
            return Err(predecessor("metadata chain contains a cycle"));
        }
        let previous = current
            .previous
            .clone()
            .or_else(|| {
                if proof.metadata_location.as_deref() == Some(location.as_str())
                    && let MetadataPredecessorObservation::Candidate {
                        location,
                        timestamp_ms,
                    } = &proof.metadata_predecessor
                {
                    return Some(crate::iceberg::spec::MetadataLog {
                        metadata_file: location.clone(),
                        timestamp_ms: *timestamp_ms,
                    });
                }
                None
            })
            .ok_or_else(|| {
                predecessor(
                    "actual commit or predecessor is absent from the available metadata chain",
                )
            })?;
        let bytes = io
            .new_input(&previous.metadata_file)
            .map_err(predecessor_read_error)?
            .read()
            .await
            .map_err(predecessor_read_error)?;
        let before: AuditDocument =
            serde_json::from_slice(&bytes).map_err(|e| predecessor(e.to_string()))?;
        let before = before.checked()?;
        drop(bytes);
        if before.cursor.uuid != uuid
            || before.cursor.updated_ms != previous.timestamp_ms
            || before.cursor.updated_ms > current.updated_ms
        {
            return Err(predecessor(
                "metadata predecessor identity or timestamp contradicts its link",
            ));
        }
        if current.marker.as_deref() == Some(expected_marker)
            && before.cursor.marker.as_deref() != Some(expected_marker)
        {
            let mut protection = Reachability::default();
            let mut lost = Vec::new();
            for (name, reference) in &before.cursor.refs {
                if !current.refs.contains_key(name) && !requested_removals.contains(name) {
                    let list = before
                        .snapshots
                        .get(&reference.snapshot_id)
                        .ok_or_else(|| predecessor("lost ref points to an unavailable snapshot"))?;
                    protection.extend(
                        &cache
                            .root(reference.snapshot_id, list, before.cursor.version, io)
                            .await?
                            .live,
                    );
                    if let Some(paths) = before.statistics.get(&reference.snapshot_id) {
                        protection.paths.extend(paths.iter().cloned());
                    }
                    lost.push(format!(
                        "{name}: snapshot={}, manifest-list={list}",
                        reference.snapshot_id
                    ));
                }
            }
            return Ok((protection, lost));
        }
        // Each predecessor is reduced to the exact next cursor. Unrelated
        // properties, snapshot bodies, schemas, specs and history are dropped.
        current = before.cursor;
        location = previous.metadata_file;
    }
}

#[derive(Clone)]
struct AuditCursor {
    uuid: uuid::Uuid,
    updated_ms: i64,
    version: FormatVersion,
    marker: Option<String>,
    refs: HashMap<String, crate::iceberg::spec::SnapshotReference>,
    previous: Option<crate::iceberg::spec::MetadataLog>,
}
impl AuditCursor {
    fn from_metadata(metadata: &TableMetadata) -> Self {
        Self {
            uuid: metadata.uuid(),
            updated_ms: metadata.last_updated_ms(),
            version: metadata.format_version(),
            marker: metadata.properties().get(MARKER).cloned(),
            refs: metadata.refs().clone(),
            previous: metadata.metadata_log().last().cloned(),
        }
    }
}
#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct AuditDocument {
    #[serde(rename = "table-uuid")]
    uuid: uuid::Uuid,
    last_updated_ms: i64,
    format_version: FormatVersion,
    #[serde(default, deserialize_with = "read_marker")]
    properties: Option<String>,
    #[serde(default, deserialize_with = "read_refs")]
    refs: Option<HashMap<String, crate::iceberg::spec::SnapshotReference>>,
    #[serde(default)]
    current_snapshot_id: Option<i64>,
    #[serde(default)]
    snapshots: Vec<AuditSnapshot>,
    #[serde(default)]
    statistics: Vec<AuditStatistics>,
    #[serde(default)]
    partition_statistics: Vec<AuditStatistics>,
    #[serde(default, deserialize_with = "read_last_log")]
    metadata_log: Option<crate::iceberg::spec::MetadataLog>,
}
#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct AuditSnapshot {
    snapshot_id: i64,
    manifest_list: String,
}
#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct AuditStatistics {
    snapshot_id: i64,
    statistics_path: String,
}
struct CheckedAuditDocument {
    cursor: AuditCursor,
    snapshots: BTreeMap<i64, String>,
    statistics: BTreeMap<i64, BTreeSet<String>>,
}
impl AuditDocument {
    fn checked(self) -> Result<CheckedAuditDocument, ConnectorError> {
        let mut snapshots = BTreeMap::new();
        for snapshot in self.snapshots {
            if snapshot.manifest_list.is_empty()
                || snapshots
                    .insert(snapshot.snapshot_id, snapshot.manifest_list)
                    .is_some()
            {
                return Err(predecessor(
                    "historical snapshot roots are empty or duplicate",
                ));
            }
        }
        let refs = self.refs.unwrap_or_else(|| {
            // Iceberg v1/v2 omit refs to express implicit main. This is the
            // standard metadata rule used by the vendor decoder as well.
            self.current_snapshot_id
                .filter(|id| *id != -1)
                .map(|id| {
                    HashMap::from([(
                        "main".into(),
                        crate::iceberg::spec::SnapshotReference::new(
                            id,
                            crate::iceberg::spec::SnapshotRetention::branch(None, None, None),
                        ),
                    )])
                })
                .unwrap_or_default()
        });
        if refs
            .values()
            .any(|reference| !snapshots.contains_key(&reference.snapshot_id))
        {
            return Err(predecessor(
                "historical ref points to an unavailable snapshot root",
            ));
        }
        if self.metadata_log.as_ref().is_some_and(|entry| {
            entry.metadata_file.is_empty() || entry.timestamp_ms > self.last_updated_ms
        }) {
            return Err(predecessor("historical predecessor link is invalid"));
        }
        let mut statistics: BTreeMap<i64, BTreeSet<String>> = BTreeMap::new();
        for file in self.statistics.into_iter().chain(self.partition_statistics) {
            if file.statistics_path.is_empty() {
                return Err(predecessor("historical statistics path is empty"));
            }
            statistics
                .entry(file.snapshot_id)
                .or_default()
                .insert(file.statistics_path);
        }
        Ok(CheckedAuditDocument {
            statistics,
            cursor: AuditCursor {
                uuid: self.uuid,
                updated_ms: self.last_updated_ms,
                version: self.format_version,
                marker: self.properties,
                refs,
                previous: self.metadata_log,
            },
            snapshots,
        })
    }
}
fn read_marker<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    struct Visitor;
    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = Option<String>;
        fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
            Ok(None)
        }
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a metadata property map")
        }
        fn visit_map<M: serde::de::MapAccess<'de>>(
            self,
            mut map: M,
        ) -> std::result::Result<Self::Value, M::Error> {
            let mut marker = None;
            while let Some(key) = map.next_key::<String>()? {
                if key == MARKER {
                    if marker.is_some() {
                        return Err(serde::de::Error::custom("duplicate maintenance marker"));
                    }
                    marker = Some(map.next_value::<String>()?);
                } else {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
            Ok(marker)
        }
    }
    deserializer.deserialize_any(Visitor)
}
fn read_refs<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<HashMap<String, crate::iceberg::spec::SnapshotReference>>, D::Error>
{
    struct Visitor;
    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = Option<HashMap<String, crate::iceberg::spec::SnapshotReference>>;
        fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
            Ok(None)
        }
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a snapshot reference map")
        }
        fn visit_map<M: serde::de::MapAccess<'de>>(
            self,
            mut map: M,
        ) -> std::result::Result<Self::Value, M::Error> {
            let mut refs = HashMap::new();
            while let Some((key, value)) =
                map.next_entry::<String, crate::iceberg::spec::SnapshotReference>()?
            {
                if key.is_empty() || refs.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("empty or duplicate snapshot ref"));
                }
            }
            Ok(Some(refs))
        }
    }
    deserializer.deserialize_any(Visitor)
}
fn read_last_log<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<crate::iceberg::spec::MetadataLog>, D::Error> {
    struct Visitor;
    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = Option<crate::iceberg::spec::MetadataLog>;
        fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
            Ok(None)
        }
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a metadata history sequence")
        }
        fn visit_seq<S: serde::de::SeqAccess<'de>>(
            self,
            mut seq: S,
        ) -> std::result::Result<Self::Value, S::Error> {
            let mut last: Option<crate::iceberg::spec::MetadataLog> = None;
            while let Some(entry) = seq.next_element::<crate::iceberg::spec::MetadataLog>()? {
                if entry.metadata_file.is_empty()
                    || last
                        .as_ref()
                        .is_some_and(|last| last.timestamp_ms > entry.timestamp_ms)
                {
                    return Err(serde::de::Error::custom(
                        "metadata history is empty or nonchronological",
                    ));
                }
                last = Some(entry);
            }
            Ok(last)
        }
    }
    deserializer.deserialize_any(Visitor)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExactObject {
    version: Option<String>,
    etag: Option<String>,
    size: u64,
    mtime_ms: i64,
}
impl ExactObject {
    fn from(metadata: &crate::opendal::Metadata, versioned: bool) -> Result<Self, ConnectorError> {
        let mtime_ms = metadata
            .last_modified()
            .map(|value| value.into_inner().as_millisecond())
            .ok_or_else(|| corrupt("Reclamation object has no reliable modification time"))?;
        Ok(Self {
            version: if versioned {
                metadata.version().map(str::to_owned)
            } else {
                None
            },
            etag: metadata.etag().map(str::to_owned),
            size: metadata.content_length(),
            mtime_ms,
        })
    }
}
fn protect_half_references(candidates: &Reachability, live: &mut Reachability) {
    let live_dv = live.dv.clone();
    for (puffin, data) in candidates.dv.iter().chain(live_dv.iter()) {
        if data.iter().any(|path| live.data.contains(path)) {
            live.paths.insert(puffin.clone());
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ReclamationReport {
    pub candidates: usize,
    pub protected: usize,
    pub deleted: usize,
    pub already_absent: usize,
    pub failed: usize,
    pub pending: usize,
    pub delete_requests: usize,
    pub catalog_reads: usize,
    pub list_reads: usize,
    pub manifest_reads: usize,
    pub puffin_reads: usize,
    pub roots: usize,
    pub audit_complete: bool,
    pub lost_refs: Vec<String>,
    pub failure: Option<ConnectorError>,
}
impl ReclamationReport {
    pub(crate) fn diagnostic_failure(&self) -> Option<ConnectorError> {
        self.failure.as_ref().map(|failure| ConnectorError::new(failure.kind(), format!("{}; reclamation candidates={}, protected={}, deleted={}, already_absent={}, failed={}, pending={}, catalog_reads={}, list_reads={}, manifest_reads={}, puffin_reads={}; lost_refs=[{}]", failure, self.candidates, self.protected, self.deleted, self.already_absent, self.failed, self.pending, self.catalog_reads, self.list_reads, self.manifest_reads, self.puffin_reads, self.lost_refs.join("; "))))
    }
}
pub(crate) async fn reclaim(
    owner: &IcebergMetadataContext,
    target: CatalogTableName,
    retention: &RetentionPlan,
    marker: &str,
    proof: &CommitProof,
    original: &ConnectorRequestContext,
) -> ReclamationReport {
    let mut report = ReclamationReport::default();
    let mut cache = ScanCache::default();
    report.failure = reclaim_round(
        owner,
        target,
        retention,
        marker,
        proof,
        original,
        &mut cache,
        &mut report,
    )
    .await
    .err();
    report.catalog_reads = cache.catalog_reads;
    report.list_reads = cache.list_reads;
    report.manifest_reads = cache.manifest_reads;
    report.puffin_reads = cache.puffin_reads;
    report.roots = cache.roots.len();
    tracing::info!(
        catalog_reads = report.catalog_reads,
        list_reads = report.list_reads,
        manifest_reads = report.manifest_reads,
        puffin_reads = report.puffin_reads,
        roots = report.roots,
        candidates = report.candidates,
        protected = report.protected,
        deleted = report.deleted,
        already_absent = report.already_absent,
        failed = report.failed,
        pending = report.pending,
        audit_complete = report.audit_complete,
        stopped = report.failure.is_some(),
        "Iceberg expiration reclamation exited"
    );
    report
}
async fn reclaim_round(
    owner: &IcebergMetadataContext,
    target: CatalogTableName,
    retention: &RetentionPlan,
    marker: &str,
    proof: &CommitProof,
    original: &ConnectorRequestContext,
    cache: &mut ScanCache,
    report: &mut ReclamationReport,
) -> Result<(), ConnectorError> {
    reclaim_round_using(
        &ReclamationPorts {
            catalog: owner.novarocks_catalog().as_ref(),
            planning_binding: owner.resources().planning_binding(),
            clock: &MonotonicClock,
            storage: &AuthorizedStorage,
        },
        target,
        retention,
        marker,
        proof,
        original,
        cache,
        report,
    )
    .await
}
async fn reclaim_round_using(
    ports: &ReclamationPorts<'_>,
    target: CatalogTableName,
    retention: &RetentionPlan,
    marker: &str,
    proof: &CommitProof,
    original: &ConnectorRequestContext,
    cache: &mut ScanCache,
    report: &mut ReclamationReport,
) -> Result<(), ConnectorError> {
    let catalog = ports.catalog;
    let planning_binding = ports.planning_binding;
    let clock = ports.clock;
    let storage = ports.storage;
    if proof.effect != novarocks_spi::connector::ExternalMutationEffect::Applied {
        return Err(corrupt("Reclamation requires exact Applied publication"));
    }
    let policy = catalog.evidence_policy();
    let grant = policy.begin_reclamation()?;
    // Reclamation gets its own deadline, while statement stop prevents new GC
    // work. An already-issued object request is always joined to actual exit.
    let context = grant
        .for_io(original)?
        .with_additional_stop(original.stop().clone());
    context.check_active()?;
    let binding = planning_binding.for_request(context.clone());
    let io = storage.file_io(&target_location_hint(proof, retention)?, binding.clone());
    let uuid = proof
        .table_uuid
        .as_deref()
        .and_then(|id| uuid::Uuid::parse_str(id).ok())
        .ok_or_else(|| corrupt("Applied expiration proof has no exact table UUID"))?;
    let mut view = None;
    let result = async {
        let mut protected = Reachability::default();
        refresh(
            catalog,
            clock,
            &target,
            &binding,
            &mut view,
            uuid,
            cache,
            &io,
            &mut protected,
        )
        .await?;
        let (lost_protection, lost) = audit_predecessor(
            view.as_ref().unwrap(),
            marker,
            proof,
            &retention.removed_refs,
            &io,
            cache,
        )
        .await?;
        protected.extend(&lost_protection);
        report.audit_complete = true;
        report.lost_refs = lost.clone();
        let mut candidates = Reachability::default();
        for snapshot in &retention.removed_snapshots {
            context.check_active()?;
            candidates.extend(
                &cache
                    .snapshot(snapshot, retention.format_version, &io)
                    .await?
                    .candidates,
            );
        }
        candidates
            .paths
            .extend(retention.removed_statistics_paths.iter().cloned());
        candidates
            .puffin
            .extend(retention.removed_statistics_puffin.iter().cloned());
        for (path, length) in &retention.removed_statistics_lengths {
            if candidates
                .lengths
                .insert(path.clone(), *length)
                .is_some_and(|previous| previous != *length)
            {
                candidates.conflicting_lengths = true;
            }
        }
        report.candidates = candidates.paths.len();
        report.pending = candidates.paths.len();
        // Complete candidate footer facts include blobs absent from live manifests.
        for (path, targets) in cache
            .puffin_associations(&candidates.puffin, &candidates.lengths, &io)
            .await?
        {
            candidates.dv.entry(path).or_default().extend(targets);
        }
        if candidates.conflicting_lengths {
            return Err(corrupt("Candidate paths have contradictory inline lengths"));
        }
        let mut pending = candidates.paths.clone();
        report.candidates = pending.len();
        report.pending = pending.len();
        let admission = catalog.listing_admission();
        // Coverage and protection are monotonic for this one bounded execution.
        while !pending.is_empty() {
            context.check_active()?;
            protect_half_references(&candidates, &mut protected);
            let before = pending.len();
            pending.retain(|path| {
                !protected.paths.contains(path)
                    && !crate::catalog_metadata_root::is_catalog_metadata_root(path)
            });
            report.protected += before - pending.len();
            report.pending = pending.len();
            if pending.is_empty() {
                break;
            }
            if refresh_due(
                view.as_ref().unwrap().requested,
                clock.now(),
                policy.freshness,
            ) {
                refresh(
                    catalog,
                    clock,
                    &target,
                    &binding,
                    &mut view,
                    uuid,
                    cache,
                    &io,
                    &mut protected,
                )
                .await?;
                continue;
            }
            let candidate = pending.first().unwrap().clone();
            let (operator, path) = storage.delete_access(&candidate, &binding)?;
            let versioned = operator.info().capability().delete_with_version;
            let first = admission
                .run_wait_for_exit(&context, async {
                    context.check_active()?;
                    operator.stat(&path).await.map_err(object_error)
                })
                .await;
            let identity = match first {
                Ok(metadata) => {
                    if candidates
                        .lengths
                        .get(&candidate)
                        .is_some_and(|length| *length != metadata.content_length())
                    {
                        return Err(corrupt(
                            "Reclamation candidate length differs from immutable manifest facts",
                        ));
                    }
                    ExactObject::from(&metadata, versioned)?
                }
                Err(error) if error.kind() == ConnectorErrorKind::NotFound => {
                    pending.remove(&candidate);
                    report.already_absent += 1;
                    report.pending = pending.len();
                    continue;
                }
                Err(error) => return Err(error),
            };
            let mut refresh_required = false;
            let requested = view.as_ref().unwrap().requested;
            admission
                .run_wait_for_exit(&context, async {
                    // A second exact stat enforces the existing ETag/size+mtime
                    // contract. Versioned stores additionally delete the exact version.
                    let current = operator.stat(&path).await.map_err(object_error)?;
                    if ExactObject::from(&current, versioned)? != identity {
                        return Err(corrupt("Reclamation candidate object was replaced"));
                    }
                    context.check_active()?;
                    if refresh_due(requested, clock.now(), policy.freshness) {
                        refresh_required = true;
                        return Ok(());
                    }
                    report.delete_requests += 1;
                    let result = match &identity.version {
                        Some(version) => operator.delete_with(&path).version(version).await,
                        None => operator.delete(&path).await,
                    };
                    // This runs before the actual-exit wrapper may return a typed
                    // stop/deadline. An acknowledged delete remains a real fact.
                    record_delete_result(
                        report,
                        result.as_ref().map(|_| ()).map_err(|error| error.kind()),
                    );
                    match result {
                        Ok(()) => Ok(()),
                        Err(error) if error.kind() == crate::opendal::ErrorKind::NotFound => Ok(()),
                        Err(error) => Err(object_error(error)),
                    }
                })
                .await?;
            if refresh_required {
                refresh(
                    catalog,
                    clock,
                    &target,
                    &binding,
                    &mut view,
                    uuid,
                    cache,
                    &io,
                    &mut protected,
                )
                .await?;
                continue;
            }
            pending.remove(&candidate);
            report.pending = pending.len();
        }
        if !lost.is_empty() {
            return Err(corrupt(format!(
                "Expiration Applied with unexpectedly lost refs (files protected): {}",
                lost.join("; ")
            )));
        }
        Ok(())
    }
    .await;
    result.map_err(|error: ConnectorError| {
        if error.kind() == ConnectorErrorKind::DeadlineExceeded
            && !error.message().contains("freshness_exhausted")
            && view.as_ref().is_some_and(|current| refresh_due(current.requested, clock.now(), policy.freshness))
        {
            error.map_message(|message| format!("freshness_exhausted: pending reclamation could not requalify within its unchanged GC grant; {message}"))
        } else {
            error
        }
    })
}
fn record_delete_result(
    report: &mut ReclamationReport,
    result: std::result::Result<(), crate::opendal::ErrorKind>,
) {
    match result {
        Ok(()) => {
            report.deleted += 1;
            report.pending = report.pending.saturating_sub(1);
        }
        Err(crate::opendal::ErrorKind::NotFound) => {
            report.already_absent += 1;
            report.pending = report.pending.saturating_sub(1);
        }
        Err(_) => report.failed += 1,
    }
}

fn refresh_due(requested: Instant, now: Instant, freshness: std::time::Duration) -> bool {
    now.saturating_duration_since(requested) > freshness
}

fn target_location_hint(
    proof: &CommitProof,
    retention: &RetentionPlan,
) -> Result<String, ConnectorError> {
    proof
        .metadata_location
        .as_deref()
        .map(str::to_owned)
        .or_else(|| {
            retention
                .removed_snapshots
                .first()
                .map(|s| s.manifest_list().to_owned())
        })
        .ok_or_else(|| predecessor("Applied response has no metadata location"))
}
fn predecessor(message: impl std::fmt::Display) -> ConnectorError {
    corrupt(format!(
        "predecessor_unavailable: {message}; expiration Applied, no files deleted"
    ))
}
fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}
fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}
fn iceberg_error(error: crate::iceberg::Error) -> ConnectorError {
    use novarocks_spi::connector::ConnectorMutationFailureKind as Failure;
    let failure = crate::commit::attempt::before_dispatch_failure(&error);
    let kind = match failure.kind() {
        Failure::Interrupted => ConnectorErrorKind::Interrupted,
        Failure::Cancelled => ConnectorErrorKind::Cancelled,
        Failure::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
        Failure::PermissionDenied | Failure::Unauthenticated => {
            ConnectorErrorKind::PermissionDenied
        }
        Failure::NotFound => ConnectorErrorKind::NotFound,
        Failure::Unsupported => ConnectorErrorKind::Unsupported,
        Failure::ResourceExhausted => ConnectorErrorKind::ResourceExhausted,
        Failure::CorruptData => ConnectorErrorKind::CorruptData,
        Failure::InvalidRequest | Failure::Conflict | Failure::AlreadyExists => {
            ConnectorErrorKind::InvalidRequest
        }
        Failure::Internal => ConnectorErrorKind::Internal,
        Failure::Unavailable => ConnectorErrorKind::Unavailable,
    };
    ConnectorError::from_domain(kind, failure).map_message(|_| error.to_string())
}
fn predecessor_read_error(error: crate::iceberg::Error) -> ConnectorError {
    let error = iceberg_error(error);
    let kind = match error.kind() {
        ConnectorErrorKind::Cancelled
        | ConnectorErrorKind::DeadlineExceeded
        | ConnectorErrorKind::PermissionDenied => error.kind(),
        _ => ConnectorErrorKind::CorruptData,
    };
    ConnectorError::new(
        kind,
        format!("predecessor_unavailable: {error}; expiration Applied, no files deleted"),
    )
}
fn object_error(error: crate::opendal::Error) -> ConnectorError {
    let kind = match error.kind() {
        crate::opendal::ErrorKind::NotFound => ConnectorErrorKind::NotFound,
        crate::opendal::ErrorKind::PermissionDenied => ConnectorErrorKind::PermissionDenied,
        crate::opendal::ErrorKind::Unsupported => ConnectorErrorKind::Unsupported,
        _ => ConnectorErrorKind::Unavailable,
    };
    ConnectorError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{
        ManifestListWriter, NestedField, Operation, PartitionSpec, PrimitiveType, Schema,
        SortOrder, Summary, TableMetadataBuilder, Type,
    };
    use std::sync::Arc;
    use std::time::Duration;

    pub(super) fn metadata() -> TableMetadata {
        let metadata = TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://gc/table".into(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let mut historical = serde_json::to_value(metadata).unwrap();
        historical["last-updated-ms"] = serde_json::json!(0);
        serde_json::from_value(historical).unwrap()
    }
    async fn snapshot(io: &FileIO, id: i64, path: &str) -> Snapshot {
        let output = io.new_output(path).unwrap().writer().await.unwrap();
        ManifestListWriter::v2(output, id, None, id)
            .close()
            .await
            .unwrap();
        Snapshot::builder()
            .with_snapshot_id(id)
            .with_sequence_number(id)
            .with_timestamp_ms(id * 1000)
            .with_manifest_list(path.to_owned())
            .with_schema_id(0)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
    }
    #[tokio::test]
    async fn complete_roots_include_unreferenced_history_and_cache_survives_rollback() {
        let io = FileIO::new_with_memory();
        let first = snapshot(&io, 1, "memory://gc/old.avro").await;
        let second = snapshot(&io, 2, "memory://gc/new.avro").await;
        let mut metadata = metadata()
            .into_builder(None)
            .add_snapshot(first.clone())
            .unwrap()
            .add_snapshot(second.clone())
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert!(metadata.refs().is_empty());
        let mut cache = ScanCache::default();
        let roots = cache.metadata(&metadata, &io).await.unwrap();
        assert_eq!(
            roots.paths,
            BTreeSet::from([
                first.manifest_list().to_owned(),
                second.manifest_list().to_owned()
            ])
        );
        assert_eq!(cache.list_reads, 2);
        // Reappearing old roots are covered by exact identity, never ID order.
        let complete = metadata.clone();
        metadata = metadata
            .into_builder(None)
            .remove_snapshots(&[2])
            .build()
            .unwrap()
            .metadata;
        cache.metadata(&metadata, &io).await.unwrap();
        assert_eq!(cache.list_reads, 2);
        metadata = complete;
        cache.metadata(&metadata, &io).await.unwrap();
        assert_eq!(cache.list_reads, 2);
        assert_eq!(cache.roots.len(), 2);
    }
    #[tokio::test]
    async fn partial_or_failed_root_never_registers_completed_coverage() {
        let io = FileIO::new_with_memory();
        let good = snapshot(&io, 1, "memory://gc/valid.avro").await;
        let missing = Snapshot::builder()
            .with_snapshot_id(2)
            .with_sequence_number(2)
            .with_timestamp_ms(2000)
            .with_manifest_list("memory://gc/missing.avro")
            .with_schema_id(0)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();
        let mut cache = ScanCache::default();
        cache.snapshot(&good, FormatVersion::V2, &io).await.unwrap();
        assert!(
            cache
                .snapshot(&missing, FormatVersion::V2, &io)
                .await
                .is_err()
        );
        assert_eq!(cache.roots.len(), 1);
        assert!(!cache.lists.contains_key(missing.manifest_list()));
    }
    #[test]
    fn unmatched_304_cannot_create_or_refresh_a_complete_view() {
        let mut view = None;
        assert!(
            accept_complete_load(&mut view, CatalogMetadataLoad::NotModified, Instant::now())
                .is_err()
        );
        accept_complete_load(
            &mut view,
            CatalogMetadataLoad::Complete {
                base: Box::new(CatalogCommitBase {
                    metadata: metadata(),
                    metadata_location: "memory://gc/v1.json".into(),
                }),
                validator: None,
            },
            Instant::now(),
        )
        .unwrap();
        let original = view.as_ref().unwrap().requested;
        assert!(
            accept_complete_load(&mut view, CatalogMetadataLoad::NotModified, Instant::now())
                .is_err()
        );
        assert_eq!(view.as_ref().unwrap().requested, original);
    }
    async fn audit_fixture(io: &FileIO) -> (FreshView, CommitProof, String, String) {
        let before_path = "memory://gc/before.json";
        let actual_path = "memory://gc/actual.json";
        let latest_path = "memory://gc/later.json";
        let root = snapshot(io, 1, "memory://gc/lost-root.avro").await;
        let before = metadata()
            .into_builder(None)
            .add_snapshot(root)
            .unwrap()
            .set_ref(
                "lost",
                crate::iceberg::spec::SnapshotReference::new(
                    1,
                    crate::iceberg::spec::SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let mut before_json = serde_json::to_value(&before).unwrap();
        before_json["statistics"] = serde_json::json!([{"snapshot-id":1,"statistics-path":"memory://gc/lost-stats.puffin","file-size-in-bytes":200,"file-footer-size-in-bytes":100,"blob-metadata":[]}]);
        before_json["partition-statistics"] = serde_json::json!([{"snapshot-id":1,"statistics-path":"memory://gc/lost-partition.parquet","file-size-in-bytes":200}]);
        io.new_output(before_path)
            .unwrap()
            .write(serde_json::to_vec(&before_json).unwrap().into())
            .await
            .unwrap();
        let actual = before
            .clone()
            .into_builder(Some(before_path.into()))
            .remove_snapshots(&[1])
            .set_properties(HashMap::from([(MARKER.into(), "exact-operation".into())]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        io.new_output(actual_path)
            .unwrap()
            .write(serde_json::to_vec(&actual).unwrap().into())
            .await
            .unwrap();
        let latest = actual
            .clone()
            .into_builder(Some(actual_path.into()))
            .set_properties(HashMap::from([("later-property".into(), "later".into())]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let mut proof = CommitProof::applied(None).with_table_uuid(latest.uuid().to_string());
        proof.metadata_location = Some(Arc::from(latest_path));
        proof.metadata_predecessor = MetadataPredecessorObservation::Candidate {
            location: actual_path.into(),
            timestamp_ms: actual.last_updated_ms(),
        };
        (
            FreshView {
                base: CatalogCommitBase {
                    metadata: latest,
                    metadata_location: latest_path.into(),
                },
                validator: None,
                requested: Instant::now(),
            },
            proof,
            before_path.into(),
            "memory://gc/lost-root.avro".into(),
        )
    }
    #[tokio::test]
    async fn later_marker_response_walks_to_actual_introduction_and_protects_lost_ref_files() {
        let io = FileIO::new_with_memory();
        let (view, proof, _, root) = audit_fixture(&io).await;
        let mut cache = ScanCache::default();
        let (protected, lost) = audit_predecessor(
            &view,
            "exact-operation",
            &proof,
            &BTreeSet::new(),
            &io,
            &mut cache,
        )
        .await
        .unwrap();
        assert_eq!(
            protected.paths,
            BTreeSet::from([
                root.clone(),
                "memory://gc/lost-stats.puffin".into(),
                "memory://gc/lost-partition.parquet".into()
            ])
        );
        assert_eq!(
            lost,
            vec![format!("lost: snapshot=1, manifest-list={root}")]
        );
        assert_eq!(cache.list_reads, 1);
        // An explicit removal is not a lost ref and needs no protection scan.
        let (protected, lost) = audit_predecessor(
            &view,
            "exact-operation",
            &proof,
            &BTreeSet::from(["lost".into()]),
            &io,
            &mut cache,
        )
        .await
        .unwrap();
        assert!(protected.paths.is_empty());
        assert!(lost.is_empty());
        assert_eq!(cache.list_reads, 1);
    }
    #[tokio::test]
    async fn exact_response_hint_survives_later_log_truncation_but_missing_actual_predecessor_fails_closed()
     {
        let io = FileIO::new_with_memory();
        let (mut view, proof, before, root) = audit_fixture(&io).await;
        let mut document = serde_json::to_value(&view.base.metadata).unwrap();
        document["metadata-log"] = serde_json::json!([]);
        view.base.metadata = serde_json::from_value(document).unwrap();
        let (protected, _) = audit_predecessor(
            &view,
            "exact-operation",
            &proof,
            &BTreeSet::new(),
            &io,
            &mut ScanCache::default(),
        )
        .await
        .unwrap();
        assert!(protected.paths.contains(&root));
        io.delete(&before).await.unwrap();
        let error = audit_predecessor(
            &view,
            "exact-operation",
            &proof,
            &BTreeSet::new(),
            &io,
            &mut ScanCache::default(),
        )
        .await
        .err()
        .unwrap();
        assert!(error.to_string().contains("predecessor_unavailable"));
    }
    #[test]
    fn minimal_history_parser_keeps_only_last_link_and_rejects_duplicate_roots() {
        let uuid = uuid::Uuid::new_v4();
        let mut document = serde_json::json!({"table-uuid":uuid,"format-version":2,"last-updated-ms":3,"metadata-log":[{"metadata-file":"one","timestamp-ms":1},{"metadata-file":"two","timestamp-ms":2}],"snapshots":[],"refs":{},"schemas":"ignored large unrelated body","properties":{"irrelevant":{"large":"ignored"},MARKER:"exact"}});
        let parsed: AuditDocument = serde_json::from_value(document.clone()).unwrap();
        let checked = parsed.checked().unwrap();
        assert_eq!(checked.cursor.previous.unwrap().metadata_file, "two");
        assert_eq!(checked.cursor.marker.as_deref(), Some("exact"));
        document["snapshots"] = serde_json::json!([{"snapshot-id":7,"manifest-list":"one"},{"snapshot-id":7,"manifest-list":"two"}]);
        assert!(
            serde_json::from_value::<AuditDocument>(document)
                .unwrap()
                .checked()
                .is_err()
        );
    }
    #[tokio::test]
    async fn owned_delete_ack_is_recorded_before_stop_wrapper_returns_and_no_work_detaches() {
        use crate::catalog::listing_admission::ListingAdmission;
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            stop.view(),
            1024,
            4096,
        )
        .unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (exited_tx, mut exited_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut report = ReclamationReport {
                candidates: 2,
                pending: 2,
                ..Default::default()
            };
            let result = ListingAdmission::default()
                .run_wait_for_exit(&context, async {
                    started_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    record_delete_result(&mut report, Ok(()));
                    Ok(())
                })
                .await;
            exited_tx.send(()).unwrap();
            (result, report)
        });
        started_rx.await.unwrap();
        stop.request_stop();
        tokio::task::yield_now().await;
        assert!(matches!(
            exited_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        release_tx.send(()).unwrap();
        let (result, report) = task.await.unwrap();
        assert_eq!(result.unwrap_err().kind(), ConnectorErrorKind::Cancelled);
        assert_eq!(report.deleted, 1);
        assert_eq!(report.pending, 1);
        assert_eq!(report.failed, 0);
    }
    #[test]
    fn early_failure_preserves_lost_ref_locations_and_actual_progress() {
        let report = ReclamationReport {
            candidates: 7,
            deleted: 1,
            pending: 4,
            protected: 2,
            audit_complete: true,
            lost_refs: vec!["lost: snapshot=9, manifest-list=memory://gc/recover.avro".into()],
            failure: Some(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "later object timed out",
            )),
            ..Default::default()
        };
        let diagnostic = report.diagnostic_failure().unwrap();
        assert_eq!(diagnostic.kind(), ConnectorErrorKind::DeadlineExceeded);
        let text = diagnostic.to_string();
        assert!(text.contains("later object timed out"));
        assert!(text.contains("snapshot=9"));
        assert!(text.contains("memory://gc/recover.avro"));
        assert!(text.contains("deleted=1"));
        assert!(text.contains("pending=4"));
    }
    #[test]
    fn reclamation_and_predecessor_read_preserve_typed_stop_deadline_and_permission() {
        for kind in [
            ConnectorErrorKind::Cancelled,
            ConnectorErrorKind::DeadlineExceeded,
            ConnectorErrorKind::PermissionDenied,
        ] {
            let make = || {
                crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::Unexpected,
                    "file read failed",
                )
                .with_source(ConnectorError::new(kind, "typed source"))
            };
            assert_eq!(iceberg_error(make()).kind(), kind);
            let predecessor = predecessor_read_error(make());
            assert_eq!(predecessor.kind(), kind);
            assert!(predecessor.to_string().contains("predecessor_unavailable"));
        }
        assert_eq!(
            object_error(crate::opendal::Error::new(
                crate::opendal::ErrorKind::PermissionDenied,
                "denied"
            ))
            .kind(),
            ConnectorErrorKind::PermissionDenied
        );
    }
    #[test]
    fn candidate_and_live_dv_associations_protect_the_whole_puffin_monotonically() {
        let candidates = Reachability {
            dv: BTreeMap::from([(
                "shared.puffin".into(),
                BTreeSet::from(["live.parquet".into(), "dead.parquet".into()]),
            )]),
            ..Default::default()
        };
        let mut live = Reachability {
            data: BTreeSet::from(["live.parquet".into()]),
            ..Default::default()
        };
        protect_half_references(&candidates, &mut live);
        assert!(live.paths.contains("shared.puffin"));
        live.data.clear();
        protect_half_references(&candidates, &mut live);
        assert!(live.paths.contains("shared.puffin"));
    }
    #[test]
    fn exact_object_identity_includes_version_etag_size_and_mtime() {
        let original = ExactObject {
            version: Some("v1".into()),
            etag: Some("one".into()),
            size: 42,
            mtime_ms: 1000,
        };
        let mut replaced = original.clone();
        replaced.version = Some("v2".into());
        assert_ne!(original, replaced);
        let mut replaced = original.clone();
        replaced.etag = Some("two".into());
        assert_ne!(original, replaced);
        let mut replaced = original.clone();
        replaced.size += 1;
        assert_ne!(original, replaced);
        let mut replaced = original.clone();
        replaced.mtime_ms += 1;
        assert_ne!(original, replaced);
    }
    #[test]
    fn delete_freshness_uses_request_start_and_includes_wait_and_scan_time() {
        let started = Instant::now();
        let window = Duration::from_secs(60);
        assert!(!refresh_due(started, started + window, window));
        assert!(refresh_due(
            started,
            started + window + Duration::from_nanos(1),
            window
        ));
    }
}
