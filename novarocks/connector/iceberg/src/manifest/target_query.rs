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

//! Exact data and position-delete facts for a declared target set.

#![allow(
    dead_code,
    reason = "RWM-6 consumes this scoped facts interface after switching the writer inputs."
)]

use std::collections::{BTreeMap, BTreeSet, HashSet, btree_map::Entry};
use std::num::NonZeroU64;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use novarocks_spi::connector::{
    ConnectorErrorKind, ConnectorExecutionResources, ConnectorRequestContext,
    ConnectorResourceClass, ConnectorResourceReservation,
};

use crate::access_binding::IcebergReadBinding;

use crate::commit::manifest_sources::{LocatedEntry, SnapshotSources, duplicate_identity};
use crate::commit::model::{EntryIdentity, FrozenEntry};
use crate::commit::write_stack::domain::IcebergDeleteTargetLocator;
use crate::delete_semantics::{
    ConstructionFailure, DataFileFact, DataSequenceNumber, DeleteCandidateIndex, DeleteFact,
    DeleteObservation, DeleteSemanticsError, FileMetrics, POSITION_FILE_PATH_FIELD_ID, ReadDomain,
    TypedPartition, validate_partition_binding,
};
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, DataFileFormat, Manifest, ManifestContentType, ManifestFile, Schema,
    StructType, TableMetadata,
};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::read_snapshot::{
    manifest_metrics_for_metadata, mint_read_domain, observed_raw_delete_entry,
};

use super::binding::{BoundEntry, EntrySequences};
use super::partition_pruning::may_contain;
use super::source::{BindingOrigin, ManifestSource};

/// T declares position semantics only. It has no equality completeness claim.
#[derive(Debug)]
pub(crate) struct RangeCompletion {
    pub(crate) domain: Arc<ReadDomain>,
    pub(crate) manifest_list: String,
    pub(crate) targets: BTreeMap<String, TypedPartition>,
    pub(crate) processed: BTreeMap<usize, ManifestFile>,
}

#[derive(Debug)]
pub(crate) struct TargetPositionFact {
    pub(crate) entry: LocatedEntry,
    delete: Arc<DeleteFact>,
    _source: Arc<BoundLookup>,
}

impl TargetPositionFact {
    pub(crate) fn delete(&self) -> &DeleteFact {
        &self.delete
    }
}

impl Deref for TargetPositionFact {
    type Target = LocatedEntry;

    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}

#[derive(Debug)]
pub(crate) struct TargetFacts {
    pub(crate) data: LocatedEntry,
    pub(crate) positions: Vec<TargetPositionFact>,
}

impl TargetFacts {
    pub(crate) fn matches_data_locator(
        &self,
        completion: &RangeCompletion,
        locator: &IcebergDeleteTargetLocator,
    ) -> Result<bool> {
        let descriptor = completion
            .processed
            .get(&self.data.source_index)
            .ok_or_else(|| {
                invalid("Target data entry source is absent from the completed range")
            })?;
        Ok(completion.manifest_list == locator.manifest_list()
            && descriptor.manifest_path == locator.manifest_path()
            && u64::try_from(self.data.source_index).ok() == Some(locator.manifest_ordinal())
            && u64::try_from(self.data.ordinal).ok() == Some(locator.entry_ordinal())
            && completion.domain.endpoint().snapshot_id() == locator.snapshot_id()
            && self.data.frozen.identity().path() == locator.data_file_path()
            && self.data.frozen.facts().record_count == locator.record_count())
    }

    pub(crate) fn data_locator(
        &self,
        completion: &RangeCompletion,
    ) -> Result<IcebergDeleteTargetLocator> {
        let descriptor = completion
            .processed
            .get(&self.data.source_index)
            .ok_or_else(|| {
                invalid("Target data entry source is absent from the completed range")
            })?;
        IcebergDeleteTargetLocator::try_new(
            completion.manifest_list.clone(),
            descriptor.manifest_path.clone(),
            u64::try_from(self.data.source_index)
                .map_err(|_| invalid("Manifest ordinal overflow"))?,
            u64::try_from(self.data.ordinal).map_err(|_| invalid("Entry ordinal overflow"))?,
            completion.domain.endpoint().snapshot_id(),
            self.data.frozen.identity().path().to_owned(),
            self.data.frozen.facts().record_count,
        )
        .map_err(|error| invalid(error.to_string()))
    }
}

#[derive(Debug)]
pub(crate) struct TargetObservation {
    pub(crate) files: BTreeMap<String, TargetFacts>,
    pub(crate) completion: RangeCompletion,
}

#[derive(Clone, Copy, Debug)]
struct ResolvedEntry {
    sequences: EntrySequences,
    first_row_id: Option<i64>,
}

#[derive(Debug)]
pub(crate) struct RawManifest {
    manifest: Manifest,
    _receipt: Option<Mutex<ConnectorResourceReservation>>,
}

impl RawManifest {
    pub(crate) fn manifest(&self) -> &Manifest {
        &self.manifest
    }
}

#[derive(Debug)]
pub(crate) struct BoundLookup {
    raw: Arc<RawManifest>,
    bindings: Vec<ResolvedEntry>,
    data: BTreeMap<String, Vec<usize>>,
    position_entries: BTreeMap<usize, LocatedEntry>,
    positions: DeleteObservation,
    _receipt: Option<Mutex<ConnectorResourceReservation>>,
}

impl BoundLookup {
    pub(crate) fn raw(&self) -> &Manifest {
        &self.raw.manifest
    }

    pub(crate) fn entries<'a>(
        &'a self,
        source: ManifestSource<'a>,
    ) -> impl Iterator<Item = BoundEntry<'a>> + 'a {
        self.bindings
            .iter()
            .enumerate()
            .map(move |(ordinal, bound)| self.entry(source, ordinal, *bound))
    }

    fn entry<'a>(
        &'a self,
        source: ManifestSource<'a>,
        ordinal: usize,
        bound: ResolvedEntry,
    ) -> BoundEntry<'a> {
        BoundEntry {
            source,
            ordinal,
            raw: &self.raw.manifest.entries()[ordinal],
            sequences: bound.sequences,
            first_row_id: bound.first_row_id,
        }
    }
}

type RawManifestCache = Arc<Mutex<BTreeMap<(String, i64), Arc<RawManifest>>>>;

struct Composite {
    index: DeleteCandidateIndex,
    _sources: Vec<Arc<BoundLookup>>,
    _receipt: Option<Mutex<ConnectorResourceReservation>>,
}

/// One authorized immutable endpoint/list and its source interpretation.
/// Full descriptors and committed origin are fixed by the owned list. A
/// different list or interpretation must construct a different scope.
pub(crate) struct ManifestReadScope {
    metadata: Arc<TableMetadata>,
    schema: Arc<Schema>,
    domain: Arc<ReadDomain>,
    manifest_list: String,
    descriptors: Option<BTreeMap<usize, ManifestFile>>,
    raw: RawManifestCache,
    original_raw: Option<RawManifestCache>,
    bound: BTreeMap<usize, Arc<BoundLookup>>,
    composites: BTreeMap<Vec<usize>, Composite>,
    resources: Option<ConnectorExecutionResources>,
    binding: Option<IcebergReadBinding>,
    owned_request: Option<ConnectorRequestContext>,
    owned_peak: u64,
}

impl ManifestReadScope {
    pub(crate) fn new(metadata: Arc<TableMetadata>, snapshot_id: i64) -> Result<Self> {
        let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
            invalid(format!(
                "Target snapshot {snapshot_id} is absent from metadata"
            ))
        })?;
        let schema = snapshot.schema(&metadata)?;
        let domain = mint_read_domain(&metadata, snapshot_id, &schema).map_err(invalid)?;
        let manifest_list = snapshot.manifest_list().to_owned();
        Ok(Self {
            metadata,
            schema,
            domain,
            manifest_list,
            descriptors: None,
            raw: RawManifestCache::default(),
            original_raw: None,
            bound: BTreeMap::new(),
            composites: BTreeMap::new(),
            resources: None,
            binding: None,
            owned_request: None,
            owned_peak: 0,
        })
    }

    pub(crate) fn with_writer_resources(
        mut self,
        resources: ConnectorExecutionResources,
        binding: IcebergReadBinding,
    ) -> Self {
        self.resources = Some(resources);
        self.binding = Some(binding);
        self
    }

    pub(crate) fn with_owned_request(mut self, request: &ConnectorRequestContext) -> Self {
        if request.io_admission().is_some() {
            self.owned_request = Some(request.clone());
        }
        self
    }

    pub(crate) async fn reserve_owned(&mut self, additional: u64) -> Result<()> {
        let Some(request) = &self.owned_request else {
            return Ok(());
        };
        if additional == 0 {
            return Ok(());
        }
        let peak = self
            .owned_peak
            .checked_add(additional)
            .and_then(NonZeroU64::new)
            .ok_or_else(|| invalid("target observation metadata bytes overflow"))?;
        request
            .reserve_owned_metadata(peak)
            .await
            .map_err(storage_failure)?;
        self.owned_peak = peak.get();
        Ok(())
    }

    /// A different list/endpoint gets its own bound facts. The original
    /// operation may still reuse raw objects through the same authorization.
    pub(crate) fn rebind(&self, metadata: Arc<TableMetadata>, snapshot_id: i64) -> Result<Self> {
        let mut scope = Self::new(metadata, snapshot_id)?;
        scope.original_raw = Some(self.raw.clone());
        scope.resources = self.resources.clone();
        scope.binding = self.binding.clone();
        scope.owned_request = self.owned_request.clone();
        scope.owned_peak = self.owned_peak;
        Ok(scope)
    }

    pub(crate) fn evict(&mut self) {
        self.composites.clear();
        self.bound.retain(|_, entry| Arc::strong_count(entry) > 1);
        self.raw
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .retain(|_, raw| Arc::strong_count(raw) > 1);
    }

    /// Only the exact committed source, including its full list descriptor,
    /// can borrow this scope's original bound facts.
    pub(crate) fn cached_source(&self, source: ManifestSource<'_>) -> Option<&BoundLookup> {
        if source.origin
            != (BindingOrigin::Committed {
                snapshot_id: self.domain.endpoint().snapshot_id(),
                list_path: &self.manifest_list,
            })
        {
            return None;
        }
        let ordinal = self
            .descriptors
            .as_ref()?
            .iter()
            .find_map(|(ordinal, descriptor)| {
                (descriptor == source.descriptor).then_some(ordinal)
            })?;
        self.bound.get(ordinal).map(Arc::as_ref)
    }

    /// Borrow an already observed immutable object when a new list or origin
    /// requires rebinding. The original scope keeps the storage interpretation.
    pub(crate) async fn cached_raw(
        &self,
        source: ManifestSource<'_>,
    ) -> Result<Option<Arc<RawManifest>>> {
        self.authorize(&source.descriptor.manifest_path).await?;
        let key = (
            source.descriptor.manifest_path.clone(),
            source.descriptor.manifest_length,
        );
        Ok(self.raw_entry(&key))
    }

    // An attempt can borrow immutable E objects. Objects first loaded in B
    // stay in the round's own cache and release with that round's accounting.
    fn raw_entry(&self, key: &(String, i64)) -> Option<Arc<RawManifest>> {
        let lookup = |cache: &RawManifestCache| {
            cache
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .get(key)
                .cloned()
        };
        lookup(&self.raw).or_else(|| self.original_raw.as_ref().and_then(lookup))
    }

    async fn authorize(&self, path: &str) -> Result<()> {
        if let Some(binding) = &self.binding {
            binding
                .resolve_access_for_request_async(path)
                .await
                .map_err(storage_failure)?;
        }
        Ok(())
    }

    async fn read_raw(
        &mut self,
        source: ManifestSource<'_>,
        io: &FileIO,
        active: &(impl Fn() -> Result<()> + Sync),
    ) -> Result<Arc<RawManifest>> {
        let key = (
            source.descriptor.manifest_path.clone(),
            source.descriptor.manifest_length,
        );
        if let Some(raw) = self.raw_entry(&key) {
            return Ok(raw);
        }
        let encoded_bytes = u64::try_from(source.descriptor.manifest_length)
            .ok()
            .filter(|length| *length != 0)
            .ok_or_else(|| invalid("Manifest descriptor has no valid immutable object length"))?;
        self.reserve_owned(encoded_bytes).await?;
        let mut receipt = self.reserve_cache(encoded_bytes)?;
        let manifest = source.load_sized(io).await?;
        active()?;
        let actual = raw_manifest_bytes(&manifest, source.descriptor);
        self.reserve_owned(actual.saturating_sub(encoded_bytes))
            .await?;
        self.grow_cache(&mut receipt, actual)?;
        let raw = Arc::new(RawManifest {
            manifest,
            _receipt: receipt,
        });
        self.raw
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(key, raw.clone());
        Ok(raw)
    }

    fn reserve_cache(&mut self, bytes: u64) -> Result<Option<Mutex<ConnectorResourceReservation>>> {
        let Some(resources) = self.resources.clone() else {
            return Ok(None);
        };
        let reserve = || resources.try_reserve(ConnectorResourceClass::WriterState, bytes);
        let receipt = match reserve() {
            Err(error) if error.kind() == ConnectorErrorKind::ResourceExhausted => {
                self.evict();
                reserve()
            }
            result => result,
        }
        .map_err(storage_failure)?;
        Ok(Some(Mutex::new(receipt)))
    }

    fn grow_cache(
        &mut self,
        receipt: &mut Option<Mutex<ConnectorResourceReservation>>,
        bytes: u64,
    ) -> Result<()> {
        let Some(receipt) = receipt else {
            return Ok(());
        };
        let receipt = receipt
            .get_mut()
            .unwrap_or_else(|poison| poison.into_inner());
        let additional = bytes.saturating_sub(receipt.bytes());
        match receipt.try_grow(additional) {
            Err(error) if error.kind() == ConnectorErrorKind::ResourceExhausted => {
                self.evict();
                receipt.try_grow(additional)
            }
            result => result,
        }
        .map_err(storage_failure)
    }

    pub(crate) fn snapshot_sources(&self) -> Result<SnapshotSources<'_>> {
        let snapshot = self
            .metadata
            .snapshot_by_id(self.domain.endpoint().snapshot_id())
            .ok_or_else(|| invalid("Fixed target snapshot disappeared from pinned metadata"))?;
        let descriptors = self
            .descriptors
            .as_ref()
            .ok_or_else(|| invalid("Fixed target scope has no completed manifest list"))?;
        Ok(SnapshotSources::from_indexed_descriptors(
            snapshot,
            descriptors
                .iter()
                .map(|(&ordinal, descriptor)| (ordinal, descriptor.clone())),
        ))
    }

    pub(crate) fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn domain(&self) -> &Arc<ReadDomain> {
        &self.domain
    }

    pub(crate) async fn observe(
        &mut self,
        targets: BTreeMap<String, TypedPartition>,
        io: &FileIO,
        active: impl Fn() -> Result<()> + Sync,
    ) -> Result<TargetObservation> {
        self.observe_inner(targets, io, active, true).await
    }

    /// A preparation round treats a disappeared target as a conflict only after
    /// its bounded observation completed. Storage and malformed-data failures
    /// still leave this method as their original errors.
    pub(crate) async fn observe_present(
        &mut self,
        targets: BTreeMap<String, TypedPartition>,
        io: &FileIO,
        active: impl Fn() -> Result<()> + Sync,
    ) -> Result<TargetObservation> {
        self.observe_inner(targets, io, active, false).await
    }

    pub(crate) fn observed_data_file(
        &self,
        physical: &LocatedEntry,
    ) -> Result<&crate::iceberg::spec::DataFile> {
        let entry = self
            .bound
            .get(&physical.source_index)
            .and_then(|bound| bound.raw().entries().get(physical.ordinal))
            .ok_or_else(|| invalid("Completed target lost its original bound data entry"))?;
        Ok(entry.data_file())
    }

    /// A commit removes live physical entries even when a deletion vector
    /// supersedes their row-read effect. This borrows only already bound sources.
    pub(crate) fn live_position_entries(&self) -> impl Iterator<Item = &LocatedEntry> {
        self.bound
            .values()
            .flat_map(|source| source.position_entries.values())
    }

    /// Locate exact live entries for deletion using their own frozen partition
    /// facts. This physical lookup neither changes nor consults position read
    /// precedence, so it can locate an equality delete or a shadowed position
    /// delete by its own identity.
    pub(crate) async fn locate_live_removals(
        &mut self,
        required: &BTreeMap<&EntryIdentity, &FrozenEntry>,
        io: &FileIO,
        active: &(impl Fn() -> Result<()> + Sync),
    ) -> Result<BTreeMap<EntryIdentity, LocatedEntry>> {
        self.locate_removals(required, false, io, active).await
    }

    /// The supplied data entries were already located on this snapshot.
    pub(crate) async fn locate_live_dangling_dvs(
        &mut self,
        removed_data: &BTreeMap<&EntryIdentity, &FrozenEntry>,
        io: &FileIO,
        active: &(impl Fn() -> Result<()> + Sync),
    ) -> Result<BTreeMap<EntryIdentity, LocatedEntry>> {
        self.locate_removals(removed_data, true, io, active).await
    }

    async fn locate_removals(
        &mut self,
        required: &BTreeMap<&EntryIdentity, &FrozenEntry>,
        dangling_only: bool,
        io: &FileIO,
        active: &(impl Fn() -> Result<()> + Sync),
    ) -> Result<BTreeMap<EntryIdentity, LocatedEntry>> {
        active()?;
        let domain = self.domain.clone();
        let endpoint = domain.endpoint();
        let metadata = self.metadata.clone();
        let list_path = self.manifest_list.clone();
        let mut by_spec = BTreeMap::<i32, (StructType, HashSet<(TypedPartition, bool)>)>::new();
        let mut required_paths = BTreeSet::new();
        let mut dangling_data_paths = BTreeSet::new();
        for (&identity, entry) in required {
            active()?;
            let facts = entry.facts();
            let Some(spec) = metadata.partition_spec_by_id(facts.partition_spec_id) else {
                // A required entry cannot be present under a retired spec.
                // The commit owner will project it as a missing entry.
                continue;
            };
            let data = matches!(identity, EntryIdentity::DataFile { .. });
            let (partition_type, partitions) = match by_spec.entry(facts.partition_spec_id) {
                Entry::Occupied(slot) => slot.into_mut(),
                Entry::Vacant(slot) => slot.insert((
                    endpoint
                        .partition_type(facts.partition_spec_id)
                        .map_err(delete_error)?,
                    HashSet::new(),
                )),
            };
            let partition = TypedPartition::bind_type(spec, partition_type, &facts.partition)
                .map_err(delete_error)?;
            partitions.insert((partition, data));
            if !dangling_only {
                required_paths.insert(identity.path());
            }
            if data {
                dangling_data_paths.insert(identity.path());
            }
        }
        let mut selected = Vec::new();
        for (&ordinal, descriptor) in self.load_descriptors(io, active).await? {
            active()?;
            let spec_id = descriptor.partition_spec_id;
            let Some((_, partitions)) = by_spec.get(&spec_id) else {
                continue;
            };
            let spec = metadata
                .partition_spec_by_id(spec_id)
                .ok_or_else(|| invalid(format!("Unknown target partition spec {spec_id}")))?;
            let data = descriptor.content == ManifestContentType::Data;
            if dangling_only && data {
                continue;
            }
            if partitions.iter().any(|(partition, data_requested)| {
                (!data || *data_requested) && may_contain(descriptor, spec, partition)
            }) {
                selected.push((ordinal, descriptor.clone()));
            }
        }
        let mut located = BTreeMap::new();
        for (source_index, descriptor) in &selected {
            active()?;
            let source = ManifestSource::committed(endpoint.snapshot_id(), &list_path, descriptor);
            let index = self.bound_source(*source_index, source, io, active).await?;
            for entry in index.entries(source) {
                active()?;
                if !entry.raw.is_alive() {
                    continue;
                }
                let file = entry.raw.data_file();
                let may_dangle = file.content_type() == DataContentType::PositionDeletes
                    && file.file_format() == DataFileFormat::Puffin
                    && file
                        .referenced_data_file()
                        .is_some_and(|target| dangling_data_paths.contains(target.as_str()));
                if !required_paths.contains(file.file_path()) && !may_dangle {
                    continue;
                }
                let entry = LocatedEntry::from_bound(*source_index, &entry)?;
                let identity = entry.frozen.identity();
                if required.contains_key(identity) || may_dangle {
                    match located.entry(identity.clone()) {
                        Entry::Vacant(slot) => {
                            slot.insert(entry);
                        }
                        Entry::Occupied(slot) => {
                            return Err(duplicate_identity(slot.key()));
                        }
                    }
                }
            }
        }
        active()?;
        Ok(located)
    }

    async fn load_descriptors(
        &mut self,
        io: &FileIO,
        active: &(impl Fn() -> Result<()> + Sync),
    ) -> Result<&BTreeMap<usize, ManifestFile>> {
        self.authorize(&self.manifest_list).await?;
        if self.descriptors.is_none() {
            let encoded_size = if self.owned_request.is_some() {
                let bytes = io.new_input(&self.manifest_list)?.metadata().await?.size;
                self.reserve_owned(bytes).await?;
                bytes
            } else {
                0
            };
            let snapshot = self
                .metadata
                .snapshot_by_id(self.domain.endpoint().snapshot_id())
                .ok_or_else(|| invalid("Fixed target snapshot disappeared from pinned metadata"))?;
            let sources = SnapshotSources::load(&self.metadata, snapshot, io, active).await?;
            let descriptors = sources.into_indexed_descriptors();
            if self.owned_request.is_some() {
                let bytes = descriptor_bytes(descriptors.values())?;
                self.reserve_owned(bytes.saturating_sub(encoded_size))
                    .await?;
            }
            self.descriptors = Some(descriptors);
        }
        self.descriptors
            .as_ref()
            .ok_or_else(|| invalid("Manifest list not loaded"))
    }

    async fn bound_source(
        &mut self,
        source_index: usize,
        source: ManifestSource<'_>,
        io: &FileIO,
        active: &(impl Fn() -> Result<()> + Sync),
    ) -> Result<Arc<BoundLookup>> {
        self.authorize(&source.descriptor.manifest_path).await?;
        if let Some(index) = self.bound.get(&source_index) {
            return Ok(index.clone());
        }
        let spec_id = source.descriptor.partition_spec_id;
        let metadata = self.metadata.clone();
        let spec = metadata
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| invalid(format!("Unknown target partition spec {spec_id}")))?;
        let partition_type = self
            .domain
            .endpoint()
            .partition_type(spec_id)
            .map_err(delete_error)?;
        let raw = self.read_raw(source, io, active).await?;
        let minimum = (std::mem::size_of::<BoundLookup>() as u64).saturating_add(
            raw.manifest.entries().len() as u64 * std::mem::size_of::<ResolvedEntry>() as u64,
        );
        self.reserve_owned(minimum).await?;
        let mut receipt = self.reserve_cache(minimum)?;
        let mut index = bind_source(
            source_index,
            source,
            &self.schema,
            spec,
            &partition_type,
            raw,
            active,
        )?;
        let actual = bound_lookup_bytes(&index);
        self.reserve_owned(actual.saturating_sub(minimum)).await?;
        self.grow_cache(&mut receipt, actual)?;
        index._receipt = receipt;
        let index = Arc::new(index);
        self.bound.insert(source_index, index.clone());
        Ok(index)
    }

    async fn observe_inner(
        &mut self,
        targets: BTreeMap<String, TypedPartition>,
        io: &FileIO,
        active: impl Fn() -> Result<()> + Sync,
        require_all: bool,
    ) -> Result<TargetObservation> {
        active()?;
        let domain = self.domain.clone();
        let endpoint = domain.endpoint();
        let partition_bindings = endpoint.bind_partition_specs().map_err(delete_error)?;
        let mut partitions_by_spec: BTreeMap<i32, Vec<&TypedPartition>> = BTreeMap::new();
        for partition in targets.values() {
            active()?;
            validate_partition_binding(&partition_bindings, partition).map_err(delete_error)?;
            partitions_by_spec
                .entry(partition.spec_id())
                .or_default()
                .push(partition);
        }
        let mut processed = BTreeMap::new();
        let mut delete_sources = BTreeMap::new();
        let mut selected_deletes = Vec::new();
        let mut data = BTreeMap::new();
        let mut selected_bound = BTreeMap::new();
        if !targets.is_empty() {
            let metadata = self.metadata.clone();
            let manifest_list = self.manifest_list.clone();
            let descriptors = self.load_descriptors(io, &active).await?;
            for (&ordinal, descriptor) in descriptors {
                active()?;
                let spec_id = descriptor.partition_spec_id;
                let Some(partitions) = partitions_by_spec.get(&spec_id) else {
                    continue;
                };
                let spec = metadata
                    .partition_spec_by_id(spec_id)
                    .ok_or_else(|| invalid(format!("Unknown target partition spec {spec_id}")))?;
                if partitions
                    .iter()
                    .any(|partition| may_contain(descriptor, spec, partition))
                {
                    processed.insert(ordinal, descriptor.clone());
                }
            }
            for (&source_index, descriptor) in &processed {
                active()?;
                let spec_id = descriptor.partition_spec_id;
                let spec = metadata
                    .partition_spec_by_id(spec_id)
                    .ok_or_else(|| invalid(format!("Unknown target partition spec {spec_id}")))?;
                let source =
                    ManifestSource::committed(endpoint.snapshot_id(), &manifest_list, descriptor);
                let partition_type = endpoint.partition_type(spec_id).map_err(delete_error)?;
                let index = self.bound_source(source_index, source, io, &active).await?;
                match descriptor.content {
                    ManifestContentType::Data => {
                        for (path, expected) in &targets {
                            active()?;
                            let Some(matches) = index.data.get(path) else {
                                continue;
                            };
                            for &ordinal in matches {
                                let entry = index.entry(source, ordinal, index.bindings[ordinal]);
                                let file = entry.raw.data_file();
                                let partition = TypedPartition::bind_type(
                                    spec,
                                    &partition_type,
                                    file.partition(),
                                )
                                .map_err(delete_error)?;
                                if &partition != expected {
                                    return Err(invalid(format!(
                                        "Target data file {path} differs from its pinned typed partition"
                                    )));
                                }
                                let located = LocatedEntry::from_bound(source_index, &entry)?;
                                if data.contains_key(path) {
                                    return Err(duplicate_identity(located.frozen.identity()));
                                }
                                let sequence = entry.sequences.data_sequence.ok_or_else(|| {
                                    invalid("Target data file has no resolved data sequence")
                                })?;
                                let fact = DataFileFact::try_new(
                                    path.as_str(),
                                    DataSequenceNumber::try_new(sequence).map_err(delete_error)?,
                                    partition,
                                    file.record_count(),
                                    FileMetrics::default(),
                                )
                                .map_err(delete_error)?;
                                data.insert(path.clone(), (fact, located));
                            }
                        }
                    }
                    ManifestContentType::Deletes => {
                        selected_deletes.push(source_index);
                        delete_sources.insert(descriptor.manifest_path.as_str(), source_index);
                        selected_bound.insert(source_index, index);
                    }
                }
            }
        }
        if !self.composites.contains_key(&selected_deletes) {
            let minimum = std::mem::size_of::<Composite>() as u64;
            self.reserve_owned(minimum).await?;
            let mut receipt = self.reserve_cache(minimum)?;
            let facts = selected_bound
                .values()
                .flat_map(|lookup| lookup.positions.facts().iter().cloned());
            let index = DeleteCandidateIndex::from_observed_facts_checked(
                self.domain.clone(),
                facts,
                &active,
            )
            .map_err(construction_error)?;
            let actual = composite_bytes(&index, selected_deletes.len());
            self.reserve_owned(actual.saturating_sub(minimum)).await?;
            self.grow_cache(&mut receipt, actual)?;
            self.composites.insert(
                selected_deletes.clone(),
                Composite {
                    index,
                    _sources: selected_bound.values().cloned().collect(),
                    _receipt: receipt,
                },
            );
        }
        let index = &self.composites[&selected_deletes].index;
        let mut files = BTreeMap::new();
        for path in targets.keys() {
            active()?;
            let Some((fact, data)) = data.remove(path) else {
                if require_all {
                    return Err(invalid(format!(
                        "Target data file {path} is absent from the declared snapshot range"
                    )));
                }
                continue;
            };
            let set = index.for_data(&fact).map_err(delete_error)?;
            let positions = set
                .members()
                .map(|delete| {
                    let provenance = delete
                        .provenance()
                        .ok_or_else(|| invalid("Observed position delete has no source path"))?;
                    let source_index = delete_sources
                        .get(provenance.manifest_path.as_ref())
                        .ok_or_else(|| {
                            invalid("Observed position delete source was not processed")
                        })?;
                    let ordinal = delete.physical_ordinal().ok_or_else(|| {
                        invalid("Observed position delete has no physical entry ordinal")
                    })?;
                    let source = &selected_bound[source_index];
                    let entry =
                        source
                            .position_entries
                            .get(&ordinal)
                            .cloned()
                            .ok_or_else(|| {
                                invalid("Observed position delete has no frozen source entry")
                            })?;
                    Ok(TargetPositionFact {
                        entry,
                        delete: Arc::clone(delete),
                        _source: source.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            files.insert(path.clone(), TargetFacts { data, positions });
        }
        active()?;
        Ok(TargetObservation {
            files,
            completion: RangeCompletion {
                domain: self.domain.clone(),
                manifest_list: self.manifest_list.clone(),
                targets,
                processed,
            },
        })
    }
}

fn bind_source(
    source_index: usize,
    source: ManifestSource<'_>,
    schema: &Schema,
    spec: &crate::iceberg::spec::PartitionSpec,
    partition_type: &crate::iceberg::spec::StructType,
    raw: Arc<RawManifest>,
    active: &impl Fn() -> Result<()>,
) -> Result<BoundLookup> {
    active()?;
    let manifest = &raw.manifest;
    if manifest.metadata().partition_spec != *spec {
        return Err(invalid(format!(
            "Manifest {} partition spec differs from pinned metadata",
            source.descriptor.manifest_path,
        )));
    }
    let mut data: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut raw_positions = Vec::new();
    let mut position_entries = BTreeMap::new();
    let mut bindings = Vec::with_capacity(manifest.entries().len());
    for entry in source.bind(manifest)? {
        active()?;
        let entry = entry?;
        bindings.push(ResolvedEntry {
            sequences: entry.sequences,
            first_row_id: entry.first_row_id,
        });
        if !entry.raw.is_alive() {
            continue;
        }
        let file = entry.raw.data_file();
        match (source.descriptor.content, file.content_type()) {
            (ManifestContentType::Data, DataContentType::Data) => {
                if let Some(ordinals) = data.get_mut(file.file_path()) {
                    ordinals.push(entry.ordinal);
                } else {
                    data.insert(file.file_path().to_owned(), vec![entry.ordinal]);
                }
            }
            (ManifestContentType::Deletes, DataContentType::PositionDeletes) => {
                let partition = TypedPartition::bind_type(spec, partition_type, file.partition())
                    .map_err(delete_error)?;
                let metrics = manifest_metrics_for_metadata(file, schema, active)?;
                let path_metrics = metrics
                    .fields()
                    .get(&POSITION_FILE_PATH_FIELD_ID)
                    .map(|field| (POSITION_FILE_PATH_FIELD_ID, field.clone()))
                    .into_iter()
                    .collect();
                let delete = observed_raw_delete_entry(
                    &entry,
                    manifest.metadata().format_version,
                    schema,
                    partition,
                    FileMetrics::new(path_metrics),
                )
                .map_err(invalid)?;
                raw_positions.push((Some(entry.ordinal), delete));
                position_entries.insert(
                    entry.ordinal,
                    LocatedEntry::from_bound(source_index, &entry)?,
                );
            }
            (ManifestContentType::Deletes, DataContentType::EqualityDeletes) => {}
            _ => {
                return Err(invalid(format!(
                    "Manifest {} contains an entry of the wrong content type",
                    source.descriptor.manifest_path,
                )));
            }
        }
    }
    let path = Arc::from(source.descriptor.manifest_path.as_str());
    let positions = DeleteObservation::from_source_entries_checked([(path, raw_positions)], active)
        .map_err(construction_error)?;
    Ok(BoundLookup {
        raw,
        bindings,
        data,
        position_entries,
        positions,
        _receipt: None,
    })
}

pub(crate) fn descriptor_bytes<'a>(
    mut files: impl Iterator<Item = &'a ManifestFile>,
) -> Result<u64> {
    use crate::iceberg::spec::FieldSummary;
    let overflow = || invalid("target manifest-list descriptor backing overflow");
    files.try_fold(0u64, |total, file| {
        let mut bytes = (std::mem::size_of::<ManifestFile>() as u64)
            .checked_add(file.manifest_path.capacity() as u64)
            .and_then(|bytes| {
                bytes.checked_add(file.key_metadata.as_ref().map_or(0, Vec::capacity) as u64)
            })
            .ok_or_else(overflow)?;
        if let Some(partitions) = &file.partitions {
            let backing = partitions
                .capacity()
                .checked_mul(std::mem::size_of::<FieldSummary>())
                .ok_or_else(overflow)?;
            bytes = bytes.checked_add(backing as u64).ok_or_else(overflow)?;
            for partition in partitions {
                for bound in [&partition.lower_bound, &partition.upper_bound]
                    .into_iter()
                    .flatten()
                {
                    bytes = bytes
                        .checked_add(bound.capacity() as u64)
                        .ok_or_else(overflow)?;
                }
            }
        }
        total.checked_add(bytes).ok_or_else(overflow)
    })
}

pub(crate) fn raw_manifest_bytes(raw: &Manifest, descriptor: &ManifestFile) -> u64 {
    use crate::iceberg::spec::ManifestEntry;
    let backing =
        raw.entries()
            .iter()
            .fold(std::mem::size_of::<RawManifest>() as u64, |bytes, entry| {
                let file = entry.data_file();
                let integer_maps = [
                    file.column_sizes(),
                    file.value_counts(),
                    file.null_value_counts(),
                    file.nan_value_counts(),
                ]
                .into_iter()
                .map(|map| (map.capacity() * std::mem::size_of::<(i32, u64)>()) as u64)
                .fold(0, u64::saturating_add);
                let bounds = [file.lower_bounds(), file.upper_bounds()]
                    .into_iter()
                    .map(|map| {
                        let elements = (map.capacity()
                            * std::mem::size_of::<(i32, crate::iceberg::spec::Datum)>())
                            as u64;
                        map.values().fold(elements, |sum, datum| {
                            let backing = match datum.literal() {
                                crate::iceberg::spec::PrimitiveLiteral::String(value) => {
                                    value.capacity()
                                }
                                crate::iceberg::spec::PrimitiveLiteral::Binary(value) => {
                                    value.capacity()
                                }
                                _ => 0,
                            };
                            sum.saturating_add(backing as u64)
                        })
                    })
                    .fold(0, u64::saturating_add);
                bytes
                    .saturating_add(std::mem::size_of::<ManifestEntry>() as u64)
                    .saturating_add(std::mem::size_of::<Arc<ManifestEntry>>() as u64)
                    .saturating_add(file.file_path().len() as u64)
                    .saturating_add(file.referenced_data_file().map_or(0, |path| path.len()) as u64)
                    .saturating_add(file.key_metadata().map_or(0, |key| key.len()) as u64)
                    .saturating_add(integer_maps)
                    .saturating_add(bounds)
            });
    // The SDK retains its parsed header and partition tuples in addition to
    // the measured entry/index geometry.
    backing.saturating_add(u64::try_from(descriptor.manifest_length).unwrap_or(0))
}

fn bound_lookup_bytes(lookup: &BoundLookup) -> u64 {
    let bindings = lookup.bindings.capacity() as u64 * std::mem::size_of::<ResolvedEntry>() as u64;
    let data = lookup.data.iter().fold(0u64, |bytes, (path, ordinals)| {
        bytes
            .saturating_add(path.capacity() as u64)
            .saturating_add(std::mem::size_of::<(String, Vec<usize>)>() as u64)
            .saturating_add(ordinals.capacity() as u64 * std::mem::size_of::<usize>() as u64)
    });
    let positions = lookup.positions.facts().iter().fold(0u64, |bytes, fact| {
        bytes
            .saturating_add(std::mem::size_of::<(DeleteFact, LocatedEntry)>() as u64)
            .saturating_add(fact.descriptor_variable_bytes() as u64)
    });
    (std::mem::size_of::<BoundLookup>() as u64)
        .saturating_add(bindings)
        .saturating_add(data)
        .saturating_add(positions)
}

fn composite_bytes(index: &DeleteCandidateIndex, sources: usize) -> u64 {
    let size = index.size();
    (std::mem::size_of::<Composite>() as u64)
        .saturating_add(sources as u64 * std::mem::size_of::<Arc<BoundLookup>>() as u64)
        .saturating_add(
            size.member_references as u64 * std::mem::size_of::<(Arc<DeleteFact>, usize)>() as u64,
        )
        .saturating_add(
            size.buckets as u64
                * std::mem::size_of::<crate::delete_semantics::FrozenBucket>() as u64,
        )
}

fn storage_failure(cause: novarocks_spi::connector::ConnectorError) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Iceberg target scope storage or memory access failed",
    )
    .with_source(cause)
}

/// Single-observation compatibility for callers that do not reuse a scope.
pub(crate) async fn observe_targets(
    metadata: &TableMetadata,
    snapshot_id: i64,
    targets: BTreeMap<String, TypedPartition>,
    io: &FileIO,
    active: impl Fn() -> Result<()> + Sync,
) -> Result<TargetObservation> {
    ManifestReadScope::new(Arc::new(metadata.clone()), snapshot_id)?
        .observe(targets, io, active)
        .await
}

fn delete_error(error: DeleteSemanticsError) -> Error {
    invalid(error.message)
}

fn construction_error(error: ConstructionFailure<Error>) -> Error {
    match error {
        ConstructionFailure::Original(error) => error,
        ConstructionFailure::Semantic(error) => delete_error(error),
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}
#[cfg(test)]
mod tests;
