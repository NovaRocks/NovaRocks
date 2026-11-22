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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::commit::base_removal::{
    locate_on_base, manifest_removal_location, missing_required_file,
};
use crate::commit::helpers::{snapshot_total_records, target_ref_snapshot_id};
use crate::commit::manifest_sources::{LocatedEntry, SnapshotSources};
use crate::commit::model::{ArtifactWriter, EntryIdentity, FrozenEntry, OperationIntent};
use crate::commit::operation::IcebergCommitOperation;
use crate::commit::row_delta_dv_metadata::{
    logical_file_size, write_added_entry_groups_with_partitions,
};
use crate::commit::staging::{PreparedChange, Preparer, StagedView};
use crate::commit::write_stack::domain::{
    IcebergArtifactPartition, IcebergDeleteTargetLocator, IcebergFinalDeleteEffect,
    IcebergFinalDeleteKind,
};
use crate::commit::write_stack::receiving::ReceivedCompactFiles;
use crate::commit::write_stack::receiving::compact::{
    AddedCounters, CompactDeleteKind, PhysicalAdditionCounts,
};
use crate::delete_semantics::{DeleteKind, TypedPartition};
use crate::iceberg::spec::{DataContentType, DataFile, Operation, TableMetadata};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::target_query::{ManifestReadScope, TargetFacts, TargetObservation};
use crate::write_descriptor::decode_partition_descriptor_with_type;

pub(in crate::commit::write_stack) struct MaterializedDeleteStep {
    targets: BTreeMap<String, Target>,
    observed: tokio::sync::OnceCell<CompletedStep>,
    compact: Option<CompactStage>,
}

struct CompactStage {
    operation: IcebergCommitOperation,
    stage: usize,
    counts: AddedCounters,
}

struct ObservedTarget<'a> {
    path: &'a str,
    partition: &'a IcebergArtifactPartition,
    effect: Option<EffectRef<'a>>,
}

struct EffectRef<'a> {
    locator: &'a IcebergDeleteTargetLocator,
    old_references: &'a [EntryIdentity],
    removes_data: bool,
}

impl<'a> EffectRef<'a> {
    fn from_whole(effect: &'a IcebergFinalDeleteEffect) -> Self {
        let (old_references, removes_data) = match effect.kind() {
            IcebergFinalDeleteKind::RemoveDataFile {
                old_exclusive_deletes,
                ..
            } => (old_exclusive_deletes.as_ref(), true),
            IcebergFinalDeleteKind::ReplacePositions(vector) => {
                (vector.merged_old_references(), false)
            }
        };
        Self {
            locator: effect.target(),
            old_references,
            removes_data,
        }
    }
}

pub(super) struct Target {
    pub(super) partition: IcebergArtifactPartition,
    pub(super) final_effect: Option<IcebergFinalDeleteEffect>,
}

struct CompletedStep {
    scope: ManifestReadScope,
    targets: TargetObservation,
    removed: Vec<LocatedEntry>,
}

impl MaterializedDeleteStep {
    pub(super) fn new(targets: BTreeMap<String, Target>) -> Self {
        Self {
            targets,
            observed: tokio::sync::OnceCell::new(),
            compact: None,
        }
    }

    pub(in crate::commit::write_stack) fn compact(
        operation: IcebergCommitOperation,
        stage: usize,
        counts: AddedCounters,
    ) -> Self {
        Self {
            targets: BTreeMap::new(),
            observed: tokio::sync::OnceCell::new(),
            compact: Some(CompactStage {
                operation,
                stage,
                counts,
            }),
        }
    }

    fn observed_targets<'a>(
        &'a self,
        compact: Option<&'a ReceivedCompactFiles>,
    ) -> impl Iterator<Item = ObservedTarget<'a>> {
        self.targets
            .iter()
            .map(|(path, target)| ObservedTarget {
                path,
                partition: &target.partition,
                effect: target.final_effect.as_ref().map(EffectRef::from_whole),
            })
            .chain(compact.into_iter().flat_map(|files| {
                files
                    .v2_targets
                    .iter()
                    .map(|(path, partition)| ObservedTarget {
                        path,
                        partition,
                        effect: None,
                    })
                    .chain(files.entries().filter_map(|entry| {
                        let effect = entry.delete_effect.as_ref()?;
                        let (partition, old_references, removes_data) = match &effect.kind {
                            CompactDeleteKind::RemoveDataFile {
                                partition,
                                old_exclusive_deletes,
                            } => (partition, old_exclusive_deletes.as_ref(), true),
                            CompactDeleteKind::ReplacePositions {
                                partition,
                                merged_old_references,
                            } => (partition.as_ref(), merged_old_references.as_slice(), false),
                        };
                        Some(ObservedTarget {
                            path: effect.target.data_file_path(),
                            partition,
                            effect: Some(EffectRef {
                                locator: &effect.target,
                                old_references,
                                removes_data,
                            }),
                        })
                    }))
            }))
    }

    pub(in crate::commit::write_stack) async fn observe(
        &self,
        source: Arc<TableMetadata>,
        snapshot_id: i64,
        io: &dyn ArtifactWriter,
        request: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Vec<FrozenEntry>> {
        let observed = self
            .observed
            .get_or_try_init(|| async {
                let mut scope =
                    ManifestReadScope::new(source, snapshot_id)?.with_owned_request(request);
                let pending = self
                    .compact
                    .as_ref()
                    .map(|compact| compact.operation.pending_stage_new_files(compact.stage))
                    .transpose()?;
                let compact_files = pending.as_ref().map(|pending| &pending.received);
                let target_bytes =
                    self.observed_targets(compact_files)
                        .try_fold(0u64, |total, target| {
                            total
                                .checked_add(observation_target_bytes(
                                    target.path,
                                    target.partition,
                                    compact_files.is_some(),
                                )?)
                                .ok_or_else(|| invalid("materialized target backing overflow"))
                        })?;
                scope.reserve_owned(target_bytes).await?;
                let mut typed = BTreeMap::new();
                for target in self.observed_targets(compact_files) {
                    io.check_active()?;
                    let spec_id = target.partition.partition_spec_id();
                    let spec = scope
                        .metadata()
                        .partition_spec_by_id(spec_id)
                        .ok_or_else(|| {
                            invalid("materialized target names an unknown partition spec")
                        })?;
                    let partition_type = scope
                        .domain()
                        .endpoint()
                        .partition_type(spec_id)
                        .map_err(|error| invalid(error.to_string()))?;
                    let tuple = decode_partition_descriptor_with_type(
                        target.partition.descriptor(),
                        &partition_type,
                    )
                    .map_err(|error| invalid(error.detail_message()))?;
                    let value = TypedPartition::bind_type(spec, &partition_type, &tuple)
                        .map_err(|error| invalid(error.to_string()))?;
                    typed.insert(target.path.to_owned(), value);
                }
                let observation = scope
                    .observe(typed, io.file_io(), || io.check_active())
                    .await?;
                let mut removed = Vec::new();
                let mut removed_ids = BTreeSet::new();
                let sources = scope.snapshot_sources()?;
                for target in self.observed_targets(compact_files) {
                    let data = observation.files.get(target.path).ok_or_else(|| {
                        invalid("materialized target is absent from the completed observation")
                    })?;
                    let Some(effect) = target.effect else {
                        continue;
                    };
                    if !data.matches_data_locator(&observation.completion, effect.locator)? {
                        return Err(invalid(
                            "materialized delete differs from its exact source locator",
                        ));
                    }
                    let old = exclusive_positions(data)?;
                    if effect.removes_data {
                        push_removal(&mut removed, &mut removed_ids, &sources, &data.data)?;
                    }
                    let reported = effect.old_references;
                    if !reported.iter().eq(old.keys().copied()) {
                        return Err(invalid(
                            "materialized delete differs from its exact exclusive old positions",
                        ));
                    }
                    for entry in old.into_values() {
                        push_removal(&mut removed, &mut removed_ids, &sources, entry)?;
                    }
                }
                io.check_active()?;
                Ok::<_, Error>(CompletedStep {
                    scope,
                    targets: observation,
                    removed,
                })
            })
            .await?;
        Ok(observed
            .removed
            .iter()
            .map(|entry| entry.frozen.clone())
            .collect())
    }

    pub(crate) fn observed_data_file(
        &self,
        path: &str,
    ) -> Result<(&TableMetadata, i32, &DataFile)> {
        let observed = self
            .observed
            .get()
            .ok_or_else(|| invalid("Materialized delete lost its source observation"))?;
        let data = &observed
            .targets
            .files
            .get(path)
            .ok_or_else(|| invalid("Removed data file is absent from the source observation"))?
            .data;
        Ok((
            observed.scope.metadata(),
            data.frozen.facts().partition_spec_id,
            observed.scope.observed_data_file(data)?,
        ))
    }
}

pub(in crate::commit::write_stack) fn observation_target_bytes(
    path: &str,
    partition: &IcebergArtifactPartition,
    compact: bool,
) -> Result<u64> {
    // Compact target input is already covered by the original receipt.
    // Reserve the newly created typed-map slot, decoding scratch and output.
    use crate::delete_semantics::CanonicalScalar;
    use crate::iceberg::spec::{Literal, PrimitiveType};

    let (fixed, field_bytes) = if compact {
        let arc_headers = 4 * std::mem::size_of::<std::sync::atomic::AtomicUsize>();
        let fields = 2 * std::mem::size_of::<(i32, PrimitiveType)>()
            + 2 * std::mem::size_of::<Option<CanonicalScalar>>();
        (
            std::mem::size_of::<(String, TypedPartition)>() + arc_headers,
            std::mem::size_of::<Option<Literal>>() + fields,
        )
    } else {
        (
            std::mem::size_of::<(String, Target, TypedPartition)>(),
            std::mem::size_of::<crate::write_descriptor::IcebergPartitionValueDescriptor>(),
        )
    };
    let bytes = partition
        .descriptor()
        .values
        .iter()
        .try_fold(fixed, |total, field| {
            let value_bytes = field.datum_bytes.as_ref().map_or(0, |bytes| {
                if compact {
                    bytes.len()
                } else {
                    bytes.capacity()
                }
            });
            let peak_value = if compact {
                value_bytes.checked_mul(2)
            } else {
                Some(value_bytes)
            };
            peak_value
                .and_then(|bytes| total.checked_add(bytes))
                .and_then(|total| total.checked_add(field_bytes))
                .ok_or_else(|| invalid("materialized target partition backing overflow"))
        })?;
    bytes
        .checked_add(path.len())
        .and_then(|total| u64::try_from(total).ok())
        .ok_or_else(|| invalid("materialized target backing overflow"))
}

fn exclusive_positions(facts: &TargetFacts) -> Result<BTreeMap<&EntryIdentity, &LocatedEntry>> {
    let mut result = BTreeMap::new();
    for position in &facts.positions {
        let exclusive = match position.delete().kind() {
            DeleteKind::Position { exact_target } => exact_target.is_some(),
            DeleteKind::DeletionVector { .. } => true,
            DeleteKind::Equality(_) => {
                return Err(invalid("equality delete entered position proof"));
            }
        };
        if exclusive
            && result
                .insert(position.entry.frozen.identity(), &position.entry)
                .is_some()
        {
            return Err(invalid(
                "materialized source repeats an exclusive position-delete entry",
            ));
        }
    }
    Ok(result)
}

fn push_removal(
    output: &mut Vec<LocatedEntry>,
    seen: &mut BTreeSet<EntryIdentity>,
    sources: &SnapshotSources<'_>,
    entry: &LocatedEntry,
) -> Result<()> {
    if !seen.insert(entry.frozen.identity().clone()) {
        return Err(invalid("materialized step repeats one removed entry"));
    }
    output.push(manifest_removal_location(sources, entry.clone())?);
    Ok(())
}

#[cfg(test)]
pub(crate) async fn observed_materialized_staging_fixture(
    metadata: Arc<TableMetadata>,
    intent: &OperationIntent,
    io: &dyn ArtifactWriter,
) -> Result<impl Preparer> {
    let snapshot = intent
        .start()
        .ok_or_else(|| invalid("fixture source is empty"))?;
    let mut scope = ManifestReadScope::new(metadata, snapshot.snapshot_id)?;
    let mut partitions = BTreeMap::new();
    let mut targets = BTreeMap::new();
    for addition in &intent.changes().added {
        let file = addition.file();
        if file.content_type() != DataContentType::PositionDeletes {
            continue;
        }
        let path = file
            .referenced_data_file()
            .ok_or_else(|| invalid("fixture output has no referenced data file"))?;
        let spec_id = addition.partition_spec_id();
        let spec = scope
            .metadata()
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| invalid("fixture output has no partition spec"))?;
        let ty = scope
            .domain()
            .endpoint()
            .partition_type(spec_id)
            .map_err(|error| invalid(error.to_string()))?;
        let partition = TypedPartition::bind_type(spec, &ty, file.partition())
            .map_err(|error| invalid(error.to_string()))?;
        if partitions.insert(path.clone(), partition).is_some() {
            return Err(invalid(
                "fixture produced multiple deletion vectors for one data file",
            ));
        }
        let descriptor =
            crate::write_descriptor::encode_partition_descriptor_with_type(file.partition(), &ty)
                .map_err(|error| invalid(error.to_string()))?;
        targets.insert(
            path,
            Target {
                partition: IcebergArtifactPartition::try_new(
                    String::new(),
                    String::new(),
                    spec_id,
                    descriptor,
                )
                .map_err(|error| invalid(error.message()))?,
                final_effect: None,
            },
        );
    }
    let observed = scope
        .observe(partitions, io.file_io(), || io.check_active())
        .await?;
    let sources = scope.snapshot_sources()?;
    let mut seen = BTreeSet::new();
    let mut removed = Vec::new();
    for removal in &intent.changes().removed {
        let entry = observed
            .files
            .values()
            .find_map(|target| {
                std::iter::once(&target.data)
                    .chain(target.positions.iter().map(|position| &position.entry))
                    .find(|entry| entry.frozen == *removal)
            })
            .ok_or_else(|| invalid("fixture removal was outside the observed range"))?;
        push_removal(&mut removed, &mut seen, &sources, entry)?;
    }
    let step = Arc::new(MaterializedDeleteStep::new(targets));
    step.observed
        .set(CompletedStep {
            scope,
            targets: observed,
            removed,
        })
        .map_err(|_| invalid("fixture observation was already set"))?;
    Ok(step)
}

#[async_trait::async_trait]
impl Preparer for Arc<MaterializedDeleteStep> {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        intent: &OperationIntent,
    ) -> Result<PreparedChange> {
        let observed = self
            .observed
            .get()
            .ok_or_else(|| invalid("materialized delete lost its source observation"))?;
        let source_id = observed.scope.domain().endpoint().snapshot_id();
        let current = view
            .metadata()
            .snapshot_for_ref(view.target_ref())
            .ok_or_else(|| {
                missing_required_file(
                    crate::commit::model::CommitOperation::Write,
                    "current target snapshot",
                )
            })?;
        let original = observed
            .scope
            .metadata()
            .snapshot_by_id(source_id)
            .ok_or_else(|| invalid("completed source snapshot is absent"))?;
        let version = view.metadata().format_version();
        if self
            .compact
            .as_ref()
            .is_some_and(|compact| compact.counts.position_deletes.files != 0)
        {
            crate::commit::new_manifests::validate_position_format(
                observed.scope.metadata().format_version(),
                version,
            )?;
        }
        let rebound;
        let (scope, removed) = if current.snapshot_id() == source_id {
            if current.manifest_list() != original.manifest_list() {
                return Err(invalid(
                    "An immutable materialized source snapshot changed its manifest list",
                ));
            }
            (&observed.scope, observed.removed.as_slice())
        } else {
            rebound = observed.rebind(view, current.snapshot_id()).await?;
            (&rebound.0, rebound.1.as_slice())
        };
        let sources = scope.snapshot_sources()?;
        let removed = removed.iter().collect::<Vec<_>>();
        let mut summary = match &self.compact {
            Some(compact) => Summary {
                data: compact.counts.data.into(),
                position: compact.counts.position_deletes.into(),
                ..Summary::default()
            },
            None => legacy_addition_summary(view, intent, &self.targets)?,
        };
        for entry in &removed {
            let count = entry.frozen.facts().record_count;
            match entry.content_type {
                DataContentType::Data => summary.removed_data.add(count, entry.logical_size)?,
                DataContentType::PositionDeletes => {
                    summary.removed_position.add(count, entry.logical_size)?
                }
                _ => return Err(invalid("unsupported materialized row-delta removal")),
            }
        }
        let operation = if summary.data.files == 0 {
            Operation::Delete
        } else if summary.position.files == 0 && removed.is_empty() {
            Operation::Append
        } else {
            Operation::Overwrite
        };
        let parent = target_ref_snapshot_id(view.metadata(), view.target_ref());
        let total = snapshot_total_records(view.metadata(), parent)
            .map_err(invalid)?
            .map(|old| {
                old.checked_sub(summary.removed_data.rows)
                    .and_then(|count| count.checked_add(summary.data.rows))
                    .ok_or_else(|| invalid("materialized physical record total overflow"))
            })
            .transpose()?;
        let summary = summary.finish(total)?;
        let snapshot = view.stage_snapshot_id()?;
        let historical = crate::read_snapshot::frozen_partition_types(
            observed.scope.metadata(),
            observed.scope.schema(),
        )
        .map_err(invalid)?;
        let added = if let Some(compact) = &self.compact {
            let (added, _) = crate::commit::new_manifests::prepare_added_manifests(
                &compact.operation,
                view,
                compact.stage,
                snapshot,
                Some(&historical),
            )
            .await?;
            added
        } else {
            write_added_entry_groups_with_partitions(
                view,
                snapshot,
                intent.changes().added.clone(),
                Some(&historical),
            )
            .await?
        };
        let manifests = crate::commit::manifest_transform::ordinary_manifests_scoped(
            view,
            snapshot,
            &sources,
            &removed,
            added,
            scope,
            current.snapshot_id() != source_id,
        )
        .await?;
        crate::commit::overwrite::prepare_ordinary_snapshot_change(
            view, intent, snapshot, operation, manifests, summary, false,
        )
        .await
    }
}

impl CompletedStep {
    async fn rebind(
        &self,
        view: &StagedView<'_>,
        snapshot: i64,
    ) -> Result<(ManifestReadScope, Vec<LocatedEntry>)> {
        let mut scope = self.scope.rebind(view.metadata_ref(), snapshot)?;
        let targets = &self.targets.completion.targets;
        // TypedPartition shares its field and value arrays with E. Charge
        // newly backed path keys and map slots while both ranges coexist.
        let target_bytes = targets.keys().try_fold(0u64, |total, path| {
            std::mem::size_of::<(String, TypedPartition)>()
                .checked_add(path.len())
                .and_then(|bytes| u64::try_from(bytes).ok())
                .and_then(|bytes| total.checked_add(bytes))
                .ok_or_else(|| invalid("Materialized round target size overflow"))
        })?;
        scope.reserve_owned(target_bytes).await?;
        let current = scope
            .observe_present(targets.clone(), view.artifacts().file_io(), || {
                view.artifacts().check_active()
            })
            .await?;
        // Retain the row mutation's position-read checks for every observed
        // target, including files that the write does not physically remove.
        for (path, original) in &self.targets.files {
            view.artifacts().check_active()?;
            let actual = current.files.get(path).ok_or_else(|| {
                missing_required_file(crate::commit::model::CommitOperation::Write, path)
            })?;
            if actual.data.frozen != original.data.frozen {
                return Err(missing_required_file(
                    crate::commit::model::CommitOperation::Write,
                    path,
                ));
            }
        }
        let removed = locate_on_base(
            &mut scope,
            self.removed.iter().map(|entry| &entry.frozen),
            crate::commit::model::CommitOperation::Write,
            view.artifacts().file_io(),
            || view.artifacts().check_active(),
        )
        .await?;
        Ok((scope, removed))
    }
}

fn legacy_addition_summary(
    view: &StagedView<'_>,
    intent: &OperationIntent,
    targets: &BTreeMap<String, Target>,
) -> Result<Summary> {
    let mut summary = Summary::default();
    let mut output_ids = BTreeSet::new();
    for added in &intent.changes().added {
        view.artifacts().check_active()?;
        let file = added.file();
        let identity = EntryIdentity::try_from(file)?;
        if !output_ids.insert(identity.clone()) {
            return Err(invalid(
                "materialized write has a repeated or non-inheriting addition",
            ));
        }
        let declared_target = file
            .referenced_data_file()
            .filter(|path| targets.contains_key(path));
        crate::commit::write_stack::receiving::validation::validate_materialized_addition(
            &identity,
            declared_target.as_deref(),
            added,
        )
        .map_err(|error| invalid(error.message()))?;
        let counters = match file.content_type() {
            DataContentType::Data => &mut summary.data,
            DataContentType::PositionDeletes => &mut summary.position,
            _ => unreachable!("validated materialized addition content"),
        };
        counters.add(file.record_count(), logical_file_size(file)?)?;
    }
    Ok(summary)
}

#[derive(Default)]
struct Counts {
    files: u64,
    rows: u64,
    size: u64,
}

impl From<PhysicalAdditionCounts> for Counts {
    fn from(counts: PhysicalAdditionCounts) -> Self {
        Self {
            files: counts.files,
            rows: counts.rows,
            size: counts.bytes,
        }
    }
}

impl Counts {
    fn add(&mut self, rows: u64, size: u64) -> Result<()> {
        self.files = add(self.files, 1)?;
        self.rows = add(self.rows, rows)?;
        self.size = add(self.size, size)?;
        Ok(())
    }
}

#[derive(Default)]
struct Summary {
    data: Counts,
    position: Counts,
    removed_data: Counts,
    removed_position: Counts,
}

impl Summary {
    fn finish(self, total: Option<u64>) -> Result<HashMap<String, String>> {
        let mut values = HashMap::new();
        for (key, value) in [
            ("added-data-files", self.data.files),
            ("added-records", self.data.rows),
            ("added-delete-files", self.position.files),
            ("added-position-delete-files", self.position.files),
            ("added-position-deletes", self.position.rows),
            ("deleted-data-files", self.removed_data.files),
            ("deleted-records", self.removed_data.rows),
            ("removed-delete-files", self.removed_position.files),
            ("removed-position-delete-files", self.removed_position.files),
            ("removed-position-deletes", self.removed_position.rows),
            ("added-files-size", add(self.data.size, self.position.size)?),
            (
                "removed-files-size",
                add(self.removed_data.size, self.removed_position.size)?,
            ),
        ] {
            if value != 0 {
                values.insert(key.to_owned(), value.to_string());
            }
        }
        if let Some(total) = total {
            values.insert("total-records".into(), total.to_string());
        }
        Ok(values)
    }
}

fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| invalid("materialized snapshot counter overflow"))
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message.into())
}

#[cfg(test)]
mod tests;
