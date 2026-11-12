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

use crate::commit::helpers::{snapshot_total_records, target_ref_snapshot_id};
use crate::commit::manifest_sources::{LocatedEntry, SnapshotSources};
use crate::commit::model::{ArtifactWriter, EntryIdentity, FrozenEntry, OperationIntent, SeqField};
use crate::commit::row_delta_dv_metadata::{
    logical_file_size, write_added_entry_groups_with_partitions,
};
use crate::commit::staging::{PreparedChange, Preparer, StagedView};
use crate::commit::write_stack::domain::{
    IcebergArtifactPartition, IcebergFinalDeleteEffect, IcebergFinalDeleteKind,
};
use crate::delete_semantics::{DeleteKind, TypedPartition};
use crate::iceberg::spec::{DataContentType, Operation, TableMetadata};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::target_query::{ManifestReadScope, TargetFacts};
use crate::write_descriptor::decode_partition_descriptor_with_type;

pub(super) struct MaterializedDeleteStep {
    targets: BTreeMap<String, Target>,
    observed: tokio::sync::OnceCell<CompletedStep>,
}

pub(super) struct Target {
    pub(super) partition: IcebergArtifactPartition,
    pub(super) final_effect: Option<IcebergFinalDeleteEffect>,
}

struct CompletedStep {
    scope: ManifestReadScope,
    removed: Vec<LocatedEntry>,
}

impl MaterializedDeleteStep {
    pub(super) fn new(targets: BTreeMap<String, Target>) -> Self {
        Self {
            targets,
            observed: tokio::sync::OnceCell::new(),
        }
    }

    pub(super) async fn observe(
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
                let target_bytes =
                    self.targets
                        .iter()
                        .try_fold(0u64, |total, (path, target)| {
                            let descriptor = target.partition.descriptor();
                            let fixed = std::mem::size_of::<(String, Target, TypedPartition)>();
                            let values =
                                descriptor.values.iter().try_fold(fixed, |bytes, value| {
                                    bytes
                                        .checked_add(std::mem::size_of_val(value))
                                        .and_then(|bytes| {
                                            bytes.checked_add(
                                                value.datum_bytes.as_ref().map_or(0, Vec::capacity),
                                            )
                                        })
                                        .ok_or_else(|| {
                                            invalid(
                                                "materialized target partition backing overflow",
                                            )
                                        })
                                })?;
                            total
                                .checked_add(values as u64)
                                .and_then(|total| total.checked_add(path.capacity() as u64))
                                .ok_or_else(|| invalid("materialized target backing overflow"))
                        })?;
                scope.reserve_owned(target_bytes).await?;
                let mut typed = BTreeMap::new();
                for (path, target) in &self.targets {
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
                    typed.insert(path.clone(), value);
                }
                let observation = scope
                    .observe(typed, io.file_io(), || io.check_active())
                    .await?;
                let mut removed = Vec::new();
                let mut removed_ids = BTreeSet::new();
                let sources = scope.snapshot_sources()?;
                for (path, target) in &self.targets {
                    let data = observation.files.get(path).ok_or_else(|| {
                        invalid("materialized target is absent from the completed observation")
                    })?;
                    let Some(effect) = &target.final_effect else {
                        continue;
                    };
                    if effect.target() != &data.data_locator(&observation.completion)? {
                        return Err(invalid(
                            "materialized delete differs from its exact source locator",
                        ));
                    }
                    let old = exclusive_positions(data)?;
                    let reported = match effect.kind() {
                        IcebergFinalDeleteKind::RemoveDataFile {
                            old_exclusive_deletes,
                            ..
                        } => {
                            push_removal(&mut removed, &mut removed_ids, &sources, &data.data)?;
                            old_exclusive_deletes.as_ref()
                        }
                        IcebergFinalDeleteKind::ReplacePositions(vector) => {
                            vector.merged_old_references()
                        }
                    };
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
                Ok::<_, Error>(CompletedStep { scope, removed })
            })
            .await?;
        Ok(observed
            .removed
            .iter()
            .map(|entry| entry.frozen.clone())
            .collect())
    }
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
    let mut located = entry.clone();
    located.source_index = sources
        .source_index_for_list_ordinal(entry.source_index)
        .ok_or_else(|| invalid("materialized locator does not belong to the completed list"))?;
    output.push(located);
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
        .set(CompletedStep { scope, removed })
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
        let sources = observed.scope.snapshot_sources()?;
        let source_id = observed.scope.domain().endpoint().snapshot_id();
        let current = view
            .metadata()
            .snapshot_for_ref(view.target_ref())
            .ok_or_else(|| invalid("materialized delete has no current source snapshot"))?;
        let original = observed
            .scope
            .metadata()
            .snapshot_by_id(source_id)
            .ok_or_else(|| invalid("completed source snapshot is absent"))?;
        if current.snapshot_id() != source_id || current.manifest_list() != original.manifest_list()
        {
            return Err(invalid("materialized delete source changed before staging"));
        }
        let removed = observed.removed.iter().collect::<Vec<_>>();
        let mut summary = Summary::default();
        let mut output_ids = BTreeSet::new();
        for added in &intent.changes().added {
            view.artifacts().check_active()?;
            let file = added.file();
            if added.data_sequence() != SeqField::Inherit
                || !output_ids.insert(EntryIdentity::try_from(file)?)
            {
                return Err(invalid(
                    "materialized write has a repeated or non-inheriting addition",
                ));
            }
            match file.content_type() {
                DataContentType::Data if file.first_row_id().is_none() => {
                    summary
                        .data
                        .add(file.record_count(), logical_file_size(file)?)?;
                }
                DataContentType::PositionDeletes => {
                    let referenced = file
                        .referenced_data_file()
                        .ok_or_else(|| invalid("materialized delete file has no exact target"))?;
                    if !self.targets.contains_key(referenced.as_str()) {
                        return Err(invalid("materialized output references a different target"));
                    }
                    summary
                        .position
                        .add(file.record_count(), logical_file_size(file)?)?;
                }
                _ => return Err(invalid("unsupported materialized row-delta addition")),
            }
        }
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
        let snapshot = crate::commit::staging::new_snapshot_id(view.metadata());
        let historical = crate::read_snapshot::frozen_partition_types(
            observed.scope.metadata(),
            observed.scope.schema(),
        )
        .map_err(invalid)?;
        let added = write_added_entry_groups_with_partitions(
            view,
            snapshot,
            intent.changes().added.clone(),
            Some(&historical),
        )
        .await?;
        let manifests = crate::commit::manifest_transform::ordinary_manifests_scoped(
            view,
            snapshot,
            &sources,
            &removed,
            added,
            &observed.scope,
        )
        .await?;
        crate::commit::overwrite::prepare_ordinary_snapshot_change(
            view, intent, snapshot, operation, manifests, summary, false,
        )
        .await
    }
}

#[derive(Default)]
struct Counts {
    files: u64,
    rows: u64,
    size: u64,
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
