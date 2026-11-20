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

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use novarocks_spi::connector::{ConnectorError, ConnectorMutationFailure};

use super::history::{HistoryGap, HistoryWindow, history_window};
use super::{ValidationInputs, Verdict};
use crate::commit::conflict::{
    CandidateFile, MayMatch, RowDeltaValidationFacts, validation_conflict,
};
use crate::commit::manifest_sources::SnapshotSources;
use crate::commit::model::{ArtifactWriter, Dependency, EntryIdentity, IsolationLevel};
use crate::delete_semantics::{POSITION_FILE_PATH_FIELD_ID, TypedPartition};
use crate::iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestContentType, ManifestStatus, Operation,
    PrimitiveLiteral, StructType, TableMetadata,
};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::binding::BoundEntry;
use crate::read_snapshot::historical_mutation_partition_types;

const DELETED_DATA_OPERATIONS: &[Operation] =
    &[Operation::Overwrite, Operation::Replace, Operation::Delete];
const ADDED_DATA_OPERATIONS: &[Operation] = &[Operation::Append, Operation::Overwrite];
const ADDED_DELETE_OPERATIONS: &[Operation] = &[Operation::Overwrite, Operation::Delete];
const ADDED_DV_OPERATIONS: &[Operation] =
    &[Operation::Overwrite, Operation::Delete, Operation::Replace];

/// The caller lends the exact files already proved live at E by its scoped source.
pub(crate) trait RemovedDataFiles: Send + Sync {
    fn removed_data_file(&self, path: &str) -> Result<CandidateFile<'_>>;
}

#[derive(Clone)]
pub(crate) struct RowDeltaWindowCheck {
    isolation: IsolationLevel,
    scope: Arc<dyn MayMatch>,
    facts: Arc<RowDeltaValidationFacts>,
    writes_data: bool,
    writes_deletes: bool,
}

impl RowDeltaWindowCheck {
    // The write operation installs this dependency when it freezes its row-delta intent.
    #[allow(
        dead_code,
        reason = "The write operation switches dependencies in RWM-12 integration"
    )]
    pub(crate) fn new(
        isolation: IsolationLevel,
        scope: Arc<dyn MayMatch>,
        facts: Arc<RowDeltaValidationFacts>,
        writes_data: bool,
        writes_deletes: bool,
    ) -> Self {
        Self {
            isolation,
            scope,
            facts,
            writes_data,
            writes_deletes,
        }
    }
}

impl fmt::Debug for RowDeltaWindowCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowDeltaWindowCheck")
            .field("isolation", &self.isolation)
            .field("writes_data", &self.writes_data)
            .field("writes_deletes", &self.writes_deletes)
            .field("referenced", &self.facts.referenced.len())
            .field("removed", &self.facts.removed.len())
            .field("dv_targets", &self.facts.dv_targets.len())
            .finish()
    }
}

impl PartialEq for RowDeltaWindowCheck {
    fn eq(&self, other: &Self) -> bool {
        self.isolation == other.isolation
            && self.writes_data == other.writes_data
            && self.writes_deletes == other.writes_deletes
            && Arc::ptr_eq(&self.scope, &other.scope)
            && Arc::ptr_eq(&self.facts, &other.facts)
    }
}

impl Eq for RowDeltaWindowCheck {}

pub(super) async fn validate_row_delta(
    dependency: &Dependency,
    check: &RowDeltaWindowCheck,
    inputs: &ValidationInputs<'_>,
    start: Option<i64>,
    io: &dyn ArtifactWriter,
    removed_data: Option<&dyn RemovedDataFiles>,
) -> Result<Verdict> {
    io.check_active()?;
    let metadata = inputs.metadata;
    let window = match history_window(metadata, start, inputs.parent) {
        Ok(window) => window,
        Err(reason) => {
            return Ok(Verdict::Unprovable {
                dependency: dependency.clone(),
                reason,
            });
        }
    };
    let (start, _) = window.endpoints();
    let scan = WindowFiles {
        metadata,
        io,
        window: &window,
    };
    let facts = &check.facts;
    if check.writes_deletes && !facts.referenced.is_subset(&facts.removed) {
        let owners = snapshot_ids(&window, DELETED_DATA_OPERATIONS);
        if let Some(path) = scan
            .find(
                DELETED_DATA_OPERATIONS,
                ManifestContentType::Data,
                false,
                |entry| {
                    if entry.raw.status != ManifestStatus::Deleted || !has_owner(entry, &owners) {
                        return Ok(None);
                    }
                    let file = data_candidate(metadata, entry)?;
                    let path = file.file.file_path();
                    if facts.referenced.contains(path)
                        && !facts.removed.contains(path)
                        && may_match(check, &file)?
                    {
                        return Ok(Some(path.to_owned()));
                    }
                    Ok(None)
                },
            )
            .await?
        {
            return conflict(format!("Cannot commit, missing data files: [{path}]"));
        }
    }

    if check.isolation == IsolationLevel::Serializable {
        let owners = snapshot_ids(&window, ADDED_DATA_OPERATIONS);
        if let Some(path) = scan
            .find(
                ADDED_DATA_OPERATIONS,
                ManifestContentType::Data,
                true,
                |entry| {
                    if entry.raw.status != ManifestStatus::Added || !has_owner(entry, &owners) {
                        return Ok(None);
                    }
                    let file = data_candidate(metadata, entry)?;
                    Ok(may_match(check, &file)?.then(|| file.file.file_path().to_owned()))
                },
            )
            .await?
        {
            return conflict(format!(
                "Found conflicting files that can contain records matching {}: [{path}]",
                check.scope
            ));
        }
    }

    if check.writes_data {
        let start_sequence = start
            .and_then(|id| metadata.snapshot_by_id(id))
            .map_or(0, |snapshot| snapshot.sequence_number());
        let mut first_delete = None;
        let mut partition_types = None;
        let replaced = scan
            .find(
                ADDED_DELETE_OPERATIONS,
                ManifestContentType::Deletes,
                false,
                |entry| {
                    if !entry.raw.is_alive() || delete_sequence(entry)? <= start_sequence {
                        return Ok(None);
                    }
                    let file = delete_candidate(metadata, entry)?;
                    if !may_match(check, &file)? {
                        return Ok(None);
                    }
                    let path = file.file.file_path();
                    if facts.removed.is_empty() {
                        return Ok(Some(path.to_owned()));
                    }
                    if let Some(path) = replaced_data_file(
                        io,
                        &file,
                        &facts.removed,
                        removed_data,
                        &mut partition_types,
                    )? {
                        return Ok(Some(path));
                    }
                    // Hold only the first possible conflict while looking for the earlier
                    // validation rule over the same stream.
                    first_delete.get_or_insert_with(|| path.to_owned());
                    Ok(None)
                },
            )
            .await?;
        if facts.removed.is_empty() {
            first_delete = replaced;
        } else if let Some(path) = replaced {
            return conflict(format!(
                "Cannot commit, found new delete for replaced data file: {path}"
            ));
        }
        if let Some(path) = first_delete {
            return conflict(format!(
                "Found new conflicting delete files that can apply to records matching {}: [{path}]",
                check.scope
            ));
        }
    }

    if let Some(path) = facts.removed.intersection(&facts.referenced).next() {
        return conflict(format!(
            "Cannot delete data files [{path}] that are referenced by new delete files"
        ));
    }

    if !facts.dv_targets.is_empty() {
        let owners = snapshot_ids(&window, ADDED_DV_OPERATIONS);
        if let Some(detail) = scan.find(ADDED_DV_OPERATIONS, ManifestContentType::Deletes, true, |entry| {
            let file = entry.raw.data_file();
            if !entry.raw.is_alive() || file.file_format() != DataFileFormat::Puffin || !has_owner(entry, &owners) {
                return Ok(None);
            }
            let Some(target) = file.referenced_data_file() else {
                return Err(invalid("Concurrent deletion vector is missing its referenced data file"));
            };
            if !facts.dv_targets.contains(target.as_str()) || !may_match(check, &delete_candidate(metadata, entry)?)? {
                return Ok(None);
            }
            let EntryIdentity::DeletionVector { path, offset, length, .. } = EntryIdentity::try_from(file)? else {
                return Err(invalid("Concurrent Puffin entry does not contain a deletion vector"));
            };
            Ok(Some(format!(
                "Found concurrently added DV for {target}: DV{{location={path}, offset={offset}, length={length}, referencedDataFile={target}}}"
            )))
        }).await? {
            return conflict(detail);
        }
    }

    io.check_active()?;
    Ok(Verdict::Holds)
}

fn snapshot_ids(window: &HistoryWindow, operations: &[Operation]) -> BTreeSet<i64> {
    window
        .filtered(operations)
        .map(|snapshot| snapshot.snapshot_id())
        .collect()
}

fn has_owner(entry: &BoundEntry<'_>, owners: &BTreeSet<i64>) -> bool {
    entry
        .sequences
        .snapshot_id
        .is_some_and(|id| owners.contains(&id))
}

fn delete_sequence(entry: &BoundEntry<'_>) -> Result<i64> {
    entry
        .sequences
        .data_sequence
        .filter(|sequence| *sequence >= 0)
        .ok_or_else(|| {
            invalid(format!(
                "Delete file {} has no resolved data sequence",
                entry.raw.data_file().file_path()
            ))
        })
}

fn may_match(check: &RowDeltaWindowCheck, file: &CandidateFile<'_>) -> Result<bool> {
    check
        .scope
        .may_match(file)
        .map_err(|error: ConnectorError| {
            Error::new(ErrorKind::Unexpected, error.message()).with_source(error)
        })
}

fn data_candidate<'a>(
    metadata: &'a TableMetadata,
    entry: &BoundEntry<'a>,
) -> Result<CandidateFile<'a>> {
    let file = entry.raw.data_file();
    if file.content_type() != DataContentType::Data {
        return Err(invalid("A data manifest contains a delete entry"));
    }
    Ok(CandidateFile {
        metadata,
        spec_id: entry.source.descriptor.partition_spec_id,
        file,
    })
}

fn delete_candidate<'a>(
    metadata: &'a TableMetadata,
    entry: &BoundEntry<'a>,
) -> Result<CandidateFile<'a>> {
    let file = entry.raw.data_file();
    if !matches!(
        file.content_type(),
        DataContentType::EqualityDeletes | DataContentType::PositionDeletes
    ) {
        return Err(invalid("A delete manifest contains a data entry"));
    }
    Ok(CandidateFile {
        metadata,
        spec_id: entry.source.descriptor.partition_spec_id,
        file,
    })
}

struct WindowFiles<'a> {
    metadata: &'a TableMetadata,
    io: &'a dyn ArtifactWriter,
    window: &'a HistoryWindow,
}

impl WindowFiles<'_> {
    async fn find(
        &self,
        operations: &[Operation],
        content: ManifestContentType,
        must_have_additions: bool,
        mut find: impl FnMut(&BoundEntry<'_>) -> Result<Option<String>>,
    ) -> Result<Option<String>> {
        for snapshot in self.window.filtered(operations) {
            let sources = SnapshotSources::load(self.metadata, snapshot, self.io.file_io(), || {
                self.io.check_active()
            })
            .await?;
            let result = sources
                .fold(
                    self.io.file_io(),
                    || self.io.check_active(),
                    |descriptor| {
                        descriptor.content == content
                            && descriptor.added_snapshot_id == snapshot.snapshot_id()
                            && (!must_have_additions || descriptor.has_added_files())
                    },
                    |entry| match find(&entry)? {
                        Some(detail) => {
                            Err(Error::new(ErrorKind::Unexpected, "Row-delta entry matched")
                                .with_source(MatchedEntry(detail)))
                        }
                        None => Ok(()),
                    },
                )
                .await;
            if let Err(error) = result {
                use std::error::Error as _;
                if let Some(matched) = error
                    .source()
                    .and_then(|cause| cause.downcast_ref::<MatchedEntry>())
                {
                    return Ok(Some(matched.0.clone()));
                }
                return Err(error);
            }
        }
        Ok(None)
    }
}

// SnapshotSources::fold stops at the first match without reading another
// manifest. Original reader, predicate and control errors travel unchanged.
#[derive(Debug)]
struct MatchedEntry(String);

impl fmt::Display for MatchedEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for MatchedEntry {}

fn conflict(message: String) -> Result<Verdict> {
    Ok(Verdict::ValidationConflict(validation_conflict(message)))
}

pub(super) fn history_conflict(
    reason: &HistoryGap,
    metadata: &TableMetadata,
    start: Option<i64>,
    parent: Option<i64>,
) -> ConnectorMutationFailure {
    let endpoint = |id: Option<i64>| id.map_or_else(|| "null".into(), |id| id.to_string());
    match reason {
        HistoryGap::StartExpired(id) | HistoryGap::StartNotAncestor { start: id, .. } => {
            validation_conflict(format!(
                "Snapshot {id} is not an ancestor of {}",
                endpoint(parent)
            ))
        }
        HistoryGap::SnapshotMissing(_) | HistoryGap::Cycle(_) => {
            // This path is only used after the exact window reports a gap.
            let mut seen = BTreeSet::new();
            let mut next = parent;
            let mut last = None;
            while let Some(id) = next {
                if !seen.insert(id) {
                    break;
                }
                let Some(snapshot) = metadata.snapshot_by_id(id) else {
                    break;
                };
                last = Some(id);
                next = snapshot.parent_snapshot_id();
            }
            validation_conflict(format!(
                "Cannot determine history between starting snapshot {} and the last known ancestor {}",
                endpoint(start),
                endpoint(last)
            ))
        }
    }
}

fn replaced_data_file(
    io: &dyn ArtifactWriter,
    delete: &CandidateFile<'_>,
    removed: &BTreeSet<Arc<str>>,
    removed_data: Option<&dyn RemovedDataFiles>,
    partition_types: &mut Option<BTreeMap<i32, StructType>>,
) -> Result<Option<String>> {
    let lookup = removed_data.ok_or_else(|| {
        Error::new(
            ErrorKind::Unexpected,
            "Row-delta operation lost the scoped source of removed data files",
        )
    })?;
    let target = position_target(delete.file)?;
    if target
        .as_deref()
        .is_some_and(|target| !removed.contains(target))
    {
        return Ok(None);
    }
    let spec = delete
        .metadata
        .partition_spec_by_id(delete.spec_id)
        .ok_or_else(|| {
            invalid(format!(
                "Delete file has unknown partition spec {}",
                delete.spec_id
            ))
        })?;
    let global =
        spec.is_unpartitioned() && delete.file.content_type() == DataContentType::EqualityDeletes;
    let partition = if global {
        None
    } else {
        let types = match partition_types {
            Some(types) => types,
            None => partition_types.insert(
                historical_mutation_partition_types(
                    delete.metadata,
                    delete.metadata.current_schema(),
                )
                .map_err(invalid)?,
            ),
        };
        let ty = types.get(&delete.spec_id).ok_or_else(|| {
            invalid(format!(
                "Delete file has no retained partition type for spec {}",
                delete.spec_id
            ))
        })?;
        let key = TypedPartition::bind_type(spec, ty, delete.file.partition())
            .map_err(|error| invalid(error.message))?;
        Some((ty, key))
    };

    let same_partition = |data: &CandidateFile<'_>| -> Result<bool> {
        let Some((ty, key)) = &partition else {
            return Ok(true);
        };
        if data.spec_id != delete.spec_id {
            return Ok(false);
        }
        let data_partition = TypedPartition::bind_type(spec, ty, data.file.partition())
            .map_err(|error| invalid(error.message))?;
        Ok(&data_partition == key)
    };

    if let Some(target) = target {
        io.check_active()?;
        if !same_partition(&lookup.removed_data_file(&target)?)? {
            return Err(invalid(format!(
                "Delete file {} differs from the partition of its exact target {target}",
                delete.file.file_path()
            )));
        }
        return Ok(Some(target.into_owned()));
    }
    for path in removed {
        io.check_active()?;
        let source = lookup.removed_data_file(path)?;
        if same_partition(&source)? && equality_can_match(delete.file, source.file, delete.metadata)
        {
            return Ok(Some(path.to_string()));
        }
    }
    Ok(None)
}

fn position_target(file: &DataFile) -> Result<Option<Cow<'_, str>>> {
    if file.content_type() == DataContentType::EqualityDeletes {
        return Ok(None);
    }
    if let Some(path) = file.referenced_data_file() {
        return Ok(Some(Cow::Owned(path)));
    }
    if file.file_format() == DataFileFormat::Puffin {
        return Err(invalid(
            "Concurrent deletion vector is missing its referenced data file",
        ));
    }
    let lower = file.lower_bounds().get(&POSITION_FILE_PATH_FIELD_ID);
    let upper = file.upper_bounds().get(&POSITION_FILE_PATH_FIELD_ID);
    match (
        lower.map(|bound| bound.literal()),
        upper.map(|bound| bound.literal()),
    ) {
        (Some(PrimitiveLiteral::String(lower)), Some(PrimitiveLiteral::String(upper)))
            if lower == upper =>
        {
            Ok(Some(Cow::Borrowed(lower.as_str())))
        }
        _ => Ok(None),
    }
}

// The delete index checks equality field ranges after partition and sequence.
fn equality_can_match(delete: &DataFile, data: &DataFile, metadata: &TableMetadata) -> bool {
    let Some(ids) = delete.equality_ids() else {
        return true;
    };
    ids.iter().all(|id| {
        let field = metadata.current_schema().field_by_id(*id).or_else(|| {
            // A delete can still carry metrics for a field that was dropped.
            metadata
                .schemas_iter()
                .filter_map(|schema| Some((schema.schema_id(), schema.field_by_id(*id)?)))
                .max_by_key(|(schema_id, _)| *schema_id)
                .map(|(_, field)| field)
        });
        let Some(field) = field else {
            return true;
        };
        if !field.field_type.is_primitive() {
            return true;
        }
        let data_nulls = data.null_value_counts().get(id).copied();
        let delete_nulls = delete.null_value_counts().get(id).copied();
        let data_values = data.value_counts().get(id).copied();
        let delete_values = delete.value_counts().get(id).copied();

        if !field.required {
            // Two nullable files might have a matching null, even when their
            // ordinary values' bounds are disjoint.
            if data_nulls != Some(0) && delete_nulls != Some(0) {
                return true;
            }
            if (data_nulls.is_some() && data_nulls == data_values && delete_nulls == Some(0))
                || (delete_nulls.is_some()
                    && delete_nulls == delete_values
                    && data_nulls == Some(0))
            {
                return false;
            }
        }
        let (Some(data_lower), Some(data_upper), Some(delete_lower), Some(delete_upper)) = (
            data.lower_bounds().get(id),
            data.upper_bounds().get(id),
            delete.lower_bounds().get(id),
            delete.upper_bounds().get(id),
        ) else {
            return true;
        };
        !matches!(
            data_lower.partial_cmp(delete_upper),
            Some(std::cmp::Ordering::Greater)
        ) && !matches!(
            delete_lower.partial_cmp(data_upper),
            Some(std::cmp::Ordering::Greater)
        )
    })
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

#[cfg(test)]
mod tests;
