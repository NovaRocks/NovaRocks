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

//! RewriteFiles validates the history and new position deletes at the target parent.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::history::rewrite_files_history_window;
use super::window::{
    WindowFiles, WindowTrace, delete_candidate, delete_sequence, has_owner, position_target,
    snapshot_ids,
};
use super::{ValidationInputs, Verdict};
use crate::commit::conflict::{CandidateFile, validation_conflict};
use crate::commit::model::{
    ArtifactWriter, CommitOperation, EntryIdentity, FrozenEntry, StartSnapshot,
};
use crate::delete_semantics::TypedPartition;
use crate::iceberg::spec::{
    DataContentType, FormatVersion, ManifestContentType, Operation, StructType,
};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::read_snapshot::historical_mutation_partition_types;

const POSITION_DELETE_OPERATIONS: &[Operation] = &[Operation::Overwrite, Operation::Delete];
const POSITION_DELETE_CHECK: &str = "position_deletes_for_replaced_data";

/// Borrows the operation's retained facts. A retry may change the parent, but
/// neither the paths nor the starting sequence are read again from that parent.
pub(super) struct RewriteFilesWindowCheck<'a> {
    replaced: &'a [FrozenEntry],
    start: Option<StartSnapshot>,
}

impl<'a> RewriteFilesWindowCheck<'a> {
    pub(super) fn new(replaced: &'a [FrozenEntry], start: Option<StartSnapshot>) -> Self {
        Self { replaced, start }
    }
}

pub(super) async fn validate_rewrite_files(
    check: RewriteFilesWindowCheck<'_>,
    inputs: &ValidationInputs<'_>,
    io: &dyn ArtifactWriter,
) -> Result<Verdict> {
    if inputs.metadata.format_version() == FormatVersion::V1 {
        return Ok(Verdict::Holds);
    }
    io.check_active()?;
    let start = check.start.map(|start| start.snapshot_id);
    let mut trace = WindowTrace::new(
        CommitOperation::Optimize,
        None,
        inputs,
        start,
        io,
        vec!["history"],
    );
    let window = match rewrite_files_history_window(inputs.metadata, start, inputs.parent) {
        Ok(window) => window,
        Err(last_known) => {
            trace.reject("history", None);
            let endpoint = |id: Option<i64>| id.map_or_else(|| "null".into(), |id| id.to_string());
            return Ok(Verdict::ValidationConflict(validation_conflict(format!(
                "Cannot determine history between starting snapshot {} and the last known ancestor {}",
                endpoint(start),
                endpoint(last_known),
            ))));
        }
    };
    trace.window_len = Some(window.snapshots().len());
    trace.checks.push(POSITION_DELETE_CHECK);
    let owners = snapshot_ids(&window, POSITION_DELETE_OPERATIONS);
    let mut replaced = ReplacedData::new(check.replaced);
    if !owners.is_empty() && !replaced.paths.is_empty() {
        let start_sequence = check.start.map_or(0, |start| start.sequence_number);
        let scan = WindowFiles {
            metadata: inputs.metadata,
            io,
            window: &window,
        };
        if let Some(path) = scan
            .find(
                POSITION_DELETE_OPERATIONS,
                ManifestContentType::Deletes,
                false,
                |entry| {
                    if !entry.raw.is_alive()
                        || entry.raw.data_file().content_type() == DataContentType::EqualityDeletes
                        || !has_owner(entry, &owners)
                        || delete_sequence(entry)? <= start_sequence
                    {
                        return Ok(None);
                    }
                    let file = delete_candidate(inputs.metadata, entry)?;
                    replaced
                        .matching_path(&file, io)
                        .map(|path| path.map(str::to_owned))
                },
            )
            .await?
        {
            io.check_active()?;
            trace.reject(POSITION_DELETE_CHECK, Some(&path));
            return Ok(Verdict::ValidationConflict(validation_conflict(format!(
                "Cannot commit, found new position delete for replaced data file: {path}"
            ))));
        }
    }
    io.check_active()?;
    trace.complete = true;
    Ok(Verdict::Holds)
}

struct ReplacedData<'a> {
    entries: &'a [FrozenEntry],
    paths: BTreeSet<&'a str>,
    partition_types: Option<BTreeMap<i32, StructType>>,
    by_partition: BTreeMap<i32, HashMap<TypedPartition, &'a str>>,
}

impl<'a> ReplacedData<'a> {
    fn new(entries: &'a [FrozenEntry]) -> Self {
        let paths = entries
            .iter()
            .filter_map(|entry| match entry.identity() {
                EntryIdentity::DataFile { path } => Some(path.as_str()),
                _ => None,
            })
            .collect();
        Self {
            entries,
            paths,
            partition_types: None,
            by_partition: BTreeMap::new(),
        }
    }

    fn matching_path(
        &mut self,
        delete: &CandidateFile<'_>,
        io: &dyn ArtifactWriter,
    ) -> Result<Option<&'a str>> {
        if let Some(target) = position_target(delete.file)? {
            return Ok(self.paths.get(target.as_ref()).copied());
        }

        // A position-delete file without a unique target path may apply to
        // any data file in its exact spec and typed partition.
        let invalid = |message| Error::new(ErrorKind::DataInvalid, message);
        let spec = delete
            .metadata
            .partition_spec_by_id(delete.spec_id)
            .ok_or_else(|| {
                invalid(format!(
                    "Delete file has unknown partition spec {}",
                    delete.spec_id
                ))
            })?;
        let partition_types = &mut self.partition_types;
        let partition_types = match partition_types {
            Some(types) => types,
            None => partition_types.insert(
                historical_mutation_partition_types(
                    delete.metadata,
                    delete.metadata.current_schema(),
                )
                .map_err(invalid)?,
            ),
        };
        let ty = partition_types.get(&delete.spec_id).ok_or_else(|| {
            invalid(format!(
                "Delete file has no retained partition type for spec {}",
                delete.spec_id
            ))
        })?;
        let partition = TypedPartition::bind_type(spec, ty, delete.file.partition())
            .map_err(|error| invalid(error.message))?;
        let replaced = match self.by_partition.entry(delete.spec_id) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(slot) => {
                let mut paths = HashMap::new();
                for entry in self.entries {
                    io.check_active()?;
                    if let EntryIdentity::DataFile { path } = entry.identity()
                        && entry.facts().partition_spec_id == delete.spec_id
                    {
                        let key = TypedPartition::bind_type(spec, ty, &entry.facts().partition)
                            .map_err(|error| invalid(error.message))?;
                        paths.entry(key).or_insert(path.as_str());
                    }
                }
                slot.insert(paths)
            }
        };
        Ok(replaced.get(&partition).copied())
    }
}

#[cfg(test)]
mod tests;
