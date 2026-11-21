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

use std::collections::BTreeMap;

use crate::iceberg::io::FileIO;
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::target_query::ManifestReadScope;

use super::manifest_sources::{LocatedEntry, SnapshotSources, duplicate_identity};
use super::model::FrozenEntry;

/// Resolve the required entries against the already-bound attempt base.
/// Removing a data entry also removes its current deletion vector, if any.
pub(crate) async fn locate_on_base<'a>(
    scope: &mut ManifestReadScope,
    required: impl IntoIterator<Item = &'a FrozenEntry>,
    io: &FileIO,
    active: impl Fn() -> Result<()> + Sync,
) -> Result<Vec<LocatedEntry>> {
    let mut entries = BTreeMap::new();
    for entry in required {
        active()?;
        if entries.insert(entry.identity(), entry).is_some() {
            return Err(duplicate_identity(entry.identity()));
        }
    }
    let located = scope.locate_live_removals(&entries, io, &active).await?;
    for (identity, expected) in entries {
        active()?;
        let actual = located.get(identity);
        if !actual.is_some_and(|entry| &entry.frozen == expected) {
            return Err(missing_required_file(identity.path()));
        }
    }
    let sources = scope.snapshot_sources()?;
    located
        .into_values()
        .map(|entry| {
            active()?;
            manifest_removal_location(&sources, entry)
        })
        .collect()
}

/// Complete an existing selection authored from the same immutable base.
#[cfg(test)]
pub(super) async fn include_dangling_on_base(
    scope: &mut ManifestReadScope,
    removals: &mut BTreeMap<super::model::EntryIdentity, LocatedEntry>,
    io: &FileIO,
    active: impl Fn() -> Result<()> + Sync,
) -> Result<()> {
    let mut data = BTreeMap::new();
    for (identity, entry) in removals.iter() {
        active()?;
        if matches!(identity, super::model::EntryIdentity::DataFile { .. }) {
            data.insert(identity, &entry.frozen);
        }
    }
    if data.is_empty() {
        return Ok(());
    }
    let dangling = scope.locate_live_dangling_dvs(&data, io, &active).await?;
    let sources = scope.snapshot_sources()?;
    for (identity, entry) in dangling {
        active()?;
        let entry = manifest_removal_location(&sources, entry)?;
        match removals.entry(identity) {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(entry);
            }
            std::collections::btree_map::Entry::Occupied(slot) => {
                let selected = slot.get();
                if selected.source_index != entry.source_index || selected.ordinal != entry.ordinal
                {
                    return Err(duplicate_identity(slot.key()));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn manifest_removal_location(
    sources: &SnapshotSources<'_>,
    mut entry: LocatedEntry,
) -> Result<LocatedEntry> {
    entry.source_index = sources
        .source_index_for_list_ordinal(entry.source_index)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Removal locator does not belong to the completed list",
            )
        })?;
    Ok(entry)
}

pub(super) fn missing_required_file(path: &str) -> Error {
    super::new_manifests::commit_validation_error(format!(
        "Missing required files to delete: {path}"
    ))
}

#[cfg(test)]
mod tests;
