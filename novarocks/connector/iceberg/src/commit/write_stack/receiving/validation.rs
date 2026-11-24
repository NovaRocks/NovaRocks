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

//! Per-fragment admission for Iceberg write receiving and publication.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use novarocks_spi::connector::write_stack::{ConnectorWriterPhysicalContext, WriteTargetOrdinal};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::delete_file::IcebergFileFormat;

use super::super::domain::{
    IcebergArtifactPartition, IcebergCommitArtifact, IcebergCommitFragment, IcebergCreationReceipt,
    IcebergDeletionVectorArtifact, IcebergFinalDeleteKind, IcebergWriteBranch,
    IcebergWriteCommitSession, corrupt, invalid,
};

type Creation = (WriteTargetOrdinal, IcebergCreationReceipt);

#[derive(Debug, Default)]
struct IndexKeys<'a> {
    entries: BTreeSet<EntryKey<'a>>,
    creations_by_path: BTreeMap<Cow<'a, str>, Creation>,
    paths_by_creation_id: BTreeMap<[u8; 16], Cow<'a, str>>,
    delete_targets: BTreeSet<Cow<'a, str>>,
}

/// Committed compact-receiving keys, retained only until receiving closes.
#[derive(Debug, Default)]
pub(crate) struct StatementIndexes(IndexKeys<'static>);

/// Only the new keys admitted for one record. The caller funds these keys and
/// materializes their backing before releasing the original fragments.
pub(crate) struct RecordIndexDelta<'a>(IndexKeys<'a>);

impl RecordIndexDelta<'_> {
    /// Call after the original retention author has funded the quoted backing.
    pub(crate) fn into_owned(self) -> RecordIndexDelta<'static> {
        let IndexKeys {
            entries,
            creations_by_path,
            paths_by_creation_id,
            delete_targets,
        } = self.0;
        RecordIndexDelta(IndexKeys {
            entries: entries.into_iter().map(EntryKey::into_owned).collect(),
            creations_by_path: creations_by_path
                .into_iter()
                .map(|(key, value)| (Cow::Owned(key.into_owned()), value))
                .collect(),
            paths_by_creation_id: paths_by_creation_id
                .into_iter()
                .map(|(key, value)| (key, Cow::Owned(value.into_owned())))
                .collect(),
            delete_targets: delete_targets
                .into_iter()
                .map(|key| Cow::Owned(key.into_owned()))
                .collect(),
        })
    }
}

impl StatementIndexes {
    pub(crate) fn apply(&mut self, delta: RecordIndexDelta<'static>) {
        let IndexKeys {
            entries,
            creations_by_path,
            paths_by_creation_id,
            delete_targets,
        } = delta.0;
        self.0.entries.extend(entries);
        self.0.creations_by_path.extend(creations_by_path);
        self.0.paths_by_creation_id.extend(paths_by_creation_id);
        self.0.delete_targets.extend(delete_targets);
    }
}

pub(crate) struct FragmentAdmission<'session, 'fragment> {
    handle: &'session IcebergWriteCommitSession,
    previous: &'session StatementIndexes,
    keys: IndexKeys<'fragment>,
    // Record-local witnesses only. Across records the compact deletion effect
    // owner supplies its already-retained partition for this exact path.
    v2_partitions: BTreeMap<&'fragment str, &'fragment IcebergArtifactPartition>,
}

struct FragmentKeys<'a> {
    identity: EntryKey<'a>,
    new_creation: Option<(Cow<'a, str>, Creation)>,
    delete_target: Option<Cow<'a, str>>,
}

impl<'session, 'fragment> FragmentAdmission<'session, 'fragment> {
    pub(crate) fn for_record(
        handle: &'session IcebergWriteCommitSession,
        previous: &'session StatementIndexes,
    ) -> Self {
        Self {
            handle,
            previous,
            keys: IndexKeys::default(),
            v2_partitions: BTreeMap::new(),
        }
    }

    /// Stage one compact fragment using borrowed keys and quote the bytes
    /// needed to own them. `previous_v2_partition` comes from the existing
    /// effect for the same exact referenced path. Nothing is installed here.
    pub(crate) fn admit_record_fragment(
        &mut self,
        target: WriteTargetOrdinal,
        fragment: &'fragment IcebergCommitFragment,
        admits_creation: impl FnOnce(&ConnectorWriterPhysicalContext) -> Result<bool, ConnectorError>,
        previous_v2_partition: Option<&IcebergArtifactPartition>,
    ) -> Result<usize, ConnectorError> {
        let keys = self.check_fragment(target, fragment, admits_creation)?;
        if self.handle.mutation_read().is_some()
            && let IcebergCommitArtifact::DataFile(file) = fragment.artifact()
            && file.first_row_id().is_some()
        {
            return Err(invalid("unsupported materialized row-delta addition"));
        }
        let bytes = keys.retained_bytes()?;
        self.check_record_position_partition(fragment, previous_v2_partition)?;
        self.keys.insert(keys);
        Ok(bytes)
    }

    fn check_record_position_partition(
        &mut self,
        fragment: &'fragment IcebergCommitFragment,
        previous: Option<&IcebergArtifactPartition>,
    ) -> Result<(), ConnectorError> {
        if self.handle.mutation_read().is_some()
            && let IcebergCommitArtifact::PositionDeleteFile(file) = fragment.artifact()
        {
            let referenced = file.referenced_data_file();
            let partition = file.partition();
            if previous
                .or_else(|| self.v2_partitions.get(referenced).copied())
                .is_some_and(|old| old != partition)
            {
                return Err(corrupt(
                    "Iceberg materialized step has conflicting effects for one data file",
                ));
            }
            self.v2_partitions.entry(referenced).or_insert(partition);
        }
        Ok(())
    }

    pub(crate) fn finish_record(self) -> RecordIndexDelta<'fragment> {
        RecordIndexDelta(self.keys)
    }

    fn check_fragment(
        &self,
        ordinal: WriteTargetOrdinal,
        fragment: &'fragment IcebergCommitFragment,
        admits_creation: impl FnOnce(&ConnectorWriterPhysicalContext) -> Result<bool, ConnectorError>,
    ) -> Result<FragmentKeys<'fragment>, ConnectorError> {
        let fixed = self.handle.mutation_read().is_some();
        if matches!(fragment.artifact(), IcebergCommitArtifact::FinalDelete(_)) && !fixed {
            return Err(invalid(
                "Iceberg position materialization requires its pinned write endpoint",
            ));
        }
        let new_creation = self.check_creation(ordinal, fragment, admits_creation)?;
        // The normal planner emits dense sorted targets. The validated handle
        // can also retain a different ordering, so keep exact identity lookup.
        // In compact mode this avoids rebuilding the full session index for
        // every output record of a large rewrite.
        let targets = self.handle.targets();
        let target = targets
            .get(ordinal.get() as usize)
            .filter(|target| target.ordinal() == ordinal)
            .or_else(|| targets.iter().find(|target| target.ordinal() == ordinal))
            .ok_or_else(|| {
                invalid(format!(
                    "Iceberg commit fragment names write target {} which this session never sealed",
                    ordinal.get()
                ))
            })?;
        let branch = target.branch();
        if fragment.branch() != branch {
            return Err(invalid(format!(
                "Iceberg commit fragment describes a {} artifact but write target {} drives the {} branch",
                fragment.branch().as_str(),
                ordinal.get(),
                branch.as_str()
            )));
        }
        let identity = EntryKey::from_fragment(fragment);
        if self.keys.entries.contains(&identity) || self.previous.0.entries.contains(&identity) {
            return Err(corrupt(format!(
                "Iceberg prepared write set repeats staged artifact {}",
                fragment.path()
            )));
        }
        let mut delete_target = None;
        if let Some(referenced) = fragment.referenced_data_file() {
            let distributed_v2 = fixed
                && matches!(
                    fragment.artifact(),
                    IcebergCommitArtifact::PositionDeleteFile(_)
                );
            if !distributed_v2 {
                if self.keys.delete_targets.contains(referenced)
                    || self.previous.0.delete_targets.contains(referenced)
                {
                    return Err(corrupt(format!(
                        "Iceberg prepared write set stages more than one delete artifact for data file {referenced}"
                    )));
                }
                delete_target = Some(referenced);
            }
            if fixed {
                match fragment.artifact() {
                    IcebergCommitArtifact::FinalDelete(_) => {}
                    IcebergCommitArtifact::PositionDeleteFile(_)
                        if fragment.merged_old_references().is_empty() => {}
                    _ => {
                        return Err(corrupt(
                            "Iceberg fixed delete endpoint received an obsolete delete artifact",
                        ));
                    }
                }
            } else {
                return Err(invalid(
                    "Iceberg received a position artifact without a fixed mutation",
                ));
            }
        }
        if matches!(fragment.artifact(), IcebergCommitArtifact::DataFile(file)
            if file.metrics().record_count() == 0)
        {
            return Err(invalid(format!(
                "Iceberg staged data file {} reports zero rows",
                fragment.path()
            )));
        }
        Ok(FragmentKeys {
            identity,
            new_creation: new_creation.map(|(path, creation)| (Cow::Borrowed(path), creation)),
            delete_target: delete_target.map(Cow::Borrowed),
        })
    }

    fn check_creation(
        &self,
        ordinal: WriteTargetOrdinal,
        fragment: &'fragment IcebergCommitFragment,
        admits_creation: impl FnOnce(&ConnectorWriterPhysicalContext) -> Result<bool, ConnectorError>,
    ) -> Result<Option<(&'fragment str, Creation)>, ConnectorError> {
        let (Some(scope), Some(path)) = (self.handle.creation_scope(), fragment.new_object_path())
        else {
            return Ok(None);
        };
        let receipt = fragment.creation().ok_or_else(|| {
            corrupt("Iceberg received managed output lacks its original creation receipt")
        })?;
        // Multiple entries can share a Puffin object. Each entry must still
        // have been delivered by its exact admitted query.
        if !admits_creation(receipt.physical())? {
            return Err(cohort_mismatch());
        }
        let creation = (ordinal, receipt);
        if let Some(previous) = self
            .keys
            .creations_by_path
            .get(path)
            .or_else(|| self.previous.0.creations_by_path.get(path))
        {
            if previous != &creation {
                return Err(corrupt(
                    "Iceberg received creation facts disagree about a physical object",
                ));
            }
            return Ok(None);
        }
        let format = if fragment.branch() == IcebergWriteBranch::DeletionVector {
            IcebergFileFormat::Puffin
        } else {
            IcebergFileFormat::Parquet
        };
        if scope.identity(*receipt.physical(), path, format)? != receipt.identity()
            || self
                .keys
                .paths_by_creation_id
                .get(&receipt.create_id())
                .or_else(|| {
                    self.previous
                        .0
                        .paths_by_creation_id
                        .get(&receipt.create_id())
                })
                .is_some_and(|previous| previous.as_ref() != path)
        {
            return Err(cohort_mismatch());
        }
        Ok(Some((path, creation)))
    }
}

impl<'a> IndexKeys<'a> {
    fn insert(&mut self, keys: FragmentKeys<'a>) {
        self.entries.insert(keys.identity);
        if let Some((path, creation)) = keys.new_creation {
            self.paths_by_creation_id
                .insert(creation.1.create_id(), path.clone());
            self.creations_by_path.insert(path, creation);
        }
        if let Some(path) = keys.delete_target {
            self.delete_targets.insert(path);
        }
    }
}

// Mirrors the logical entry identity while the staging indexes still borrow
// from the input. Keeping the full DV range and target preserves exact equality.
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
enum EntryKey<'a> {
    Data(Cow<'a, str>),
    Delete(Cow<'a, str>),
    Vector {
        path: Cow<'a, str>,
        offset: i64,
        length: i64,
        referenced_data_file: Cow<'a, str>,
    },
}

impl<'a> EntryKey<'a> {
    fn from_fragment(fragment: &'a IcebergCommitFragment) -> Self {
        match fragment.artifact() {
            IcebergCommitArtifact::DataFile(file) => Self::Data(Cow::Borrowed(file.path())),
            IcebergCommitArtifact::PositionDeleteFile(file) => {
                Self::Delete(Cow::Borrowed(file.path()))
            }
            IcebergCommitArtifact::EqualityDeleteFile(file) => {
                Self::Delete(Cow::Borrowed(file.path()))
            }
            IcebergCommitArtifact::DeletionVector(file) => {
                Self::vector(file, file.referenced_data_file())
            }
            IcebergCommitArtifact::FinalDelete(effect) => {
                let referenced = effect.target().data_file_path();
                match effect.kind() {
                    IcebergFinalDeleteKind::RemoveDataFile { .. } => {
                        Self::Data(Cow::Borrowed(referenced))
                    }
                    IcebergFinalDeleteKind::ReplacePositions(file) => {
                        Self::vector(file, referenced)
                    }
                }
            }
        }
    }

    fn vector(file: &'a IcebergDeletionVectorArtifact, referenced_data_file: &'a str) -> Self {
        Self::Vector {
            path: Cow::Borrowed(file.path()),
            offset: file.content_range().offset(),
            length: file.content_range().size_in_bytes(),
            referenced_data_file: Cow::Borrowed(referenced_data_file),
        }
    }

    fn into_owned(self) -> EntryKey<'static> {
        match self {
            Self::Data(path) => EntryKey::Data(Cow::Owned(path.into_owned())),
            Self::Delete(path) => EntryKey::Delete(Cow::Owned(path.into_owned())),
            Self::Vector {
                path,
                offset,
                length,
                referenced_data_file,
            } => EntryKey::Vector {
                path: Cow::Owned(path.into_owned()),
                offset,
                length,
                referenced_data_file: Cow::Owned(referenced_data_file.into_owned()),
            },
        }
    }

    fn path_bytes(&self) -> Result<usize, ConnectorError> {
        match self {
            Self::Data(path) | Self::Delete(path) => Ok(path.len()),
            Self::Vector {
                path,
                referenced_data_file,
                ..
            } => checked_add(path.len(), referenced_data_file.len()),
        }
    }
}

impl FragmentKeys<'_> {
    fn retained_bytes(&self) -> Result<usize, ConnectorError> {
        let mut bytes = checked_add(size_of::<EntryKey<'_>>(), self.identity.path_bytes()?)?;
        if let Some((path, _)) = &self.new_creation {
            bytes = checked_add(bytes, size_of::<(Cow<'_, str>, Creation)>())?;
            bytes = checked_add(bytes, size_of::<([u8; 16], Cow<'_, str>)>())?;
            bytes = checked_add(checked_add(bytes, path.len())?, path.len())?;
        }
        if let Some(path) = &self.delete_target {
            bytes = checked_add(checked_add(bytes, size_of::<Cow<'_, str>>())?, path.len())?;
        }
        Ok(bytes)
    }
}

fn checked_add(total: usize, additional: usize) -> Result<usize, ConnectorError> {
    total.checked_add(additional).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg write receiving index backing overflows the result budget",
        )
    })
}

fn cohort_mismatch() -> ConnectorError {
    corrupt("Iceberg received object creation differs from its completed writer cohort")
}

#[cfg(test)]
mod tests;
