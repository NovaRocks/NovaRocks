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

//! Provider custody of root write records before the publication handoff.

// Design: ADR-0239 (docs/adr/ADR-0239-iceberg-compact-receiving-and-rolling-manifests.md)

pub(crate) mod validation;

use std::collections::{BTreeMap, LinkedList};
use std::sync::Arc;

use novarocks_spi::connector::read_stack::runtime::ConnectorRewrittenTargetFact;
use novarocks_spi::connector::write_stack::{
    ConnectorCommitFragment, ConnectorWriteProductionProof, ConnectorWriteQueryIdentity,
    ConnectorWriterPhysicalContext, WriteTargetOrdinal, validate_query_target_ordinals,
    validate_write_fragment_targets,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPayloadRetentionGuard, ConnectorProviderBindingKey,
};

use crate::commit::conflict_scope::{
    ConflictScope, IcebergRewrittenTargetFact, RewrittenTargetBinding, WrittenPartitionCollector,
    WrittenPartitionDomain,
};
use crate::iceberg::spec::TableMetadata;

use super::domain::{
    IcebergArtifactPartition, IcebergCommitArtifact, IcebergSealedWriteTarget,
    IcebergWriteCommitSession, IcebergWriteFlavor, invalid,
};
use super::runtime::IcebergWriteAdapter;
use compact::{AddedCounters, CompactEntry};
use validation::{FragmentAdmission, StatementIndexes};

pub(crate) mod compact;

type WriteFragment = (WriteTargetOrdinal, ConnectorCommitFragment);

/// Provider-owned receiving state for the frozen write flavor.
#[derive(Debug, Default)]
pub(crate) struct IcebergReceiving {
    queries: BTreeMap<ConnectorWriteQueryIdentity, QueryReceiving>,
    records: Vec<ReceivedRecord>,
    compact: Option<CompactReceiving>,
    rewrite_groups: Option<usize>,
    rewritten_target: Option<Box<RewrittenTargetReceiving>>,
}

#[derive(Debug)]
struct RewrittenTargetReceiving {
    owner: ConnectorProviderBindingKey,
    binding: RewrittenTargetBinding,
    metadata_digest: [u8; 32],
    collector: WrittenPartitionCollector,
    written: Option<WrittenPartitionDomain>,
}

fn rewritten_target_error(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

#[derive(Default)]
struct CompactReceiving {
    indexes: StatementIndexes,
    files: ReceivedCompactFiles,
}

#[derive(Default)]
pub(crate) struct ReceivedCompactFiles {
    // One exact partition witness per V2 target. Its backing is prepaid by
    // the first compact fragment for that target.
    pub(crate) v2_targets: BTreeMap<String, IcebergArtifactPartition>,
    // Each node owns one paid entry and can release its allocation immediately
    // after the manifest write. Grouping happens once, during receiving.
    pub(crate) groups: BTreeMap<AdditionGroup, LinkedList<CompactEntry>>,
    pub(crate) author: Option<ConnectorPayloadRetentionGuard>,
    pub(crate) counts: AddedCounters,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum AdditionGroup {
    Additions { spec: i32, deletes: bool },
    Removals,
}

impl ReceivedCompactFiles {
    pub(crate) fn has_delete_effects(&self) -> bool {
        !self.v2_targets.is_empty()
            || self.groups.keys().any(|group| {
                matches!(
                    group,
                    AdditionGroup::Removals | AdditionGroup::Additions { deletes: true, .. }
                )
            })
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = &CompactEntry> {
        self.groups.values().flatten()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    pub(crate) fn push(&mut self, entry: CompactEntry) {
        use crate::iceberg::spec::ManifestContentType;
        let group = entry
            .addition
            .as_ref()
            .map_or(AdditionGroup::Removals, |addition| {
                AdditionGroup::Additions {
                    spec: addition.partition_spec_id,
                    deletes: addition.content == ManifestContentType::Deletes,
                }
            });
        self.groups.entry(group).or_default().push_back(entry);
    }
}

impl std::fmt::Debug for CompactReceiving {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompactReceiving")
            .field("groups", &self.files.groups.len())
            .field("counts", &self.files.counts)
            .finish()
    }
}

struct QueryReceiving {
    targets: Vec<WriteTargetOrdinal>,
    production: Option<Arc<dyn ConnectorWriteProductionProof>>,
    rewritten_target: Option<Arc<IcebergRewrittenTargetFact>>,
    confirmed: bool,
}

impl std::fmt::Debug for QueryReceiving {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryReceiving")
            .field("targets", &self.targets)
            .field("has_production", &self.production.is_some())
            .field("confirmed", &self.confirmed)
            .finish()
    }
}

impl QueryReceiving {
    fn validate_arrival(&self, fragments: &[WriteFragment]) -> Result<(), ConnectorError> {
        if self.confirmed {
            return Err(invalid("Iceberg write query already completed receiving"));
        }
        validate_write_fragment_targets(fragments, &self.targets)
    }
}

#[derive(Debug)]
struct ReceivedRecord {
    query: ConnectorWriteQueryIdentity,
    fragments: Vec<WriteFragment>,
}

pub(crate) struct ReceivedFragment<'a> {
    pub(crate) target: WriteTargetOrdinal,
    pub(crate) fragment: &'a ConnectorCommitFragment,
    production: Option<&'a dyn ConnectorWriteProductionProof>,
}

impl ReceivedFragment<'_> {
    pub(crate) fn admits_creation(
        &self,
        physical: &ConnectorWriterPhysicalContext,
    ) -> Result<bool, ConnectorError> {
        match self.production {
            Some(production) => production.admits(self.target, physical),
            None => Ok(false),
        }
    }
}

fn matches_rewritten_target(metadata: &TableMetadata, binding: RewrittenTargetBinding) -> bool {
    metadata.uuid() == binding.table_uuid
        && match binding.snapshot_id {
            Some(snapshot_id) => metadata.snapshot_by_id(snapshot_id).is_some(),
            None => metadata.current_snapshot_id().is_none(),
        }
}

impl IcebergReceiving {
    #[cfg(test)]
    pub(crate) fn from_compact_files(files: ReceivedCompactFiles) -> Self {
        Self {
            compact: Some(CompactReceiving {
                files,
                ..CompactReceiving::default()
            }),
            ..Self::default()
        }
    }

    pub(super) fn with_compact_mode(compact: bool, rewrite_groups: Option<usize>) -> Self {
        Self {
            compact: compact.then(CompactReceiving::default),
            rewrite_groups,
            ..Self::default()
        }
    }

    pub(super) fn enable_fixed_mutation(&mut self, flavor: IcebergWriteFlavor) {
        if matches!(
            flavor,
            IcebergWriteFlavor::RowMutationPositionDelete
                | IcebergWriteFlavor::RowMutationDeletionVector
        ) {
            assert!(
                self.is_unstarted(),
                "fixed mutation must be bound before receiving"
            );
            self.compact = Some(CompactReceiving::default());
        }
    }

    pub(crate) fn bind_rewritten_target(
        &mut self,
        owner: ConnectorProviderBindingKey,
        binding: RewrittenTargetBinding,
        metadata: &TableMetadata,
    ) -> Result<(), ConnectorError> {
        if !self.is_unstarted() || self.rewritten_target.is_some() {
            return Err(rewritten_target_error(
                "Iceberg rewritten target binding was installed after receiving started",
            ));
        }
        if !matches_rewritten_target(metadata, binding) {
            return Err(rewritten_target_error(
                "Iceberg rewritten target binding differs from the original write observation",
            ));
        }
        self.rewritten_target = Some(Box::new(RewrittenTargetReceiving {
            owner,
            binding,
            metadata_digest: crate::read_snapshot::metadata_identity_digest(metadata)
                .map_err(rewritten_target_error)?,
            collector: WrittenPartitionCollector::new(metadata),
            written: Some(WrittenPartitionDomain::default()),
        }));
        Ok(())
    }

    pub(crate) fn row_conflict_scope(
        &mut self,
        source: &TableMetadata,
    ) -> Result<Option<ConflictScope>, ConnectorError> {
        if self.rewritten_target.is_some() {
            self.require_complete()?;
        }
        let Some(receiving) = self.rewritten_target.as_mut() else {
            return Ok(None);
        };
        if !matches_rewritten_target(source, receiving.binding)
            || crate::read_snapshot::metadata_identity_digest(source)
                .map_err(rewritten_target_error)?
                != receiving.metadata_digest
        {
            return Err(rewritten_target_error(
                "Iceberg conflict scope requires the original write observation",
            ));
        }
        let fact = self
            .queries
            .values()
            .next()
            .and_then(|query| query.rewritten_target.as_deref())
            .ok_or_else(|| {
                rewritten_target_error("Iceberg row-level write lost its rewritten target fact")
            })?;
        let written = receiving.written.take().ok_or_else(|| {
            rewritten_target_error("Iceberg row-level conflict scope was already taken")
        })?;
        ConflictScope::compose(fact, written, source).map(Some)
    }

    pub(crate) fn compact_files(&self) -> Option<&ReceivedCompactFiles> {
        self.compact.as_ref().map(|compact| &compact.files)
    }

    pub(crate) fn take_compact_files(&mut self) -> Option<ReceivedCompactFiles> {
        self.compact.take().map(|compact| compact.files)
    }

    pub(crate) fn arrive_original(
        &mut self,
        handle: &IcebergWriteCommitSession,
        adapter: &IcebergWriteAdapter,
        query: ConnectorWriteQueryIdentity,
        mut fragments: Vec<WriteFragment>,
        author: &ConnectorPayloadRetentionGuard,
    ) -> Result<(), ConnectorError> {
        let Some(compact) = self.compact.as_mut() else {
            return self.arrive(query, &mut fragments);
        };
        let registration = self
            .queries
            .get(&query)
            .ok_or_else(|| invalid("Iceberg write arrival names an unregistered query"))?;
        registration.validate_arrival(&fragments)?;
        if fragments.is_empty() {
            return Ok(());
        }
        let rewrite = handle
            .flavor()
            .is_distributed_rewrite()
            .then_some(handle.table());
        let mut record_partitions = WrittenPartitionDomain::default();
        let mut admission = FragmentAdmission::for_record(handle, &compact.indexes);
        let mut counts = compact.files.counts;
        let mut prepared = Vec::with_capacity(fragments.len());
        let mut v2_targets = BTreeMap::<&str, &IcebergArtifactPartition>::new();
        for (target, fragment) in &fragments {
            let typed = adapter.commit_fragment(fragment)?;
            let position_target = match typed.artifact() {
                IcebergCommitArtifact::PositionDeleteFile(file)
                    if handle.mutation_read().is_some() =>
                {
                    Some(file)
                }
                _ => None,
            };
            let previous_partition = position_target.and_then(|file| {
                compact
                    .files
                    .v2_targets
                    .get(file.referenced_data_file())
                    .or_else(|| v2_targets.get(file.referenced_data_file()).copied())
            });
            let index_bytes = admission.admit_record_fragment(
                *target,
                typed,
                |physical| match registration.production.as_deref() {
                    Some(production) => production.admits(*target, physical),
                    None => Ok(false),
                },
                previous_partition,
            )?;
            let witness_bytes = if let Some(file) = position_target
                && previous_partition.is_none()
            {
                v2_targets.insert(file.referenced_data_file(), file.partition());
                compact::position_target_index_bytes(file.referenced_data_file(), file.partition())?
            } else {
                0
            };
            let index_bytes = index_bytes.checked_add(witness_bytes).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg compact receiving index size overflow",
                )
            })?;
            if let Some(receiving) = self.rewritten_target.as_ref() {
                let existing = receiving.written.as_ref().ok_or_else(|| {
                    rewritten_target_error("Iceberg row-level conflict scope was already taken")
                })?;
                let partition = match typed.artifact() {
                    IcebergCommitArtifact::FinalDelete(effect) => Some(effect.partition()),
                    _ => typed.partition(),
                };
                receiving
                    .collector
                    .observe(existing, &mut record_partitions, partition, author)?;
            }
            let entry = CompactEntry::prepare(adapter, fragment, index_bytes, author, rewrite)?;
            counts = counts.checked_add(entry.counts())?;
            prepared.push(entry);
        }
        let retain_author = if compact.files.author.is_none() {
            Some(author.try_clone().map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
            })?)
        } else {
            None
        };
        let mut entries = Vec::with_capacity(prepared.len());
        let delta = admission.finish_record().into_owned();
        let new_targets: BTreeMap<_, _> = v2_targets
            .into_iter()
            .map(|(path, partition)| (path.to_owned(), partition.clone()))
            .collect();
        for (preflight, (_, fragment)) in prepared.iter_mut().zip(fragments) {
            match preflight.finish(adapter, &mut Some(fragment)) {
                Ok(entry) => entries.push(entry),
                Err(error) => {
                    // The failed preflight and the entries already moved still
                    // pay for the pending keys until that backing is destroyed.
                    drop((delta, new_targets));
                    return Err(error);
                }
            }
        }
        compact.indexes.apply(delta);
        if let Some(written) = self
            .rewritten_target
            .as_mut()
            .and_then(|state| state.written.as_mut())
        {
            written.apply(record_partitions);
        }
        compact.files.v2_targets.extend(new_targets);
        for entry in entries {
            compact.files.push(entry);
        }
        compact.files.counts = counts;
        if let Some(author) = retain_author {
            compact.files.author = Some(author);
        }
        Ok(())
    }

    /// A failed registration leaves the session unchanged and the proposed
    /// production proof with its caller.
    pub(crate) fn begin_query(
        &mut self,
        query: ConnectorWriteQueryIdentity,
        targets: &[WriteTargetOrdinal],
        production: &mut Option<Arc<dyn ConnectorWriteProductionProof>>,
        sealed: &[IcebergSealedWriteTarget],
        rewritten_target: Option<&ConnectorRewrittenTargetFact>,
    ) -> Result<(), ConnectorError> {
        validate_query_target_ordinals(targets)?;
        if self.rewrite_groups.is_some() && targets.len() != 1 {
            return Err(invalid(
                "Iceberg rewrite query must receive exactly one frozen group",
            ));
        }
        if targets
            .iter()
            .any(|ordinal| !sealed.iter().any(|target| target.ordinal() == *ordinal))
        {
            return Err(invalid(
                "Iceberg write query targets are outside its original sealed session",
            ));
        }
        let fact = match (&self.rewritten_target, rewritten_target) {
            (None, None) => None,
            (None, Some(_)) => {
                return Err(rewritten_target_error(
                    "Iceberg rewritten target fact requires an admitted row-level write",
                ));
            }
            (Some(expected), Some(fact)) => {
                if !self.queries.is_empty() || self.compact.is_none() {
                    return Err(rewritten_target_error(
                        "Iceberg row-level write expected one compact receiving query",
                    ));
                }
                fact.validate_owner(&expected.owner).map_err(|_| {
                    rewritten_target_error(
                        "Iceberg rewritten target fact belongs to another provider generation",
                    )
                })?;
                let fact = fact
                    .value::<IcebergRewrittenTargetFact>(expected.owner.instance_id())
                    .cloned()?;
                if fact.binding != expected.binding {
                    return Err(rewritten_target_error(
                        "Iceberg rewritten target fact differs from the admitted table, snapshot or preparation",
                    ));
                }
                Some(fact)
            }
            (Some(_), None) => {
                return Err(rewritten_target_error(
                    "Iceberg row-level write is missing its rewritten target fact",
                ));
            }
        };
        let std::collections::btree_map::Entry::Vacant(entry) = self.queries.entry(query) else {
            return Err(invalid("Iceberg write query is already registered"));
        };
        entry.insert(QueryReceiving {
            targets: targets.to_vec(),
            production: production.take(),
            rewritten_target: fact,
            confirmed: false,
        });
        Ok(())
    }

    /// Check the entire root record before transferring any fragment.
    pub(crate) fn arrive(
        &mut self,
        query: ConnectorWriteQueryIdentity,
        fragments: &mut Vec<WriteFragment>,
    ) -> Result<(), ConnectorError> {
        let registration = self
            .queries
            .get(&query)
            .ok_or_else(|| invalid("Iceberg write arrival names an unregistered query"))?;
        registration.validate_arrival(fragments)?;
        if !fragments.is_empty() {
            self.records.push(ReceivedRecord {
                query,
                fragments: std::mem::take(fragments),
            });
        }
        Ok(())
    }

    pub(crate) fn confirm(
        &mut self,
        query: ConnectorWriteQueryIdentity,
    ) -> Result<(), ConnectorError> {
        let registration = self
            .queries
            .get_mut(&query)
            .ok_or_else(|| invalid("Iceberg write confirmation names an unregistered query"))?;
        if registration.confirmed {
            return Err(invalid("Iceberg write query already completed receiving"));
        }
        registration.confirmed = true;
        Ok(())
    }

    pub(crate) fn require_complete(&self) -> Result<(), ConnectorError> {
        if self.queries.values().any(|query| !query.confirmed) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "Iceberg write session has queries that have not completed receiving",
            ));
        }
        Ok(())
    }

    /// The session's dense target set represents its exact frozen groups.
    /// Query confirmation proves a group even when it emits zero fragments.
    /// This finish-only check is separate from actual exit for partial aborts.
    pub(super) fn require_rewrite_groups_complete(&self) -> Result<(), ConnectorError> {
        let Some(expected) = self.rewrite_groups else {
            return Ok(());
        };
        if self.queries.len() != expected {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "Iceberg rewrite receiving does not cover every frozen group",
            ));
        }
        let mut seen = [false; novarocks_connector_contract::MAX_CONNECTOR_WRITE_TARGETS];
        for query in self.queries.values() {
            let [ordinal] = query.targets.as_slice() else {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Iceberg rewrite query lost its one frozen group",
                ));
            };
            let index = ordinal.get() as usize;
            if index >= expected || std::mem::replace(&mut seen[index], true) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Iceberg rewrite receiving repeats or names a foreign frozen group",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.records.is_empty()
            && self
                .compact
                .as_ref()
                .is_none_or(|compact| compact.files.is_empty())
    }

    pub(crate) fn is_unstarted(&self) -> bool {
        self.queries.is_empty()
    }

    pub(crate) fn fragments(&self) -> impl Iterator<Item = ReceivedFragment<'_>> {
        self.records.iter().flat_map(|record| {
            let production = self.queries[&record.query].production.as_deref();
            record
                .fragments
                .iter()
                .map(move |(target, fragment)| ReceivedFragment {
                    target: *target,
                    fragment,
                    production,
                })
        })
    }
}

#[cfg(test)]
mod tests;
