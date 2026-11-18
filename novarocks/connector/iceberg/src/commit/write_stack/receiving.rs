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

pub(crate) mod validation;

use std::collections::{BTreeMap, LinkedList};
use std::sync::Arc;

use novarocks_spi::connector::write_stack::{
    ConnectorCommitFragment, ConnectorWriteProductionProof, ConnectorWriteQueryIdentity,
    ConnectorWriterPhysicalContext, WriteTargetOrdinal, validate_query_target_ordinals,
    validate_write_fragment_targets,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPayloadRetentionGuard,
};

use super::domain::{
    IcebergSealedWriteTarget, IcebergWriteCommitSession, IcebergWriteFlavor, invalid,
};
use super::runtime::IcebergWriteAdapter;
use compact::{AddedCounters, CompactEntry};
use validation::{FragmentAdmission, StatementIndexes};

#[allow(
    dead_code,
    reason = "Later compact flavors consume the retained delete effects in RWM-16 P09"
)]
pub(crate) mod compact;

type WriteFragment = (WriteTargetOrdinal, ConnectorCommitFragment);

/// Provider-owned receiving state for the frozen write flavor.
#[derive(Debug, Default)]
pub(crate) struct IcebergReceiving {
    queries: BTreeMap<ConnectorWriteQueryIdentity, QueryReceiving>,
    records: Vec<ReceivedRecord>,
    compact: Option<CompactReceiving>,
}

#[derive(Default)]
struct CompactReceiving {
    indexes: StatementIndexes,
    files: ReceivedCompactFiles,
}

#[derive(Default)]
pub(crate) struct ReceivedCompactFiles {
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

impl IcebergReceiving {
    pub(crate) fn for_flavor(flavor: IcebergWriteFlavor) -> Self {
        Self {
            compact: (flavor == IcebergWriteFlavor::Append).then(CompactReceiving::default),
            ..Self::default()
        }
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
        let mut admission = FragmentAdmission::for_record(handle, &compact.indexes);
        let mut counts = compact.files.counts;
        let mut prepared = Vec::with_capacity(fragments.len());
        for (target, fragment) in &fragments {
            let typed = adapter.commit_fragment(fragment)?;
            let index_bytes = admission.admit_record_fragment(
                *target,
                typed,
                |physical| match registration.production.as_deref() {
                    Some(production) => production.admits(*target, physical),
                    None => Ok(false),
                },
                None,
            )?;
            let entry = CompactEntry::prepare(adapter, fragment, index_bytes, author)?;
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
        for (preflight, (_, fragment)) in prepared.iter_mut().zip(fragments) {
            match preflight.finish(adapter, &mut Some(fragment)) {
                Ok(entry) => entries.push(entry),
                Err(error) => {
                    // The failed preflight and the entries already moved still
                    // pay for the pending keys until that backing is destroyed.
                    drop(delta);
                    return Err(error);
                }
            }
        }
        compact.indexes.apply(delta);
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
    ) -> Result<(), ConnectorError> {
        validate_query_target_ordinals(targets)?;
        if targets
            .iter()
            .any(|ordinal| !sealed.iter().any(|target| target.ordinal() == *ordinal))
        {
            return Err(invalid(
                "Iceberg write query targets are outside its original sealed session",
            ));
        }
        let std::collections::btree_map::Entry::Vacant(entry) = self.queries.entry(query) else {
            return Err(invalid("Iceberg write query is already registered"));
        };
        entry.insert(QueryReceiving {
            targets: targets.to_vec(),
            production: production.take(),
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
