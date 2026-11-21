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

//! Provider-owned metadata deletion evaluated on each attempt's branch head.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::iceberg::expr::Predicate;
use crate::iceberg::spec::{DataContentType, ManifestFile, TableMetadata};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::target_query::ManifestReadScope;

use super::conflict::CandidateFile;
use super::manifest_sources::{LocatedEntry, duplicate_identity};
use super::model::OperationIntent;
use super::staging::{PreparedChange, Preparer, StagedView};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FileVerdict {
    AllRows,
    SomeRows,
    NoRows,
}

/// One judge evaluates the same frozen row predicate at two levels.
pub(crate) trait RowFilterJudge: Send + Sync {
    /// An inclusive test: false is proof that the manifest cannot match.
    fn manifest_may_match(
        &self,
        filter: &Predicate,
        metadata: &TableMetadata,
        manifest: &ManifestFile,
    ) -> bool;

    fn file_verdict(&self, filter: &Predicate, file: &CandidateFile<'_>) -> FileVerdict;
}

pub(crate) struct AlwaysTrue;

impl RowFilterJudge for AlwaysTrue {
    fn manifest_may_match(
        &self,
        _filter: &Predicate,
        _metadata: &TableMetadata,
        _manifest: &ManifestFile,
    ) -> bool {
        true
    }

    fn file_verdict(&self, _filter: &Predicate, _file: &CandidateFile<'_>) -> FileVerdict {
        FileVerdict::AllRows
    }
}

/// Selection is always derived from B. The operation intent owns the protocol label.
pub(crate) struct FilterDeletePreparer {
    filter: Predicate,
    judge: Arc<dyn RowFilterJudge>,
}

impl FilterDeletePreparer {
    pub(crate) fn new(filter: Predicate, judge: Arc<dyn RowFilterJudge>) -> Self {
        Self { filter, judge }
    }

    pub(crate) fn always_true() -> Self {
        Self::new(Predicate::AlwaysTrue, Arc::new(AlwaysTrue))
    }
}

#[async_trait]
impl Preparer for FilterDeletePreparer {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        intent: &OperationIntent,
    ) -> Result<PreparedChange> {
        let metadata = view.metadata();
        if !intent.changes().added.is_empty() || !intent.changes().removed.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Filtered delete cannot include explicit file changes",
            ));
        }

        let artifacts = view.artifacts();
        let active = || artifacts.check_active();
        let sources = view.target_sources().await?;
        let mut removals = BTreeMap::new();
        let mut scope = None;
        if let Some(sources) = &sources {
            sources
                .fold_indexed(
                    artifacts.file_io(),
                    active,
                    |manifest| self.judge.manifest_may_match(&self.filter, metadata, manifest),
                    |index, entry| {
                        if !entry.raw.is_alive() {
                            return Ok(());
                        }
                        let file = entry.raw.data_file();
                        let candidate = CandidateFile {
                            metadata,
                            spec_id: entry.source.descriptor.partition_spec_id,
                            file,
                        };
                        match self.judge.file_verdict(&self.filter, &candidate) {
                            FileVerdict::AllRows => {
                                let entry = LocatedEntry::from_bound(index, &entry)?;
                                let identity = entry.frozen.identity().clone();
                                match removals.entry(identity) {
                                    std::collections::btree_map::Entry::Vacant(slot) => {
                                        slot.insert(entry);
                                    }
                                    std::collections::btree_map::Entry::Occupied(slot) => {
                                        return Err(duplicate_identity(slot.key()));
                                    }
                                }
                            }
                            FileVerdict::SomeRows if file.content_type() == DataContentType::Data => {
                                return Err(super::new_manifests::commit_validation_error(format!(
                                    "Cannot delete file where some, but not all, rows match filter {}: {}",
                                    self.filter,
                                    file.file_path(),
                                )));
                            }
                            FileVerdict::SomeRows | FileVerdict::NoRows => {}
                        }
                        Ok(())
                    },
                )
                .await?;
            if removals
                .values()
                .any(|entry| entry.content_type == DataContentType::Data)
                && sources.sources().any(|source| {
                    source.descriptor.content == crate::iceberg::spec::ManifestContentType::Deletes
                })
            {
                let parent = metadata
                    .snapshot_for_ref(view.target_ref())
                    .expect("loaded target");
                let mut base = ManifestReadScope::new(view.metadata_ref(), parent.snapshot_id())?;
                super::base_removal::include_dangling_on_base(
                    &mut base,
                    &mut removals,
                    artifacts.file_io(),
                    active,
                )
                .await?;
                scope = Some(base);
            }
        }

        let snapshot_id = view.stage_snapshot_id()?;
        let removed: Vec<_> = removals.values().collect();
        let manifests = super::manifest_transform::filtered_manifests(
            view,
            snapshot_id,
            sources.as_ref(),
            &removed,
            scope.as_ref(),
        )
        .await?;
        super::overwrite::prepare_filtered_delete_snapshot_change(
            view,
            intent,
            snapshot_id,
            manifests,
            &removed,
        )
        .await
    }
}

#[cfg(test)]
mod tests;
