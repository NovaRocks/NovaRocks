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

#[cfg(test)]
use crate::iceberg::expr::Predicate;
#[cfg(test)]
use crate::iceberg::spec::TableMetadata;
use crate::iceberg::spec::{DataContentType, ManifestFile};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::target_query::ManifestReadScope;

use super::conflict::CandidateFile;
use super::delete_condition::IcebergDeleteConditionFact;
use super::manifest_sources::{LocatedEntry, duplicate_identity};
use super::model::OperationIntent;
use super::staging::{PreparedChange, Preparer, StagedView};
use condition::ConditionJudge;

mod condition;
mod metrics;
mod projection;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FileVerdict {
    All,
    Partial,
    None,
}

#[cfg(test)]
trait RowFilterJudge: Send + Sync {
    fn manifest_may_match(
        &self,
        filter: &Predicate,
        metadata: &TableMetadata,
        manifest: &ManifestFile,
    ) -> bool;

    fn file_verdict(&self, filter: &Predicate, file: &CandidateFile<'_>) -> FileVerdict;
}

enum Selection {
    AllRows,
    Condition(Arc<IcebergDeleteConditionFact>),
    #[cfg(test)]
    Scripted(Predicate, Arc<dyn RowFilterJudge>),
}

// Design: ADR-0251 (docs/adr/ADR-0251-iceberg-maintenance-commits-by-operation.md)
/// Selection is always derived from B. The operation intent owns the protocol label.
pub(crate) struct FilterDeletePreparer {
    selection: Selection,
    full_refresh: bool,
}

impl FilterDeletePreparer {
    #[cfg(test)]
    fn new(filter: Predicate, judge: Arc<dyn RowFilterJudge>) -> Self {
        Self {
            selection: Selection::Scripted(filter, judge),
            full_refresh: false,
        }
    }

    pub(crate) fn always_true() -> Self {
        Self {
            selection: Selection::AllRows,
            full_refresh: false,
        }
    }

    pub(crate) fn for_condition(fact: Arc<IcebergDeleteConditionFact>) -> Self {
        Self {
            selection: Selection::Condition(fact),
            full_refresh: false,
        }
    }

    fn manifest_may_match(
        &self,
        _metadata: &crate::iceberg::spec::TableMetadata,
        prepared: Option<&ConditionJudge<'_>>,
        manifest: &ManifestFile,
    ) -> bool {
        #[cfg(test)]
        if let Selection::Scripted(filter, judge) = &self.selection {
            return judge.manifest_may_match(filter, _metadata, manifest);
        }
        prepared.is_none_or(|judge| judge.manifest_may_match(manifest))
    }

    fn file_verdict(
        &self,
        prepared: Option<&ConditionJudge<'_>>,
        file: &CandidateFile<'_>,
    ) -> Result<FileVerdict> {
        #[cfg(test)]
        if let Selection::Scripted(filter, judge) = &self.selection {
            return Ok(judge.file_verdict(filter, file));
        }
        prepared.map_or(Ok(FileVerdict::All), |judge| judge.file_verdict(file))
    }

    fn filter_description(&self) -> String {
        match &self.selection {
            Selection::AllRows => "true".to_owned(),
            Selection::Condition(fact) => condition::describe(&fact.condition),
            #[cfg(test)]
            Selection::Scripted(filter, _) => filter.to_string(),
        }
    }

    pub(crate) fn for_full_refresh() -> Self {
        Self {
            full_refresh: true,
            ..Self::always_true()
        }
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
        if (!self.full_refresh && !intent.changes().added.is_empty())
            || !intent.changes().removed.is_empty()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Filtered delete cannot include explicit file changes",
            ));
        }

        let artifacts = view.artifacts();
        let active = || artifacts.check_active();
        active()?;
        let operation = intent.protocol().operation();
        let mut prepared = match &self.selection {
            Selection::Condition(fact) => {
                Some(ConditionJudge::for_base(fact, metadata, operation)?)
            }
            _ => None,
        };
        let sources = view.target_sources().await?;
        let mut removals = BTreeMap::new();
        let mut scope = None;
        if let Some(sources) = &sources {
            if let Some(judge) = &mut prepared {
                judge.prepare_specs(
                    metadata,
                    sources
                        .sources()
                        .map(|source| source.descriptor.partition_spec_id),
                    &active,
                )?;
            }
            sources
                .fold_indexed(
                    artifacts.file_io(),
                    active,
                    |manifest| self.manifest_may_match(metadata, prepared.as_ref(), manifest),
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
                        match self.file_verdict(prepared.as_ref(), &candidate)? {
                            FileVerdict::All => {
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
                            FileVerdict::Partial if file.content_type() == DataContentType::Data => {
                                return Err(super::new_manifests::commit_validation_error_for(operation, format!(
                                    "Cannot delete file where some, but not all, rows match filter {}: {}",
                                    self.filter_description(),
                                    file.file_path(),
                                )));
                            }
                            FileVerdict::Partial | FileVerdict::None => {}
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
            self.full_refresh,
        )
        .await
    }
}

#[cfg(test)]
mod tests;
