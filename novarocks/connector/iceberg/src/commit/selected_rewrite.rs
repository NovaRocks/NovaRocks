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

//! One-snapshot replacement of a provider-frozen Iceberg file set.
//!
//! Resolve the original selected entries on the current publication base and
//! consume new files from their operation-owned compact stage.

#[cfg(test)]
use std::collections::BTreeSet;

use crate::commit::model::EntryIdentity;
use async_trait::async_trait;

#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(crate) struct SelectedRewriteFiles {
    pub(crate) data_paths: BTreeSet<EntryIdentity>,
    pub(crate) delete_paths: BTreeSet<EntryIdentity>,
}

#[cfg(test)]
impl SelectedRewriteFiles {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.data_paths.is_empty() {
            return Err("selected rewrite file set is empty".to_string());
        }
        if self.data_paths.iter().any(|path| path.validate().is_err())
            || self
                .delete_paths
                .iter()
                .any(|path| path.validate().is_err())
            || !self.data_paths.is_disjoint(&self.delete_paths)
        {
            return Err("selected rewrite file set is invalid".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) struct SelectedRewritePreparer {
    pub(crate) files: SelectedRewriteFiles,
}

#[cfg(test)]
#[async_trait]
impl super::staging::Preparer for SelectedRewritePreparer {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        self.files.validate().map_err(selected_invalid)?;
        let (sources, live) = view.observe_live_sources().await?;
        let live_data = live
            .keys()
            .filter(|id| matches!(id, EntryIdentity::DataFile { .. }))
            .cloned()
            .collect::<BTreeSet<_>>();
        let live_deletes = live
            .keys()
            .filter(|id| !matches!(id, EntryIdentity::DataFile { .. }))
            .cloned()
            .collect::<BTreeSet<_>>();
        if !self.files.data_paths.is_subset(&live_data)
            || !self.files.delete_paths.is_subset(&live_deletes)
        {
            return Err(selected_invalid(
                "Selected rewrite frozen logical set is no longer live at the target ref",
            ));
        }
        let removed = self
            .files
            .data_paths
            .union(&self.files.delete_paths)
            .cloned()
            .collect();
        let intent_removed = intent
            .changes()
            .removed
            .iter()
            .map(|entry| entry.identity().clone())
            .collect::<BTreeSet<_>>();
        if intent_removed != removed {
            return Err(selected_invalid(
                "Selected rewrite intent differs from its frozen removal identity set",
            ));
        }
        super::rewrite_data_files::prepare_physical_rewrite(
            view,
            intent,
            (sources.as_ref(), &live),
            &removed,
        )
        .await
    }
}

#[cfg(test)]
fn selected_invalid(message: impl Into<String>) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, message)
}

pub(crate) struct CompactRewritePreparer {
    pub(super) operation: super::operation::IcebergCommitOperation,
    pub(super) stage: usize,
}

#[async_trait]
impl super::staging::Preparer for CompactRewritePreparer {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        use super::model::{CommitOperation, CommitProtocol};
        use crate::iceberg::spec::Operation;

        let CommitProtocol::RewriteFiles { replaced, .. } = intent.protocol() else {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::Unexpected,
                "Compact rewrite preparer requires its original RewriteFiles intent",
            ));
        };
        let validation = |detail: String| {
            crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, detail.clone())
                .with_source(super::commit_failure::validation_failure(
                    CommitOperation::Optimize,
                    detail,
                ))
        };
        let current = view
            .metadata()
            .snapshot_for_ref(view.target_ref())
            .ok_or_else(|| {
                let path = replaced.first().map_or("", |entry| entry.identity().path());
                super::base_removal::missing_required_file(CommitOperation::Optimize, path)
            })?;
        let snapshot_id = current.snapshot_id();
        let mut scope = crate::manifest::target_query::ManifestReadScope::new(
            view.metadata_ref(),
            snapshot_id,
        )?
        .with_owned_request(self.operation.request_context());
        let removed = super::base_removal::locate_on_base(
            &mut scope,
            replaced.iter(),
            CommitOperation::Optimize,
            view.artifacts().file_io(),
            || view.artifacts().check_active(),
        )
        .await?;
        let sources = scope.snapshot_sources()?;
        let removed = removed.iter().collect::<Vec<_>>();
        let snapshot = view.stage_snapshot_id()?;
        let (added, counts) = super::new_manifests::prepare_added_manifests(
            &self.operation,
            view,
            self.stage,
            snapshot,
            None,
        )
        .await?;
        let mut summary = super::overwrite::snapshot_file_summary(&[], &removed)?;
        summary.extend(super::overwrite::compact_append_summary(counts));
        let removed_rows = replaced
            .iter()
            .filter(|entry| matches!(entry.identity(), EntryIdentity::DataFile { .. }))
            .try_fold(0u64, |sum, entry| {
                sum.checked_add(entry.facts().record_count).ok_or_else(|| {
                    validation("Replaced Iceberg record count is unrepresentable".to_string())
                })
            })?;
        if counts.data.rows > removed_rows {
            return Err(validation(format!(
                "Invalid REPLACE operation: {} added records > {removed_rows} replaced records",
                counts.data.rows,
            )));
        }
        super::overwrite::set_physical_rows_after_removal(view, &mut summary)?;
        let manifests = super::manifest_transform::ordinary_manifests_scoped(
            view, snapshot, &sources, &removed, added, &scope, true,
        )
        .await?;
        super::overwrite::prepare_ordinary_snapshot_change(
            view,
            intent,
            snapshot,
            Operation::Replace,
            manifests,
            summary,
            false,
        )
        .await
    }
}

#[cfg(test)]
#[path = "selected_rewrite/tests.rs"]
mod preparer_tests;
