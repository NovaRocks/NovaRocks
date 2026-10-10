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

//! Prepare copy-on-write replacements from an immutable source graph.
//!
//! Source row IDs survive replacement; net-new MERGE outputs remain unassigned
//! until the canonical manifest-list writer allocates their publication range.

use std::collections::HashSet;

use crate::iceberg::spec::{DataContentType, FormatVersion, Operation};
use async_trait::async_trait;

use super::helpers::target_ref_snapshot_id;
use crate::commit::WrittenFile;

// `Eq` is intentionally omitted: `appended_files: Vec<WrittenFile>` and
// `WrittenFile` is `PartialEq`-only (it carries stats fields not suited to `Eq`).
#[derive(Clone, Debug, PartialEq)]
pub struct CowUpdateRewriteSet {
    pub base_snapshot_id: i64,
    pub target_table_uuid: String,
    pub updated_row_ids: Vec<i64>,
    pub touched_data_files: Vec<CowUpdateTouchedFile>,
    /// BE-written data files that are NET-NEW to this commit (e.g. a folded MERGE
    /// not-matched INSERT), not tied to any rewritten `old_file`. Added to the same
    /// Overwrite snapshot alongside the rewrite outputs. Empty for a pure UPDATE.
    pub appended_files: Vec<WrittenFile>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CowUpdateTouchedFile {
    pub old_file: String,
    pub new_files: Vec<String>,
    pub row_ids: Vec<i64>,
}

fn to_iceberg_data_invalid(s: String) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, s)
}

/// Copy-on-write preparation uses the frozen source graph; ref stability is a dependency.
pub(crate) struct CowUpdatePreparer {
    pub rewrite: CowUpdateRewriteSet,
}

#[async_trait]
impl crate::commit::staging::Preparer for CowUpdatePreparer {
    async fn prepare(
        &self,
        view: &crate::commit::staging::StagedView<'_>,
        intent: &crate::commit::model::OperationIntent,
    ) -> crate::iceberg::Result<crate::commit::staging::PreparedChange> {
        use super::row_delta_dv_metadata::write_added_entry_groups;
        use crate::commit::model::{Dependency, EntryIdentity, SeqField};
        if view.metadata().format_version() != FormatVersion::V3 {
            return Err(to_iceberg_data_invalid(
                "Copy-on-write row updates require an Iceberg V3 table".into(),
            ));
        }
        if !intent.dependencies().contains(&Dependency::RefUnchanged)
            || intent.start().map(|s| s.snapshot_id) != Some(self.rewrite.base_snapshot_id)
            || self.rewrite.target_table_uuid != view.metadata().uuid().to_string()
        {
            return Err(to_iceberg_data_invalid(
                "Copy-on-write requires an exact frozen source and RefUnchanged dependency".into(),
            ));
        }
        if self.rewrite.touched_data_files.is_empty() || self.rewrite.updated_row_ids.is_empty() {
            return Err(to_iceberg_data_invalid(
                "Copy-on-write requires touched data files and updated row IDs".into(),
            ));
        }
        let mut old = HashSet::new();
        let mut new = HashSet::new();
        let mut touched_rows = HashSet::new();
        for touched in &self.rewrite.touched_data_files {
            if !old.insert(touched.old_file.clone())
                || touched.new_files.is_empty()
                || touched.row_ids.is_empty()
            {
                return Err(to_iceberg_data_invalid(
                    "Copy-on-write source files must be unique with replacement files and row IDs"
                        .into(),
                ));
            }
            for row in &touched.row_ids {
                if *row < 0 || !touched_rows.insert(*row) {
                    return Err(to_iceberg_data_invalid(
                        "Copy-on-write touched row IDs must be unique and nonnegative".into(),
                    ));
                }
            }
            for path in &touched.new_files {
                if !new.insert(path.clone()) {
                    return Err(to_iceberg_data_invalid(
                        "Duplicate copy-on-write replacement path".into(),
                    ));
                }
            }
        }
        let updated = self
            .rewrite
            .updated_row_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if updated.len() != self.rewrite.updated_row_ids.len() || !updated.is_subset(&touched_rows)
        {
            return Err(to_iceberg_data_invalid(
                "Copy-on-write updated row IDs are not a unique subset of the touched source rows"
                    .into(),
            ));
        }
        let appended = self
            .rewrite
            .appended_files
            .iter()
            .map(|f| f.path.clone())
            .collect::<HashSet<_>>();
        if appended.len() != self.rewrite.appended_files.len() || !appended.is_disjoint(&new) {
            return Err(to_iceberg_data_invalid(
                "Copy-on-write append paths must be unique and distinct from replacement paths"
                    .into(),
            ));
        }
        let parent = target_ref_snapshot_id(view.metadata(), view.target_ref());
        let mut inputs = crate::commit::dependency::ValidationInputs::new(
            view.metadata(),
            parent,
            view.artifacts(),
        );
        let live = inputs.live_set().await?;
        let removed = intent
            .changes()
            .removed
            .iter()
            .map(|e| e.identity().clone())
            .collect::<HashSet<_>>();
        let mut expected = old
            .iter()
            .cloned()
            .map(|path| EntryIdentity::DataFile { path })
            .collect::<HashSet<_>>();
        expected.extend(
            live.keys()
                .filter(|id| {
                    matches!(id,
                        EntryIdentity::DeletionVector { referenced_data_file, .. }
                            if old.contains(referenced_data_file)
                    )
                })
                .cloned(),
        );
        if removed.len() != intent.changes().removed.len() || removed != expected {
            return Err(to_iceberg_data_invalid(
                "Copy-on-write removed entries must equal touched data files and their deletion vectors".into(),
            ));
        }
        for frozen in &intent.changes().removed {
            if live
                .get(frozen.identity())
                .is_none_or(|e| e.frozen != *frozen)
            {
                return Err(to_iceberg_data_invalid(
                    "Copy-on-write source no longer matches its frozen entry facts".into(),
                ));
            }
        }
        let mut seen = HashSet::new();
        for added in &intent.changes().added {
            let file = added.file();
            if file.content_type() != DataContentType::Data
                || added.data_sequence() != SeqField::Inherit
                || !seen.insert(file.file_path().to_owned())
                || live.contains_key(&EntryIdentity::try_from(file)?)
            {
                return Err(to_iceberg_data_invalid("Copy-on-write additions must be unique new data files inheriting commit sequence".into()));
            }
            if appended.contains(file.file_path()) {
                if file.first_row_id().is_some() {
                    return Err(to_iceberg_data_invalid(
                        "Net-new copy-on-write rows must leave first-row-id unassigned".into(),
                    ));
                }
            } else if new.contains(file.file_path()) {
                let source = self
                    .rewrite
                    .touched_data_files
                    .iter()
                    .find(|t| t.new_files.iter().any(|p| p == file.file_path()))
                    .expect("declared replacement");
                let minimum = source.row_ids.iter().copied().min();
                if file.first_row_id() != minimum {
                    return Err(to_iceberg_data_invalid(
                        "Copy-on-write replacement must freeze its actual source row-ID minimum"
                            .into(),
                    ));
                }
            } else {
                return Err(to_iceberg_data_invalid(
                    "Undeclared copy-on-write output file".into(),
                ));
            }
        }
        if seen != new.union(&appended).cloned().collect() {
            return Err(to_iceberg_data_invalid(
                "Copy-on-write frozen additions omit a declared output file".into(),
            ));
        }
        let removed_entries = removed
            .iter()
            .map(|id| live[id].clone())
            .collect::<Vec<_>>();
        let mut summary =
            super::overwrite::snapshot_file_summary(&intent.changes().added, &removed_entries)?;
        super::overwrite::set_visible_rows_after_removal(view, &removed_entries, &mut summary)?;
        let snapshot_id = crate::commit::staging::new_snapshot_id(view.metadata());
        let mut manifests = super::overwrite::write_live_entry_groups(
            view,
            snapshot_id,
            live.iter()
                .map(|(id, e)| (e.clone(), removed.contains(id)))
                .collect(),
        )
        .await?;
        manifests.extend(
            write_added_entry_groups(view, snapshot_id, intent.changes().added.clone()).await?,
        );
        super::overwrite::prepare_snapshot_change(
            view,
            intent,
            snapshot_id,
            Operation::Overwrite,
            manifests,
            summary,
            false,
        )
        .await
    }
}
