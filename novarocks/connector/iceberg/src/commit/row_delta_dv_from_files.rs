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

//! Prepare immutable v3 row mutations while preserving full logical DV identity.
//!
//! Multiple new blobs for one referenced data file are coalesced under the
//! attempt owner. Physical superseded-object cleanup belongs to finalization.

use std::collections::{BTreeMap, HashSet};

use crate::iceberg::spec::{FormatVersion, Operation};
use async_trait::async_trait;

use super::helpers::{snapshot_total_records, target_ref_snapshot_id};
use super::row_delta_dv_metadata::{WrittenDvFile, dv_total_records, to_iceberg_unexpected};
use crate::commit::{
    DeletionVector, read_deletion_vector_puffin, write_single_deletion_vector_puffin,
};

/// MoR preparation preserves full logical DV identity across a shared Puffin object.
pub(crate) struct RowDeltaDvFromFilesPreparer;

#[async_trait]
impl crate::commit::staging::Preparer for RowDeltaDvFromFilesPreparer {
    async fn prepare(
        &self,
        view: &crate::commit::staging::StagedView<'_>,
        intent: &crate::commit::model::OperationIntent,
    ) -> crate::iceberg::Result<crate::commit::staging::PreparedChange> {
        use super::row_delta_dv_metadata::{
            RowLiveIndex, logical_file_size, write_added_entry_groups,
        };
        use crate::commit::model::{EntryIdentity, SeqField};
        if view.metadata().format_version() != FormatVersion::V3 {
            return Err(to_iceberg_unexpected(
                "Deletion vectors require an Iceberg V3 table".into(),
            ));
        }
        if intent.changes().added.is_empty() {
            return Ok(crate::commit::staging::PreparedChange {
                updates: vec![],
                requirements: vec![],
            });
        }
        let index = RowLiveIndex::load(view).await?;
        let mut data = Vec::new();
        let mut dvs = Vec::new();
        let mut identities = HashSet::new();
        for added in &intent.changes().added {
            if added.data_sequence() != SeqField::Inherit {
                return Err(to_iceberg_unexpected(
                    "MoR additions must inherit their actual commit sequence".into(),
                ));
            }
            let identity = EntryIdentity::try_from(added.file())?;
            if !identities.insert(identity.clone()) || index.live.contains_key(&identity) {
                return Err(to_iceberg_unexpected(format!(
                    "Duplicate or already-live MoR addition {identity:?}"
                )));
            }
            match identity {
                EntryIdentity::DataFile { .. } => {
                    if added.file().first_row_id().is_some() {
                        return Err(to_iceberg_unexpected("MoR output can contain new rows and must leave first-row-id unassigned".into()));
                    }
                    data.push(added.clone());
                }
                EntryIdentity::DeletionVector {
                    referenced_data_file,
                    ..
                } => {
                    let source = index
                        .live
                        .get(&EntryIdentity::DataFile {
                            path: referenced_data_file.clone(),
                        })
                        .ok_or_else(|| {
                            to_iceberg_unexpected(format!(
                                "Deletion vector references absent data file {referenced_data_file}"
                            ))
                        })?;
                    if added.file().partition() != source.file.partition()
                        || added.partition_spec_id() != source.frozen.facts().partition_spec_id
                    {
                        return Err(to_iceberg_unexpected(
                            "Deletion-vector partition does not match referenced data file".into(),
                        ));
                    }
                    dvs.push(added.clone());
                }
                _ => {
                    return Err(to_iceberg_unexpected(
                        "MoR row-lineage output requires Data or Puffin deletion vectors".into(),
                    ));
                }
            }
        }
        let touched = dvs
            .iter()
            .map(|a| a.file().referenced_data_file().expect("validated DV"))
            .collect::<HashSet<_>>();
        let replaced = touched
            .iter()
            .filter_map(|p| index.dvs_by_data.get(p).cloned())
            .collect::<HashSet<_>>();
        let frozen_removed = intent
            .changes()
            .removed
            .iter()
            .map(|e| e.identity().clone())
            .collect::<HashSet<_>>();
        if frozen_removed.len() != intent.changes().removed.len() || frozen_removed != replaced {
            return Err(to_iceberg_unexpected(
                "MoR removed DV identities do not match the exact touched live set".into(),
            ));
        }
        for frozen in &intent.changes().removed {
            if index
                .live
                .get(frozen.identity())
                .is_none_or(|entry| entry.frozen != *frozen)
            {
                return Err(to_iceberg_unexpected(
                    "Removed deletion vector no longer matches its frozen source facts".into(),
                ));
            }
        }
        let dvs = coalesce_intent_dvs(view, &index, dvs).await?;
        let new_dv_records = dvs.iter().try_fold(0u64, |sum, a| {
            sum.checked_add(a.file().record_count())
                .ok_or_else(|| to_iceberg_unexpected("DV cardinality overflow".into()))
        })?;
        let removed_records = replaced.iter().try_fold(0u64, |sum, id| {
            sum.checked_add(index.live[id].file.record_count())
                .ok_or_else(|| to_iceberg_unexpected("Removed DV cardinality overflow".into()))
        })?;
        let newly_deleted = new_dv_records.checked_sub(removed_records).ok_or_else(|| {
            to_iceberg_unexpected(
                "Replacement deletion vector lost source deleted positions".into(),
            )
        })?;
        let data_records = data.iter().try_fold(0u64, |sum, a| {
            sum.checked_add(a.file().record_count())
                .ok_or_else(|| to_iceberg_unexpected("MoR data count overflow".into()))
        })?;
        let parent = target_ref_snapshot_id(view.metadata(), view.target_ref());
        let total = dv_total_records(
            snapshot_total_records(view.metadata(), parent).map_err(to_iceberg_unexpected)?,
            newly_deleted,
            data_records,
        )
        .map_err(to_iceberg_unexpected)?;
        let mut summary = std::collections::HashMap::new();
        if !dvs.is_empty() {
            summary.insert("added-delete-files".into(), dvs.len().to_string());
            summary.insert("added-position-delete-files".into(), dvs.len().to_string());
            summary.insert("added-position-deletes".into(), new_dv_records.to_string());
            summary.insert("deleted-records".into(), newly_deleted.to_string());
        }
        if !data.is_empty() {
            summary.insert("added-data-files".into(), data.len().to_string());
            summary.insert("added-records".into(), data_records.to_string());
        }
        if !replaced.is_empty() {
            summary.insert("removed-delete-files".into(), replaced.len().to_string());
            summary.insert(
                "removed-position-delete-files".into(),
                replaced.len().to_string(),
            );
            summary.insert(
                "removed-position-deletes".into(),
                removed_records.to_string(),
            );
            let removed_size = replaced.iter().try_fold(0u64, |sum, id| {
                sum.checked_add(logical_file_size(&index.live[id].file)?)
                    .ok_or_else(|| to_iceberg_unexpected("Removed DV size overflow".into()))
            })?;
            summary.insert("removed-files-size".into(), removed_size.to_string());
        }
        let added_size = dvs.iter().chain(&data).try_fold(0u64, |sum, a| {
            sum.checked_add(logical_file_size(a.file())?)
                .ok_or_else(|| to_iceberg_unexpected("MoR added size overflow".into()))
        })?;
        summary.insert("added-files-size".into(), added_size.to_string());
        if let Some(total) = total {
            summary.insert("total-records".into(), total.to_string());
        }
        let snapshot_id = crate::commit::staging::new_snapshot_id(view.metadata());
        let mut manifests = super::overwrite::write_live_entry_groups(
            view,
            snapshot_id,
            index
                .live
                .into_iter()
                .map(|(identity, live)| (live, replaced.contains(&identity)))
                .collect(),
        )
        .await?;
        let operation = if data.is_empty() {
            Operation::Delete
        } else if dvs.is_empty() {
            Operation::Append
        } else {
            Operation::Overwrite
        };
        manifests.extend(
            write_added_entry_groups(view, snapshot_id, dvs.into_iter().chain(data)).await?,
        );
        // The preparer never deletes a superseded BE object: another logical DV
        // may still use the same physical Puffin. Finalization owns reachability.
        super::overwrite::prepare_snapshot_change(
            view,
            intent,
            snapshot_id,
            operation,
            manifests,
            summary,
            false,
        )
        .await
    }
}

async fn coalesce_intent_dvs(
    view: &crate::commit::staging::StagedView<'_>,
    index: &super::row_delta_dv_metadata::RowLiveIndex,
    dvs: Vec<crate::commit::model::AddedContent>,
) -> crate::iceberg::Result<Vec<crate::commit::model::AddedContent>> {
    use crate::commit::model::{AddedContent, ArtifactClass, ArtifactKind, EntryIdentity};
    let mut groups: BTreeMap<String, Vec<AddedContent>> = BTreeMap::new();
    for added in dvs {
        groups
            .entry(added.file().referenced_data_file().expect("validated DV"))
            .or_default()
            .push(added);
    }
    let mut out = Vec::new();
    for (referenced, group) in groups {
        if group.len() == 1 {
            out.push(group.into_iter().next().expect("one DV"));
            continue;
        }
        let mut merged = DeletionVector::new();
        for added in group {
            view.artifacts().check_active()?;
            let file = added.file();
            merged.merge(
                &read_deletion_vector_puffin(
                    view.artifacts().file_io(),
                    file.file_path(),
                    file.content_offset().expect("validated DV"),
                    file.content_size_in_bytes().expect("validated DV"),
                )
                .await
                .map_err(|e| to_iceberg_unexpected(format!("Read coalesced DV failed: {e}")))?,
            );
        }
        let object = view
            .artifacts()
            .allocate(ArtifactClass::Attempt, ArtifactKind::DeletionVector)?;
        let written = write_single_deletion_vector_puffin(
            view.artifacts().file_io(),
            object.path(),
            &referenced,
            &merged,
        )
        .await
        .map_err(|e| to_iceberg_unexpected(format!("Write coalesced DV failed: {e}")))?;
        let source = &index.live[&EntryIdentity::DataFile { path: referenced }];
        let facts = source.frozen.facts();
        let live = super::row_delta_dv_metadata::LiveFile {
            data_file: source.file.clone(),
            partition_spec_id: facts.partition_spec_id,
            snapshot_id: facts
                .added_snapshot_id
                .ok_or_else(|| to_iceberg_unexpected("Data source has no added snapshot".into()))?,
            sequence_number: facts.data_sequence.ok_or_else(|| {
                to_iceberg_unexpected("Data source has no assigned sequence".into())
            })?,
            file_sequence_number: facts.file_sequence,
        };
        out.push(AddedContent::new_logical_data(
            super::row_delta_dv_metadata::dv_data_file(&WrittenDvFile::from(written), &live)
                .map_err(to_iceberg_unexpected)?,
            facts.partition_spec_id,
        )?);
    }
    Ok(out)
}
