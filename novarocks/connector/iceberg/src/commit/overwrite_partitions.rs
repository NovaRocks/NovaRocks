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

//! Dynamic partition replacement preserves untouched logical entries.
//!
//! The touched set uses output partitions bound to the staged default spec.
//! Historical-spec matching remains unsupported and fails before publication.
//! Existing assigned row IDs are carried; unassigned historical rows obtain
//! their first range only through the successful manifest-list write.

use std::collections::HashSet;

use crate::iceberg::spec::{FormatVersion, Operation};
use async_trait::async_trait;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PartitionMatch {
    InSet,
    NotInSet,
    DifferentSpec,
}

fn partition_match_in_touched(
    base: &crate::iceberg::spec::Struct,
    base_spec_id: i32,
    current_spec_id: i32,
    touched: &[crate::iceberg::spec::Struct],
) -> PartitionMatch {
    if base_spec_id != current_spec_id {
        PartitionMatch::DifferentSpec
    } else if touched.iter().any(|candidate| candidate == base) {
        PartitionMatch::InSet
    } else {
        PartitionMatch::NotInSet
    }
}

/// Dynamic partition replacement preserves untouched logical identities.
pub(crate) struct OverwritePartitionsPreparer;

#[async_trait]
impl super::staging::Preparer for OverwritePartitionsPreparer {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        super::overwrite::validate_added_data(intent)?;
        let metadata = view.metadata();
        if metadata.format_version() != FormatVersion::V3 {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "Overwrite partitions requires a V3 table",
            ));
        }
        if intent
            .changes()
            .added
            .iter()
            .any(|a| a.partition_spec_id() != metadata.default_partition_spec_id())
        {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "Overwrite partitions output must use the staged default partition spec",
            ));
        }
        let (sources, live) = view.observe_live_sources().await?;
        let touched: Vec<_> = intent
            .changes()
            .added
            .iter()
            .map(|a| a.file().partition().clone())
            .collect();
        let mut removed = Vec::new();
        for entry in live.values() {
            let deleted = match partition_match_in_touched(
                &entry.frozen.facts().partition,
                entry.frozen.facts().partition_spec_id,
                metadata.default_partition_spec_id(),
                &touched,
            ) {
                PartitionMatch::InSet => true,
                PartitionMatch::NotInSet => false,
                PartitionMatch::DifferentSpec => {
                    return Err(crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Overwrite partitions cannot match a historical partition spec; consolidate first",
                    ));
                }
            };
            if deleted {
                removed.push(entry);
            }
        }
        let removed_data: HashSet<_> = removed
            .iter()
            .filter_map(|entry| match entry.frozen.identity() {
                super::model::EntryIdentity::DataFile { path } => Some(path.clone()),
                _ => None,
            })
            .collect();
        let selected = removed
            .iter()
            .map(|entry| entry.frozen.identity())
            .collect::<HashSet<_>>();
        let dependent = live
            .values()
            .filter(|entry| {
                !selected.contains(entry.frozen.identity())
                    && matches!(
                        entry.frozen.identity(),
                        super::model::EntryIdentity::DeletionVector { referenced_data_file, .. }
                            if removed_data.contains(referenced_data_file)
                    )
            })
            .collect::<Vec<_>>();
        removed.extend(dependent);
        let snapshot_id = super::staging::new_snapshot_id(metadata);
        let added = super::overwrite::write_added_intent_data(view, intent, snapshot_id).await?;
        let manifests = super::manifest_transform::ordinary_manifests(
            view,
            snapshot_id,
            sources.as_ref(),
            &removed,
            added,
        )
        .await?;
        let mut summary =
            super::overwrite::snapshot_file_summary(&intent.changes().added, &removed)?;
        super::overwrite::set_physical_rows_after_removal(view, &mut summary)?;
        summary.insert("replace-partitions".into(), "true".into());
        super::overwrite::prepare_ordinary_snapshot_change(
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
