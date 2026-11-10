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

//! Prepare immutable position/equality-delete additions for the exact target ref.
//!
//! Added entries inherit their actual publication sequences. The canonical
//! manifest-list writer determines any first row-ID assignment for history.

use std::collections::HashMap;

use crate::iceberg::spec::{DataContentType, Operation};
use async_trait::async_trait;

fn to_iceberg_unexpected(s: String) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, s)
}

/// Position/equality-delete preparation consumes frozen additions once per attempt.
pub(crate) struct RowDeltaPreparer;

#[async_trait]
impl crate::commit::staging::Preparer for RowDeltaPreparer {
    async fn prepare(
        &self,
        view: &crate::commit::staging::StagedView<'_>,
        intent: &crate::commit::model::OperationIntent,
    ) -> crate::iceberg::Result<crate::commit::staging::PreparedChange> {
        use crate::commit::model::SeqField;
        use crate::commit::row_delta_dv_metadata::{logical_file_size, write_added_entry_groups};
        if intent.changes().added.is_empty() {
            return Ok(crate::commit::staging::PreparedChange {
                updates: vec![],
                requirements: vec![],
            });
        }
        if !intent.changes().removed.is_empty() {
            return Err(to_iceberg_unexpected(
                "Position-delete intent cannot remove entries".into(),
            ));
        }
        let sources = view.target_sources().await?;
        let referenced = intent
            .changes()
            .added
            .iter()
            .filter_map(|added| added.file().referenced_data_file())
            .collect::<std::collections::BTreeSet<_>>();
        let live = match &sources {
            Some(sources) => {
                let paths = referenced.iter().map(String::as_str).collect();
                sources
                    .live_data_matches(&paths, view.artifacts().file_io(), || {
                        view.artifacts().check_active()
                    })
                    .await?
            }
            None => Default::default(),
        };
        let mut summary = HashMap::new();
        let mut records = [0u64; 2];
        let mut counts = [0usize; 2];
        let mut size = 0u64;
        for added in &intent.changes().added {
            let file = added.file();
            if added.data_sequence() != SeqField::Inherit {
                return Err(to_iceberg_unexpected(
                    "New delete entries must inherit their data sequence".into(),
                ));
            }
            let idx =
                match file.content_type() {
                    DataContentType::PositionDeletes => 0,
                    DataContentType::EqualityDeletes
                        if file.equality_ids().is_some_and(|ids| !ids.is_empty()) =>
                    {
                        1
                    }
                    _ => return Err(to_iceberg_unexpected(
                        "Row delta requires position deletes or equality deletes with equality IDs"
                            .into(),
                    )),
                };
            if let Some(path) = file.referenced_data_file()
                && !live.contains(&path)
            {
                return Err(to_iceberg_unexpected(format!(
                    "Position delete references absent data file {path}"
                )));
            }
            counts[idx] += 1;
            records[idx] = records[idx]
                .checked_add(file.record_count())
                .ok_or_else(|| to_iceberg_unexpected("Delete record count overflow".into()))?;
            size = size
                .checked_add(logical_file_size(file)?)
                .ok_or_else(|| to_iceberg_unexpected("Delete file size overflow".into()))?;
        }
        for (idx, kind) in ["position", "equality"].into_iter().enumerate() {
            if counts[idx] > 0 {
                summary.insert(
                    format!("added-{kind}-delete-files"),
                    counts[idx].to_string(),
                );
                summary.insert(format!("added-{kind}-deletes"), records[idx].to_string());
            }
        }
        summary.insert(
            "added-delete-files".into(),
            intent.changes().added.len().to_string(),
        );
        summary.insert("added-files-size".into(), size.to_string());
        let snapshot_id = crate::commit::staging::new_snapshot_id(view.metadata());
        let added =
            write_added_entry_groups(view, snapshot_id, intent.changes().added.clone()).await?;
        let manifests = super::manifest_transform::ordinary_manifests(
            view,
            snapshot_id,
            sources.as_ref(),
            &[],
            added,
        )
        .await?;
        crate::commit::overwrite::prepare_ordinary_snapshot_change(
            view,
            intent,
            snapshot_id,
            Operation::Delete,
            manifests,
            summary,
            false,
        )
        .await
    }
}
