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

//! TRUNCATE prepares a delete snapshot for the exact target ref.
//!
//! Every live data or delete entry is marked Deleted with its original facts.
//! Schema, partitioning and other refs are preserved; no row IDs are allocated.

use crate::iceberg::spec::{FormatVersion, Operation};
use async_trait::async_trait;

/// Remove every live logical entry from the exact target ref.
pub(crate) struct TruncatePreparer;

#[async_trait]
impl super::staging::Preparer for TruncatePreparer {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        if view.metadata().format_version() == FormatVersion::V1 {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "TRUNCATE does not support V1 tables",
            ));
        }
        if !intent.changes().added.is_empty() {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "TRUNCATE cannot add files",
            ));
        }
        let parent = view
            .metadata()
            .snapshot_for_ref(intent.target_ref())
            .map(|s| s.snapshot_id());
        let mut inputs =
            super::dependency::ValidationInputs::new(view.metadata(), parent, view.artifacts());
        let removed: Vec<_> = inputs.live_set().await?.values().cloned().collect();
        let snapshot_id = super::staging::new_snapshot_id(view.metadata());
        let manifests = super::overwrite::write_live_entry_groups(
            view,
            snapshot_id,
            removed.iter().cloned().map(|e| (e, true)).collect(),
        )
        .await?;
        super::overwrite::prepare_snapshot_change(
            view,
            intent,
            snapshot_id,
            Operation::Delete,
            manifests,
            super::overwrite::snapshot_file_summary(&[], &removed)?,
            true,
        )
        .await
    }
}
