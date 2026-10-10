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

//! Prepare an append from immutable file facts and the current target parent.

use crate::iceberg::spec::Operation;
use async_trait::async_trait;

/// An append freezes file facts once and prepares fresh manifests per attempt.
pub(crate) struct FastAppendPreparer;

#[async_trait]
impl super::staging::Preparer for FastAppendPreparer {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        super::overwrite::validate_added_data(intent)?;
        let metadata = view.metadata();
        let parent = metadata
            .snapshot_for_ref(intent.target_ref())
            .map(|s| s.snapshot_id());
        let mut manifests = if let Some(id) = parent {
            view.artifacts().check_active()?;
            let snapshot = metadata.snapshot_by_id(id).ok_or_else(|| {
                crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::DataInvalid,
                    "Append target ref snapshot is absent",
                )
            })?;
            let list = snapshot
                .load_manifest_list(view.artifacts().file_io(), metadata)
                .await?;
            view.artifacts().check_active()?;
            list.entries().to_vec()
        } else {
            Vec::new()
        };
        let snapshot_id = super::staging::new_snapshot_id(metadata);
        manifests
            .extend(super::overwrite::write_added_intent_data(view, intent, snapshot_id).await?);
        super::overwrite::prepare_snapshot_change(
            view,
            intent,
            snapshot_id,
            Operation::Append,
            manifests,
            super::overwrite::snapshot_file_summary(&intent.changes().added, &[])?,
            false,
        )
        .await
    }
}
