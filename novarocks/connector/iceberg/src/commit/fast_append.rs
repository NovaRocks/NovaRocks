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
        prepare_append(view, intent, None).await
    }
}

pub(crate) struct CompactFastAppendPreparer(pub(crate) super::operation::IcebergCommitOperation);

#[async_trait]
impl super::staging::Preparer for CompactFastAppendPreparer {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        prepare_append(view, intent, Some(&self.0)).await
    }
}

async fn prepare_append(
    view: &super::staging::StagedView<'_>,
    intent: &super::model::OperationIntent,
    operation: Option<&super::operation::IcebergCommitOperation>,
) -> crate::iceberg::Result<super::staging::PreparedChange> {
    if operation.is_none() {
        super::overwrite::validate_added_data(intent)?;
    }
    let sources = view.target_sources().await?;
    let snapshot_id = view.stage_snapshot_id()?;
    let (added, summary) = if let Some(operation) = operation {
        let stage = intent.compact_stage().ok_or_else(|| {
            crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::Unexpected,
                "Compact append is missing its stage source",
            )
        })?;
        let (manifests, counters) = super::new_manifests::prepare_added_manifests(
            operation,
            view,
            stage,
            snapshot_id,
            None,
        )
        .await?;
        (
            manifests,
            super::overwrite::compact_append_summary(counters),
        )
    } else {
        let added = super::overwrite::write_added_intent_data(view, intent, snapshot_id).await?;
        (
            added,
            super::overwrite::snapshot_file_summary(&intent.changes().added, &[])?,
        )
    };
    let manifests = super::manifest_transform::ordinary_manifests(
        view,
        snapshot_id,
        sources.as_ref(),
        &[],
        added,
    )
    .await?;
    super::overwrite::prepare_ordinary_snapshot_change(
        view,
        intent,
        snapshot_id,
        Operation::Append,
        manifests,
        summary,
        false,
    )
    .await
}
