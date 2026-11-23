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
        prepare_append(view, intent, None, false, None).await
    }
}

pub(crate) struct CompactFastAppendPreparer(pub(crate) super::operation::IcebergCommitOperation);

impl CompactFastAppendPreparer {
    pub(crate) async fn prepare_full_refresh(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
        witness: Option<(String, String)>,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        prepare_append(view, intent, Some(&self.0), true, witness).await
    }
}

#[async_trait]
impl super::staging::Preparer for CompactFastAppendPreparer {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        prepare_append(view, intent, Some(&self.0), false, None).await
    }
}

#[cfg(test)]
pub(crate) struct FullRefreshAppendFixture(pub(crate) Option<(String, String)>);

#[cfg(test)]
#[async_trait]
impl super::staging::Preparer for FullRefreshAppendFixture {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        prepare_append(view, intent, None, true, self.0.clone()).await
    }
}

async fn prepare_append(
    view: &super::staging::StagedView<'_>,
    intent: &super::model::OperationIntent,
    operation: Option<&super::operation::IcebergCommitOperation>,
    full_refresh: bool,
    witness: Option<(String, String)>,
) -> crate::iceberg::Result<super::staging::PreparedChange> {
    if operation.is_none() {
        super::overwrite::validate_added_data(intent)?;
    }
    let sources = view.target_sources().await?;
    let snapshot_id = view.stage_snapshot_id()?;
    let (added, mut summary) = if let Some(operation) = operation {
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
    if full_refresh {
        if let Some((key, value)) = witness
            && (intent.summary().contains_key(&key) || summary.insert(key, value).is_some())
        {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "Full refresh witness conflicts with an existing snapshot fact",
            ));
        }
        let manifests = manifests.into_snapshot_manifests(&mut summary);
        return super::overwrite::prepare_snapshot_change_with_summary(
            view,
            intent,
            snapshot_id,
            Operation::Append,
            manifests,
            summary,
            super::overwrite::SnapshotSummaryMode::FullRefreshAppend,
        )
        .await;
    }
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
