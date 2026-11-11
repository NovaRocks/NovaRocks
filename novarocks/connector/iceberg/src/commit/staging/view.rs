// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::BTreeMap;

use crate::commit::manifest_sources::{LocatedEntry, SnapshotSources};
use crate::commit::model::{ArtifactWriter, EntryIdentity, OperationIntent};
use crate::commit::preparation::PreparationContext;
use crate::iceberg::Result;
use crate::iceberg::spec::{ManifestList, Snapshot, TableMetadata};

/// Each preparer borrows exactly the canonical prefix and the attempt IO owner.
pub struct StagedView<'a> {
    metadata: &'a TableMetadata,
    intent: &'a OperationIntent,
    artifacts: &'a dyn ArtifactWriter,
    context: PreparationContext,
    completed_list: Option<&'a (String, ManifestList)>,
}

impl<'a> StagedView<'a> {
    pub(super) fn new(
        metadata: &'a TableMetadata,
        intent: &'a OperationIntent,
        artifacts: &'a dyn ArtifactWriter,
        context: PreparationContext,
        completed_list: Option<&'a (String, ManifestList)>,
    ) -> Self {
        Self {
            metadata,
            intent,
            artifacts,
            context,
            completed_list,
        }
    }
    pub fn metadata(&self) -> &TableMetadata {
        self.metadata
    }
    pub fn artifacts(&self) -> &dyn ArtifactWriter {
        self.artifacts
    }
    pub(crate) fn preparation(&self) -> PreparationContext {
        self.context
    }

    pub(crate) async fn target_sources(&self) -> Result<Option<SnapshotSources<'_>>> {
        let Some(snapshot) = self.metadata.snapshot_for_ref(self.target_ref()) else {
            return Ok(None);
        };
        load_snapshot_sources(self.metadata, self.artifacts, self.completed_list, snapshot)
            .await
            .map(Some)
    }
    pub(crate) async fn observe_live_sources(
        &self,
    ) -> Result<(
        Option<SnapshotSources<'_>>,
        BTreeMap<EntryIdentity, LocatedEntry>,
    )> {
        let sources = self.target_sources().await?;
        let entries = match &sources {
            Some(sources) => {
                sources
                    .live_entries(self.artifacts.file_io(), || self.artifacts.check_active())
                    .await?
            }
            None => BTreeMap::new(),
        };
        Ok((sources, entries))
    }

    pub fn target_ref(&self) -> &str {
        self.intent.target_ref()
    }
}

pub(super) async fn load_snapshot_sources<'a>(
    metadata: &TableMetadata,
    artifacts: &dyn ArtifactWriter,
    completed_list: Option<&(String, ManifestList)>,
    snapshot: &'a Snapshot,
) -> Result<SnapshotSources<'a>> {
    let active = || artifacts.check_active();
    if let Some((path, list)) = completed_list
        && path == snapshot.manifest_list()
    {
        return SnapshotSources::from_completed_list(snapshot, list, active);
    }
    SnapshotSources::load(metadata, snapshot, artifacts.file_io(), active).await
}
