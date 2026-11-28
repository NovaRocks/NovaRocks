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

//! A logical operation freezes its source facts once, before any publication attempt.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::connector::read_stack::ConnectorMvTargetPartitionSelection;

use super::{
    AddedContent, CommitProtocol, Dependency, FrozenEntry, OperationToken, RequestShape, invalid,
};
use crate::iceberg::{Result, TableIdent};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableTarget {
    pub ident: TableIdent,
    /// Absent only for an operation creating a table.
    pub uuid: Option<Uuid>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StartSnapshot {
    pub snapshot_id: i64,
    pub sequence_number: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IsolationLevel {
    Snapshot,
    Serializable,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FileChanges {
    pub added: Vec<AddedContent>,
    pub removed: Vec<FrozenEntry>,
}

/// Mutable construction input; no mutable references survive intent freezing.
#[derive(Debug)]
pub struct OperationIntentParts {
    pub target: TableTarget,
    pub target_ref: String,
    pub start: Option<StartSnapshot>,
    pub changes: FileChanges,
    pub dependencies: Vec<Dependency>,
    pub(crate) protocol: CommitProtocol,
    pub shape: RequestShape,
    pub summary: BTreeMap<String, String>,
    pub token: OperationToken,
}

#[derive(Debug)]
// Design: ADR-0171 (docs/adr/ADR-0171-commit-operation-model.md)
pub struct OperationIntent {
    parts: OperationIntentParts,
    snapshot_loading: crate::catalog::SnapshotLoading,
    compact_stage: Option<usize>,
    partition_replace: Option<Arc<ConnectorMvTargetPartitionSelection>>,
}

impl OperationIntent {
    pub fn new(parts: OperationIntentParts) -> Result<Self> {
        if parts.target.ident.name.is_empty() || parts.target_ref.is_empty() {
            return Err(invalid(
                "Commit intent requires an exact table name and target ref",
            ));
        }
        if (parts.shape == RequestShape::Create) != parts.target.uuid.is_none() {
            return Err(invalid("Only create intents may omit the table UUID"));
        }
        if parts.start.is_some_and(|start| start.sequence_number < 0) {
            return Err(invalid("Start snapshot sequence must be nonnegative"));
        }
        if matches!(
            &parts.protocol,
            CommitProtocol::Truncate | CommitProtocol::Delete
        ) && (parts.shape != RequestShape::SnapshotProducing
            || parts.target_ref != "main"
            || parts.start.is_some()
            || !parts.dependencies.is_empty())
        {
            return Err(invalid(match &parts.protocol {
                CommitProtocol::Delete => {
                    "DELETE requires the main branch and no frozen ref dependency"
                }
                _ => "TRUNCATE requires the main branch and no frozen ref dependency",
            }));
        }
        if matches!(&parts.protocol, CommitProtocol::RowDelta { .. })
            && (parts.shape != RequestShape::SnapshotProducing || !parts.dependencies.is_empty())
        {
            return Err(invalid(
                "Row-delta requires a snapshot operation with one validation window",
            ));
        }
        if matches!(&parts.protocol, CommitProtocol::Append { .. })
            && (parts.shape != RequestShape::SnapshotProducing || !parts.dependencies.is_empty())
        {
            return Err(invalid(
                "Append requires a snapshot operation without a read dependency",
            ));
        }
        if parts.protocol.is_mv()
            && (parts.shape != RequestShape::SnapshotProducing
                || parts.target_ref != "main"
                || !parts.dependencies.is_empty())
        {
            return Err(invalid(
                "Materialized view protocol requires a snapshot-producing main intent",
            ));
        }
        if matches!(&parts.protocol, CommitProtocol::CreateTable)
            && parts.shape != RequestShape::Create
        {
            return Err(invalid("Create-table protocol requires a create request"));
        }
        if parts.shape == RequestShape::Create && parts.start.is_some() {
            return Err(invalid(
                "Create intent cannot carry an existing start snapshot",
            ));
        }
        if parts.shape == RequestShape::FencedMarkers
            && (parts.target_ref != "main"
                || !matches!(&parts.protocol, CommitProtocol::Legacy)
                || parts.dependencies != [Dependency::RefUnchanged]
                || !parts.changes.added.is_empty()
                || !parts.changes.removed.is_empty())
        {
            return Err(invalid(
                "Fenced marker intent requires a fixed main and no file changes",
            ));
        }
        Ok(Self {
            parts,
            snapshot_loading: crate::catalog::SnapshotLoading::All,
            compact_stage: None,
            partition_replace: None,
        })
    }
    pub(crate) fn with_compact_stage(mut self, stage: usize) -> Result<Self> {
        let supports_compact = self.shape() == RequestShape::SnapshotProducing
            || matches!(self.protocol(), CommitProtocol::CreateTable);
        if !supports_compact || !self.parts.changes.added.is_empty() {
            return Err(invalid(
                "Compact additions require an exclusive snapshot stage source",
            ));
        }
        self.compact_stage = Some(stage);
        Ok(self)
    }

    pub(crate) fn compact_stage(&self) -> Option<usize> {
        self.compact_stage
    }

    pub(crate) fn with_partition_replace(
        mut self,
        selection: Arc<ConnectorMvTargetPartitionSelection>,
    ) -> Result<Self> {
        if !self.protocol().is_partition_replace()
            || self.start().map(|snapshot| snapshot.snapshot_id) != Some(selection.snapshot_id())
        {
            return Err(invalid("Partition replacement has a different fixed base"));
        }
        self.partition_replace = Some(selection);
        Ok(self)
    }

    pub(crate) fn partition_replace(&self) -> Option<&ConnectorMvTargetPartitionSelection> {
        self.partition_replace.as_deref()
    }
    pub(crate) fn with_snapshot_loading(mut self, mode: crate::catalog::SnapshotLoading) -> Self {
        self.snapshot_loading = mode;
        self
    }
    pub(crate) fn snapshot_loading(&self) -> crate::catalog::SnapshotLoading {
        self.snapshot_loading
    }
    /// A ref-frozen operation cannot be rebased after another commit wins.
    pub fn permits_conflict_retry(&self) -> bool {
        self.shape() != RequestShape::Create
            && self.shape() != RequestShape::DocumentExpiry
            && !self.dependencies().contains(&Dependency::RefUnchanged)
            && self.protocol().fixed_mv_main().is_none()
    }
    pub(crate) const fn protocol(&self) -> &CommitProtocol {
        &self.parts.protocol
    }
    pub fn target(&self) -> &TableTarget {
        &self.parts.target
    }
    pub fn target_ref(&self) -> &str {
        &self.parts.target_ref
    }
    pub const fn start(&self) -> Option<StartSnapshot> {
        self.parts.start
    }
    pub fn changes(&self) -> &FileChanges {
        &self.parts.changes
    }
    pub fn dependencies(&self) -> &[Dependency] {
        &self.parts.dependencies
    }
    pub const fn shape(&self) -> RequestShape {
        self.parts.shape
    }
    pub fn summary(&self) -> &BTreeMap<String, String> {
        &self.parts.summary
    }
    pub const fn token(&self) -> OperationToken {
        self.parts.token
    }
}
