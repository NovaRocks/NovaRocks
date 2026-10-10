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

use super::{AddedContent, Dependency, FrozenEntry, OperationToken, RequestShape, invalid};
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
    pub isolation: IsolationLevel,
    pub shape: RequestShape,
    pub summary: BTreeMap<String, String>,
    pub token: OperationToken,
}

#[derive(Debug)]
// Design: ADR-0171 (docs/adr/ADR-0171-commit-operation-model.md)
pub struct OperationIntent {
    parts: OperationIntentParts,
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
        if parts.shape == RequestShape::Create && parts.start.is_some() {
            return Err(invalid(
                "Create intent cannot carry an existing start snapshot",
            ));
        }
        Ok(Self { parts })
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
    pub const fn isolation(&self) -> IsolationLevel {
        self.parts.isolation
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
