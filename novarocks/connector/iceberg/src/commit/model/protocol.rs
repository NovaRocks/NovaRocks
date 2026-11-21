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

//! Immutable operation policy carried by each Iceberg commit intent.

use super::{FrozenEntry, IsolationLevel};
use crate::commit::dependency::RowDeltaWindowCheck;
use crate::iceberg::TableRequirement;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitOperation {
    Legacy,
    Write,
    Optimize,
    Truncate,
    Delete,
    OptimizeManifests,
    ExpireSnapshots,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitStyle {
    Legacy,
    Transaction,
    Update,
}

impl CommitOperation {
    pub(crate) const fn style(self) -> CommitStyle {
        match self {
            Self::Legacy => CommitStyle::Legacy,
            Self::Write | Self::Optimize => CommitStyle::Transaction,
            Self::Truncate | Self::Delete | Self::OptimizeManifests | Self::ExpireSnapshots => {
                CommitStyle::Update
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WriterSchemaSpec {
    pub(crate) schema_id: i32,
    pub(crate) default_spec_id: i32,
}

impl WriterSchemaSpec {
    pub(crate) fn requirements(self) -> [TableRequirement; 2] {
        [
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: self.schema_id,
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: self.default_spec_id,
            },
        ]
    }
}

/// The session selects a policy before publication has its complete retained window.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) enum WriteSessionCommitProtocol {
    #[default]
    Legacy,
    RowDelta(IsolationLevel),
}

impl WriteSessionCommitProtocol {
    pub(crate) const fn operation(self) -> CommitOperation {
        match self {
            Self::Legacy => CommitOperation::Legacy,
            Self::RowDelta(_) => CommitOperation::Write,
        }
    }

    pub(crate) const fn row_delta_isolation(self) -> Option<IsolationLevel> {
        match self {
            Self::Legacy => None,
            Self::RowDelta(isolation) => Some(isolation),
        }
    }
}

/// Encodes only combinations authored by their operation.
#[derive(Debug)]
pub(crate) enum CommitProtocol {
    Legacy,
    RowDelta {
        window: RowDeltaWindowCheck,
        writer: WriterSchemaSpec,
    },
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "The OPTIMIZE cutover supplies this frozen intent."
        )
    )]
    RewriteFiles {
        replaced: Arc<[FrozenEntry]>,
    },
}

impl CommitProtocol {
    pub(crate) const fn operation(&self) -> CommitOperation {
        match self {
            Self::Legacy => CommitOperation::Legacy,
            Self::RowDelta { .. } => CommitOperation::Write,
            Self::RewriteFiles { .. } => CommitOperation::Optimize,
        }
    }

    pub(crate) fn window(&self) -> WindowValidation<'_> {
        match self {
            Self::Legacy => WindowValidation::None,
            Self::RowDelta { window, .. } => WindowValidation::RowDelta(window),
            Self::RewriteFiles { replaced } => WindowValidation::RewriteFiles(replaced),
        }
    }

    pub(crate) const fn writer_schema_spec(&self) -> Option<WriterSchemaSpec> {
        match self {
            Self::Legacy | Self::RewriteFiles { .. } => None,
            Self::RowDelta { writer, .. } => Some(*writer),
        }
    }
}

pub(crate) enum WindowValidation<'a> {
    None,
    RowDelta(&'a RowDeltaWindowCheck),
    RewriteFiles(&'a [FrozenEntry]),
}

#[cfg(test)]
mod tests;
