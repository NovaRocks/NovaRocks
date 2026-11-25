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
use crate::iceberg::{TableRequirement, spec::TableMetadata};
use novarocks_spi::connector::{ConnectorSlotName, DocumentIdentity};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitOperation {
    Legacy,
    Write,
    Insert,
    CreateTable,
    Optimize,
    Truncate,
    Delete,
    OptimizeManifests,
    ExpireSnapshots,
    RefreshMaterializedView,
    AlterMaterializedViewProperties,
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
            Self::Write
            | Self::Insert
            | Self::CreateTable
            | Self::Optimize
            | Self::RefreshMaterializedView => CommitStyle::Transaction,
            Self::Truncate
            | Self::Delete
            | Self::OptimizeManifests
            | Self::ExpireSnapshots
            | Self::AlterMaterializedViewProperties => CommitStyle::Update,
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
    Append,
    RowDelta(IsolationLevel),
    RewriteFiles,
    RefreshMaterializedView(MvRefreshMode),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvRefreshMode {
    Full,
    Repartition,
    Incremental,
    MetadataOnly,
}

/// Digests include the slot writer as well as the slot's format and value.
pub(crate) type SlotExpectations = BTreeMap<ConnectorSlotName, Option<DocumentIdentity>>;

#[derive(Clone, Debug, Default)]
pub(crate) struct MvRefreshDependencies {
    pub(crate) roots: BTreeMap<String, Option<DocumentIdentity>>,
    pub(crate) slots: SlotExpectations,
}

pub(crate) const MV_MAIN_ADVANCED: &str =
    "Materialized view storage table changed during incremental refresh";

impl WriteSessionCommitProtocol {
    pub(crate) const fn operation(self) -> CommitOperation {
        match self {
            Self::Legacy => CommitOperation::Legacy,
            Self::Append => CommitOperation::Insert,
            Self::RowDelta(_) => CommitOperation::Write,
            Self::RewriteFiles => CommitOperation::Optimize,
            Self::RefreshMaterializedView(_) => CommitOperation::RefreshMaterializedView,
        }
    }

    pub(crate) const fn row_delta_isolation(self) -> Option<IsolationLevel> {
        match self {
            Self::Legacy | Self::Append | Self::RewriteFiles | Self::RefreshMaterializedView(_) => {
                None
            }
            Self::RowDelta(isolation) => Some(isolation),
        }
    }
}

// Design: ADR-0251 (docs/adr/ADR-0251-iceberg-maintenance-commits-by-operation.md)
/// Encodes only combinations authored by their operation.
#[derive(Debug)]
#[cfg_attr(test, derive(Clone))]
pub(crate) enum CommitProtocol {
    Legacy,
    CreateTable,
    Truncate,
    Append {
        output_spec_id: i32,
    },
    Delete,
    RowDelta {
        window: RowDeltaWindowCheck,
        writer: WriterSchemaSpec,
    },
    RewriteFiles {
        replaced: Arc<[FrozenEntry]>,
        output_spec_id: i32,
    },
    MvRefresh {
        mode: MvRefreshMode,
        admitted: MvRefreshDependencies,
        writer: WriterSchemaSpec,
    },
    MvAlterProperties {
        slots: SlotExpectations,
    },
}

impl CommitProtocol {
    pub(crate) const fn operation(&self) -> CommitOperation {
        match self {
            Self::Legacy => CommitOperation::Legacy,
            Self::CreateTable => CommitOperation::CreateTable,
            Self::Append { .. } => CommitOperation::Insert,
            Self::Truncate => CommitOperation::Truncate,
            Self::Delete => CommitOperation::Delete,
            Self::RowDelta { .. } => CommitOperation::Write,
            Self::RewriteFiles { .. } => CommitOperation::Optimize,
            Self::MvRefresh { .. } => CommitOperation::RefreshMaterializedView,
            Self::MvAlterProperties { .. } => CommitOperation::AlterMaterializedViewProperties,
        }
    }

    pub(crate) fn window(&self) -> WindowValidation<'_> {
        match self {
            Self::Legacy
            | Self::CreateTable
            | Self::Append { .. }
            | Self::Truncate
            | Self::Delete
            | Self::MvRefresh { .. }
            | Self::MvAlterProperties { .. } => WindowValidation::None,
            Self::RowDelta { window, .. } => WindowValidation::RowDelta(window),
            Self::RewriteFiles { replaced, .. } => WindowValidation::RewriteFiles(replaced),
        }
    }

    pub(crate) const fn writer_schema_spec(&self) -> Option<WriterSchemaSpec> {
        match self {
            Self::Legacy
            | Self::CreateTable
            | Self::Append { .. }
            | Self::Truncate
            | Self::Delete
            | Self::RewriteFiles { .. }
            | Self::MvAlterProperties { .. } => None,
            Self::RowDelta { writer, .. } | Self::MvRefresh { writer, .. } => Some(*writer),
        }
    }

    pub(crate) const fn output_spec_requirement(&self) -> Option<TableRequirement> {
        match self {
            Self::Append { output_spec_id } | Self::RewriteFiles { output_spec_id, .. } => {
                Some(TableRequirement::DefaultSpecIdMatch {
                    default_spec_id: *output_spec_id,
                })
            }
            Self::Legacy
            | Self::CreateTable
            | Self::Truncate
            | Self::Delete
            | Self::RowDelta { .. }
            | Self::MvRefresh { .. }
            | Self::MvAlterProperties { .. } => None,
        }
    }

    pub(crate) const fn requires_target_branch(&self) -> bool {
        matches!(
            self,
            Self::Truncate
                | Self::Delete
                | Self::RewriteFiles { .. }
                | Self::MvRefresh { .. }
                | Self::MvAlterProperties { .. }
        )
    }

    pub(crate) const fn is_mv(&self) -> bool {
        matches!(
            self,
            Self::MvRefresh { .. } | Self::MvAlterProperties { .. }
        )
    }

    pub(crate) const fn fixed_mv_main(&self) -> Option<&'static str> {
        match self {
            Self::MvRefresh {
                mode: MvRefreshMode::Incremental | MvRefreshMode::MetadataOnly,
                ..
            } => Some(MV_MAIN_ADVANCED),
            Self::MvRefresh {
                mode: MvRefreshMode::Repartition,
                ..
            } => Some("Materialized view storage table changed during repartition"),
            _ => None,
        }
    }
}

/// A table with no current snapshot has an empty main branch without a ref entry.
pub(crate) fn validate_target_branch(metadata: &TableMetadata, name: &str) -> Result<(), String> {
    match metadata.refs().get(name) {
        Some(reference) if reference.is_branch() => Ok(()),
        Some(_) => Err(format!(
            "{name} is a tag, not a branch. Tags cannot be targets for producing snapshots"
        )),
        None if name == "main" && metadata.current_snapshot_id().is_none() => Ok(()),
        None => Err(format!("Iceberg target branch does not exist: {name}")),
    }
}

pub(crate) enum WindowValidation<'a> {
    None,
    RowDelta(&'a RowDeltaWindowCheck),
    RewriteFiles(&'a [FrozenEntry]),
}

#[cfg(test)]
mod tests;
