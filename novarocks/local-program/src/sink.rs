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

//! Pure output topology. Destination addresses, transmitters and queues are
//! task-owned bindings; only branch semantics and frozen expressions live here.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use novarocks_execution_contract::DataStreamPartitionType;
use novarocks_types::SlotId;

use crate::{ImmutableExpressions, ProgramExprId};

pub const MAX_STATIC_SINK_BRANCHES: usize = 4_096;
pub const MAX_STATIC_SINK_COLUMNS: usize = 4_096;
pub const MAX_STATIC_SINK_EXPRESSIONS: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticSinkError {
    InvalidDestinationNode,
    DuplicateOutputColumn,
    TooManyColumns,
    TooManyExpressions,
    InvalidPartitionExpressions,
    InvalidExpression,
    EmptyBranches,
    TooManyBranches,
    SplitArityMismatch,
}

impl fmt::Display for StaticSinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid static sink: {self:?}")
    }
}

impl std::error::Error for StaticSinkError {}

#[derive(Clone, Debug)]
pub struct StaticStreamBranch {
    dest_node_id: i32,
    partition_type: DataStreamPartitionType,
    partition_exprs: Arc<[ProgramExprId]>,
    output_columns: Arc<[SlotId]>,
    limit: Option<i64>,
}

impl StaticStreamBranch {
    pub fn try_new(
        dest_node_id: i32,
        partition_type: DataStreamPartitionType,
        partition_exprs: Vec<ProgramExprId>,
        output_columns: Vec<SlotId>,
        limit: Option<i64>,
    ) -> Result<Self, StaticSinkError> {
        if dest_node_id < 0 {
            return Err(StaticSinkError::InvalidDestinationNode);
        }
        if partition_exprs.len() > MAX_STATIC_SINK_EXPRESSIONS {
            return Err(StaticSinkError::TooManyExpressions);
        }
        if output_columns.len() > MAX_STATIC_SINK_COLUMNS {
            return Err(StaticSinkError::TooManyColumns);
        }
        if !partition_type.requires_exprs() && !partition_exprs.is_empty() {
            return Err(StaticSinkError::InvalidPartitionExpressions);
        }
        if output_columns
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != output_columns.len()
        {
            return Err(StaticSinkError::DuplicateOutputColumn);
        }
        Ok(Self {
            dest_node_id,
            partition_type,
            partition_exprs: Arc::from(partition_exprs),
            output_columns: Arc::from(output_columns),
            limit,
        })
    }

    pub const fn dest_node_id(&self) -> i32 {
        self.dest_node_id
    }
    pub const fn partition_type(&self) -> DataStreamPartitionType {
        self.partition_type
    }
    pub fn partition_exprs(&self) -> &[ProgramExprId] {
        &self.partition_exprs
    }
    pub fn output_columns(&self) -> &[SlotId] {
        &self.output_columns
    }
    pub const fn limit(&self) -> Option<i64> {
        self.limit
    }
}

/// One immutable arena per stream sink. Multicast and split branches share
/// that arena, preserving exact expression IDs without per-branch duplication.
#[derive(Clone, Debug)]
pub enum StaticSinkProgram {
    Result,
    Noop,
    DataStream {
        branch: StaticStreamBranch,
        arena: Arc<ImmutableExpressions>,
    },
    MultiCastDataStream {
        branches: Arc<[StaticStreamBranch]>,
        arena: Arc<ImmutableExpressions>,
    },
    SplitDataStream {
        branches: Arc<[StaticStreamBranch]>,
        split_exprs: Arc<[ProgramExprId]>,
        arena: Arc<ImmutableExpressions>,
        fanout: bool,
    },
}

impl StaticSinkProgram {
    pub fn validate(&self) -> Result<(), StaticSinkError> {
        match self {
            Self::Result | Self::Noop => Ok(()),
            Self::DataStream { branch, arena } => validate_branch(branch, arena),
            Self::MultiCastDataStream { branches, arena } => validate_branches(branches, arena),
            Self::SplitDataStream {
                branches,
                split_exprs,
                arena,
                ..
            } => {
                validate_branches(branches, arena)?;
                if branches.len() != split_exprs.len() {
                    return Err(StaticSinkError::SplitArityMismatch);
                }
                if split_exprs.iter().any(|id| arena.node(*id).is_none()) {
                    return Err(StaticSinkError::InvalidExpression);
                }
                Ok(())
            }
        }
    }

    pub fn try_data_stream(
        branch: StaticStreamBranch,
        arena: Arc<ImmutableExpressions>,
    ) -> Result<Self, StaticSinkError> {
        validate_branch(&branch, &arena)?;
        Ok(Self::DataStream { branch, arena })
    }

    pub fn try_multicast(
        branches: Vec<StaticStreamBranch>,
        arena: Arc<ImmutableExpressions>,
    ) -> Result<Self, StaticSinkError> {
        validate_branches(&branches, &arena)?;
        Ok(Self::MultiCastDataStream {
            branches: Arc::from(branches),
            arena,
        })
    }

    pub fn try_split(
        branches: Vec<StaticStreamBranch>,
        split_exprs: Vec<ProgramExprId>,
        arena: Arc<ImmutableExpressions>,
        fanout: bool,
    ) -> Result<Self, StaticSinkError> {
        validate_branches(&branches, &arena)?;
        if branches.len() != split_exprs.len() {
            return Err(StaticSinkError::SplitArityMismatch);
        }
        if split_exprs.iter().any(|id| arena.node(*id).is_none()) {
            return Err(StaticSinkError::InvalidExpression);
        }
        Ok(Self::SplitDataStream {
            branches: Arc::from(branches),
            split_exprs: Arc::from(split_exprs),
            arena,
            fanout,
        })
    }

    pub fn branches(&self) -> &[StaticStreamBranch] {
        match self {
            Self::DataStream { branch, .. } => std::slice::from_ref(branch),
            Self::MultiCastDataStream { branches, .. } | Self::SplitDataStream { branches, .. } => {
                branches
            }
            Self::Result | Self::Noop => &[],
        }
    }

    pub fn arena(&self) -> Option<&Arc<ImmutableExpressions>> {
        match self {
            Self::DataStream { arena, .. }
            | Self::MultiCastDataStream { arena, .. }
            | Self::SplitDataStream { arena, .. } => Some(arena),
            Self::Result | Self::Noop => None,
        }
    }

    pub fn split_exprs(&self) -> &[ProgramExprId] {
        match self {
            Self::SplitDataStream { split_exprs, .. } => split_exprs,
            _ => &[],
        }
    }

    pub const fn fanout(&self) -> bool {
        matches!(self, Self::SplitDataStream { fanout: true, .. })
    }
}

fn validate_branches(
    branches: &[StaticStreamBranch],
    arena: &ImmutableExpressions,
) -> Result<(), StaticSinkError> {
    if branches.is_empty() {
        return Err(StaticSinkError::EmptyBranches);
    }
    if branches.len() > MAX_STATIC_SINK_BRANCHES {
        return Err(StaticSinkError::TooManyBranches);
    }
    for branch in branches {
        validate_branch(branch, arena)?;
    }
    Ok(())
}

fn validate_branch(
    branch: &StaticStreamBranch,
    arena: &ImmutableExpressions,
) -> Result<(), StaticSinkError> {
    if branch
        .partition_exprs
        .iter()
        .any(|id| arena.node(*id).is_none())
    {
        return Err(StaticSinkError::InvalidExpression);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_schema::DataType;

    use super::*;
    use crate::{StaticExprKind, StaticExprNode};

    fn arena() -> Arc<ImmutableExpressions> {
        Arc::new(
            ImmutableExpressions::try_new(
                vec![StaticExprNode::new(
                    StaticExprKind::SlotId(SlotId::new(1)),
                    DataType::Int64,
                    None,
                )],
                false,
                HashMap::new(),
                None,
            )
            .unwrap(),
        )
    }

    #[test]
    fn exact_stream_and_split_semantics_share_one_pure_arena() {
        let arena = arena();
        let branch = StaticStreamBranch::try_new(
            9,
            DataStreamPartitionType::HashPartitioned,
            vec![ProgramExprId::new(0)],
            vec![SlotId::new(1)],
            Some(7),
        )
        .unwrap();
        let split = StaticSinkProgram::try_split(
            vec![branch.clone(), branch],
            vec![ProgramExprId::new(0); 2],
            Arc::clone(&arena),
            true,
        )
        .unwrap();
        assert!(Arc::ptr_eq(split.arena().unwrap(), &arena));
        assert_eq!(split.branches().len(), 2);
        assert_eq!(split.branches()[0].limit(), Some(7));
        assert!(split.fanout());
    }

    #[test]
    fn invalid_branch_and_split_fail_before_runtime_binding() {
        let arena = arena();
        let duplicate = StaticStreamBranch::try_new(
            9,
            DataStreamPartitionType::Random,
            vec![],
            vec![SlotId::new(1), SlotId::new(1)],
            None,
        );
        assert!(matches!(
            duplicate,
            Err(StaticSinkError::DuplicateOutputColumn)
        ));
        let branch = StaticStreamBranch::try_new(
            9,
            DataStreamPartitionType::HashPartitioned,
            vec![ProgramExprId::new(0)],
            vec![SlotId::new(1)],
            None,
        )
        .unwrap();
        assert!(matches!(
            StaticSinkProgram::try_split(vec![branch.clone()], vec![], Arc::clone(&arena), false),
            Err(StaticSinkError::SplitArityMismatch)
        ));
        assert!(matches!(
            StaticSinkProgram::try_multicast(vec![], Arc::clone(&arena)),
            Err(StaticSinkError::EmptyBranches)
        ));
        assert!(matches!(
            StaticSinkProgram::try_data_stream(
                StaticStreamBranch::try_new(
                    9,
                    DataStreamPartitionType::HashPartitioned,
                    vec![ProgramExprId::new(2)],
                    vec![],
                    None
                )
                .unwrap(),
                arena
            ),
            Err(StaticSinkError::InvalidExpression)
        ));
    }
}
