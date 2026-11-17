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

//! Lower each frozen sort mode into the sole local ordering owner.

use crate::{assert_rows::reserve_vec, lowering::FragmentCompileError};
use novarocks_local_program::{
    LocalSortMode, PartitionRank, ProgramExprId, ProgramNodeId, ProgramNodeKind, SortExpression,
    StaticLayout,
};
use novarocks_physical_plan::{
    ExprId, NodeKind, NullOrdering, PhysicalNode, SortDirection, SortMode,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use std::{collections::BTreeMap, num::NonZeroUsize};

pub(crate) fn lower_sort(
    node: &PhysicalNode,
    input: ProgramNodeId,
    layout: &StaticLayout,
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    control: &dyn PureCompileControl,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(node, input, layout, expressions, &mut work);
    if matches!(&result, Err(FragmentCompileError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn lower_core(
    node: &PhysicalNode,
    input: ProgramNodeId,
    layout: &StaticLayout,
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    let NodeKind::Sort { order_by, mode } = &node.kind else {
        return Err(FragmentCompileError::Invalid("sort physical kind differs"));
    };
    let partition_by = match mode {
        SortMode::Global => &[][..],
        SortMode::Analytic { partition_by } | SortMode::PartitionTopN { partition_by, .. } => {
            partition_by
        }
    };
    if node.inputs.len() != 1
        || match mode {
            SortMode::Global => order_by.is_empty(),
            SortMode::Analytic { .. } | SortMode::PartitionTopN { .. } => partition_by.is_empty(),
        }
        || node.output.columns.len() != layout.slots().len()
    {
        return Err(FragmentCompileError::Invalid(
            "sort input, keys or output width differs",
        ));
    }
    let order_by = lower_sort_keys(order_by, expressions, work)?;
    let mode = match mode {
        SortMode::Global => LocalSortMode::Full,
        SortMode::Analytic { .. } => LocalSortMode::Partitioned {
            partition_by: lower_sort_keys(partition_by, expressions, work)?,
        },
        SortMode::PartitionTopN { limit, kind, .. } => {
            let limit = usize::try_from(*limit).map_err(|_| {
                FragmentCompileError::Invalid("partition TopN limit exceeds host range")
            })?;
            let limit = NonZeroUsize::new(limit).ok_or(FragmentCompileError::Invalid(
                "partition TopN limit is zero",
            ))?;
            let rank = match kind {
                novarocks_physical_plan::PartitionTopNType::RowNumber => PartitionRank::RowNumber,
                novarocks_physical_plan::PartitionTopNType::Rank => PartitionRank::Rank,
                novarocks_physical_plan::PartitionTopNType::DenseRank => PartitionRank::DenseRank,
            };
            LocalSortMode::PartitionTopN {
                partition_by: lower_sort_keys(partition_by, expressions, work)?,
                limit,
                rank,
            }
        }
    };
    Ok((
        ProgramNodeKind::Sort {
            input,
            order_by,
            mode,
        },
        layout.clone(),
    ))
}

/// Global and analytic Sort and ordinary TopN borrow one ordered-key
/// projection author.
pub(crate) fn lower_sort_keys(
    order_by: &[novarocks_physical_plan::SortExpr],
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<SortExpression>, FragmentCompileError> {
    let mut keys = Vec::new();
    reserve_vec(&mut keys, order_by.len(), work)?;
    for key in order_by {
        let expr = expressions
            .get(&key.expr)
            .copied()
            .ok_or(FragmentCompileError::Invalid("missing sort key expression"));
        work.step()?;
        keys.push(SortExpression {
            expr: expr?,
            asc: matches!(key.direction, SortDirection::Ascending),
            nulls_first: matches!(key.null_ordering, NullOrdering::First),
        });
    }
    Ok(keys)
}
