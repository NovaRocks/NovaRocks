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

//! Ordinary row-count TopN uses the sole ordered-key projection and local sort owner.
//!
//! Every phase of a `Rows` reduction lowers to the same local operation: the
//! rows `[offset, offset + limit)` of this instance's input under the frozen
//! ordering. The phase is a cross-instance fact the checked physical contract
//! already discharges, so no phase or sequence identity reaches the program:
//!
//! - `Single` and `Final` read one Singleton, single-copy input, so their
//!   instance input is the whole relation and the window is the answer.
//! - `Partial` keeps its input distribution and carries offset 0 with the
//!   limit `final.limit + final.offset` its sequence trace proves. Each
//!   instance keeps its own top `limit` rows. A row with fewer than `limit`
//!   rows strictly ahead of it in the relation has fewer in its own instance,
//!   so that prefix keeps it or an equal-keyed row in its place. The union of
//!   the instance prefixes therefore holds a complete window under the frozen
//!   ordering, ties resolved per instance, and the Final over the gathered
//!   union selects it.
//!
//! `GroupedStates` merges partial states by the full group key (spec §5.8)
//! uses its own local state-reduction node and exact prepared call sites.

use crate::{lowering::FragmentCompileError, sort::lower_sort_keys};
use novarocks_local_program::{LocalSortMode, ProgramNodeId, ProgramNodeKind, StaticLayout};
use novarocks_physical_plan::{NodeKind, PhysicalNode, TopNPhase, TopNReduction};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

pub(crate) fn lower_topn(
    node: &PhysicalNode,
    input: ProgramNodeId,
    layout: &StaticLayout,
    expressions: &crate::expressions::RootMap,
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
    expressions: &crate::expressions::RootMap,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    let NodeKind::TopN {
        order_by,
        limit,
        offset,
        phase,
        reduction: TopNReduction::Rows,
    } = &node.kind
    else {
        return Err(FragmentCompileError::Invalid(
            "row-count TopN physical kind differs",
        ));
    };
    // A partial prefix is cut before global completion, so an offset there
    // would drop rows the final window may need.
    let partial_offset = matches!(phase, TopNPhase::Partial { .. }) && *offset != 0;
    let shape = node.inputs.len() == 1
        && !order_by.is_empty()
        && node.output.columns.len() == layout.slots().len();
    let limit = usize::try_from(*limit);
    let offset = usize::try_from(*offset);
    work.step()?;
    if !shape {
        return Err(FragmentCompileError::Invalid(
            "ordinary TopN input, keys or output width differs",
        ));
    }
    if partial_offset {
        return Err(FragmentCompileError::Invalid(
            "partial TopN carries an offset before global completion",
        ));
    }
    let limit =
        limit.map_err(|_| FragmentCompileError::Invalid("TopN limit exceeds host range"))?;
    let offset =
        offset.map_err(|_| FragmentCompileError::Invalid("TopN offset exceeds host range"))?;
    let extent = limit.checked_add(offset);
    work.step()?;
    extent.ok_or(FragmentCompileError::Invalid(
        "TopN limit and offset exceed host range",
    ))?;
    let keys = lower_sort_keys(order_by, expressions, work)?;
    Ok((
        ProgramNodeKind::Sort {
            input,
            order_by: keys,
            mode: LocalSortMode::RowsTopN { limit, offset },
        },
        layout.clone(),
    ))
}

/// Admission and lowering share the frozen GroupedStates shape check.
pub(crate) fn admit_grouped_topn_shape(node: &PhysicalNode) -> Result<(), FragmentCompileError> {
    let NodeKind::TopN {
        reduction: TopNReduction::GroupedStates { group_by, .. },
        phase,
        offset,
        ..
    } = &node.kind
    else {
        return Err(FragmentCompileError::Invalid(
            "GroupedTopN physical kind differs",
        ));
    };
    if *offset != 0
        || !matches!(phase, TopNPhase::Partial { .. })
        || group_by.is_empty()
        || node.inputs.len() != 1
    {
        return Err(FragmentCompileError::Invalid(
            "GroupedTopN requires complete keys and a partial zero-offset input",
        ));
    }
    Ok(())
}

/// Lower a grouped reduction through the original aggregate call/layout leaves.
pub(crate) fn lower_grouped_topn_with_creation<H>(
    package: &novarocks_physical_plan::FragmentPackage,
    node: &PhysicalNode,
    input: ProgramNodeId,
    expressions: &crate::expressions::RootMap,
    slots: &[novarocks_types::SlotId],
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<(ProgramNodeKind, StaticLayout), crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = (|| -> Result<_, crate::FragmentCreationError<H>> {
        let NodeKind::TopN {
            reduction:
                TopNReduction::GroupedStates {
                    group_by,
                    calls,
                    comparator,
                },
            order_by,
            limit,
            ..
        } = &node.kind
        else {
            return Err(
                (FragmentCompileError::Invalid("GroupedTopN physical kind differs")).into(),
            );
        };
        admit_grouped_topn_shape(node)?;
        let width = group_by
            .len()
            .checked_add(calls.len())
            .ok_or(FragmentCompileError::Invalid("GroupedTopN width exhausted"))?;
        if node.output.columns.len() != width
            || slots.len() != width
            || order_by.len() != group_by.len()
        {
            return Err((FragmentCompileError::Invalid(
                "GroupedTopN output or complete key width differs",
            ))
            .into());
        }
        let expected = group_by
            .iter()
            .map(|(_, value)| value)
            .chain(calls.iter().map(|call| &call.output));
        for (actual, expected) in node.output.columns.iter().zip(expected) {
            work.step()?;
            if actual != expected {
                return Err((FragmentCompileError::Invalid(
                    "GroupedTopN output differs from its keys and new state calls",
                ))
                .into());
            }
        }
        let mut groups = Vec::new();
        crate::assert_rows::reserve_vec(&mut groups, group_by.len(), &mut work)?;
        for (id, value) in group_by {
            let definition = crate::expressions::bound_root(package, node.id, *id)?;
            work.step()?;
            if !matches!(definition.kind, novarocks_expression_contract::ExprKind::Value(source) if source == *value)
                || !novarocks_physical_plan::grouped_state_topn_key_supported(
                    *comparator,
                    &definition.ty,
                )
            {
                return Err((FragmentCompileError::Invalid(
                    "GroupedTopN key is not an exact supported direct value",
                ))
                .into());
            }
            groups.push(*expressions.get(id).ok_or(FragmentCompileError::Invalid(
                "missing lowered GroupedTopN key",
            ))?);
        }
        let mut permutation = Vec::new();
        crate::assert_rows::reserve_vec(&mut permutation, order_by.len(), &mut work)?;
        for key in order_by {
            let definition = crate::expressions::bound_root(package, node.id, key.expr)?;
            let novarocks_expression_contract::ExprKind::Value(value) = definition.kind else {
                return Err((FragmentCompileError::Invalid(
                    "GroupedTopN rank is not a direct value",
                ))
                .into());
            };
            let mut ordinal = None;
            for (index, (_, group)) in group_by.iter().enumerate() {
                work.step()?;
                if *group == value {
                    ordinal = Some(index);
                }
            }
            let ordinal = ordinal.ok_or(FragmentCompileError::Invalid(
                "GroupedTopN rank is not a complete key",
            ))?;
            if permutation.contains(&ordinal) {
                return Err(
                    (FragmentCompileError::Invalid("GroupedTopN rank repeats a key")).into(),
                );
            }
            permutation.push(ordinal);
        }
        let keys = lower_sort_keys(order_by, expressions, &mut work)?;
        let mut functions = Vec::new();
        crate::assert_rows::reserve_vec(&mut functions, calls.len(), &mut work)?;
        for call in calls {
            work.step()?;
            if !matches!(
                call.binding.phase,
                novarocks_physical_plan::AggregatePhase::Intermediate { .. }
            ) || call.arguments.len() != 1
                || call.distinct
                || !call.order_by.is_empty()
            {
                return Err((FragmentCompileError::Invalid(
                    "GroupedTopN call is not an exact Intermediate merge",
                ))
                .into());
            }
            functions.push(crate::aggregate::lower_call(
                call,
                expressions,
                &mut work,
                creation,
            )?);
        }
        let layout = crate::aggregate::lower_output_layout_with_creation(
            package,
            node,
            slots,
            width,
            "local_grouped_topn",
            &mut work,
            creation,
        )?;
        let limit = usize::try_from(*limit)
            .map_err(|_| FragmentCompileError::Invalid("GroupedTopN limit exceeds host range"))?;
        Ok((
            ProgramNodeKind::GroupedTopN {
                input,
                group_by: groups,
                order_by: keys,
                rank_group_ordinals: permutation,
                limit,
                comparator: *comparator,
                functions,
            },
            layout,
        ))
    })();
    crate::creation::finish_attempt(result, work)
}
