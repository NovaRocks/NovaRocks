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

//! Normalize every proved UnionAll branch with the existing Project vocabulary.
//! The sole channel and expression authors supply exact input occurrences and
//! definitions. This leaf neither casts values nor creates runtime capabilities.
//! Type/layout clones and opaque Arc construction remain caller admission work,
//! not a host allocation grant or a model of Arrow's internal copy behavior.

use crate::{FragmentCompileError, assert_rows::reserve_vec, channels::UnionChannelBranch};
use novarocks_local_program::{
    DiagnosticSourceNodeId, MAX_PROGRAM_NODES, MAX_PROGRAM_TYPED_CHANNELS, MAX_STATIC_EXPRESSIONS,
    ProgramChannelLayoutRole, ProgramChannelSite, ProgramExprId, ProgramNode, ProgramNodeId,
    ProgramNodeKind, StaticLayout,
};
use novarocks_physical_plan::{FragmentPackage, NodeKind, PhysicalNode, SetOperationKind};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use novarocks_types::SlotId;
use std::alloc::Layout;

/// Emit ordered subordinate normalizers followed by the actual UnionAll.
/// `definitions` are already authored SlotId reads with each source's complete
/// type; their arena, lexical bindings, total counts and identity allocation
/// remain with the original compiler owners. Shared layouts preserve the
/// published output types without changing the source expression definitions.
pub(crate) fn lower_union_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    local: ProgramNodeId,
    branches: &[UnionChannelBranch],
    definitions: &[Vec<ProgramExprId>],
    slots: &[SlotId],
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<Vec<ProgramNode>, crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core_with_creation(
        package,
        node,
        local,
        branches,
        definitions,
        slots,
        &mut work,
        creation,
    );
    crate::creation::finish_attempt(result, work)
}

fn lower_core_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    local: ProgramNodeId,
    branches: &[UnionChannelBranch],
    definitions: &[Vec<ProgramExprId>],
    slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<Vec<ProgramNode>, crate::FragmentCreationError<H>> {
    let fragment = package.fragment();
    let original = fragment.nodes().get(&node.id);
    let same = original.is_some_and(|original| std::ptr::eq(original, node));
    work.step()?;
    if !same {
        return Err((FragmentCompileError::Invalid(
            "UnionAll source is outside its original fragment",
        ))
        .into());
    }
    let NodeKind::SetOp {
        kind: SetOperationKind::UnionAll,
        input_mappings,
    } = &node.kind
    else {
        return Err((FragmentCompileError::Invalid("UnionAll source kind differs")).into());
    };
    let width = node.output.columns.len();
    let shape = node.inputs.len() >= 2
        && input_mappings.len() == node.inputs.len()
        && branches.len() == node.inputs.len()
        && definitions.len() == branches.len()
        && slots.len() == width;
    work.step()?;
    if !shape {
        return Err((FragmentCompileError::Invalid(
            "UnionAll branch or output occurrence shape differs",
        ))
        .into());
    }
    // Bound this leaf's real expanded contributions before traversing inner
    // mappings or reserving outputs. The final owners also admit global totals.
    let node_count = branches
        .len()
        .checked_add(1)
        .filter(|&count| count <= MAX_PROGRAM_NODES)
        .ok_or(CompileControlError::ResourceExhausted)?;
    let expression_count = branches
        .len()
        .checked_mul(width)
        .filter(|&count| count <= MAX_STATIC_EXPRESSIONS)
        .ok_or(CompileControlError::ResourceExhausted)?;
    let channel_count = node_count
        .checked_mul(width)
        .filter(|&count| count <= MAX_PROGRAM_TYPED_CHANNELS)
        .ok_or(CompileControlError::ResourceExhausted)?;
    Layout::array::<ProgramNode>(node_count).map_err(|_| CompileControlError::ResourceExhausted)?;
    Layout::array::<ProgramExprId>(expression_count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    Layout::array::<SlotId>(channel_count).map_err(|_| CompileControlError::ResourceExhausted)?;
    work.step()?;

    for ((branch, definition), (&child_id, mapping)) in branches
        .iter()
        .zip(definitions)
        .zip(node.inputs.iter().zip(input_mappings))
    {
        let child = fragment.nodes().get(&child_id);
        let shape = mapping.len() == width
            && branch.sources.len() == width
            && definition.len() == width
            && branch.normalizer != local
            && branch.normalizer != branch.input
            && branch.input != local;
        work.step()?;
        if !shape {
            return Err((FragmentCompileError::Invalid(
                "UnionAll normalization branch shape differs",
            ))
            .into());
        }
        let child = child.ok_or(FragmentCompileError::Invalid(
            "UnionAll physical input is absent",
        ))?;
        for ((source, &mapped), &output) in
            branch.sources.iter().zip(mapping).zip(&node.output.columns)
        {
            let source_ordinal = match source.input.source {
                ProgramChannelSite::Layout {
                    node: owner,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal,
                } if owner == branch.input => usize::try_from(ordinal).ok(),
                _ => None,
            };
            let same = source_ordinal.and_then(|ordinal| child.output.columns.get(ordinal))
                == Some(&mapped);
            let source_value = fragment.values().get(&mapped);
            let output_value = fragment.values().get(&output);
            work.step()?;
            if !same {
                return Err((FragmentCompileError::Invalid(
                    "UnionAll mapping differs from its proved input occurrence",
                ))
                .into());
            }
            let source_value = source_value.ok_or(FragmentCompileError::Invalid(
                "UnionAll mapped value is absent",
            ))?;
            let output_value = output_value.ok_or(FragmentCompileError::Invalid(
                "UnionAll output value is absent",
            ))?;
            exact_type(&source.ty, &source_value.ty, work)?;
            let widens = !source_value.ty.nullable() || output_value.ty.nullable();
            work.step()?;
            if !widens {
                return Err((FragmentCompileError::Invalid(
                    "UnionAll output narrows input nullability",
                ))
                .into());
            }
            work.flush()?;
            if !source_value
                .ty
                .same_physical_binding_ignoring_root_nullability_observed(
                    &output_value.ty,
                    || work.step().map_err(FragmentCompileError::from),
                )?
            {
                return Err((FragmentCompileError::Invalid(
                    "UnionAll complete value types differ",
                ))
                .into());
            }
            work.flush()?;
        }
    }

    let layout = output_layout_with_creation(package, node, local, slots, work, creation)?;
    let mut nodes = Vec::new();
    reserve_vec(&mut nodes, node_count, work)?;
    let mut inputs = Vec::new();
    reserve_vec(&mut inputs, branches.len(), work)?;
    let diagnostic = DiagnosticSourceNodeId::new(node.id.get());
    for (branch, definitions) in branches.iter().zip(definitions) {
        let exprs = copy_items(definitions, work)?;
        let expr_slot_ids = copy_items(slots, work)?;
        let sources = source_diagnostic(diagnostic, work)?;
        let kind = ProgramNodeKind::Project {
            input: branch.input,
            is_subordinate: true,
            validate_final_result_input: false,
            exprs,
            expr_slot_ids,
            expr_slot_schemas: None,
            output_indices: None,
        };
        let output = layout.clone();
        work.step()?;
        work.flush()?;
        let normalizer = ProgramNode::new_local(branch.normalizer, sources, kind, output);
        work.step()?;
        work.flush()?;
        nodes.push(normalizer);
        inputs.push(branch.normalizer);
        work.step()?;
    }
    let sources = source_diagnostic(diagnostic, work)?;
    work.flush()?;
    let union =
        ProgramNode::new_local(local, sources, ProgramNodeKind::UnionAll { inputs }, layout);
    work.step()?;
    work.flush()?;
    nodes.push(union);
    work.step()?;
    Ok(nodes)
}

fn exact_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    work.flush()?;
    let same = left.exactly_equals_observed::<FragmentCompileError>(right, || {
        work.step().map_err(Into::into)
    })?;
    work.flush()?;
    if !same {
        return Err(FragmentCompileError::Invalid(
            "UnionAll complete value types differ",
        ));
    }
    Ok(())
}

fn output_layout_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    local: ProgramNodeId,
    slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<StaticLayout, crate::FragmentCreationError<H>> {
    if let Some(layout) =
        crate::writer::finish_input_layout_with_creation(package, node, slots, work, creation)?
    {
        return Ok(layout);
    }
    let mut result_names = package
        .result()
        .filter(|result| result.output.columns.len() == node.output.columns.len());
    if let Some(result) = result_names {
        for (actual, expected) in node.output.columns.iter().zip(&result.output.columns) {
            let same = actual == expected;
            work.step()?;
            if !same {
                result_names = None;
                break;
            }
        }
    }
    let mut typed_layout =
        crate::typed_layout::TypedLayoutBuilder::new_with_creation(slots.len(), work, creation)?;
    for (ordinal, &value) in node.output.columns.iter().enumerate() {
        let definition = package.fragment().values().get(&value);
        let result_name = result_names.and_then(|result| result.fields.get(ordinal));
        work.step()?;
        let definition = definition.ok_or(FragmentCompileError::Invalid(
            "UnionAll output value is absent",
        ))?;
        let name = if let Some(field) = result_name {
            copy_name_with_creation(
                field.alias.as_deref().unwrap_or(&field.name),
                work,
                creation,
            )?
        } else {
            work.flush()?;
            let name = crate::typed_layout::local_name_with_creation(
                "local",
                local.index(),
                ordinal,
                work,
                creation,
            )?;
            work.step()?;
            work.flush()?;
            name
        };
        work.flush()?;
        typed_layout.push_with_creation(&definition.ty, name, work, creation)?;
        work.step()?;
    }
    let layout = typed_layout.finish_with_creation(slots, work, creation)?;
    work.flush()?;
    Ok(layout)
}

fn copy_items<T: Copy>(
    source: &[T],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<T>, FragmentCompileError> {
    let mut copied = Vec::new();
    reserve_vec(&mut copied, source.len(), work)?;
    for &item in source {
        copied.push(item);
        work.step()?;
    }
    Ok(copied)
}

fn source_diagnostic(
    source: DiagnosticSourceNodeId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<DiagnosticSourceNodeId>, FragmentCompileError> {
    let mut sources = Vec::new();
    reserve_vec(&mut sources, 1, work)?;
    sources.push(source);
    work.step()?;
    Ok(sources)
}

fn copy_name_with_creation<H>(
    source: &str,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<String, crate::FragmentCreationError<H>> {
    crate::typed_layout::copy_name_with_creation(source, work, creation).map_err(Into::into)
}
