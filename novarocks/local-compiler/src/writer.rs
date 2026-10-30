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

//! Admit and lower the writer family: one `TableWriter` rooting its writer
//! fragment and one `TableFinish` rooting the finish fragment.
//!
//! Writer relations are positional. Every relation layout (the writer's
//! multiplex output, the writer-result receiver and the finish's root result)
//! is named by its frozen relation field names and typed by its frozen field
//! types, with compiler-allocated slots; nothing is keyed by the SPI's
//! reserved relation slot IDs.
//!
//! The writer projection is a separate `WriterProjection` arena of synthetic
//! slot reads, one per target input value, read from the writer's actual
//! input occurrence. Its layout is the provider recipe's own input fields, so
//! the provider names and field metadata are kept exactly; only a field's
//! top-level nullability follows the value that feeds it. A nullable value
//! feeding a non-null target field is the writer's row obligation, checked by
//! the executor before any provider I/O.
//!
//! Writer statistics are compiled by `writer_statistics`: a writer's partial
//! calls write its multiplex relation's auxiliary channels, and a finish's
//! final calls and grouped Unpivot expand them into Root artifact rows.
//! The finish's frozen writer-input purpose survives local multi-writer merging.
//! A partitioned receiver
//! retains its exact edge keys for the original per-driver local shuffle.

use crate::{assert_rows::reserve_vec, lowering::FragmentCompileError};
use novarocks_connector_contract::ConnectorWriteRecipe;
use novarocks_local_program::{
    BindingRequirement, ExpressionsCompileError, ImmutableExpressions, ProgramChannelLayoutRole,
    ProgramChannelSite, ProgramControlFlow, ProgramEvaluationDomain, ProgramExprId,
    ProgramExpressionArena, ProgramExpressionRootSite, ProgramExpressionUse, ProgramLexicalSource,
    ProgramNodeId, ProgramNodeKind, ProgramRootUseBinding, ProgramSlotBinding, ProgramUseRef,
    StaticExprKind, StaticExprNode, StaticLayout, StaticWriterProjection,
};
use novarocks_physical_plan::{
    Distribution, EdgeKind, ExprId, FragmentPackage, FragmentSink, NodeKind, OutboundFragmentCut,
    PhysicalNode, ValueId, WriterRelationSchema,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, FunctionArgumentType,
    FunctionValueType, PureCompileControl,
};
use novarocks_types::SlotId;
use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroUsize,
    sync::Arc,
};

/// Additional typed channels one writer-family node owns beyond its output
/// occurrences: the writer's projection and multiplex roles, or the finish's
/// multiplex and root-result roles and one final output per final call.
pub(crate) fn extra_channels(package: &FragmentPackage, node: &PhysicalNode) -> Option<usize> {
    match &node.kind {
        NodeKind::TableWriter { target } => {
            node.output.columns.len().checked_add(target.input.len())
        }
        NodeKind::TableFinish(spec) => {
            let input = node
                .inputs
                .get(spec.inputs.writer_results as usize)
                .and_then(|input| package.fragment().nodes().get(input))
                .map_or(0, |input| input.output.columns.len());
            node.output
                .columns
                .len()
                .checked_add(input)?
                .checked_add(spec.final_aggregates.len())
        }
        _ => Some(0),
    }
}

/// Admit one writer-family node before channels or expressions exist.
///
/// A `TableWriter` is the one root of its fragment and streams exactly its
/// writer relation; a partitioned input at multiple drivers requires an exact
/// exchange receiver whose edge keys can be shuffled locally. A
/// `TableFinish` is the root of a Result fragment over its writer multiplex
/// relation and, for logical counts, an independent summary receiver.
/// Statistics are admitted by their own owner.
pub(crate) fn admit_writer_family(
    package: &FragmentPackage,
    node: &PhysicalNode,
    recipe: Option<&ConnectorWriteRecipe>,
    pipeline_dop: NonZeroUsize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    let fragment = package.fragment();
    let unsupported = |feature| FragmentCompileError::Unsupported {
        node: Some(node.id),
        feature,
    };
    work.step()?;
    if node.id != fragment.root() {
        return Err(unsupported(
            "writer-family node below its fragment root (one writer per fragment)",
        ));
    }
    match &node.kind {
        NodeKind::TableWriter { target } => {
            if !matches!(fragment.sink(), FragmentSink::Stream { .. }) {
                return Err(unsupported("table writer without a writer-result stream"));
            }
            // An edge co-locates keys per instance. The runtime uses those
            // exact receiver keys to restore co-location per writer driver.
            if matches!(
                target.required_distribution,
                Distribution::Hash { .. } | Distribution::BucketShuffle { .. }
            ) && pipeline_dop.get() != 1
            {
                let input = match node.inputs.as_ref() {
                    [id] => fragment.nodes().get(id),
                    _ => None,
                };
                let mut exact = false;
                if let Some(input) = input
                    && matches!(input.kind, NodeKind::ExchangeSource { .. })
                {
                    for cut in package.cuts().inbound.iter() {
                        let addressed = cut.destination_node == input.id;
                        let same = cut.partitioning.destination == target.required_distribution;
                        work.step()?;
                        exact |= addressed && same;
                    }
                }
                if !exact {
                    return Err(unsupported("partitioned writer input at pipeline DOP > 1"));
                }
            }
            let recipe = recipe.ok_or(FragmentCompileError::Invalid(
                "table writer has no provider write recipe",
            ))?;
            let fields = recipe.draft().input().field_count();
            work.step()?;
            if target.input.len() != target.target_fields.len()
                || target.input.len() != fields
                || target.input.is_empty()
            {
                return Err(FragmentCompileError::Invalid(
                    "table writer input, target fields and recipe input differ in width",
                ));
            }
            for (value, field) in target.input.iter().zip(target.target_fields.iter()) {
                let same = field.input == *value;
                work.step()?;
                if !same {
                    return Err(FragmentCompileError::Invalid(
                        "table writer target field reads a foreign input value",
                    ));
                }
            }
            require_relation_output(node, &target.output_schema, work)?;
            crate::writer_statistics::admit_partial_calls(node, target, work)
        }
        NodeKind::TableFinish(spec) => {
            if !matches!(
                fragment.sink(),
                FragmentSink::Result | FragmentSink::RootResult(_)
            ) {
                return Err(unsupported("table finish without a Result sink"));
            }
            let input = node
                .inputs
                .get(spec.inputs.writer_results as usize)
                .and_then(|id| fragment.nodes().get(id))
                .ok_or(FragmentCompileError::Invalid(
                    "missing table finish writer input",
                ))?;
            let logical =
                spec.count_mode == novarocks_physical_plan::FinishCountMode::LogicalChanges;
            if node.inputs.len() != if logical { 2 } else { 1 } {
                return Err(FragmentCompileError::Invalid(
                    "table finish input roles differ",
                ));
            }
            if logical {
                let summary = spec
                    .inputs
                    .logical_summary
                    .and_then(|port| node.inputs.get(port as usize))
                    .and_then(|id| fragment.nodes().get(id))
                    .ok_or(FragmentCompileError::Invalid(
                        "missing logical finish summary input",
                    ))?;
                let mut carried = false;
                for cut in package.cuts().inbound.iter() {
                    carried |= cut.destination_node == summary.id
                        && cut.delivery == novarocks_physical_plan::DeliveryRequirement::MustDrain
                        && matches!(
                            cut.payload,
                            novarocks_physical_plan::ExchangePayloadContract::LogicalSummary(_)
                        );
                    work.step()?;
                }
                if !matches!(summary.kind, NodeKind::ExchangeSource { .. }) || !carried {
                    return Err(FragmentCompileError::Invalid(
                        "logical finish requires its exact summary receiver",
                    ));
                }
            }
            // The finish reads its input relation positionally: the receiver's
            // occurrences are exactly the frozen input schema.
            require_relation_output(input, &spec.input_schema, work)?;
            require_relation_output(node, &spec.output_schema, work)?;
            let result = package.result().ok_or(FragmentCompileError::Invalid(
                "table finish has no result port",
            ))?;
            let same = result.output == node.output;
            work.step()?;
            if !same {
                return Err(FragmentCompileError::Invalid(
                    "table finish result port differs from its root relation",
                ));
            }
            crate::writer_statistics::admit_finish_statistics(node, spec, work)
        }
        _ => Err(FragmentCompileError::Invalid("writer family kind differs")),
    }
}

/// A writer-result stream carries exactly the writer relation: its fields are
/// the root writer's output schema by order, name and type.
pub(crate) fn admit_writer_result_cut(
    package: &FragmentPackage,
    cut: &OutboundFragmentCut,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    let fragment = package.fragment();
    let root = fragment
        .nodes()
        .get(&fragment.root())
        .ok_or(FragmentCompileError::Invalid("missing stream root node"))?;
    let NodeKind::TableWriter { target } = &root.kind else {
        return Err(FragmentCompileError::Unsupported {
            node: None,
            feature: "writer-result stream from a fragment not rooted at a table writer",
        });
    };
    let writer_result = cut.writer_result().ok_or(FragmentCompileError::Invalid(
        "table writer stream carries no writer-result cut",
    ))?;
    if cut.kind != EdgeKind::Stream
        || writer_result.write_target_ordinal != target.write_target_ordinal
        || writer_result.schema_revision != target.output_schema.revision
        || writer_result.fields.len() != target.output_schema.fields.len()
    {
        return Err(FragmentCompileError::Invalid(
            "writer-result cut differs from its writer relation",
        ));
    }
    for (carried, field) in writer_result
        .fields
        .iter()
        .zip(target.output_schema.fields.iter())
    {
        let same = carried.source == field.value
            && carried.name == field.name
            && carried.role == field.role
            && carried.ty == field.ty;
        work.step()?;
        if !same {
            return Err(FragmentCompileError::Invalid(
                "writer-result cut field differs from its writer relation field",
            ));
        }
    }
    Ok(())
}

/// The node's output occurrences are its relation's field values, in order.
fn require_relation_output(
    node: &PhysicalNode,
    schema: &WriterRelationSchema,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    if node.output.columns.len() != schema.fields.len() {
        return Err(FragmentCompileError::Invalid(
            "writer relation width differs from its output occurrences",
        ));
    }
    for (value, field) in node.output.columns.iter().zip(schema.fields.iter()) {
        let same = *value == field.value;
        work.step()?;
        if !same {
            return Err(FragmentCompileError::Invalid(
                "writer relation field differs from its output occurrence",
            ));
        }
    }
    Ok(())
}

/// One relation layout: the frozen field names and types, positionally, with
/// compiler-allocated slots.
fn relation_layout_inner_with_creation<H>(
    schema: &WriterRelationSchema,
    slots: &[SlotId],
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<StaticLayout, crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = (|| {
        if slots.len() != schema.fields.len() {
            return Err((FragmentCompileError::Invalid(
                "writer relation width differs from its channels",
            ))
            .into());
        }
        let mut typed_layout = crate::typed_layout::TypedLayoutBuilder::new_with_creation(
            slots.len(),
            &mut work,
            creation,
        )?;
        for field in schema.fields.iter() {
            typed_layout.push_with_creation(
                &field.ty,
                crate::typed_layout::copy_name_with_creation(&field.name, &mut work, creation)?,
                &mut work,
                creation,
            )?;
        }
        typed_layout
            .finish_with_creation(slots, &mut work, creation)
            .map_err(Into::into)
    })();
    crate::creation::finish_attempt(result, work)
}

/// The finish's frozen writer-input declaration also owns a local merger's
/// output purpose. Generic UnionAll labels cannot replace protocol field names.
pub(crate) fn finish_input_layout_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<Option<StaticLayout>, crate::FragmentCreationError<H>> {
    let root = package
        .fragment()
        .nodes()
        .get(&package.fragment().root())
        .ok_or(FragmentCompileError::Invalid("missing fragment root"))?;
    work.step()?;
    let NodeKind::TableFinish(spec) = &root.kind else {
        return Ok(None);
    };
    let input = root
        .inputs
        .get(spec.inputs.writer_results as usize)
        .ok_or(FragmentCompileError::Invalid("missing finish writer input"))?;
    work.step()?;
    if *input != node.id {
        return Ok(None);
    }
    require_relation_output(node, &spec.input_schema, work)?;
    work.flush()?;
    relation_layout_inner_with_creation(&spec.input_schema, slots, work.control(), creation)
        .map(Some)
}

/// The complete WriterProjection-arena scope of one writer: its flow and
/// types, the root binding of each projected field and each read's exact
/// input occurrence.
pub(crate) struct WriterFlow {
    pub arena: ProgramExpressionArena,
    pub flow: ProgramControlFlow,
    pub types: novarocks_type_contract::FunctionArgumentTypes,
    pub roots: Vec<ProgramRootUseBinding>,
    pub slots: Vec<ProgramSlotBinding>,
}

pub(crate) struct LoweredWriter {
    pub kind: ProgramNodeKind,
    pub layout: StaticLayout,
    pub requirement: BindingRequirement,
    /// Typed channels of the writer's projection and multiplex roles; the
    /// node-output channels are pushed by the caller as for every node.
    pub channels: Vec<(ProgramChannelSite, FunctionValueType)>,
    pub flow: WriterFlow,
}

/// The writer's actual input and the slots its relations own.
pub(crate) struct WriterLoweringInput<'a> {
    pub child: ProgramNodeId,
    pub child_node: &'a PhysicalNode,
    pub child_layout: &'a StaticLayout,
    /// Fresh slots of the multiplex output relation, in field order.
    pub output_slots: &'a [SlotId],
    /// Fresh slots of the projected provider input, in recipe field order.
    pub projection_slots: &'a [SlotId],
}

pub(crate) fn lower_writer_with_creation<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    id: ProgramNodeId,
    recipe: &ConnectorWriteRecipe,
    input: WriterLoweringInput<'_>,
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    mut temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
) -> Result<LoweredWriter, crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_writer_core_with_creation(
        package,
        node,
        id,
        recipe,
        input,
        &mut work,
        creation,
        crate::creation::borrow_scope(&mut temporary),
        factory,
    );
    crate::creation::finish_attempt(result, work)
}

fn lower_writer_core_with_creation<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    id: ProgramNodeId,
    recipe: &ConnectorWriteRecipe,
    input: WriterLoweringInput<'_>,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    mut temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
) -> Result<LoweredWriter, crate::FragmentCreationError<H>> {
    let NodeKind::TableWriter { target } = &node.kind else {
        return Err((FragmentCompileError::Invalid("TableWriter kind differs")).into());
    };
    let values = package.fragment().values();
    let recipe_fields = recipe.draft().input();
    let width = target.input.len();
    work.step()?;
    if width != recipe_fields.field_count()
        || width != input.projection_slots.len()
        || input.child_layout.slots().len() != input.child_node.output.columns.len()
    {
        return Err((FragmentCompileError::Invalid(
            "table writer projection width differs from its recipe or channels",
        ))
        .into());
    }
    let mut nodes = Vec::new();
    let mut type_scope = match factory {
        Some(f) => {
            work.flush()?;
            Some(
                f.begin_scope()
                    .map_err(crate::FragmentCreationError::Creator)?,
            )
        }
        None => None,
    };
    let mut types = Vec::new();
    let mut typed_layout =
        crate::typed_layout::TypedLayoutBuilder::new_with_creation(width, work, creation)?;
    let mut ordinals = Vec::new();
    let mut channels = Vec::new();
    reserve_vec(&mut nodes, width, work)?;
    match type_scope.as_mut() {
        Some(scope) => crate::creation::reserve_vec(&mut types, width, Some(scope), work)?,
        None => reserve_vec(&mut types, width, work)?,
    }
    reserve_vec(&mut ordinals, width, work)?;
    reserve_vec(
        &mut channels,
        width
            .checked_add(node.output.columns.len())
            .ok_or(CompileControlError::ResourceExhausted)?,
        work,
    )?;
    for (ordinal, (value, binding)) in target
        .input
        .iter()
        .zip(recipe_fields.fields_iter())
        .enumerate()
    {
        // Each projected field reads the first input occurrence of its value;
        // a repeated input value is one value.
        let source = first_ordinal(&input.child_node.output.columns, *value, work)?.ok_or(
            FragmentCompileError::Invalid("table writer input value is outside its input port"),
        )?;
        let ty = &values
            .get(value)
            .ok_or(FragmentCompileError::Invalid(
                "missing table writer input type",
            ))?
            .ty;
        work.flush()?;
        nodes.push(StaticExprNode::new_value_typed(
            StaticExprKind::SlotId(input.child_layout.slots()[source]),
            ty.clone_observed(|| work.step())?,
        ));
        types.push(FunctionArgumentType::Value(
            ty.clone_observed(|| work.step())?,
        ));
        // Provider attributes stay on the write recipe. This execution
        // projection is authored by the complete input value binding.
        typed_layout.push_with_creation(
            ty,
            crate::typed_layout::copy_name_with_creation(binding.field().name(), work, creation)?,
            work,
            creation,
        )?;
        channels.push((
            ProgramChannelSite::Layout {
                node: id,
                role: ProgramChannelLayoutRole::WriterProjection,
                ordinal: u32::try_from(ordinal)
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
            },
            ty.clone(),
        ));
        ordinals.push(u32::try_from(source).map_err(|_| CompileControlError::ResourceExhausted)?);
        work.step()?;
    }
    work.flush()?;
    let projection_layout =
        typed_layout.finish_with_creation(input.projection_slots, work, creation)?;
    work.flush()?;
    // A slot read needs no exception, dictionary or session-timezone
    // capability; the arena is distinct from the Main arena by design.
    let types = crate::creation::publish_argument_types(
        types,
        type_scope
            .as_mut()
            .map(|s| s as &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>),
        work,
    )
    .map_err(crate::creation::argument_types_failure)?;
    let arena = ImmutableExpressions::try_new_for_compile(
        nodes,
        false,
        HashMap::new(),
        None,
        work.control(),
    )
    .map_err(|error| match error {
        ExpressionsCompileError::Control(cause) => FragmentCompileError::Control(cause),
        error => FragmentCompileError::Owner {
            phase: "writer projection expressions",
            error: novarocks_failure_contract::SharedFailureCause::new(error),
        },
    })?;
    let arena = Arc::new(arena);
    work.flush()?;
    let multiplex = relation_layout_inner_with_creation(
        &target.output_schema,
        input.output_slots,
        work.control(),
        creation,
    )?;
    for (ordinal, field) in target.output_schema.fields.iter().enumerate() {
        channels.push((
            ProgramChannelSite::Layout {
                node: id,
                role: ProgramChannelLayoutRole::WriterMultiplex,
                ordinal: u32::try_from(ordinal)
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
            },
            field.ty.clone(),
        ));
        work.step()?;
    }
    let partial_aggregates = crate::writer_statistics::lower_partial_calls(
        target,
        input.projection_slots,
        input.output_slots,
        work,
        creation,
    )?;
    let arena_id = ProgramExpressionArena::WriterProjection(id);
    let flow = projection_flow(
        id,
        input.child,
        arena_id,
        &ordinals,
        work,
        crate::creation::borrow_scope(&mut temporary),
        creation,
        factory,
    )?;
    work.flush()?;
    Ok(LoweredWriter {
        kind: ProgramNodeKind::TableWriter {
            input: input.child,
            target: target.write_target_ordinal,
            expected_layout: projection_layout.clone(),
            projection: StaticWriterProjection {
                arena,
                expressions: (0..width).map(ProgramExprId::new).collect(),
                layout: projection_layout,
            },
            writer_multiplex_layout: multiplex.clone(),
            // This carrier's exact prepared handles are in its state templates.
            // It never enters the PlanTree/local independent-source bridge.
            partial_preparation: None,
            partial_aggregates,
        },
        requirement: BindingRequirement::TableWriter {
            node: id,
            layout: multiplex.clone(),
        },
        layout: multiplex,
        channels,
        flow: WriterFlow {
            arena: arena_id,
            flow: flow.0,
            types,
            roots: flow.1,
            slots: flow.2,
        },
    })
}

type ProjectionFlow = (
    ProgramControlFlow,
    Vec<ProgramRootUseBinding>,
    Vec<ProgramSlotBinding>,
);

/// One root domain and one eager Value occurrence per projected field `i`,
/// bound to `WriterProjection { node, expression: i }` and read from the
/// writer input's actual output occurrence. Identities are dense in this
/// fresh arena scope.
fn projection_flow<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    writer: ProgramNodeId,
    input: ProgramNodeId,
    arena: ProgramExpressionArena,
    ordinals: &[u32],
    work: &mut CompileCheckpoints<'_>,
    mut temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    _creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    factory: Option<&F>,
) -> Result<ProjectionFlow, crate::FragmentCreationError<H>> {
    let mut domains = Vec::new();
    let mut uses = Vec::new();
    let mut roots = Vec::new();
    let mut slots = Vec::new();
    crate::creation::reserve_vec(
        &mut domains,
        ordinals.len(),
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    crate::creation::reserve_vec(
        &mut uses,
        ordinals.len(),
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    crate::creation::reserve_vec(
        &mut roots,
        ordinals.len(),
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    crate::creation::reserve_vec(
        &mut slots,
        ordinals.len(),
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    for (expression, &ordinal) in ordinals.iter().enumerate() {
        let identity =
            u32::try_from(expression).map_err(|_| CompileControlError::ResourceExhausted)?;
        let domain = EvaluationDomainId::new(identity);
        let use_id = ExpressionUseId::new(identity);
        domains.push(ProgramEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        });
        uses.push(ProgramExpressionUse {
            context: ExpressionEffectContext {
                use_id,
                domain,
                demand: EvaluationDemand::Value,
            },
            definition: ProgramExprId::new(expression),
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        roots.push(ProgramRootUseBinding {
            site: ProgramExpressionRootSite::WriterProjection {
                node: writer,
                expression: identity,
            },
            use_id,
        });
        slots.push(ProgramSlotBinding {
            occurrence: ProgramUseRef { arena, use_id },
            source: ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                node: input,
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal,
            }),
        });
        work.step()?;
    }
    work.flush()?;
    let flow = match factory {
        Some(f) => {
            let mut retained = f
                .begin_scope()
                .map_err(crate::FragmentCreationError::Creator)?;
            ProgramControlFlow::try_new_with_creator_in(
                domains,
                uses,
                ordinals.len(),
                crate::creation::borrow_scope(&mut temporary)
                    .expect("strong entry supplies temporary scope"),
                &mut retained,
                work.control(),
            )?
        }
        None => ProgramControlFlow::try_new(domains, uses, ordinals.len(), work.control())?,
    };
    work.flush()?;
    Ok((flow, roots, slots))
}

pub(crate) struct LoweredFinish {
    pub kind: ProgramNodeKind,
    pub layout: StaticLayout,
    pub requirement: BindingRequirement,
    /// Typed channels of the finish's multiplex and root-result roles and of
    /// each final call's output.
    pub channels: Vec<(ProgramChannelSite, FunctionValueType)>,
}

/// The finish's actual input and the slots its relations and statistics own.
pub(crate) struct FinishLoweringInput<'a> {
    pub child: ProgramNodeId,
    pub child_layout: &'a StaticLayout,
    pub summary: Option<(ProgramNodeId, &'a StaticLayout)>,
    /// Fresh slots of the Root relation, in field order.
    pub slots: &'a [SlotId],
    /// Fresh internal slots of the statistics: the grouped Unpivot's
    /// grouping output followed by one final output per call; empty without
    /// statistics.
    pub statistics_slots: &'a [SlotId],
    /// The Main-arena definition of each physical expression, read by the
    /// grouped Unpivot's scalar constants.
    pub expressions: &'a BTreeMap<ExprId, ProgramExprId>,
}

/// Lower one admitted finish over its writer multiplex and optional summary. The finish
/// reads the receiver's layout unchanged as its multiplex relation and
/// publishes its root relation with the frozen relation names.
pub(crate) fn lower_finish_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    id: ProgramNodeId,
    input: FinishLoweringInput<'_>,
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<LoweredFinish, crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = (|| {
        let NodeKind::TableFinish(spec) = &node.kind else {
            return Err((FragmentCompileError::Invalid("TableFinish kind differs")).into());
        };
        let (child, child_layout, slots) = (input.child, input.child_layout, input.slots);
        if child_layout.slots().len() != spec.input_schema.fields.len() {
            return Err((FragmentCompileError::Invalid(
                "table finish input layout differs from its input relation",
            ))
            .into());
        }
        work.flush()?;
        let root = relation_layout_inner_with_creation(
            &spec.output_schema,
            slots,
            work.control(),
            creation,
        )?;
        let statistics = crate::writer_statistics::lower_finish_statistics(
            package,
            spec,
            crate::writer_statistics::FinishStatisticsChannels {
                node: id,
                input_slots: child_layout.slots(),
                root_slots: slots,
                statistics_slots: input.statistics_slots,
                expressions: input.expressions,
            },
            &mut work,
            creation,
        )?;
        let mut channels = Vec::new();
        reserve_vec(
            &mut channels,
            spec.input_schema
                .fields
                .len()
                .checked_add(spec.output_schema.fields.len())
                .and_then(|count| count.checked_add(statistics.channels.len()))
                .ok_or(CompileControlError::ResourceExhausted)?,
            &mut work,
        )?;
        for (role, schema) in [
            (
                ProgramChannelLayoutRole::WriterMultiplex,
                &spec.input_schema,
            ),
            (
                ProgramChannelLayoutRole::WriterRootResult,
                &spec.output_schema,
            ),
        ] {
            for (ordinal, field) in schema.fields.iter().enumerate() {
                channels.push((
                    ProgramChannelSite::Layout {
                        node: id,
                        role,
                        ordinal: u32::try_from(ordinal)
                            .map_err(|_| CompileControlError::ResourceExhausted)?,
                    },
                    field.ty.clone(),
                ));
                work.step()?;
            }
        }
        channels.extend(statistics.channels);
        work.flush()?;
        Ok(LoweredFinish {
            kind: ProgramNodeKind::TableFinish {
                count_mode: spec.count_mode,
                input_roles: spec.inputs,
                logical_summary_layout: input.summary.map(|(_, layout)| layout.clone()),
                inputs: {
                    let mut inputs = vec![child; node.inputs.len()];
                    inputs[spec.inputs.writer_results as usize] = child;
                    if let Some((summary, _)) = input.summary {
                        inputs[spec
                            .inputs
                            .logical_summary
                            .ok_or(FragmentCompileError::Invalid("summary input has no role"))?
                            as usize] = summary;
                    }
                    inputs
                },
                expected_targets: spec.expected_target_ordinals.to_vec(),
                writer_multiplex_layout: child_layout.clone(),
                root_result_layout: root.clone(),
                final_aggregates: statistics.plan,
            },
            requirement: BindingRequirement::TableFinish {
                node: id,
                layout: root.clone(),
            },
            layout: root,
            channels,
        })
    })();
    crate::creation::finish_attempt(result, work)
}

fn first_ordinal(
    columns: &[ValueId],
    value: ValueId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<usize>, FragmentCompileError> {
    for (ordinal, candidate) in columns.iter().enumerate() {
        let found = *candidate == value;
        work.step()?;
        if found {
            return Ok(Some(ordinal));
        }
    }
    Ok(None)
}
