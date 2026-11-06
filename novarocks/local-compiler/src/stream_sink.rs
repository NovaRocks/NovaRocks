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

//! Exact branch-local stream projection and one shared Sink expression arena.

use crate::{assert_rows::reserve_vec, lowering::FragmentCompileError};
use novarocks_local_program::*;
use novarocks_physical_plan::{
    Distribution, EdgeId, FragmentPackage, FragmentSink, OutboundFragmentCut, ValueId,
};
use novarocks_type_contract::{
    BucketLayoutAlgorithm, CompileCheckpoints, CompileControlError, CompilePhase, ControlShape,
    EvaluationDemand, EvaluationDomainId, ExpressionEffectContext, ExpressionUseId,
    FunctionArgumentType, PartitionHashAlgorithm, PureCompileControl,
};
use novarocks_types::SlotId;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

pub(crate) struct SinkFlow {
    pub flow: ProgramControlFlow,
    pub types: novarocks_type_contract::FunctionArgumentTypes,
    pub roots: Vec<ProgramRootUseBinding>,
    pub slots: Vec<ProgramSlotBinding>,
}
pub(crate) struct LoweredStreamSink {
    pub sink: StaticSinkProgram,
    pub requirements: Vec<BindingRequirement>,
    pub flow: SinkFlow,
}
pub(crate) fn cut_role(
    role: novarocks_physical_plan::OutputRole,
) -> Result<ProgramOutputRole, FragmentCompileError> {
    Ok(match role {
        novarocks_physical_plan::OutputRole::Relational => ProgramOutputRole::Relational,
        novarocks_physical_plan::OutputRole::ProviderRoute { target } => {
            ProgramOutputRole::ProviderRoute { target }
        }
        novarocks_physical_plan::OutputRole::Summary(
            novarocks_physical_plan::SummaryKind::WriteCounts,
        ) => ProgramOutputRole::LogicalSummary,
        novarocks_physical_plan::OutputRole::Summary(
            novarocks_physical_plan::SummaryKind::DeleteVerification,
        ) => {
            return Err(FragmentCompileError::Invalid(
                "DeleteVerification has no admitted local summary author",
            ));
        }
        novarocks_physical_plan::OutputRole::WriterResults => ProgramOutputRole::WriterResults,
    })
}
pub(crate) fn cut_delivery(
    delivery: novarocks_physical_plan::DeliveryRequirement,
) -> ExchangeDeliveryRequirement {
    match delivery {
        novarocks_physical_plan::DeliveryRequirement::DemandDriven => {
            ExchangeDeliveryRequirement::DemandDriven
        }
        novarocks_physical_plan::DeliveryRequirement::MustDrain => {
            ExchangeDeliveryRequirement::MustDrain
        }
    }
}

enum OutputSelection<'a> {
    Stream(&'a OutboundFragmentCut),
    Router {
        aliases: &'a BTreeMap<EdgeId, Arc<[SlotId]>>,
        summary_slots: &'a [SlotId],
    },
}
pub(crate) fn lower_stream_sink_with_creation<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    package: &FragmentPackage,
    cut: &OutboundFragmentCut,
    root: ProgramNodeId,
    root_layout: &StaticLayout,
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
) -> Result<LoweredStreamSink, crate::FragmentCreationError<H>> {
    lower_selected(
        package,
        OutputSelection::Stream(cut),
        root,
        root_layout,
        control,
        creation,
        temporary,
        factory,
    )
}
pub(crate) fn lower_router_sink_with_creation<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    package: &FragmentPackage,
    root: ProgramNodeId,
    root_layout: &StaticLayout,
    aliases: &BTreeMap<EdgeId, Arc<[SlotId]>>,
    summary_slots: &[SlotId],
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
) -> Result<LoweredStreamSink, crate::FragmentCreationError<H>> {
    lower_selected(
        package,
        OutputSelection::Router {
            aliases,
            summary_slots,
        },
        root,
        root_layout,
        control,
        creation,
        temporary,
        factory,
    )
}
fn lower_selected<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    selection: OutputSelection<'_>,
    root: ProgramNodeId,
    root_layout: &StaticLayout,
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    mut temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
) -> Result<LoweredStreamSink, crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(
        package,
        selection,
        root,
        root_layout,
        &mut work,
        creation,
        crate::creation::borrow_scope(&mut temporary),
        factory,
    );
    crate::creation::finish_attempt(result, work)
}
struct SinkRead {
    site: ProgramExpressionRootSite,
    ordinal: u32,
    demand: EvaluationDemand,
}
fn lower_core<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    selection: OutputSelection<'_>,
    root: ProgramNodeId,
    root_layout: &StaticLayout,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    mut temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
) -> Result<LoweredStreamSink, crate::FragmentCreationError<H>> {
    let fragment = package.fragment();
    let root_values = &fragment
        .nodes()
        .get(&fragment.root())
        .ok_or(FragmentCompileError::Invalid("missing stream root"))?
        .output
        .columns;
    if root_values.len() > root_layout.slots().len() {
        return Err(FragmentCompileError::Invalid("stream root occurrence width differs").into());
    }
    work.flush()?;
    let mut type_scope = match factory {
        Some(f) => Some(
            f.begin_scope()
                .map_err(crate::FragmentCreationError::Creator)?,
        ),
        None => None,
    };
    work.flush()?;
    let mut nodes = Vec::new();
    let mut types = Vec::new();
    let mut reads = Vec::new();
    let mut branches = Vec::new();
    let mut requirements = Vec::new();
    let mut router_routes = Vec::new();
    let mut cuts = Vec::new();
    let router = match &selection {
        OutputSelection::Stream(cut) => {
            reserve_vec(&mut cuts, 1, work)?;
            cuts.push(*cut);
            None
        }
        OutputSelection::Router { .. } => {
            let FragmentSink::Router { effect, routes } = fragment.sink() else {
                return Err(FragmentCompileError::Invalid("router sink kind differs").into());
            };
            let summary = fragment
                .summaries()
                .iter()
                .find(|summary| summary.kind() == novarocks_physical_plan::SummaryKind::WriteCounts)
                .ok_or(FragmentCompileError::Invalid(
                    "Router has no WriteCounts summary",
                ))?;
            reserve_vec(
                &mut cuts,
                routes
                    .len()
                    .checked_add(1)
                    .ok_or(CompileControlError::ResourceExhausted)?,
                work,
            )?;
            let mut by_edge = BTreeMap::new();
            for cut in package.cuts().outbound.iter() {
                if by_edge.insert(cut.edge, cut).is_some() {
                    return Err(FragmentCompileError::Invalid("duplicate router output cut").into());
                }
                work.step()?;
            }
            for edge in routes
                .iter()
                .map(|route| route.edge)
                .chain(std::iter::once(summary.edge))
            {
                cuts.push(
                    by_edge
                        .remove(&edge)
                        .ok_or(FragmentCompileError::Invalid("missing router output cut"))?,
                );
                work.step()?;
            }
            if cuts.len() != package.cuts().outbound.len() {
                return Err(
                    FragmentCompileError::Invalid("router output cut coverage differs").into(),
                );
            }
            Some((*effect, routes, *summary))
        }
    };
    if cuts.len() > MAX_STATIC_SINK_BRANCHES {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    // The shared Sink arena is bounded before constructing any definition.
    let mut definitions = 0usize;
    for cut in &cuts {
        let count = match &cut.partitioning.source {
            Distribution::ConditionalKey { keys, .. } => keys.len().checked_add(1),
            Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. } => {
                Some(keys.len())
            }
            _ => Some(0),
        }
        .ok_or(CompileControlError::ResourceExhausted)?;
        definitions = definitions
            .checked_add(count)
            .ok_or(CompileControlError::ResourceExhausted)?;
        work.step()?;
    }
    if definitions > MAX_STATIC_EXPRESSIONS {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    reserve_vec(&mut nodes, definitions, work)?;
    reserve_vec(&mut reads, definitions, work)?;
    reserve_vec(&mut router_routes, cuts.len(), work)?;
    reserve_vec(&mut branches, cuts.len(), work)?;
    reserve_vec(&mut requirements, cuts.len(), work)?;
    let mut summary_layout = None;
    for (branch_index, cut) in cuts.iter().enumerate() {
        let branch_id =
            u32::try_from(branch_index).map_err(|_| CompileControlError::ResourceExhausted)?;
        let is_summary = matches!(
            cut.payload,
            novarocks_physical_plan::ExchangePayloadContract::Summary(_)
        );
        let (partition_type, keys, routing) = match &cut.partitioning.source {
            Distribution::Singleton | Distribution::Broadcast => (
                DataStreamPartitionType::Unpartitioned,
                &[][..],
                StaticStreamRouting::Ordinary,
            ),
            Distribution::RoundRobin => (
                DataStreamPartitionType::Random,
                &[][..],
                StaticStreamRouting::Ordinary,
            ),
            Distribution::Hash { keys, scheme } => {
                let routing = match scheme.definition.algorithm {
                    PartitionHashAlgorithm::NativeExchangeV1 => StaticStreamRouting::Ordinary,
                    PartitionHashAlgorithm::NativeHierarchicalV1 => {
                        StaticStreamRouting::HierarchicalHashV1
                    }
                    _ => {
                        return Err(FragmentCompileError::Invalid(
                            "hash stream has a foreign algorithm",
                        )
                        .into());
                    }
                };
                (
                    DataStreamPartitionType::HashPartitioned,
                    keys.as_ref(),
                    routing,
                )
            }
            Distribution::ConditionalKey {
                keys,
                applies,
                scheme,
            } => {
                if scheme.definition.algorithm != PartitionHashAlgorithm::NativeHierarchicalV1 {
                    return Err(FragmentCompileError::Invalid(
                        "conditional stream has a foreign algorithm",
                    )
                    .into());
                }
                let expression = add_read(
                    SinkReadInput {
                        fragment,
                        values: root_values,
                        layout: root_layout,
                    },
                    *applies,
                    ProgramExpressionRootSite::SinkApplicability { branch: branch_id },
                    EvaluationDemand::TruthOnly,
                    &mut nodes,
                    &mut types,
                    &mut reads,
                    &mut type_scope,
                    work,
                )?;
                (
                    DataStreamPartitionType::HashPartitioned,
                    keys.as_ref(),
                    StaticStreamRouting::ConditionalKeyV1 {
                        applies: expression,
                    },
                )
            }
            Distribution::BucketShuffle { keys, scheme } => {
                if scheme.hash != PartitionHashAlgorithm::NativeBucketCrc32V1
                    || scheme.layout != BucketLayoutAlgorithm::DenseZeroBasedV1
                {
                    return Err(
                        FragmentCompileError::Invalid("bucket stream definition differs").into(),
                    );
                }
                (
                    DataStreamPartitionType::BucketShuffleHashPartitioned,
                    keys.as_ref(),
                    StaticStreamRouting::Ordinary,
                )
            }
            Distribution::Unconstrained => {
                return Err(FragmentCompileError::Unsupported {
                    node: None,
                    feature: "unconstrained stream partitioning",
                }
                .into());
            }
        };
        if partition_type.requires_exprs() && keys.is_empty() {
            return Err(FragmentCompileError::Invalid("keyed stream has no keys").into());
        }
        let mut partition_exprs = Vec::new();
        reserve_vec(&mut partition_exprs, keys.len(), work)?;
        for (key, value) in keys.iter().enumerate() {
            partition_exprs.push(add_read(
                SinkReadInput {
                    fragment,
                    values: root_values,
                    layout: root_layout,
                },
                *value,
                ProgramExpressionRootSite::SinkPartition {
                    branch: branch_id,
                    key: u32::try_from(key).map_err(|_| CompileControlError::ResourceExhausted)?,
                },
                EvaluationDemand::Value,
                &mut nodes,
                &mut types,
                &mut reads,
                &mut type_scope,
                work,
            )?);
        }
        let (columns, layout) = if is_summary {
            let OutputSelection::Router { summary_slots, .. } = &selection else {
                return Err(
                    FragmentCompileError::Invalid("summary output belongs to a router").into(),
                );
            };
            let (_, _, summary) =
                router.ok_or(FragmentCompileError::Invalid("missing summary owner"))?;
            let mut typed =
                crate::typed_layout::TypedLayoutBuilder::new_with_creation(3, work, creation)?;
            for ((_, value), name) in summary
                .columns
                .fields()
                .into_iter()
                .zip(["inserted", "replaced", "deleted"])
            {
                let ty = &fragment
                    .values()
                    .get(&value)
                    .ok_or(FragmentCompileError::Invalid("missing summary type"))?
                    .ty;
                let name = crate::typed_layout::copy_name_with_creation(name, work, creation)?;
                typed.push_with_creation(ty, name, work, creation)?;
            }
            let layout = typed.finish_with_creation(summary_slots, work, creation)?;
            summary_layout = Some(layout.clone());
            (summary_slots.to_vec(), layout)
        } else {
            let columns = match &selection {
                OutputSelection::Router { aliases, .. } => aliases
                    .get(&cut.edge)
                    .ok_or(FragmentCompileError::Invalid("missing exact route aliases"))?
                    .to_vec(),
                OutputSelection::Stream(_) => projected_slots(cut, root_values, root_layout, work)?,
            };
            let storage = creation.storage_lifetime();
            let layout = root_layout.project_by_slots_typed_with_creator(
                &columns,
                storage,
                |layout| {
                    creation
                        .request_layout(layout)
                        .map_err(novarocks_local_program::LayoutCreationFailure::Creator)
                },
                work.control(),
            )?;
            (columns, layout)
        };
        let role = cut_role(cut.payload.role())?;
        let delivery = cut_delivery(cut.delivery);
        let branch = StaticStreamBranch::try_new(
            i32::try_from(cut.destination_node.get()).map_err(|_| {
                FragmentCompileError::Invalid("stream destination exceeds routing range")
            })?,
            partition_type,
            partition_exprs,
            columns,
            None,
            StaticStreamContract {
                role,
                delivery,
                routing,
            },
        )
        .map_err(sink_error)?;
        if let Some((_, routes, _)) = router
            && !is_summary
        {
            let route = routes
                .get(branch_index)
                .ok_or(FragmentCompileError::Invalid(
                    "router route ordinal differs",
                ))?;
            let layout = match &route.requirements.layout {
                novarocks_physical_plan::WriteLayoutPreference::None => {
                    ProgramWriteLayoutPreference::None
                }
                novarocks_physical_plan::WriteLayoutPreference::ClusterBy(values) => {
                    let mut slots = Vec::new();
                    reserve_vec(&mut slots, values.len(), work)?;
                    for value in values.iter() {
                        let mut ordinal = None;
                        for (index, (_, source)) in route.input_mapping.iter().enumerate() {
                            work.step()?;
                            if source == value {
                                ordinal = Some(index);
                                break;
                            }
                        }
                        let ordinal = ordinal.ok_or(FragmentCompileError::Invalid(
                            "layout preference is outside route data",
                        ))?;
                        slots.push(branch.output_columns()[ordinal]);
                        work.step()?;
                    }
                    ProgramWriteLayoutPreference::ClusterBy(Arc::from(slots))
                }
            };
            router_routes.push(StaticRouterRoute {
                branch: branch_id,
                layout,
                accepted_effects: Arc::from(route.accepted_effects.to_vec()),
            });
        }
        branches.push(branch);
        requirements.push(BindingRequirement::ExchangeOutput {
            branch: branch_index,
            layout,
            role,
            delivery,
        });
        work.step()?;
    }
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
            phase: "sink expressions",
            error: novarocks_failure_contract::SharedFailureCause::new(error),
        },
    })?;
    let definitions = arena.nodes().len();
    let arena = Arc::new(arena);
    let (flow, roots, slots) = read_flow(
        root,
        &reads,
        definitions,
        work,
        crate::creation::borrow_scope(&mut temporary),
        factory,
    )?;
    let sink = if let Some((effect, _, _)) = router {
        let ordinal = first_ordinal(root_values, effect, work)?.ok_or(
            FragmentCompileError::Invalid("router effect is outside root"),
        )?;
        let summary_index = u32::try_from(
            branches
                .len()
                .checked_sub(1)
                .ok_or(FragmentCompileError::Invalid("router has no summary"))?,
        )
        .map_err(|_| CompileControlError::ResourceExhausted)?;
        StaticSinkProgram::try_change_stream_router(
            branches,
            arena,
            root_layout.slots()[ordinal],
            router_routes,
            summary_index,
            summary_layout.ok_or(FragmentCompileError::Invalid(
                "router summary layout is absent",
            ))?,
        )
        .map_err(sink_error)?
    } else {
        let branch = branches
            .pop()
            .ok_or(FragmentCompileError::Invalid("stream branch is absent"))?;
        StaticSinkProgram::try_data_stream(branch, arena).map_err(sink_error)?
    };
    Ok(LoweredStreamSink {
        sink,
        requirements,
        flow: SinkFlow {
            flow,
            types,
            roots,
            slots,
        },
    })
}

struct SinkReadInput<'a> {
    fragment: &'a novarocks_physical_plan::Fragment,
    values: &'a [ValueId],
    layout: &'a StaticLayout,
}

fn add_read<H>(
    input: SinkReadInput<'_>,
    value: ValueId,
    site: ProgramExpressionRootSite,
    demand: EvaluationDemand,
    nodes: &mut Vec<StaticExprNode>,
    types: &mut Vec<FunctionArgumentType>,
    reads: &mut Vec<SinkRead>,
    scope: &mut Option<impl novarocks_type_contract::ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProgramExprId, crate::FragmentCreationError<H>> {
    let ordinal = first_ordinal(input.values, value, work)?
        .ok_or(FragmentCompileError::Invalid("sink read is outside root"))?;
    let ty = &input
        .fragment
        .values()
        .get(&value)
        .ok_or(FragmentCompileError::Invalid("missing sink read type"))?
        .ty;
    reserve_vec(nodes, 1, work)?;
    reserve_vec(reads, 1, work)?;
    match scope.as_mut() {
        Some(scope) => crate::creation::reserve_vec(types, 1, Some(scope), work)?,
        None => reserve_vec(types, 1, work)?,
    }
    let id = ProgramExprId::new(nodes.len());
    nodes.push(StaticExprNode::new_value_typed(
        StaticExprKind::SlotId(input.layout.slots()[ordinal]),
        ty.clone_observed(|| work.step())?,
    ));
    types.push(FunctionArgumentType::Value(
        ty.clone_observed(|| work.step())?,
    ));
    reads.push(SinkRead {
        site,
        ordinal: u32::try_from(ordinal).map_err(|_| CompileControlError::ResourceExhausted)?,
        demand,
    });
    Ok(id)
}
fn read_flow<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    root: ProgramNodeId,
    reads: &[SinkRead],
    definitions: usize,
    work: &mut CompileCheckpoints<'_>,
    mut temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
) -> Result<
    (
        ProgramControlFlow,
        Vec<ProgramRootUseBinding>,
        Vec<ProgramSlotBinding>,
    ),
    crate::FragmentCreationError<H>,
> {
    let mut domains = Vec::new();
    let mut uses = Vec::new();
    let mut roots = Vec::new();
    let mut slots = Vec::new();
    crate::creation::reserve_vec(
        &mut domains,
        reads.len(),
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    crate::creation::reserve_vec(
        &mut uses,
        reads.len(),
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    crate::creation::reserve_vec(
        &mut roots,
        reads.len(),
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    crate::creation::reserve_vec(
        &mut slots,
        reads.len(),
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    for (definition, read) in reads.iter().enumerate() {
        let identity =
            u32::try_from(definition).map_err(|_| CompileControlError::ResourceExhausted)?;
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
                demand: read.demand,
            },
            definition: ProgramExprId::new(definition),
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        roots.push(ProgramRootUseBinding {
            site: read.site,
            use_id,
        });
        slots.push(ProgramSlotBinding {
            occurrence: ProgramUseRef {
                arena: ProgramExpressionArena::Sink,
                use_id,
            },
            source: ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                node: root,
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: read.ordinal,
            }),
        });
        work.step()?;
    }
    let flow = match factory {
        Some(factory) => {
            let mut retained = factory
                .begin_scope()
                .map_err(crate::FragmentCreationError::Creator)?;
            ProgramControlFlow::try_new_with_creator_in(
                domains,
                uses,
                definitions,
                crate::creation::borrow_scope(&mut temporary)
                    .expect("strong entry supplies temporary scope"),
                &mut retained,
                work.control(),
            )?
        }
        None => ProgramControlFlow::try_new(domains, uses, definitions, work.control())?,
    };
    Ok((flow, roots, slots))
}

fn projected_slots(
    cut: &OutboundFragmentCut,
    values: &[ValueId],
    layout: &StaticLayout,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<SlotId>, FragmentCompileError> {
    let mut positional = cut.projection.len() == values.len();
    for (projected, value) in cut.projection.iter().zip(values) {
        positional &= projected.value == *value;
        work.step()?;
    }
    let mut slots = Vec::new();
    reserve_vec(&mut slots, cut.projection.len(), work)?;
    let mut seen = BTreeSet::new();
    for (index, projected) in cut.projection.iter().enumerate() {
        let ordinal = if positional {
            index
        } else {
            first_ordinal(values, projected.value, work)?.ok_or(FragmentCompileError::Invalid(
                "stream value is outside root",
            ))?
        };
        let slot = layout.slots()[ordinal];
        if !seen.insert(slot) {
            return Err(FragmentCompileError::Unsupported {
                node: None,
                feature: "ordinary stream duplicate projection without aliases",
            });
        }
        slots.push(slot);
        work.step()?;
    }
    if slots.is_empty() && !values.is_empty() {
        return Err(FragmentCompileError::Unsupported {
            node: None,
            feature: "empty stream projection of a non-empty root",
        });
    }
    Ok(slots)
}
fn first_ordinal(
    values: &[ValueId],
    value: ValueId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<usize>, FragmentCompileError> {
    for (ordinal, candidate) in values.iter().enumerate() {
        work.step()?;
        if *candidate == value {
            return Ok(Some(ordinal));
        }
    }
    Ok(None)
}
fn sink_error(error: StaticSinkError) -> FragmentCompileError {
    FragmentCompileError::Owner {
        phase: "stream sink",
        error: novarocks_failure_contract::SharedFailureCause::new(error),
    }
}
