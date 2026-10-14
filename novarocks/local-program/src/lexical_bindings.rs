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

//! Mandatory occurrence-local lexical sources over one complete program.
//!
//! Parameters and common locals borrow the actual ordered lambda declaration;
//! captures borrow full types from the enclosing frame or the checked channel
//! table. Slot numbers identify declared slots, never an inferred capture ABI.
//! This proves local closure correspondence, not that physical lowering retained
//! original binding identity, and supplies no runtime evaluation or hoist grant.

use crate::{
    LocalProgramGraph, ProgramCallSite, ProgramChannelLayoutRole, ProgramChannelSite,
    ProgramExprId, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind, ProgramTypedChannels, ProgramUseRef, StaticExprKind,
};
use novarocks_functions::{FunctionArgumentType, FunctionValueType, PreparedPureKernel};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape,
    MAX_CONTROL_USE_REFERENCES, PureCompileControl, ValueTypeError,
    arrow_data_types_exact_observed,
};
use novarocks_types::SlotId;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProgramLexicalSource {
    Parameter {
        lambda: ProgramUseRef,
        ordinal: u32,
    },
    Common {
        lambda: ProgramUseRef,
        ordinal: u32,
    },
    Capture {
        lambda: ProgramUseRef,
        ordinal: u32,
    },
    Input(ProgramChannelSite),
    Projected {
        node: ProgramNodeId,
        expression: u32,
    },
    WriterProjected {
        node: ProgramNodeId,
        expression: u32,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgramLambdaBinding {
    pub edge: ProgramUseRef,
    pub captures: Box<[ProgramLexicalSource]>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramSlotBinding {
    pub occurrence: ProgramUseRef,
    pub source: ProgramLexicalSource,
}
#[derive(Clone, Debug)]
pub struct ProgramLexicalBindings {
    channels: ProgramTypedChannels,
    lambdas: Arc<BTreeMap<ProgramUseRef, ProgramLambdaBinding>>,
    slots: Arc<BTreeMap<ProgramUseRef, ProgramLexicalSource>>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramLexicalBindingError {
    Control(CompileControlError),
    ValueType(ValueTypeError),
    TooManyItems,
    DuplicateBinding,
    IncompleteCoverage,
    InvalidOccurrence,
    InvalidLambda,
    InvalidSource,
    WrongScope,
    ForwardCommon,
    ShadowedSource,
    WrongSlot,
    TypeMismatch,
    UnusedCapture,
}
impl From<CompileControlError> for ProgramLexicalBindingError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<ValueTypeError> for ProgramLexicalBindingError {
    fn from(value: ValueTypeError) -> Self {
        Self::ValueType(value)
    }
}
impl fmt::Display for ProgramLexicalBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid local lexical bindings: {self:?}")
    }
}
impl std::error::Error for ProgramLexicalBindingError {}
#[derive(Clone, Copy)]
struct Scope {
    root: ProgramExpressionRootSite,
    lambda: Option<ProgramUseRef>,
    // A common expression sees parameters and only earlier common locals.
    common_before: Option<usize>,
}
struct LambdaInfo {
    definition: ProgramExprId,
    enclosing: Scope,
    locals: Arc<BTreeMap<SlotId, DeclaredLocal>>,
}
#[derive(Clone, Copy)]
enum DeclaredLocal {
    Parameter(u32),
    Common(u32),
}
#[derive(Clone, Copy)]
struct Fact<'a> {
    slot: SlotId,
    ty: &'a FunctionValueType,
}

impl ProgramLexicalBindings {
    pub fn try_new(
        channels: ProgramTypedChannels,
        bindings: Vec<ProgramLambdaBinding>,
        slot_bindings: Vec<ProgramSlotBinding>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramLexicalBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        // These limits bound new closure records, not schema width or a runtime
        // allocation wallet. Existing control references retain their own bound.
        let mut items = bindings
            .len()
            .checked_add(slot_bindings.len())
            .filter(|n| *n <= MAX_CONTROL_USE_REFERENCES)
            .ok_or(ProgramLexicalBindingError::TooManyItems)?;
        let mut lambdas = BTreeMap::new();
        for binding in bindings {
            work.step()?;
            items = items
                .checked_add(binding.captures.len())
                .filter(|n| *n <= MAX_CONTROL_USE_REFERENCES)
                .ok_or(ProgramLexicalBindingError::TooManyItems)?;
            for _ in &binding.captures {
                work.step()?;
            }
            if lambdas.insert(binding.edge, binding).is_some() {
                return Err(ProgramLexicalBindingError::DuplicateBinding);
            }
        }
        let mut slots = BTreeMap::new();
        for binding in slot_bindings {
            work.step()?;
            if slots.insert(binding.occurrence, binding.source).is_some() {
                return Err(ProgramLexicalBindingError::DuplicateBinding);
            }
        }
        let typed = channels.expressions();
        let resolved = typed.resolved_calls();
        let snapshot = resolved.snapshot();
        let projected = ProjectedSources::try_new(&channels, &mut work)?;
        let mut kernels = BTreeMap::new();
        for (site, call) in resolved.calls() {
            work.step()?;
            let (ProgramCallSite::Expression(occurrence), PreparedPureKernel::HigherOrder(kernel)) =
                (site, call.specialization().prepared())
            else {
                continue;
            };
            let edge = ProgramUseRef {
                arena: occurrence.arena,
                use_id: kernel.contract().body_edge_use(),
            };
            if kernels.insert(edge, kernel).is_some() {
                return Err(ProgramLexicalBindingError::InvalidLambda);
            }
        }
        // Walk each real occurrence once, retaining its actual enclosing frame.
        // Definitions can be reused under distinct frame occurrences.
        let mut scopes = BTreeMap::new();
        let mut infos = BTreeMap::new();
        let mut order = Vec::new();
        let mut declarations = BTreeMap::new();
        for (root, use_id) in snapshot.bindings() {
            work.step()?;
            let arena = root.arena();
            let flow = &snapshot.flows()[&arena];
            let definitions = &snapshot.roots().arenas()[&arena];
            let mut stack = vec![(
                *use_id,
                Scope {
                    root: *root,
                    lambda: None,
                    common_before: None,
                },
            )];
            while let Some((use_id, scope)) = stack.pop() {
                work.step()?;
                let occurrence = ProgramUseRef { arena, use_id };
                if scopes.insert(occurrence, scope).is_some() {
                    return Err(ProgramLexicalBindingError::InvalidOccurrence);
                }
                let invocation = &flow.uses()[&use_id];
                if matches!(
                    definitions
                        .node(invocation.definition)
                        .map(|node| node.kind()),
                    Some(StaticExprKind::LambdaFunction { .. })
                ) && invocation.control != ControlShape::LambdaBody
                {
                    return Err(ProgramLexicalBindingError::InvalidLambda);
                }
                if invocation.control == ControlShape::LambdaBody {
                    let Some(StaticExprKind::LambdaFunction {
                        body,
                        arg_slots,
                        common_sub_exprs,
                        ..
                    }) = definitions
                        .node(invocation.definition)
                        .map(|value| value.kind())
                    else {
                        return Err(ProgramLexicalBindingError::InvalidLambda);
                    };
                    let Some(FunctionArgumentType::Lambda {
                        parameter_types, ..
                    }) = typed.definition_type(arena, invocation.definition)
                    else {
                        return Err(ProgramLexicalBindingError::InvalidLambda);
                    };
                    if arg_slots.len() != parameter_types.len()
                        || invocation.arguments.len() != common_sub_exprs.len() + 1
                        || !kernels.contains_key(&occurrence)
                        || !lambdas.contains_key(&occurrence)
                    {
                        return Err(ProgramLexicalBindingError::InvalidLambda);
                    }
                    let key = (arena, invocation.definition);
                    let locals = if let Some(locals) = declarations.get(&key) {
                        Arc::clone(locals)
                    } else {
                        let mut locals = BTreeMap::new();
                        for (ordinal, slot) in arg_slots.iter().enumerate() {
                            work.step()?;
                            let ordinal = u32::try_from(ordinal)
                                .map_err(|_| ProgramLexicalBindingError::TooManyItems)?;
                            if locals
                                .insert(*slot, DeclaredLocal::Parameter(ordinal))
                                .is_some()
                            {
                                return Err(ProgramLexicalBindingError::InvalidLambda);
                            }
                        }
                        for (ordinal, (slot, _)) in common_sub_exprs.iter().enumerate() {
                            work.step()?;
                            let ordinal = u32::try_from(ordinal)
                                .map_err(|_| ProgramLexicalBindingError::TooManyItems)?;
                            if locals
                                .insert(*slot, DeclaredLocal::Common(ordinal))
                                .is_some()
                            {
                                return Err(ProgramLexicalBindingError::InvalidLambda);
                            }
                        }
                        let locals = Arc::new(locals);
                        declarations.insert(key, locals.clone());
                        locals
                    };
                    for (ordinal, (_, definition)) in common_sub_exprs.iter().enumerate() {
                        work.step()?;
                        if flow.uses()[&invocation.arguments[ordinal]].definition != *definition {
                            return Err(ProgramLexicalBindingError::InvalidLambda);
                        }
                    }
                    if flow.uses()[invocation.arguments.last().unwrap()].definition != *body {
                        return Err(ProgramLexicalBindingError::InvalidLambda);
                    }
                    infos.insert(
                        occurrence,
                        LambdaInfo {
                            definition: invocation.definition,
                            enclosing: scope,
                            locals,
                        },
                    );
                    order.push(occurrence);
                    for (ordinal, child) in invocation.arguments.iter().enumerate().rev() {
                        work.step()?;
                        stack.push((
                            *child,
                            Scope {
                                root: *root,
                                lambda: Some(occurrence),
                                common_before: if ordinal == common_sub_exprs.len() {
                                    None
                                } else {
                                    Some(ordinal)
                                },
                            },
                        ));
                    }
                } else {
                    for child in invocation.arguments.iter().rev() {
                        work.step()?;
                        stack.push((*child, scope));
                    }
                }
            }
        }
        if infos.len() != lambdas.len() || infos.len() != kernels.len() {
            return Err(ProgramLexicalBindingError::IncompleteCoverage);
        }
        // Outer frames precede their nested frames, so captures never require
        // recursive source resolution or a forward reference to mutable state.
        let mut capture_facts = BTreeMap::new();
        let mut used = BTreeSet::new();
        for edge in order {
            work.step()?;
            let info = &infos[&edge];
            let binding = &lambdas[&edge];
            let expected = kernels[&edge].body_contract().capture_types();
            if binding.captures.len() != expected.len() {
                return Err(ProgramLexicalBindingError::IncompleteCoverage);
            }
            for (ordinal, (source, expected)) in binding.captures.iter().zip(expected).enumerate() {
                work.step()?;
                let fact = SourceProof {
                    channels: &channels,
                    projected: &projected,
                    infos: &infos,
                    captures: &capture_facts,
                    used: &mut used,
                }
                .fact(*source, info.enclosing, &mut work)?;
                exact_type(fact.ty, expected, &mut work)?;
                capture_facts.insert((edge, ordinal as u32), fact);
            }
        }
        let mut actual_slots = 0usize;
        for (occurrence, scope) in &scopes {
            work.step()?;
            let flow = &snapshot.flows()[&occurrence.arena];
            let invocation = &flow.uses()[&occurrence.use_id];
            let definition = snapshot.roots().arenas()[&occurrence.arena]
                .node(invocation.definition)
                .unwrap();
            let StaticExprKind::SlotId(slot) = definition.kind() else {
                continue;
            };
            actual_slots += 1;
            let source = slots
                .get(occurrence)
                .ok_or(ProgramLexicalBindingError::IncompleteCoverage)?;
            let fact = SourceProof {
                channels: &channels,
                projected: &projected,
                infos: &infos,
                captures: &capture_facts,
                used: &mut used,
            }
            .fact(*source, *scope, &mut work)?;
            if fact.slot != *slot {
                return Err(ProgramLexicalBindingError::WrongSlot);
            }
            let Some(FunctionArgumentType::Value(actual)) =
                typed.definition_type(occurrence.arena, invocation.definition)
            else {
                return Err(ProgramLexicalBindingError::TypeMismatch);
            };
            exact_type(actual, fact.ty, &mut work)?;
        }
        if actual_slots != slots.len() {
            return Err(ProgramLexicalBindingError::IncompleteCoverage);
        }
        for (edge, binding) in &lambdas {
            for ordinal in 0..binding.captures.len() {
                work.step()?;
                if !used.contains(&(*edge, ordinal as u32)) {
                    return Err(ProgramLexicalBindingError::UnusedCapture);
                }
            }
        }
        work.finish()?;
        Ok(Self {
            channels,
            lambdas: Arc::new(lambdas),
            slots: Arc::new(slots),
        })
    }
    pub const fn channels(&self) -> &ProgramTypedChannels {
        &self.channels
    }
    pub fn lambdas(&self) -> &BTreeMap<ProgramUseRef, ProgramLambdaBinding> {
        &self.lambdas
    }
    pub fn slots(&self) -> &BTreeMap<ProgramUseRef, ProgramLexicalSource> {
        &self.slots
    }
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum ProjectionOwner {
    Node(ProgramNodeId),
    Writer(ProgramNodeId),
}
#[derive(Clone, Copy)]
struct ProjectedPosition {
    expression: u32,
}
struct ProjectedSources {
    by_slot: BTreeMap<(ProjectionOwner, SlotId), Vec<ProjectedPosition>>,
    by_position: BTreeMap<(ProjectionOwner, u32), (SlotId, ProgramExprId)>,
}
impl ProjectedSources {
    fn try_new(
        channels: &ProgramTypedChannels,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ProgramLexicalBindingError> {
        let mut checked = Self {
            by_slot: BTreeMap::new(),
            by_position: BTreeMap::new(),
        };
        for (index, node) in channels
            .expressions()
            .resolved_calls()
            .snapshot()
            .program()
            .nodes()
            .iter()
            .enumerate()
        {
            work.step()?;
            let node_id = ProgramNodeId::new(index);
            let (owner, expressions, slots) = match node.kind() {
                ProgramNodeKind::Project {
                    exprs,
                    expr_slot_ids,
                    ..
                } => (
                    ProjectionOwner::Node(node_id),
                    exprs,
                    expr_slot_ids.as_slice(),
                ),
                ProgramNodeKind::TableWriter { projection, .. } => (
                    ProjectionOwner::Writer(node_id),
                    &projection.expressions,
                    projection.layout.slots(),
                ),
                _ => continue,
            };
            if expressions.len() != slots.len() {
                return Err(ProgramLexicalBindingError::InvalidSource);
            }
            for (ordinal, (definition, slot)) in expressions.iter().zip(slots).enumerate() {
                work.step()?;
                let expression =
                    u32::try_from(ordinal).map_err(|_| ProgramLexicalBindingError::TooManyItems)?;
                checked
                    .by_slot
                    .entry((owner, *slot))
                    .or_default()
                    .push(ProjectedPosition { expression });
                checked
                    .by_position
                    .insert((owner, expression), (*slot, *definition));
            }
        }
        Ok(checked)
    }
    fn root(root: ProgramExpressionRootSite) -> Option<(ProjectionOwner, u32)> {
        match root {
            ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::ProjectOutput { expression },
            } => Some((ProjectionOwner::Node(node), expression)),
            ProgramExpressionRootSite::WriterProjection { node, expression } => {
                Some((ProjectionOwner::Writer(node), expression))
            }
            _ => None,
        }
    }
    fn latest(
        &self,
        root: ProgramExpressionRootSite,
        slot: SlotId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<ProgramLexicalSource>, ProgramLexicalBindingError> {
        work.step()?;
        let Some((owner, before)) = Self::root(root) else {
            return Ok(None);
        };
        let Some(producers) = self.by_slot.get(&(owner, slot)) else {
            return Ok(None);
        };
        let (mut low, mut high) = (0, producers.len());
        while low < high {
            work.step()?;
            let middle = low + (high - low) / 2;
            if producers[middle].expression < before {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low.checked_sub(1).map(|index| match owner {
            ProjectionOwner::Node(node) => ProgramLexicalSource::Projected {
                node,
                expression: producers[index].expression,
            },
            ProjectionOwner::Writer(node) => ProgramLexicalSource::WriterProjected {
                node,
                expression: producers[index].expression,
            },
        }))
    }
    fn fact<'a>(
        &self,
        source: ProgramLexicalSource,
        root: ProgramExpressionRootSite,
        channels: &'a ProgramTypedChannels,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Fact<'a>, ProgramLexicalBindingError> {
        work.step()?;
        let (owner, expression, arena) = match source {
            ProgramLexicalSource::Projected { node, expression } => (
                ProjectionOwner::Node(node),
                expression,
                crate::ProgramExpressionArena::Main,
            ),
            ProgramLexicalSource::WriterProjected { node, expression } => (
                ProjectionOwner::Writer(node),
                expression,
                crate::ProgramExpressionArena::WriterProjection(node),
            ),
            _ => return Err(ProgramLexicalBindingError::InvalidSource),
        };
        let Some((actual_owner, before)) = Self::root(root) else {
            return Err(ProgramLexicalBindingError::WrongScope);
        };
        if actual_owner != owner || expression >= before {
            return Err(ProgramLexicalBindingError::WrongScope);
        }
        let (slot, definition) = self
            .by_position
            .get(&(owner, expression))
            .ok_or(ProgramLexicalBindingError::InvalidSource)?;
        let Some(FunctionArgumentType::Value(ty)) =
            channels.expressions().definition_type(arena, *definition)
        else {
            return Err(ProgramLexicalBindingError::TypeMismatch);
        };
        Ok(Fact { slot: *slot, ty })
    }
}

struct SourceProof<'a, 'proof> {
    channels: &'a ProgramTypedChannels,
    projected: &'proof ProjectedSources,
    infos: &'proof BTreeMap<ProgramUseRef, LambdaInfo>,
    captures: &'proof BTreeMap<(ProgramUseRef, u32), Fact<'a>>,
    used: &'proof mut BTreeSet<(ProgramUseRef, u32)>,
}
impl<'a> SourceProof<'a, '_> {
    fn fact(
        &mut self,
        source: ProgramLexicalSource,
        scope: Scope,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Fact<'a>, ProgramLexicalBindingError> {
        let channels = self.channels;
        let projected = self.projected;
        let infos = self.infos;
        let captures = self.captures;
        let used = &mut *self.used;
        work.step()?;
        let typed = channels.expressions();
        let definitions = typed.resolved_calls().snapshot().roots().arenas();
        let fact = match source {
            ProgramLexicalSource::Parameter { lambda, ordinal } => {
                if scope.lambda != Some(lambda) {
                    return Err(ProgramLexicalBindingError::WrongScope);
                }
                let info = infos
                    .get(&lambda)
                    .ok_or(ProgramLexicalBindingError::InvalidSource)?;
                let StaticExprKind::LambdaFunction { arg_slots, .. } = definitions[&lambda.arena]
                    .node(info.definition)
                    .unwrap()
                    .kind()
                else {
                    return Err(ProgramLexicalBindingError::InvalidLambda);
                };
                let Some(FunctionArgumentType::Lambda {
                    parameter_types, ..
                }) = typed.definition_type(lambda.arena, info.definition)
                else {
                    return Err(ProgramLexicalBindingError::InvalidLambda);
                };
                Fact {
                    slot: *arg_slots
                        .get(ordinal as usize)
                        .ok_or(ProgramLexicalBindingError::InvalidSource)?,
                    ty: parameter_types
                        .get(ordinal as usize)
                        .ok_or(ProgramLexicalBindingError::InvalidSource)?,
                }
            }
            ProgramLexicalSource::Common { lambda, ordinal } => {
                if scope.lambda != Some(lambda) {
                    return Err(ProgramLexicalBindingError::WrongScope);
                }
                if scope
                    .common_before
                    .is_some_and(|limit| ordinal as usize >= limit)
                {
                    return Err(ProgramLexicalBindingError::ForwardCommon);
                }
                let info = infos
                    .get(&lambda)
                    .ok_or(ProgramLexicalBindingError::InvalidSource)?;
                let StaticExprKind::LambdaFunction {
                    common_sub_exprs, ..
                } = definitions[&lambda.arena]
                    .node(info.definition)
                    .unwrap()
                    .kind()
                else {
                    return Err(ProgramLexicalBindingError::InvalidLambda);
                };
                let (slot, definition) = common_sub_exprs
                    .get(ordinal as usize)
                    .ok_or(ProgramLexicalBindingError::InvalidSource)?;
                let Some(FunctionArgumentType::Value(ty)) =
                    typed.definition_type(lambda.arena, *definition)
                else {
                    return Err(ProgramLexicalBindingError::TypeMismatch);
                };
                Fact { slot: *slot, ty }
            }
            ProgramLexicalSource::Capture { lambda, ordinal } => {
                if scope.lambda != Some(lambda) {
                    return Err(ProgramLexicalBindingError::WrongScope);
                }
                let fact = *captures
                    .get(&(lambda, ordinal))
                    .ok_or(ProgramLexicalBindingError::InvalidSource)?;
                used.insert((lambda, ordinal));
                fact
            }
            ProgramLexicalSource::Input(site) => {
                if scope.lambda.is_some() {
                    return Err(ProgramLexicalBindingError::WrongScope);
                }
                if !input_scope(channels, scope.root, site)? {
                    return Err(ProgramLexicalBindingError::WrongScope);
                }
                Fact {
                    slot: channels
                        .channel_slot(site)
                        .ok_or(ProgramLexicalBindingError::InvalidSource)?,
                    ty: channels
                        .channel_type(site)
                        .ok_or(ProgramLexicalBindingError::InvalidSource)?,
                }
            }
            ProgramLexicalSource::Projected { .. }
            | ProgramLexicalSource::WriterProjected { .. } => {
                if scope.lambda.is_some() {
                    return Err(ProgramLexicalBindingError::WrongScope);
                }
                projected.fact(source, scope.root, channels, work)?
            }
        };
        if scope.lambda.is_none()
            && let Some(latest) = projected.latest(scope.root, fact.slot, work)?
            && latest != source
        {
            return Err(ProgramLexicalBindingError::ShadowedSource);
        }
        if let Some(lambda) = scope.lambda
            && let Some(local) = infos[&lambda].locals.get(&fact.slot)
        {
            let (visible, nearest) = match *local {
                DeclaredLocal::Parameter(ordinal) => {
                    (true, ProgramLexicalSource::Parameter { lambda, ordinal })
                }
                DeclaredLocal::Common(ordinal) => (
                    scope
                        .common_before
                        .is_none_or(|limit| (ordinal as usize) < limit),
                    ProgramLexicalSource::Common { lambda, ordinal },
                ),
            };
            if visible && nearest != source {
                return Err(ProgramLexicalBindingError::ShadowedSource);
            }
        }
        Ok(fact)
    }
}
fn exact_type(
    actual: &FunctionValueType,
    expected: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramLexicalBindingError> {
    work.step()?;
    if actual.nullable != expected.nullable
        || actual.logical_type != expected.logical_type
        || !arrow_data_types_exact_observed::<ProgramLexicalBindingError>(
            &actual.data_type,
            &expected.data_type,
            || work.step().map_err(Into::into),
        )?
    {
        Err(ProgramLexicalBindingError::TypeMismatch)
    } else {
        Ok(())
    }
}
fn input_scope(
    channels: &ProgramTypedChannels,
    root: ProgramExpressionRootSite,
    site: ProgramChannelSite,
) -> Result<bool, ProgramLexicalBindingError> {
    let ProgramChannelSite::Layout { node, role, .. } = site else {
        return Ok(false);
    };
    let program = channels.expressions().resolved_calls().snapshot().program();
    // An empty port has no input channel, so no input source is in scope.
    Ok(root_input_layout(program, root)? == ProgramRootInput::Layout { node, role })
}

/// The input port an expression root reads its input slots from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramRootInput {
    /// The root reads this one layout.
    Layout {
        node: ProgramNodeId,
        role: ProgramChannelLayoutRole,
    },
    /// The root reads no input layout. It is evaluated over an explicit empty
    /// port: a batch of exactly one row and no column.
    Empty,
}

/// The one input port an expression root reads its input slots from. Both
/// lexical binding and every executor of a compiled root use this authority,
/// so an executor presents exactly the port the bindings were checked against.
pub fn root_input_layout(
    program: &LocalProgramGraph,
    root: ProgramExpressionRootSite,
) -> Result<ProgramRootInput, ProgramLexicalBindingError> {
    use ProgramChannelLayoutRole as Layout;
    use ProgramNodeExpressionRole as Role;
    let (node, role) = match root {
        ProgramExpressionRootSite::WriterProjection { node: owner, .. } => {
            let ProgramNodeKind::TableWriter { input, .. } = program.nodes()[owner.index()].kind()
            else {
                return Err(ProgramLexicalBindingError::InvalidSource);
            };
            (*input, Layout::NodeOutput)
        }
        ProgramExpressionRootSite::SinkPartition { .. }
        | ProgramExpressionRootSite::SinkSplitPredicate { .. } => {
            (program.root(), Layout::NodeOutput)
        }
        ProgramExpressionRootSite::Node {
            node: owner,
            role: root_role,
        } => {
            let kind = program.nodes()[owner.index()].kind();
            match (kind, root_role) {
                (ProgramNodeKind::Values { .. }, Role::ValuesCell { .. }) => {
                    return Ok(ProgramRootInput::Empty);
                }
                (
                    ProgramNodeKind::Join { .. },
                    Role::JoinProbeKey { .. } | Role::RuntimeFilter { .. },
                ) => (owner, Layout::JoinLeft),
                (ProgramNodeKind::Join { .. }, Role::JoinBuildKey { .. }) => {
                    (owner, Layout::JoinRight)
                }
                (ProgramNodeKind::Join { .. }, Role::JoinResidual)
                | (ProgramNodeKind::NestedLoopJoin { .. }, Role::NestedLoopPredicate) => {
                    (owner, Layout::JoinScope)
                }
                (ProgramNodeKind::Scan { .. } | ProgramNodeKind::ExchangeSource { .. }, _) => {
                    (owner, Layout::NodeOutput)
                }
                (
                    ProgramNodeKind::GroupedTopN { input, .. },
                    Role::TopNGroup { .. }
                    | Role::TopNOrder { .. }
                    | Role::TopNStateArgument { .. },
                ) => (*input, Layout::NodeOutput),
                (ProgramNodeKind::TableFinish { .. }, Role::FinishUnpivotConstant { .. }) => {
                    (owner, Layout::WriterRootResult)
                }
                (
                    ProgramNodeKind::AssertNumRows { input, .. }
                    | ProgramNodeKind::Project { input, .. }
                    | ProgramNodeKind::Unpivot { input, .. }
                    | ProgramNodeKind::Filter { input, .. }
                    | ProgramNodeKind::Repeat { input, .. }
                    | ProgramNodeKind::ChangeEventExpand { input, .. }
                    | ProgramNodeKind::Limit { input, .. }
                    | ProgramNodeKind::Sort { input, .. }
                    | ProgramNodeKind::TableFunction { input, .. }
                    | ProgramNodeKind::Aggregate { input, .. }
                    | ProgramNodeKind::Analytic { input, .. }
                    | ProgramNodeKind::RuntimeFilterConsumer { input, .. }
                    | ProgramNodeKind::TableWriter { input, .. },
                    _,
                ) => (*input, Layout::NodeOutput),
                _ => return Err(ProgramLexicalBindingError::InvalidSource),
            }
        }
    };
    Ok(ProgramRootInput::Layout { node, role })
}

#[cfg(test)]
mod tests;
