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

//! Frozen exact-owner attachments to actual local expression/operator calls.
//!
//! This constructor checks same-snapshot occurrence, ordered source channels
//! and typed lifecycle correspondence. It is not complete expression typing,
//! lexical capture closure or a proof that lowering preserved physical binding
//! identity: the compiler owns that proof. Legacy function family/name tags
//! never select or authenticate an implementation here. No mutable instance,
//! Task capability, memory grant or runtime name resolver is stored.

use crate::{
    ProgramExpressionArena, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind, ProgramRootControlBindings, StaticAggregateCall, StaticExprKind, StaticLayout,
    StaticWindowFunction, WindowBoundary, WindowFrame, WindowFunctionKind, WindowType,
};
use novarocks_functions::{
    AggregateCallContract, AggregateKernelPhase, FunctionArgumentType, FunctionCallContract,
    FunctionResultType, PreparedAggregateHandle, PreparedHigherOrderKernel, PreparedPureKernel,
    PreparedScalarKernel, PreparedTableKernel, PreparedWindowKernel, PureCallSpecialization,
    PureImplementationDeclaration, PurePreparationSource, ScopedExpressionEffects,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, EvaluationDemand,
    ExpressionUseId, FunctionValueType, MAX_CONTROL_USE_REFERENCES, PureCompileControl,
    ValueTypeError, WindowBound, WindowFrameExclusion, WindowFrameUnits,
    arrow_data_types_exact_observed,
};
use novarocks_types::SlotId;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProgramUseRef {
    pub arena: ProgramExpressionArena,
    pub use_id: ExpressionUseId,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProgramCallSite {
    Expression(ProgramUseRef),
    Aggregate { node: ProgramNodeId, call: u32 },
    TopNState { node: ProgramNodeId, call: u32 },
    Window { node: ProgramNodeId, call: u32 },
    WriterPartial { node: ProgramNodeId, call: u32 },
    WriterFinal { node: ProgramNodeId, call: u32 },
    Table { node: ProgramNodeId },
}

/// A static ownership position, not an already created driver/body instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramExpressionCallScope {
    pub root: ProgramExpressionRootSite,
    pub occurrence: ProgramUseRef,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramAggregateCallScope {
    Aggregate { node: ProgramNodeId, call: u32 },
    TopNState { node: ProgramNodeId, call: u32 },
    WriterPartial { node: ProgramNodeId, call: u32 },
    WriterFinal { node: ProgramNodeId, call: u32 },
}

/// Borrow actual prepared state facts. Bounds and layouts are not duplicated
/// into another authority, and none is a host allocation authorization.
#[derive(Debug)]
pub enum ProgramStateTemplate<'a> {
    Scalar {
        scope: ProgramExpressionCallScope,
        kernel: &'a Arc<dyn PreparedScalarKernel>,
    },
    ScalarInvocation {
        scope: ProgramExpressionCallScope,
        kernel: &'a Arc<dyn novarocks_functions::PreparedInvocationScalarKernel>,
    },
    HigherOrder {
        scope: ProgramExpressionCallScope,
        kernel: &'a Arc<dyn PreparedHigherOrderKernel>,
    },
    Aggregate {
        scope: ProgramAggregateCallScope,
        kernel: &'a PreparedAggregateHandle,
    },
    /// Aggregate OVER retains Aggregate identity but owns a partition lifecycle.
    WindowPartition {
        node: ProgramNodeId,
        call: u32,
        kernel: &'a Arc<dyn PreparedWindowKernel>,
    },
    TableCursor {
        node: ProgramNodeId,
        kernel: &'a Arc<dyn PreparedTableKernel>,
    },
    ControlIntrinsic {
        scope: ProgramExpressionCallScope,
        call: &'a FunctionCallContract,
    },
}

#[derive(Clone, Copy, Debug)]
enum CheckedScope {
    Expression(ProgramExpressionCallScope),
    Aggregate(ProgramAggregateCallScope),
    Window { node: ProgramNodeId, call: u32 },
    Table { node: ProgramNodeId },
}

#[derive(Clone, Debug)]
pub struct ProgramResolvedCall {
    specialization: PureCallSpecialization,
    scope: CheckedScope,
}
impl ProgramResolvedCall {
    pub fn specialization(&self) -> &PureCallSpecialization {
        &self.specialization
    }
    pub fn call_contract(&self) -> &FunctionCallContract {
        self.specialization.call_contract()
    }
    pub fn implementation(&self) -> &PureImplementationDeclaration {
        self.specialization.implementation()
    }
    pub fn effects(&self) -> ScopedExpressionEffects {
        self.specialization.effects()
    }
    pub fn state_template(&self) -> ProgramStateTemplate<'_> {
        match (self.specialization.prepared(), self.scope) {
            (PreparedPureKernel::Scalar(kernel), CheckedScope::Expression(scope)) => {
                ProgramStateTemplate::Scalar { scope, kernel }
            }
            (PreparedPureKernel::ScalarInvocation(kernel), CheckedScope::Expression(scope)) => {
                ProgramStateTemplate::ScalarInvocation { scope, kernel }
            }
            (PreparedPureKernel::HigherOrder(kernel), CheckedScope::Expression(scope)) => {
                ProgramStateTemplate::HigherOrder { scope, kernel }
            }
            (PreparedPureKernel::Aggregate(kernel), CheckedScope::Aggregate(scope)) => {
                ProgramStateTemplate::Aggregate { scope, kernel }
            }
            (PreparedPureKernel::Window(kernel), CheckedScope::Window { node, call }) => {
                ProgramStateTemplate::WindowPartition { node, call, kernel }
            }
            (PreparedPureKernel::Table(kernel), CheckedScope::Table { node }) => {
                ProgramStateTemplate::TableCursor { node, kernel }
            }
            (PreparedPureKernel::ControlIntrinsic(call), CheckedScope::Expression(scope)) => {
                ProgramStateTemplate::ControlIntrinsic { scope, call }
            }
            _ => unreachable!("checked factory pairs each site with its actual lifecycle"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProgramResolvedCalls {
    snapshot: ProgramRootControlBindings,
    calls: Arc<BTreeMap<ProgramCallSite, ProgramResolvedCall>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramResolvedCallsError {
    Control(CompileControlError),
    TooManyItems,
    InvalidSite,
    DuplicateSite,
    MissingSite(ProgramCallSite),
    WrongSource,
    WrongContext,
    InvalidDomain,
    SharedUse,
    WrongControl,
    WrongArguments,
    WrongBody,
    WrongLifecycle,
    WrongPhase,
    WrongOrder,
    WrongWindow,
    WrongStateFormat,
    TypeMismatch,
    InvalidType,
}
impl fmt::Display for ProgramResolvedCallsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid resolved local calls: {self:?}")
    }
}
impl std::error::Error for ProgramResolvedCallsError {}
impl From<CompileControlError> for ProgramResolvedCallsError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ValueTypeError> for ProgramResolvedCallsError {
    fn from(_: ValueTypeError) -> Self {
        Self::InvalidType
    }
}

impl ProgramResolvedCalls {
    pub fn try_new(
        snapshot: ProgramRootControlBindings,
        calls: Vec<(ProgramCallSite, PureCallSpecialization)>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramResolvedCallsError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        if calls.len() > MAX_CONTROL_USE_REFERENCES {
            return Err(ProgramResolvedCallsError::TooManyItems);
        }
        let mut pending = BTreeMap::new();
        for (site, call) in calls {
            work.step()?;
            if call.source() != PurePreparationSource::Frozen {
                return Err(ProgramResolvedCallsError::WrongSource);
            }
            if pending.insert(site, call).is_some() {
                return Err(ProgramResolvedCallsError::DuplicateSite);
            }
        }
        let mut references = 0usize;
        for flow in snapshot.flows().values() {
            references = references
                .checked_add(flow.use_reference_count())
                .filter(|n| *n <= MAX_CONTROL_USE_REFERENCES)
                .ok_or(ProgramResolvedCallsError::TooManyItems)?;
            work.step()?;
        }
        // Cache only source channels actually requested by call sites. Each
        // borrowed layout is scanned once; schema width is not an invocation
        // count limit, and only requested ordinals are retained.
        let mut slots = RequestedSlots::default();
        for node in snapshot.program().nodes() {
            work.step()?;
            match node.kind() {
                ProgramNodeKind::TableWriter {
                    partial_aggregates,
                    projection,
                    writer_multiplex_layout,
                    ..
                } => {
                    for call in partial_aggregates {
                        slots.request(
                            &projection.layout,
                            call.input_slot_id,
                            &mut references,
                            &mut work,
                        )?;
                        slots.request(
                            writer_multiplex_layout,
                            call.intermediate_slot_id,
                            &mut references,
                            &mut work,
                        )?;
                    }
                }
                ProgramNodeKind::TableFinish {
                    final_aggregates,
                    writer_multiplex_layout,
                    ..
                } => {
                    for call in &final_aggregates.calls {
                        slots.request(
                            writer_multiplex_layout,
                            call.intermediate_input_slot_id,
                            &mut references,
                            &mut work,
                        )?;
                    }
                }
                ProgramNodeKind::TableFunction {
                    input, param_slots, ..
                } => {
                    let layout = snapshot.program().nodes()[input.index()].output_layout();
                    for slot in param_slots {
                        slots.request(layout, *slot, &mut references, &mut work)?;
                    }
                }
                _ => {}
            }
        }
        slots.index(&mut work)?;
        // Each graph is already an occurrence forest. Index ownership once;
        // do not repeatedly walk all roots to locate each nested call.
        let mut owners = BTreeMap::new();
        for (root, use_id) in snapshot.bindings() {
            let arena = root.arena();
            let flow = &snapshot.flows()[&arena];
            let mut stack = vec![*use_id];
            while let Some(use_id) = stack.pop() {
                work.step()?;
                if owners
                    .insert(ProgramUseRef { arena, use_id }, *root)
                    .is_some()
                {
                    return Err(ProgramResolvedCallsError::SharedUse);
                }
                let invocation = &flow.uses()[&use_id];
                for child in invocation.arguments.iter().rev() {
                    work.step()?;
                    stack.push(*child);
                }
            }
        }
        let mut checked = BTreeMap::new();
        for (arena, flow) in snapshot.flows() {
            let definitions = &snapshot.roots().arenas()[arena];
            for (use_id, invocation) in flow.uses() {
                work.step()?;
                let definition = definitions
                    .node(invocation.definition)
                    .ok_or(ProgramResolvedCallsError::InvalidSite)?;
                let args = match definition.kind() {
                    StaticExprKind::FunctionCall { args, .. }
                    | StaticExprKind::BoundCall { args } => args,
                    _ => continue,
                };
                let occurrence = ProgramUseRef {
                    arena: *arena,
                    use_id: *use_id,
                };
                let site = ProgramCallSite::Expression(occurrence);
                let call = pending
                    .remove(&site)
                    .ok_or(ProgramResolvedCallsError::MissingSite(site))?;
                validate_to_base64_source(
                    &snapshot, occurrence, args, &call, &pending, &checked, &mut work,
                )?;
                validate_expression(&snapshot, occurrence, args, &call, &mut work)?;
                let scope = ProgramExpressionCallScope {
                    root: *owners
                        .get(&occurrence)
                        .ok_or(ProgramResolvedCallsError::InvalidSite)?,
                    occurrence,
                };
                checked.insert(
                    site,
                    ProgramResolvedCall {
                        specialization: call,
                        scope: CheckedScope::Expression(scope),
                    },
                );
            }
        }
        let mut relational_ids = BTreeSet::new();
        for (index, node) in snapshot.program().nodes().iter().enumerate() {
            work.step()?;
            let node_id = ProgramNodeId::new(index);
            let mut attach = |site,
                              scope,
                              validate: &mut dyn FnMut(
                &PureCallSpecialization,
                &mut CompileCheckpoints<'_>,
            )
                -> Result<(), ProgramResolvedCallsError>|
             -> Result<(), ProgramResolvedCallsError> {
                work.step()?;
                references = references
                    .checked_add(1)
                    .filter(|n| *n <= MAX_CONTROL_USE_REFERENCES)
                    .ok_or(ProgramResolvedCallsError::TooManyItems)?;
                let call = pending
                    .remove(&site)
                    .ok_or(ProgramResolvedCallsError::MissingSite(site))?;
                validate_relational_context(&snapshot, &call, &mut relational_ids)?;
                validate(&call, &mut work)?;
                checked.insert(
                    site,
                    ProgramResolvedCall {
                        specialization: call,
                        scope,
                    },
                );
                Ok(())
            };
            match node.kind() {
                ProgramNodeKind::Aggregate {
                    functions,
                    need_finalize,
                    ..
                } => {
                    for (ordinal, function) in functions.iter().enumerate() {
                        let call = u32::try_from(ordinal)
                            .map_err(|_| ProgramResolvedCallsError::TooManyItems)?;
                        attach(
                            ProgramCallSite::Aggregate {
                                node: node_id,
                                call,
                            },
                            CheckedScope::Aggregate(ProgramAggregateCallScope::Aggregate {
                                node: node_id,
                                call,
                            }),
                            &mut |token, work| {
                                validate_aggregate(
                                    &snapshot,
                                    ProgramAggregateCallScope::Aggregate {
                                        node: node_id,
                                        call,
                                    },
                                    function,
                                    *need_finalize,
                                    token,
                                    work,
                                )
                            },
                        )?;
                    }
                }
                ProgramNodeKind::GroupedTopN { functions, .. } => {
                    for (ordinal, function) in functions.iter().enumerate() {
                        let call = u32::try_from(ordinal)
                            .map_err(|_| ProgramResolvedCallsError::TooManyItems)?;
                        let scope = ProgramAggregateCallScope::TopNState {
                            node: node_id,
                            call,
                        };
                        attach(
                            ProgramCallSite::TopNState {
                                node: node_id,
                                call,
                            },
                            CheckedScope::Aggregate(scope),
                            &mut |token, work| {
                                validate_aggregate(&snapshot, scope, function, false, token, work)
                            },
                        )?;
                    }
                }
                ProgramNodeKind::Analytic { functions, .. } => {
                    for (ordinal, function) in functions.iter().enumerate() {
                        let call = u32::try_from(ordinal)
                            .map_err(|_| ProgramResolvedCallsError::TooManyItems)?;
                        attach(
                            ProgramCallSite::Window {
                                node: node_id,
                                call,
                            },
                            CheckedScope::Window {
                                node: node_id,
                                call,
                            },
                            &mut |token, work| {
                                validate_window(&snapshot, node_id, call, function, token, work)
                            },
                        )?;
                    }
                }
                ProgramNodeKind::TableWriter {
                    partial_aggregates,
                    projection,
                    writer_multiplex_layout,
                    ..
                } => {
                    for (ordinal, function) in partial_aggregates.iter().enumerate() {
                        let call = u32::try_from(ordinal)
                            .map_err(|_| ProgramResolvedCallsError::TooManyItems)?;
                        attach(
                            ProgramCallSite::WriterPartial {
                                node: node_id,
                                call,
                            },
                            CheckedScope::Aggregate(ProgramAggregateCallScope::WriterPartial {
                                node: node_id,
                                call,
                            }),
                            &mut |token, work| {
                                let aggregate = aggregate_contract(token)?;
                                if aggregate.phase() != AggregateKernelPhase::Partial
                                    || aggregate.call().selected().argument_types.len() != 1
                                    || aggregate.call().logical_argument_count() != 1
                                    || aggregate.distinct()
                                    || !aggregate.order_keys().is_empty()
                                {
                                    return Err(ProgramResolvedCallsError::WrongPhase);
                                }
                                check_aggregate_source(&function.resolved, aggregate, work)?;
                                let argument = aggregate
                                    .logical_argument_types()
                                    .next()
                                    .ok_or(ProgramResolvedCallsError::WrongArguments)?;
                                slots.check(
                                    &projection.layout,
                                    function.input_slot_id,
                                    argument,
                                    work,
                                )?;
                                slots.check(
                                    writer_multiplex_layout,
                                    function.intermediate_slot_id,
                                    aggregate.intermediate_type(),
                                    work,
                                )
                            },
                        )?;
                    }
                }
                ProgramNodeKind::TableFinish {
                    final_aggregates,
                    writer_multiplex_layout,
                    ..
                } => {
                    for (ordinal, function) in final_aggregates.calls.iter().enumerate() {
                        let call = u32::try_from(ordinal)
                            .map_err(|_| ProgramResolvedCallsError::TooManyItems)?;
                        attach(
                            ProgramCallSite::WriterFinal {
                                node: node_id,
                                call,
                            },
                            CheckedScope::Aggregate(ProgramAggregateCallScope::WriterFinal {
                                node: node_id,
                                call,
                            }),
                            &mut |token, work| {
                                let aggregate = aggregate_contract(token)?;
                                if aggregate.phase() != AggregateKernelPhase::Final {
                                    return Err(ProgramResolvedCallsError::WrongPhase);
                                }
                                check_aggregate_source(&function.resolved, aggregate, work)?;
                                slots.check(
                                    writer_multiplex_layout,
                                    function.intermediate_input_slot_id,
                                    aggregate
                                        .state_input_type()
                                        .ok_or(ProgramResolvedCallsError::WrongArguments)?,
                                    work,
                                )?;
                                // The finish owner creates this internal output
                                // column outside the input multiplex layout.
                                // Full typed slot facts are a later contract.
                                Ok(())
                            },
                        )?;
                    }
                }
                ProgramNodeKind::TableFunction {
                    input,
                    param_slots,
                    param_types,
                    ret_types,
                    fn_result_slots,
                    ..
                } => {
                    attach(
                        ProgramCallSite::Table { node: node_id },
                        CheckedScope::Table { node: node_id },
                        &mut |token, work| {
                            let PreparedPureKernel::Table(kernel) = token.prepared() else {
                                return Err(ProgramResolvedCallsError::WrongLifecycle);
                            };
                            let contract = kernel.contract();
                            if contract.argument_types().len() != param_slots.len()
                                || param_types.len() != param_slots.len()
                                || contract.result_types().len() != ret_types.len()
                                || ret_types.len() != fn_result_slots.len()
                            {
                                return Err(ProgramResolvedCallsError::WrongArguments);
                            }
                            let layout = snapshot.program().nodes()[input.index()].output_layout();
                            for ((slot, old_type), ty) in param_slots
                                .iter()
                                .zip(param_types)
                                .zip(contract.argument_types())
                            {
                                slots.check(layout, *slot, ty, work)?;
                                check_arrow(old_type, &ty.data_type, work)?;
                            }
                            for (old_type, ty) in ret_types.iter().zip(contract.result_types()) {
                                check_arrow(old_type, &ty.data_type, work)?;
                            }
                            Ok(())
                        },
                    )?;
                }
                _ => {}
            }
        }
        if !pending.is_empty() {
            return Err(ProgramResolvedCallsError::InvalidSite);
        }
        work.finish()?;
        Ok(Self {
            snapshot,
            calls: Arc::new(checked),
        })
    }
    pub const fn snapshot(&self) -> &ProgramRootControlBindings {
        &self.snapshot
    }
    pub fn calls(&self) -> &BTreeMap<ProgramCallSite, ProgramResolvedCall> {
        &self.calls
    }
}

fn validate_expression(
    snapshot: &ProgramRootControlBindings,
    occurrence: ProgramUseRef,
    args: &[crate::ProgramExprId],
    token: &PureCallSpecialization,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramResolvedCallsError> {
    let flow = &snapshot.flows()[&occurrence.arena];
    let invocation = &flow.uses()[&occurrence.use_id];
    let call = token.call_contract();
    if call.context() != invocation.context || token.effects().context() != invocation.context {
        return Err(ProgramResolvedCallsError::WrongContext);
    }
    if !call
        .effects()
        .argument_control
        .matches_scalar_shape(invocation.control)
    {
        return Err(ProgramResolvedCallsError::WrongControl);
    }
    if !matches!(
        token.prepared(),
        PreparedPureKernel::Scalar(_)
            | PreparedPureKernel::ScalarInvocation(_)
            | PreparedPureKernel::HigherOrder(_)
            | PreparedPureKernel::ControlIntrinsic(_)
    ) {
        return Err(ProgramResolvedCallsError::WrongLifecycle);
    }
    if call.selected().argument_types.len() != args.len()
        || call.logical_argument_count() != args.len()
    {
        return Err(ProgramResolvedCallsError::WrongArguments);
    }
    let definitions = &snapshot.roots().arenas()[&occurrence.arena];
    if call.function_id().as_str() == "builtin.scalar/regexp_count/v1" {
        if invocation.control != ControlShape::Eager || args.len() != 2 {
            return Err(ProgramResolvedCallsError::WrongArguments);
        }
        let pattern = definitions
            .node(args[1])
            .ok_or(ProgramResolvedCallsError::InvalidSite)?;
        let expected = if matches!(pattern.kind(), crate::StaticExprKind::Constant(_)) {
            novarocks_type_contract::RegexpCountPatternSource::NativeV1Utf8LiteralWhenPresent
        } else {
            novarocks_type_contract::RegexpCountPatternSource::Dynamic
        };
        if call.regexp_count_pattern_source() != Some(expected) {
            return Err(ProgramResolvedCallsError::WrongArguments);
        }
    } else if call.regexp_count_pattern_source().is_some() {
        return Err(ProgramResolvedCallsError::WrongArguments);
    }
    if let ControlShape::TemporalSource(shape) = invocation.control {
        let source = call
            .temporal_source()
            .ok_or(ProgramResolvedCallsError::WrongArguments)?;
        source
            .facts
            .validate()
            .map_err(|_| ProgramResolvedCallsError::WrongArguments)?;
        if source.facts.shape() != shape || source.sources.len() != invocation.arguments.len() {
            return Err(ProgramResolvedCallsError::WrongArguments);
        }
        let roles = shape.roles();
        for (ordinal, (use_id, source)) in
            invocation.arguments.iter().zip(&source.sources).enumerate()
        {
            work.step()?;
            let child = &flow.uses()[use_id];
            if Some(source.role) != roles[ordinal] || source.context != child.context {
                return Err(ProgramResolvedCallsError::WrongArguments);
            }
            check_arrow(
                definitions
                    .node(child.definition)
                    .ok_or(ProgramResolvedCallsError::InvalidSite)?
                    .data_type(),
                &source.value_type.data_type,
                work,
            )?;
        }
    } else if call.temporal_source().is_some() {
        return Err(ProgramResolvedCallsError::WrongArguments);
    } else if matches!(
        invocation.control,
        ControlShape::TypeOnly | ControlShape::NoArguments
    ) {
        if !invocation.arguments.is_empty() {
            return Err(ProgramResolvedCallsError::WrongArguments);
        }
        if invocation.control == ControlShape::NoArguments {
            // Demand-zero removes actual uses, never static definition/type
            // validation. Retain every original full-N source association.
            for (definition, ty) in args.iter().zip(&call.selected().argument_types) {
                work.step()?;
                let FunctionArgumentType::Value(ty) = ty else {
                    return Err(ProgramResolvedCallsError::WrongArguments);
                };
                check_arrow(
                    definitions
                        .node(*definition)
                        .ok_or(ProgramResolvedCallsError::InvalidSite)?
                        .data_type(),
                    &ty.data_type,
                    work,
                )?;
            }
        }
    } else {
        if invocation.arguments.len() != args.len() {
            return Err(ProgramResolvedCallsError::WrongArguments);
        }
        for ((use_id, definition), ty) in invocation
            .arguments
            .iter()
            .zip(args)
            .zip(&call.selected().argument_types)
        {
            work.step()?;
            let child = &flow.uses()[use_id];
            if child.definition != *definition {
                return Err(ProgramResolvedCallsError::WrongArguments);
            }
            match ty {
                FunctionArgumentType::Value(ty) => check_arrow(
                    definitions
                        .node(*definition)
                        .ok_or(ProgramResolvedCallsError::InvalidSite)?
                        .data_type(),
                    &ty.data_type,
                    work,
                )?,
                FunctionArgumentType::Lambda { .. } => {
                    if !matches!(
                        definitions.node(*definition).map(|node| node.kind()),
                        Some(StaticExprKind::LambdaFunction { .. })
                    ) {
                        return Err(ProgramResolvedCallsError::WrongBody);
                    }
                }
            }
        }
    }
    if let PreparedPureKernel::HigherOrder(kernel) = token.prepared() {
        let contract = kernel.contract();
        if invocation.arguments.get(contract.body_ordinal()) != Some(&contract.body_edge_use()) {
            return Err(ProgramResolvedCallsError::WrongBody);
        }
        let edge = &flow.uses()[&contract.body_edge_use()];
        let Some(StaticExprKind::LambdaFunction {
            body,
            arg_slots,
            common_sub_exprs,
            ..
        }) = definitions.node(edge.definition).map(|node| node.kind())
        else {
            return Err(ProgramResolvedCallsError::WrongBody);
        };
        if edge.control != ControlShape::LambdaBody
            || edge.arguments.len() != common_sub_exprs.len().saturating_add(1)
            || arg_slots.len() != contract.parameter_types().len()
        {
            return Err(ProgramResolvedCallsError::WrongBody);
        }
        for ((_, definition), use_id) in common_sub_exprs.iter().zip(&edge.arguments) {
            work.step()?;
            let common_use = &flow.uses()[use_id];
            if common_use.definition != *definition
                || common_use.context.domain != edge.context.domain
                || common_use.context.demand != EvaluationDemand::Value
            {
                return Err(ProgramResolvedCallsError::WrongBody);
            }
        }
        let body_use = &flow.uses()[edge.arguments.last().expect("checked lambda body arity")];
        if body_use.definition != *body || body_use.context != kernel.body_contract().context() {
            return Err(ProgramResolvedCallsError::WrongBody);
        }
        check_arrow(
            definitions
                .node(*body)
                .ok_or(ProgramResolvedCallsError::InvalidSite)?
                .data_type(),
            &contract.body_result_type().data_type,
            work,
        )?;
    }
    let FunctionResultType::Scalar(result) = &call.selected().result_type else {
        return Err(ProgramResolvedCallsError::WrongLifecycle);
    };
    if invocation.context.demand == EvaluationDemand::TruthOnly
        && (!matches!(result.data_type, arrow_schema::DataType::Boolean)
            || result.logical_type != novarocks_type_contract::ValueLogicalType::Physical)
    {
        return Err(ProgramResolvedCallsError::TypeMismatch);
    }
    check_arrow(
        definitions
            .node(invocation.definition)
            .ok_or(ProgramResolvedCallsError::InvalidSite)?
            .data_type(),
        &result.data_type,
        work,
    )
}

fn validate_relational_context(
    snapshot: &ProgramRootControlBindings,
    token: &PureCallSpecialization,
    ids: &mut BTreeSet<ExpressionUseId>,
) -> Result<(), ProgramResolvedCallsError> {
    let context = token.call_contract().context();
    if context.demand != EvaluationDemand::Value || token.effects().context() != context {
        return Err(ProgramResolvedCallsError::WrongContext);
    }
    let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
    if flow.uses().contains_key(&context.use_id) || !ids.insert(context.use_id) {
        return Err(ProgramResolvedCallsError::SharedUse);
    }
    let domain = flow
        .domains()
        .get(&context.domain)
        .ok_or(ProgramResolvedCallsError::InvalidDomain)?;
    if domain.parent.is_some() || domain.guard.is_some() {
        return Err(ProgramResolvedCallsError::InvalidDomain);
    }
    Ok(())
}

fn aggregate_contract(
    token: &PureCallSpecialization,
) -> Result<&AggregateCallContract, ProgramResolvedCallsError> {
    match token.prepared() {
        PreparedPureKernel::Aggregate(kernel) => Ok(kernel.contract()),
        _ => Err(ProgramResolvedCallsError::WrongLifecycle),
    }
}

fn validate_aggregate(
    snapshot: &ProgramRootControlBindings,
    scope: ProgramAggregateCallScope,
    source: &StaticAggregateCall,
    finalize: bool,
    token: &PureCallSpecialization,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramResolvedCallsError> {
    let (node, ordinal) = match scope {
        ProgramAggregateCallScope::Aggregate { node, call }
        | ProgramAggregateCallScope::TopNState { node, call } => (node, call),
        _ => return Err(ProgramResolvedCallsError::InvalidSite),
    };
    let role = |argument| match scope {
        ProgramAggregateCallScope::TopNState { .. } => {
            ProgramNodeExpressionRole::TopNStateArgument {
                call: ordinal,
                argument,
            }
        }
        _ => ProgramNodeExpressionRole::AggregateInput {
            call: ordinal,
            argument,
        },
    };
    let contract = aggregate_contract(token)?;
    check_aggregate_source(&source.resolved, contract, work)?;
    let same_interpretation = match (
        &source.state_interpretation,
        contract.state_interpretation(),
    ) {
        (None, None) => true,
        (Some(left), Some(right)) => left.matches_observed(right, || work.step())?,
        _ => false,
    };
    if !same_interpretation {
        return Err(ProgramResolvedCallsError::WrongOrder);
    }
    let phase = match (source.input_is_intermediate, finalize) {
        (false, false) => AggregateKernelPhase::Partial,
        (false, true) => AggregateKernelPhase::Single,
        (true, false) => AggregateKernelPhase::Intermediate,
        (true, true) => AggregateKernelPhase::Final,
    };
    if contract.phase() != phase {
        return Err(ProgramResolvedCallsError::WrongPhase);
    }
    check_order(
        contract,
        &source.order.is_asc_order,
        &source.order.nulls_first,
        source.order.is_distinct,
        work,
    )?;
    if phase.consumes_logical_arguments() {
        if source.inputs.len() != contract.call().selected().argument_types.len() {
            return Err(ProgramResolvedCallsError::WrongArguments);
        }
        for (argument, ty) in contract
            .logical_argument_types()
            .chain(contract.order_argument_types())
            .enumerate()
        {
            check_argument_root(
                snapshot,
                node,
                role(u32::try_from(argument).map_err(|_| ProgramResolvedCallsError::TooManyItems)?),
                ty,
                work,
            )?;
        }
    } else {
        if source.inputs.len() != 1 {
            return Err(ProgramResolvedCallsError::WrongArguments);
        }
        check_argument_root(
            snapshot,
            node,
            role(0),
            contract
                .state_input_type()
                .ok_or(ProgramResolvedCallsError::WrongArguments)?,
            work,
        )?;
    }
    Ok(())
}

/// Compare mandatory legacy carriers only. Optional types and display names
/// never establish logical identity or an exact selected implementation.
fn check_aggregate_source(
    source: &novarocks_functions::ResolvedAggregateSignature,
    contract: &AggregateCallContract,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramResolvedCallsError> {
    if &source.state_format != contract.state_format() {
        return Err(ProgramResolvedCallsError::WrongStateFormat);
    }
    if source.argument_types.len() != contract.call().selected().argument_types.len() {
        return Err(ProgramResolvedCallsError::WrongArguments);
    }
    for (source, ty) in source.argument_types.iter().zip(
        contract
            .logical_argument_types()
            .chain(contract.order_argument_types()),
    ) {
        check_arrow(source, &ty.data_type, work)?;
    }
    check_arrow(
        &source.intermediate_type,
        &contract.intermediate_type().data_type,
        work,
    )?;
    check_arrow(&source.output_type, &contract.final_type().data_type, work)
}

fn check_order(
    contract: &AggregateCallContract,
    ascending: &[bool],
    nulls: &[bool],
    distinct: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramResolvedCallsError> {
    if contract.distinct() != distinct
        || ascending.len() != contract.order_keys().len()
        || nulls.len() != ascending.len()
    {
        return Err(ProgramResolvedCallsError::WrongOrder);
    }
    for ((ascending, nulls), key) in ascending.iter().zip(nulls).zip(contract.order_keys()) {
        work.step()?;
        if (*ascending, *nulls) != (key.ascending, key.nulls_first) {
            return Err(ProgramResolvedCallsError::WrongOrder);
        }
    }
    Ok(())
}

/// A relational argument is an independent unguarded Value root. Its domain
/// is its own: a relational call context never borrows an expression
/// occurrence's domain, and the argument contributes only neutral effects.
fn check_argument_root(
    snapshot: &ProgramRootControlBindings,
    node: ProgramNodeId,
    role: ProgramNodeExpressionRole,
    ty: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramResolvedCallsError> {
    let site = ProgramExpressionRootSite::Node { node, role };
    let use_id = snapshot
        .bindings()
        .get(&site)
        .ok_or(ProgramResolvedCallsError::WrongArguments)?;
    let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
    let invocation = &flow.uses()[use_id];
    let unguarded = flow
        .domains()
        .get(&invocation.context.domain)
        .is_some_and(|domain| domain.parent.is_none() && domain.guard.is_none());
    if !unguarded || invocation.context.demand != EvaluationDemand::Value {
        return Err(ProgramResolvedCallsError::WrongContext);
    }
    let definition = snapshot.roots().arenas()[&ProgramExpressionArena::Main]
        .node(invocation.definition)
        .ok_or(ProgramResolvedCallsError::InvalidSite)?;
    check_arrow(definition.data_type(), &ty.data_type, work)
}

/// Each call carries its own frame and NULL treatment. A legacy call's frame
/// is exactly its prepared options' frame. A `Prepared` call carries the
/// explicit frame its compiler froze: equal to an explicit prepared frame, or,
/// where the prepared options preserve the absence of a frame, the compiler's
/// default derivation, which owns that proof. Such a default never has an
/// offset, so an offset frame there is refused.
fn validate_window(
    snapshot: &ProgramRootControlBindings,
    node: ProgramNodeId,
    ordinal: u32,
    source: &StaticWindowFunction,
    token: &PureCallSpecialization,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramResolvedCallsError> {
    let PreparedPureKernel::Window(kernel) = token.prepared() else {
        return Err(ProgramResolvedCallsError::WrongLifecycle);
    };
    let contract = kernel.contract();
    if contract.aggregate().is_some() != source.aggregate_binding.is_some() {
        return Err(ProgramResolvedCallsError::WrongLifecycle);
    }
    if let (Some(aggregate), Some((_, source))) = (contract.aggregate(), &source.aggregate_binding)
    {
        check_aggregate_source(source, aggregate, work)?;
    }
    if source.ignore_nulls != contract.options().ignore_nulls() {
        return Err(ProgramResolvedCallsError::WrongWindow);
    }
    let frame = source.frame.as_ref().map(convert_frame).transpose()?;
    let prepared = matches!(source.kind, WindowFunctionKind::Prepared);
    let frame_matches = match (contract.options().frame(), frame.as_ref()) {
        (Some(expected), Some(actual)) => expected == actual,
        (None, None) => !prepared,
        (None, Some(actual)) => {
            prepared
                && [actual.start, actual.end].iter().all(|bound| {
                    !matches!(bound, WindowBound::Preceding(_) | WindowBound::Following(_))
                })
        }
        (Some(_), None) => false,
    };
    if !frame_matches {
        return Err(ProgramResolvedCallsError::WrongWindow);
    }
    if let Some(aggregate) = contract.aggregate() {
        match &source.kind {
            WindowFunctionKind::ArrayAgg {
                is_distinct,
                is_asc_order,
                nulls_first,
            } => check_order(aggregate, is_asc_order, nulls_first, *is_distinct, work)?,
            _ => check_order(aggregate, &[], &[], false, work)?,
        }
    }
    if source.args.len() != contract.call().selected().argument_types.len() {
        return Err(ProgramResolvedCallsError::WrongArguments);
    }
    for (argument, ty) in contract
        .logical_argument_types()
        .chain(contract.order_argument_types())
        .enumerate()
    {
        check_argument_root(
            snapshot,
            node,
            ProgramNodeExpressionRole::WindowInput {
                call: ordinal,
                argument: u32::try_from(argument)
                    .map_err(|_| ProgramResolvedCallsError::TooManyItems)?,
            },
            ty,
            work,
        )?;
    }
    check_arrow(&source.return_type, &contract.result_type().data_type, work)
}

fn convert_frame(
    frame: &WindowFrame,
) -> Result<novarocks_type_contract::WindowFrame<u64>, ProgramResolvedCallsError> {
    fn bound(bound: WindowBoundary) -> Result<WindowBound<u64>, ProgramResolvedCallsError> {
        match bound {
            WindowBoundary::CurrentRow => Ok(WindowBound::CurrentRow),
            WindowBoundary::Preceding(0) | WindowBoundary::Following(0) => {
                Ok(WindowBound::CurrentRow)
            }
            WindowBoundary::Preceding(offset) => u64::try_from(offset)
                .map(WindowBound::Preceding)
                .map_err(|_| ProgramResolvedCallsError::WrongWindow),
            WindowBoundary::Following(offset) => u64::try_from(offset)
                .map(WindowBound::Following)
                .map_err(|_| ProgramResolvedCallsError::WrongWindow),
        }
    }
    Ok(novarocks_type_contract::WindowFrame {
        units: match frame.window_type {
            WindowType::Rows => WindowFrameUnits::Rows,
            WindowType::Range => WindowFrameUnits::Range,
        },
        start: frame
            .start
            .map(bound)
            .transpose()?
            .unwrap_or(WindowBound::UnboundedPreceding),
        end: frame
            .end
            .map(bound)
            .transpose()?
            .unwrap_or(WindowBound::UnboundedFollowing),
        exclusion: WindowFrameExclusion::NoOthers,
    })
}

/// Keys identify the exact borrowed layout object, never a schema digest or
/// equal-width surrogate. No pointer is dereferenced through an integer key.
#[derive(Default)]
struct RequestedSlots<'a> {
    layouts: BTreeMap<usize, (&'a StaticLayout, BTreeMap<SlotId, Option<usize>>)>,
}
impl<'a> RequestedSlots<'a> {
    fn request(
        &mut self,
        layout: &'a StaticLayout,
        slot: SlotId,
        references: &mut usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ProgramResolvedCallsError> {
        work.step()?;
        *references = references
            .checked_add(1)
            .filter(|n| *n <= MAX_CONTROL_USE_REFERENCES)
            .ok_or(ProgramResolvedCallsError::TooManyItems)?;
        self.layouts
            .entry(layout as *const StaticLayout as usize)
            .or_insert_with(|| (layout, BTreeMap::new()))
            .1
            .entry(slot)
            .or_insert(None);
        Ok(())
    }
    fn index(
        &mut self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ProgramResolvedCallsError> {
        for (layout, requested) in self.layouts.values_mut() {
            work.step()?;
            for (ordinal, slot) in layout.slots().iter().enumerate() {
                work.step()?;
                if let Some(found) = requested.get_mut(slot) {
                    *found = Some(ordinal);
                }
            }
        }
        Ok(())
    }
    fn check(
        &self,
        layout: &StaticLayout,
        slot: SlotId,
        ty: &FunctionValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ProgramResolvedCallsError> {
        work.step()?;
        let ordinal = self
            .layouts
            .get(&(layout as *const StaticLayout as usize))
            .and_then(|(_, requested)| requested.get(&slot))
            .copied()
            .flatten()
            .ok_or(ProgramResolvedCallsError::WrongArguments)?;
        check_arrow(
            layout.schema().field(ordinal).data_type(),
            &ty.data_type,
            work,
        )
    }
}

/// This is an observed carrier consistency check only. It cannot recover the
/// logical identity or NULL facts absent from the legacy source representation.
fn check_arrow(
    actual: &arrow_schema::DataType,
    expected: &arrow_schema::DataType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramResolvedCallsError> {
    if arrow_data_types_exact_observed::<ProgramResolvedCallsError>(actual, expected, || {
        work.step().map_err(Into::into)
    })? {
        Ok(())
    } else {
        Err(ProgramResolvedCallsError::TypeMismatch)
    }
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
pub(crate) mod relational_tests;

fn validate_to_base64_source(
    snapshot: &ProgramRootControlBindings,
    occurrence: ProgramUseRef,
    args: &[crate::ProgramExprId],
    token: &PureCallSpecialization,
    pending: &BTreeMap<ProgramCallSite, PureCallSpecialization>,
    checked: &BTreeMap<ProgramCallSite, ProgramResolvedCall>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramResolvedCallsError> {
    let call = token.call_contract();
    if call.function_id().as_str() != "builtin.scalar/to_base64/v1" {
        if call.to_base64_byte_source().is_some() {
            return Err(ProgramResolvedCallsError::WrongArguments);
        }
        return Ok(());
    }
    let flow = &snapshot.flows()[&occurrence.arena];
    let invocation = &flow.uses()[&occurrence.use_id];
    if invocation.control != ControlShape::Eager
        || args.len() != 1
        || invocation.arguments.len() != 1
    {
        return Err(ProgramResolvedCallsError::WrongArguments);
    }
    let definitions = &snapshot.roots().arenas()[&occurrence.arena];
    let source = definitions
        .node(args[0])
        .ok_or(ProgramResolvedCallsError::InvalidSite)?;
    work.step()?;
    let (name, emitted) = match source.kind() {
        crate::StaticExprKind::Literal(_) => return Err(ProgramResolvedCallsError::WrongArguments),
        crate::StaticExprKind::BoundCall { .. } => {
            let child = ProgramCallSite::Expression(ProgramUseRef {
                arena: occurrence.arena,
                use_id: invocation.arguments[0],
            });
            let child = pending
                .get(&child)
                .map(|t| t.call_contract())
                .or_else(|| checked.get(&child).map(ProgramResolvedCall::call_contract))
                .ok_or(ProgramResolvedCallsError::MissingSite(child))?;
            (
                Some(
                    novarocks_type_contract::native_v1_function_name(child.function_id())
                        .map_err(|_| ProgramResolvedCallsError::WrongArguments)?,
                ),
                true,
            )
        }
        crate::StaticExprKind::FunctionCall {
            kind: crate::StaticFunctionKind::Encryption(name),
            ..
        } => (Some(*name), false),
        _ => (None, false),
    };
    let expected = if emitted {
        work.flush()?;
        let fact =
            novarocks_type_contract::ToBase64ByteSource::from_immediate_native_function_name(name);
        work.flush()?;
        fact
    } else {
        novarocks_type_contract::ToBase64ByteSource::from_immediate_encryption_identity(name)
    };
    if call.to_base64_byte_source() != Some(expected) {
        return Err(ProgramResolvedCallsError::WrongArguments);
    }
    Ok(())
}
