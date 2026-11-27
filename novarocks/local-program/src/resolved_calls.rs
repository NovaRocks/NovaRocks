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

//! Frozen exact-owner attachments to actual local relational calls.
//!
//! This constructor checks exact call identity, ordered source channels
//! and typed lifecycle correspondence. It is not complete expression typing,
//! lexical capture closure or a proof that lowering preserved physical binding
//! identity: the compiler owns that proof. Legacy function family/name tags
//! never select or authenticate an implementation here. No mutable instance,
//! Task capability, memory grant or runtime name resolver is stored.

use crate::creation::{ProgramCreation, ProgramCreationFailure};
use crate::{
    ProgramNodeId, ProgramNodeKind, StaticAggregateCall, StaticLayout, StaticWindowFunction,
    WindowBoundary, WindowFrame, WindowFunctionKind, WindowType,
};
use novarocks_functions::{
    AggregateCallContract, AggregateKernelPhase, FunctionCallContract, PreparedAggregateHandle,
    PreparedPureKernel, PreparedTableKernel, PreparedWindowKernel, PureCallSpecialization,
    PureImplementationDeclaration, PurePreparationSource, ScopedCallEffects,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, FunctionValueType,
    MAX_CONTROL_USE_REFERENCES, PureCompileControl, ValueSchemaCreationScope, ValueTypeError,
    ValueTypeStorageLifetime, WindowBound, WindowFrameExclusion, WindowFrameUnits,
    arrow_data_types_exact_with_profile_observed,
};
use novarocks_types::SlotId;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProgramCallSite {
    Aggregate { node: ProgramNodeId, call: u32 },
    TopNState { node: ProgramNodeId, call: u32 },
    Window { node: ProgramNodeId, call: u32 },
    WriterPartial { node: ProgramNodeId, call: u32 },
    WriterFinal { node: ProgramNodeId, call: u32 },
    Table { node: ProgramNodeId },
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
}

#[derive(Clone, Copy, Debug)]
enum CheckedScope {
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
    pub fn effects(&self) -> ScopedCallEffects {
        self.specialization.effects()
    }
    pub fn state_template(&self) -> ProgramStateTemplate<'_> {
        match (self.specialization.prepared(), self.scope) {
            (PreparedPureKernel::Aggregate(kernel), CheckedScope::Aggregate(scope)) => {
                ProgramStateTemplate::Aggregate { scope, kernel }
            }
            (PreparedPureKernel::Window(kernel), CheckedScope::Window { node, call }) => {
                ProgramStateTemplate::WindowPartition { node, call, kernel }
            }
            (PreparedPureKernel::Table(kernel), CheckedScope::Table { node }) => {
                ProgramStateTemplate::TableCursor { node, kernel }
            }
            _ => unreachable!("checked factory pairs each site with its actual lifecycle"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProgramResolvedCalls {
    calls: Arc<BTreeMap<ProgramCallSite, ProgramResolvedCall>>,
    _storage: ValueTypeStorageLifetime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgramResolvedCallsError {
    Control(CompileControlError),
    TooManyItems,
    InvalidSite,
    DuplicateSite,
    MissingSite(ProgramCallSite),
    WrongSource,
    WrongContext,
    DuplicateRelationCall,
    WrongArguments,
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

impl<H> From<ProgramResolvedCallsError> for ProgramCreationFailure<ProgramResolvedCallsError, H> {
    fn from(error: ProgramResolvedCallsError) -> Self {
        match error {
            ProgramResolvedCallsError::Control(error) => Self::Control(error),
            other => Self::Owner(other),
        }
    }
}
impl ProgramResolvedCalls {
    pub fn try_new(
        graph: &crate::LocalProgramGraph,
        units: &[Arc<novarocks_expression::CompiledExprSet>],
        calls: Vec<(ProgramCallSite, PureCallSpecialization)>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramResolvedCallsError> {
        Self::try_new_created::<std::convert::Infallible>(graph, units, calls, None, None, control)
            .map_err(|error| match error {
                ProgramCreationFailure::Control(error) => ProgramResolvedCallsError::Control(error),
                ProgramCreationFailure::Owner(error) => error,
                ProgramCreationFailure::Resource(_) | ProgramCreationFailure::Capacity(_) => {
                    ProgramResolvedCallsError::Control(CompileControlError::ResourceExhausted)
                }
                ProgramCreationFailure::Creator(never) => match never {},
            })
    }
    pub fn try_new_with_creator_in<H>(
        graph: &crate::LocalProgramGraph,
        units: &[Arc<novarocks_expression::CompiledExprSet>],
        calls: Vec<(ProgramCallSite, PureCallSpecialization)>,
        temporary: &mut dyn ValueSchemaCreationScope<H>,
        retained: &mut dyn ValueSchemaCreationScope<H>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramCreationFailure<ProgramResolvedCallsError, H>> {
        Self::try_new_created(
            graph,
            units,
            calls,
            Some(temporary),
            Some(retained),
            control,
        )
    }
    fn try_new_created<H>(
        graph: &crate::LocalProgramGraph,
        units: &[Arc<novarocks_expression::CompiledExprSet>],
        calls: Vec<(ProgramCallSite, PureCallSpecialization)>,
        temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
        retained: Option<&mut dyn ValueSchemaCreationScope<H>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramCreationFailure<ProgramResolvedCallsError, H>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let mut temporary = ProgramCreation { creator: temporary };
        let mut retained = ProgramCreation { creator: retained };
        if calls.len() > MAX_CONTROL_USE_REFERENCES {
            return Err(ProgramResolvedCallsError::TooManyItems.into());
        }
        let mut pending = BTreeMap::new();
        for (site, call) in calls {
            work.step()?;
            if call.source() != PurePreparationSource::Frozen {
                return Err(ProgramResolvedCallsError::WrongSource.into());
            }
            // Requests follow actual insertions, including a duplicate's
            // replacement operation. Removal later never resets the allowance.
            if !pending.contains_key(&site) {
                temporary.before_tree_insert::<ProgramResolvedCallsError,
                    ProgramCallSite, PureCallSpecialization>(pending.len(), &mut work)?;
            }
            if pending.insert(site, call).is_some() {
                return Err(ProgramResolvedCallsError::DuplicateSite.into());
            }
        }
        let mut references = 0usize;
        for unit in units {
            let flow = unit.control_flow();
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
        for node in graph.nodes() {
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
                            &mut temporary,
                            &mut work,
                        )?;
                        slots.request(
                            writer_multiplex_layout,
                            call.intermediate_slot_id,
                            &mut temporary,
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
                            &mut temporary,
                            &mut work,
                        )?;
                    }
                }
                ProgramNodeKind::TableFunction {
                    input, param_slots, ..
                } => {
                    let layout = graph.nodes()[input.index()].output_layout();
                    for slot in param_slots {
                        slots.request(layout, *slot, &mut temporary, &mut work)?;
                    }
                }
                _ => {}
            }
        }
        slots.index(&mut work)?;
        let mut checked = BTreeMap::new();
        let mut relational_ids = BTreeSet::new();
        for (index, node) in graph.nodes().iter().enumerate() {
            work.step()?;
            let node_id = ProgramNodeId::new(index);
            let mut attach = |site,
                              scope,
                              validate: &mut dyn FnMut(
                &PureCallSpecialization,
                &mut CompileCheckpoints<'_>,
            )
                -> Result<(), ProgramResolvedCallsError>|
             -> Result<
                (),
                ProgramCreationFailure<ProgramResolvedCallsError, H>,
            > {
                work.step()?;
                references = references
                    .checked_add(1)
                    .filter(|n| *n <= MAX_CONTROL_USE_REFERENCES)
                    .ok_or(ProgramResolvedCallsError::TooManyItems)?;
                let call = pending
                    .remove(&site)
                    .ok_or(ProgramResolvedCallsError::MissingSite(site))?;
                validate_relational_context(&call, &mut relational_ids, &mut temporary, &mut work)?;
                validate(&call, &mut work)?;
                retained.before_tree_insert::<ProgramResolvedCallsError,
                    ProgramCallSite, ProgramResolvedCall>(checked.len(), &mut work)?;
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
                                    units,
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
                                validate_aggregate(units, scope, function, false, token, work)
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
                            &mut |token, work| validate_window(units, function, token, work),
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
                                check_aggregate_source(
                                    function.frozen.as_deref(),
                                    Some(&function.resolved),
                                    aggregate,
                                    work,
                                )?;
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
                                check_aggregate_source(
                                    function.frozen.as_deref(),
                                    Some(&function.resolved),
                                    aggregate,
                                    work,
                                )?;
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
                            let layout = graph.nodes()[input.index()].output_layout();
                            for ((slot, old_type), ty) in param_slots
                                .iter()
                                .zip(param_types)
                                .zip(contract.argument_types())
                            {
                                slots.check(layout, *slot, ty, work)?;
                                check_arrow(old_type, ty.data_type(), work)?;
                            }
                            for (old_type, ty) in ret_types.iter().zip(contract.result_types()) {
                                check_arrow(old_type, ty.data_type(), work)?;
                            }
                            Ok(())
                        },
                    )?;
                }
                _ => {}
            }
        }
        if !pending.is_empty() {
            return Err(ProgramResolvedCallsError::InvalidSite.into());
        }
        let storage = retained.storage();
        retained.before_arc::<ProgramResolvedCallsError,
            BTreeMap<ProgramCallSite, ProgramResolvedCall>>(&mut work)?;
        work.finish()?;
        Ok(Self {
            calls: Arc::new(checked),
            _storage: storage,
        })
    }
    pub fn calls(&self) -> &BTreeMap<ProgramCallSite, ProgramResolvedCall> {
        &self.calls
    }
}

fn validate_relational_context<H>(
    token: &PureCallSpecialization,
    ids: &mut BTreeSet<novarocks_type_contract::RelationCallId>,
    temporary: &mut ProgramCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramCreationFailure<ProgramResolvedCallsError, H>> {
    let context = token.call_contract().context();
    let relation = context
        .relation()
        .filter(|context| context.is_call())
        .ok_or(ProgramResolvedCallsError::WrongContext)?;
    if token.effects().context() != context {
        return Err(ProgramResolvedCallsError::WrongContext.into());
    }
    // Relation identities are independent of every expression unit. A token
    // names this actual call role, never an expression use or its region.
    let id = relation.identity.call;
    if !ids.contains(&id) {
        temporary.before_tree_insert::<ProgramResolvedCallsError, novarocks_type_contract::RelationCallId, ()>(ids.len(), work)?;
    }
    if !ids.insert(id) {
        return Err(ProgramResolvedCallsError::DuplicateRelationCall.into());
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
    units: &[Arc<novarocks_expression::CompiledExprSet>],
    scope: ProgramAggregateCallScope,
    source: &StaticAggregateCall,
    finalize: bool,
    token: &PureCallSpecialization,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramResolvedCallsError> {
    if !matches!(
        scope,
        ProgramAggregateCallScope::Aggregate { .. } | ProgramAggregateCallScope::TopNState { .. }
    ) {
        return Err(ProgramResolvedCallsError::InvalidSite);
    }
    let contract = aggregate_contract(token)?;
    check_aggregate_source(
        source.frozen.as_deref(),
        source.resolved.as_ref(),
        contract,
        work,
    )?;
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
            check_argument_root(units, source.inputs[argument], ty, work)?;
        }
    } else {
        if source.inputs.len() != 1 {
            return Err(ProgramResolvedCallsError::WrongArguments);
        }
        check_argument_root(
            units,
            source.inputs[0],
            contract
                .state_input_type()
                .ok_or(ProgramResolvedCallsError::WrongArguments)?,
            work,
        )?;
    }
    Ok(())
}

/// Compare the complete frozen source against the actual kernel owner. The
/// legacy carrier-only representation remains an explicit expansion adapter.
fn check_aggregate_source(
    frozen: Option<&crate::FrozenAggregateSelection>,
    source: Option<&novarocks_functions::ResolvedAggregateSignature>,
    contract: &AggregateCallContract,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramResolvedCallsError> {
    if let Some(frozen) = frozen {
        return if frozen.matches_selection_observed::<ProgramResolvedCallsError>(
            contract.call().function_id(),
            contract.call().selected(),
            contract.call().logical_argument_count(),
            || work.step().map_err(Into::into),
        )? {
            Ok(())
        } else {
            Err(ProgramResolvedCallsError::TypeMismatch)
        };
    }
    // Expansion-only carrier representation. Strong lowered calls retain the
    // original complete selection above; this adapter is retired with P07.
    let source = source.ok_or(ProgramResolvedCallsError::WrongLifecycle)?;
    let state = contract
        .merge_state()
        .ok_or(ProgramResolvedCallsError::WrongLifecycle)?;
    if source.state_format != state.state_format {
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
        check_arrow(source, ty.data_type(), work)?;
    }
    check_arrow(
        &source.intermediate_type,
        state.intermediate_type.data_type(),
        work,
    )?;
    check_arrow(&source.output_type, contract.final_type().data_type(), work)
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
    units: &[Arc<novarocks_expression::CompiledExprSet>],
    reference: crate::ProgramRootRef,
    ty: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramResolvedCallsError> {
    let unit = units
        .get(reference.unit.index())
        .ok_or(ProgramResolvedCallsError::InvalidSite)?;
    let root = unit
        .root(reference.root)
        .ok_or(ProgramResolvedCallsError::WrongArguments)?;
    let flow = unit.control_flow();
    let invocation = flow
        .uses()
        .get(&root.use_id)
        .ok_or(ProgramResolvedCallsError::WrongArguments)?;
    let unguarded = flow
        .regions()
        .get(&invocation.context.region)
        .is_some_and(|region| region.parent.is_none() && region.guard.is_none());
    if !unguarded || invocation.context.demand != EvaluationDemand::Value {
        return Err(ProgramResolvedCallsError::WrongContext);
    }
    let actual = crate::unit_refs::root_type(units, reference)
        .ok_or(ProgramResolvedCallsError::TypeMismatch)?;
    // Complete argument/nullability correspondence stays with the final
    // channel author. This attachment preserves its original carrier gate
    // and error precedence while borrowing the actual unit root.
    check_arrow(actual.data_type(), ty.data_type(), work)
}

/// Each call carries its own frame and NULL treatment. A legacy call's frame
/// is exactly its prepared options' frame. A `Prepared` call carries the
/// explicit frame its compiler froze: equal to an explicit prepared frame, or,
/// where the prepared options preserve the absence of a frame, the compiler's
/// default derivation, which owns that proof. Such a default never has an
/// offset, so an offset frame there is refused.
fn validate_window(
    units: &[Arc<novarocks_expression::CompiledExprSet>],
    source: &StaticWindowFunction,
    token: &PureCallSpecialization,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramResolvedCallsError> {
    let PreparedPureKernel::Window(kernel) = token.prepared() else {
        return Err(ProgramResolvedCallsError::WrongLifecycle);
    };
    let contract = kernel.contract();
    if contract.aggregate().is_some()
        != (source.frozen_aggregate.is_some() || source.aggregate_binding.is_some())
    {
        return Err(ProgramResolvedCallsError::WrongLifecycle);
    }
    if let Some(aggregate) = contract.aggregate() {
        check_aggregate_source(
            source.frozen_aggregate.as_deref(),
            source.aggregate_binding.as_ref().map(|(_, legacy)| legacy),
            aggregate,
            work,
        )?;
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
        let same_interpretation = match (
            &source.state_interpretation,
            aggregate.state_interpretation(),
        ) {
            (None, None) => true,
            (Some(left), Some(right)) => left.matches_observed(right, || work.step())?,
            _ => false,
        };
        if !same_interpretation {
            return Err(ProgramResolvedCallsError::WrongOrder);
        }
        match &source.kind {
            WindowFunctionKind::ArrayAgg {
                is_distinct,
                is_asc_order,
                nulls_first,
            } => check_order(aggregate, is_asc_order, nulls_first, *is_distinct, work)?,
            WindowFunctionKind::Prepared if source.state_interpretation.is_some() => {
                let facts = source
                    .state_interpretation
                    .as_ref()
                    .expect("checked state facts");
                if aggregate.distinct() != facts.distinct
                    || aggregate.order_keys().len() != facts.order_keys.len()
                {
                    return Err(ProgramResolvedCallsError::WrongOrder);
                }
                for (actual, expected) in aggregate.order_keys().iter().zip(facts.order_keys.iter())
                {
                    work.step()?;
                    if (actual.ascending, actual.nulls_first)
                        != (expected.ascending, expected.nulls_first)
                    {
                        return Err(ProgramResolvedCallsError::WrongOrder);
                    }
                }
            }
            _ => check_order(aggregate, &[], &[], false, work)?,
        }
    } else if source.state_interpretation.is_some() {
        return Err(ProgramResolvedCallsError::WrongOrder);
    }
    if source.args.len() != contract.logical_argument_types().len()
        || source.function_order_by.len() != contract.order_argument_types().len()
    {
        return Err(ProgramResolvedCallsError::WrongArguments);
    }
    for (reference, ty) in source.args.iter().chain(&source.function_order_by).zip(
        contract
            .logical_argument_types()
            .chain(contract.order_argument_types()),
    ) {
        check_argument_root(units, *reference, ty, work)?;
    }
    check_arrow(
        &source.return_type,
        contract.result_type().data_type(),
        work,
    )
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
    fn request<H>(
        &mut self,
        layout: &'a StaticLayout,
        slot: SlotId,
        temporary: &mut ProgramCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ProgramCreationFailure<ProgramResolvedCallsError, H>> {
        work.step()?;
        let key = layout as *const StaticLayout as usize;
        if !self.layouts.contains_key(&key) {
            temporary.before_tree_insert::<ProgramResolvedCallsError, usize,
                (&'a StaticLayout, BTreeMap<SlotId, Option<usize>>)>(self.layouts.len(), work)?;
        }
        let requested = &mut self
            .layouts
            .entry(key)
            .or_insert_with(|| (layout, BTreeMap::new()))
            .1;
        if !requested.contains_key(&slot) {
            temporary.before_tree_insert::<ProgramResolvedCallsError, SlotId, Option<usize>>(
                requested.len(),
                work,
            )?;
        }
        requested.entry(slot).or_insert(None);
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
            ty.data_type(),
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
    if arrow_data_types_exact_with_profile_observed::<ProgramResolvedCallsError>(
        actual,
        expected,
        novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
        || work.step().map_err(Into::into),
    )? {
        Ok(())
    } else {
        Err(ProgramResolvedCallsError::TypeMismatch)
    }
}

#[cfg(test)]
pub(crate) mod relational_tests;
