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

//! Mandatory complete types for actual operator/layout channel positions.
//! Calls borrow the same frozen prepared owner through ProgramTypedExpressions;
//! this table is not a second function binding or logical-type inference path.
//! Root field attributes remain in the actual immutable StaticLayout. Explicit
//! value types add NULL/logical facts; optional legacy slot metadata is ignored.
//! JoinScope exposes complete actual field facts only. This is not a proof of
//! every outer-join output NULL rule or of an arbitrary source/capture path.

use crate::{
    AnalyticOutputColumn, ProgramCallSite, ProgramExpressionArena, ProgramNodeId, ProgramNodeKind,
    ProgramTypedExpressions, StaticLayout, TableFunctionOutputSlot,
};
use novarocks_functions::{
    AggregateCallContract, FunctionArgumentType, FunctionValueType, KernelFailure,
    PreparedPureKernel, validate_function_value_type_observed,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl, ValueTypeError,
    arrow_data_types_exact_observed,
};
use novarocks_types::SlotId;
use std::{collections::BTreeMap, fmt, sync::Arc};

/// Independent channel-definition bound, not the invocation/use-reference
/// budget. Repeated wide schemas under distinct actual node positions count
/// as distinct channels and must not consume the 65,536-call control budget.
pub const MAX_PROGRAM_TYPED_CHANNELS: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProgramChannelLayoutRole {
    NodeOutput,
    WriterProjection,
    WriterMultiplex,
    WriterRootResult,
    JoinLeft,
    JoinRight,
    JoinScope,
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProgramChannelSite {
    Layout {
        node: ProgramNodeId,
        role: ProgramChannelLayoutRole,
        ordinal: u32,
    },
    /// Actual finish-owned internal output, independent of its input layout.
    WriterFinalOutput { node: ProgramNodeId, call: u32 },
    /// Every actual produced relation column, including unprojected results.
    TableResult { node: ProgramNodeId, result: u32 },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgramChannelTypeError {
    Control(CompileControlError),
    ValueType(ValueTypeError),
    Kernel(KernelFailure),
    TooManyChannels,
    DuplicateSite,
    MissingSite(ProgramChannelSite),
    InvalidSite,
    WrongShape,
    WrongKind,
    TypeMismatch,
}
impl From<CompileControlError> for ProgramChannelTypeError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ValueTypeError> for ProgramChannelTypeError {
    fn from(error: ValueTypeError) -> Self {
        Self::ValueType(error)
    }
}
impl fmt::Display for ProgramChannelTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid complete local channel types: {self:?}")
    }
}
impl std::error::Error for ProgramChannelTypeError {}

type SlotPosition = (ProgramNodeId, ProgramChannelLayoutRole, SlotId);
#[derive(Clone, Debug)]
pub struct ProgramTypedChannels {
    expressions: ProgramTypedExpressions,
    channels: Arc<BTreeMap<ProgramChannelSite, FunctionValueType>>,
    slots: Arc<BTreeMap<SlotPosition, u32>>,
}
impl ProgramTypedChannels {
    pub fn try_new(
        expressions: ProgramTypedExpressions,
        channels: Vec<(ProgramChannelSite, FunctionValueType)>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramChannelTypeError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        if channels.len() > MAX_PROGRAM_TYPED_CHANNELS {
            return Err(ProgramChannelTypeError::TooManyChannels);
        }
        let program = expressions.resolved_calls().snapshot().program();
        let mut expected_count = 0usize;
        for node in program.nodes() {
            work.step()?;
            visit_layouts(node.kind(), node.output_layout(), |_, layout| {
                work.step()?;
                add_count(&mut expected_count, layout.slots().len())
            })?;
            match node.kind() {
                ProgramNodeKind::TableFinish {
                    final_aggregates, ..
                } => add_count(&mut expected_count, final_aggregates.calls.len())?,
                ProgramNodeKind::TableFunction {
                    fn_result_slots, ..
                } => add_count(&mut expected_count, fn_result_slots.len())?,
                _ => {}
            }
        }
        let mut entries = BTreeMap::new();
        for (site, ty) in channels {
            work.step()?;
            if entries.insert(site, ty).is_some() {
                return Err(ProgramChannelTypeError::DuplicateSite);
            }
        }
        let mut slots = BTreeMap::new();
        for (index, node) in program.nodes().iter().enumerate() {
            let node_id = ProgramNodeId::new(index);
            visit_layouts(node.kind(), node.output_layout(), |role, layout| {
                for (ordinal, (slot, field)) in layout
                    .slots()
                    .iter()
                    .zip(layout.schema().fields())
                    .enumerate()
                {
                    work.step()?;
                    let ordinal = ordinal_id(ordinal)?;
                    let site = ProgramChannelSite::Layout {
                        node: node_id,
                        role,
                        ordinal,
                    };
                    let ty = entries
                        .get(&site)
                        .ok_or(ProgramChannelTypeError::MissingSite(site))?;
                    validate(ty, &mut work)?;
                    same_carrier(field.data_type(), &ty.data_type, &mut work)?;
                    if field.is_nullable() != ty.nullable {
                        return Err(ProgramChannelTypeError::TypeMismatch);
                    }
                    if field
                        .metadata()
                        .contains_key(novarocks_type_contract::NR_LOGICAL_TYPE_KEY)
                        && novarocks_type_contract::field_logical_type(field)? != ty.logical_type
                    {
                        return Err(ProgramChannelTypeError::TypeMismatch);
                    }
                    slots.insert((node_id, role, *slot), ordinal);
                }
                Ok(())
            })?;
            match node.kind() {
                ProgramNodeKind::TableFinish {
                    final_aggregates, ..
                } => {
                    for call in 0..final_aggregates.calls.len() {
                        work.step()?;
                        validate_entry(
                            &entries,
                            ProgramChannelSite::WriterFinalOutput {
                                node: node_id,
                                call: ordinal_id(call)?,
                            },
                            &mut work,
                        )?;
                    }
                }
                ProgramNodeKind::TableFunction {
                    fn_result_slots, ..
                } => {
                    for result in 0..fn_result_slots.len() {
                        work.step()?;
                        validate_entry(
                            &entries,
                            ProgramChannelSite::TableResult {
                                node: node_id,
                                result: ordinal_id(result)?,
                            },
                            &mut work,
                        )?;
                    }
                }
                _ => {}
            }
        }
        if entries.len() != expected_count {
            return Err(ProgramChannelTypeError::InvalidSite);
        }
        let checked = Self {
            expressions,
            channels: Arc::new(entries),
            slots: Arc::new(slots),
        };
        checked.validate_operators(&mut work)?;
        work.finish()?;
        Ok(checked)
    }
    pub const fn expressions(&self) -> &ProgramTypedExpressions {
        &self.expressions
    }
    pub fn channels(&self) -> &BTreeMap<ProgramChannelSite, FunctionValueType> {
        &self.channels
    }
    pub fn channel_type(&self, site: ProgramChannelSite) -> Option<&FunctionValueType> {
        self.channels.get(&site)
    }
    pub fn slot_type(
        &self,
        node: ProgramNodeId,
        role: ProgramChannelLayoutRole,
        slot: SlotId,
    ) -> Option<&FunctionValueType> {
        let ordinal = *self.slots.get(&(node, role, slot))?;
        self.channel_type(ProgramChannelSite::Layout {
            node,
            role,
            ordinal,
        })
    }
    pub fn channel_layout(
        &self,
        node: ProgramNodeId,
        role: ProgramChannelLayoutRole,
    ) -> Option<&StaticLayout> {
        let node = self
            .expressions
            .resolved_calls()
            .snapshot()
            .program()
            .nodes()
            .get(node.index())?;
        layout_for(node.kind(), node.output_layout(), role)
    }
    pub fn channel_slot(&self, site: ProgramChannelSite) -> Option<SlotId> {
        match site {
            ProgramChannelSite::Layout {
                node,
                role,
                ordinal,
            } => self
                .channel_layout(node, role)?
                .slots()
                .get(ordinal as usize)
                .copied(),
            ProgramChannelSite::WriterFinalOutput { node, call } => match self
                .expressions
                .resolved_calls()
                .snapshot()
                .program()
                .nodes()
                .get(node.index())?
                .kind()
            {
                ProgramNodeKind::TableFinish {
                    final_aggregates, ..
                } => final_aggregates
                    .calls
                    .get(call as usize)
                    .map(|call| call.final_output_slot_id),
                _ => None,
            },
            ProgramChannelSite::TableResult { node, result } => match self
                .expressions
                .resolved_calls()
                .snapshot()
                .program()
                .nodes()
                .get(node.index())?
                .kind()
            {
                ProgramNodeKind::TableFunction {
                    fn_result_slots, ..
                } => fn_result_slots.get(result as usize).copied(),
                _ => None,
            },
        }
    }
    fn actual_slot(
        &self,
        node: ProgramNodeId,
        role: ProgramChannelLayoutRole,
        slot: SlotId,
    ) -> Result<&FunctionValueType, ProgramChannelTypeError> {
        self.slot_type(node, role, slot)
            .ok_or(ProgramChannelTypeError::WrongShape)
    }
    fn output(
        &self,
        node: ProgramNodeId,
        ordinal: usize,
    ) -> Result<&FunctionValueType, ProgramChannelTypeError> {
        let site = ProgramChannelSite::Layout {
            node,
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: ordinal_id(ordinal)?,
        };
        self.channel_type(site)
            .ok_or(ProgramChannelTypeError::WrongShape)
    }
    fn definition(
        &self,
        arena: ProgramExpressionArena,
        id: crate::ProgramExprId,
    ) -> Result<&FunctionValueType, ProgramChannelTypeError> {
        match self.expressions.definition_type(arena, id) {
            Some(FunctionArgumentType::Value(ty)) => Ok(ty),
            _ => Err(ProgramChannelTypeError::WrongKind),
        }
    }
    fn aggregate(
        &self,
        site: ProgramCallSite,
    ) -> Result<&AggregateCallContract, ProgramChannelTypeError> {
        match self
            .expressions
            .resolved_calls()
            .calls()
            .get(&site)
            .map(|entry| entry.specialization().prepared())
        {
            Some(PreparedPureKernel::Aggregate(kernel)) => Ok(kernel.contract()),
            _ => Err(ProgramChannelTypeError::WrongKind),
        }
    }
    fn validate_operators(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ProgramChannelTypeError> {
        use ProgramChannelLayoutRole as Role;
        let program = self.expressions.resolved_calls().snapshot().program();
        for (index, node) in program.nodes().iter().enumerate() {
            work.step()?;
            let node_id = ProgramNodeId::new(index);
            match node.kind() {
                ProgramNodeKind::Values { values } => {
                    // A dynamic cell is assembled into its column unchanged,
                    // so its definition has exactly the column's full type.
                    for cell in values.dynamic_cells() {
                        same_value(
                            self.output(node_id, cell.column as usize)?,
                            self.definition(ProgramExpressionArena::Main, cell.definition)?,
                            false,
                            work,
                        )?;
                    }
                }
                ProgramNodeKind::GenerateSeries {
                    input,
                    parameter_slots,
                } => {
                    let input_layout = program.nodes()[input.index()].output_layout();
                    for slot in parameter_slots.iter() {
                        let ordinal = input_layout
                            .slots()
                            .iter()
                            .position(|candidate| candidate == slot)
                            .ok_or(ProgramChannelTypeError::WrongShape)?;
                        same_value(
                            self.output(*input, ordinal)?,
                            self.output(node_id, 0)?,
                            false,
                            work,
                        )?;
                        work.step()?;
                    }
                }
                ProgramNodeKind::Aggregate {
                    group_by,
                    functions,
                    ..
                }
                | ProgramNodeKind::GroupedTopN {
                    group_by,
                    functions,
                    ..
                } => {
                    let grouped = matches!(node.kind(), ProgramNodeKind::GroupedTopN { .. });
                    if let ProgramNodeKind::GroupedTopN {
                        order_by,
                        rank_group_ordinals,
                        comparator,
                        ..
                    } = node.kind()
                    {
                        let arena = &self
                            .expressions
                            .resolved_calls()
                            .snapshot()
                            .roots()
                            .arenas()[&ProgramExpressionArena::Main];
                        for (key, group) in order_by.iter().zip(rank_group_ordinals) {
                            work.step()?;
                            let id = *group_by
                                .get(*group)
                                .ok_or(ProgramChannelTypeError::WrongShape)?;
                            let group_slot = arena
                                .node(id)
                                .ok_or(ProgramChannelTypeError::InvalidSite)?
                                .kind();
                            let rank_slot = arena
                                .node(key.expr)
                                .ok_or(ProgramChannelTypeError::InvalidSite)?
                                .kind();
                            if !matches!((group_slot, rank_slot), (crate::StaticExprKind::SlotId(a), crate::StaticExprKind::SlotId(b)) if a == b)
                            {
                                return Err(ProgramChannelTypeError::WrongShape);
                            }
                            let ty = self.definition(ProgramExpressionArena::Main, id)?;
                            if !comparator.supports_value_type(ty)
                                || !novarocks_type_contract::PartitionHashAlgorithm::NativeExchangeV1.supports_partition_key(&ty.data_type)
                            { return Err(ProgramChannelTypeError::TypeMismatch); }
                            same_value(
                                ty,
                                self.definition(ProgramExpressionArena::Main, key.expr)?,
                                false,
                                work,
                            )?;
                        }
                    }
                    // Actual aggregate arrays are group keys followed by call
                    // emissions; the existing runtime consumes that prefix.
                    for (ordinal, id) in group_by.iter().enumerate() {
                        same_value(
                            self.output(node_id, ordinal)?,
                            self.definition(ProgramExpressionArena::Main, *id)?,
                            false,
                            work,
                        )?;
                    }
                    for (ordinal, source) in functions.iter().enumerate() {
                        work.step()?;
                        let call = ordinal_id(ordinal)?;
                        let site = if grouped {
                            ProgramCallSite::TopNState {
                                node: node_id,
                                call,
                            }
                        } else {
                            ProgramCallSite::Aggregate {
                                node: node_id,
                                call,
                            }
                        };
                        let contract = self.aggregate(site)?;
                        if contract.phase().consumes_logical_arguments() {
                            let expected = contract
                                .logical_argument_types()
                                .chain(contract.order_argument_types());
                            if source.inputs.len()
                                != contract.call().selected().argument_types.len()
                            {
                                return Err(ProgramChannelTypeError::WrongShape);
                            }
                            for (id, expected) in source.inputs.iter().zip(expected) {
                                same_value(
                                    self.definition(ProgramExpressionArena::Main, *id)?,
                                    expected,
                                    true,
                                    work,
                                )?;
                            }
                        } else {
                            if source.inputs.len() != 1 {
                                return Err(ProgramChannelTypeError::WrongShape);
                            }
                            same_value(
                                self.definition(ProgramExpressionArena::Main, source.inputs[0])?,
                                contract
                                    .state_input_type()
                                    .ok_or(ProgramChannelTypeError::WrongShape)?,
                                true,
                                work,
                            )?;
                        }
                        let output = if contract.phase().produces_final_result() {
                            contract.final_type()
                        } else {
                            contract.intermediate_type()
                        };
                        same_value(
                            self.output(
                                node_id,
                                group_by
                                    .len()
                                    .checked_add(ordinal)
                                    .ok_or(ProgramChannelTypeError::TooManyChannels)?,
                            )?,
                            output,
                            false,
                            work,
                        )?;
                    }
                }
                ProgramNodeKind::Analytic {
                    input,
                    functions,
                    output_columns,
                    ..
                } => {
                    for (ordinal, source) in functions.iter().enumerate() {
                        work.step()?;
                        let site = ProgramCallSite::Window {
                            node: node_id,
                            call: ordinal_id(ordinal)?,
                        };
                        let Some(PreparedPureKernel::Window(kernel)) = self
                            .expressions
                            .resolved_calls()
                            .calls()
                            .get(&site)
                            .map(|entry| entry.specialization().prepared())
                        else {
                            return Err(ProgramChannelTypeError::WrongKind);
                        };
                        let contract = kernel.contract();
                        let expected = contract
                            .logical_argument_types()
                            .chain(contract.order_argument_types());
                        if source.args.len() != contract.call().selected().argument_types.len() {
                            return Err(ProgramChannelTypeError::WrongShape);
                        }
                        for (id, expected) in source.args.iter().zip(expected) {
                            same_value(
                                self.definition(ProgramExpressionArena::Main, *id)?,
                                expected,
                                true,
                                work,
                            )?;
                        }
                    }
                    for (ordinal, output) in output_columns.iter().enumerate() {
                        work.step()?;
                        let expected = match output {
                            AnalyticOutputColumn::InputSlotId(slot) => {
                                self.actual_slot(*input, Role::NodeOutput, *slot)?
                            }
                            AnalyticOutputColumn::Window(call) => {
                                let site = ProgramCallSite::Window {
                                    node: node_id,
                                    call: ordinal_id(*call)?,
                                };
                                let Some(PreparedPureKernel::Window(kernel)) = self
                                    .expressions
                                    .resolved_calls()
                                    .calls()
                                    .get(&site)
                                    .map(|entry| entry.specialization().prepared())
                                else {
                                    return Err(ProgramChannelTypeError::WrongKind);
                                };
                                kernel.contract().result_type()
                            }
                        };
                        same_value(self.output(node_id, ordinal)?, expected, false, work)?;
                    }
                }
                ProgramNodeKind::TableWriter {
                    projection,
                    writer_multiplex_layout,
                    partial_aggregates,
                    ..
                } => {
                    for (ordinal, id) in projection.expressions.iter().enumerate() {
                        let site = ProgramChannelSite::Layout {
                            node: node_id,
                            role: Role::WriterProjection,
                            ordinal: ordinal_id(ordinal)?,
                        };
                        same_value(
                            self.channel_type(site)
                                .ok_or(ProgramChannelTypeError::WrongShape)?,
                            self.definition(
                                ProgramExpressionArena::WriterProjection(node_id),
                                *id,
                            )?,
                            false,
                            work,
                        )?;
                    }
                    for (ordinal, source) in partial_aggregates.iter().enumerate() {
                        let contract = self.aggregate(ProgramCallSite::WriterPartial {
                            node: node_id,
                            call: ordinal_id(ordinal)?,
                        })?;
                        let expected = contract
                            .logical_argument_types()
                            .next()
                            .ok_or(ProgramChannelTypeError::WrongShape)?;
                        same_value(
                            self.actual_slot(
                                node_id,
                                Role::WriterProjection,
                                source.input_slot_id,
                            )?,
                            expected,
                            true,
                            work,
                        )?;
                        // The auxiliary channel carries this state on the
                        // writer's aggregate rows only and is NULL on every
                        // other row, so it may widen the state's nullability.
                        same_value(
                            contract.intermediate_type(),
                            self.actual_slot(
                                node_id,
                                Role::WriterMultiplex,
                                source.intermediate_slot_id,
                            )?,
                            true,
                            work,
                        )?;
                    }
                    for slot in writer_multiplex_layout.slots() {
                        same_value(
                            self.actual_slot(node_id, Role::NodeOutput, *slot)?,
                            self.actual_slot(node_id, Role::WriterMultiplex, *slot)?,
                            false,
                            work,
                        )?;
                    }
                }
                ProgramNodeKind::TableFinish {
                    inputs,
                    writer_multiplex_layout,
                    root_result_layout,
                    final_aggregates,
                    ..
                } => {
                    for input in inputs {
                        for slot in writer_multiplex_layout.slots() {
                            same_value(
                                self.actual_slot(*input, Role::NodeOutput, *slot)?,
                                self.actual_slot(node_id, Role::WriterMultiplex, *slot)?,
                                false,
                                work,
                            )?;
                        }
                    }
                    for slot in root_result_layout.slots() {
                        same_value(
                            self.actual_slot(node_id, Role::NodeOutput, *slot)?,
                            self.actual_slot(node_id, Role::WriterRootResult, *slot)?,
                            false,
                            work,
                        )?;
                    }
                    for (ordinal, source) in final_aggregates.calls.iter().enumerate() {
                        let call = ordinal_id(ordinal)?;
                        let contract = self.aggregate(ProgramCallSite::WriterFinal {
                            node: node_id,
                            call,
                        })?;
                        same_value(
                            self.actual_slot(
                                node_id,
                                Role::WriterMultiplex,
                                source.intermediate_input_slot_id,
                            )?,
                            contract
                                .state_input_type()
                                .ok_or(ProgramChannelTypeError::WrongShape)?,
                            true,
                            work,
                        )?;
                        let site = ProgramChannelSite::WriterFinalOutput {
                            node: node_id,
                            call,
                        };
                        let output = self
                            .channel_type(site)
                            .ok_or(ProgramChannelTypeError::WrongShape)?;
                        same_value(output, contract.final_type(), false, work)?;
                        // A directly exported internal slot must keep the same
                        // type; an unpivot-transformed result may omit it.
                        if let Some(root_output) = self.slot_type(
                            node_id,
                            Role::WriterRootResult,
                            source.final_output_slot_id,
                        ) {
                            same_value(root_output, output, false, work)?;
                        }
                    }
                }
                ProgramNodeKind::TableFunction {
                    input,
                    param_slots,
                    fn_result_slots,
                    output_slot_sources,
                    outer_slots,
                    is_left_join,
                    ..
                } => {
                    let site = ProgramCallSite::Table { node: node_id };
                    let Some(PreparedPureKernel::Table(kernel)) = self
                        .expressions
                        .resolved_calls()
                        .calls()
                        .get(&site)
                        .map(|entry| entry.specialization().prepared())
                    else {
                        return Err(ProgramChannelTypeError::WrongKind);
                    };
                    let contract = kernel.contract();
                    if param_slots.len() != contract.argument_types().len()
                        || fn_result_slots.len() != contract.result_types().len()
                        || output_slot_sources.len() != node.output_layout().slots().len()
                    {
                        return Err(ProgramChannelTypeError::WrongShape);
                    };
                    for (slot, expected) in param_slots.iter().zip(contract.argument_types()) {
                        same_value(
                            self.actual_slot(*input, Role::NodeOutput, *slot)?,
                            expected,
                            true,
                            work,
                        )?;
                    }
                    for (ordinal, expected) in contract.result_types().iter().enumerate() {
                        let site = ProgramChannelSite::TableResult {
                            node: node_id,
                            result: ordinal_id(ordinal)?,
                        };
                        same_value(
                            self.channel_type(site)
                                .ok_or(ProgramChannelTypeError::WrongShape)?,
                            expected,
                            false,
                            work,
                        )?;
                    }
                    for slot in outer_slots {
                        work.step()?;
                        self.actual_slot(*input, Role::NodeOutput, *slot)?;
                    }
                    for (ordinal, output) in output_slot_sources.iter().enumerate() {
                        work.step()?;
                        let expected = match output {
                            TableFunctionOutputSlot::Outer { slot } => {
                                self.actual_slot(*input, Role::NodeOutput, *slot)?
                            }
                            TableFunctionOutputSlot::Result { index } => self
                                .channel_type(ProgramChannelSite::TableResult {
                                    node: node_id,
                                    result: ordinal_id(*index)?,
                                })
                                .ok_or(ProgramChannelTypeError::WrongShape)?,
                        };
                        if *is_left_join && matches!(output, TableFunctionOutputSlot::Result { .. })
                        {
                            let actual = self.output(node_id, ordinal)?;
                            work.step()?;
                            if !actual.nullable || actual.logical_type != expected.logical_type {
                                return Err(ProgramChannelTypeError::TypeMismatch);
                            }
                            same_carrier(&actual.data_type, &expected.data_type, work)?;
                        } else {
                            same_value(self.output(node_id, ordinal)?, expected, false, work)?;
                        }
                    }
                }
                ProgramNodeKind::Join {
                    left,
                    right,
                    left_layout,
                    right_layout,
                    ..
                }
                | ProgramNodeKind::NestedLoopJoin {
                    left,
                    right,
                    left_layout,
                    right_layout,
                    ..
                } => {
                    for (input, role, layout) in [
                        (*left, Role::JoinLeft, left_layout),
                        (*right, Role::JoinRight, right_layout),
                    ] {
                        for slot in layout.slots() {
                            same_value(
                                self.actual_slot(node_id, role, *slot)?,
                                self.actual_slot(input, Role::NodeOutput, *slot)?,
                                false,
                                work,
                            )?;
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}
fn layout_for<'a>(
    kind: &'a ProgramNodeKind,
    output: &'a StaticLayout,
    role: ProgramChannelLayoutRole,
) -> Option<&'a StaticLayout> {
    use ProgramChannelLayoutRole as Role;
    match (role, kind) {
        (Role::NodeOutput, _) => Some(output),
        (Role::WriterProjection, ProgramNodeKind::TableWriter { projection, .. }) => {
            Some(&projection.layout)
        }
        (
            Role::WriterMultiplex,
            ProgramNodeKind::TableWriter {
                writer_multiplex_layout,
                ..
            }
            | ProgramNodeKind::TableFinish {
                writer_multiplex_layout,
                ..
            },
        ) => Some(writer_multiplex_layout),
        (
            Role::WriterRootResult,
            ProgramNodeKind::TableFinish {
                root_result_layout, ..
            },
        ) => Some(root_result_layout),
        (
            Role::JoinLeft,
            ProgramNodeKind::Join { left_layout, .. }
            | ProgramNodeKind::NestedLoopJoin { left_layout, .. },
        ) => Some(left_layout),
        (
            Role::JoinRight,
            ProgramNodeKind::Join { right_layout, .. }
            | ProgramNodeKind::NestedLoopJoin { right_layout, .. },
        ) => Some(right_layout),
        (
            Role::JoinScope,
            ProgramNodeKind::Join {
                join_scope_layout, ..
            }
            | ProgramNodeKind::NestedLoopJoin {
                join_scope_layout, ..
            },
        ) => Some(join_scope_layout),
        _ => None,
    }
}
fn visit_layouts(
    kind: &ProgramNodeKind,
    output: &StaticLayout,
    mut visit: impl FnMut(
        ProgramChannelLayoutRole,
        &StaticLayout,
    ) -> Result<(), ProgramChannelTypeError>,
) -> Result<(), ProgramChannelTypeError> {
    use ProgramChannelLayoutRole as Role;
    for role in [
        Role::NodeOutput,
        Role::WriterProjection,
        Role::WriterMultiplex,
        Role::WriterRootResult,
        Role::JoinLeft,
        Role::JoinRight,
        Role::JoinScope,
    ] {
        if let Some(layout) = layout_for(kind, output, role) {
            visit(role, layout)?;
        }
    }
    Ok(())
}
fn add_count(count: &mut usize, amount: usize) -> Result<(), ProgramChannelTypeError> {
    *count = count
        .checked_add(amount)
        .filter(|n| *n <= MAX_PROGRAM_TYPED_CHANNELS)
        .ok_or(ProgramChannelTypeError::TooManyChannels)?;
    Ok(())
}
fn ordinal_id(ordinal: usize) -> Result<u32, ProgramChannelTypeError> {
    u32::try_from(ordinal).map_err(|_| ProgramChannelTypeError::TooManyChannels)
}
fn validate_entry(
    entries: &BTreeMap<ProgramChannelSite, FunctionValueType>,
    site: ProgramChannelSite,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramChannelTypeError> {
    validate(
        entries
            .get(&site)
            .ok_or(ProgramChannelTypeError::MissingSite(site))?,
        work,
    )
}
fn validate(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramChannelTypeError> {
    validate_function_value_type_observed(value, work).map_err(|error| match error {
        KernelFailure::Cancelled => {
            ProgramChannelTypeError::Control(CompileControlError::Cancelled)
        }
        KernelFailure::DeadlineExceeded => {
            ProgramChannelTypeError::Control(CompileControlError::DeadlineExceeded)
        }
        KernelFailure::ResourceExhausted => {
            ProgramChannelTypeError::Control(CompileControlError::ResourceExhausted)
        }
        error => ProgramChannelTypeError::Kernel(error),
    })
}
fn same_carrier(
    actual: &arrow_schema::DataType,
    expected: &arrow_schema::DataType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramChannelTypeError> {
    if arrow_data_types_exact_observed::<ProgramChannelTypeError>(actual, expected, || {
        work.step().map_err(Into::into)
    })? {
        Ok(())
    } else {
        Err(ProgramChannelTypeError::TypeMismatch)
    }
}
fn same_value(
    actual: &FunctionValueType,
    expected: &FunctionValueType,
    argument: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramChannelTypeError> {
    work.step()?;
    let nullable = if argument {
        expected.nullable || !actual.nullable
    } else {
        expected.nullable == actual.nullable
    };
    if !nullable || actual.logical_type != expected.logical_type {
        return Err(ProgramChannelTypeError::TypeMismatch);
    };
    same_carrier(&actual.data_type, &expected.data_type, work)
}

#[cfg(test)]
mod tests;
