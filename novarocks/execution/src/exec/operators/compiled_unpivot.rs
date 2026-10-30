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

//! Compiled Unpivot: every input row becomes one output row per frozen value
//! mapping.
//!
//! Output channels follow the node's frozen layout exactly: passthrough
//! channels copy their input channel, the value channel takes the mapping's
//! input channel, and each literal channel takes the mapping's constant. A
//! scalar constant is a compiled `UnpivotConstant` root, evaluated by this
//! driver's own instance once per input batch over every input row; Int32List
//! and Utf8Map constants are frozen values materialized with the output
//! channel's exact Arrow type. Expansion is mapping-major (the relational
//! contract promises no order), and every output batch honors the frozen row
//! and byte budgets through the same budget search as the legacy Unpivot.
//!
//! Reused legacy pieces are array code only: segment flattening, owned part
//! concatenation and the budget search. The legacy ExprArena constant
//! evaluation is not used.
//!
//! The expansion itself -- per-channel producers over one input batch and its
//! evaluated constants, emitted under the frozen budgets -- is the reusable
//! `UnpivotExpansion` core; the writer's grouped Unpivot runs it once per
//! target with that target's own mappings.

use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::alloc::Layout;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, ListArray, MapArray, StringArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, FieldRef};
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind, StaticLayout, UnpivotConstant,
};
use novarocks_types::SlotId;

use super::compiled_expression::{RuntimeKernelControl, evaluate_all, instances, root_value_type};
use super::unpivot_processor::{
    concat_owned_with_construction, next_bounded_output_with_construction, unpivot_copy_failure,
    unpivot_vec,
};
use super::unpivot_processor::{unpivot_arrow, unpivot_contract};
use crate::exec::chunk::{Chunk, ChunkBatchProjection, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputConstructionOwner, InputEncodingPolicy,
    Operator,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::PreparedInput,
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
    step_context::StepContext,
};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use novarocks_functions::array_projection::ArrayNormalizationStep;
use novarocks_functions::{KernelEvaluationControl, KernelFailure};
use novarocks_type_contract::{ValueSchemaCreation, ValueTypeStorageLifetime};

/// The frozen source of one literal channel for one mapping.
#[derive(Clone)]
pub(crate) enum LiteralSource {
    /// Index of an evaluated scalar constant column.
    Root(usize),
    /// A frozen Int32 list, materialized with the channel's list item field.
    Int32List(Arc<[i32]>),
    /// A frozen Utf8 map, materialized with the channel's entries field.
    Utf8Map(Arc<[(Arc<str>, Arc<str>)]>),
}

/// The producer of one output channel, in output order.
#[derive(Clone)]
pub(crate) enum Producer {
    Passthrough {
        input: usize,
    },
    /// The input ordinal of each mapping's value.
    Value {
        inputs: Vec<usize>,
    },
    /// Each mapping's constant.
    Literal {
        sources: Vec<LiteralSource>,
    },
}

pub struct CompiledUnpivotProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    producers: Arc<[Producer]>,
    mappings: usize,
    max_output_rows: usize,
    max_output_bytes: usize,
    output_layout: StaticLayout,
    input_layout: StaticLayout,
    error: Arc<RuntimeErrorState>,
}

pub(crate) fn list_item_field(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(item) if item.data_type() == &DataType::Int32 => Some(item),
        _ => None,
    }
}

pub(crate) fn map_entries_field(data_type: &DataType) -> Option<(&FieldRef, bool)> {
    let DataType::Map(entries, sorted) = data_type else {
        return None;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return None;
    };
    (fields.len() == 2
        && fields
            .iter()
            .all(|field| field.data_type() == &DataType::Utf8))
    .then_some((entries, *sorted))
}

impl CompiledUnpivotProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<Self> {
        let at = node.index();
        let nodes = program.graph().nodes();
        let graph_node = nodes.get(at).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                format!("compiled Unpivot node {at} is absent"),
            )
        })?;
        let ProgramNodeKind::Unpivot {
            input,
            passthrough_columns,
            value_output_slot_id,
            literal_output_slot_ids,
            value_mappings,
            max_output_rows,
            max_output_bytes,
        } = graph_node.kind()
        else {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled local node {at} is not an Unpivot"),
            ));
        };
        let invalid = |what: String| {
            program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled Unpivot at local node {at} {what}"),
            )
        };
        if value_mappings.is_empty() || *max_output_rows == 0 || *max_output_bytes == 0 {
            return Err(invalid(
                "needs a mapping and nonzero output budgets".to_string(),
            ));
        }
        let input_layout = nodes
            .get(input.index())
            .ok_or_else(|| invalid("has no input".to_string()))?
            .output_layout();
        let layout = graph_node.output_layout();
        let input_ordinal = |slot: SlotId| {
            input_layout
                .slots()
                .iter()
                .position(|candidate| *candidate == slot)
                .ok_or_else(|| invalid(format!("reads slot {slot} outside its input")))
        };
        let output_ordinal = |slot: SlotId| {
            layout
                .slots()
                .iter()
                .position(|candidate| *candidate == slot)
                .ok_or_else(|| invalid(format!("writes slot {slot} outside its output")))
        };
        if !input_layout.is_typed() || !layout.is_typed() {
            return Err(invalid(
                "requires complete typed input and output declarations".to_owned(),
            ));
        }
        let binding_matches = |source: usize, target: usize| -> ExecutionResult<bool> {
            let source = input_layout
                .value_binding(input_layout.slots()[source])
                .ok_or_else(|| invalid("has no source value binding".to_owned()))?;
            let target = layout
                .value_binding(layout.slots()[target])
                .ok_or_else(|| invalid("has no target value binding".to_owned()))?;
            Ok((!source.nullable() || target.nullable())
                && source
                    .borrowed()
                    .same_physical_binding_ignoring_root_nullability_observed(
                        target.borrowed(),
                        || Ok::<(), std::convert::Infallible>(()),
                    )
                    .unwrap_or_else(|never| match never {}))
        };
        let output_type = |ordinal: usize| layout.schema().field(ordinal).data_type();
        let mut producers: Vec<Option<Producer>> = vec![None; layout.slots().len()];
        let mut assign = |ordinal: usize, producer: Producer| {
            if producers[ordinal].replace(producer).is_some() {
                return Err(invalid(format!(
                    "output channel {ordinal} has two producers"
                )));
            }
            Ok(())
        };
        for column in passthrough_columns {
            let source = input_ordinal(column.input_slot_id)?;
            let target = output_ordinal(column.output_slot_id)?;
            if !binding_matches(source, target)? {
                return Err(invalid(format!(
                    "passthrough slot {} changes type",
                    column.input_slot_id
                )));
            }
            assign(target, Producer::Passthrough { input: source })?;
        }
        let value = output_ordinal(*value_output_slot_id)?;
        let mut value_inputs = Vec::with_capacity(value_mappings.len());
        for mapping in value_mappings {
            let source = input_ordinal(mapping.input_value_slot_id)?;
            if !binding_matches(source, value)? {
                return Err(invalid(format!(
                    "value slot {} differs from the value channel type",
                    mapping.input_value_slot_id
                )));
            }
            value_inputs.push(source);
        }
        assign(
            value,
            Producer::Value {
                inputs: value_inputs,
            },
        )?;
        let mut sites = Vec::new();
        for (literal, slot) in literal_output_slot_ids.iter().enumerate() {
            let target = output_ordinal(*slot)?;
            let mut sources = Vec::with_capacity(value_mappings.len());
            for (mapping_ordinal, mapping) in value_mappings.iter().enumerate() {
                let constant = mapping.constants.get(literal).ok_or_else(|| {
                    invalid(format!("mapping {mapping_ordinal} lacks literal {literal}"))
                })?;
                let source = match constant {
                    UnpivotConstant::Scalar { .. } => {
                        sites.push(ProgramExpressionRootSite::Node {
                            node,
                            role: ProgramNodeExpressionRole::UnpivotConstant {
                                mapping: u32::try_from(mapping_ordinal)
                                    .map_err(|_| invalid("has too many mappings".to_string()))?,
                                constant: u32::try_from(literal)
                                    .map_err(|_| invalid("has too many literals".to_string()))?,
                            },
                        });
                        let declared =
                            root_value_type(&program, *sites.last().expect("site was added"))?;
                        let target_binding = layout
                            .value_binding(*slot)
                            .ok_or_else(|| invalid("literal binding is absent".to_owned()))?;
                        if (declared.nullable() && !target_binding.nullable())
                            || !declared
                                .typed_binding()
                                .borrowed()
                                .same_physical_binding_ignoring_root_nullability_observed(
                                    target_binding.borrowed(),
                                    || Ok::<(), std::convert::Infallible>(()),
                                )
                                .unwrap_or_else(|never| match never {})
                        {
                            return Err(invalid(
                                "literal changes its complete value declaration".to_owned(),
                            ));
                        }
                        LiteralSource::Root(sites.len() - 1)
                    }
                    UnpivotConstant::Int32List(values) => {
                        if list_item_field(output_type(target)).is_none()
                            || !matches!(
                                layout.value_binding(*slot).map(|binding| binding.value_type().kind()),
                                Some(novarocks_type_contract::ValueKind::List(item)) if matches!(item.kind(), novarocks_type_contract::ValueKind::Scalar(DataType::Int32))
                            )
                        {
                            return Err(invalid(format!(
                                "Int32List constant targets {:?}",
                                output_type(target)
                            )));
                        }
                        LiteralSource::Int32List(Arc::from(values.as_slice()))
                    }
                    UnpivotConstant::Utf8Map(entries) => {
                        if map_entries_field(output_type(target)).is_none()
                            || !matches!(
                                layout.value_binding(*slot).map(|binding| binding.value_type().kind()),
                                Some(novarocks_type_contract::ValueKind::Map { key, value }) if matches!(key.kind(), novarocks_type_contract::ValueKind::String) && matches!(value.kind(), novarocks_type_contract::ValueKind::String)
                            )
                        {
                            return Err(invalid(format!(
                                "Utf8Map constant targets {:?}",
                                output_type(target)
                            )));
                        }
                        LiteralSource::Utf8Map(Arc::from(entries.as_slice()))
                    }
                };
                sources.push(source);
            }
            assign(target, Producer::Literal { sources })?;
        }
        if value_mappings
            .iter()
            .any(|mapping| mapping.constants.len() != literal_output_slot_ids.len())
        {
            return Err(invalid(
                "has a mapping of another literal width".to_string(),
            ));
        }
        let producers = producers
            .into_iter()
            .enumerate()
            .map(|(ordinal, producer)| {
                producer.ok_or_else(|| invalid(format!("output channel {ordinal} has no producer")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let output_layout = layout.clone();
        let input_layout = input_layout.clone();
        let (max_output_rows, max_output_bytes) = (*max_output_rows, *max_output_bytes);
        let mappings = value_mappings.len();
        Ok(Self {
            name: format!("COMPILED_UNPIVOT (node={at})"),
            program,
            sites,
            producers: Arc::from(producers),
            mappings,
            max_output_rows,
            max_output_bytes,
            output_layout,
            input_layout,
            error,
        })
    }
}

impl OperatorFactory for CompiledUnpivotProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(CompiledUnpivotProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            sites: self.sites.clone(),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
            expansion: None,
            output_layout: self.output_layout.clone(),
            input_layout: self.input_layout.clone(),
            producers: self.producers.clone(),
            mappings: self.mappings,
            max_output_rows: self.max_output_rows,
            max_output_bytes: self.max_output_bytes,
            construction: None,
            instance_storage: None,
            mem_tracker: None,
        }))
    }
}

/// One input batch and its evaluated scalar constant columns.
struct Expanding {
    batch: ChunkBatchProjection,
    constants: Vec<ArrayRef>,
    storage: Option<ValueTypeStorageLifetime>,
}

/// The reusable Unpivot expansion: each input row becomes one output row per
/// mapping, mapping-major, every output batch within the frozen row and byte
/// budgets. It owns at most one input batch at a time.
pub(crate) struct UnpivotExpansion {
    producers: Arc<[Producer]>,
    mappings: usize,
    max_output_rows: usize,
    max_output_bytes: usize,
    output: ChunkSchemaRef,
    input: Option<Expanding>,
    cursor: usize,
    hint: Option<usize>,
}

impl UnpivotExpansion {
    pub(crate) fn new(
        producers: Arc<[Producer]>,
        output: ChunkSchemaRef,
        mappings: usize,
        max_output_rows: usize,
        max_output_bytes: usize,
    ) -> Self {
        Self {
            producers,
            mappings,
            max_output_rows,
            max_output_bytes,
            output,
            input: None,
            cursor: 0,
            hint: None,
        }
    }

    /// Start expanding one input batch with its evaluated scalar constant
    /// columns, one per `LiteralSource::Root` index.
    pub(crate) fn start(
        &mut self,
        batch: ChunkBatchProjection,
        constants: Vec<ArrayRef>,
    ) -> ExecutionResult<()> {
        if self.input.is_some() {
            return Err(unpivot_contract(
                "compiled Unpivot received input while expanding",
            ));
        }
        // A scalar constant must already carry its literal channel's type.
        for producer in self.producers.iter().enumerate() {
            let (ordinal, Producer::Literal { sources }) = producer else {
                continue;
            };
            let expected = self.output.arrow_schema_ref();
            let expected = expected.field(ordinal).data_type();
            for source in sources {
                if let LiteralSource::Root(root) = source {
                    let constant = constants
                        .get(*root)
                        .ok_or_else(|| unpivot_contract("compiled Unpivot constant is absent"))?;
                    if constant.data_type() != expected {
                        return Err(unpivot_contract(format_args!(
                            "compiled Unpivot constant has type {:?}, not {expected:?}",
                            constant.data_type()
                        )));
                    }
                }
            }
        }
        self.input = Some(Expanding {
            batch,
            constants,
            storage: None,
        });
        self.cursor = 0;
        Ok(())
    }

    /// A complete typed caller supplies both original backing responsibility
    /// and the actual owner of its freshly sliced constant/reference scaffolds.
    /// This shares the existing start/type gate; it creates no second decoder.
    pub(crate) fn start_typed(
        &mut self,
        batch: ChunkBatchProjection,
        constants: Vec<ArrayRef>,
        storage: ValueTypeStorageLifetime,
    ) -> ExecutionResult<()> {
        if !self.output.is_typed() {
            return Err(ConstructionFailure::MissingOwner.into());
        }
        self.start(batch, constants)?;
        if let Some(input) = &mut self.input {
            input.storage = Some(storage);
        }
        Ok(())
    }

    pub(crate) fn step_typed(
        &mut self,
        tracker: Option<&Arc<MemTracker>>,
        factory: &dyn InputConstructionFactory,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<StepResult> {
        let Some(input) = self.input.as_ref() else {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        };
        let total = input
            .batch
            .len()
            .checked_mul(self.mappings)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let (producers, output, cursor) = (&self.producers, &self.output, self.cursor);
        let (chunk, rows) = next_bounded_output_with_construction(
            total - cursor,
            self.max_output_rows,
            self.max_output_bytes,
            &mut self.hint,
            tracker,
            factory,
            control,
            |len| build_output_typed(producers, output, input, cursor, len, factory, control),
        )?;
        self.cursor += rows;
        let next = if self.cursor == total {
            self.input = None;
            self.cursor = 0;
            NextStep::NeedInput
        } else {
            NextStep::Runnable
        };
        Ok(StepResult::new(Some(chunk), next, 0))
    }
}

struct CompiledUnpivotProcessor {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    expansion: Option<UnpivotExpansion>,
    output_layout: StaticLayout,
    input_layout: StaticLayout,
    producers: Arc<[Producer]>,
    mappings: usize,
    max_output_rows: usize,
    max_output_bytes: usize,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    instance_storage: Option<ValueTypeStorageLifetime>,
    mem_tracker: Option<Arc<MemTracker>>,
}

#[derive(Clone, Copy, Debug)]
enum LiteralOffsetLayout {
    List,
    Map,
    Utf8,
}
#[derive(Debug)]
struct LiteralOffsetExtent {
    layout: LiteralOffsetLayout,
    one_value: u128,
    required: u128,
}
impl std::fmt::Display for LiteralOffsetExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "compiled Unpivot {:?} offset extent {} exceeds {}",
            self.layout,
            self.required,
            i32::MAX
        )
    }
}
impl std::error::Error for LiteralOffsetExtent {}
impl novarocks_failure_contract::FailureProjection for LiteralOffsetExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let code = match self.layout {
            LiteralOffsetLayout::List => 11,
            LiteralOffsetLayout::Map => 12,
            LiteralOffsetLayout::Utf8 => 13,
        };
        FailureFact::new(
            FailureClass::Representation,
            crate::runtime::fragment::ExecutionReason::UnpivotLiteralOffset.reason(),
        )
        .with_representation(RepresentationFacts {
            boundary: if self.one_value > i32::MAX as u128 {
                RepresentationBoundary::MinimumUnit
            } else {
                RepresentationBoundary::SingleOutput
            },
            layout: RepresentationLayout {
                domain: domain::EXECUTION,
                code: std::num::NonZeroU16::new(code).unwrap(),
            },
            required: u64::try_from(if self.one_value > i32::MAX as u128 {
                self.one_value
            } else {
                self.required
            })
            .ok(),
            extent_limit: Some(i32::MAX as u64),
            representable_prefix: None,
        })
    }
}
fn literal_offset_extent(
    layout: LiteralOffsetLayout,
    one_value: u128,
    copies: usize,
) -> ExecutionResult<usize> {
    let offset_count = copies.checked_add(1).ok_or_else(|| {
        ExecutionFailure::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?;
    Layout::array::<i32>(offset_count).map_err(|_| {
        ExecutionFailure::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?;
    let required = one_value.checked_mul(copies as u128).ok_or_else(|| {
        ExecutionFailure::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?;
    if required > i32::MAX as u128 {
        return Err(ExecutionFailure::from_domain(LiteralOffsetExtent {
            layout,
            one_value,
            required,
        }));
    }
    Ok(required as usize)
}

fn int32_list(item: &FieldRef, values: &[i32], copies: usize) -> ExecutionResult<ArrayRef> {
    let total = literal_offset_extent(LiteralOffsetLayout::List, values.len() as u128, copies)?;
    let mut flat = Vec::with_capacity(total);
    for _ in 0..copies {
        flat.extend_from_slice(values);
    }
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(values.len(), copies));
    ListArray::try_new(
        Arc::clone(item),
        offsets,
        Arc::new(Int32Array::from(flat)),
        None,
    )
    .map(|array| Arc::new(array) as ArrayRef)
    .map_err(unpivot_arrow)
}

fn utf8_map<T: AsRef<str>>(
    entries_field: &FieldRef,
    sorted: bool,
    entries: &[(T, T)],
    copies: usize,
) -> ExecutionResult<ArrayRef> {
    let DataType::Struct(fields) = entries_field.data_type() else {
        return Err(unpivot_contract(
            "compiled Unpivot Utf8Map entries are not a struct",
        ));
    };
    let total = literal_offset_extent(LiteralOffsetLayout::Map, entries.len() as u128, copies)?;
    let key_bytes = entries
        .iter()
        .map(|(key, _)| key.as_ref().len() as u128)
        .sum::<u128>();
    let value_bytes = entries
        .iter()
        .map(|(_, value)| value.as_ref().len() as u128)
        .sum::<u128>();
    literal_offset_extent(LiteralOffsetLayout::Utf8, key_bytes, copies)?;
    literal_offset_extent(LiteralOffsetLayout::Utf8, value_bytes, copies)?;
    let mut keys = Vec::with_capacity(total);
    let mut values = Vec::with_capacity(total);
    for _ in 0..copies {
        for (key, value) in entries {
            keys.push(key.as_ref());
            values.push(value.as_ref());
        }
    }
    let (keys, values) = (StringArray::from(keys), StringArray::from(values));
    let entries_array = StructArray::try_new(
        fields.clone(),
        vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
        None,
    )
    .map_err(unpivot_arrow)?;
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(entries.len(), copies));
    MapArray::try_new(
        Arc::clone(entries_field),
        offsets,
        entries_array,
        None,
        sorted,
    )
    .map(|array| Arc::new(array) as ArrayRef)
    .map_err(unpivot_arrow)
}

/// Borrow the existing frozen protocol; strings are never recopied to share
/// the constructor with the native generic Unpivot source.
pub(crate) enum FixedLiteralView<'a, T: AsRef<str>> {
    Int32List(&'a [i32]),
    Utf8Map(&'a [(T, T)]),
}
pub(crate) fn materialize_fixed_literal<T: AsRef<str>>(
    source: FixedLiteralView<'_, T>,
    field: &arrow::datatypes::Field,
    rows: usize,
) -> ExecutionResult<ArrayRef> {
    match source {
        FixedLiteralView::Int32List(values) => int32_list(
            list_item_field(field.data_type())
                .ok_or_else(|| unpivot_contract("compiled Unpivot list channel changed type"))?,
            values,
            rows,
        ),
        FixedLiteralView::Utf8Map(entries) => {
            let (entries_field, sorted) = map_entries_field(field.data_type())
                .ok_or_else(|| unpivot_contract("compiled Unpivot map channel changed type"))?;
            utf8_map(entries_field, sorted, entries, rows)
        }
    }
}
fn materialize_literal(
    source: &LiteralSource,
    field: &arrow::datatypes::Field,
    rows: usize,
) -> ExecutionResult<ArrayRef> {
    match source {
        LiteralSource::Int32List(values) => {
            materialize_fixed_literal(FixedLiteralView::<Arc<str>>::Int32List(values), field, rows)
        }
        LiteralSource::Utf8Map(entries) => {
            materialize_fixed_literal(FixedLiteralView::Utf8Map(entries), field, rows)
        }
        LiteralSource::Root(_) => Err(unpivot_contract(
            "compiled Unpivot scalar requires its evaluated source",
        )),
    }
}

/// One expansion/materialization body shared with the explicit legacy grouped
/// adapter. The runtime route supplies its original creator before each step.
#[derive(Clone, Copy)]
enum ColumnVectorRole {
    Temporary,
    Result,
}
fn build_columns<E>(
    producers: &[Producer],
    schema: &arrow::datatypes::Schema,
    input: &Expanding,
    segments: &[super::unpivot_processor::Segment],
    mut vector: impl FnMut(ColumnVectorRole, usize) -> Result<Vec<ArrayRef>, E>,
    mut slice: impl FnMut(&ArrayRef, usize, usize) -> Result<ArrayRef, E>,
    mut literal: impl FnMut(&LiteralSource, &arrow::datatypes::Field, usize) -> Result<ArrayRef, E>,
    mut concatenate: impl FnMut(Vec<ArrayRef>, usize) -> Result<ArrayRef, E>,
) -> Result<Vec<ArrayRef>, E> {
    let mut columns = vector(ColumnVectorRole::Result, producers.len())?;
    for (ordinal, producer) in producers.iter().enumerate() {
        let field = schema.field(ordinal);
        let mut parts = vector(ColumnVectorRole::Temporary, segments.len())?;
        for segment in segments {
            let (offset, rows) = (segment.input_offset, segment.len);
            parts.push(match producer {
                Producer::Passthrough { input: source } => {
                    slice(input.batch.record_batch().column(*source), offset, rows)?
                }
                Producer::Value { inputs } => slice(
                    input
                        .batch
                        .record_batch()
                        .column(inputs[segment.mapping_index]),
                    offset,
                    rows,
                )?,
                Producer::Literal { sources } => match &sources[segment.mapping_index] {
                    LiteralSource::Root(root) => slice(&input.constants[*root], offset, rows)?,
                    source => literal(source, field, rows)?,
                },
            });
        }
        columns.push(concatenate(parts, ordinal)?);
    }
    Ok(columns)
}

fn request_arc<T>(owner: &dyn InputConstructionOwner) -> ExecutionResult<()> {
    owner.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<T>())
            .map_err(ConstructionFailure::Resource)?,
    )?;
    Ok(())
}

fn primitive_adoption(
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<()> {
    control.checkpoint(0)?;
    let mut request = |layout| owner.request(layout);
    let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
    let mut creation =
        ValueSchemaCreation::new(owner.storage_lifetime(), &mut request, &mut opaque);
    novarocks_functions::selected_copy::preflight_primitive_vec_adoption_construction_observed(
        &mut creation,
        &mut |boundary| {
            owner.observe(
                0,
                &[],
                ArrayNormalizationStep::InspectConstructionWork {
                    units: usize::from(!boundary),
                },
            )
        },
    )
    .map_err(unpivot_copy_failure)
}

/// The fixed literal protocol remains Int32List/Utf8Map. This observes the
/// actual Arrow 60.1.0 constructors, without deriving a value domain.
fn observe_literal(
    source: &LiteralSource,
    rows: usize,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<()> {
    match source {
        LiteralSource::Int32List(values) => observe_fixed_literal(
            FixedLiteralView::<Arc<str>>::Int32List(values),
            rows,
            owner,
            control,
        ),
        LiteralSource::Utf8Map(entries) => {
            observe_fixed_literal(FixedLiteralView::Utf8Map(entries), rows, owner, control)
        }
        LiteralSource::Root(_) => Err(ConstructionFailure::InvalidSource(
            "evaluated scalar is not a fixed literal constructor".to_owned(),
        )
        .into()),
    }
}
pub(crate) fn observe_fixed_literal<T: AsRef<str>>(
    source: FixedLiteralView<'_, T>,
    rows: usize,
    owner: &dyn InputConstructionOwner,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<()> {
    let extent = |n: usize| n.checked_mul(rows).ok_or(KernelFailure::ResourceExhausted);
    let offsets = rows
        .checked_add(1)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<i32>(offsets).map_err(|_| KernelFailure::ResourceExhausted)?;
    primitive_adoption(owner, control)?;
    match source {
        FixedLiteralView::Int32List(values) => {
            i32::try_from(extent(values.len())?).map_err(|_| KernelFailure::ResourceExhausted)?;
            Layout::array::<i32>(extent(values.len())?)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            primitive_adoption(owner, control)?;
            request_arc::<Int32Array>(owner)?;
            request_arc::<ListArray>(owner)?;
        }
        FixedLiteralView::Utf8Map(entries) => {
            let count = extent(entries.len())?;
            i32::try_from(count).map_err(|_| KernelFailure::ResourceExhausted)?;
            owner.request(
                Layout::array::<&str>(count).map_err(|_| KernelFailure::ResourceExhausted)?,
            )?;
            owner.request(
                Layout::array::<&str>(count).map_err(|_| KernelFailure::ResourceExhausted)?,
            )?;
            // byte_array::from_iter_values uses rounded MutableBuffer offsets
            // and doubling byte growth. These are opaque operation bounds,
            // separate from the two actual source-reference Vec requests.
            for key in [true, false] {
                let mut bytes = 0usize;
                for (k, v) in entries.iter() {
                    control.checkpoint(1)?;
                    bytes = bytes
                        .checked_add(if key {
                            k.as_ref().len()
                        } else {
                            v.as_ref().len()
                        })
                        .ok_or(KernelFailure::ResourceExhausted)?;
                }
                bytes = extent(bytes)?;
                i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
                let offset_bytes = count
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(std::mem::size_of::<i32>()))
                    .and_then(|n| n.checked_add(63))
                    .map(|n| n & !63)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                let mut growth =
                    novarocks_functions::arrow_ipc_construction::SchemaRequestAccumulator::default(
                    );
                growth.geometric::<u8, novarocks_functions::arrow_ipc_construction::IpcConstructionError<ConstructionFailure>>(bytes, 64, 1,
                    &mut || control.checkpoint(0).map_err(novarocks_functions::arrow_ipc_construction::IpcConstructionError::Control))
                    .map_err(|error| match error {
                        novarocks_functions::arrow_ipc_construction::IpcConstructionError::Control(cause) => ExecutionFailure::from(cause),
                        novarocks_functions::arrow_ipc_construction::IpcConstructionError::Creator(cause) => ExecutionFailure::from(cause),
                        novarocks_functions::arrow_ipc_construction::IpcConstructionError::Geometry(novarocks_functions::arrow_ipc_construction::IpcGeometryError::Overflow) => ExecutionFailure::from(KernelFailure::ResourceExhausted),
                        other => ExecutionFailure::from(ConstructionFailure::InvalidSource(other.to_string())),
                    })?;
                offset_bytes
                    .checked_add(growth.bytes)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                primitive_adoption(owner, control)?;
                primitive_adoption(owner, control)?;
                request_arc::<StringArray>(owner)?;
            }
            owner.request(
                Layout::array::<ArrayRef>(2).map_err(|_| KernelFailure::ResourceExhausted)?,
            )?;
            request_arc::<MapArray>(owner)?;
        }
    }
    control.checkpoint(0)?;
    Ok(())
}

fn build_output_typed(
    producers: &[Producer],
    output: &ChunkSchemaRef,
    input: &Expanding,
    start: usize,
    len: usize,
    factory: &dyn InputConstructionFactory,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<Chunk> {
    let owner = factory.begin_typed_construction(output)?;
    let temporary = factory.begin_typed_construction(output)?;
    let input_storage = input
        .storage
        .as_ref()
        .ok_or(ConstructionFailure::MissingOwner)?;
    let count = if len == 0 {
        0
    } else {
        (start % input.batch.len())
            .checked_add(len - 1)
            .ok_or(KernelFailure::ResourceExhausted)?
            / input.batch.len()
            + 1
    };
    let mut segments = unpivot_vec::<super::unpivot_processor::Segment>(count, temporary.as_ref())?;
    super::unpivot_processor::visit_flattened_segments(start, len, input.batch.len(), |segment| {
        segments.push(segment)
    });
    let schema = output.arrow_schema_ref();
    let columns = build_columns(
        producers,
        &schema,
        input,
        &segments,
        |role, count| match role {
            ColumnVectorRole::Result => unpivot_vec(count, owner.as_ref()),
            ColumnVectorRole::Temporary => unpivot_vec(count, temporary.as_ref()),
        },
        |source, offset, length| {
            temporary.observe(
                0,
                &[],
                ArrayNormalizationStep::SliceArray {
                    source,
                    offset,
                    length,
                },
            )?;
            let result = source.slice(offset, length);
            temporary.observe(0, &[], ArrayNormalizationStep::OpaqueOperationFinished)?;
            Ok(result)
        },
        |literal, field, rows| {
            observe_literal(literal, rows, temporary.as_ref(), control)?;
            let result = materialize_literal(literal, field, rows)?;
            control.checkpoint(0)?;
            Ok(result)
        },
        |parts, ordinal| {
            concat_owned_with_construction(
                parts,
                output.slot_ids()[ordinal],
                temporary.as_ref(),
                owner.as_ref(),
                control,
            )
        },
    )?;
    let storage = if len == 0 {
        owner.storage_lifetime()
    } else {
        owner
            .storage_lifetime()
            .retaining_observed(input_storage, |layout| owner.request(layout))?
    };
    Ok(Chunk::try_new_typed_with_columns_owned_observed::<
        ConstructionFailure,
        _,
    >(output.clone(), columns, len, storage, |step| {
        owner.observe(0, &[], step)
    })?)
}

impl OperatorLifecycle for CompiledUnpivotProcessor {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.control.bind_construction_factory(factory.clone());
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if self.expansion.is_some() {
            return Ok(());
        }
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let output = factory.create_compiled_chunk_schema(&self.output_layout)?;
        self.expansion = Some(UnpivotExpansion::new(
            self.producers.clone(),
            output,
            self.mappings,
            self.max_output_rows,
            self.max_output_bytes,
        ));
        Ok(())
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
        self.mem_tracker = Some(tracker);
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        if let Some(expansion) = &mut self.expansion {
            expansion.input = None;
        }
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        if let Some(expansion) = &mut self.expansion {
            expansion.input = None;
        }
        Ok(())
    }
    fn cancel(&mut self) {
        if let Some(expansion) = &mut self.expansion {
            expansion.input = None;
        }
    }
}
impl InputEncodingPolicy for CompiledUnpivotProcessor {}
impl TransformKernel for CompiledUnpivotProcessor {
    fn apply(
        &mut self,
        _ctx: &mut StepContext<'_>,
        chunk: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = chunk.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        if chunk.is_empty() {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        }
        self.expansion
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        if chunk.chunk_schema().slot_ids() != self.input_layout.slots() {
            return Err(ConstructionFailure::InvalidSource(
                "compiled Unpivot input occurrences differ from its declaration".to_owned(),
            )
            .into());
        }
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_input_construction(&chunk)?;
        for (ordinal, slot) in chunk.chunk_schema().slots().iter().enumerate() {
            let declared = self
                .input_layout
                .value_binding(self.input_layout.slots()[ordinal])
                .ok_or_else(|| {
                    ConstructionFailure::InvalidSource(
                        "compiled Unpivot input binding is absent".to_owned(),
                    )
                })?;
            if slot.name() != self.input_layout.schema().field(ordinal).name()
                || !slot.typed_binding().borrowed().exactly_equals_observed(
                    declared.borrowed(),
                    || {
                        owner.observe(
                            ordinal,
                            &[],
                            ArrayNormalizationStep::InspectConstructionWork { units: 1 },
                        )
                    },
                )?
            {
                return Err(ConstructionFailure::InvalidSource(
                    "compiled Unpivot input changes its complete value binding".to_owned(),
                )
                .into());
            }
        }
        if self.instances.is_none() {
            if !self.sites.is_empty() {
                owner.request(
                    Layout::array::<CompiledExpressionInstance>(self.sites.len())
                        .map_err(|_| KernelFailure::ResourceExhausted)?,
                )?;
            }
            self.instance_storage = Some(owner.storage_lifetime());
        }
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let instances = self.instances.as_mut().expect("instances were created");
        let mut constants = unpivot_vec::<ArrayRef>(self.sites.len(), owner.as_ref())?;
        for (instance, site) in instances.iter_mut().zip(&self.sites) {
            constants.push(evaluate_all(
                instance,
                *site,
                chunk.record_batch(),
                &self.control,
            )?);
        }
        let storage = chunk.source_storage_lifetime_observed::<ConstructionFailure>(
            owner.storage_lifetime(),
            |layout| owner.request(layout),
        )?;
        let expansion = self
            .expansion
            .as_mut()
            .ok_or(ConstructionFailure::MissingOwner)?;
        expansion.input = Some(Expanding {
            batch: chunk.into_batch_projection(),
            constants,
            storage: Some(storage),
        });
        expansion.cursor = 0;
        Ok(StepResult::new(None, NextStep::Runnable, rows))
    }
    fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        self.expansion
            .as_mut()
            .ok_or(ConstructionFailure::MissingOwner)?
            .step_typed(self.mem_tracker.as_ref(), factory.as_ref(), &self.control)
    }
}

#[cfg(test)]
#[path = "compiled_unpivot/creation_tests.rs"]
mod creation_tests;
