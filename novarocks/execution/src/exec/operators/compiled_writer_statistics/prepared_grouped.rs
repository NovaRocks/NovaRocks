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

//! The default host's independent grouped constants and mapping scaffold.
//! All value/shape facts are borrowed from the original writer declarations;
//! the actual expansion, byte search, copy and Root publishing bodies are shared.

use super::*;
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::table_write_aggregate::WriterGroupedUnpivotPlan as NativeGroupedPlan;
use crate::exec::node::unpivot::UnpivotConstant as NativeConstant;
use novarocks_type_contract::FunctionValueType;

fn root_slot(
    original: &ChunkSlotSchema,
    nullable: bool,
    owner: &dyn InputConstructionOwner,
) -> ExecutionResult<ChunkSlotSchema> {
    if original.nullable() == nullable {
        return Ok(original.clone());
    }
    let storage = owner
        .storage_lifetime()
        .retaining_observed(&original.storage_lifetime(), |layout| owner.request(layout))?;
    let binding = original.typed_binding().with_nullable_observed(
        nullable,
        storage.clone(),
        |layout| owner.request(layout),
        |units| statistics_work(owner, units),
    )?;
    let field = original.field_ref();
    owner.observe(
        0,
        &[],
        ArrayNormalizationStep::RebuildField {
            source: field.as_ref(),
            target: binding.data_type(),
            name: field.name(),
        },
    )?;
    let field = Arc::new(Field::new(
        field.name(),
        binding.data_type().clone(),
        nullable,
    ));
    Ok(ChunkSlotSchema::try_new_typed_observed::<
        ConstructionFailure,
        _,
    >(
        original.slot_id(),
        field,
        binding,
        original.unique_id(),
        storage,
        |step| owner.observe(0, &[], step),
    )?)
}
fn arc_slice<T>(values: Vec<T>, owner: &dyn InputConstructionOwner) -> ExecutionResult<Arc<[T]>> {
    if values.capacity() != values.len() {
        owner.request(
            Layout::array::<T>(values.len()).map_err(|_| KernelFailure::ResourceExhausted)?,
        )?;
    }
    owner.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(
            Layout::array::<T>(values.len()).map_err(|_| KernelFailure::ResourceExhausted)?,
        )
        .map_err(ConstructionFailure::Resource)?,
    )?;
    Ok(Arc::from(values))
}
fn text(value: &str, owner: &dyn InputConstructionOwner) -> ExecutionResult<Arc<str>> {
    statistics_work(owner, value.len())?;
    owner.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(
            Layout::array::<u8>(value.len()).map_err(|_| KernelFailure::ResourceExhausted)?,
        )
        .map_err(ConstructionFailure::Resource)?,
    )?;
    Ok(Arc::from(value))
}

pub(crate) fn create_prepared_grouped_in(
    arena: &ExprArena,
    plan: &NativeGroupedPlan,
    mut final_chunk: Chunk,
    root_schema: ChunkSchemaRef,
    tracker: Option<Arc<MemTracker>>,
    factory: Arc<dyn InputConstructionFactory>,
    error: Arc<RuntimeErrorState>,
) -> ExecutionResult<Box<dyn GroupedUnpivotSource>> {
    if !root_schema.is_typed() || !final_chunk.chunk_schema().is_typed() {
        return Err(ConstructionFailure::MissingOwner.into());
    }
    if plan.literal_output_slot_ids.len() != 3 {
        return Err(ConstructionFailure::InvalidSource(
            "writer grouped literal occurrence arity changed".into(),
        )
        .into());
    }
    if let Some(tracker) = &tracker {
        final_chunk
            .try_transfer_to_typed(tracker)
            .map_err(ConstructionFailure::Memory)?;
    }
    let owner = factory.begin_typed_construction(&root_schema)?;
    let temporary = factory.begin_typed_construction(&root_schema)?;
    let mut control = RuntimeKernelControl::new(error);
    control.bind_construction_factory(factory.clone());
    let grouping_values = final_chunk.column_by_slot_id(plan.grouping_output_slot_id)?;
    let groups = grouping_values
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| {
            ConstructionFailure::InvalidSource("writer final grouping carrier is not Int32".into())
        })?;
    let mut rows = super::super::unpivot_processor::unpivot_vec::<(u32, usize)>(
        groups.len(),
        temporary.as_ref(),
    )?;
    let shape = novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<u32, ()>(
        groups.len(),
    )
    .map_err(grouped_hashmap_error)?;
    temporary.observe_opaque_upper_bound(shape.request_bytes_upper_bound)?;
    crate::exec::expr::ExprEvaluationControl::checkpoint(&control)?;
    let mut seen = std::collections::HashSet::with_capacity(groups.len());
    for row in 0..groups.len() {
        crate::exec::expr::ExprEvaluationControl::checkpoint(&control)?;
        if groups.is_null(row) {
            return Err("table finish final aggregate produced a null grouping key".into());
        }
        let target = target_ordinal_from_wire(groups.value(row))
            .map_err(|cause| format!("table finish final aggregate grouping key: {cause}"))?
            .get();
        if !seen.insert(target) {
            return Err(
                format!("table finish final aggregate produced duplicate target {target}").into(),
            );
        }
        if !plan
            .mappings
            .iter()
            .any(|mapping| mapping.grouping_key == target)
        {
            return Err(format!("table finish final aggregate produced target {target} with no grouped Unpivot mapping").into());
        }
        rows.push((target, row));
    }
    crate::exec::expr::ExprEvaluationControl::checkpoint(&control)?;
    let scalar_count = plan
        .mappings
        .iter()
        .try_fold(0usize, |sum, mapping| {
            sum.checked_add(
                mapping
                    .constants
                    .iter()
                    .filter(|constant| matches!(constant, NativeConstant::Scalar { .. }))
                    .count(),
            )
        })
        .ok_or(KernelFailure::ResourceExhausted)?;
    let mut constants =
        super::super::unpivot_processor::unpivot_vec::<ArrayRef>(scalar_count, owner.as_ref())?;
    let mut source_storage = owner.storage_lifetime();
    // Literal evaluation uses the original arena algorithm. The typed row is
    // a wrapper over the existing final batch, never an invented programme.
    let row = if scalar_count == 0 {
        None
    } else {
        let scope = factory.begin_input_construction(&final_chunk)?;
        Some(final_chunk.slice_typed_observed::<ConstructionFailure, _>(
            0,
            1,
            scope.storage_lifetime(),
            |step| scope.observe(0, &[], step),
        )?)
    };
    let mut targets =
        super::super::unpivot_processor::unpivot_vec::<GroupedTarget>(rows.len(), owner.as_ref())?;
    let grouping_ordinal = final_chunk
        .chunk_schema()
        .index_of(plan.grouping_output_slot_id)
        .ok_or_else(|| {
            ConstructionFailure::InvalidSource("writer grouping output occurrence is absent".into())
        })?;
    for (target, row_index) in rows {
        let count = plan
            .mappings
            .iter()
            .filter(|mapping| mapping.grouping_key == target)
            .count();
        let output_order = [
            plan.passthrough_output_slot_id,
            plan.literal_output_slot_ids[0],
            plan.literal_output_slot_ids[1],
            plan.value_output_slot_id,
            plan.literal_output_slot_ids[2],
        ];
        let mut slots = super::super::unpivot_processor::unpivot_vec::<ChunkSlotSchema>(
            output_order.len(),
            owner.as_ref(),
        )?;
        let mut producers = super::super::unpivot_processor::unpivot_vec::<Producer>(
            output_order.len(),
            owner.as_ref(),
        )?;
        for output_slot in output_order {
            let original = root_schema.slot(output_slot).ok_or_else(|| {
                ConstructionFailure::InvalidSource("writer Root output occurrence is absent".into())
            })?;
            let mut nullable = false;
            let producer = if output_slot == plan.passthrough_output_slot_id {
                Producer::Passthrough {
                    input: grouping_ordinal,
                }
            } else if output_slot == plan.value_output_slot_id {
                let expected =
                    FunctionValueType::try_from_typed_binding(original.typed_binding().clone())
                        .map_err(|_| {
                            ConstructionFailure::InvalidSource(
                                "writer Root value declaration is invalid".into(),
                            )
                        })?;
                let mut inputs =
                    super::super::unpivot_processor::unpivot_vec::<usize>(count, owner.as_ref())?;
                for mapping in plan
                    .mappings
                    .iter()
                    .filter(|mapping| mapping.grouping_key == target)
                {
                    statistics_work(owner.as_ref(), 1)?;
                    let source = final_chunk
                        .chunk_schema()
                        .slot(mapping.input_value_slot_id)
                        .ok_or_else(|| {
                            ConstructionFailure::InvalidSource(
                                "writer final value occurrence is absent".into(),
                            )
                        })?;
                    let actual =
                        FunctionValueType::try_from_typed_binding(source.typed_binding().clone())
                            .map_err(|_| {
                            ConstructionFailure::InvalidSource(
                                "writer final value declaration is invalid".into(),
                            )
                        })?;
                    if !writer_binding_matches(&expected, &actual, owner.as_ref())? {
                        return Err(ConstructionFailure::InvalidSource(
                            "writer final value differs from its full Root declaration".into(),
                        )
                        .into());
                    }
                    nullable |= source.nullable();
                    inputs.push(
                        final_chunk
                            .chunk_schema()
                            .index_of(mapping.input_value_slot_id)
                            .expect("checked source occurrence"),
                    );
                }
                Producer::Value { inputs }
            } else {
                let literal = plan
                    .literal_output_slot_ids
                    .iter()
                    .position(|slot| *slot == output_slot)
                    .expect("explicit literal role");
                let expected =
                    FunctionValueType::try_from_typed_binding(original.typed_binding().clone())
                        .map_err(|_| {
                            ConstructionFailure::InvalidSource(
                                "writer literal declaration is invalid".into(),
                            )
                        })?;
                let mut sources = super::super::unpivot_processor::unpivot_vec::<LiteralSource>(
                    count,
                    owner.as_ref(),
                )?;
                for mapping in plan
                    .mappings
                    .iter()
                    .filter(|mapping| mapping.grouping_key == target)
                {
                    statistics_work(owner.as_ref(), 1)?;
                    let constant = mapping.constants.get(literal).ok_or_else(|| {
                        ConstructionFailure::InvalidSource(
                            "writer literal occurrence is absent".into(),
                        )
                    })?;
                    let source = match constant {
                        NativeConstant::Scalar {
                            expr_id,
                            nullable: promise,
                        } => {
                            let actual = arena.value_type(*expr_id).ok_or_else(|| {
                                ConstructionFailure::InvalidSource(
                                    "writer literal complete declaration is absent".into(),
                                )
                            })?;
                            if actual.nullable() != *promise
                                || !writer_binding_matches(&expected, actual, owner.as_ref())?
                            {
                                return Err(ConstructionFailure::InvalidSource(
                                    "writer literal differs from its complete Root declaration"
                                        .into(),
                                )
                                .into());
                            }
                            nullable |= *promise;
                            if !matches!(
                                arena.node(*expr_id),
                                Some(ExprNode::Literal(_) | ExprNode::Constant(_))
                            ) {
                                return Err(ConstructionFailure::InvalidSource(
                                    "writer scalar source is not its original literal".into(),
                                )
                                .into());
                            }
                            crate::exec::expr::ExprEvaluationControl::checkpoint(&control)?;
                            let evaluation = crate::exec::expr::ExprEvaluationContext::new(
                                arena,
                                owner.as_ref(),
                                &control,
                            );
                            let array = evaluation
                                .eval(*expr_id, row.as_ref().expect("actual literal row"))?;
                            crate::exec::expr::ExprEvaluationControl::checkpoint(&control)?;
                            let array = crate::exec::chunk::normalize_execution_array_observed::<
                                ConstructionFailure,
                                _,
                            >(&array, |step| {
                                owner.observe(0, &[], step)
                            })?;
                            if let Some(ExprNode::Constant(value)) = arena.node(*expr_id) {
                                source_storage = source_storage.retaining_observed(
                                    &value.pool().storage_lifetime(),
                                    |layout| owner.request(layout),
                                )?;
                            }
                            let index = constants.len();
                            constants.push(array);
                            LiteralSource::Root(index)
                        }
                        NativeConstant::Int32List(values) => {
                            if !matches!(expected.value_type().kind(), novarocks_type_contract::ValueKind::List(item) if item.kind() == &novarocks_type_contract::ValueKind::Scalar(DataType::Int32) && !item.nullable())
                            {
                                return Err(ConstructionFailure::InvalidSource(
                                    "writer Int32List differs from its declared Root domain".into(),
                                )
                                .into());
                            }
                            let mut copied = super::super::unpivot_processor::unpivot_vec::<i32>(
                                values.len(),
                                owner.as_ref(),
                            )?;
                            copied.extend_from_slice(values);
                            statistics_work(owner.as_ref(), values.len())?;
                            LiteralSource::Int32List(arc_slice(copied, owner.as_ref())?)
                        }
                        NativeConstant::Utf8Map(entries) => {
                            if !matches!(expected.value_type().kind(), novarocks_type_contract::ValueKind::Map {key,value} if key.kind() == &novarocks_type_contract::ValueKind::String && value.kind() == &novarocks_type_contract::ValueKind::String && !key.nullable() && !value.nullable())
                            {
                                return Err(ConstructionFailure::InvalidSource(
                                    "writer Utf8Map differs from its declared Root domains".into(),
                                )
                                .into());
                            }
                            let mut copied = super::super::unpivot_processor::unpivot_vec::<(
                                Arc<str>,
                                Arc<str>,
                            )>(
                                entries.len(), owner.as_ref()
                            )?;
                            for (key, value) in entries {
                                copied.push((
                                    text(key, owner.as_ref())?,
                                    text(value, owner.as_ref())?,
                                ));
                            }
                            LiteralSource::Utf8Map(arc_slice(copied, owner.as_ref())?)
                        }
                    };
                    sources.push(source);
                }
                Producer::Literal { sources }
            };
            slots.push(root_slot(original, nullable, owner.as_ref())?);
            producers.push(producer);
        }
        let output = ChunkSchema::try_new_typed_observed::<ConstructionFailure>(
            slots,
            owner.storage_lifetime(),
            |layout| owner.request(layout),
            |units| statistics_work(owner.as_ref(), units),
        )?;
        grouped_arc::<ChunkSchema>(owner.as_ref())?;
        targets.push(GroupedTarget {
            target,
            row: row_index,
            expansion: UnpivotExpansion::new(
                arc_slice(producers, owner.as_ref())?,
                Arc::new(output),
                count,
                plan.max_output_rows,
                plan.max_output_bytes,
            ),
        });
    }
    grouped_arc::<Observable>(owner.as_ref())?;
    let observable = Arc::new(Observable::new());
    owner.request(Layout::new::<CompiledGroupedUnpivot>())?;
    Ok(Box::new(CompiledGroupedUnpivot {
        final_chunk,
        constants: GroupedConstants {
            arrays: constants,
            storage: source_storage,
        },
        root_schema,
        targets,
        next: 0,
        started: false,
        tracker,
        source_observable: observable,
        factory,
        control,
        _storage: owner.storage_lifetime(),
    }))
}
