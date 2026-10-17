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

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray};
use arrow::compute::take;
use arrow::datatypes::{DataType, Int32Type};

use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use novarocks_types::SlotId;

pub fn is_supported_i32_string_dictionary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Dictionary(key_type, value_type)
            if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
    )
}

pub fn referenced_slots<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr_id: ExprId,
) -> Option<BTreeSet<SlotId>> {
    let mut slots = BTreeSet::new();
    visit_referenced_slots(arena, expr_id, &mut |slot| {
        slots.insert(slot);
    })
    .then_some(slots)
}

/// Observe the same lexical references without constructing a temporary set.
/// An invalid expression cannot establish an encoding capability.
pub(crate) fn expression_uses_slot<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr_id: ExprId,
    slot_id: SlotId,
) -> Option<bool> {
    let mut found = false;
    visit_referenced_slots(arena, expr_id, &mut |slot| found |= slot == slot_id).then_some(found)
}

pub fn expr_can_peel_from_slot<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr_id: ExprId,
    slot_id: SlotId,
) -> bool {
    peel_source_slot(arena, expr_id) == Some(slot_id)
}

pub fn try_peel_dict_expr<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr_id: ExprId,
    chunk: &Chunk,
) -> Result<Option<ArrayRef>, __Eval::Error> {
    Ok((try_peel_dict_expr_with(
        arena,
        expr_id,
        chunk,
        |slot, values| -> Result<Chunk, __Eval::Error> {
            Ok(dict_values_chunk_for_source(chunk, slot, values)?)
        },
        |_| Ok(()),
        |_, _| Ok(()),
    ))?)
}

pub(crate) struct OwnedDictionaryPeel {
    pub array: ArrayRef,
    /// Present only when this producer has created new dictionary values.
    pub fresh_field: Option<arrow::datatypes::FieldRef>,
    pub storage: novarocks_type_contract::ValueTypeStorageLifetime,
}

pub(crate) fn try_peel_dict_expr_owned(
    arena: &ExprArena,
    expr_id: ExprId,
    chunk: &Chunk,
    output_name: &str,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    control: &dyn crate::exec::expr::ExprEvaluationControl,
) -> crate::runtime::fragment::ExecutionResult<Option<OwnedDictionaryPeel>> {
    use crate::exec::chunk::{ArrayNormalizationStep, ChunkSlotSchema};
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_type_contract::{ValueSchemaCreation, owned_resources::layout::arc_layout};
    let Some(source_slot) = peel_source_slot(arena, expr_id) else {
        return Ok(None);
    };
    let Some(source) = chunk.column_by_slot_id(source_slot).ok() else {
        return Ok(None);
    };
    if !is_supported_i32_string_dictionary(source.data_type()) {
        return Ok(None);
    }
    let dict = source
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| {
            ConstructionFailure::InvalidSource(
                "dictionary peel physical wrapper differs from its carrier".into(),
            )
        })?;
    if dict.values().null_count() > 0 {
        return Ok(None);
    }
    let declaration = arena.value_type(expr_id).ok_or_else(|| {
        ConstructionFailure::InvalidSource(
            "dictionary peel lacks its complete output declaration".into(),
        )
    })?;
    validate_peel_source_bindings(arena, expr_id, chunk, owner)?;
    let storage = chunk.source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
        owner.request(layout)
    })?;
    let evaluation = crate::exec::expr::ExprEvaluationContext::new(arena, owner, control);
    let array = try_peel_dict_expr_with(
        &evaluation,
        expr_id,
        chunk,
        |slot, values| -> crate::runtime::fragment::ExecutionResult<Chunk> {
            let source = chunk.chunk_schema().slot(slot).ok_or_else(|| {
                ConstructionFailure::InvalidSource("dictionary peel source is absent".into())
            })?;
            if !source.is_typed() {
                return Err(ConstructionFailure::InvalidSource(
                    "dictionary peel source lacks its complete binding".into(),
                )
                .into());
            }
            // The whole-values fast path was refused above if any value is NULL.
            // Preserve the actual source declaration; unused NULLs never widen it.
            let field =
                novarocks_functions::array_projection::rebuild_field::<ConstructionFailure, _>(
                    source.field(),
                    source.typed_binding().data_type(),
                    source.name(),
                    &mut |step| owner.observe(0, &[], step),
                )?;
            let value_slot = ChunkSlotSchema::try_new_typed_observed::<ConstructionFailure, _>(
                slot,
                field,
                source.typed_binding().clone(),
                source.unique_id(),
                storage.clone(),
                |step| owner.observe(0, &[], step),
            )?;
            owner.request(std::alloc::Layout::new::<ChunkSlotSchema>())?;
            let mut slots = Vec::with_capacity(1);
            slots.push(value_slot);
            let schema = ChunkSchema::try_new_typed_observed::<ConstructionFailure>(
                slots,
                storage.clone(),
                |layout| owner.request(layout),
                |units| {
                    owner.observe(
                        0,
                        &[],
                        ArrayNormalizationStep::InspectConstructionWork { units },
                    )
                },
            )?;
            owner.request(
                arc_layout(std::alloc::Layout::new::<ChunkSchema>())
                    .map_err(ConstructionFailure::from)?,
            )?;
            let schema = Arc::new(schema);
            let rows = values.len();
            owner.request(std::alloc::Layout::new::<ArrayRef>())?;
            let mut columns = Vec::with_capacity(1);
            columns.push(values);
            Ok(Chunk::try_new_typed_with_columns_owned_observed::<
                ConstructionFailure,
                _,
            >(
                schema,
                columns,
                rows,
                storage.clone(),
                |step| owner.observe(0, &[], step),
            )?)
        },
        |_| -> crate::runtime::fragment::ExecutionResult<()> {
            // Arrow's actual DictionaryArray constructor creates its two boxed
            // DataType operands and the new shared array wrapper.
            owner.request(std::alloc::Layout::new::<DataType>())?;
            owner.request(std::alloc::Layout::new::<DataType>())?;
            Ok(owner.request(
                arc_layout(std::alloc::Layout::new::<DictionaryArray<Int32Type>>())
                    .map_err(ConstructionFailure::from)?,
            )?)
        },
        |values, dict| -> crate::runtime::fragment::ExecutionResult<()> {
            let mut request = |layout| owner.request(layout);
            let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
            let mut creation = ValueSchemaCreation::new(storage.clone(), &mut request, &mut opaque);
            // Immutable ingress has checked every live signed Int32 key. This
            // borrowed unsigned view is used only by the copy geometry author;
            // the original signed keys and NULL mask still drive Arrow take.
            let unsigned = arrow::array::UInt32Array::new(
                arrow::buffer::ScalarBuffer::new(
                    dict.keys().values().inner().clone(),
                    0,
                    dict.len(),
                ),
                dict.keys().nulls().cloned(),
            );
            novarocks_functions::selected_copy::preflight_take_construction_unified_observed(
                values.as_ref(),
                novarocks_functions::selected_copy::CopyIndexView::UInt32(&unsigned),
                &mut creation,
                &mut |_| control.checkpoint().map_err(ConstructionFailure::from),
            )
            .map_err(|error| -> crate::runtime::fragment::ExecutionFailure {
                match error {
                    novarocks_functions::selected_copy::CopyConstructionError::Creator(error) => {
                        error.into()
                    }
                    novarocks_functions::selected_copy::CopyConstructionError::Resource(error) => {
                        ConstructionFailure::Resource(error).into()
                    }
                    novarocks_functions::selected_copy::CopyConstructionError::Copy(error) => {
                        match error {
                            novarocks_functions::selected_copy::CopyError::Control(error) => {
                                error.into()
                            }
                            error => ConstructionFailure::InvalidSource(format!(
                                "dictionary peel copy: {error:?}"
                            ))
                            .into(),
                        }
                    }
                }
            })?;
            Ok(())
        },
    )?;
    let Some(array) = array else {
        return Ok(None);
    };
    let fresh_field = if is_supported_i32_string_dictionary(array.data_type()) {
        owner.observe(
            0,
            &[],
            ArrayNormalizationStep::RebuildField {
                source: chunk
                    .chunk_schema()
                    .slot(peel_source_slot(arena, expr_id).ok_or_else(|| {
                        ConstructionFailure::InvalidSource(
                            "dictionary peel has no actual source".into(),
                        )
                    })?)
                    .ok_or_else(|| {
                        ConstructionFailure::InvalidSource(
                            "dictionary peel source field is absent".into(),
                        )
                    })?
                    .field(),
                target: array.data_type(),
                name: output_name,
            },
        )?;
        #[allow(deprecated)]
        let field = arrow::datatypes::Field::new_dict(
            output_name,
            array.data_type().clone(),
            declaration.nullable(),
            0,
            false,
        );
        Some(Arc::new(field))
    } else {
        None
    };
    Ok(Some(OwnedDictionaryPeel {
        array,
        fresh_field,
        storage,
    }))
}

/// Validate only this installed PEEL grammar's actual reachable sources.
/// This has no lambda/binder vocabulary and cannot authorize a general arena.
fn validate_peel_source_bindings<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    chunk: &Chunk,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
) -> Result<(), crate::exec::pipeline::operator::ConstructionFailure> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    let work = || {
        owner.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 },
        )
    };
    work()?;
    match arena.node(expr) {
        Some(ExprNode::SlotId(slot)) => {
            let source = chunk.chunk_schema().slot(*slot).ok_or_else(|| {
                ConstructionFailure::InvalidSource("dictionary peel source is absent".into())
            })?;
            let expected = arena.value_type(expr).ok_or_else(|| {
                ConstructionFailure::InvalidSource(
                    "dictionary peel source expression lacks its complete declaration".into(),
                )
            })?;
            if !source.is_typed() {
                return Err(ConstructionFailure::InvalidSource(
                    "dictionary peel source lacks its complete binding".into(),
                ));
            }
            let source_binding = source.typed_binding();
            if source_binding
                .value_type()
                .shares_storage(expected.value_type())
                && source_binding
                    .layout()
                    .shares_storage(expected.typed_binding().layout())
            {
                return Ok(());
            }
            let actual = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                source_binding.clone_observed(work)?,
            )
            .map_err(|error| ConstructionFailure::InvalidSource(error.to_string()))?;
            if !actual.fits_physical_binding_observed(expected, work)? {
                return Err(ConstructionFailure::InvalidSource(
                    "dictionary peel source differs from its complete expression declaration"
                        .into(),
                ));
            }
            Ok(())
        }
        Some(ExprNode::Cast(child, _)) | Some(ExprNode::Clone(child)) => {
            validate_peel_source_bindings(arena, *child, chunk, owner)
        }
        Some(ExprNode::FunctionCall { args, .. }) => {
            for child in args {
                validate_peel_source_bindings(arena, *child, chunk, owner)?;
            }
            Ok(())
        }
        Some(ExprNode::Literal(_)) => Ok(()),
        _ => Err(ConstructionFailure::InvalidSource(
            "dictionary peel has no installed grammar for this source".into(),
        )),
    }
}

fn try_peel_dict_expr_with<
    __Eval: crate::exec::expr::ExprEvaluation + ?Sized,
    CallbackError: Into<__Eval::Error>,
>(
    arena: &__Eval,
    expr_id: ExprId,
    chunk: &Chunk,
    mut value_port: impl FnMut(SlotId, ArrayRef) -> Result<Chunk, CallbackError>,
    mut rewrap: impl FnMut(&DictionaryArray<Int32Type>) -> Result<(), CallbackError>,
    mut expand: impl FnMut(&ArrayRef, &DictionaryArray<Int32Type>) -> Result<(), CallbackError>,
) -> Result<Option<ArrayRef>, __Eval::Error> {
    let Some(source_slot) = peel_source_slot(arena, expr_id) else {
        return Ok(None);
    };
    let Some(source_idx) = chunk.slot_id_to_index().get(&source_slot).copied() else {
        return Ok(None);
    };
    let Some(source) = chunk.columns().get(source_idx) else {
        return Ok(None);
    };
    if !is_supported_i32_string_dictionary(source.data_type()) {
        return Ok(None);
    }
    let dict = source
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| {
            format!(
                "dictionary peel expected Dictionary(Int32, Utf8), got {:?}",
                source.data_type()
            )
        })
        .map_err(__Eval::Error::from)?;
    if dict.values().null_count() > 0 {
        return Ok(None);
    }

    let value_chunk = value_port(source_slot, Arc::clone(dict.values())).map_err(Into::into)?;
    let new_values = arena.eval(expr_id, &value_chunk)?;

    if new_values.data_type() == &DataType::Utf8 && new_values.null_count() == 0 {
        rewrap(dict).map_err(Into::into)?;
        let peeled = DictionaryArray::<Int32Type>::try_new(dict.keys().clone(), new_values)
            .map_err(|e| __Eval::Error::from(format!("dictionary peel rewrap failed: {e}")))?;
        return Ok(Some(Arc::new(peeled) as ArrayRef));
    }

    expand(&new_values, dict).map_err(Into::into)?;
    let expanded = take(new_values.as_ref(), dict.keys(), None)
        .map_err(|e| __Eval::Error::from(format!("dictionary peel take failed: {e}")))?;
    Ok(Some(expanded))
}

fn dict_values_chunk_for_source(
    chunk: &Chunk,
    source_slot: SlotId,
    values: ArrayRef,
) -> Result<Chunk, String> {
    let source_slot_schema = chunk
        .chunk_schema()
        .slot(source_slot)
        .ok_or_else(|| format!("slot id {} missing from chunk schema", source_slot))?;
    let value_field = source_slot_schema
        .field()
        .clone()
        .with_data_type(values.data_type().clone())
        .with_nullable(values.is_nullable());
    let value_slot = source_slot_schema.with_field(value_field)?;
    let value_schema = Arc::new(ChunkSchema::try_new(vec![value_slot])?);
    Chunk::try_new_with_columns(value_schema, vec![values])
}

fn peel_source_slot<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr_id: ExprId,
) -> Option<SlotId> {
    if !is_peel_safe_expr(arena, expr_id) {
        return None;
    }
    let mut source = None;
    let mut multiple = false;
    let valid = visit_referenced_slots(arena, expr_id, &mut |slot| match source {
        None => source = Some(slot),
        Some(previous) => multiple |= previous != slot,
    });
    (valid && !multiple).then_some(source).flatten()
}

fn visit_referenced_slots<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr_id: ExprId,
    visit: &mut impl FnMut(SlotId),
) -> bool {
    visit_referenced_slots_with_bound(arena, expr_id, None, visit)
}

struct LexicalBindings<'a> {
    parent: Option<&'a LexicalBindings<'a>>,
    arguments: &'a [SlotId],
    common: &'a [(SlotId, ExprId)],
}
impl LexicalBindings<'_> {
    fn contains(&self, slot: SlotId) -> bool {
        let mut frame = Some(self);
        while let Some(current) = frame {
            if current.arguments.contains(&slot)
                || current.common.iter().any(|(bound, _)| *bound == slot)
            {
                return true;
            }
            frame = current.parent;
        }
        false
    }
}

fn visit_referenced_slots_with_bound<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr_id: ExprId,
    bound: Option<&LexicalBindings<'_>>,
    visit: &mut impl FnMut(SlotId),
) -> bool {
    let Some(node) = arena.node(expr_id) else {
        return false;
    };
    match node {
        ExprNode::Constant(_) | ExprNode::Literal(_) => true,
        ExprNode::SlotId(slot_id) => {
            if !bound.is_some_and(|bound| bound.contains(*slot_id)) {
                visit(*slot_id);
            }
            true
        }
        ExprNode::ArrayExpr { elements } | ExprNode::StructExpr { fields: elements } => {
            visit_all_referenced_slots_with_bound(arena, elements, bound, visit)
        }
        ExprNode::LambdaFunction {
            body,
            arg_slots,
            common_sub_exprs,
            ..
        } => {
            let nested_bound = LexicalBindings {
                parent: bound,
                arguments: arg_slots,
                common: common_sub_exprs,
            };
            visit_referenced_slots_with_bound(arena, *body, Some(&nested_bound), visit)
                && common_sub_exprs.iter().all(|(_, expr_id)| {
                    visit_referenced_slots_with_bound(arena, *expr_id, Some(&nested_bound), visit)
                })
        }
        ExprNode::DictDecode { child, .. }
        | ExprNode::Cast(child, _)
        | ExprNode::CastTime(child, _)
        | ExprNode::CastTimeFromDatetime(child, _)
        | ExprNode::Not(child)
        | ExprNode::IsNull(child)
        | ExprNode::IsNotNull(child)
        | ExprNode::Clone(child) => visit_referenced_slots_with_bound(arena, *child, bound, visit),
        ExprNode::Add(left, right, _)
        | ExprNode::Sub(left, right, _)
        | ExprNode::Mul(left, right, _)
        | ExprNode::Div(left, right, _)
        | ExprNode::Mod(left, right, _)
        | ExprNode::Eq(left, right)
        | ExprNode::EqForNull(left, right)
        | ExprNode::Ne(left, right)
        | ExprNode::Lt(left, right)
        | ExprNode::Le(left, right)
        | ExprNode::Gt(left, right)
        | ExprNode::Ge(left, right)
        | ExprNode::And(left, right)
        | ExprNode::Or(left, right) => {
            visit_referenced_slots_with_bound(arena, *left, bound, visit)
                && visit_referenced_slots_with_bound(arena, *right, bound, visit)
        }
        ExprNode::In { child, values, .. } => {
            visit_referenced_slots_with_bound(arena, *child, bound, visit)
                && visit_all_referenced_slots_with_bound(arena, values, bound, visit)
        }
        ExprNode::Case { children, .. } => {
            visit_all_referenced_slots_with_bound(arena, children, bound, visit)
        }
        ExprNode::FunctionCall { args, .. } => {
            visit_all_referenced_slots_with_bound(arena, args, bound, visit)
        }
    }
}

fn visit_all_referenced_slots_with_bound<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr_ids: &[ExprId],
    bound: Option<&LexicalBindings<'_>>,
    visit: &mut impl FnMut(SlotId),
) -> bool {
    expr_ids
        .iter()
        .copied()
        .all(|expr_id| visit_referenced_slots_with_bound(arena, expr_id, bound, visit))
}

fn is_peel_safe_expr<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr_id: ExprId,
) -> bool {
    let Some(node) = arena.node(expr_id) else {
        return false;
    };
    match node {
        ExprNode::SlotId(_) => true,
        ExprNode::Literal(value) => !matches!(value, LiteralValue::Null),
        ExprNode::Cast(child, novarocks_type_contract::DecimalOverflowPolicy::OutputNull)
        | ExprNode::Clone(child) => is_peel_safe_expr(arena, *child),
        ExprNode::FunctionCall { kind, args } if is_peel_safe_function(*kind) => args
            .iter()
            .copied()
            .all(|arg| is_peel_safe_expr(arena, arg)),
        _ => false,
    }
}

fn is_peel_safe_function(kind: FunctionKind) -> bool {
    matches!(kind, FunctionKind::Upper | FunctionKind::Substring)
        || matches!(
            kind,
            FunctionKind::String(
                "lower"
                    | "lcase"
                    | "trim"
                    | "ltrim"
                    | "rtrim"
                    | "reverse"
                    | "length"
                    | "char_length"
            )
        )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type};

    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::exec::expr::function::FunctionKind;
    use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
    use novarocks_types::SlotId;

    use super::{
        expr_can_peel_from_slot, expression_uses_slot, is_supported_i32_string_dictionary,
        referenced_slots, try_peel_dict_expr,
    };

    fn chunk_with_column(slot_id: SlotId, values: ArrayRef) -> Chunk {
        let slot = ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new("status", values.data_type().clone(), true),
            None,
            None,
        );
        Chunk::try_new_with_columns(
            Arc::new(ChunkSchema::try_new(vec![slot]).unwrap()),
            vec![values],
        )
        .unwrap()
    }

    fn dict_status_chunk(slot_id: SlotId) -> Chunk {
        let values: ArrayRef = Arc::new(
            vec![
                Some("PAID"),
                None,
                Some("New"),
                Some("PAID"),
                Some(" shipped "),
            ]
            .into_iter()
            .collect::<DictionaryArray<Int32Type>>(),
        );
        chunk_with_column(slot_id, values)
    }

    fn lower_expr_for_slot(arena: &mut ExprArena, slot: SlotId) -> ExprId {
        let source = arena.push_typed(ExprNode::SlotId(slot), DataType::Utf8);
        arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("lower"),
                args: vec![source],
            },
            DataType::Utf8,
        )
    }

    #[test]
    fn peel_lower_over_utf8_dictionary_reuses_keys_and_lowercases_values() {
        let slot = SlotId::new(7);
        let chunk = dict_status_chunk(slot);
        let mut arena = ExprArena::default();
        let lower = lower_expr_for_slot(&mut arena, slot);

        assert!(expr_can_peel_from_slot(&arena, lower, slot));
        let out = try_peel_dict_expr(&arena, lower, &chunk)
            .expect("peel should not error")
            .expect("lower should peel");

        let dict = out
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("dictionary output");
        let input = chunk.columns()[0]
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("input dictionary");
        assert_eq!(dict.keys(), input.keys());
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string values");
        assert!(
            values
                .iter()
                .flatten()
                .all(|v: &str| v == v.to_lowercase().as_str())
        );
        assert!(dict.is_null(1));
    }

    #[test]
    fn unsafe_coalesce_is_not_peelable_but_still_reports_referenced_slot() {
        let slot = SlotId::new(8);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(slot), DataType::Utf8);
        let fallback = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("missing".to_string())),
            DataType::Utf8,
        );
        let coalesce = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Coalesce,
                args: vec![source, fallback],
            },
            DataType::Utf8,
        );

        assert_eq!(
            referenced_slots(&arena, coalesce)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![slot]
        );
        assert!(!expr_can_peel_from_slot(&arena, coalesce, slot));
    }

    #[test]
    fn lambda_slot_collection_reports_captures_but_excludes_bound_and_cse_slots() {
        let lambda_arg = SlotId::new(21);
        let captured_outer = SlotId::new(22);
        let cse_slot = SlotId::new(23);
        let captured_by_cse = SlotId::new(24);

        let mut arena = ExprArena::default();
        let arg_ref = arena.push_typed(ExprNode::SlotId(lambda_arg), DataType::Int32);
        let captured_ref = arena.push_typed(ExprNode::SlotId(captured_outer), DataType::Int32);
        let cse_ref = arena.push_typed(ExprNode::SlotId(cse_slot), DataType::Int32);
        let body = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![arg_ref, captured_ref, cse_ref],
            },
            DataType::Null,
        );
        let cse_expr = arena.push_typed(ExprNode::SlotId(captured_by_cse), DataType::Int32);
        let lambda = arena.push_typed(
            ExprNode::LambdaFunction {
                body,
                arg_slots: vec![lambda_arg],
                common_sub_exprs: vec![(cse_slot, cse_expr)],
                is_nondeterministic: false,
            },
            DataType::Null,
        );

        assert_eq!(
            referenced_slots(&arena, lambda)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![captured_outer, captured_by_cse]
        );
        assert!(!expr_can_peel_from_slot(&arena, lambda, captured_outer));
        for (slot, expected) in [
            (lambda_arg, false),
            (captured_outer, true),
            (cse_slot, false),
            (captured_by_cse, true),
        ] {
            assert_eq!(expression_uses_slot(&arena, lambda, slot), Some(expected));
        }
    }

    #[test]
    fn dictionary_values_with_null_entries_are_not_peeled() {
        let slot = SlotId::new(10);
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("PAID"), None, Some("New")]));
        let keys = Int32Array::from(vec![Some(0), Some(1), None, Some(2)]);
        let dict: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());
        let chunk = chunk_with_column(slot, dict);
        let mut arena = ExprArena::default();
        let lower = lower_expr_for_slot(&mut arena, slot);

        assert!(expr_can_peel_from_slot(&arena, lower, slot));
        assert!(
            try_peel_dict_expr(&arena, lower, &chunk)
                .expect("null dictionary values should not error")
                .is_none()
        );
    }

    #[test]
    fn non_string_peel_expands_flat_values_by_keys() {
        let slot = SlotId::new(9);
        let chunk = dict_status_chunk(slot);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(slot), DataType::Utf8);
        let length = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("length"),
                args: vec![source],
            },
            DataType::Int64,
        );

        let out = try_peel_dict_expr(&arena, length, &chunk)
            .expect("peel should not error")
            .expect("length should compute over dictionary values and flatten");

        assert_eq!(out.data_type(), &DataType::Int64);
        let flat = out
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("flat length output");
        assert_eq!(flat.value(0), 4);
        assert!(flat.is_null(1));
        assert_eq!(flat.value(2), 3);
    }

    #[test]
    fn supported_dictionary_type_requires_int32_keys_and_string_values() {
        let utf8_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let int_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));
        let int64_key_dict =
            DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8));

        assert!(is_supported_i32_string_dictionary(&utf8_dict));
        assert!(!is_supported_i32_string_dictionary(&int_dict));
        assert!(!is_supported_i32_string_dictionary(&int64_key_dict));
    }

    #[test]
    fn dictionary_peel_requires_a_non_reporting_cast_policy() {
        use novarocks_type_contract::DecimalOverflowPolicy as Policy;
        let mut arena = ExprArena::default();
        let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let null_cast = arena.push_typed(
            ExprNode::Cast(input, Policy::OutputNull),
            DataType::Decimal128(10, 0),
        );
        let error_cast = arena.push_typed(
            ExprNode::Cast(input, Policy::ReportError),
            DataType::Decimal128(10, 0),
        );
        assert!(super::is_peel_safe_expr(&arena, null_cast));
        assert!(!super::is_peel_safe_expr(&arena, error_cast));
    }
}
