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

//! Exact selected computation for the hidden value-domain conversions.
//! Shared-buffer metadata reconstruction and Arrow copy remain opaque library
//! work bracketed by the original control. No allocation grant is claimed.

use super::value_conversion::{
    JSON_TEXT, LARGEINT_DECIMAL, LARGEINT_FLOAT, LARGEINT_SIGNED, LARGEINT_TEXT, NULL_LIFT,
    OPAQUE_BINARY, SIGNED_LARGEINT, UTF8_LARGEINT,
};
use crate::{
    EvaluatedArgument, FunctionArgumentType, FunctionBindingSelection, KernelEvaluationControl,
    KernelFailure, ScalarCallContract, ScalarCallInput, SelectedValues, Selection,
    kernel_control::{compile_failure, internal, invalid},
    kernel_input::{EvaluationCheckpoints, validate_argument_observed, validate_type_observed},
    selected_copy::{self, CopyError},
};
use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeListArray, LargeListArray, ListArray, MapArray,
    PrimitiveArray, StructArray, UInt64Array,
    builder::{FixedSizeBinaryBuilder, PrimitiveBuilder},
    new_empty_array,
    types::*,
};
use arrow_schema::DataType;
use arrow_select::take::{TakeOptions, take};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, ExpressionEffectContext,
    PureCompileControl, arrow_data_types_exact_observed,
};
use std::{ops::Range, sync::Arc};

#[derive(Clone, Copy, Debug)]
enum Signed {
    I8,
    I16,
    I32,
    I64,
}
impl Signed {
    fn from_type(ty: &DataType) -> Result<Self, KernelFailure> {
        match ty {
            DataType::Int8 => Ok(Self::I8),
            DataType::Int16 => Ok(Self::I16),
            DataType::Int32 => Ok(Self::I32),
            DataType::Int64 => Ok(Self::I64),
            _ => Err(invalid("conversion has a foreign frozen signed carrier")),
        }
    }
}
#[derive(Clone, Copy, Debug)]
enum Operation {
    SameShape,
    SignedLargeInt(Signed),
    LargeIntSigned(Signed),
    LargeIntDecimal(super::value_conversion::largeint_decimal::DecimalRecipe),
    LargeIntF32,
    LargeIntF64,
    LargeIntText,
    Utf8LargeInt,
    NullLift,
}
#[derive(Clone, Copy, Debug)]
enum JsonKind {
    Keep,
    List,
    LargeList,
    FixedList,
    Struct,
    Map,
}
#[derive(Debug)]
struct JsonNode {
    kind: JsonKind,
    children: Range<usize>,
}

/// Algorithms and changed paths belong to the same canonical selected owner.
/// Shared selection backing remains owned by the immutable scalar contract.
#[derive(Debug)]
pub(super) struct ConversionRecipe {
    selected: Arc<FunctionBindingSelection>,
    context: ExpressionEffectContext,
    policy: DecimalOverflowPolicy,
    operation: Operation,
    nodes: Vec<JsonNode>,
    edges: Vec<usize>,
    retained: usize,
}
impl ConversionRecipe {
    pub(super) fn try_new(
        contract: &ScalarCallContract,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let outcome = (|| {
            let [FunctionArgumentType::Value(source)] = contract.selected().argument_types.as_ref()
            else {
                return Err(invalid("conversion requires one canonical value source"));
            };
            let target = contract.result_type();
            validate_type_observed(source, &mut work)?;
            validate_type_observed(target, &mut work)?;
            let operation = match contract.selected().overload.as_str() {
                JSON_TEXT | OPAQUE_BINARY => Operation::SameShape,
                SIGNED_LARGEINT => {
                    Operation::SignedLargeInt(Signed::from_type(source.data_type())?)
                }
                LARGEINT_SIGNED => {
                    Operation::LargeIntSigned(Signed::from_type(target.data_type())?)
                }
                LARGEINT_DECIMAL => Operation::LargeIntDecimal(
                    super::value_conversion::largeint_decimal::DecimalRecipe::try_new(
                        target.data_type(),
                    )
                    .ok_or_else(|| invalid("conversion has a foreign frozen Decimal target"))?,
                ),
                LARGEINT_FLOAT => match target.data_type() {
                    DataType::Float32 => Operation::LargeIntF32,
                    DataType::Float64 => Operation::LargeIntF64,
                    _ => return Err(invalid("conversion has a foreign frozen float target")),
                },
                LARGEINT_TEXT => Operation::LargeIntText,
                UTF8_LARGEINT => Operation::Utf8LargeInt,
                NULL_LIFT => Operation::NullLift,
                _ => return Err(invalid("conversion has an unknown canonical overload")),
            };
            work.step().map_err(compile_failure)?;
            let mut recipe = Self {
                selected: Arc::clone(contract.call().selected_owner()),
                context: contract.context(),
                policy: contract.decimal_overflow_policy(),
                operation,
                nodes: Vec::new(),
                edges: Vec::new(),
                retained: 0,
            };
            if matches!(operation, Operation::SameShape) {
                recipe.prepare_json(source.data_type(), target.data_type(), &mut work)?;
            }
            recipe.retained = std::mem::size_of::<Self>()
                .checked_add(
                    recipe
                        .nodes
                        .capacity()
                        .checked_mul(std::mem::size_of::<JsonNode>())
                        .ok_or(KernelFailure::ResourceExhausted)?,
                )
                .and_then(|bytes| {
                    bytes.checked_add(
                        recipe
                            .edges
                            .capacity()
                            .checked_mul(std::mem::size_of::<usize>())?,
                    )
                })
                .filter(|bytes| *bytes <= isize::MAX as usize)
                .ok_or(KernelFailure::ResourceExhausted)?;
            Ok(recipe)
        })();
        // All callback paths use this work latch or the original typed mapping.
        if primary(&outcome) {
            return outcome;
        }
        work.finish().map_err(compile_failure)?;
        outcome
    }
    pub(super) fn retained_bytes(&self) -> usize {
        self.retained
    }
    fn prepare_json(
        &mut self,
        source: &DataType,
        target: &DataType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, KernelFailure> {
        let same = arrow_data_types_exact_observed::<KernelFailure>(source, target, || {
            work.step().map_err(compile_failure)
        })?;
        let (kind, count) = if same {
            (JsonKind::Keep, 0)
        } else {
            match (source, target) {
                (DataType::List(_), DataType::List(_)) => (JsonKind::List, 1),
                (DataType::LargeList(_), DataType::LargeList(_)) => (JsonKind::LargeList, 1),
                (DataType::FixedSizeList(_, a), DataType::FixedSizeList(_, b)) if a == b => {
                    (JsonKind::FixedList, 1)
                }
                (DataType::Struct(a), DataType::Struct(b)) if a.len() == b.len() => {
                    (JsonKind::Struct, a.len())
                }
                (DataType::Map(_, a), DataType::Map(_, b)) if a == b => (JsonKind::Map, 1),
                _ => {
                    return Err(invalid(
                        "canonical JSON conversion changed an unsupported structure",
                    ));
                }
            }
        };
        let node = self.nodes.len();
        let first = self.edges.len();
        let end = first
            .checked_add(count)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let next = node
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        extent(end, std::mem::size_of::<usize>())?;
        extent(next, std::mem::size_of::<JsonNode>())?;
        if next > self.nodes.capacity() {
            work.flush().map_err(compile_failure)?;
            let reserved = self
                .nodes
                .try_reserve(1)
                .map_err(|_| KernelFailure::ResourceExhausted);
            reserved?;
            work.flush().map_err(compile_failure)?;
        }
        if end > self.edges.capacity() {
            work.flush().map_err(compile_failure)?;
            let reserved = self
                .edges
                .try_reserve(count)
                .map_err(|_| KernelFailure::ResourceExhausted);
            reserved?;
            work.flush().map_err(compile_failure)?;
        }
        self.nodes.push(JsonNode {
            kind,
            children: first..end,
        });
        work.step().map_err(compile_failure)?;
        for _ in 0..count {
            self.edges.push(0);
            work.step().map_err(compile_failure)?;
        }
        for ordinal in 0..count {
            let child = self.prepare_json(
                child_type(source, ordinal)?,
                child_type(target, ordinal)?,
                work,
            )?;
            self.edges[first + ordinal] = child;
            work.step().map_err(compile_failure)?;
        }
        Ok(node)
    }
    pub(super) fn evaluate<'a>(
        &self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let same = Arc::ptr_eq(&self.selected, input.contract().call().selected_owner())
            && self.context == input.contract().context()
            && self.policy == input.contract().decimal_overflow_policy();
        work.step()?;
        if !same {
            work.finish()?;
            return Err(invalid(
                "conversion input belongs to a different canonical contract",
            ));
        }
        let [FunctionArgumentType::Value(source)] = self.selected.argument_types.as_ref() else {
            work.finish()?;
            return Err(invalid("conversion lost its canonical value source"));
        };
        let [argument] = input.arguments() else {
            work.finish()?;
            return Err(invalid("conversion requires one evaluated argument"));
        };
        // This shared owner flushes its own ordinary exits and returns callback
        // refusal directly; never add a tail callback after its error.
        work.flush()?;
        validate_argument_observed(*argument, input.selection(), source, control)?;
        let outcome = (|| {
            let target = input.contract().result_type();
            let selection = input.selection();
            // Class heads are required even for empty/strict-NULL domains.
            check_classes(argument.array().as_ref(), &mut work)?;
            if matches!(self.operation, Operation::NullLift) && !selection.is_empty() {
                return Err(invalid(
                    "strict NULL lift received a nonempty active call domain",
                ));
            }
            let mut errors = Vec::new();
            let output = if selection.is_empty() {
                work.flush()?;
                let output = new_empty_array(target.data_type());
                work.flush()?;
                output
            } else {
                match self.operation {
                    Operation::SameShape => {
                        // Canonical classes are checked before opaque Arrow take,
                        // including unchanged encoded siblings and NULL parents.
                        let compact = compact(*argument, selection, &mut work)?;
                        self.rebind(0, &compact, target.data_type(), &mut work)?
                    }
                    Operation::SignedLargeInt(width) => {
                        signed_largeint(width, *argument, selection, &mut work)?
                    }
                    Operation::LargeIntSigned(width) => {
                        largeint_signed(width, *argument, selection, &mut work)?
                    }
                    Operation::LargeIntDecimal(recipe) => largeint_decimal(
                        recipe,
                        self.policy,
                        *argument,
                        selection,
                        &mut errors,
                        &mut work,
                    )?,
                    Operation::LargeIntF32 => {
                        largeint_float::<Float32Type>(*argument, selection, &mut work, |value| {
                            value as f32
                        })?
                    }
                    Operation::LargeIntF64 => {
                        largeint_float::<Float64Type>(*argument, selection, &mut work, |value| {
                            value as f64
                        })?
                    }
                    Operation::LargeIntText => largeint_text(*argument, selection, &mut work)?,
                    Operation::Utf8LargeInt => utf8_largeint(*argument, selection, &mut work)?,
                    Operation::NullLift => {
                        return Err(internal("NULL lift active domain was not excluded"));
                    }
                }
            };
            SelectedValues::try_new_observed::<KernelFailure>(
                selection,
                target.data_type(),
                output,
                errors.into_boxed_slice(),
                || work.step(),
            )
        })();
        finish_evaluation(outcome, work)
    }
    fn rebind(
        &self,
        node: usize,
        array: &ArrayRef,
        target: &DataType,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<ArrayRef, KernelFailure> {
        let node = self
            .nodes
            .get(node)
            .ok_or_else(|| internal("JSON recipe node is missing"))?;
        let child = |ordinal| {
            self.edges
                .get(node.children.start + ordinal)
                .copied()
                .ok_or_else(|| internal("JSON recipe child is missing"))
        };
        let output: ArrayRef = match (node.kind, target) {
            (JsonKind::Keep, _) => super::value_conversion::share_same_structure_keep(array),
            (JsonKind::List, DataType::List(field)) => {
                let source: &ListArray = downcast(array)?;
                let values = self.rebind(child(0)?, source.values(), field.data_type(), work)?;
                work.flush()?;
                let output = ListArray::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    values,
                    source.nulls().cloned(),
                )
                .map_err(|_| internal("canonical JSON list reconstruction failed"));
                work.flush()?;
                Arc::new(output?)
            }
            (JsonKind::LargeList, DataType::LargeList(field)) => {
                let source: &LargeListArray = downcast(array)?;
                let values = self.rebind(child(0)?, source.values(), field.data_type(), work)?;
                work.flush()?;
                let output = LargeListArray::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    values,
                    source.nulls().cloned(),
                )
                .map_err(|_| internal("canonical JSON large-list reconstruction failed"));
                work.flush()?;
                Arc::new(output?)
            }
            (JsonKind::FixedList, DataType::FixedSizeList(field, width)) => {
                let source: &FixedSizeListArray = downcast(array)?;
                let values = self.rebind(child(0)?, source.values(), field.data_type(), work)?;
                work.flush()?;
                let output = FixedSizeListArray::try_new_with_length(
                    Arc::clone(field),
                    *width,
                    values,
                    source.nulls().cloned(),
                    source.len(),
                )
                .map_err(|_| internal("canonical JSON fixed-list reconstruction failed"));
                work.flush()?;
                Arc::new(output?)
            }
            (JsonKind::Struct, DataType::Struct(fields)) => {
                let source: &StructArray = downcast(array)?;
                let mut columns = Vec::with_capacity(fields.len());
                for (ordinal, field) in fields.iter().enumerate() {
                    columns.push(self.rebind(
                        child(ordinal)?,
                        source.column(ordinal),
                        field.data_type(),
                        work,
                    )?);
                    work.step()?;
                }
                work.flush()?;
                let output = StructArray::try_new_with_length(
                    fields.clone(),
                    columns,
                    source.nulls().cloned(),
                    source.len(),
                )
                .map_err(|_| internal("canonical JSON struct reconstruction failed"));
                work.flush()?;
                Arc::new(output?)
            }
            (JsonKind::Map, DataType::Map(field, sorted)) => {
                let source: &MapArray = downcast(array)?;
                // Map entries are concrete StructArray; only its immutable
                // shallow handles are copied before the changed-path walk.
                work.flush()?;
                let entries = Arc::new(source.entries().clone()) as ArrayRef;
                work.flush()?;
                let entries = self.rebind(child(0)?, &entries, field.data_type(), work)?;
                let entries: &StructArray = downcast(&entries)?;
                work.flush()?;
                let output = MapArray::try_new(
                    Arc::clone(field),
                    source.offsets().clone(),
                    entries.clone(),
                    source.nulls().cloned(),
                    *sorted,
                )
                .map_err(|_| internal("canonical JSON map reconstruction failed"));
                work.flush()?;
                Arc::new(output?)
            }
            _ => {
                return Err(internal(
                    "JSON recipe target differs from its frozen structure",
                ));
            }
        };
        work.step()?;
        Ok(output)
    }
}
fn primary<T>(result: &Result<T, KernelFailure>) -> bool {
    matches!(
        result,
        Err(KernelFailure::Cancelled
            | KernelFailure::DeadlineExceeded
            | KernelFailure::ResourceExhausted)
    )
}
fn finish_evaluation<'a>(
    result: Result<SelectedValues<'a>, KernelFailure>,
    work: EvaluationCheckpoints<'_>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    // Direct extent/reservation failures are primary even without a callback
    // latch. Do not replace them with a new tail control refusal.
    if primary(&result) {
        return result;
    }
    work.finish()?;
    result
}
fn child_type(ty: &DataType, ordinal: usize) -> Result<&DataType, KernelFailure> {
    match ty {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _)
            if ordinal == 0 =>
        {
            Ok(field.data_type())
        }
        DataType::Struct(fields) => fields
            .get(ordinal)
            .map(|field| field.data_type())
            .ok_or_else(|| invalid("canonical JSON child type is missing")),
        _ => Err(invalid("canonical JSON container type is missing")),
    }
}
fn downcast<T: 'static>(array: &ArrayRef) -> Result<&T, KernelFailure> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| internal("conversion carrier has a foreign array implementation"))
}
fn extent(rows: usize, width: usize) -> Result<(), KernelFailure> {
    let bytes = rows
        .checked_mul(width)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let bitmap = rows
        .checked_add(7)
        .ok_or(KernelFailure::ResourceExhausted)?
        / 8;
    if bytes > isize::MAX as usize || bitmap > isize::MAX as usize {
        return Err(KernelFailure::ResourceExhausted);
    }
    Ok(())
}
fn compact(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    if matches!(argument, EvaluatedArgument::SelectedColumn(_))
        || matches!(argument, EvaluatedArgument::Column(_)) && selection.is_all()
    {
        work.step()?;
        return Ok(Arc::clone(argument.array()));
    }
    extent(selection.len(), std::mem::size_of::<Option<u64>>())?;
    let mut indices = Vec::with_capacity(selection.len());
    for (ordinal, row) in selection.iter().enumerate() {
        indices.push(Some(
            u64::try_from(argument.value_row(ordinal, row))
                .map_err(|_| KernelFailure::ResourceExhausted)?,
        ));
        work.step()?;
    }
    work.flush()?;
    selected_copy::preflight_take(argument.array().as_ref(), &indices, |boundary| {
        if boundary { work.flush() } else { work.step() }
    })
    .map_err(|error| match error {
        CopyError::Control(cause) => cause,
        CopyError::Extent => KernelFailure::ResourceExhausted,
        CopyError::Invalid(_) | CopyError::Unsupported(_) => {
            invalid("conversion selected copy violates its carrier protocol")
        }
    })?;
    work.flush()?;
    let indices = UInt64Array::from_iter(indices);
    work.flush()?;
    let result = take(
        argument.array().as_ref(),
        &indices,
        Some(TakeOptions { check_bounds: true }),
    )
    .map_err(|_| internal("conversion selected Arrow copy failed"));
    work.flush()?;
    result
}

fn large_source<'a>(
    argument: EvaluatedArgument<'a>,
) -> Result<&'a FixedSizeBinaryArray, KernelFailure> {
    let source: &FixedSizeBinaryArray = downcast(argument.array())?;
    if source.value_length() != 16 {
        return Err(internal("conversion LargeInt has a foreign fixed width"));
    }
    Ok(source)
}
fn large_value(source: &FixedSizeBinaryArray, row: usize) -> Result<i128, KernelFailure> {
    let bytes = source
        .value(row)
        .try_into()
        .map_err(|_| internal("conversion LargeInt value has a foreign fixed width"))?;
    Ok(i128::from_be_bytes(bytes))
}
fn signed_largeint(
    width: Signed,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    extent(selection.len(), 16)?;
    macro_rules! run {
        ($ty:ty) => {{
            let source: &PrimitiveArray<$ty> = downcast(argument.array())?;
            let mut output = FixedSizeBinaryBuilder::with_capacity(selection.len(), 16);
            for (ordinal, row) in selection.iter().enumerate() {
                let row = argument.value_row(ordinal, row);
                if source.is_null(row) {
                    output.append_null();
                } else {
                    output
                        .append_value(i128::from(source.value(row)).to_be_bytes())
                        .map_err(|_| {
                            internal("conversion LargeInt append violated its fixed width")
                        })?;
                }
                work.step()?;
            }
            Ok(Arc::new(output.finish()) as ArrayRef)
        }};
    }
    match width {
        Signed::I8 => run!(Int8Type),
        Signed::I16 => run!(Int16Type),
        Signed::I32 => run!(Int32Type),
        Signed::I64 => run!(Int64Type),
    }
}
fn largeint_decimal(
    recipe: super::value_conversion::largeint_decimal::DecimalRecipe,
    policy: DecimalOverflowPolicy,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    errors: &mut Vec<crate::RowDataError>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let source = large_source(argument)?;
    extent(
        selection.len(),
        if recipe.is_decimal128() { 16 } else { 32 },
    )?;
    macro_rules! convert {
        ($ty:ty, $extract:expr) => {{
            work.flush()?;
            let mut output = PrimitiveBuilder::<$ty>::with_capacity(selection.len())
                .with_precision_and_scale(recipe.precision(), recipe.scale())
                .map_err(|_| invalid("conversion Decimal output differs from its frozen parameters"))?;
            work.flush()?;
            for (ordinal, logical_row) in selection.iter().enumerate() {
                let row = argument.value_row(ordinal, logical_row);
                work.step()?;
                if source.is_null(row) {
                    output.append_null();
                    continue;
                }
                let value = recipe.convert(large_value(source, row)?);
                if let Some(value) = value {
                    output.append_value(($extract)(value)?);
                } else {
                    output.append_null();
                    if policy == DecimalOverflowPolicy::ReportError {
                        work.flush()?;
                        errors.push(crate::RowDataError::new(ordinal,
                            "Expr evaluate meet error: The numeric type cast involving decimal overflows"));
                        work.flush()?;
                    }
                }
                work.step()?;
            }
            work.flush()?;
            let output = Arc::new(output.finish()) as ArrayRef;
            work.flush()?;
            Ok(output)
        }};
    }
    if recipe.is_decimal128() {
        convert!(Decimal128Type, |value: arrow_buffer::i256| value
            .to_i128()
            .ok_or_else(|| internal(
                "conversion Decimal128 coefficient escaped its precision"
            )))
    } else {
        convert!(Decimal256Type, |value| Ok::<_, KernelFailure>(value))
    }
}

fn utf8_largeint(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let source: &arrow_array::StringArray = downcast(argument.array())?;
    extent(selection.len(), 16)?;
    work.flush()?;
    let mut output = FixedSizeBinaryBuilder::with_capacity(selection.len(), 16);
    work.flush()?;
    for (ordinal, row) in selection.iter().enumerate() {
        let row = argument.value_row(ordinal, row);
        work.step()?;
        let value = if source.is_null(row) {
            None
        } else {
            work.flush()?;
            let value = crate::largeint_text::parse_utf8(source.value(row));
            work.flush()?;
            value
        };
        if let Some(value) = value {
            output
                .append_value(value.to_be_bytes())
                .map_err(|_| internal("conversion LargeInt append violated its fixed width"))?;
        } else {
            output.append_null();
        }
        work.step()?;
    }
    work.flush()?;
    let output = Arc::new(output.finish()) as ArrayRef;
    work.flush()?;
    Ok(output)
}

fn largeint_text(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let source = large_source(argument)?;
    // Offset slots follow the exact selected rows. Text bytes are counted from
    // the ONE formatted result below, without a conservative per-row type cap.
    extent(
        selection
            .len()
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?,
        4,
    )?;
    let mut bytes = 0usize;
    work.flush()?;
    let mut output = arrow_array::builder::StringBuilder::new();
    work.flush()?;
    for (ordinal, row) in selection.iter().enumerate() {
        let row = argument.value_row(ordinal, row);
        work.step()?;
        if source.is_null(row) {
            output.append_null();
        } else {
            work.flush()?;
            let text =
                crate::largeint_text::value_text(source, row).map_err(|error| internal(&error))?;
            work.flush()?;
            bytes = bytes
                .checked_add(text.len())
                .ok_or(KernelFailure::ResourceExhausted)?;
            i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
            output.append_value(text);
        }
        work.step()?;
    }
    work.flush()?;
    let result = Arc::new(output.finish()) as ArrayRef;
    work.flush()?;
    Ok(result)
}

fn largeint_signed(
    width: Signed,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let source = large_source(argument)?;
    macro_rules! run {
        ($ty:ty,$native:ty) => {{
            extent(selection.len(), std::mem::size_of::<$native>())?;
            let mut output = PrimitiveBuilder::<$ty>::with_capacity(selection.len());
            for (ordinal, row) in selection.iter().enumerate() {
                let row = argument.value_row(ordinal, row);
                if source.is_null(row) {
                    output.append_null();
                } else {
                    output.append_option(<$native>::try_from(large_value(source, row)?).ok());
                }
                work.step()?;
            }
            Ok(Arc::new(output.finish()) as ArrayRef)
        }};
    }
    match width {
        Signed::I8 => run!(Int8Type, i8),
        Signed::I16 => run!(Int16Type, i16),
        Signed::I32 => run!(Int32Type, i32),
        Signed::I64 => run!(Int64Type, i64),
    }
}
fn largeint_float<T: ArrowPrimitiveType>(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
    cast: impl Fn(i128) -> T::Native,
) -> Result<ArrayRef, KernelFailure> {
    let source = large_source(argument)?;
    extent(selection.len(), std::mem::size_of::<T::Native>())?;
    let mut output = PrimitiveBuilder::<T>::with_capacity(selection.len());
    for (ordinal, row) in selection.iter().enumerate() {
        let row = argument.value_row(ordinal, row);
        if source.is_null(row) {
            output.append_null();
        } else {
            output.append_value(cast(large_value(source, row)?));
        }
        work.step()?;
    }
    Ok(Arc::new(output.finish()))
}

/// Opaque Arrow take may downcast every touched child. Check canonical concrete
/// classes without reading row payloads, including unchanged encoded siblings.
fn check_classes(
    array: &dyn Array,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    macro_rules! leaf {
        ($ty:ty) => {
            if !array.as_any().is::<$ty>() {
                return Err(internal(
                    "conversion carrier has a foreign array implementation",
                ));
            }
        };
    }
    macro_rules! primitive {
        ($ty:ty) => {
            leaf!(PrimitiveArray<$ty>)
        };
    }
    macro_rules! dictionary {
        ($ty:ty) => {{
            let array = array
                .as_any()
                .downcast_ref::<arrow_array::DictionaryArray<$ty>>()
                .ok_or_else(|| {
                    internal("conversion dictionary has a foreign array implementation")
                })?;
            check_classes(array.values().as_ref(), work)?;
        }};
    }
    macro_rules! run {
        ($ty:ty) => {{
            let array = array
                .as_any()
                .downcast_ref::<arrow_array::RunArray<$ty>>()
                .ok_or_else(|| internal("conversion run array has a foreign implementation"))?;
            check_classes(array.values().as_ref(), work)?;
        }};
    }
    match array.data_type() {
        DataType::Null => leaf!(arrow_array::NullArray),
        DataType::Boolean => leaf!(arrow_array::BooleanArray),
        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::UInt8 => primitive!(UInt8Type),
        DataType::UInt16 => primitive!(UInt16Type),
        DataType::UInt32 => primitive!(UInt32Type),
        DataType::UInt64 => primitive!(UInt64Type),
        DataType::Float16 => primitive!(Float16Type),
        DataType::Float32 => primitive!(Float32Type),
        DataType::Float64 => primitive!(Float64Type),
        DataType::Date32 => primitive!(Date32Type),
        DataType::Date64 => primitive!(Date64Type),
        DataType::Timestamp(unit, _) => match unit {
            arrow_schema::TimeUnit::Second => primitive!(TimestampSecondType),
            arrow_schema::TimeUnit::Millisecond => primitive!(TimestampMillisecondType),
            arrow_schema::TimeUnit::Microsecond => primitive!(TimestampMicrosecondType),
            arrow_schema::TimeUnit::Nanosecond => primitive!(TimestampNanosecondType),
        },
        DataType::Time32(arrow_schema::TimeUnit::Second) => primitive!(Time32SecondType),
        DataType::Time32(arrow_schema::TimeUnit::Millisecond) => primitive!(Time32MillisecondType),
        DataType::Time64(arrow_schema::TimeUnit::Microsecond) => primitive!(Time64MicrosecondType),
        DataType::Time64(arrow_schema::TimeUnit::Nanosecond) => primitive!(Time64NanosecondType),
        DataType::Duration(unit) => match unit {
            arrow_schema::TimeUnit::Second => primitive!(DurationSecondType),
            arrow_schema::TimeUnit::Millisecond => primitive!(DurationMillisecondType),
            arrow_schema::TimeUnit::Microsecond => primitive!(DurationMicrosecondType),
            arrow_schema::TimeUnit::Nanosecond => primitive!(DurationNanosecondType),
        },
        DataType::Interval(unit) => match unit {
            arrow_schema::IntervalUnit::YearMonth => primitive!(IntervalYearMonthType),
            arrow_schema::IntervalUnit::DayTime => primitive!(IntervalDayTimeType),
            arrow_schema::IntervalUnit::MonthDayNano => primitive!(IntervalMonthDayNanoType),
        },
        DataType::Decimal32(_, _) => primitive!(Decimal32Type),
        DataType::Decimal64(_, _) => primitive!(Decimal64Type),
        DataType::Decimal128(_, _) => primitive!(Decimal128Type),
        DataType::Decimal256(_, _) => primitive!(Decimal256Type),
        DataType::Utf8 => leaf!(arrow_array::StringArray),
        DataType::LargeUtf8 => leaf!(arrow_array::LargeStringArray),
        DataType::Binary => leaf!(arrow_array::BinaryArray),
        DataType::LargeBinary => leaf!(arrow_array::LargeBinaryArray),
        DataType::Utf8View => leaf!(arrow_array::StringViewArray),
        DataType::BinaryView => leaf!(arrow_array::BinaryViewArray),
        DataType::FixedSizeBinary(_) => leaf!(FixedSizeBinaryArray),
        DataType::List(_) => {
            let array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| internal("conversion list has a foreign implementation"))?;
            check_classes(array.values().as_ref(), work)?;
        }
        DataType::LargeList(_) => {
            let array = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| internal("conversion large list has a foreign implementation"))?;
            check_classes(array.values().as_ref(), work)?;
        }
        DataType::FixedSizeList(_, _) => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| internal("conversion fixed list has a foreign implementation"))?;
            check_classes(array.values().as_ref(), work)?;
        }
        DataType::ListView(_) => {
            let array = array
                .as_any()
                .downcast_ref::<arrow_array::ListViewArray>()
                .ok_or_else(|| internal("conversion list view has a foreign implementation"))?;
            check_classes(array.values().as_ref(), work)?;
        }
        DataType::LargeListView(_) => {
            let array = array
                .as_any()
                .downcast_ref::<arrow_array::LargeListViewArray>()
                .ok_or_else(|| {
                    internal("conversion large list view has a foreign implementation")
                })?;
            check_classes(array.values().as_ref(), work)?;
        }
        DataType::Struct(_) => {
            let array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| internal("conversion struct has a foreign implementation"))?;
            for child in array.columns() {
                check_classes(child.as_ref(), work)?;
            }
        }
        DataType::Map(_, _) => {
            let array = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| internal("conversion map has a foreign implementation"))?;
            check_classes(array.entries(), work)?;
        }
        DataType::Union(fields, _) => {
            let array = array
                .as_any()
                .downcast_ref::<arrow_array::UnionArray>()
                .ok_or_else(|| internal("conversion union has a foreign implementation"))?;
            for (id, _) in fields.iter() {
                check_classes(array.child(id).as_ref(), work)?;
            }
        }
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => dictionary!(Int8Type),
            DataType::Int16 => dictionary!(Int16Type),
            DataType::Int32 => dictionary!(Int32Type),
            DataType::Int64 => dictionary!(Int64Type),
            DataType::UInt8 => dictionary!(UInt8Type),
            DataType::UInt16 => dictionary!(UInt16Type),
            DataType::UInt32 => dictionary!(UInt32Type),
            DataType::UInt64 => dictionary!(UInt64Type),
            _ => return Err(internal("conversion dictionary has a foreign key type")),
        },
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => run!(Int16Type),
            DataType::Int32 => run!(Int32Type),
            DataType::Int64 => run!(Int64Type),
            _ => return Err(internal("conversion run array has a foreign index type")),
        },
        _ => return Err(invalid("conversion has an invalid Arrow carrier")),
    }
    work.step()?;
    Ok(())
}

#[cfg(test)]
#[path = "value_conversion_kernel/tests.rs"]
mod tests;
