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

//! Exact physical Unpivot projection into the existing local vocabulary.
//! Input representatives are supplied by the sole channel author. No runtime
//! capability, constant re-admission, or formal allocation grant is created.

use novarocks_functions::ConstantError;
use novarocks_local_program::{
    LayoutCompileError, ProgramExprId, ProgramNodeId, ProgramNodeKind, StaticLayout,
    UnpivotConstant, UnpivotMapping, UnpivotPassthrough,
};
use novarocks_physical_plan::{
    ConstantReferenceError, ExprId, ExprKind, Fragment, FragmentPackage, NodeKind, PhysicalNode,
    UnpivotConstant as SourceConstant, ValueId,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    ValueTypeError,
};
use novarocks_types::SlotId;
use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};

#[derive(Debug)]
pub(crate) enum UnpivotLoweringError {
    Control(CompileControlError),
    ValueType(ValueTypeError),
    Layout(LayoutCompileError),
    Constant(ConstantReferenceError),
    Invalid(&'static str),
}
impl From<CompileControlError> for UnpivotLoweringError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<ValueTypeError> for UnpivotLoweringError {
    fn from(e: ValueTypeError) -> Self {
        Self::ValueType(e)
    }
}
impl From<LayoutCompileError> for UnpivotLoweringError {
    fn from(e: LayoutCompileError) -> Self {
        match e {
            LayoutCompileError::Control(c) => Self::Control(c),
            e => Self::Layout(e),
        }
    }
}
impl From<ConstantReferenceError> for UnpivotLoweringError {
    fn from(error: ConstantReferenceError) -> Self {
        match error {
            ConstantReferenceError::Control(cause) => Self::Control(cause),
            error => Self::Constant(error),
        }
    }
}
impl From<ConstantError> for UnpivotLoweringError {
    fn from(error: ConstantError) -> Self {
        ConstantReferenceError::from(error).into()
    }
}
impl fmt::Display for UnpivotLoweringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::ValueType(e) => e.fmt(f),
            Self::Layout(e) => e.fmt(f),
            Self::Constant(e) => e.fmt(f),
            Self::Invalid(m) => write!(f, "invalid Unpivot lowering: {m}"),
        }
    }
}
impl Error for UnpivotLoweringError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::ValueType(e) => Some(e),
            Self::Layout(e) => Some(e),
            Self::Constant(e) => Some(e),
            Self::Invalid(_) => None,
        }
    }
}

pub(crate) struct UnpivotInputPort<'a> {
    pub node: &'a PhysicalNode,
    pub slots: &'a [SlotId],
    pub representatives: &'a BTreeMap<ValueId, usize>,
}
pub(crate) struct UnpivotChannelPlan {
    pub slots: Arc<[SlotId]>,
    pub port: BTreeMap<ValueId, usize>,
    pub sources: BTreeMap<ValueId, SlotId>,
}
pub(crate) struct UnpivotLoweringInput<'a> {
    pub node: ProgramNodeId,
    pub layout: &'a StaticLayout,
    pub sources: &'a BTreeMap<ValueId, SlotId>,
    pub expressions: &'a BTreeMap<ExprId, ProgramExprId>,
}

fn exact_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), UnpivotLoweringError> {
    work.flush()?;
    let same = left.exactly_equals_observed::<UnpivotLoweringError>(right, || {
        work.control()
            .checkpoint(CompilePhase::LowerProgram, 0)
            .map_err(Into::into)
    })?;
    work.flush()?;
    if same {
        Ok(())
    } else {
        Err(UnpivotLoweringError::Invalid(
            "Unpivot complete value types differ",
        ))
    }
}
fn value_type(
    fragment: &Fragment,
    value: ValueId,
) -> Result<&FunctionValueType, UnpivotLoweringError> {
    fragment
        .values()
        .get(&value)
        .map(|v| &v.ty)
        .ok_or(UnpivotLoweringError::Invalid("missing Unpivot value"))
}
fn source_slot(
    sources: &BTreeMap<ValueId, SlotId>,
    value: ValueId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SlotId, UnpivotLoweringError> {
    let slot = sources.get(&value).copied();
    work.step()?;
    slot.ok_or(UnpivotLoweringError::Invalid(
        "Unpivot input has no proved channel",
    ))
}

pub(crate) fn plan_unpivot_channels(
    fragment: &Fragment,
    node: &PhysicalNode,
    input: UnpivotInputPort<'_>,
    next_slot: &mut u64,
    work: &mut CompileCheckpoints<'_>,
) -> Result<UnpivotChannelPlan, UnpivotLoweringError> {
    let NodeKind::Unpivot { spec } = &node.kind else {
        return Err(UnpivotLoweringError::Invalid("Unpivot source kind differs"));
    };
    if node.inputs.as_ref() != [input.node.id]
        || input.slots.len() != input.node.output.columns.len()
    {
        return Err(UnpivotLoweringError::Invalid(
            "Unpivot input channel shape differs",
        ));
    }
    let mut sources = BTreeMap::new();
    for (&value, &ordinal) in input.representatives {
        let valid = input.node.output.columns.get(ordinal) == Some(&value);
        let slot = input.slots.get(ordinal).copied();
        work.step()?;
        if !valid {
            return Err(UnpivotLoweringError::Invalid(
                "Unpivot input representative differs",
            ));
        }
        sources.insert(
            value,
            slot.ok_or(UnpivotLoweringError::Invalid("missing Unpivot input slot"))?,
        );
        work.step()?;
    }
    // Multiple passthrough outputs may read the same proved child value. Each
    // distinct output occurrence still gets its own output slot and header.
    let mut roles = BTreeMap::new();
    for &(source, output) in &spec.passthrough {
        source_slot(&sources, source, work)?;
        exact_type(
            value_type(fragment, source)?,
            value_type(fragment, output)?,
            work,
        )?;
        let old = roles.insert(output, ());
        work.step()?;
        if old.is_some() {
            return Err(UnpivotLoweringError::Invalid(
                "duplicate Unpivot output role",
            ));
        }
    }
    let old = roles.insert(spec.value_output, ());
    work.step()?;
    if old.is_some() {
        return Err(UnpivotLoweringError::Invalid(
            "duplicate Unpivot output role",
        ));
    }
    for &output in &spec.literal_outputs {
        let old = roles.insert(output, ());
        work.step()?;
        if old.is_some() {
            return Err(UnpivotLoweringError::Invalid(
                "duplicate Unpivot output role",
            ));
        }
    }
    if roles.len() != node.output.columns.len() {
        return Err(UnpivotLoweringError::Invalid(
            "Unpivot output role width differs",
        ));
    }
    let mut slots = Vec::new();
    let mut port = BTreeMap::new();
    for (ordinal, &value) in node.output.columns.iter().enumerate() {
        let covered = roles.remove(&value).is_some();
        work.step()?;
        if !covered {
            return Err(UnpivotLoweringError::Invalid(
                "Unpivot ordered output role differs",
            ));
        }
        let slot = u32::try_from(*next_slot)
            .map_err(|_| UnpivotLoweringError::Invalid("Unpivot slot identity exhausted"))?;
        *next_slot = next_slot
            .checked_add(1)
            .ok_or(UnpivotLoweringError::Invalid(
                "Unpivot slot identity exhausted",
            ))?;
        slots.push(SlotId::new(slot));
        port.insert(value, ordinal);
        work.step()?;
    }
    for mapping in &spec.mappings {
        source_slot(&sources, mapping.input, work)?;
    }
    work.flush()?;
    let slots = Arc::from(slots);
    work.flush()?;
    Ok(UnpivotChannelPlan {
        slots,
        port,
        sources,
    })
}

pub(crate) fn lower_unpivot_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    local: ProgramNodeId,
    input: UnpivotLoweringInput<'_>,
    planned_slots: &[SlotId],
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<(ProgramNodeKind, StaticLayout), crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core_with_creation(
        package,
        node,
        local,
        input,
        planned_slots,
        &mut work,
        creation,
    );
    crate::creation::finish_attempt(result, work)
}

fn lower_core_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    local: ProgramNodeId,
    input: UnpivotLoweringInput<'_>,
    planned_slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<(ProgramNodeKind, StaticLayout), crate::FragmentCreationError<H>> {
    let fragment = package.fragment();
    let NodeKind::Unpivot { spec } = &node.kind else {
        return Err((UnpivotLoweringError::Invalid("Unpivot source kind differs")).into());
    };
    if node.inputs.len() != 1
        || spec.mappings.is_empty()
        || planned_slots.len() != node.output.columns.len()
    {
        return Err((UnpivotLoweringError::Invalid("Unpivot materialized shape differs")).into());
    }
    let child = &fragment.nodes()[&node.inputs[0]];
    if child.output.columns.len() != input.layout.slots().len() {
        return Err((UnpivotLoweringError::Invalid("Unpivot child layout width differs")).into());
    }
    for (ordinal, &value) in child.output.columns.iter().enumerate() {
        work.flush()?;
        let actual =
            input
                .layout
                .value_binding_at(ordinal)
                .ok_or(UnpivotLoweringError::Invalid(
                    "Unpivot child has no complete value binding",
                ))?;
        if !actual.exactly_equals_observed::<UnpivotLoweringError>(
            value_type(fragment, value)?.typed_binding(),
            || work.step().map_err(Into::into),
        )? {
            return Err(
                (UnpivotLoweringError::Invalid("Unpivot complete value types differ")).into(),
            );
        }
        work.flush()?;
        work.step()?;
    }
    let mut outputs = BTreeMap::new();
    for (ordinal, &value) in node.output.columns.iter().enumerate() {
        let old = outputs.insert(value, ordinal);
        work.step()?;
        if old.is_some() {
            return Err(
                (UnpivotLoweringError::Invalid("duplicate Unpivot output occurrence")).into(),
            );
        }
    }
    let output_slot = |value| {
        outputs
            .get(&value)
            .map(|&ordinal| planned_slots[ordinal])
            .ok_or(UnpivotLoweringError::Invalid("missing Unpivot output slot"))
    };
    let mut passthrough_columns = Vec::new();
    let mut source_fields = BTreeMap::new();
    let mut slot_ordinals = BTreeMap::new();
    for (ordinal, &slot) in input.layout.slots().iter().enumerate() {
        slot_ordinals.insert(slot, ordinal);
        work.step()?;
    }
    for &(source, output) in &spec.passthrough {
        let input_slot_id = source_slot(input.sources, source, work)?;
        let output_slot_id = output_slot(output)?;
        work.step()?;
        exact_type(
            value_type(fragment, source)?,
            value_type(fragment, output)?,
            work,
        )?;
        let ordinal = slot_ordinals.get(&input_slot_id).copied();
        work.step()?;
        let ordinal = ordinal.ok_or(UnpivotLoweringError::Invalid(
            "missing Unpivot passthrough field",
        ))?;
        // The complete child header is borrowed; only an exact result label
        // may replace its diagnostic name during output materialization.
        source_fields.insert(output, &input.layout.schema().fields()[ordinal]);
        passthrough_columns.push(UnpivotPassthrough {
            input_slot_id,
            output_slot_id,
        });
        work.step()?;
    }
    let value_output_slot_id = output_slot(spec.value_output)?;
    work.step()?;
    let mut literal_output_slot_ids = Vec::new();
    for &output in &spec.literal_outputs {
        literal_output_slot_ids.push(output_slot(output)?);
        work.step()?;
    }
    let mut value_mappings = Vec::new();
    for mapping in &spec.mappings {
        let input_value_slot_id = source_slot(input.sources, mapping.input, work)?;
        if mapping.constants.len() != spec.literal_outputs.len() {
            return Err((UnpivotLoweringError::Invalid("Unpivot constant width differs")).into());
        }
        // Nullability is authored as an OR across mappings, so each source
        // keeps its complete domain and may only widen to that output root.
        work.flush()?;
        let source_type = value_type(fragment, mapping.input)?;
        let output_type = value_type(fragment, spec.value_output)?;
        if source_type.nullable() && !output_type.nullable() {
            return Err((UnpivotLoweringError::Invalid(
                "Unpivot mapping narrows source nullability",
            ))
            .into());
        }
        if !source_type
            .same_physical_binding_ignoring_root_nullability_observed(output_type, || {
                work.step().map_err(UnpivotLoweringError::from)
            })?
        {
            return Err(
                (UnpivotLoweringError::Invalid("Unpivot complete value types differ")).into(),
            );
        }
        work.flush()?;
        let mut constants = Vec::new();
        for (constant, &output) in mapping.constants.iter().zip(&spec.literal_outputs) {
            let lowered = match constant {
                SourceConstant::Scalar(id) => {
                    let definition =
                        fragment
                            .expressions()
                            .get(*id)
                            .ok_or(UnpivotLoweringError::Invalid(
                                "missing Unpivot scalar constant",
                            ))?;
                    let valid = matches!(definition.kind, ExprKind::Constant(_));
                    let expr_id = input.expressions.get(id).copied();
                    work.step()?;
                    if !valid {
                        return Err((UnpivotLoweringError::Invalid(
                            "Unpivot scalar source is not a static constant",
                        ))
                        .into());
                    }
                    work.flush()?;
                    let output_type = value_type(fragment, output)?;
                    if definition.ty.nullable() && !output_type.nullable() {
                        return Err((UnpivotLoweringError::Invalid(
                            "Unpivot constant narrows source nullability",
                        ))
                        .into());
                    }
                    if !definition
                        .ty
                        .same_physical_binding_ignoring_root_nullability_observed(
                            output_type,
                            || work.step().map_err(UnpivotLoweringError::from),
                        )?
                    {
                        return Err((UnpivotLoweringError::Invalid(
                            "Unpivot constant complete value types differ",
                        ))
                        .into());
                    }
                    work.flush()?;
                    UnpivotConstant::Scalar {
                        expr_id: expr_id.ok_or(UnpivotLoweringError::Invalid(
                            "missing lowered Unpivot constant",
                        ))?,
                        nullable: definition.ty.nullable(),
                    }
                }
                SourceConstant::Int32List(reference) => {
                    let value = package
                        .constants()
                        .resolve_source_observed(*reference, work)?;
                    special_source_domain(value.value_type(), value_type(fragment, output)?, work)?;
                    work.flush()?;
                    let selected =
                        value.int32_list_observed(CompilePhase::LowerProgram, work.control())?;
                    work.flush()?;
                    let selected = selected.ok_or(UnpivotLoweringError::Invalid(
                        "Unpivot Int32List source is NULL",
                    ))?;
                    let mut copied = reserve_collection::<i32>(selected.len(), work)?;
                    for index in 0..selected.len() {
                        let item = selected.item_observed(index, work)?;
                        let item = item.ok_or(UnpivotLoweringError::Invalid(
                            "Unpivot Int32List item is NULL",
                        ))?;
                        copied.push(item);
                        work.step()?;
                    }
                    UnpivotConstant::Int32List(copied)
                }
                SourceConstant::Utf8Map(reference) => {
                    let value = package
                        .constants()
                        .resolve_source_observed(*reference, work)?;
                    special_source_domain(value.value_type(), value_type(fragment, output)?, work)?;
                    work.flush()?;
                    let selected =
                        value.utf8_map_observed(CompilePhase::LowerProgram, work.control())?;
                    work.flush()?;
                    let selected = selected.ok_or(UnpivotLoweringError::Invalid(
                        "Unpivot Utf8Map source is NULL",
                    ))?;
                    let mut copied =
                        reserve_collection::<(Arc<str>, Arc<str>)>(selected.len(), work)?;
                    for index in 0..selected.len() {
                        let (key, value) = selected.item_observed(index, work)?;
                        let key = key
                            .ok_or(UnpivotLoweringError::Invalid("Unpivot Utf8Map key is NULL"))?;
                        let value = value.ok_or(UnpivotLoweringError::Invalid(
                            "Unpivot Utf8Map value is NULL",
                        ))?;
                        copied.push((copy_text(key, work)?, copy_text(value, work)?));
                        work.step()?;
                    }
                    UnpivotConstant::Utf8Map(copied)
                }
            };
            constants.push(lowered);
            work.step()?;
        }
        value_mappings.push(UnpivotMapping {
            input_value_slot_id,
            constants,
        });
        work.step()?;
    }
    let mut result_names = package
        .result()
        .filter(|r| r.output.columns.len() == node.output.columns.len());
    if let Some(result) = result_names {
        for (actual, expected) in node.output.columns.iter().zip(&result.output.columns) {
            let same = actual == expected;
            work.step()?;
            if !same {
                result_names = None;
                break;
            }
        }
    }
    let mut typed_layout = crate::typed_layout::TypedLayoutBuilder::new_with_creation(
        planned_slots.len(),
        work,
        creation,
    )?;
    for (ordinal, &value) in node.output.columns.iter().enumerate() {
        let name = result_names
            .and_then(|r| r.fields.get(ordinal))
            .map(|f| f.alias.as_deref().unwrap_or(&f.name));
        work.flush()?;
        let name =
            match name.or_else(|| source_fields.get(&value).map(|field| field.name().as_str())) {
                Some(name) => crate::typed_layout::copy_name_with_creation(name, work, creation)?,
                None => crate::typed_layout::local_name_with_creation(
                    "local",
                    local.index(),
                    ordinal,
                    work,
                    creation,
                )?,
            };
        typed_layout.push_with_creation(value_type(fragment, value)?, name, work, creation)?;
        work.step()?;
    }
    let max_output_rows = usize::try_from(spec.max_output_rows)
        .map_err(|_| UnpivotLoweringError::Invalid("Unpivot row bound exceeds host range"))?;
    let max_output_bytes = usize::try_from(spec.max_output_bytes)
        .map_err(|_| UnpivotLoweringError::Invalid("Unpivot byte bound exceeds host range"))?;
    if max_output_rows == 0 || max_output_bytes == 0 {
        return Err((UnpivotLoweringError::Invalid("Unpivot bounds are zero")).into());
    }
    work.flush()?;
    let layout = typed_layout.finish_with_creation(planned_slots, work, creation)?;
    Ok((
        ProgramNodeKind::Unpivot {
            input: input.node,
            passthrough_columns,
            value_output_slot_id,
            literal_output_slot_ids,
            value_mappings,
            max_output_rows,
            max_output_bytes,
        },
        layout,
    ))
}
/// A sealed package has checked the collection-specific source profile. Keep
/// its actual source type intact while proving the individual output edge;
/// the physical owner has already checked the complete mapping nullable OR.
fn special_source_domain(
    source: &FunctionValueType,
    output: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), UnpivotLoweringError> {
    let nonnullable = !source.nullable();
    work.step()?;
    if !nonnullable {
        return Err(UnpivotLoweringError::Invalid(
            "Unpivot special collection source must be nonnullable",
        ));
    }
    if !source.same_value_domain_observed::<UnpivotLoweringError>(output, || {
        work.step().map_err(Into::into)
    })? {
        return Err(UnpivotLoweringError::Invalid(
            "Unpivot special collection source/output domains differ",
        ));
    }
    Ok(())
}

fn reserve_collection<T>(
    len: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<T>, UnpivotLoweringError> {
    std::alloc::Layout::array::<T>(len)
        .map_err(|_| UnpivotLoweringError::Control(CompileControlError::ResourceExhausted))?;
    let mut output = Vec::new();
    work.flush()?;
    let reserved = output.try_reserve_exact(len);
    reservation_exit(reserved, work)?;
    Ok(output)
}

fn copy_text(
    source: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<str>, UnpivotLoweringError> {
    std::alloc::Layout::array::<u8>(source.len())
        .map_err(|_| UnpivotLoweringError::Control(CompileControlError::ResourceExhausted))?;
    let mut copied = String::new();
    work.flush()?;
    let reserved = copied.try_reserve_exact(source.len());
    reservation_exit(reserved, work)?;
    for character in source.chars() {
        copied.push(character);
        work.step()?;
    }
    work.flush()?;
    let copied = Arc::from(copied);
    work.flush()?;
    Ok(copied)
}

fn reservation_exit(
    reserved: Result<(), std::collections::TryReserveError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), UnpivotLoweringError> {
    reserved.map_err(|_| UnpivotLoweringError::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Control {
        observations: Mutex<Vec<(CompilePhase, u32)>>,
        refuse: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut observations = self.observations.lock().unwrap();
            let index = observations.len();
            observations.push((phase, units));
            match self.refuse.clone() {
                Some((at, cause)) if index == at => Err(cause),
                _ => Ok(()),
            }
        }
    }

    #[test]
    fn unpivot_text_reservation_failure_is_primary_resource_and_skips_post_refusal_observation() {
        let control = Control {
            refuse: Some((1, CompileControlError::Cancelled)),
            ..Default::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        let mut allocation = Vec::<u8>::new();
        let refused = allocation.try_reserve_exact(usize::MAX);
        assert!(refused.is_err());
        assert!(matches!(
            reservation_exit(refused, &mut work),
            Err(UnpivotLoweringError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(
            *control.observations.lock().unwrap(),
            [(CompilePhase::LowerProgram, 0)]
        );
        assert_eq!(allocation.capacity(), 0);
        assert!(matches!(
            reserve_collection::<i32>(usize::MAX, &mut work),
            Err(UnpivotLoweringError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(
            *control.observations.lock().unwrap(),
            [(CompilePhase::LowerProgram, 0)]
        );
        // A successful opaque reservation still observes its exit and may be
        // refused there; the failed reservation never reaches that callback.
        assert!(matches!(
            reservation_exit(allocation.try_reserve_exact(0), &mut work),
            Err(UnpivotLoweringError::Control(
                CompileControlError::Cancelled
            ))
        ));
    }

    #[test]
    fn unpivot_actual_unicode_text_copy_observes_each_quantum_and_keeps_original_control_prefix() {
        let source = "\u{03bb}".repeat(768);
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        let copied = copy_text(&source, &mut work).unwrap();
        assert_eq!(copied.as_ref(), source);
        let trace = control.observations.into_inner().unwrap();
        assert_eq!(trace.iter().filter(|(_, units)| *units == 256).count(), 3);
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 1..trace.len() {
                let refused = Control {
                    refuse: Some((at, cause.clone())),
                    ..Default::default()
                };
                let mut work =
                    CompileCheckpoints::try_new(&refused, CompilePhase::LowerProgram).unwrap();
                assert!(
                    matches!(copy_text(&source, &mut work), Err(UnpivotLoweringError::Control(actual)) if actual == cause)
                );
                assert_eq!(*refused.observations.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
