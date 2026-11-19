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

//! Fragment-level constant consumers and checked backing projection.
#[cfg(test)]
use novarocks_constant_contract::ConstantPool;
use novarocks_constant_contract::{
    ConstantError, ConstantPoolId, ConstantPools, ConstantReference, ConstantTableError,
};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, FunctionValueType};
#[cfg(test)]
use std::collections::BTreeMap;
use std::fmt;

/// Preserve every actual pool address used by expressions, original call
/// requests or either relational/writer-statistics Unpivot. Identity is sparse
/// namespace identity, even when two keys loan the same checked backing.
pub fn project_fragment_constants_observed(
    pools: &ConstantPools,
    fragment: &crate::Fragment,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConstantPools, ConstantReferenceError> {
    let mut projected = ConstantPools::empty();
    visit_typed_constant_references_observed(fragment, work, |reference, expected, work| {
        let value = pools.resolve_observed(reference, expected, work)?;
        work.flush()?;
        if !projected.entries().contains_key(&reference.pool) {
            projected.insert(reference.pool, value.pool().clone())?;
        }
        work.step()?;
        work.flush()?;
        Ok(())
    })?;
    visit_unpivot_constants_observed(fragment, work, |_, constant, _, work| {
        if let Some(reference) = collection_reference(constant) {
            let value = pools.resolve_source_observed(reference, work)?;
            work.flush()?;
            if !projected.entries().contains_key(&reference.pool) {
                projected.insert(reference.pool, value.pool().clone())?;
            }
            work.step()?;
            work.flush()?;
        }
        Ok(())
    })?;
    visit_window_constant_references_in(fragment, work, |reference, work| {
        let value = pools.resolve_source_observed(reference, work)?;
        work.flush()?;
        if !projected.entries().contains_key(&reference.pool) {
            projected.insert(reference.pool, value.pool().clone())?;
        }
        work.step()?;
        work.flush()?;
        Ok(())
    })?;
    Ok(projected)
}

/// Visit the actual typed expression and original request references. Every
/// source definition, request and ordered argument is observed, including
/// nonconstant values and Lambdas. Collection consumer policy remains in the
/// original Unpivot visitor rather than becoming a second typed-reference DSL.
/// The caller owns admission, entry and completion on this same meter.
fn visit_typed_constant_references_observed(
    fragment: &crate::Fragment,
    work: &mut CompileCheckpoints<'_>,
    mut visit: impl FnMut(
        ConstantReference,
        &FunctionValueType,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), ConstantReferenceError>,
) -> Result<(), ConstantReferenceError> {
    for node in fragment.nodes().values() {
        let unit = node.expressions.as_ref();
        work.step()?;
        if let Some(unit) = unit {
            for expression in unit.definitions() {
                work.step()?;
                if let crate::ExprKind::Constant(reference) = expression.kind {
                    visit(reference, &expression.ty, work)?;
                }
            }
        }
    }
    let mut request = |request: &crate::PhysicalCallRequest,
                       work: &mut CompileCheckpoints<'_>|
     -> Result<(), ConstantReferenceError> {
        work.step()?;
        for argument in &request.arguments {
            work.step()?;
            if let novarocks_function_contract::FunctionArgument::Value {
                value_type,
                constant: Some(reference),
            } = argument
            {
                visit(*reference, value_type, work)?;
            }
        }
        Ok(())
    };
    for original in fragment.call_requests().entries().values() {
        request(original, work)?;
    }
    for node in fragment.nodes().values() {
        let unit = node.expressions.as_ref();
        work.step()?;
        if let Some(unit) = unit {
            for original in unit.requests().values() {
                request(original, work)?;
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstantReferenceError {
    Control(CompileControlError),
    Constant(ConstantError),
    Structure(crate::ValidationErrors),
    Table(ConstantTableError),
    UnusedPools,
    InvalidConsumer(&'static str),
}
impl From<novarocks_type_contract::ValueTypeError> for ConstantReferenceError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Constant(ConstantError::Type(error))
    }
}
impl From<CompileControlError> for ConstantReferenceError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ConstantError> for ConstantReferenceError {
    fn from(error: ConstantError) -> Self {
        match error {
            ConstantError::Control(error) => Self::Control(error),
            error => Self::Constant(error),
        }
    }
}
impl From<ConstantTableError> for ConstantReferenceError {
    fn from(error: ConstantTableError) -> Self {
        match error {
            ConstantTableError::Control(cause) => Self::Control(cause),
            ConstantTableError::Constant(cause) => Self::from(cause),
            other => Self::Table(other),
        }
    }
}
impl fmt::Display for ConstantReferenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(formatter),
            Self::Constant(error) => error.fmt(formatter),
            Self::Structure(error) => error.fmt(formatter),
            Self::Table(error) => error.fmt(formatter),
            Self::UnusedPools => {
                formatter.write_str("constant pool table contains unused definitions")
            }
            Self::InvalidConsumer(message) => formatter.write_str(message),
        }
    }
}
impl std::error::Error for ConstantReferenceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Constant(source) => Some(source),
            Self::Structure(source) => Some(source),
            Self::Table(source) => Some(source),
            Self::UnusedPools | Self::InvalidConsumer(_) => None,
        }
    }
}

pub(crate) fn collection_reference(constant: &crate::UnpivotConstant) -> Option<ConstantReference> {
    match constant {
        crate::UnpivotConstant::Int32List(reference)
        | crate::UnpivotConstant::Utf8Map(reference) => Some(*reference),
        crate::UnpivotConstant::Scalar(_) => None,
    }
}

/// Visit all source definitions, including noncollection constants and nodes.
/// Do not hide work by filtering these source occurrences before this owner.
pub(crate) fn visit_unpivot_constants_observed(
    fragment: &crate::Fragment,
    work: &mut CompileCheckpoints<'_>,
    mut visit: impl FnMut(
        crate::NodeId,
        &crate::UnpivotConstant,
        Option<&FunctionValueType>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), ConstantReferenceError>,
) -> Result<(), ConstantReferenceError> {
    for node in fragment.nodes().values() {
        work.step()?;
        match &node.kind {
            crate::NodeKind::Unpivot { spec } => {
                for mapping in &spec.mappings {
                    work.step()?;
                    for (index, constant) in mapping.constants.iter().enumerate() {
                        work.step()?;
                        let output = spec
                            .literal_outputs
                            .get(index)
                            .and_then(|id| fragment.values().get(id))
                            .map(|v| &v.ty);
                        visit(node.id, constant, output, work)?;
                    }
                }
            }
            crate::NodeKind::TableFinish(spec) => {
                if let Some(spec) = &spec.grouped_unpivot {
                    for mapping in &spec.mappings {
                        work.step()?;
                        for (index, constant) in mapping.constants.iter().enumerate() {
                            work.step()?;
                            let output = spec
                                .literal_outputs
                                .get(index)
                                .and_then(|id| fragment.values().get(id))
                                .map(|v| &v.ty);
                            visit(node.id, constant, output, work)?;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct UnpivotCollectionUsage {
    pub(crate) items: usize,
    pub(crate) payload_bytes: u64,
}

fn resource_refusal() -> ConstantReferenceError {
    ConstantReferenceError::Control(CompileControlError::ResourceExhausted)
}

fn strictly_increasing(
    left: &str,
    right: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ConstantReferenceError> {
    for (&left, &right) in left.as_bytes().iter().zip(right.as_bytes()) {
        let order = left.cmp(&right);
        work.step()?;
        if order != std::cmp::Ordering::Equal {
            return Ok(order == std::cmp::Ordering::Less);
        }
    }
    let ordered = left.len() < right.len();
    work.step()?;
    Ok(ordered)
}

/// Observation policy for the original constant consumer and resource bodies.
/// Caller mode borrows the existing meter; it does not admit opaque scratch.
#[derive(Clone, Copy)]
pub(crate) enum ConstantValidationMode {
    Plain,
    Caller,
}

/// Sole special-consumer policy. Generic checked Map/List readers remain
/// permissive; this consumer retains the original exact special source rules.
pub(crate) fn unpivot_collection_usage_observed(
    pools: &ConstantPools,
    constant: &crate::UnpivotConstant,
    output: Option<&FunctionValueType>,
    max_items: usize,
    max_bytes: u64,
    work: &mut CompileCheckpoints<'_>,
) -> Result<UnpivotCollectionUsage, ConstantReferenceError> {
    unpivot_collection_usage_core(
        pools,
        constant,
        output,
        max_items,
        max_bytes,
        ConstantValidationMode::Plain,
        work,
    )
}

pub(crate) fn unpivot_collection_usage_in(
    pools: &ConstantPools,
    constant: &crate::UnpivotConstant,
    output: Option<&FunctionValueType>,
    max_items: usize,
    max_bytes: u64,
    work: &mut CompileCheckpoints<'_>,
) -> Result<UnpivotCollectionUsage, ConstantReferenceError> {
    unpivot_collection_usage_core(
        pools,
        constant,
        output,
        max_items,
        max_bytes,
        ConstantValidationMode::Caller,
        work,
    )
}

fn unpivot_collection_usage_core(
    pools: &ConstantPools,
    constant: &crate::UnpivotConstant,
    output: Option<&FunctionValueType>,
    max_items: usize,
    max_bytes: u64,
    mode: ConstantValidationMode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<UnpivotCollectionUsage, ConstantReferenceError> {
    let reference = collection_reference(constant).ok_or(
        ConstantReferenceError::InvalidConsumer("Unpivot collection source is scalar"),
    )?;
    let source = pools.resolve_source_observed(reference, work)?;
    let ty = source.value_type();
    let valid = !ty.nullable()
        && crate::validation::unpivot_collection_binding_matches(constant, ty)
        // Actual encoding/source annotations are checked against the retained
        // source Field; the semantic binding cannot authorize an encoding.
        && crate::validation::unpivot_collection_carrier_matches(constant, source.field().data_type());
    work.step()?;
    if !valid {
        return Err(ConstantReferenceError::InvalidConsumer(
            "Unpivot collection source differs from its exact non-null special type",
        ));
    }
    let output = output.ok_or(ConstantReferenceError::InvalidConsumer(
        "Unpivot literal output is missing",
    ))?;
    if !ty
        .same_value_domain_observed(output, || work.step().map_err(ConstantReferenceError::from))?
        || !novarocks_type_contract::arrow_data_types_exact_observed(
            ty.data_type(),
            output.data_type(),
            || work.step().map_err(ConstantReferenceError::from),
        )?
    {
        return Err(ConstantReferenceError::InvalidConsumer(
            "Unpivot collection source differs from its complete output domain",
        ));
    }
    match constant {
        crate::UnpivotConstant::Int32List(_) => {
            let view =
                match mode {
                    ConstantValidationMode::Plain => {
                        work.flush()?;
                        let view = source
                            .int32_list_observed(
                                novarocks_type_contract::CompilePhase::Validate,
                                work.control(),
                            )?
                            .ok_or(ConstantReferenceError::InvalidConsumer(
                                "Unpivot collection root is NULL",
                            ))?;
                        work.flush()?;
                        view
                    }
                    ConstantValidationMode::Caller => source.int32_list_in(work)?.ok_or(
                        ConstantReferenceError::InvalidConsumer("Unpivot collection root is NULL"),
                    )?,
                };
            let items = view.len();
            let bytes = u64::try_from(items)
                .map_err(|_| resource_refusal())?
                .checked_mul(4)
                .ok_or_else(resource_refusal)?;
            let admitted = items <= max_items && bytes <= max_bytes;
            if !admitted {
                return Err(resource_refusal());
            }
            work.step()?;
            for index in 0..items {
                if view.item_observed(index, work)?.is_none() {
                    return Err(ConstantReferenceError::InvalidConsumer(
                        "Unpivot Int32List item is NULL",
                    ));
                }
            }
            Ok(UnpivotCollectionUsage {
                items,
                payload_bytes: bytes,
            })
        }
        crate::UnpivotConstant::Utf8Map(_) => {
            let view = match mode {
                ConstantValidationMode::Plain => {
                    work.flush()?;
                    let view = source
                        .utf8_map_observed(
                            novarocks_type_contract::CompilePhase::Validate,
                            work.control(),
                        )?
                        .ok_or(ConstantReferenceError::InvalidConsumer(
                            "Unpivot collection root is NULL",
                        ))?;
                    work.flush()?;
                    view
                }
                ConstantValidationMode::Caller => {
                    source
                        .utf8_map_in(work)?
                        .ok_or(ConstantReferenceError::InvalidConsumer(
                            "Unpivot collection root is NULL",
                        ))?
                }
            };
            let items = view.len();
            let admitted = items <= max_items;
            if !admitted {
                return Err(resource_refusal());
            }
            work.step()?;
            let mut bytes = 0_u64;
            let mut previous = None;
            for index in 0..items {
                let (key, value) = view.item_observed(index, work)?;
                let (Some(key), Some(value)) = (key, value) else {
                    return Err(ConstantReferenceError::InvalidConsumer(
                        "Unpivot Utf8Map key/value is NULL",
                    ));
                };
                let nonempty = !key.is_empty();
                work.step()?;
                if !nonempty {
                    return Err(ConstantReferenceError::InvalidConsumer(
                        "unpivot map keys must be non-empty and strictly increasing",
                    ));
                }
                if let Some(previous) = previous
                    && !strictly_increasing(previous, key, work)?
                {
                    return Err(ConstantReferenceError::InvalidConsumer(
                        "unpivot map keys must be non-empty and strictly increasing",
                    ));
                }
                previous = Some(key);
                bytes = bytes
                    .checked_add(u64::try_from(key.len()).map_err(|_| resource_refusal())?)
                    .and_then(|b| b.checked_add(u64::try_from(value.len()).ok()?))
                    .ok_or_else(resource_refusal)?;
                let admitted = bytes <= max_bytes;
                if !admitted {
                    return Err(resource_refusal());
                }
                work.step()?;
            }
            Ok(UnpivotCollectionUsage {
                items,
                payload_bytes: bytes,
            })
        }
        crate::UnpivotConstant::Scalar(_) => unreachable!("collection source matched above"),
    }
}

/// Source-address/type validation and actual scalar consumer facts. A mutable
/// fragment definition is still a construction input; this check is mandatory
/// before publishing its enclosing plan or local package.
pub(crate) fn validate_fragment_constants_observed(
    fragment: &crate::Fragment,
    pools: &ConstantPools,
    require_closed: bool,
    limits: crate::PlanLimits,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(), ConstantReferenceError> {
    use novarocks_type_contract::CompilePhase;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = validate_fragment_constants_core(
        fragment,
        pools,
        require_closed,
        limits,
        ConstantValidationMode::Plain,
        &mut work,
    );
    if matches!(result, Err(ConstantReferenceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

/// The caller owns entry and completion of the original mandatory validation.
/// This reuses every source/consumer/closure law; scratch admission is separate.
pub(crate) fn validate_fragment_constants_in(
    fragment: &crate::Fragment,
    pools: &ConstantPools,
    require_closed: bool,
    limits: crate::PlanLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantReferenceError> {
    validate_fragment_constants_core(
        fragment,
        pools,
        require_closed,
        limits,
        ConstantValidationMode::Caller,
        work,
    )
}

fn validate_fragment_constants_core(
    fragment: &crate::Fragment,
    pools: &ConstantPools,
    require_closed: bool,
    limits: crate::PlanLimits,
    mode: ConstantValidationMode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantReferenceError> {
    // A whole plan has one table spanning peer fragments; its enclosing
    // plan gate accounts that table. Only a closed local package owns it
    // within this fragment envelope.
    if require_closed {
        let mut errors = crate::validation::ValidationContext::new();
        let mut usage = crate::resource::CutResourcePreflight::new();
        usage.add_fragment(fragment, &mut errors);
        usage.add_constants_observed(pools, work)?;
        match mode {
            ConstantValidationMode::Plain => {
                usage.add_unpivot_sources_observed(fragment, pools, limits, work)?
            }
            ConstantValidationMode::Caller => {
                usage.add_unpivot_sources_in(fragment, pools, limits, work)?
            }
        }
        usage.validate("package.constants.resources", &mut errors);
        if !errors.is_empty() {
            return Err(ConstantReferenceError::Structure(
                crate::ValidationErrors::from_collector(errors),
            ));
        }
    }
    let mut used = std::collections::BTreeSet::new();
    visit_typed_constant_references_observed(fragment, work, |reference, expected, work| {
        pools.resolve_observed(reference, expected, work)?;
        if require_closed {
            work.flush()?;
            used.insert(reference.pool);
            work.step()?;
            work.flush()?;
        }
        Ok(())
    })?;
    for node in fragment.nodes().values() {
        work.step()?;
        if let crate::NodeKind::Window(spec) = &node.kind {
            for call in &spec.calls {
                work.step()?;
                if let Some(frame) = &call.frame {
                    validate_window_constants(pools, frame, work)?;
                }
            }
        }
    }
    visit_window_constant_references_in(fragment, work, |reference, work| {
        let value = pools.resolve_source_observed(reference, work)?;
        if require_closed {
            work.flush()?;
            used.insert(reference.pool);
            work.step()?;
            work.flush()?;
        }
        drop(value);
        Ok(())
    })?;
    validate_unpivot_constants(fragment, pools, limits, &mut used, mode, work)?;
    if require_closed && used.len() != pools.entries().len() {
        return Err(ConstantReferenceError::UnusedPools);
    }
    Ok(())
}

/// Read the original window-bound constant through its admitted source type.
/// Checked references retain the existing exact non-null I64/U64 rules;
/// The caller owns entry and the ordinary/success footer. Originating control
/// or resource failures return without an additional observation.
pub fn window_offset_observed(
    pools: &ConstantPools,
    reference: ConstantReference,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<u64>, ConstantReferenceError> {
    work.step()?;
    let value = pools.resolve_source_observed(reference, work)?;
    let ty = value.value_type();
    if ty.nullable() || ty.logical_type() != novarocks_type_contract::ValueLogicalType::Physical {
        return Err(ConstantReferenceError::InvalidConsumer(
            "window constant offset must be an exact non-null physical integer",
        ));
    }
    let offset = match ty.data_type() {
        arrow_schema::DataType::UInt64 => value.try_u64()?,
        arrow_schema::DataType::Int64 => {
            value.try_i64()?.and_then(|value| u64::try_from(value).ok())
        }
        _ => None,
    };
    if offset.is_none() || offset == Some(0) {
        return Err(ConstantReferenceError::InvalidConsumer(
            "window constant offset must be a non-negative non-null exact I64/U64; zero requires CURRENT ROW",
        ));
    }
    Ok(offset)
}
fn validate_window_constants(
    pools: &ConstantPools,
    frame: &crate::WindowFrame,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantReferenceError> {
    let mut offsets = [None, None];
    for (index, bound) in [&frame.start, &frame.end].into_iter().enumerate() {
        work.step()?;
        if let crate::WindowBound::Preceding(id) | crate::WindowBound::Following(id) = bound {
            offsets[index] = window_offset_observed(pools, *id, work)?;
        }
    }
    let ordered = match (&frame.start, &frame.end, offsets) {
        (
            crate::WindowBound::Preceding(_),
            crate::WindowBound::Preceding(_),
            [Some(start), Some(end)],
        ) => start >= end,
        (
            crate::WindowBound::Following(_),
            crate::WindowBound::Following(_),
            [Some(start), Some(end)],
        ) => start <= end,
        _ => true,
    };
    if !ordered {
        return Err(ConstantReferenceError::InvalidConsumer(
            "window frame start follows its end after comparing exact constant offsets",
        ));
    }
    Ok(())
}
fn validate_unpivot_constants(
    fragment: &crate::Fragment,
    pools: &ConstantPools,
    limits: crate::PlanLimits,
    used: &mut std::collections::BTreeSet<ConstantPoolId>,
    mode: ConstantValidationMode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantReferenceError> {
    let mut previous_node = None;
    let mut bytes = 0_u64;
    let mut items = 0_usize;
    visit_unpivot_constants_observed(fragment, work, |node, constant, output, work| {
        if previous_node != Some(node) {
            previous_node = Some(node);
            bytes = 0;
            items = 0;
        }
        let selected = match constant {
            crate::UnpivotConstant::Scalar(id) => {
                let Some(expression) = fragment.root_expression(node, *id) else {
                    return Ok(());
                };
                if let crate::ExprKind::Constant(reference) = expression.kind {
                    let value = pools.resolve_observed(reference, &expression.ty, work)?;
                    match mode {
                        ConstantValidationMode::Plain => {
                            work.flush()?;
                            let selected = value.selected_payload_bytes_observed(
                                novarocks_type_contract::CompilePhase::Validate,
                                work.control(),
                            )?;
                            work.flush()?;
                            selected
                        }
                        ConstantValidationMode::Caller => value.selected_payload_bytes_in(work)?,
                    }
                } else {
                    0
                }
            }
            crate::UnpivotConstant::Int32List(reference)
            | crate::UnpivotConstant::Utf8Map(reference) => {
                let usage = unpivot_collection_usage_core(
                    pools,
                    constant,
                    output,
                    limits
                        .unpivot_collection_items
                        .checked_sub(items)
                        .ok_or_else(resource_refusal)?,
                    (crate::MAX_UNPIVOT_LITERAL_BYTES as u64)
                        .checked_sub(bytes)
                        .ok_or_else(resource_refusal)?,
                    mode,
                    work,
                )?;
                items = items
                    .checked_add(usage.items)
                    .ok_or_else(resource_refusal)?;
                used.insert(reference.pool);
                work.step()?;
                usage.payload_bytes
            }
        };
        bytes = bytes.checked_add(selected).ok_or_else(resource_refusal)?;
        if bytes > crate::MAX_UNPIVOT_LITERAL_BYTES as u64 {
            return Err(resource_refusal());
        }
        Ok(())
    })
}

pub(crate) fn validate_plan_constants_observed(
    plan: &crate::PhysicalPlan,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(), ConstantReferenceError> {
    use novarocks_type_contract::CompilePhase;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = validate_plan_constants_core(plan, ConstantValidationMode::Plain, &mut work);
    if matches!(result, Err(ConstantReferenceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
pub(crate) fn validate_plan_constants_in(
    plan: &crate::PhysicalPlan,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantReferenceError> {
    validate_plan_constants_core(plan, ConstantValidationMode::Caller, work)
}

fn validate_plan_constants_core(
    plan: &crate::PhysicalPlan,
    mode: ConstantValidationMode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantReferenceError> {
    let mut errors = crate::validation::ValidationContext::new();
    match mode {
        ConstantValidationMode::Plain => {
            crate::resource::validate_plan_resources_observed(plan, &mut errors, work)?
        }
        ConstantValidationMode::Caller => {
            crate::resource::validate_plan_resources_in(plan, &mut errors, work)?
        }
    }
    if !errors.is_empty() {
        return Err(ConstantReferenceError::Structure(
            crate::ValidationErrors::from_collector(errors),
        ));
    }
    let mut used = std::collections::BTreeSet::new();
    for fragment in plan.fragments().values() {
        work.step()?;
        match mode {
            ConstantValidationMode::Plain => {
                work.flush()?;
                validate_fragment_constants_observed(
                    fragment,
                    plan.constants(),
                    false,
                    crate::PlanLimits::FROZEN,
                    work.control(),
                )?;
                work.flush()?;
            }
            ConstantValidationMode::Caller => validate_fragment_constants_in(
                fragment,
                plan.constants(),
                false,
                crate::PlanLimits::FROZEN,
                work,
            )?,
        }
        visit_typed_constant_references_observed(fragment, work, |reference, _, work| {
            work.flush()?;
            used.insert(reference.pool);
            work.step()?;
            work.flush()?;
            Ok(())
        })?;
        visit_unpivot_constants_observed(fragment, work, |_, constant, _, work| {
            if let Some(reference) = collection_reference(constant) {
                used.insert(reference.pool);
                work.step()?;
            }
            Ok(())
        })?;
        visit_window_constant_references_in(fragment, work, |reference, work| {
            work.flush()?;
            used.insert(reference.pool);
            work.step()?;
            work.flush()?;
            Ok(())
        })?;
    }
    if used.len() != plan.constants().entries().len() {
        return Err(ConstantReferenceError::UnusedPools);
    }
    Ok(())
}

#[cfg(test)]
#[path = "constants/request_reference_tests.rs"]
mod request_reference_tests;
#[cfg(test)]
mod tests;

fn visit_window_constant_references_in(
    fragment: &crate::Fragment,
    work: &mut CompileCheckpoints<'_>,
    mut visit: impl FnMut(
        ConstantReference,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), ConstantReferenceError>,
) -> Result<(), ConstantReferenceError> {
    for node in fragment.nodes().values() {
        work.step()?;
        if let crate::NodeKind::Window(spec) = &node.kind {
            for call in &spec.calls {
                if let Some(frame) = &call.frame {
                    for bound in [&frame.start, &frame.end] {
                        work.step()?;
                        if let crate::WindowBound::Preceding(reference)
                        | crate::WindowBound::Following(reference) = bound
                        {
                            visit(*reference, work)?;
                        }
                    }
                }
                work.step()?;
            }
        }
    }
    Ok(())
}
