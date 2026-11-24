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

//! Typed projection of the flat v2 control component. This operates on DTOs
//! after resource-safe carrier admission; it is not a Prost first-allocation
//! preflight or the complete fragment-package codec.

use novarocks_expression_contract::{ExprId, ExprNode};
use novarocks_proto_models::physical_control_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, EvaluationDemand,
    EvaluationRegionId, ExpressionControlFlow, ExpressionControlFlowError, ExpressionEffectContext,
    ExpressionEvaluationRegion, ExpressionInvocation, ExpressionUseId, GuardKind,
    MAX_CONTROL_DEFINITIONS, MAX_CONTROL_USE_REFERENCES, PureCompileControl, RegionGuard,
};
use std::{alloc::Layout, fmt};
mod resources;
use novarocks_type_contract::{
    ControlOwnedResourceFacts, ControlResourceError, control_resource_add, control_resource_mul,
    expression_control_flow_edge_resource_facts, expression_control_flow_header_resource_facts,
};
pub use resources::{ControlProjectionFacts, ControlProjectionLimits};
use resources::{Model, unbounded};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlCodecError {
    Control(CompileControlError),
    InvalidShape(&'static str),
    Flow(ExpressionControlFlowError),
    Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
}
impl fmt::Display for ControlCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::InvalidShape(message) => f.write_str(message),
            Self::Flow(error) => error.fmt(f),
            Self::Resource(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ControlCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Flow(source) => Some(source),
            Self::Resource(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ControlCodecError {}
impl From<CompileControlError> for ControlCodecError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ExpressionControlFlowError> for ControlCodecError {
    fn from(error: ExpressionControlFlowError) -> Self {
        match error {
            ExpressionControlFlowError::Control(error) => Self::Control(error),
            error => Self::Flow(error),
        }
    }
}
impl From<ControlResourceError> for ControlCodecError {
    fn from(e: ControlResourceError) -> Self {
        match e {
            ControlResourceError::Control(c) => Self::Control(c),
            ControlResourceError::SourceModel(m) => Self::InvalidShape(m),
        }
    }
}
// The original facade observes only its original mapper loops. Parent-owned
// admission additionally brackets allocations in the same mapper body.
fn reserve<T>(
    v: &mut Vec<T>,
    n: usize,
    observed: bool,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), ControlCodecError> {
    if observed {
        w.flush()?;
    }
    v.try_reserve_exact(n)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    if observed {
        w.step()?;
        w.flush()?;
    }
    Ok(())
}
struct UnitDefinitions<'a>(&'a [ExprNode]);
impl novarocks_type_contract::ExpressionDefinitionMembership<ExprId> for UnitDefinitions<'_> {
    fn definition_count(&self) -> usize {
        self.0.len()
    }
    fn contains_definition(&self, id: ExprId) -> bool {
        self.0
            .get(id.get() as usize)
            .is_some_and(|node| node.id == id)
    }
}

fn required(value: Option<u32>, message: &'static str) -> Result<u32, ControlCodecError> {
    value.ok_or(ControlCodecError::InvalidShape(message))
}
pub(crate) fn encode_demand(demand: EvaluationDemand) -> i32 {
    match demand {
        EvaluationDemand::Value => wire::EvaluationDemand::Value as i32,
        EvaluationDemand::TruthOnly => wire::EvaluationDemand::TruthOnly as i32,
    }
}
pub(crate) fn decode_demand(demand: i32) -> Result<EvaluationDemand, ControlCodecError> {
    match wire::EvaluationDemand::try_from(demand) {
        Ok(wire::EvaluationDemand::Value) => Ok(EvaluationDemand::Value),
        Ok(wire::EvaluationDemand::TruthOnly) => Ok(EvaluationDemand::TruthOnly),
        _ => Err(ControlCodecError::InvalidShape(
            "unknown or unspecified evaluation demand",
        )),
    }
}
fn encode_shape(shape: ControlShape) -> wire::ControlShape {
    use wire::control_shape::Kind;
    let simple = match shape {
        ControlShape::Eager => Some(wire::SimpleControl::Eager),
        ControlShape::TypeOnly => Some(wire::SimpleControl::TypeOnly),
        ControlShape::NoArguments => Some(wire::SimpleControl::NoArguments),
        ControlShape::LambdaBody => Some(wire::SimpleControl::LambdaBody),
        ControlShape::Conjunction => Some(wire::SimpleControl::Conjunction),
        ControlShape::Disjunction => Some(wire::SimpleControl::Disjunction),
        ControlShape::If => Some(wire::SimpleControl::If),
        ControlShape::Coalesce => Some(wire::SimpleControl::Coalesce),
        ControlShape::Try => Some(wire::SimpleControl::Try),
        ControlShape::OrderedRegexp => Some(wire::SimpleControl::OrderedRegexp),
        ControlShape::NullIf
        | ControlShape::Case { .. }
        | ControlShape::HigherOrder { .. }
        | ControlShape::TemporalSource(_)
        | ControlShape::Between { .. }
        | ControlShape::Membership { .. } => None,
    };
    let kind = match (simple, shape) {
        (Some(simple), _) => Kind::Simple(simple as i32),
        (
            None,
            ControlShape::Case {
                simple,
                arms,
                has_else,
            },
        ) => Kind::CaseControl(wire::CaseControl {
            simple,
            arms,
            has_else,
        }),
        (
            None,
            ControlShape::HigherOrder {
                body_ordinal,
                body_demand,
            },
        ) => Kind::HigherOrder(wire::HigherOrderControl {
            body_ordinal,
            body_demand: encode_demand(body_demand),
        }),
        (None, ControlShape::Between { negated }) => Kind::BetweenNegated(negated),
        (None, ControlShape::Membership { negated }) => Kind::MembershipNegated(negated),
        (None, ControlShape::NullIf) => Kind::NullIf(wire::NullIfControl {
            equality_function: novarocks_type_contract::OperatorFunction::Equal
                .function_id_text()
                .into(),
        }),
        (None, ControlShape::TemporalSource(shape)) => {
            Kind::TemporalSource(encode_temporal_shape(shape))
        }
        (None, _) => unreachable!("all fixed control shapes have simple encoding"),
    };
    wire::ControlShape { kind: Some(kind) }
}
fn decode_shape(shape: &wire::ControlShape) -> Result<ControlShape, ControlCodecError> {
    use wire::control_shape::Kind;
    match shape
        .kind
        .as_ref()
        .ok_or(ControlCodecError::InvalidShape("control kind is missing"))?
    {
        Kind::Simple(simple) => match wire::SimpleControl::try_from(*simple) {
            Ok(wire::SimpleControl::Eager) => Ok(ControlShape::Eager),
            Ok(wire::SimpleControl::TypeOnly) => Ok(ControlShape::TypeOnly),
            Ok(wire::SimpleControl::NoArguments) => Ok(ControlShape::NoArguments),
            Ok(wire::SimpleControl::LambdaBody) => Ok(ControlShape::LambdaBody),
            Ok(wire::SimpleControl::Conjunction) => Ok(ControlShape::Conjunction),
            Ok(wire::SimpleControl::Disjunction) => Ok(ControlShape::Disjunction),
            Ok(wire::SimpleControl::If) => Ok(ControlShape::If),
            Ok(wire::SimpleControl::Coalesce) => Ok(ControlShape::Coalesce),
            Ok(wire::SimpleControl::Try) => Ok(ControlShape::Try),
            Ok(wire::SimpleControl::OrderedRegexp) => Ok(ControlShape::OrderedRegexp),
            _ => Err(ControlCodecError::InvalidShape(
                "unknown or unspecified control shape",
            )),
        },
        Kind::BetweenNegated(negated) => Ok(ControlShape::Between { negated: *negated }),
        Kind::MembershipNegated(negated) => Ok(ControlShape::Membership { negated: *negated }),
        Kind::NullIf(plan) => {
            if plan.equality_function
                != novarocks_type_contract::OperatorFunction::Equal.function_id_text()
            {
                return Err(ControlCodecError::InvalidShape(
                    "NULLIF requires its exact equality identity",
                ));
            }
            Ok(ControlShape::NullIf)
        }
        Kind::TemporalSource(value) => {
            decode_temporal_shape(*value).map(ControlShape::TemporalSource)
        }
        Kind::CaseControl(case) => Ok(ControlShape::Case {
            simple: case.simple,
            arms: case.arms,
            has_else: case.has_else,
        }),
        Kind::HigherOrder(higher) => Ok(ControlShape::HigherOrder {
            body_ordinal: higher.body_ordinal,
            body_demand: decode_demand(higher.body_demand)?,
        }),
    }
}
fn encode_guard(guard: RegionGuard) -> wire::RegionGuard {
    use wire::region_guard::Kind;
    let kind = match guard.kind {
        GuardKind::IfThen => Kind::Simple(wire::SimpleGuard::IfThen as i32),
        GuardKind::IfElse => Kind::Simple(wire::SimpleGuard::IfElse as i32),
        GuardKind::CaseElse => Kind::Simple(wire::SimpleGuard::CaseElse as i32),
        GuardKind::TryBody => Kind::Simple(wire::SimpleGuard::TryBody as i32),
        GuardKind::NullIfAfterLeft => Kind::Simple(wire::SimpleGuard::NullIfAfterLeft as i32),
        GuardKind::LambdaInvocation => Kind::Simple(wire::SimpleGuard::LambdaInvocation as i32),
        GuardKind::CoalesceAfterNull { ordinal } => Kind::CoalesceAfterNullOrdinal(ordinal),
        GuardKind::CaseWhen { arm } => Kind::CaseWhenArm(arm),
        GuardKind::CaseThen { arm } => Kind::CaseThenArm(arm),
        GuardKind::BetweenAfterSource { ordinal } => Kind::BetweenAfterSourceOrdinal(ordinal),
        GuardKind::MembershipAfterSource { ordinal } => Kind::MembershipAfterSourceOrdinal(ordinal),
        GuardKind::OrderedRegexpAfterArgument { ordinal } => {
            Kind::OrderedRegexpAfterArgumentOrdinal(ordinal)
        }
        GuardKind::TemporalAfterSource { ordinal } => Kind::TemporalAfterSourceOrdinal(ordinal),
        GuardKind::TemporalInvocationNull => {
            Kind::Simple(wire::SimpleGuard::TemporalInvocationNull as i32)
        }
    };
    wire::RegionGuard {
        owner_use_id: Some(guard.owner.get()),
        kind: Some(kind),
    }
}
fn decode_guard(guard: &wire::RegionGuard) -> Result<RegionGuard, ControlCodecError> {
    use wire::region_guard::Kind;
    let owner = ExpressionUseId::new(required(guard.owner_use_id, "guard owner is missing")?);
    let kind = match guard
        .kind
        .as_ref()
        .ok_or(ControlCodecError::InvalidShape("guard kind is missing"))?
    {
        Kind::Simple(simple) => match wire::SimpleGuard::try_from(*simple) {
            Ok(wire::SimpleGuard::IfThen) => GuardKind::IfThen,
            Ok(wire::SimpleGuard::IfElse) => GuardKind::IfElse,
            Ok(wire::SimpleGuard::CaseElse) => GuardKind::CaseElse,
            Ok(wire::SimpleGuard::TryBody) => GuardKind::TryBody,
            Ok(wire::SimpleGuard::NullIfAfterLeft) => GuardKind::NullIfAfterLeft,
            Ok(wire::SimpleGuard::LambdaInvocation) => GuardKind::LambdaInvocation,
            Ok(wire::SimpleGuard::TemporalInvocationNull) => GuardKind::TemporalInvocationNull,
            _ => {
                return Err(ControlCodecError::InvalidShape(
                    "unknown or unspecified guard kind",
                ));
            }
        },
        Kind::CoalesceAfterNullOrdinal(ordinal) => {
            GuardKind::CoalesceAfterNull { ordinal: *ordinal }
        }
        Kind::CaseWhenArm(arm) => GuardKind::CaseWhen { arm: *arm },
        Kind::CaseThenArm(arm) => GuardKind::CaseThen { arm: *arm },
        Kind::BetweenAfterSourceOrdinal(ordinal) => {
            GuardKind::BetweenAfterSource { ordinal: *ordinal }
        }
        Kind::MembershipAfterSourceOrdinal(ordinal) => {
            GuardKind::MembershipAfterSource { ordinal: *ordinal }
        }
        Kind::OrderedRegexpAfterArgumentOrdinal(ordinal) => {
            GuardKind::OrderedRegexpAfterArgument { ordinal: *ordinal }
        }
        Kind::TemporalAfterSourceOrdinal(ordinal) => {
            GuardKind::TemporalAfterSource { ordinal: *ordinal }
        }
    };
    Ok(RegionGuard { owner, kind })
}

/// Canonical table order follows the checked sparse IDs and typed root sites.
/// Definition sharing retains distinct invocation occurrences and demands.
pub fn encode_expression_control(
    roots: &ExpressionControlFlow<ExprId>,
    control: &dyn PureCompileControl,
) -> Result<wire::ExpressionControl, ControlCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let mut model = encode_model(roots, 0, unbounded())?;
    let encoded = encode_in(roots, &mut model, &mut |_| Ok(()), false, &mut work);
    if let Err(ControlCodecError::Control(c)) = &encoded {
        return Err(c.clone().into());
    }
    work.finish()?;
    Ok(encoded?.0)
}

/// Explicit FE pure author using its caller's original checkpoint stream.
/// This is the original writer body, without a constructor grant or footer.
pub fn encode_expression_control_pure_in(
    roots: &ExpressionControlFlow<ExprId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::ExpressionControl, ControlCodecError> {
    let mut model = encode_model(roots, 0, unbounded())?;
    encode_in(roots, &mut model, &mut |_| Ok(()), false, work).map(|encoded| encoded.0)
}

/// Complete control projection from its actual immutable flow source, the
/// caller's work scope and one union source invoice. No footer.
pub fn encode_expression_control_observed(
    flow: &ExpressionControlFlow<ExprId>,
    source_retained_bytes: usize,
    limits: ControlProjectionLimits,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::ExpressionControl, ControlProjectionFacts), ControlCodecError> {
    // Borrow the actual immutable control owner. Unit root correspondence is
    // validated by its own constructor, never reconstructed as a second bank.
    let mut model = encode_model(flow, source_retained_bytes, limits)?;
    model.gate(admit)?;
    model.floor(flow_source_floor(flow)?)?;
    encode_in(flow, &mut model, admit, true, work)
}
/// Necessary original backing only: stored public map headers, key/value
/// payloads and actual owned argument/root slices. Private BTree capacity,
/// Arc holder metadata and allocator growth are deliberately not guessed.
fn flow_source_floor(flow: &ExpressionControlFlow<ExprId>) -> Result<usize, ControlCodecError> {
    let regions = flow.regions();
    let uses = flow.uses();
    let edges = flow.use_reference_count().checked_sub(uses.len()).ok_or(
        ControlCodecError::InvalidShape("cached flow references omit actual uses"),
    )?;
    let mut floor = std::mem::size_of_val(flow);
    floor = control_resource_add(floor, std::mem::size_of_val(regions))?;
    floor = control_resource_add(
        floor,
        control_resource_mul(
            regions.len(),
            control_resource_add(
                std::mem::size_of::<EvaluationRegionId>(),
                std::mem::size_of::<ExpressionEvaluationRegion>(),
            )?,
        )?,
    )?;
    floor = control_resource_add(floor, std::mem::size_of_val(uses))?;
    floor = control_resource_add(
        floor,
        control_resource_mul(
            uses.len(),
            control_resource_add(
                std::mem::size_of::<ExpressionUseId>(),
                std::mem::size_of::<ExpressionInvocation<ExprId>>(),
            )?,
        )?,
    )?;
    floor = control_resource_add(
        floor,
        control_resource_mul(edges, std::mem::size_of::<ExpressionUseId>())?,
    )?;
    floor = control_resource_add(floor, std::mem::size_of_val(flow.root_use_ids()))?;
    Ok(floor)
}

fn encode_model(
    roots: &ExpressionControlFlow<ExprId>,
    b: usize,
    limits: ControlProjectionLimits,
) -> Result<Model, ControlCodecError> {
    let f = roots;
    let mut m = Model::new(f.regions().len(), f.use_reference_count(), b, limits);
    m.resources
        .buffer::<wire::EvaluationRegion>(f.regions().len(), 1)?;
    m.resources
        .buffer::<wire::ExpressionUse>(f.uses().len(), 1)?;
    // One non-empty argument buffer per use is the maximum request count. The
    // total payload is the cached actual reference count, not a source-B guess.
    let edges = f.use_reference_count() - f.uses().len();
    m.resources.buffer::<u32>(edges, 1)?;
    if edges > 0 {
        m.resources.merge(ControlOwnedResourceFacts {
            allocation_requests_upper_bound: f.uses().len().saturating_sub(1),
            ..Default::default()
        })?;
    }
    m.resources.work(control_resource_mul(
        control_resource_add(
            control_resource_add(f.regions().len(), f.use_reference_count())?,
            0,
        )?,
        16,
    )?)?;
    Ok(m)
}
fn encode_in(
    roots: &ExpressionControlFlow<ExprId>,
    m: &mut Model,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    observed: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::ExpressionControl, ControlProjectionFacts), ControlCodecError> {
    m.gate(admit)?;
    let flow = roots;
    let mut regions = Vec::new();
    reserve(&mut regions, flow.regions().len(), observed, work)?;
    for region in flow.regions().values() {
        regions.push(wire::EvaluationRegion {
            id: region.id.get(),
            parent_region_id: region.parent.map(EvaluationRegionId::get),
            guard: region.guard.map(encode_guard),
        });
        work.step()?;
    }
    let mut uses = Vec::new();
    reserve(&mut uses, flow.uses().len(), observed, work)?;
    for invocation in flow.uses().values() {
        if observed {
            work.flush()?;
        }
        let mut arguments = Vec::new();
        reserve(&mut arguments, invocation.arguments.len(), observed, work)?;
        for argument in &invocation.arguments {
            arguments.push(argument.get());
            work.step()?;
        }
        uses.push(wire::ExpressionUse {
            id: invocation.context.use_id.get(),
            definition_id: Some(invocation.definition.get()),
            region_id: Some(invocation.context.region.get()),
            demand: encode_demand(invocation.context.demand),
            control: Some(encode_shape(invocation.control)),
            argument_use_ids: arguments,
        });
        work.step()?;
    }

    let facts = m.gate(admit)?;
    Ok((wire::ExpressionControl { regions, uses }, facts))
}

/// Original convenience facade. The new observed component below borrows a
/// parent scope and never invokes another owner through this facade.
pub fn decode_expression_control(
    definitions: &[ExprNode],
    input: &wire::ExpressionControl,
    control: &dyn PureCompileControl,
) -> Result<ExpressionControlFlow<ExprId>, ControlCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_expression_control_core(
        definitions,
        input,
        None,
        unbounded(),
        &mut |_| Ok(()),
        &mut work,
    )
    .map(|(flow, _)| flow);
    if matches!(result, Err(ControlCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn boxed<T>(
    v: Vec<T>,
    observed: bool,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Box<[T]>, ControlCodecError> {
    if observed {
        w.flush()?;
    }
    let v = v.into_boxed_slice();
    if observed {
        w.step()?;
    }
    if observed {
        w.flush()?;
    }
    Ok(v)
}
fn original_decode_header(input: &wire::ExpressionControl) -> Result<(), ControlCodecError> {
    if input.regions.len() > MAX_CONTROL_DEFINITIONS
        || input.uses.len() > MAX_CONTROL_USE_REFERENCES
    {
        return Err(ExpressionControlFlowError::TooManyItems.into());
    }
    Ok(())
}
fn decode_model(
    input: &wire::ExpressionControl,
    b: usize,
    limits: ControlProjectionLimits,
) -> Result<Model, ControlCodecError> {
    decode_parts_model(input, b, limits)
}
fn decode_parts_model(
    input: &wire::ExpressionControl,
    b: usize,
    limits: ControlProjectionLimits,
) -> Result<Model, ControlCodecError> {
    decode_parts_model_core(input, b, Some(limits))
}
fn decode_parts_model_core(
    input: &wire::ExpressionControl,
    b: usize,
    limits: Option<ControlProjectionLimits>,
) -> Result<Model, ControlCodecError> {
    let mut m = match limits {
        Some(limits) => Model::new(input.regions.len(), input.uses.len(), b, limits),
        None => Model::input_derived(input.regions.len(), input.uses.len(), b),
    };
    m.resources
        .buffer::<ExpressionEvaluationRegion>(input.regions.len(), 1)?;
    m.resources
        .buffer::<ExpressionInvocation<ExprId>>(input.uses.len(), 1)?;
    m.resources.work(control_resource_mul(
        control_resource_add(
            control_resource_add(input.regions.len(), input.uses.len())?,
            0,
        )?,
        16,
    )?)?;
    let mut future = novarocks_type_contract::ControlResourceCounter::default();
    future.merge(expression_control_flow_header_resource_facts::<ExprId>(
        input.regions.len(),
        input.uses.len(),
    )?)?;

    m.future = future.facts();
    Ok(m)
}
pub fn decode_expression_control_observed(
    definitions: &[ExprNode],
    input: &wire::ExpressionControl,
    source_retained_bytes: usize,
    limits: ControlProjectionLimits,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ExpressionControlFlow<ExprId>, ControlProjectionFacts), ControlCodecError> {
    decode_expression_control_core(
        definitions,
        input,
        Some(source_retained_bytes),
        limits,
        admit,
        work,
    )
}
fn decode_expression_control_core(
    definitions: &[ExprNode],
    input: &wire::ExpressionControl,
    source: Option<usize>,
    limits: ControlProjectionLimits,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ExpressionControlFlow<ExprId>, ControlProjectionFacts), ControlCodecError> {
    let mut model = decode_model(input, source.unwrap_or(0), limits)?;
    if source.is_none() {
        model.infer_input_source();
    }
    model.gate(admit)?;
    let floor = control_source_floor(input)?;
    model.receiving_floor(floor)?;
    let (regions, uses) = project_decode_in(
        input,
        &mut model,
        admit,
        Some(floor),
        source.is_some(),
        work,
    )?;
    model.planned_child = model.future;
    model.future = ControlOwnedResourceFacts::default();
    let mut child = ControlOwnedResourceFacts::default();
    let mut ordinary = None;
    let flow = ExpressionControlFlow::try_new_in(
        regions,
        uses,
        &UnitDefinitions(definitions),
        &mut |facts| {
            child = *facts;
            match model.gate_with(*facts, admit) {
                Ok(_) => Ok(()),
                Err(error) => {
                    let cause = match &error {
                        ControlCodecError::Control(cause) => cause.clone(),
                        _ => CompileControlError::from_domain(error.clone()),
                    };
                    ordinary = Some(error);
                    Err(cause)
                }
            }
        },
        work,
    );
    if let Some(error) = ordinary {
        return Err(error);
    }
    let flow = flow?;
    model.resources.merge(ControlOwnedResourceFacts {
        allocation_requests_upper_bound: child
            .allocation_requests_upper_bound
            .max(model.planned_child.allocation_requests_upper_bound),
        allocation_request_bytes_upper_bound: child
            .allocation_request_bytes_upper_bound
            .max(model.planned_child.allocation_request_bytes_upper_bound),
        cumulative_work_upper_bound: child
            .cumulative_work_upper_bound
            .max(model.planned_child.cumulative_work_upper_bound),
    })?;
    model.planned_child = ControlOwnedResourceFacts::default();
    let facts = model.gate(admit)?;
    Ok((flow, facts))
}
/// Construct the same checked flow using actual receiving storage. The raw
/// argument Boxes and each independently cloneable graph Arc use the same
/// retained scope; dropping a containing Unit cannot retire their budget.
pub(crate) fn decode_expression_control_with_creator_observed_in<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    definitions: &[ExprNode],
    input: &wire::ExpressionControl,
    source: usize,
    limits: ControlProjectionLimits,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (ExpressionControlFlow<ExprId>, ControlProjectionFacts),
    crate::host_projection_v2::ProjectionFailure<ControlCodecError, H>,
> {
    use crate::host_projection_v2::ProjectionFailure as Failure;
    use novarocks_type_contract::{
        ExpressionControlCreationFailure as C, ValueSchemaScopeBeginFailure as B,
    };
    let mut model = decode_model(input, source, limits)?;
    model.gate(admit)?;
    let floor = control_source_floor(input)?;
    model.floor(floor)?;
    let map = |error| match error {
        C::Control(error) => Failure::Codec(ControlCodecError::Control(error)),
        C::Creator(error) => Failure::Host(error),
        C::Resource(error) => Failure::Codec(ControlCodecError::Resource(error)),
        C::Owner(error) => Failure::Codec(ControlCodecError::from(error)),
    };
    let flow = if let Some(factory) = factory {
        work.flush().map_err(ControlCodecError::from)?;
        let mut temporary = factory
            .begin_scope_observed(&mut |layout| {
                model
                    .resources
                    .layout(layout, 1)
                    .map_err(ControlCodecError::from)?;
                model.gate(admit)?;
                Ok::<_, Failure<ControlCodecError, H>>(())
            })
            .map_err(|error| match error {
                B::Observer(error) => error,
                B::Creator(error) => Failure::Host(error),
            })?;
        work.flush().map_err(ControlCodecError::from)?;
        let mut retained = factory
            .begin_scope_observed(&mut |layout| {
                model
                    .resources
                    .layout(layout, 1)
                    .map_err(ControlCodecError::from)?;
                model.gate(admit)?;
                Ok::<_, Failure<ControlCodecError, H>>(())
            })
            .map_err(|error| match error {
                B::Observer(error) => error,
                B::Creator(error) => Failure::Host(error),
            })?;
        let (regions, uses) =
            project_funded_control_parts(input, &mut model, admit, floor, &mut retained, work)?;
        model.gate(admit)?;
        ExpressionControlFlow::try_new_with_creator_in(
            regions,
            uses,
            &UnitDefinitions(definitions),
            &mut temporary,
            &mut retained,
            work,
        )
        .map_err(map)?
    } else {
        let (regions, uses) =
            project_funded_control_parts(input, &mut model, admit, floor, creation, work)?;
        model.gate(admit)?;
        ExpressionControlFlow::try_new_with_single_creator_in(
            regions,
            uses,
            &UnitDefinitions(definitions),
            creation,
            work,
        )
        .map_err(map)?
    };
    // The original shared header/edge visitors quoted this same constructor,
    // not another graph. Commit that contribution once after it succeeds.
    model
        .resources
        .merge(model.future)
        .map_err(ControlCodecError::from)?;
    model.future = ControlOwnedResourceFacts::default();
    let facts = model.gate(admit)?;
    Ok((flow, facts))
}

fn project_funded_control_parts<H>(
    input: &wire::ExpressionControl,
    model: &mut Model,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    floor: usize,
    creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    ControlProjectionParts,
    crate::host_projection_v2::ProjectionFailure<ControlCodecError, H>,
> {
    struct Funded<'a, H, A> {
        original: CreatedControlProjection<'a, H>,
        admit: &'a mut A,
    }
    impl<H, A: FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>>
        ControlPartsProjection for Funded<'_, H, A>
    {
        type Error = crate::host_projection_v2::ProjectionFailure<ControlCodecError, H>;
        fn gate(&mut self, model: &Model) -> Result<(), Self::Error> {
            model.gate(self.admit)?;
            Ok(())
        }
        fn reserve<T>(
            &mut self,
            value: &mut Vec<T>,
            count: usize,
            observed: bool,
            work: &mut CompileCheckpoints<'_>,
        ) -> Result<(), Self::Error> {
            self.original.reserve(value, count, observed, work)
        }
        fn boxed<T>(
            &mut self,
            value: Vec<T>,
            observed: bool,
            work: &mut CompileCheckpoints<'_>,
        ) -> Result<Box<[T]>, Self::Error> {
            self.original.boxed(value, observed, work)
        }
    }
    project_decode_core(
        input,
        model,
        &mut Funded {
            original: CreatedControlProjection { creation },
            admit,
        },
        Some(floor),
        true,
        work,
    )
}

type ControlProjectionParts = (
    Vec<ExpressionEvaluationRegion>,
    Vec<ExpressionInvocation<ExprId>>,
);
fn project_decode_in(
    input: &wire::ExpressionControl,
    m: &mut Model,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    floor: Option<usize>,
    observed: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ControlProjectionParts, ControlCodecError> {
    project_decode_core(
        input,
        m,
        &mut OriginalControlProjection { admit },
        floor,
        observed,
        work,
    )
}
trait ControlPartsProjection {
    type Error: From<ControlCodecError>;
    fn gate(&mut self, model: &Model) -> Result<(), Self::Error>;
    fn reserve<T>(
        &mut self,
        value: &mut Vec<T>,
        count: usize,
        observed: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Self::Error>;
    fn boxed<T>(
        &mut self,
        value: Vec<T>,
        observed: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Box<[T]>, Self::Error>;
}
struct OriginalControlProjection<'a, A> {
    admit: &'a mut A,
}
impl<A: FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>> ControlPartsProjection
    for OriginalControlProjection<'_, A>
{
    type Error = ControlCodecError;
    fn gate(&mut self, m: &Model) -> Result<(), Self::Error> {
        m.gate(self.admit).map(|_| ())
    }
    fn reserve<T>(
        &mut self,
        v: &mut Vec<T>,
        n: usize,
        o: bool,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Self::Error> {
        reserve(v, n, o, w)
    }
    fn boxed<T>(
        &mut self,
        v: Vec<T>,
        o: bool,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Box<[T]>, Self::Error> {
        boxed(v, o, w)
    }
}
struct CreatedControlProjection<'a, H> {
    creation: &'a mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
}
impl<H> ControlPartsProjection for CreatedControlProjection<'_, H> {
    type Error = crate::host_projection_v2::ProjectionFailure<ControlCodecError, H>;
    fn gate(&mut self, m: &Model) -> Result<(), Self::Error> {
        let facts = m.check_with(ControlOwnedResourceFacts::default())?;
        let _ = facts;
        Ok(())
    }
    fn reserve<T>(
        &mut self,
        v: &mut Vec<T>,
        n: usize,
        o: bool,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Self::Error> {
        w.flush().map_err(ControlCodecError::from)?;
        let layout = Layout::array::<T>(n)
            .map_err(|_| ControlCodecError::Control(CompileControlError::ResourceExhausted))?;
        self.creation
            .request_layout(layout)
            .map_err(crate::host_projection_v2::ProjectionFailure::Host)?;
        reserve(v, n, o, w).map_err(Into::into)
    }
    fn boxed<T>(
        &mut self,
        v: Vec<T>,
        o: bool,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Box<[T]>, Self::Error> {
        w.flush().map_err(ControlCodecError::from)?;
        if v.capacity() != v.len() {
            let bytes = Layout::array::<T>(v.len())
                .map_err(|_| ControlCodecError::Control(CompileControlError::ResourceExhausted))?
                .size();
            self.creation
                .observe_opaque_upper_bound(bytes)
                .map_err(crate::host_projection_v2::ProjectionFailure::Host)?;
        }
        boxed(v, o, w).map_err(Into::into)
    }
}

/// Projected grammar facts, not a validated expression graph or root attachment.
/// Native must map these exact definitions and run its full ProgramControlFlow
/// and RootAttachment checks before publishing a LocalProgram.
pub type ExpressionControlParts = (
    Vec<ExpressionEvaluationRegion>,
    Vec<ExpressionInvocation<ExprId>>,
    novarocks_type_contract::ValueTypeStorageLifetime,
);

pub struct CreatedExpressionControlParts {
    regions: Vec<ExpressionEvaluationRegion>,
    uses: Vec<ExpressionInvocation<ExprId>>,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl CreatedExpressionControlParts {
    pub fn regions(&self) -> &[ExpressionEvaluationRegion] {
        &self.regions
    }
    pub fn uses(&self) -> &[ExpressionInvocation<ExprId>] {
        &self.uses
    }

    pub fn into_parts(self) -> ExpressionControlParts {
        (self.regions, self.uses, self.storage)
    }
}
/// Receive from this actual input's sole checked model/grammar. There is no
/// receiving-profile threshold or fabricated grant; the actual creator admits
/// each request and the same original checkpoint stream controls all work.
pub fn decode_expression_control_parts_input_derived_with_creator_in<H>(
    input: &wire::ExpressionControl,
    source_retained_bytes: usize,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (CreatedExpressionControlParts, ControlProjectionFacts),
    crate::host_projection_v2::ProjectionFailure<ControlCodecError, H>,
> {
    decode_expression_control_parts_core(input, source_retained_bytes, creation, work)
}
fn decode_expression_control_parts_core<H>(
    input: &wire::ExpressionControl,
    source_retained_bytes: usize,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (CreatedExpressionControlParts, ControlProjectionFacts),
    crate::host_projection_v2::ProjectionFailure<ControlCodecError, H>,
> {
    let mut model = decode_parts_model_core(input, source_retained_bytes, None)?;
    let storage = creation.storage_lifetime();
    let mut projection = CreatedControlProjection { creation };
    projection.gate(&model)?;
    let floor = control_source_floor(input)?;
    model.floor(floor)?;
    let (regions, uses) =
        project_decode_core(input, &mut model, &mut projection, Some(floor), true, work)?;
    // Graph/root constructor resources are admitted by the actual downstream
    // constructor. Parts do not allocate or claim those future owners.
    model.future = ControlOwnedResourceFacts::default();
    let facts = model.check_with(ControlOwnedResourceFacts::default())?;
    Ok((
        CreatedExpressionControlParts {
            regions,
            uses,
            storage,
        },
        facts,
    ))
}
fn control_source_floor(input: &wire::ExpressionControl) -> Result<usize, ControlCodecError> {
    let mut bytes = std::mem::size_of::<wire::ExpressionControl>();
    for next in [
        Layout::array::<wire::EvaluationRegion>(input.regions.capacity()),
        Layout::array::<wire::ExpressionUse>(input.uses.capacity()),
        Layout::array::<u32>(0),
    ] {
        bytes = control_resource_add(
            bytes,
            next.map_err(|_| CompileControlError::ResourceExhausted)?
                .size(),
        )?;
    }
    Ok(bytes)
}
fn project_decode_core<P: ControlPartsProjection>(
    input: &wire::ExpressionControl,
    m: &mut Model,
    projection: &mut P,
    mut source_floor: Option<usize>,
    observed: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ControlProjectionParts, P::Error> {
    projection.gate(m)?;
    (|| {
        original_decode_header(input)?;
        let mut references = input.uses.len();
        for invocation in &input.uses {
            references = references
                .checked_add(invocation.argument_use_ids.len())
                .ok_or(ControlCodecError::Flow(
                    ExpressionControlFlowError::TooManyItems,
                ))?;
            if references > MAX_CONTROL_USE_REFERENCES {
                return Err(
                    ControlCodecError::Flow(ExpressionControlFlowError::TooManyItems).into(),
                );
            }
            m.counts[1] = references;
            m.resources
                .buffer::<ExpressionUseId>(invocation.argument_use_ids.len(), 2)
                .map_err(ControlCodecError::from)?;
            m.resources
                .work(
                    control_resource_mul(invocation.argument_use_ids.len(), 16)
                        .map_err(ControlCodecError::from)?,
                )
                .map_err(ControlCodecError::from)?;
            let mut future = novarocks_type_contract::ControlResourceCounter::default();
            future.merge(m.future).map_err(ControlCodecError::from)?;
            future
                .merge(
                    expression_control_flow_edge_resource_facts(
                        invocation.argument_use_ids.len(),
                        input.regions.len(),
                        input.uses.len(),
                    )
                    .map_err(ControlCodecError::from)?,
                )
                .map_err(ControlCodecError::from)?;
            m.future = future.facts();
            projection.gate(m)?;
            if let Some(floor) = &mut source_floor {
                // Each actual argument Vec owns disjoint backing in this DTO.
                // This is a necessary floor, not a private-capacity estimate.
                let backing = Layout::array::<u32>(invocation.argument_use_ids.capacity())
                    .map_err(|_| {
                        ControlCodecError::Control(CompileControlError::ResourceExhausted)
                    })?
                    .size();
                *floor = control_resource_add(*floor, backing).map_err(ControlCodecError::from)?;
                m.receiving_floor(*floor)?;
            }
            work.step().map_err(ControlCodecError::from)?;
        }
        let mut regions = Vec::new();
        projection.reserve(&mut regions, input.regions.len(), observed, work)?;
        for region in &input.regions {
            regions.push(ExpressionEvaluationRegion {
                id: EvaluationRegionId::new(region.id),
                parent: region.parent_region_id.map(EvaluationRegionId::new),
                guard: region.guard.as_ref().map(decode_guard).transpose()?,
            });
            work.step().map_err(ControlCodecError::from)?;
        }
        let mut uses = Vec::new();
        projection.reserve(&mut uses, input.uses.len(), observed, work)?;
        for invocation in &input.uses {
            let mut arguments = Vec::new();
            projection.reserve(
                &mut arguments,
                invocation.argument_use_ids.len(),
                observed,
                work,
            )?;
            for argument in &invocation.argument_use_ids {
                arguments.push(ExpressionUseId::new(*argument));
                work.step().map_err(ControlCodecError::from)?;
            }
            uses.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(invocation.id),
                    region: EvaluationRegionId::new(required(
                        invocation.region_id,
                        "use region is missing",
                    )?),
                    demand: decode_demand(invocation.demand)?,
                },
                definition: ExprId::new(required(
                    invocation.definition_id,
                    "use definition is missing",
                )?),
                control: decode_shape(
                    invocation
                        .control
                        .as_ref()
                        .ok_or(ControlCodecError::InvalidShape("use control is missing"))?,
                )?,
                arguments: projection.boxed(arguments, observed, work)?,
            });
            work.step().map_err(ControlCodecError::from)?;
        }
        Ok((regions, uses))
    })()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "physical_control_v2/owned_tests.rs"]
mod owned_tests;

pub(crate) fn encode_temporal_shape(shape: novarocks_type_contract::TemporalSourceShape) -> i32 {
    use novarocks_type_contract::TemporalSourceShape as S;
    use wire::TemporalSourceShape as W;
    (match shape {
        S::FormatOrdinary => W::FormatOrdinary,
        S::FormatUtf8Override => W::FormatUtf8Override,
        S::SecondsDirect => W::SecondsDirect,
        S::SecondsCastString => W::SecondsCastString,
        S::SecondsCastOther => W::SecondsCastOther,
        S::SecondsRoundtrip => W::SecondsRoundtrip,
    }) as i32
}
pub(crate) fn decode_temporal_shape(
    value: i32,
) -> Result<novarocks_type_contract::TemporalSourceShape, ControlCodecError> {
    use novarocks_type_contract::TemporalSourceShape as S;
    use wire::TemporalSourceShape as W;
    Ok(match W::try_from(value) {
        Ok(W::FormatOrdinary) => S::FormatOrdinary,
        Ok(W::FormatUtf8Override) => S::FormatUtf8Override,
        Ok(W::SecondsDirect) => S::SecondsDirect,
        Ok(W::SecondsCastString) => S::SecondsCastString,
        Ok(W::SecondsCastOther) => S::SecondsCastOther,
        Ok(W::SecondsRoundtrip) => S::SecondsRoundtrip,
        _ => {
            return Err(ControlCodecError::InvalidShape(
                "unknown or missing temporal source shape",
            ));
        }
    })
}

#[cfg(test)]
#[path = "physical_control_v2/created_parts_tests.rs"]
mod created_parts_tests;
