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

//! Complete frozen call DTO projection. This preserves occurrence facts;
//! it does not authenticate an installed owner or close parameter values.

use super::owned_resources::Projection;
use super::parameters::{decode_reference, encode_reference};
use super::{SemanticsCodecError, required_id};
use crate::physical_control_v2::{decode_demand, encode_demand};
use crate::physical_node_v2::{NodeProjectionFacts, NodeProjectionLimits};
use novarocks_physical_plan::{
    Fragment, FrozenFragmentCalls, FrozenPhysicalCall, MAX_PLAN_DERIVED_CUT_ITEMS, NodeId,
    PhysicalCallSite,
};
use novarocks_proto_models::{physical_control_v2 as control_wire, physical_semantics_v2 as wire};
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, DecimalOverflowPolicy,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionFailureBehavior,
    FunctionInstanceState, FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility,
    MAX_CONTROL_USE_REFERENCES, MAX_SEMANTIC_PARAMETERS, ObservableEffects, PureCompileControl,
};
use novarocks_type_contract::{CompileControlError, ControlOwnedResourceFacts};

type E = SemanticsCodecError;

pub(super) fn encode_context(context: &ExpressionEffectContext) -> wire::ExpressionEffectContext {
    wire::ExpressionEffectContext {
        use_id: Some(context.use_id.get()),
        region_id: Some(context.region.get()),
        demand: encode_demand(context.demand),
    }
}
pub(super) fn decode_context(
    input: &wire::ExpressionEffectContext,
) -> Result<ExpressionEffectContext, E> {
    Ok(ExpressionEffectContext {
        use_id: ExpressionUseId::new(required_id(input.use_id, "missing effect use ID")?),
        region: EvaluationRegionId::new(required_id(input.region_id, "missing effect region ID")?),
        demand: decode_demand(input.demand)?,
    })
}
pub(crate) fn encode_site(site: PhysicalCallSite) -> wire::CallSite {
    use wire::call_site::Kind;
    let node_call = |node: NodeId, call| wire::NodeCallSite {
        node_id: Some(node.get()),
        call,
    };
    wire::CallSite {
        kind: Some(match site {
            PhysicalCallSite::Window { node, call } => Kind::Window(node_call(node, call)),
            PhysicalCallSite::Aggregate { node, call } => Kind::Aggregate(node_call(node, call)),
            PhysicalCallSite::TopNState { node, call } => Kind::TopNState(node_call(node, call)),
            PhysicalCallSite::WriterPartial { node, call } => {
                Kind::WriterPartial(node_call(node, call))
            }
            PhysicalCallSite::WriterFinal { node, call } => {
                Kind::WriterFinal(node_call(node, call))
            }
            PhysicalCallSite::Table { node } => Kind::TableNodeId(node.get()),
        }),
    }
}
pub(crate) fn decode_site(input: &wire::CallSite) -> Result<PhysicalCallSite, E> {
    use wire::call_site::Kind;
    let node = |value: &wire::NodeCallSite| -> Result<NodeId, E> {
        Ok(NodeId::new(required_id(
            value.node_id,
            "missing call node ID",
        )?))
    };
    Ok(
        match input
            .kind
            .as_ref()
            .ok_or(E::InvalidShape("missing call site kind"))?
        {
            Kind::Window(value) => PhysicalCallSite::Window {
                node: node(value)?,
                call: value.call,
            },
            Kind::Aggregate(value) => PhysicalCallSite::Aggregate {
                node: node(value)?,
                call: value.call,
            },
            Kind::TopNState(value) => PhysicalCallSite::TopNState {
                node: node(value)?,
                call: value.call,
            },
            Kind::WriterPartial(value) => PhysicalCallSite::WriterPartial {
                node: node(value)?,
                call: value.call,
            },
            Kind::WriterFinal(value) => PhysicalCallSite::WriterFinal {
                node: node(value)?,
                call: value.call,
            },
            Kind::TableNodeId(id) => PhysicalCallSite::Table {
                node: NodeId::new(*id),
            },
        },
    )
}
fn encode_argument_control(value: ArgumentControl) -> wire::ArgumentControl {
    use wire::SimpleArgumentControl as W;
    use wire::argument_control::Kind;
    let kind = match value {
        ArgumentControl::HigherOrder {
            body_ordinal,
            body_demand,
        } => Kind::HigherOrder(control_wire::HigherOrderControl {
            body_ordinal,
            body_demand: encode_demand(body_demand),
        }),
        ArgumentControl::Eager => Kind::Simple(W::Eager as i32),
        ArgumentControl::TypeOnly => Kind::Simple(W::TypeOnly as i32),
        ArgumentControl::NoArguments => Kind::Simple(W::NoArguments as i32),
        ArgumentControl::If => Kind::Simple(W::If as i32),
        ArgumentControl::Coalesce => Kind::Simple(W::Coalesce as i32),
        ArgumentControl::Try => Kind::Simple(W::Try as i32),
        ArgumentControl::NullIf => Kind::Simple(W::NullIf as i32),
        ArgumentControl::OrderedRegexp => Kind::Simple(W::OrderedRegexp as i32),
        ArgumentControl::OrderedNull => Kind::Simple(W::OrderedNull as i32),
        ArgumentControl::CandidateGroup => Kind::Simple(W::CandidateGroup as i32),
        ArgumentControl::SimpleCase => Kind::Simple(W::SimpleCase as i32),
        ArgumentControl::SearchedCase => Kind::Simple(W::SearchedCase as i32),
        ArgumentControl::Aggregate => Kind::Simple(W::Aggregate as i32),
        ArgumentControl::Window => Kind::Simple(W::Window as i32),
        ArgumentControl::Table => Kind::Simple(W::Table as i32),
        ArgumentControl::TemporalSource(kind) => Kind::TemporalSource(match kind {
            novarocks_type_contract::TemporalSourceKind::TimeToSec => {
                wire::TemporalSourceKind::TimeToSec
            }
            novarocks_type_contract::TemporalSourceKind::TimeFormat => {
                wire::TemporalSourceKind::TimeFormat
            }
        } as i32),
    };
    wire::ArgumentControl { kind: Some(kind) }
}
fn decode_argument_control(input: &wire::ArgumentControl) -> Result<ArgumentControl, E> {
    use wire::SimpleArgumentControl as W;
    use wire::argument_control::Kind;
    Ok(
        match input
            .kind
            .as_ref()
            .ok_or(E::InvalidShape("missing argument control kind"))?
        {
            Kind::TemporalSource(value) => {
                ArgumentControl::TemporalSource(match wire::TemporalSourceKind::try_from(*value) {
                    Ok(wire::TemporalSourceKind::TimeToSec) => {
                        novarocks_type_contract::TemporalSourceKind::TimeToSec
                    }
                    Ok(wire::TemporalSourceKind::TimeFormat) => {
                        novarocks_type_contract::TemporalSourceKind::TimeFormat
                    }
                    _ => {
                        return Err(E::InvalidShape(
                            "unknown or missing temporal source owner kind",
                        ));
                    }
                })
            }
            Kind::HigherOrder(value) => ArgumentControl::HigherOrder {
                body_ordinal: value.body_ordinal,
                body_demand: decode_demand(value.body_demand)?,
            },
            Kind::Simple(value) => match W::try_from(*value) {
                Ok(W::Eager) => ArgumentControl::Eager,
                Ok(W::TypeOnly) => ArgumentControl::TypeOnly,
                Ok(W::NoArguments) => ArgumentControl::NoArguments,
                Ok(W::If) => ArgumentControl::If,
                Ok(W::Coalesce) => ArgumentControl::Coalesce,
                Ok(W::Try) => ArgumentControl::Try,
                Ok(W::NullIf) => ArgumentControl::NullIf,
                Ok(W::OrderedRegexp) => ArgumentControl::OrderedRegexp,
                Ok(W::OrderedNull) => ArgumentControl::OrderedNull,
                Ok(W::CandidateGroup) => ArgumentControl::CandidateGroup,
                Ok(W::SimpleCase) => ArgumentControl::SimpleCase,
                Ok(W::SearchedCase) => ArgumentControl::SearchedCase,
                Ok(W::Aggregate) => ArgumentControl::Aggregate,
                Ok(W::Window) => ArgumentControl::Window,
                Ok(W::Table) => ArgumentControl::Table,
                _ => return Err(E::InvalidShape("unknown or unspecified argument control")),
            },
        },
    )
}
fn encode_proof(value: CallProofScope) -> wire::ProofScope {
    use wire::proof_scope::Kind;
    wire::ProofScope {
        kind: Some(match value {
            CallProofScope::Unconditional => Kind::Unconditional(control_wire::Empty {}),
            CallProofScope::Region(id) => Kind::RegionId(id.get()),
            CallProofScope::Relation(id) => Kind::Relation(encode_relation_id(id)),
        }),
    }
}
fn decode_proof(input: &wire::ProofScope) -> Result<CallProofScope, E> {
    use wire::proof_scope::Kind;
    Ok(
        match input
            .kind
            .as_ref()
            .ok_or(E::InvalidShape("missing proof scope kind"))?
        {
            Kind::Unconditional(_) => CallProofScope::Unconditional,
            Kind::RegionId(id) => CallProofScope::Region(EvaluationRegionId::new(*id)),
            Kind::Relation(id) => CallProofScope::Relation(decode_relation_id(id)?),
        },
    )
}
fn encode_stability(value: FunctionVolatility) -> i32 {
    match value {
        FunctionVolatility::Immutable => wire::ValueStability::Immutable as i32,
        FunctionVolatility::Stable => wire::ValueStability::Stable as i32,
        FunctionVolatility::Volatile => wire::ValueStability::Volatile as i32,
    }
}
fn decode_stability(value: i32) -> Result<FunctionVolatility, E> {
    match wire::ValueStability::try_from(value) {
        Ok(wire::ValueStability::Immutable) => Ok(FunctionVolatility::Immutable),
        Ok(wire::ValueStability::Stable) => Ok(FunctionVolatility::Stable),
        Ok(wire::ValueStability::Volatile) => Ok(FunctionVolatility::Volatile),
        _ => Err(E::InvalidShape("unknown or unspecified stability")),
    }
}
fn encode_row_error(value: FunctionIntrinsicRowError) -> i32 {
    match value {
        FunctionIntrinsicRowError::NoRowError => wire::OwnRowError::NoRowError as i32,
        FunctionIntrinsicRowError::MayRaise => wire::OwnRowError::MayRaise as i32,
        FunctionIntrinsicRowError::NotRowEvaluated => wire::OwnRowError::NotRowEvaluated as i32,
    }
}
fn decode_row_error(value: i32) -> Result<FunctionIntrinsicRowError, E> {
    match wire::OwnRowError::try_from(value) {
        Ok(wire::OwnRowError::NoRowError) => Ok(FunctionIntrinsicRowError::NoRowError),
        Ok(wire::OwnRowError::MayRaise) => Ok(FunctionIntrinsicRowError::MayRaise),
        Ok(wire::OwnRowError::NotRowEvaluated) => Ok(FunctionIntrinsicRowError::NotRowEvaluated),
        _ => Err(E::InvalidShape("unknown or unspecified row_error")),
    }
}
fn encode_failure(value: FunctionFailureBehavior) -> i32 {
    match value {
        FunctionFailureBehavior::Propagate => wire::FailureBehavior::Propagate as i32,
        FunctionFailureBehavior::ReturnsNull => wire::FailureBehavior::ReturnsNull as i32,
    }
}
fn decode_failure(value: i32) -> Result<FunctionFailureBehavior, E> {
    match wire::FailureBehavior::try_from(value) {
        Ok(wire::FailureBehavior::Propagate) => Ok(FunctionFailureBehavior::Propagate),
        Ok(wire::FailureBehavior::ReturnsNull) => Ok(FunctionFailureBehavior::ReturnsNull),
        _ => Err(E::InvalidShape("unknown or unspecified failure")),
    }
}
fn encode_null(value: &FunctionNullBehavior) -> i32 {
    match value {
        FunctionNullBehavior::Strict => wire::NullBehavior::Strict as i32,
        FunctionNullBehavior::CalledOnNull => wire::NullBehavior::CalledOnNull as i32,
        FunctionNullBehavior::ControlDefined => wire::NullBehavior::ControlDefined as i32,
        FunctionNullBehavior::StrictPositions(_) => wire::NullBehavior::StrictPositions as i32,
    }
}
fn decode_null(value: i32) -> Result<FunctionNullBehavior, E> {
    match wire::NullBehavior::try_from(value) {
        Ok(wire::NullBehavior::Strict) => Ok(FunctionNullBehavior::Strict),
        Ok(wire::NullBehavior::CalledOnNull) => Ok(FunctionNullBehavior::CalledOnNull),
        Ok(wire::NullBehavior::ControlDefined) => Ok(FunctionNullBehavior::ControlDefined),
        _ => Err(E::InvalidShape("unknown or unspecified null")),
    }
}
fn encode_state(value: FunctionInstanceState) -> i32 {
    match value {
        FunctionInstanceState::None => wire::InstanceState::None as i32,
        FunctionInstanceState::ScalarInstance => wire::InstanceState::ScalarInstance as i32,
        FunctionInstanceState::AggregateInstance => wire::InstanceState::AggregateInstance as i32,
        FunctionInstanceState::WindowPartition => wire::InstanceState::WindowPartition as i32,
        FunctionInstanceState::TableInstance => wire::InstanceState::TableInstance as i32,
    }
}
fn decode_state(value: i32) -> Result<FunctionInstanceState, E> {
    match wire::InstanceState::try_from(value) {
        Ok(wire::InstanceState::None) => Ok(FunctionInstanceState::None),
        Ok(wire::InstanceState::ScalarInstance) => Ok(FunctionInstanceState::ScalarInstance),
        Ok(wire::InstanceState::AggregateInstance) => Ok(FunctionInstanceState::AggregateInstance),
        Ok(wire::InstanceState::WindowPartition) => Ok(FunctionInstanceState::WindowPartition),
        Ok(wire::InstanceState::TableInstance) => Ok(FunctionInstanceState::TableInstance),
        _ => Err(E::InvalidShape("unknown or unspecified state")),
    }
}
pub(crate) fn encode_policy(value: DecimalOverflowPolicy) -> i32 {
    match value {
        DecimalOverflowPolicy::OutputNull => wire::DecimalOverflowPolicy::OutputNull as i32,
        DecimalOverflowPolicy::ReportError => wire::DecimalOverflowPolicy::ReportError as i32,
    }
}
pub(crate) fn decode_policy(value: i32) -> Result<DecimalOverflowPolicy, E> {
    match wire::DecimalOverflowPolicy::try_from(value) {
        Ok(wire::DecimalOverflowPolicy::OutputNull) => Ok(DecimalOverflowPolicy::OutputNull),
        Ok(wire::DecimalOverflowPolicy::ReportError) => Ok(DecimalOverflowPolicy::ReportError),
        _ => Err(E::InvalidShape("unknown or unspecified policy")),
    }
}
trait CallProjection {
    type Error: From<E>;
    fn reserve<T>(
        &mut self,
        count: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<T>, Self::Error>;
    fn boxed<T>(
        &mut self,
        value: Vec<T>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Box<[T]>, Self::Error>;
}
impl CallProjection for Projection<'_> {
    type Error = E;
    fn reserve<T>(&mut self, n: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, E> {
        Projection::reserve(self, n, work)
    }
    fn boxed<T>(
        &mut self,
        value: Vec<T>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Box<[T]>, E> {
        Projection::boxed(self, value, work)
    }
}
struct CreatedCallProjection<'a, 'c, H> {
    creation: Option<&'a mut novarocks_type_contract::ValueSchemaCreation<'c, H>>,
}
impl<H> CallProjection for CreatedCallProjection<'_, '_, H> {
    type Error = crate::host_projection_v2::ProjectionFailure<E, H>;
    fn reserve<T>(
        &mut self,
        count: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<T>, Self::Error> {
        work.flush().map_err(E::from)?;
        let layout = std::alloc::Layout::array::<T>(count)
            .map_err(|_| E::Control(CompileControlError::ResourceExhausted))?;
        if let Some(creation) = self.creation.as_mut() {
            creation
                .request_layout(layout)
                .map_err(crate::host_projection_v2::ProjectionFailure::Host)?;
        }
        let mut value = Vec::new();
        value
            .try_reserve_exact(count)
            .map_err(|_| E::Control(CompileControlError::ResourceExhausted))?;
        work.step().map_err(E::from)?;
        work.flush().map_err(E::from)?;
        Ok(value)
    }
    fn boxed<T>(
        &mut self,
        value: Vec<T>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Box<[T]>, Self::Error> {
        work.flush().map_err(E::from)?;
        // Vec::into_boxed_slice may shrink excess capacity. This is the
        // public operation's bounded reallocation, not a retained invoice.
        if value.capacity() != value.len() {
            let upper = std::alloc::Layout::array::<T>(value.len())
                .map_err(|_| E::Control(CompileControlError::ResourceExhausted))?
                .size();
            if let Some(creation) = self.creation.as_mut() {
                creation
                    .observe_opaque_upper_bound(upper)
                    .map_err(crate::host_projection_v2::ProjectionFailure::Host)?;
            }
        }
        let value = value.into_boxed_slice();
        work.step().map_err(E::from)?;
        work.flush().map_err(E::from)?;
        Ok(value)
    }
}

fn decode_null_positions<P: CallProjection>(
    input: &wire::CallEffects,
    resources: &mut P,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionNullBehavior, P::Error> {
    let positions = &input.strict_argument_ordinals;
    if input.null_behavior != wire::NullBehavior::StrictPositions as i32 {
        if !positions.is_empty() {
            return Err(E::InvalidShape("strict ordinals require positional null behavior").into());
        }
        return Ok(decode_null(input.null_behavior)?);
    }
    validate_strict_position_count(positions.len())?;
    if positions.is_empty() {
        return Err(E::InvalidShape("positional null behavior has no strict ordinal").into());
    }
    let mut output = resources.reserve(positions.len(), work)?;
    for &ordinal in positions {
        if ordinal as usize >= novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS
            || output.last().is_some_and(|previous| *previous >= ordinal)
        {
            return Err(E::InvalidShape("invalid strict argument ordinal").into());
        }
        output.push(ordinal);
        work.step().map_err(E::from)?;
    }
    Ok(FunctionNullBehavior::StrictPositions(
        resources.boxed(output, work)?,
    ))
}

fn validate_strict_position_count(count: usize) -> Result<(), E> {
    if count > novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS {
        return Err(E::InvalidShape("too many strict argument ordinals"));
    }
    Ok(())
}

fn encode_effects<P: CallProjection>(
    value: &CallEffects,
    resources: &mut P,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::CallEffects, P::Error> {
    let mut environment = resources.reserve(value.environment.len(), work)?;
    for reference in &value.environment {
        environment.push(encode_reference(reference, work)?);
    }
    let positions = value.null_behavior.strict_positions();
    let mut strict_argument_ordinals = resources.reserve(positions.len(), work)?;
    for &ordinal in positions {
        strict_argument_ordinals.push(ordinal);
        work.step().map_err(E::from)?;
    }
    Ok(wire::CallEffects {
        strict_argument_ordinals,
        value_stability: encode_stability(value.value_stability),
        own_row_error: encode_row_error(value.own_row_error),
        failure_behavior: encode_failure(value.failure_behavior),
        null_behavior: encode_null(&value.null_behavior),
        argument_control: Some(encode_argument_control(value.argument_control)),
        instance_state: encode_state(value.instance_state),
        observable_effects: Some(wire::ObservableEffects {
            rng_sampling: value.observable_effects.rng_sampling,
            warnings: value.observable_effects.warnings,
            controlled_wait: value.observable_effects.controlled_wait,
        }),
        environment,
        proof_scope: Some(encode_proof(value.proof_scope)),
    })
}
fn decode_effects<P: CallProjection>(
    input: &wire::CallEffects,
    resources: &mut P,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CallEffects, P::Error> {
    // All repeated-field bounds have been checked before allocating any output.
    let observable = input
        .observable_effects
        .as_ref()
        .ok_or(E::InvalidShape("missing observable effects"))?;
    let mut environment = resources.reserve(input.environment.len(), work)?;
    for reference in &input.environment {
        environment.push(decode_reference(reference, work)?);
    }
    let null_behavior = decode_null_positions(input, resources, work)?;
    Ok(CallEffects {
        value_stability: decode_stability(input.value_stability)?,
        own_row_error: decode_row_error(input.own_row_error)?,
        failure_behavior: decode_failure(input.failure_behavior)?,
        null_behavior,
        argument_control: decode_argument_control(
            input
                .argument_control
                .as_ref()
                .ok_or(E::InvalidShape("missing argument control"))?,
        )?,
        instance_state: decode_state(input.instance_state)?,
        observable_effects: ObservableEffects {
            rng_sampling: observable.rng_sampling,
            warnings: observable.warnings,
            controlled_wait: observable.controlled_wait,
        },
        environment: resources.boxed(environment, work)?,
        proof_scope: decode_proof(
            input
                .proof_scope
                .as_ref()
                .ok_or(E::InvalidShape("missing proof scope"))?,
        )?,
    })
}
fn count_references(items: usize, environment: usize) -> Result<usize, E> {
    if environment > MAX_SEMANTIC_PARAMETERS {
        return Err(E::InvalidShape("too many call environment references"));
    }
    count_dynamic_items(items, environment)
}
fn count_dynamic_items(items: usize, additional: usize) -> Result<usize, E> {
    let count = items
        .checked_add(additional)
        .ok_or(E::InvalidShape("call reference count overflow"))?;
    if count > MAX_PLAN_DERIVED_CUT_ITEMS {
        return Err(E::InvalidShape("too many call dynamic items"));
    }
    Ok(count)
}

fn encode_relation_id(id: novarocks_type_contract::RelationEffectId) -> wire::RelationEffectId {
    wire::RelationEffectId {
        call_id: Some(id.call.get()),
        role: match id.role {
            novarocks_type_contract::RelationEffectRole::Call => {
                wire::RelationEffectRole::Call as i32
            }
            novarocks_type_contract::RelationEffectRole::MaterializedInput => {
                wire::RelationEffectRole::MaterializedInput as i32
            }
        },
    }
}
fn decode_relation_id(
    id: &wire::RelationEffectId,
) -> Result<novarocks_type_contract::RelationEffectId, E> {
    Ok(novarocks_type_contract::RelationEffectId {
        call: novarocks_type_contract::RelationCallId::new(required_id(
            id.call_id,
            "missing relation call ID",
        )?),
        role: match wire::RelationEffectRole::try_from(id.role) {
            Ok(wire::RelationEffectRole::Call) => novarocks_type_contract::RelationEffectRole::Call,
            Ok(wire::RelationEffectRole::MaterializedInput) => {
                novarocks_type_contract::RelationEffectRole::MaterializedInput
            }
            _ => return Err(E::InvalidShape("unknown or missing relation effect role")),
        },
    })
}
fn encode_relation_context(
    context: &novarocks_type_contract::RelationEffectContext,
) -> wire::RelationEffectContext {
    wire::RelationEffectContext {
        identity: Some(encode_relation_id(context.identity)),
        demand: encode_demand(context.demand),
    }
}
fn decode_relation_context(
    context: &wire::RelationEffectContext,
) -> Result<novarocks_type_contract::RelationEffectContext, E> {
    Ok(novarocks_type_contract::RelationEffectContext {
        identity: decode_relation_id(
            context
                .identity
                .as_ref()
                .ok_or(E::InvalidShape("missing relation identity"))?,
        )?,
        demand: decode_demand(context.demand)?,
    })
}
fn encode_call_core<P: CallProjection>(
    call: &FrozenPhysicalCall,
    resources: &mut P,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::FrozenCall, P::Error> {
    Ok(wire::FrozenCall {
        site: Some(encode_site(call.site)),
        relation_context: Some(encode_relation_context(&call.context)),
        materialized_input: call
            .materialized_writer_input
            .as_ref()
            .map(encode_relation_context),
        effects: Some(encode_effects(&call.effects, resources, work)?),
        decimal_overflow_policy: Some(encode_policy(call.decimal_overflow_policy)),
    })
}
fn decode_call_core<P: CallProjection>(
    call: &wire::FrozenCall,
    resources: &mut P,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FrozenPhysicalCall, P::Error> {
    Ok(FrozenPhysicalCall {
        site: decode_site(
            call.site
                .as_ref()
                .ok_or(E::InvalidShape("missing call site"))?,
        )?,
        context: decode_relation_context(
            call.relation_context
                .as_ref()
                .ok_or(E::InvalidShape("missing relation call context"))?,
        )?,
        materialized_writer_input: call
            .materialized_input
            .as_ref()
            .map(decode_relation_context)
            .transpose()?,
        effects: decode_effects(
            call.effects
                .as_ref()
                .ok_or(E::InvalidShape("missing call effects"))?,
            resources,
            work,
        )?,
        decimal_overflow_policy: decode_policy(
            call.decimal_overflow_policy
                .ok_or(E::InvalidShape("missing call decimal overflow policy"))?,
        )?,
    })
}
fn encode_claim_core<P: CallProjection>(
    call: &novarocks_expression_contract::ExpressionCallClaim,
    resources: &mut P,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::ExpressionCallClaim, P::Error> {
    Ok(wire::ExpressionCallClaim {
        use_id: Some(call.context.use_id.get()),
        context: Some(encode_context(&call.context)),
        effects: Some(encode_effects(&call.effects, resources, work)?),
        decimal_overflow_policy: Some(encode_policy(call.decimal_overflow_policy)),
        temporal_source: call
            .temporal_source
            .as_ref()
            .map(|source| encode_temporal_source(source, resources, work))
            .transpose()?,
    })
}
fn decode_claim_core<P: CallProjection>(
    call: &wire::ExpressionCallClaim,
    resources: &mut P,
    work: &mut CompileCheckpoints<'_>,
) -> Result<novarocks_expression_contract::ExpressionCallClaim, P::Error> {
    let id = required_id(call.use_id, "missing scalar claim use ID")?;
    if call.context.as_ref().and_then(|context| context.use_id) != Some(id) {
        return Err(E::InvalidShape("scalar claim key differs from its context").into());
    }
    Ok(novarocks_expression_contract::ExpressionCallClaim {
        context: decode_context(
            call.context
                .as_ref()
                .ok_or(E::InvalidShape("missing call context"))?,
        )?,
        effects: decode_effects(
            call.effects
                .as_ref()
                .ok_or(E::InvalidShape("missing call effects"))?,
            resources,
            work,
        )?,
        temporal_source: call
            .temporal_source
            .as_ref()
            .map(|source| decode_temporal_source(source, resources, work))
            .transpose()?,
        decimal_overflow_policy: decode_policy(
            call.decimal_overflow_policy
                .ok_or(E::InvalidShape("missing call decimal overflow policy"))?,
        )?,
    })
}
pub(super) fn encode_calls(
    input: &FrozenFragmentCalls,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::FrozenCalls, E> {
    encode_calls_core(input, &mut Projection::plain(), work)
}

fn encode_calls_core(
    input: &FrozenFragmentCalls,
    resources: &mut Projection<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::FrozenCalls, E> {
    if input.entries().len() > MAX_CONTROL_USE_REFERENCES {
        return Err(E::InvalidShape("too many call entries"));
    }
    let mut count = count_references(input.entries().len(), 0)?;
    resources.items(input.entries().len())?;
    resources.buffers::<wire::FrozenCall>(input.entries().len(), 1)?;
    resources.known::<FrozenFragmentCalls>(1)?;
    resources.known::<(PhysicalCallSite, FrozenPhysicalCall)>(input.entries().len())?;
    // Guard the cumulative component footprint before output allocation.
    for call in input.entries().values() {
        let strict = call.effects.null_behavior.strict_positions().len();
        validate_strict_position_count(strict)?;
        count = count_dynamic_items(
            count_references(count, call.effects.environment.len())?,
            strict,
        )?;
        resources.items(strict)?;
        resources.buffers::<u32>(strict, 1)?;
        resources.items(call.effects.environment.len())?;
        resources.buffers::<wire::SemanticParameterRef>(call.effects.environment.len(), 1)?;
        resources.known::<novarocks_type_contract::SemanticParameterRef>(
            call.effects.environment.len(),
        )?;
        resources.gate()?;
        work.step()?;
    }
    let mut entries = resources.reserve(input.entries().len(), work)?;
    for call in input.entries().values() {
        work.step()?;
        entries.push(encode_call_core(call, resources, work)?);
    }
    Ok(wire::FrozenCalls { entries })
}

pub(super) fn decode_calls(
    fragment: &Fragment,
    input: &wire::FrozenCalls,
    work: &mut CompileCheckpoints<'_>,
    control: &dyn PureCompileControl,
) -> Result<FrozenFragmentCalls, E> {
    decode_calls_core(fragment, input, &mut Projection::plain(), work, control)
}

fn prepare_decode_calls_prefix(
    input: &wire::FrozenCalls,
    resources: &mut Projection<'_>,
    work: &mut CompileCheckpoints<'_>,
    include_final_constructor: bool,
) -> Result<ControlOwnedResourceFacts, E> {
    if input.entries.len() > MAX_CONTROL_USE_REFERENCES {
        return Err(E::InvalidShape("too many call entries"));
    }
    let mut count = count_references(input.entries.len(), 0)?;
    resources.items(input.entries.len())?;
    resources.buffers::<FrozenPhysicalCall>(input.entries.len(), 1)?;
    resources.known::<wire::FrozenCalls>(1)?;
    resources.known::<wire::FrozenCall>(input.entries.capacity())?;
    let mut constructor_prefix = ControlOwnedResourceFacts::default();
    if include_final_constructor && resources.observed_mode() {
        resources.gate()?;
        resources.child(
            &FrozenFragmentCalls::construction_resources_with_writer_inputs(
                input.entries.len(),
                input
                    .entries
                    .iter()
                    .filter(|call| {
                        matches!(
                            call.site.as_ref().and_then(|site| site.kind.as_ref()),
                            Some(
                                wire::call_site::Kind::WriterPartial(_)
                                    | wire::call_site::Kind::WriterFinal(_)
                            )
                        )
                    })
                    .count(),
            )?,
            &mut constructor_prefix,
        )?;
    }
    for call in &input.entries {
        let effects = call
            .effects
            .as_ref()
            .ok_or(E::InvalidShape("missing call effects"))?;
        let strict = effects.strict_argument_ordinals.len();
        validate_strict_position_count(strict)?;
        count = count_dynamic_items(count_references(count, effects.environment.len())?, strict)?;
        resources.items(strict)?;
        resources.buffers::<u32>(strict, 2)?;
        resources.known::<u32>(effects.strict_argument_ordinals.capacity())?;
        resources.items(effects.environment.len())?;
        resources.buffers::<novarocks_type_contract::SemanticParameterRef>(
            effects.environment.len(),
            2,
        )?;
        resources.known::<wire::SemanticParameterRef>(effects.environment.capacity())?;
        resources.gate()?;
        work.step()?;
    }
    Ok(constructor_prefix)
}

fn decode_calls_core(
    fragment: &Fragment,
    input: &wire::FrozenCalls,
    resources: &mut Projection<'_>,
    work: &mut CompileCheckpoints<'_>,
    control: &dyn PureCompileControl,
) -> Result<FrozenFragmentCalls, E> {
    let constructor_prefix = prepare_decode_calls_prefix(input, resources, work, true)?;
    let mut entries = resources.reserve(input.entries.len(), work)?;
    for call in &input.entries {
        work.step()?;
        entries.push(decode_call_core(call, resources, work)?);
    }
    // Flush projection work before entering the real same-snapshot validator.
    // Neither this constructor nor the projection supplies an owner proof.
    work.flush()?;
    if resources.observed_mode() {
        let mut previous = constructor_prefix;
        Ok(FrozenFragmentCalls::try_new_in(
            fragment,
            entries,
            &mut |facts| resources.child(facts, &mut previous),
            work,
        )?)
    } else {
        Ok(FrozenFragmentCalls::try_new(fragment, entries, control)?)
    }
}

/// Preserve the complete original call table using one admitted caller scope.
/// The caller owns entry/footer and the source union; facts are request/work
/// upper bounds, not installed-owner, effect or allocator authority.
pub fn encode_frozen_calls_observed(
    input: &FrozenFragmentCalls,
    source_retained_bytes: usize,
    limits: impl Into<Option<NodeProjectionLimits>>,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::FrozenCalls, NodeProjectionFacts), E> {
    let mut resources = Projection::observed(source_retained_bytes, limits, admit, 0)?;
    let output = encode_calls_core(input, &mut resources, work)?;
    Ok((output, resources.facts()?))
}
pub fn decode_frozen_calls_observed(
    fragment: &Fragment,
    input: &wire::FrozenCalls,
    source_retained_bytes: usize,
    limits: impl Into<Option<NodeProjectionLimits>>,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(FrozenFragmentCalls, NodeProjectionFacts), E> {
    let mut resources = Projection::observed(source_retained_bytes, limits, admit, 0)?;
    let output = decode_calls_core(fragment, input, &mut resources, work, work.control())?;
    Ok((output, resources.facts()?))
}

/// Project relation entries for installation by the sole Fragment owner.
/// This preserves its original constructor and avoids a second call table.
pub(crate) fn decode_frozen_call_entries_observed(
    input: &wire::FrozenCalls,
    source_retained_bytes: usize,
    limits: impl Into<Option<NodeProjectionLimits>>,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Vec<FrozenPhysicalCall>, NodeProjectionFacts), E> {
    let mut resources = Projection::observed(source_retained_bytes, limits, admit, 0)?;
    prepare_decode_calls_prefix(input, &mut resources, work, false)?;
    let mut entries = resources.reserve(input.entries.len(), work)?;
    for raw in &input.entries {
        work.step()?;
        entries.push(decode_call_core(raw, &mut resources, work)?);
    }
    Ok((entries, resources.facts()?))
}

/// Grammar-projected calls whose complete graph/effect validation is pending.
/// Native must map the exact definitions and validate them with its created
/// ProgramControlFlow/RootAttachment before installing any invocation.
pub struct CreatedFrozenCallParts {
    calls: Vec<FrozenPhysicalCall>,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl CreatedFrozenCallParts {
    pub fn calls(&self) -> &[FrozenPhysicalCall] {
        &self.calls
    }
    pub fn into_parts(
        self,
    ) -> (
        Vec<FrozenPhysicalCall>,
        novarocks_type_contract::ValueTypeStorageLifetime,
    ) {
        (self.calls, self.storage)
    }
}
/// Input-derived receiving mode. The sole numerical model keeps counting all
/// entries/backing/work even without a receiving-profile callback. This is not
/// an installed-effect proof or capacity grant.
pub fn decode_frozen_call_parts_input_derived_with_creator_in<H>(
    input: &wire::FrozenCalls,
    source_retained_bytes: usize,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (CreatedFrozenCallParts, NodeProjectionFacts),
    crate::host_projection_v2::ProjectionFailure<E, H>,
> {
    let mut numerical = Projection::input_derived(source_retained_bytes);
    decode_frozen_call_parts_core(input, &mut numerical, creation, work)
}
fn decode_frozen_call_parts_core<H>(
    input: &wire::FrozenCalls,
    numerical: &mut Projection<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (CreatedFrozenCallParts, NodeProjectionFacts),
    crate::host_projection_v2::ProjectionFailure<E, H>,
> {
    prepare_decode_calls_prefix(input, numerical, work, false)?;
    numerical.gate()?;
    let storage = creation.storage_lifetime();
    let mut projection = CreatedCallProjection {
        creation: Some(creation),
    };
    let mut calls = projection.reserve(input.entries.len(), work)?;
    for raw in &input.entries {
        work.step().map_err(E::from)?;
        calls.push(decode_call_core(raw, &mut projection, work)?);
    }
    let facts = numerical.facts()?;
    Ok((CreatedFrozenCallParts { calls, storage }, facts))
}
/// Explicit FE pure sender, sharing the original full call-table writer.
/// It borrows the original checkpoint stream without a capacity grant/footer.
pub fn encode_frozen_calls_pure_in(
    input: &FrozenFragmentCalls,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::FrozenCalls, E> {
    encode_calls(input, work)
}

/// A decoded writer call and the actual creator of its effect-reference
/// backing. The caller moves this guard with the prepared-call source.
pub struct CreatedFrozenWriterCall {
    call: FrozenPhysicalCall,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl CreatedFrozenWriterCall {
    pub fn call(&self) -> &FrozenPhysicalCall {
        &self.call
    }
}
/// Project one original writer call through the bulk codec's sole field body.
/// None is the explicit FE pure path, without a memory grant.
pub fn encode_frozen_writer_call_with_creator_in<H>(
    call: &FrozenPhysicalCall,
    creation: Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::FrozenCall, crate::host_projection_v2::ProjectionFailure<E, H>> {
    work.flush().map_err(E::from)?;
    validate_writer_source_shape(call)?;
    encode_call_core(call, &mut CreatedCallProjection { creation }, work)
}
/// Decode a writer-only loan, matching its actual original request definition.
/// The surrounding source author must validate real domain membership; this
/// grammar projection never claims a whole Fragment or installed effect proof.
pub fn decode_frozen_writer_call_with_creator_in<H>(
    raw: &wire::FrozenCall,
    actual_site: PhysicalCallSite,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CreatedFrozenWriterCall, crate::host_projection_v2::ProjectionFailure<E, H>> {
    work.flush().map_err(E::from)?;
    let site = decode_site(
        raw.site
            .as_ref()
            .ok_or(E::InvalidShape("missing writer call site"))?,
    )?;
    if site != actual_site
        || !matches!(
            site,
            PhysicalCallSite::WriterPartial { .. } | PhysicalCallSite::WriterFinal { .. }
        )
    {
        return Err(
            E::InvalidShape("writer call differs from its original request definition").into(),
        );
    }
    if raw.materialized_input.is_none() {
        return Err(E::InvalidShape("missing original writer Value context").into());
    }
    let effects = raw
        .effects
        .as_ref()
        .ok_or(E::InvalidShape("missing writer call effects"))?;
    if effects.environment.len() > MAX_SEMANTIC_PARAMETERS {
        return Err(E::InvalidShape("writer call environment exceeds original grammar").into());
    }
    if !effects.strict_argument_ordinals.is_empty() {
        return Err(E::InvalidShape("writer call cannot use scalar strict ordinals").into());
    }
    let storage = creation.storage_lifetime();
    let call = decode_call_core(
        raw,
        &mut CreatedCallProjection {
            creation: Some(creation),
        },
        work,
    )?;
    validate_writer_source_shape(&call)?;
    Ok(CreatedFrozenWriterCall {
        call,
        _storage: storage,
    })
}
fn validate_writer_source_shape(call: &FrozenPhysicalCall) -> Result<(), E> {
    let input = call
        .materialized_writer_input
        .ok_or(E::InvalidShape("missing original writer Value context"))?;
    if !matches!(
        call.site,
        PhysicalCallSite::WriterPartial { .. } | PhysicalCallSite::WriterFinal { .. }
    ) || !input.is_materialized_input_of(call.context)
    {
        return Err(E::InvalidShape(
            "writer frozen call changes its original source roles",
        ));
    }
    if call.effects.environment.len() > MAX_SEMANTIC_PARAMETERS
        || call.effects.proof_scope != CallProofScope::Unconditional
            && call.effects.proof_scope != CallProofScope::Relation(call.context.identity)
    {
        return Err(E::InvalidShape(
            "writer frozen call has an invalid effect scope",
        ));
    }
    Ok(())
}

pub(crate) fn encode_expression_claims_in(
    unit: &novarocks_expression_contract::BoundExprSet,
    source: usize,
    limits: impl Into<Option<NodeProjectionLimits>>,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::ExpressionClaims, NodeProjectionFacts), E> {
    let mut resources = Projection::observed(source, limits, admit, 0)?;
    let mut count = count_references(unit.claims().len(), 0)?;
    resources.items(unit.claims().len())?;
    resources.buffers::<wire::ExpressionCallClaim>(unit.claims().len(), 1)?;
    for call in unit.claims().values() {
        if let Some(source) = &call.temporal_source {
            count = count_references(
                count,
                source
                    .facts
                    .cast_chain()
                    .len()
                    .checked_add(source.sources.len())
                    .ok_or(E::InvalidShape("temporal source footprint overflow"))?,
            )?;
            resources.items(source.sources.len() + source.facts.cast_chain().len())?;
            resources.buffers::<wire::TemporalSourceOccurrence>(source.sources.len(), 1)?;
            resources.buffers::<i32>(source.facts.cast_chain().len(), 1)?;
            resources.known::<novarocks_type_contract::TemporalSourceOccurrence<
                novarocks_physical_plan::ExprId,
            >>(source.sources.len())?;
            resources.known::<novarocks_type_contract::TemporalCastKind>(
                source.facts.cast_chain().len(),
            )?;
        }
        let strict = call.effects.null_behavior.strict_positions().len();
        validate_strict_position_count(strict)?;
        count = count_dynamic_items(
            count_references(count, call.effects.environment.len())?,
            strict,
        )?;
        resources.items(strict)?;
        resources.buffers::<u32>(strict, 1)?;
        resources.items(call.effects.environment.len())?;
        resources.buffers::<wire::SemanticParameterRef>(call.effects.environment.len(), 1)?;
        resources.gate()?;
        work.step()?;
    }
    let mut entries = resources.reserve(unit.claims().len(), work)?;
    for call in unit.claims().values() {
        work.step()?;
        entries.push(encode_claim_core(call, &mut resources, work)?);
    }
    Ok((wire::ExpressionClaims { entries }, resources.facts()?))
}
fn prepare_expression_claims_decode(
    input: &wire::ExpressionClaims,
    resources: &mut Projection<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    let mut count = count_references(input.entries.len(), 0)?;
    resources.items(input.entries.len())?;
    resources
        .buffers::<novarocks_expression_contract::ExpressionCallClaim>(input.entries.len(), 1)?;
    for call in &input.entries {
        let effects = call
            .effects
            .as_ref()
            .ok_or(E::InvalidShape("missing scalar effects"))?;
        if let Some(source) = &call.temporal_source {
            validate_wire_source_extent(source)?;
            count = count_references(count, source.cast_chain.len() + source.sources.len())?;
            resources.items(source.sources.len() + source.cast_chain.len())?;
            resources.buffers::<novarocks_type_contract::TemporalSourceOccurrence<
                novarocks_physical_plan::ExprId,
            >>(source.sources.len(), 2)?;
            resources
                .buffers::<novarocks_type_contract::TemporalCastKind>(source.cast_chain.len(), 2)?;
            resources.known::<wire::TemporalSourceOccurrence>(source.sources.capacity())?;
            resources.known::<i32>(source.cast_chain.capacity())?;
        }
        let strict = effects.strict_argument_ordinals.len();
        validate_strict_position_count(strict)?;
        count = count_dynamic_items(count_references(count, effects.environment.len())?, strict)?;
        resources.items(strict)?;
        resources.buffers::<u32>(strict, 2)?;
        resources.known::<u32>(effects.strict_argument_ordinals.capacity())?;
        resources.items(effects.environment.len())?;
        resources.buffers::<novarocks_type_contract::SemanticParameterRef>(
            effects.environment.len(),
            2,
        )?;
        resources.gate()?;
        work.step()?;
    }
    let mut tree = novarocks_type_contract::ControlResourceCounter::default();
    tree.tree::<ExpressionUseId, novarocks_expression_contract::ExpressionCallClaim>(
        input.entries.len(),
    )
    .map_err(claim_resource_error)?;
    resources.child(&tree.facts(), &mut ControlOwnedResourceFacts::default())?;
    resources.gate()?;
    Ok(())
}
fn claim_resource_error(error: novarocks_type_contract::ControlResourceError) -> E {
    match error {
        novarocks_type_contract::ControlResourceError::Control(cause) => E::Control(cause),
        novarocks_type_contract::ControlResourceError::SourceModel(message) => {
            E::SourceModel(message)
        }
    }
}
pub(crate) fn decode_expression_claims_in<H>(
    input: &wire::ExpressionClaims,
    source: usize,
    limits: impl Into<Option<NodeProjectionLimits>>,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    creation: Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (
        std::collections::BTreeMap<
            ExpressionUseId,
            novarocks_expression_contract::ExpressionCallClaim,
        >,
        NodeProjectionFacts,
    ),
    crate::host_projection_v2::ProjectionFailure<E, H>,
> {
    let mut resources = Projection::observed(source, limits, admit, 0)?;
    prepare_expression_claims_decode(input, &mut resources, work)?;
    let facts = resources.facts()?;
    let mut projection = CreatedCallProjection { creation };
    let mut tree = novarocks_type_contract::ControlResourceCounter::default();
    tree.tree::<ExpressionUseId, novarocks_expression_contract::ExpressionCallClaim>(
        input.entries.len(),
    )
    .map_err(claim_resource_error)?;
    if let Some(creation) = projection.creation.as_mut() {
        creation
            .observe_opaque_upper_bound(tree.facts().allocation_request_bytes_upper_bound)
            .map_err(crate::host_projection_v2::ProjectionFailure::Host)?;
    }
    let mut entries = std::collections::BTreeMap::new();
    for raw in &input.entries {
        work.step().map_err(E::from)?;
        let claim = decode_claim_core(raw, &mut projection, work)?;
        work.flush().map_err(E::from)?;
        let replaced = entries.insert(claim.context.use_id, claim).is_some();
        work.step().map_err(E::from)?;
        work.flush().map_err(E::from)?;
        if replaced {
            return Err(E::InvalidShape("duplicate scalar claim").into());
        }
    }
    Ok((entries, facts))
}
#[cfg(test)]
#[path = "calls/tests.rs"]
mod tests;

fn validate_wire_source_extent(source: &wire::TemporalSourcePlan) -> Result<(), E> {
    if source.sources.len() > 3
        || source.cast_chain.len() > novarocks_type_contract::MAX_CONTROL_DEPTH
    {
        return Err(E::InvalidShape(
            "temporal source component exceeds exact bounds",
        ));
    }
    Ok(())
}
fn encode_temporal_source<P: CallProjection>(
    source: &novarocks_type_contract::TemporalSourcePlan<novarocks_physical_plan::ExprId>,
    resources: &mut P,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::TemporalSourcePlan, P::Error> {
    use novarocks_type_contract::{TemporalCastKind as C, TemporalSourceRole as R};
    source
        .validate_structure()
        .map_err(|_| E::InvalidShape("invalid temporal source structure"))?;
    let mut cast_chain = resources.reserve(source.facts.cast_chain().len(), work)?;
    for kind in source.facts.cast_chain() {
        work.step().map_err(E::from)?;
        cast_chain.push(match kind {
            C::Ordinary => wire::TemporalCastKind::Ordinary,
            C::Time => wire::TemporalCastKind::Time,
            C::TimeFromDatetime => wire::TemporalCastKind::TimeFromDatetime,
        } as i32);
    }
    let mut sources = resources.reserve(source.sources.len(), work)?;
    for channel in &source.sources {
        work.step().map_err(E::from)?;
        let role = match channel.role {
            R::Normal => wire::TemporalSourceRole::Normal,
            R::Format => wire::TemporalSourceRole::Format,
            R::RawOverride => wire::TemporalSourceRole::RawOverride,
            R::OriginalSeconds => wire::TemporalSourceRole::OriginalSeconds,
            R::ImmediateCastSource => wire::TemporalSourceRole::ImmediateCastSource,
            R::DeepestCastSource => wire::TemporalSourceRole::DeepestCastSource,
        };
        sources.push(wire::TemporalSourceOccurrence {
            role: role as i32,
            use_id: Some(channel.use_id.get()),
            definition_id: Some(channel.definition.get()),
        });
    }
    Ok(wire::TemporalSourcePlan {
        shape: crate::physical_control_v2::encode_temporal_shape(source.facts.shape()),
        cast_chain,
        sources,
    })
}
fn decode_temporal_source<P: CallProjection>(
    source: &wire::TemporalSourcePlan,
    resources: &mut P,
    work: &mut CompileCheckpoints<'_>,
) -> Result<novarocks_type_contract::TemporalSourcePlan<novarocks_physical_plan::ExprId>, P::Error>
{
    use novarocks_type_contract::{
        TemporalCastKind as C, TemporalSourceFacts as F, TemporalSourceRole as R,
        TemporalSourceShape as S,
    };
    validate_wire_source_extent(source)?;
    let shape = crate::physical_control_v2::decode_temporal_shape(source.shape).map_err(E::from)?;
    if source.sources.len() != shape.source_count() {
        return Err(P::Error::from(E::InvalidShape(
            "wrong temporal source channel count",
        )));
    }
    let mut cast_chain = resources.reserve(source.cast_chain.len(), work)?;
    for kind in &source.cast_chain {
        work.step().map_err(E::from)?;
        cast_chain.push(match wire::TemporalCastKind::try_from(*kind) {
            Ok(wire::TemporalCastKind::Ordinary) => C::Ordinary,
            Ok(wire::TemporalCastKind::Time) => C::Time,
            Ok(wire::TemporalCastKind::TimeFromDatetime) => C::TimeFromDatetime,
            _ => {
                return Err(P::Error::from(E::InvalidShape(
                    "unknown or missing temporal cast kind",
                )));
            }
        });
    }
    if matches!(shape, S::FormatOrdinary | S::FormatUtf8Override) && !cast_chain.is_empty() {
        return Err(P::Error::from(E::InvalidShape(
            "cast trace on TIME_FORMAT source grammar",
        )));
    }
    work.flush().map_err(E::from)?;
    let cast_chain = resources.boxed(cast_chain, work)?;
    work.flush().map_err(E::from)?;
    let facts = match shape {
        S::FormatOrdinary => F::FormatOrdinary,
        S::FormatUtf8Override => F::FormatUtf8Override,
        S::SecondsDirect => F::SecondsDirect { cast_chain },
        S::SecondsCastString => F::SecondsCastString { cast_chain },
        S::SecondsCastOther => F::SecondsCastOther { cast_chain },
        S::SecondsRoundtrip => F::SecondsRoundtrip { cast_chain },
    };
    let mut sources = resources.reserve(source.sources.len(), work)?;
    for channel in &source.sources {
        work.step().map_err(E::from)?;
        let role = match wire::TemporalSourceRole::try_from(channel.role) {
            Ok(wire::TemporalSourceRole::Normal) => R::Normal,
            Ok(wire::TemporalSourceRole::Format) => R::Format,
            Ok(wire::TemporalSourceRole::RawOverride) => R::RawOverride,
            Ok(wire::TemporalSourceRole::OriginalSeconds) => R::OriginalSeconds,
            Ok(wire::TemporalSourceRole::ImmediateCastSource) => R::ImmediateCastSource,
            Ok(wire::TemporalSourceRole::DeepestCastSource) => R::DeepestCastSource,
            _ => {
                return Err(P::Error::from(E::InvalidShape(
                    "unknown or missing temporal source role",
                )));
            }
        };
        sources.push(novarocks_type_contract::TemporalSourceOccurrence {
            role,
            use_id: ExpressionUseId::new(required_id(
                channel.use_id,
                "missing temporal source use ID",
            )?),
            definition: novarocks_physical_plan::ExprId::new(required_id(
                channel.definition_id,
                "missing temporal source definition ID",
            )?),
        });
    }
    work.flush().map_err(E::from)?;
    let sources = resources.boxed(sources, work)?;
    work.flush().map_err(E::from)?;
    let plan = novarocks_type_contract::TemporalSourcePlan { facts, sources };
    plan.validate_structure()
        .map_err(|_| E::InvalidShape("invalid temporal source structure"))?;
    Ok(plan)
}

#[cfg(test)]
#[path = "temporal_source_codec_tests.rs"]
mod temporal_source_codec_tests;

#[cfg(test)]
#[path = "calls/writer_projection_creator_tests.rs"]
mod writer_projection_creator_tests;
