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
    PhysicalCallSite, PhysicalRootUses,
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

pub(super) fn encode_context(context: &ExpressionEffectContext) -> wire::EffectContext {
    wire::EffectContext {
        use_id: Some(context.use_id.get()),
        domain_id: Some(context.region.get()),
        demand: encode_demand(context.demand),
    }
}
pub(super) fn decode_context(input: &wire::EffectContext) -> Result<ExpressionEffectContext, E> {
    Ok(ExpressionEffectContext {
        use_id: ExpressionUseId::new(required_id(input.use_id, "missing effect use ID")?),
        region: EvaluationRegionId::new(required_id(input.domain_id, "missing effect domain ID")?),
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
            PhysicalCallSite::Expression(id) => Kind::ExpressionUseId(id.get()),
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
            Kind::ExpressionUseId(id) => PhysicalCallSite::Expression(ExpressionUseId::new(*id)),
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
            CallProofScope::Region(id) => Kind::DomainId(id.get()),
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
            Kind::DomainId(id) => CallProofScope::Region(EvaluationRegionId::new(*id)),
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
fn encode_null(value: FunctionNullBehavior) -> i32 {
    match value {
        FunctionNullBehavior::Strict => wire::NullBehavior::Strict as i32,
        FunctionNullBehavior::CalledOnNull => wire::NullBehavior::CalledOnNull as i32,
        FunctionNullBehavior::ControlDefined => wire::NullBehavior::ControlDefined as i32,
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

fn encode_effects<P: CallProjection>(
    value: &CallEffects,
    resources: &mut P,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::CallEffects, P::Error> {
    let mut environment = resources.reserve(value.environment.len(), work)?;
    for reference in &value.environment {
        environment.push(encode_reference(reference, work)?);
    }
    Ok(wire::CallEffects {
        value_stability: encode_stability(value.value_stability),
        own_row_error: encode_row_error(value.own_row_error),
        failure_behavior: encode_failure(value.failure_behavior),
        null_behavior: encode_null(value.null_behavior),
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
    Ok(CallEffects {
        value_stability: decode_stability(input.value_stability)?,
        own_row_error: decode_row_error(input.own_row_error)?,
        failure_behavior: decode_failure(input.failure_behavior)?,
        null_behavior: decode_null(input.null_behavior)?,
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
    let count = items
        .checked_add(environment)
        .ok_or(E::InvalidShape("call reference count overflow"))?;
    if count > MAX_PLAN_DERIVED_CUT_ITEMS {
        return Err(E::InvalidShape("too many call dynamic items"));
    }
    Ok(count)
}

fn encode_call_core<P: CallProjection>(
    call: &FrozenPhysicalCall,
    resources: &mut P,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::FrozenCall, P::Error> {
    Ok(wire::FrozenCall {
        site: Some(encode_site(call.site)),
        context: Some(encode_context(&call.context)),
        materialized_writer_input: call.materialized_writer_input.as_ref().map(encode_context),
        effects: Some(encode_effects(&call.effects, resources, work)?),
        decimal_overflow_policy: Some(encode_policy(call.decimal_overflow_policy)),
        regexp_count_pattern_source: call.regexp_count_pattern_source.map(|source| match source {
            novarocks_type_contract::RegexpCountPatternSource::Dynamic => 0,
            novarocks_type_contract::RegexpCountPatternSource::NativeV1Utf8LiteralWhenPresent => 1,
        }),
        to_base64_byte_source: call.to_base64_byte_source.map(|source| match source {
            novarocks_type_contract::ToBase64ByteSource::Ordinary => 0,
            novarocks_type_contract::ToBase64ByteSource::NativeV1EncryptionLatin1 => 1,
        }),
        temporal_source: call
            .temporal_source
            .as_ref()
            .map(|source| encode_temporal_source(source, resources, work))
            .transpose()?,
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
            context: decode_context(
                call.context
                    .as_ref()
                    .ok_or(E::InvalidShape("missing call context"))?,
            )?,
            materialized_writer_input:call.materialized_writer_input.as_ref().map(decode_context).transpose()?,
            effects: decode_effects(
                call.effects
                    .as_ref()
                    .ok_or(E::InvalidShape("missing call effects"))?,
                resources,
                work,
            )?,
            regexp_count_pattern_source: call.regexp_count_pattern_source.map(|source| match source {
                0 => Ok(novarocks_type_contract::RegexpCountPatternSource::Dynamic),
                1 => Ok(novarocks_type_contract::RegexpCountPatternSource::NativeV1Utf8LiteralWhenPresent),
                _ => Err(E::InvalidShape("unknown regexp_count pattern source tag")),
            }).transpose()?,
            to_base64_byte_source: call.to_base64_byte_source.map(|source| match source {
                0 => Ok(novarocks_type_contract::ToBase64ByteSource::Ordinary),
                1 => Ok(novarocks_type_contract::ToBase64ByteSource::NativeV1EncryptionLatin1),
                _ => Err(E::InvalidShape("unknown to_base64 byte source tag")),
            }).transpose()?,
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
        count = count_references(count, call.effects.environment.len())?;
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
    uses: &PhysicalRootUses,
    input: &wire::FrozenCalls,
    work: &mut CompileCheckpoints<'_>,
    control: &dyn PureCompileControl,
) -> Result<FrozenFragmentCalls, E> {
    decode_calls_core(
        fragment,
        uses,
        input,
        &mut Projection::plain(),
        work,
        control,
    )
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
        count = count_references(count, effects.environment.len())?;
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
    uses: &PhysicalRootUses,
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
            uses,
            entries,
            &mut |facts| resources.child(facts, &mut previous),
            work,
        )?)
    } else {
        Ok(FrozenFragmentCalls::try_new(
            fragment, uses, entries, control,
        )?)
    }
}

/// Preserve the complete original call table using one admitted caller scope.
/// The caller owns entry/footer and the source union; facts are request/work
/// upper bounds, not installed-owner, effect or allocator authority.
pub fn encode_frozen_calls_observed(
    input: &FrozenFragmentCalls,
    source_retained_bytes: usize,
    limits: NodeProjectionLimits,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::FrozenCalls, NodeProjectionFacts), E> {
    let mut resources = Projection::observed(source_retained_bytes, limits, admit, 0)?;
    let output = encode_calls_core(input, &mut resources, work)?;
    Ok((output, resources.facts()?))
}
pub fn decode_frozen_calls_observed(
    fragment: &Fragment,
    uses: &PhysicalRootUses,
    input: &wire::FrozenCalls,
    source_retained_bytes: usize,
    limits: NodeProjectionLimits,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(FrozenFragmentCalls, NodeProjectionFacts), E> {
    let mut resources = Projection::observed(source_retained_bytes, limits, admit, 0)?;
    let output = decode_calls_core(fragment, uses, input, &mut resources, work, work.control())?;
    Ok((output, resources.facts()?))
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
    // Reject unsupported source annotations before any child allocation.
    if raw.temporal_source.is_some()
        || raw.regexp_count_pattern_source.is_some()
        || raw.to_base64_byte_source.is_some()
    {
        return Err(E::InvalidShape("writer frozen call contains foreign source controls").into());
    }
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
    if raw.materialized_writer_input.is_none() {
        return Err(E::InvalidShape("missing original writer Value context").into());
    }
    let effects = raw
        .effects
        .as_ref()
        .ok_or(E::InvalidShape("missing writer call effects"))?;
    if effects.environment.len() > MAX_SEMANTIC_PARAMETERS {
        return Err(E::InvalidShape("writer call environment exceeds original grammar").into());
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
    ) || call.context.demand != novarocks_type_contract::EvaluationDemand::Value
        || input.demand != novarocks_type_contract::EvaluationDemand::Value
        || input.use_id == call.context.use_id
        || input.region == call.context.region
        || call.temporal_source.is_some()
        || call.regexp_count_pattern_source.is_some()
        || call.to_base64_byte_source.is_some()
    {
        return Err(E::InvalidShape(
            "writer frozen call changes its original source roles",
        ));
    }
    if call.effects.environment.len() > MAX_SEMANTIC_PARAMETERS
        || call.effects.proof_scope != CallProofScope::Unconditional
            && call.effects.proof_scope != CallProofScope::Region(call.context.region)
    {
        return Err(E::InvalidShape(
            "writer frozen call has an invalid effect scope",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_constants::{self as checked, Fragment, FragmentBuilder};
    use arrow::datatypes::DataType;
    use novarocks_physical_plan::*;
    use novarocks_type_contract::{
        AggregateStateFormatId, CompileControlError, CompilePhase, ControlShape, EvaluationDemand,
        ExpressionControlFlow, ExpressionEvaluationRegion, ExpressionInvocation,
        FunctionArgumentEvaluation, FunctionId, FunctionKind, FunctionOverloadId,
        SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
    };
    use std::sync::Mutex;

    #[derive(Default)]
    struct TestControl {
        failure: Option<(usize, CompileControlError)>,
        events: Mutex<Vec<(CompilePhase, u32)>>,
    }
    impl PureCompileControl for TestControl {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut events = self.events.lock().unwrap();
            events.push((phase, units));
            if let Some((at, error)) = self.failure.clone()
                && at == events.len()
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn properties() -> PhysicalProperties {
        PhysicalProperties {
            distribution: Distribution::Singleton,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        }
    }
    fn dop() -> PipelineDopDomain {
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        }
    }
    fn boolean() -> ValueType {
        ValueType::new(DataType::Boolean, true)
    }
    fn integer() -> ValueType {
        ValueType::new(DataType::Int64, false)
    }
    fn function(kind: FunctionKind, result_type: ValueType) -> BoundFunction {
        BoundFunction {
            function_id: FunctionId::try_new("fixture/frozen-calls/exact-id").unwrap(),
            overload: FunctionOverloadId::try_new("fixture/frozen-calls/exact-overload").unwrap(),
            kind,
            argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::default()),
            result_type,

            legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
                volatility: FunctionVolatility::Immutable,
                argument_evaluation: FunctionArgumentEvaluation::Eager,
                failure_behavior: FunctionFailureBehavior::Propagate,
                intrinsic_row_error: match kind {
                    FunctionKind::Scalar | FunctionKind::Table => {
                        FunctionIntrinsicRowError::NoRowError
                    }
                    FunctionKind::Aggregate | FunctionKind::Window => {
                        FunctionIntrinsicRowError::NotRowEvaluated
                    }
                },
                semantic_parameters: Box::default(),
            }),
        }
    }
    fn effects(kind: FunctionKind, domain: EvaluationRegionId) -> CallEffects {
        CallEffects {
            value_stability: FunctionVolatility::Immutable,
            own_row_error: match kind {
                FunctionKind::Scalar | FunctionKind::Table => FunctionIntrinsicRowError::NoRowError,
                FunctionKind::Aggregate | FunctionKind::Window => {
                    FunctionIntrinsicRowError::NotRowEvaluated
                }
            },
            failure_behavior: FunctionFailureBehavior::Propagate,
            null_behavior: FunctionNullBehavior::CalledOnNull,
            argument_control: match kind {
                FunctionKind::Scalar => ArgumentControl::Eager,
                FunctionKind::Aggregate => ArgumentControl::Aggregate,
                FunctionKind::Window => ArgumentControl::Window,
                FunctionKind::Table => ArgumentControl::Table,
            },
            instance_state: match kind {
                FunctionKind::Scalar => FunctionInstanceState::None,
                FunctionKind::Aggregate => FunctionInstanceState::AggregateInstance,
                FunctionKind::Window => FunctionInstanceState::WindowPartition,
                FunctionKind::Table => FunctionInstanceState::TableInstance,
            },
            observable_effects: ObservableEffects::NONE,
            environment: Box::default(),
            proof_scope: CallProofScope::Region(domain),
        }
    }
    fn context(id: u32, domain: u32, demand: EvaluationDemand) -> ExpressionEffectContext {
        ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            region: EvaluationRegionId::new(domain),
            demand,
        }
    }
    fn domain(id: u32) -> ExpressionEvaluationRegion {
        ExpressionEvaluationRegion {
            id: EvaluationRegionId::new(id),
            parent: None,
            guard: None,
        }
    }
    fn install_node(
        builder: &mut FragmentBuilder,
        node: NodeId,
        inputs: Vec<NodeId>,
        columns: Vec<ValueId>,
        kind: NodeKind,
    ) {
        builder
            .insert_node_unchecked(PhysicalNode {
                id: node,
                required_inputs: vec![properties(); inputs.len()].into_boxed_slice(),
                inputs: inputs.into_boxed_slice(),
                output_properties: properties(),
                output: OutputPort {
                    node,
                    columns: columns.into_boxed_slice(),
                },
                kind,
            })
            .unwrap();
    }
    fn add_values(builder: &mut FragmentBuilder, count: usize, scalar: bool) -> NodeId {
        let node = builder.reserve_node_id().unwrap();
        let ty = if scalar { boolean() } else { integer() };
        let kind = if scalar {
            ExprKind::FunctionCall {
                function: function(FunctionKind::Scalar, ty.clone()),
                args: Box::default(),
            }
        } else {
            builder.constant_kind(&ty, checked::i64(17))
        };
        let expression = builder.add_expression(node, ty.clone(), kind).unwrap();
        let value = builder
            .add_value(
                ty,
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        install_node(
            builder,
            node,
            vec![],
            vec![value],
            NodeKind::Values {
                rows: vec![Box::from([expression]); count].into_boxed_slice(),
            },
        );
        node
    }
    fn leaf_roots(fragment: &Fragment) -> PhysicalRootUses {
        let roots = PhysicalExpressionRoots::try_new(fragment, &TestControl::default()).unwrap();
        let mut bindings = Vec::new();
        let mut invocations = Vec::new();
        for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
            // These fixtures explicitly use literals or zero-argument calls.
            // This is not a production inference of function control semantics.
            match &fragment.expressions().get(root.expr).unwrap().kind {
                ExprKind::Constant(_) => {}
                ExprKind::FunctionCall { args, .. } | ExprKind::WindowCall { args, .. } => {
                    assert!(args.is_empty());
                }
                _ => panic!("fixture root requires explicit invocation construction"),
            }
            let id = match ordinal {
                0 => 0,
                1 => u32::MAX,
                other => other as u32 - 1,
            };
            let current = context(id, if ordinal % 2 == 0 { 0 } else { u32::MAX }, root.demand);
            bindings.push((*site, current.use_id));
            invocations.push(ExpressionInvocation {
                context: current,
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
        }
        let flow = ExpressionControlFlow::try_new(
            vec![domain(0), domain(u32::MAX)],
            invocations,
            fragment.expressions(),
            CompilePhase::Validate,
            &TestControl::default(),
        )
        .unwrap();
        PhysicalRootUses::try_new(fragment, flow, bindings, &TestControl::default()).unwrap()
    }
    struct Fixture {
        fragment: Fragment,
        uses: PhysicalRootUses,
        calls: Vec<FrozenPhysicalCall>,
    }
    impl Fixture {
        fn checked(&self) -> Result<FrozenFragmentCalls, FrozenCallError> {
            FrozenFragmentCalls::try_new(
                &self.fragment,
                &self.uses,
                self.calls.clone(),
                &TestControl::default(),
            )
        }
    }
    fn scalar_fixture(count: usize, fragment_id: u32) -> Fixture {
        let mut builder = FragmentBuilder::new(FragmentId::new(fragment_id));
        let node = add_values(&mut builder, count, true);
        let fragment = builder
            .finish_definition(
                node,
                FragmentSink::Noop,
                dop(),
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap();
        // finish_definition already applies the real definition validator.
        let uses = leaf_roots(&fragment);
        let calls = uses
            .flow()
            .uses()
            .values()
            .map(|invocation| FrozenPhysicalCall {
                materialized_writer_input: None,
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                site: PhysicalCallSite::Expression(invocation.context.use_id),
                context: invocation.context,
                effects: effects(FunctionKind::Scalar, invocation.context.region),
            })
            .collect();
        Fixture {
            fragment,
            uses,
            calls,
        }
    }

    fn encode_checked(
        input: &FrozenFragmentCalls,
        control: &dyn PureCompileControl,
    ) -> Result<wire::FrozenCalls, E> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let result = encode_calls(input, &mut work)?;
        work.finish()?;
        Ok(result)
    }
    fn decode_checked(
        fixture: &Fixture,
        input: &wire::FrozenCalls,
        control: &dyn PureCompileControl,
    ) -> Result<FrozenFragmentCalls, E> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = decode_calls(&fixture.fragment, &fixture.uses, input, &mut work, control)?;
        work.finish()?;
        Ok(result)
    }
    fn special_fixture() -> Fixture {
        special_fixture_with_rows(1)
    }

    fn special_fixture_with_rows(input_rows: usize) -> Fixture {
        let mut builder = FragmentBuilder::new(FragmentId::new(41));
        let input = add_values(&mut builder, input_rows, false);
        let aggregate = builder.reserve_node_id().unwrap();
        let mut outputs = Vec::new();
        let mut aggregate_calls = Vec::new();
        for id in [0, u32::MAX] {
            let id = AggregateCallId::new(id);
            let output = builder
                .add_value(integer(), ValueOrigin::AggregateResult { call: id })
                .unwrap();
            outputs.push(output);
            aggregate_calls.push(AggregateCall {
                id,
                binding: AggregateBinding {
                    state_interpretation: None,
                    function: function(FunctionKind::Aggregate, integer()),
                    phase: AggregatePhase::Single,
                    logical_argument_count: 0,
                    intermediate_type: ValueType::new(DataType::Binary, false),
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    state_format: AggregateStateFormatId::try_new("fixture/count/state-v1")
                        .unwrap(),
                },
                arguments: Box::default(),
                distinct: false,
                order_by: Box::default(),
                output,
            });
        }
        install_node(
            &mut builder,
            aggregate,
            vec![input],
            outputs.clone(),
            NodeKind::Aggregate {
                group_by: Box::default(),
                calls: aggregate_calls.into_boxed_slice(),
                grouping: AggregateGrouping::Complete,
            },
        );
        let window = builder.reserve_node_id().unwrap();
        let expression = builder
            .add_expression(
                window,
                integer(),
                ExprKind::WindowCall {
                    function: function(FunctionKind::Window, integer()),
                    distinct: false,
                    args: Box::default(),
                    function_order_by: Box::default(),
                    frame: None,
                    ignore_nulls: false,
                    aggregate_binding: None,
                },
            )
            .unwrap();
        let output = builder
            .add_value(
                integer(),
                ValueOrigin::Expr {
                    node: window,
                    expr: expression,
                },
            )
            .unwrap();
        outputs.push(output);
        install_node(
            &mut builder,
            window,
            vec![aggregate],
            outputs,
            NodeKind::Window(WindowSpec {
                partition_by: Box::default(),
                order_by: Box::default(),
                expressions: Box::from([WindowExpression { expression, output }]),
            }),
        );
        let table = builder.reserve_node_id().unwrap();
        let output = builder
            .add_value(
                integer(),
                ValueOrigin::NodeOutput {
                    node: table,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        let scalar_fields = function(FunctionKind::Table, integer());
        install_node(
            &mut builder,
            table,
            vec![window],
            vec![output],
            NodeKind::TableFunction {
                function: BoundTableFunction {
                    function_id: scalar_fields.function_id,
                    overload: scalar_fields.overload,
                    argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        Box::default(),
                    ),
                    result_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        Box::from([integer()]),
                    ),

                    legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
                        volatility: scalar_fields.legacy_metadata.as_ref().unwrap().volatility,
                        argument_evaluation: scalar_fields
                            .legacy_metadata
                            .as_ref()
                            .unwrap()
                            .argument_evaluation,
                        failure_behavior: scalar_fields
                            .legacy_metadata
                            .as_ref()
                            .unwrap()
                            .failure_behavior,
                        intrinsic_row_error: scalar_fields
                            .legacy_metadata
                            .as_ref()
                            .unwrap()
                            .intrinsic_row_error,
                        semantic_parameters: Box::default(),
                    }),
                },
                arguments: Box::default(),
                outputs: Box::from([TableFunctionOutput::FunctionResult {
                    result_ordinal: 0,
                    value: output,
                }]),
                left_outer: false,
            },
        );
        let fragment = builder
            .finish_definition(
                table,
                FragmentSink::Noop,
                dop(),
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap();
        // finish_definition already applies the real definition validator.
        let uses = leaf_roots(&fragment);
        let mut calls = vec![];
        // Wide Values roots consume sequential low IDs plus MAX. Relational IDs
        // are independent fresh occurrences, never reused graph roots.
        let special_base = if input_rows == 1 {
            1
        } else {
            u32::try_from(MAX_CONTROL_USE_REFERENCES).unwrap() - 2
        };
        for (call, use_id) in [(0, special_base), (1, special_base + 1)] {
            let context = context(use_id, 0, EvaluationDemand::Value);
            calls.push(FrozenPhysicalCall {
                materialized_writer_input: None,
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                site: PhysicalCallSite::Aggregate {
                    node: aggregate,
                    call,
                },
                context,
                effects: effects(FunctionKind::Aggregate, context.region),
            });
        }
        let window_use = uses
            .flow()
            .uses()
            .values()
            .find(|invocation| invocation.definition == expression)
            .unwrap()
            .context;
        calls.push(FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            site: PhysicalCallSite::Expression(window_use.use_id),
            context: window_use,
            effects: effects(FunctionKind::Window, window_use.region),
        });
        let context = context(special_base + 2, u32::MAX, EvaluationDemand::Value);
        calls.push(FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            site: PhysicalCallSite::Table { node: table },
            context,
            effects: effects(FunctionKind::Table, context.region),
        });
        Fixture {
            fragment,
            uses,
            calls,
        }
    }

    fn rich_effects(domain: EvaluationRegionId) -> CallEffects {
        CallEffects {
            value_stability: FunctionVolatility::Stable,
            own_row_error: FunctionIntrinsicRowError::MayRaise,
            failure_behavior: FunctionFailureBehavior::ReturnsNull,
            null_behavior: FunctionNullBehavior::CalledOnNull,
            argument_control: ArgumentControl::Eager,
            instance_state: FunctionInstanceState::ScalarInstance,
            observable_effects: ObservableEffects {
                rng_sampling: true,
                warnings: true,
                controlled_wait: true,
            },
            environment: Box::from([
                SemanticParameterRef {
                    id: SemanticParameterId::new(u32::MAX),
                    expected_key: SemanticParameterKey::TimeZone,
                },
                SemanticParameterRef {
                    id: SemanticParameterId::new(0),
                    expected_key: SemanticParameterKey::AllowThrowException,
                },
            ]),
            proof_scope: CallProofScope::Region(domain),
        }
    }

    #[test]
    fn actual_scalar_occurrences_preserve_all_nine_fields_ordered_refs_and_independent_policies() {
        let mut fixture = scalar_fixture(2, u32::MAX);
        for (ordinal, call) in fixture.calls.iter_mut().enumerate() {
            call.effects = rich_effects(call.context.region);
            call.decimal_overflow_policy = if ordinal == 0 {
                DecimalOverflowPolicy::ReportError
            } else {
                DecimalOverflowPolicy::OutputNull
            };
        }
        let original = fixture.checked().unwrap();
        let dto = encode_checked(&original, &TestControl::default()).unwrap();
        let checked = decode_checked(&fixture, &dto, &TestControl::default()).unwrap();
        assert_eq!(checked, original);
        assert_eq!(
            dto.entries[0].decimal_overflow_policy,
            Some(wire::DecimalOverflowPolicy::ReportError as i32)
        );
        assert_eq!(
            dto.entries[1].decimal_overflow_policy,
            Some(wire::DecimalOverflowPolicy::OutputNull as i32)
        );
        assert_eq!(
            dto.entries[0]
                .effects
                .as_ref()
                .unwrap()
                .environment
                .iter()
                .map(|r| r.id.unwrap())
                .collect::<Vec<_>>(),
            [u32::MAX, 0]
        );
        assert_eq!(
            fixture.uses.flow().uses()[&ExpressionUseId::new(0)].definition,
            fixture.uses.flow().uses()[&ExpressionUseId::new(u32::MAX)].definition
        );
    }

    #[test]
    fn policy_absence_unspecified_and_unknown_are_never_defaulted() {
        let fixture = scalar_fixture(1, 0);
        let dto = encode_checked(&fixture.checked().unwrap(), &TestControl::default()).unwrap();
        for policy in [None, Some(0), Some(-1), Some(i32::MAX)] {
            let mut wrong = dto.clone();
            wrong.entries[0].decimal_overflow_policy = policy;
            assert!(matches!(
                decode_checked(&fixture, &wrong, &TestControl::default()),
                Err(E::InvalidShape(_))
            ));
        }
    }

    #[test]
    fn regexp_count_wire_source_unknown_and_foreign_tags_never_supply_a_default() {
        let fixture = scalar_fixture(1, 0);
        let dto = encode_checked(&fixture.checked().unwrap(), &TestControl::default()).unwrap();
        assert_eq!(dto.entries[0].regexp_count_pattern_source, None);
        for source in [-1, i32::MAX, 0, 1] {
            let mut wrong = dto.clone();
            wrong.entries[0].regexp_count_pattern_source = Some(source);
            // Unknown tags fail projection; valid tags on a foreign ordinary
            // call fail the real same-snapshot source validator.
            assert!(decode_checked(&fixture, &wrong, &TestControl::default()).is_err());
        }
        let out = decode_checked(&fixture, &dto, &TestControl::default()).unwrap();
        assert!(
            out.entries()
                .values()
                .all(|call| call.regexp_count_pattern_source.is_none())
        );
    }

    #[test]
    fn all_six_sites_preserve_zero_max_ids_and_actual_call_ordinals() {
        for id in [0, u32::MAX] {
            for site in [
                PhysicalCallSite::Expression(ExpressionUseId::new(id)),
                PhysicalCallSite::Aggregate {
                    node: NodeId::new(id),
                    call: u32::MAX,
                },
                PhysicalCallSite::TopNState {
                    node: NodeId::new(id),
                    call: 0,
                },
                PhysicalCallSite::WriterPartial {
                    node: NodeId::new(id),
                    call: u32::MAX,
                },
                PhysicalCallSite::WriterFinal {
                    node: NodeId::new(id),
                    call: 0,
                },
                PhysicalCallSite::Table {
                    node: NodeId::new(id),
                },
            ] {
                assert_eq!(decode_site(&encode_site(site)).unwrap(), site);
            }
        }
        assert!(decode_site(&wire::CallSite { kind: None }).is_err());
        let constructors: [fn(wire::NodeCallSite) -> wire::call_site::Kind; 4] = [
            wire::call_site::Kind::Aggregate,
            wire::call_site::Kind::TopNState,
            wire::call_site::Kind::WriterPartial,
            wire::call_site::Kind::WriterFinal,
        ];
        for kind in constructors {
            assert!(
                decode_site(&wire::CallSite {
                    kind: Some(kind(wire::NodeCallSite {
                        node_id: None,
                        call: 0
                    }))
                })
                .is_err()
            );
        }
    }

    #[test]
    fn contexts_require_both_ids_and_closed_demand_without_rejecting_zero_max() {
        for id in [0, u32::MAX] {
            for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
                let value = context(id, id, demand);
                assert_eq!(decode_context(&encode_context(&value)).unwrap(), value);
            }
        }
        let dto = encode_context(&context(0, u32::MAX, EvaluationDemand::TruthOnly));
        let mut bad = dto;
        bad.use_id = None;
        assert!(decode_context(&bad).is_err());
        bad = dto;
        bad.domain_id = None;
        assert!(decode_context(&bad).is_err());
        for demand in [0, -1, i32::MAX] {
            bad = dto;
            bad.demand = demand;
            assert!(decode_context(&bad).is_err());
        }
    }

    #[test]
    fn every_control_and_proof_variant_projects_exactly_including_higher_order() {
        for value in [
            ArgumentControl::Eager,
            ArgumentControl::TypeOnly,
            ArgumentControl::If,
            ArgumentControl::Coalesce,
            ArgumentControl::SimpleCase,
            ArgumentControl::SearchedCase,
            ArgumentControl::Aggregate,
            ArgumentControl::Window,
            ArgumentControl::Table,
            ArgumentControl::HigherOrder {
                body_ordinal: u32::MAX,
                body_demand: EvaluationDemand::TruthOnly,
            },
            ArgumentControl::HigherOrder {
                body_ordinal: 0,
                body_demand: EvaluationDemand::Value,
            },
        ] {
            assert_eq!(
                decode_argument_control(&encode_argument_control(value)).unwrap(),
                value
            );
        }
        for value in [
            CallProofScope::Unconditional,
            CallProofScope::Region(EvaluationRegionId::new(0)),
            CallProofScope::Region(EvaluationRegionId::new(u32::MAX)),
        ] {
            assert_eq!(decode_proof(&encode_proof(value)).unwrap(), value);
        }
        assert!(decode_argument_control(&wire::ArgumentControl { kind: None }).is_err());
        for value in [0, -1, i32::MAX] {
            assert!(
                decode_argument_control(&wire::ArgumentControl {
                    kind: Some(wire::argument_control::Kind::Simple(value))
                })
                .is_err()
            );
        }
        assert!(
            decode_argument_control(&wire::ArgumentControl {
                kind: Some(wire::argument_control::Kind::HigherOrder(
                    control_wire::HigherOrderControl {
                        body_ordinal: 0,
                        body_demand: 0
                    }
                ))
            })
            .is_err()
        );
        assert!(decode_proof(&wire::ProofScope { kind: None }).is_err());
    }

    #[test]
    fn every_closed_effect_enum_rejects_unspecified_and_unknown_values() {
        for bad in [0, -1, i32::MAX] {
            assert!(decode_stability(bad).is_err());
            assert!(decode_row_error(bad).is_err());
            assert!(decode_failure(bad).is_err());
            assert!(decode_null(bad).is_err());
            assert!(decode_state(bad).is_err());
        }
        for v in [
            FunctionVolatility::Immutable,
            FunctionVolatility::Stable,
            FunctionVolatility::Volatile,
        ] {
            assert_eq!(decode_stability(encode_stability(v)).unwrap(), v);
        }
        for v in [
            FunctionIntrinsicRowError::NoRowError,
            FunctionIntrinsicRowError::MayRaise,
            FunctionIntrinsicRowError::NotRowEvaluated,
        ] {
            assert_eq!(decode_row_error(encode_row_error(v)).unwrap(), v);
        }
        for v in [
            FunctionFailureBehavior::Propagate,
            FunctionFailureBehavior::ReturnsNull,
        ] {
            assert_eq!(decode_failure(encode_failure(v)).unwrap(), v);
        }
        for v in [
            FunctionNullBehavior::Strict,
            FunctionNullBehavior::CalledOnNull,
            FunctionNullBehavior::ControlDefined,
        ] {
            assert_eq!(decode_null(encode_null(v)).unwrap(), v);
        }
        for v in [
            FunctionInstanceState::None,
            FunctionInstanceState::ScalarInstance,
            FunctionInstanceState::AggregateInstance,
            FunctionInstanceState::WindowPartition,
            FunctionInstanceState::TableInstance,
        ] {
            assert_eq!(decode_state(encode_state(v)).unwrap(), v);
        }
    }

    #[test]
    fn mandatory_call_and_effect_messages_are_not_reconstructed_from_legacy_metadata() {
        let fixture = scalar_fixture(1, 0);
        let dto = encode_checked(&fixture.checked().unwrap(), &TestControl::default()).unwrap();
        for field in 0..7 {
            let mut bad = dto.clone();
            match field {
                0 => bad.entries[0].site = None,
                1 => bad.entries[0].context = None,
                2 => bad.entries[0].effects = None,
                3 => bad.entries[0].effects.as_mut().unwrap().argument_control = None,
                4 => bad.entries[0].effects.as_mut().unwrap().observable_effects = None,
                5 => bad.entries[0].effects.as_mut().unwrap().proof_scope = None,
                _ => {
                    bad.entries[0].effects.as_mut().unwrap().proof_scope =
                        Some(wire::ProofScope { kind: None })
                }
            }
            assert!(matches!(
                decode_checked(&fixture, &bad, &TestControl::default()),
                Err(E::InvalidShape(_))
            ));
        }
    }

    #[test]
    fn actual_snapshot_constructor_checks_coverage_context_proof_and_shared_occurrence() {
        let fixture = scalar_fixture(2, 0);
        let dto = encode_checked(&fixture.checked().unwrap(), &TestControl::default()).unwrap();
        let mut bad = dto.clone();
        bad.entries.pop();
        assert!(matches!(
            decode_checked(&fixture, &bad, &TestControl::default()),
            Err(E::Calls(FrozenCallError::MissingSite(_)))
        ));
        let mut bad = dto.clone();
        bad.entries.push(bad.entries[0].clone());
        assert_eq!(
            decode_checked(&fixture, &bad, &TestControl::default()),
            Err(E::Calls(FrozenCallError::DuplicateSite))
        );
        let mut bad = dto.clone();
        bad.entries[0].context = bad.entries[1].context;
        assert_eq!(
            decode_checked(&fixture, &bad, &TestControl::default()),
            Err(E::Calls(FrozenCallError::WrongContext))
        );
        let mut bad = dto.clone();
        bad.entries[0].effects.as_mut().unwrap().proof_scope = Some(encode_proof(
            CallProofScope::Region(EvaluationRegionId::new(u32::MAX)),
        ));
        assert_eq!(
            decode_checked(&fixture, &bad, &TestControl::default()),
            Err(E::Calls(FrozenCallError::WrongProofScope))
        );
        let mut bad = dto.clone();
        bad.entries[0].site = Some(encode_site(PhysicalCallSite::Expression(
            ExpressionUseId::new(71),
        )));
        assert!(matches!(
            decode_checked(&fixture, &bad, &TestControl::default()),
            Err(E::Calls(FrozenCallError::MissingSite(_)))
        ));
        let mut bad = dto;
        bad.entries[0].effects.as_mut().unwrap().argument_control =
            Some(encode_argument_control(ArgumentControl::TypeOnly));
        assert_eq!(
            decode_checked(&fixture, &bad, &TestControl::default()),
            Err(E::Calls(FrozenCallError::WrongControl))
        );
    }

    #[test]
    fn reference_presence_closed_key_uniqueness_and_same_snapshot_are_checked() {
        let mut fixture = scalar_fixture(1, 0);
        fixture.calls[0].effects = rich_effects(fixture.calls[0].context.region);
        let dto = encode_checked(&fixture.checked().unwrap(), &TestControl::default()).unwrap();
        for missing in [true, false] {
            let mut bad = dto.clone();
            let reference = &mut bad.entries[0].effects.as_mut().unwrap().environment[0];
            if missing {
                reference.id = None;
            } else {
                reference.expected_key = 0;
            }
            assert!(matches!(
                decode_checked(&fixture, &bad, &TestControl::default()),
                Err(E::InvalidShape(_))
            ));
        }
        let mut duplicate = dto.clone();
        let environment = &mut duplicate.entries[0].effects.as_mut().unwrap().environment;
        environment[1] = environment[0];
        assert!(matches!(
            decode_checked(&fixture, &duplicate, &TestControl::default()),
            Err(E::Calls(FrozenCallError::InvalidEffects(_)))
        ));
        let foreign = scalar_fixture(1, u32::MAX);
        let control = TestControl::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        assert_eq!(
            decode_calls(&foreign.fragment, &fixture.uses, &dto, &mut work, &control),
            Err(E::Calls(FrozenCallError::Roots(
                RootUseBindingError::WrongFragment
            )))
        );
    }

    #[test]
    fn original_control_survives_projection_and_nested_constructor_entry_256_and_tails() {
        let fixture = scalar_fixture(320, u32::MAX);
        let checked = fixture.checked().unwrap();
        let good_encode = TestControl::default();
        let dto = encode_checked(&checked, &good_encode).unwrap();
        let encode_trace = good_encode.events.into_inner().unwrap();
        let good_decode = TestControl::default();
        decode_checked(&fixture, &dto, &good_decode).unwrap();
        let decode_trace = good_decode.events.into_inner().unwrap();
        for trace in [&encode_trace, &decode_trace] {
            assert_eq!(trace[0].1, 0);
            assert!(trace.iter().any(|(_, units)| *units == 256));
            assert!(trace.iter().any(|(_, units)| *units > 0 && *units < 256));
        }
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 1..=encode_trace.len() {
                let stop = TestControl {
                    failure: Some((at, error.clone())),
                    events: Mutex::default(),
                };
                assert_eq!(
                    encode_checked(&checked, &stop),
                    Err(E::Control(error.clone()))
                );
                assert_eq!(stop.events.lock().unwrap().len(), at);
            }
            for at in 1..=decode_trace.len() {
                let stop = TestControl {
                    failure: Some((at, error.clone())),
                    events: Mutex::default(),
                };
                assert_eq!(
                    decode_checked(&fixture, &dto, &stop),
                    Err(E::Control(error.clone()))
                );
                assert_eq!(stop.events.lock().unwrap().len(), at);
            }
        }
    }

    #[test]
    fn real_relational_and_window_calls_preserve_lifecycle_context_and_require_exact_coverage() {
        let mut fixture = special_fixture();
        for call in &mut fixture.calls {
            call.decimal_overflow_policy = DecimalOverflowPolicy::ReportError;
        }
        let checked = fixture.checked().unwrap();
        let dto = encode_checked(&checked, &TestControl::default()).unwrap();
        let result = decode_checked(&fixture, &dto, &TestControl::default()).unwrap();
        assert_eq!(result, checked);
        assert_eq!(result.entries().len(), 4);
        assert_eq!(
            result
                .entries()
                .keys()
                .filter(|site| matches!(site, PhysicalCallSite::Aggregate { .. }))
                .count(),
            2
        );
        assert_eq!(
            result
                .entries()
                .keys()
                .filter(|site| matches!(site, PhysicalCallSite::Table { .. }))
                .count(),
            1
        );
        let mut missing = dto.clone();
        let index = missing
            .entries
            .iter()
            .position(|call| {
                matches!(
                    call.site.as_ref().unwrap().kind,
                    Some(wire::call_site::Kind::Aggregate(_))
                )
            })
            .unwrap();
        missing.entries.remove(index);
        assert!(matches!(
            decode_checked(&fixture, &missing, &TestControl::default()),
            Err(E::Calls(FrozenCallError::MissingSite(
                PhysicalCallSite::Aggregate { .. }
            )))
        ));
        let mut bad = dto.clone();
        let table = bad
            .entries
            .iter_mut()
            .find(|call| {
                matches!(
                    call.site.as_ref().unwrap().kind,
                    Some(wire::call_site::Kind::TableNodeId(_))
                )
            })
            .unwrap();
        table.context.as_mut().unwrap().demand = encode_demand(EvaluationDemand::TruthOnly);
        assert_eq!(
            decode_checked(&fixture, &bad, &TestControl::default()),
            Err(E::Calls(FrozenCallError::WrongContext))
        );
        let mut bad = dto;
        let table = bad
            .entries
            .iter_mut()
            .find(|call| {
                matches!(
                    call.site.as_ref().unwrap().kind,
                    Some(wire::call_site::Kind::TableNodeId(_))
                )
            })
            .unwrap();
        table.context.as_mut().unwrap().use_id = Some(0);
        assert_eq!(
            decode_checked(&fixture, &bad, &TestControl::default()),
            Err(E::Calls(FrozenCallError::SharedUse))
        );
    }

    #[test]
    fn canonical_calls_with_environment_references_can_exceed_control_reference_count() {
        // One shared definition has 32769 actual independent Values occurrences.
        // The closed TimeZone key is valid for each call; their dynamic items
        // exceed 65536 without increasing the control-flow reference count.
        let mut fixture = scalar_fixture(MAX_CONTROL_USE_REFERENCES / 2 + 1, u32::MAX);
        for call in &mut fixture.calls {
            call.effects.environment = Box::from([SemanticParameterRef {
                id: SemanticParameterId::new(0),
                expected_key: SemanticParameterKey::TimeZone,
            }]);
        }
        let checked = fixture.checked().unwrap();
        assert_eq!(checked.entries().len(), 32769);
        let dto = encode_checked(&checked, &TestControl::default()).unwrap();
        assert_eq!(dto.entries.len(), 32769);
        let decoded = decode_checked(&fixture, &dto, &TestControl::default()).unwrap();
        assert_eq!(decoded, checked);
    }

    #[test]
    fn cumulative_calls_and_environment_bounds_precede_output_allocation() {
        assert_eq!(
            count_references(MAX_CONTROL_USE_REFERENCES, 1).unwrap(),
            MAX_CONTROL_USE_REFERENCES + 1
        );
        assert_eq!(
            count_references(MAX_PLAN_DERIVED_CUT_ITEMS - 1, 1).unwrap(),
            MAX_PLAN_DERIVED_CUT_ITEMS
        );
        assert!(count_references(MAX_PLAN_DERIVED_CUT_ITEMS, 1).is_err());
        assert!(count_references(usize::MAX, 1).is_err());
        assert!(count_references(0, MAX_SEMANTIC_PARAMETERS + 1).is_err());
        let fixture = scalar_fixture(1, 0);
        let dto = encode_checked(&fixture.checked().unwrap(), &TestControl::default()).unwrap();
        let too_many_calls = wire::FrozenCalls {
            entries: vec![dto.entries[0].clone(); MAX_CONTROL_USE_REFERENCES + 1],
        };
        assert!(matches!(
            decode_checked(&fixture, &too_many_calls, &TestControl::default()),
            Err(E::InvalidShape(_))
        ));
        // A small table with duplicate occurrences reaches the canonical
        // structural rejection rather than a fabricated reference-budget error.
        let duplicate = wire::FrozenCalls {
            entries: vec![dto.entries[0].clone(); 2],
        };
        assert_eq!(
            decode_checked(&fixture, &duplicate, &TestControl::default()),
            Err(E::Calls(FrozenCallError::DuplicateSite))
        );
        let mut many_refs = dto;
        many_refs.entries[0].effects.as_mut().unwrap().environment = vec![
                wire::SemanticParameterRef {
                    id: Some(0),
                    expected_key: wire::SemanticParameterKey::TimeZone as i32
                };
                MAX_SEMANTIC_PARAMETERS + 1
            ];
        assert!(matches!(
            decode_checked(&fixture, &many_refs, &TestControl::default()),
            Err(E::InvalidShape(_))
        ));
    }
    mod observed_resources_tests {
        include!("owned_resources/tests.rs");
    }
}

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
mod writer_projection_creator_tests {
    use super::*;
    use novarocks_type_contract::{
        CompilePhase, EvaluationDemand, SemanticParameterId, SemanticParameterKey,
        ValueSchemaCreation, ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= 256);
            Ok(())
        }
    }
    fn source() -> FrozenPhysicalCall {
        let context = ExpressionEffectContext {
            use_id: ExpressionUseId::new(41),
            region: EvaluationRegionId::new(17),
            demand: EvaluationDemand::Value,
        };
        FrozenPhysicalCall {
            site: PhysicalCallSite::WriterPartial {
                node: NodeId::new(3),
                call: 0,
            },
            context,
            materialized_writer_input: Some(ExpressionEffectContext {
                use_id: ExpressionUseId::new(42),
                region: EvaluationRegionId::new(18),
                demand: EvaluationDemand::Value,
            }),
            effects: CallEffects {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: ArgumentControl::Aggregate,
                instance_state: FunctionInstanceState::AggregateInstance,
                observable_effects: ObservableEffects::NONE,
                environment: Box::from([novarocks_type_contract::SemanticParameterRef {
                    id: SemanticParameterId::new(7),
                    expected_key: SemanticParameterKey::TimeZone,
                }]),
                proof_scope: CallProofScope::Region(context.region),
            },
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            temporal_source: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
        }
    }
    struct Scope(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Scope {}
    impl Drop for Scope {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn writer_projection_roundtrip_keeps_exact_source_context_and_last_creator() {
        let source = source();
        let control = Control;
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let raw = encode_frozen_writer_call_with_creator_in::<std::convert::Infallible>(
            &source, None, &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = Arc::new(Scope(drops.clone()));
        let storage = ValueTypeStorageLifetime::held(owner.clone());
        let mut actual = Vec::new();
        let mut opaque = Vec::new();
        let mut request = |layout: std::alloc::Layout| {
            actual.push(layout);
            Ok::<_, std::convert::Infallible>(())
        };
        let mut upper = |bytes| {
            opaque.push(bytes);
            Ok::<_, std::convert::Infallible>(())
        };
        let mut creation = ValueSchemaCreation::new(storage, &mut request, &mut upper);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let created =
            decode_frozen_writer_call_with_creator_in(&raw, source.site, &mut creation, &mut work)
                .unwrap();
        work.finish().unwrap();
        assert_eq!(created.call(), &source);
        drop(creation);
        drop(owner);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(actual.iter().any(|request| request.size() > 0));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(created);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn writer_projection_rejects_missing_receipt_and_preserves_original_host_without_tail() {
        use crate::host_projection_v2::ProjectionFailure;
        let source = source();
        let control = Control;
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let raw = encode_frozen_writer_call_with_creator_in::<std::convert::Infallible>(
            &source, None, &mut work,
        )
        .unwrap();
        let original = Arc::new(17u64);
        let expected = original.clone();
        let mut requests = 0usize;
        let mut request = |_: std::alloc::Layout| {
            requests += 1;
            assert_eq!(requests, 1, "allocation after first caller refusal");
            Err(original.clone())
        };
        let mut upper = |_: usize| -> Result<(), Arc<u64>> {
            panic!("opaque callback after first caller refusal")
        };
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut upper);
        let mut absent = raw.clone();
        absent.materialized_writer_input = None;
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        assert!(matches!(
            decode_frozen_writer_call_with_creator_in(
                &absent,
                source.site,
                &mut creation,
                &mut work
            ),
            Err(ProjectionFailure::Codec(E::InvalidShape(_)))
        ));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let result =
            decode_frozen_writer_call_with_creator_in(&raw, source.site, &mut creation, &mut work);
        assert!(
            matches!(result,Err(ProjectionFailure::Host(ref cause)) if Arc::ptr_eq(cause,&expected))
        );
        drop(creation);
        assert_eq!(requests, 1);
    }
    #[test]
    fn whole_call_parts_keep_original_host_before_any_child_or_final_validator() {
        use crate::host_projection_v2::ProjectionFailure;
        let source = source();
        let control = Control;
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let raw = encode_frozen_writer_call_with_creator_in::<std::convert::Infallible>(
            &source, None, &mut work,
        )
        .unwrap();
        let source = wire::FrozenCalls { entries: vec![raw] };
        let invoice = std::mem::size_of::<wire::FrozenCalls>()
            + std::alloc::Layout::array::<wire::FrozenCall>(source.entries.capacity())
                .unwrap()
                .size()
            + std::alloc::Layout::array::<wire::SemanticParameterRef>(
                source.entries[0]
                    .effects
                    .as_ref()
                    .unwrap()
                    .environment
                    .capacity(),
            )
            .unwrap()
            .size();
        let original = Arc::new(17u64);
        let expected = original.clone();
        let mut count = 0;
        let mut exact = |_: std::alloc::Layout| {
            count += 1;
            assert_eq!(count, 1, "allocation after original refusal");
            Err(original.clone())
        };
        let mut opaque =
            |_: usize| -> Result<(), Arc<u64>> { panic!("opaque tail after original refusal") };
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let result = decode_frozen_call_parts_input_derived_with_creator_in(
            &source,
            invoice,
            &mut creation,
            &mut work,
        );
        assert!(
            matches!(result,Err(ProjectionFailure::Host(cause)) if Arc::ptr_eq(&cause,&expected))
        );
        drop(creation);
        assert_eq!(count, 1);
    }
    #[test]
    fn whole_call_parts_keep_exact_sources_and_last_finite_creator_without_fragment_proof() {
        let original = source();
        let control = Control;
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let raw = encode_frozen_writer_call_with_creator_in::<std::convert::Infallible>(
            &original, None, &mut work,
        )
        .unwrap();
        let source = wire::FrozenCalls { entries: vec![raw] };
        let invoice = std::mem::size_of::<wire::FrozenCalls>()
            + std::alloc::Layout::array::<wire::FrozenCall>(source.entries.capacity())
                .unwrap()
                .size()
            + std::alloc::Layout::array::<wire::SemanticParameterRef>(
                source.entries[0]
                    .effects
                    .as_ref()
                    .unwrap()
                    .environment
                    .capacity(),
            )
            .unwrap()
            .size();
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = Arc::new(Scope(drops.clone()));
        let mut requests = Vec::new();
        let mut exact = |layout: std::alloc::Layout| {
            requests.push(layout);
            Ok::<_, std::convert::Infallible>(())
        };
        let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
        let mut creation = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::held(owner.clone()),
            &mut exact,
            &mut opaque,
        );
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let (parts, facts) = decode_frozen_call_parts_input_derived_with_creator_in(
            &source,
            invoice,
            &mut creation,
            &mut work,
        )
        .unwrap();
        assert_eq!(parts.calls(), &[original]);
        assert!(facts.list_item_count >= 1);
        assert!(facts.allocation_request_bytes_upper_bound > 0);
        drop(creation);
        drop(owner);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(requests.iter().any(|layout| layout.size() > 0));
        let (calls, storage) = parts.into_parts();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(calls);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(storage);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
