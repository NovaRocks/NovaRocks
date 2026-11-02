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

use novarocks_physical_plan::{
    ExprId, ExpressionRootError, ExpressionRootRole, ExpressionRootSite, Fragment, JoinSide,
    NodeId, PhysicalRootUses, RootUseBindingError, physical_expression_roots_header_resource_facts,
    root_use_binding_header_resource_facts,
};
use novarocks_proto_models::physical_control_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, DomainGuard,
    EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionControlFlowError,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    GuardKind, MAX_CONTROL_DEFINITIONS, MAX_CONTROL_USE_REFERENCES, PureCompileControl,
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
    Roots(RootUseBindingError),
}
impl fmt::Display for ControlCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::InvalidShape(message) => f.write_str(message),
            Self::Flow(error) => error.fmt(f),
            Self::Roots(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ControlCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Flow(source) => Some(source),
            Self::Roots(source) => Some(source),
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
impl From<RootUseBindingError> for ControlCodecError {
    fn from(error: RootUseBindingError) -> Self {
        match error {
            RootUseBindingError::Control(error)
            | RootUseBindingError::Roots(ExpressionRootError::Control(error)) => {
                Self::Control(error)
            }
            error => Self::Roots(error),
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
        ControlShape::Case { .. }
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
            _ => Err(ControlCodecError::InvalidShape(
                "unknown or unspecified control shape",
            )),
        },
        Kind::BetweenNegated(negated) => Ok(ControlShape::Between { negated: *negated }),
        Kind::MembershipNegated(negated) => Ok(ControlShape::Membership { negated: *negated }),
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
fn encode_guard(guard: DomainGuard) -> wire::DomainGuard {
    use wire::domain_guard::Kind;
    let kind = match guard.kind {
        GuardKind::IfThen => Kind::Simple(wire::SimpleGuard::IfThen as i32),
        GuardKind::IfElse => Kind::Simple(wire::SimpleGuard::IfElse as i32),
        GuardKind::CaseElse => Kind::Simple(wire::SimpleGuard::CaseElse as i32),
        GuardKind::LambdaInvocation => Kind::Simple(wire::SimpleGuard::LambdaInvocation as i32),
        GuardKind::CoalesceAfterNull { ordinal } => Kind::CoalesceAfterNullOrdinal(ordinal),
        GuardKind::CaseWhen { arm } => Kind::CaseWhenArm(arm),
        GuardKind::CaseThen { arm } => Kind::CaseThenArm(arm),
        GuardKind::BetweenAfterSource { ordinal } => Kind::BetweenAfterSourceOrdinal(ordinal),
        GuardKind::MembershipAfterSource { ordinal } => Kind::MembershipAfterSourceOrdinal(ordinal),
        GuardKind::TemporalAfterSource { ordinal } => Kind::TemporalAfterSourceOrdinal(ordinal),
        GuardKind::TemporalInvocationNull => {
            Kind::Simple(wire::SimpleGuard::TemporalInvocationNull as i32)
        }
    };
    wire::DomainGuard {
        owner_use_id: Some(guard.owner.get()),
        kind: Some(kind),
    }
}
fn decode_guard(guard: &wire::DomainGuard) -> Result<DomainGuard, ControlCodecError> {
    use wire::domain_guard::Kind;
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
        Kind::TemporalAfterSourceOrdinal(ordinal) => {
            GuardKind::TemporalAfterSource { ordinal: *ordinal }
        }
    };
    Ok(DomainGuard { owner, kind })
}
pub(crate) fn encode_site(site: ExpressionRootSite) -> wire::RootSite {
    use wire::root_site::Role;
    let role = match site.role {
        ExpressionRootRole::ScanResidual { predicate } => Role::ScanResidual(predicate),
        ExpressionRootRole::ScanDerived { derived } => Role::ScanDerived(derived),
        ExpressionRootRole::FilterPredicate { predicate } => Role::FilterPredicate(predicate),
        ExpressionRootRole::ProjectOutput { expression } => Role::ProjectOutput(expression),
        ExpressionRootRole::AggregateGroup { group } => Role::AggregateGroup(group),
        ExpressionRootRole::AggregateArgument { call, argument } => {
            Role::AggregateArgument(wire::CallArgumentRoot { call, argument })
        }
        ExpressionRootRole::AggregateOrder { call, key } => {
            Role::AggregateOrder(wire::CallOrderRoot { call, key })
        }
        ExpressionRootRole::JoinKey { key, side } => Role::JoinKey(wire::JoinKeyRoot {
            key,
            side: match side {
                JoinSide::Left => wire::JoinSide::Left as i32,
                JoinSide::Right => wire::JoinSide::Right as i32,
            },
        }),
        ExpressionRootRole::NaaValueEqualKey { key, side } => {
            Role::NaaValueEqualKey(wire::JoinKeyRoot {
                key,
                side: match side {
                    JoinSide::Left => wire::JoinSide::Left as i32,
                    JoinSide::Right => wire::JoinSide::Right as i32,
                },
            })
        }
        ExpressionRootRole::NaaScopeKey { key, side } => Role::NaaScopeKey(wire::JoinKeyRoot {
            key,
            side: match side {
                JoinSide::Left => wire::JoinSide::Left as i32,
                JoinSide::Right => wire::JoinSide::Right as i32,
            },
        }),
        ExpressionRootRole::NaaEligibility => Role::NaaEligibility(wire::Empty {}),
        ExpressionRootRole::HashJoinResidual => Role::HashJoinResidual(wire::Empty {}),
        ExpressionRootRole::NestLoopPredicate => Role::NestLoopPredicate(wire::Empty {}),
        ExpressionRootRole::SortOrder { key } => Role::SortOrder(key),
        ExpressionRootRole::SortPartition { key } => Role::SortPartition(key),
        ExpressionRootRole::TopNOrder { key } => Role::TopNOrder(key),
        ExpressionRootRole::TopNGroup { group } => Role::TopNGroup(group),
        ExpressionRootRole::TopNStateArgument { call, argument } => {
            Role::TopNStateArgument(wire::CallArgumentRoot { call, argument })
        }
        ExpressionRootRole::TopNStateOrder { call, key } => {
            Role::TopNStateOrder(wire::CallOrderRoot { call, key })
        }
        ExpressionRootRole::WindowPartition { key } => Role::WindowPartition(key),
        ExpressionRootRole::WindowOrder { key } => Role::WindowOrder(key),
        ExpressionRootRole::WindowCall { call } => Role::WindowCall(call),
        ExpressionRootRole::ValuesCell { row, column } => {
            Role::ValuesCell(wire::ValuesRoot { row, column })
        }
        ExpressionRootRole::SeriesStart => Role::SeriesStart(wire::Empty {}),
        ExpressionRootRole::SeriesStop => Role::SeriesStop(wire::Empty {}),
        ExpressionRootRole::SeriesStep => Role::SeriesStep(wire::Empty {}),
        ExpressionRootRole::TableFunctionArgument { argument } => {
            Role::TableFunctionArgument(argument)
        }
        ExpressionRootRole::UnpivotConstant { mapping, constant } => {
            Role::UnpivotConstant(wire::MappingConstantRoot { mapping, constant })
        }
        ExpressionRootRole::ChangePredicate { event } => Role::ChangePredicate(event),
        ExpressionRootRole::ChangeAssignment { event, assignment } => {
            Role::ChangeAssignment(wire::ChangeAssignmentRoot { event, assignment })
        }
        ExpressionRootRole::FinishUnpivotConstant { mapping, constant } => {
            Role::FinishUnpivotConstant(wire::MappingConstantRoot { mapping, constant })
        }
    };
    wire::RootSite {
        node_id: Some(site.node.get()),
        role: Some(role),
    }
}
pub(crate) fn decode_site(site: &wire::RootSite) -> Result<ExpressionRootSite, ControlCodecError> {
    use wire::root_site::Role;
    let node = NodeId::new(required(site.node_id, "root node is missing")?);
    let role = match site
        .role
        .as_ref()
        .ok_or(ControlCodecError::InvalidShape("root role is missing"))?
    {
        Role::ScanResidual(value) => ExpressionRootRole::ScanResidual { predicate: *value },
        Role::ScanDerived(value) => ExpressionRootRole::ScanDerived { derived: *value },
        Role::FilterPredicate(value) => ExpressionRootRole::FilterPredicate { predicate: *value },
        Role::ProjectOutput(value) => ExpressionRootRole::ProjectOutput { expression: *value },
        Role::AggregateGroup(value) => ExpressionRootRole::AggregateGroup { group: *value },
        Role::AggregateArgument(value) => ExpressionRootRole::AggregateArgument {
            call: value.call,
            argument: value.argument,
        },
        Role::AggregateOrder(value) => ExpressionRootRole::AggregateOrder {
            call: value.call,
            key: value.key,
        },
        Role::JoinKey(value) => ExpressionRootRole::JoinKey {
            key: value.key,
            side: match wire::JoinSide::try_from(value.side) {
                Ok(wire::JoinSide::Left) => JoinSide::Left,
                Ok(wire::JoinSide::Right) => JoinSide::Right,
                _ => {
                    return Err(ControlCodecError::InvalidShape(
                        "unknown or unspecified join side",
                    ));
                }
            },
        },
        Role::NaaValueEqualKey(value) => ExpressionRootRole::NaaValueEqualKey {
            key: value.key,
            side: match wire::JoinSide::try_from(value.side) {
                Ok(wire::JoinSide::Left) => JoinSide::Left,
                Ok(wire::JoinSide::Right) => JoinSide::Right,
                _ => {
                    return Err(ControlCodecError::InvalidShape(
                        "unknown or unspecified join side",
                    ));
                }
            },
        },
        Role::NaaScopeKey(value) => ExpressionRootRole::NaaScopeKey {
            key: value.key,
            side: match wire::JoinSide::try_from(value.side) {
                Ok(wire::JoinSide::Left) => JoinSide::Left,
                Ok(wire::JoinSide::Right) => JoinSide::Right,
                _ => {
                    return Err(ControlCodecError::InvalidShape(
                        "unknown or unspecified join side",
                    ));
                }
            },
        },
        Role::NaaEligibility(_) => ExpressionRootRole::NaaEligibility,
        Role::HashJoinResidual(_) => ExpressionRootRole::HashJoinResidual,
        Role::NestLoopPredicate(_) => ExpressionRootRole::NestLoopPredicate,
        Role::SortOrder(value) => ExpressionRootRole::SortOrder { key: *value },
        Role::SortPartition(value) => ExpressionRootRole::SortPartition { key: *value },
        Role::TopNOrder(value) => ExpressionRootRole::TopNOrder { key: *value },
        Role::TopNGroup(value) => ExpressionRootRole::TopNGroup { group: *value },
        Role::TopNStateArgument(value) => ExpressionRootRole::TopNStateArgument {
            call: value.call,
            argument: value.argument,
        },
        Role::TopNStateOrder(value) => ExpressionRootRole::TopNStateOrder {
            call: value.call,
            key: value.key,
        },
        Role::WindowPartition(value) => ExpressionRootRole::WindowPartition { key: *value },
        Role::WindowOrder(value) => ExpressionRootRole::WindowOrder { key: *value },
        Role::WindowCall(value) => ExpressionRootRole::WindowCall { call: *value },
        Role::ValuesCell(value) => ExpressionRootRole::ValuesCell {
            row: value.row,
            column: value.column,
        },
        Role::SeriesStart(_) => ExpressionRootRole::SeriesStart,
        Role::SeriesStop(_) => ExpressionRootRole::SeriesStop,
        Role::SeriesStep(_) => ExpressionRootRole::SeriesStep,
        Role::TableFunctionArgument(value) => {
            ExpressionRootRole::TableFunctionArgument { argument: *value }
        }
        Role::UnpivotConstant(value) => ExpressionRootRole::UnpivotConstant {
            mapping: value.mapping,
            constant: value.constant,
        },
        Role::ChangePredicate(value) => ExpressionRootRole::ChangePredicate { event: *value },
        Role::ChangeAssignment(value) => ExpressionRootRole::ChangeAssignment {
            event: value.event,
            assignment: value.assignment,
        },
        Role::FinishUnpivotConstant(value) => ExpressionRootRole::FinishUnpivotConstant {
            mapping: value.mapping,
            constant: value.constant,
        },
    };
    Ok(ExpressionRootSite { node, role })
}

/// Canonical table order follows the checked sparse IDs and typed root sites.
/// Definition sharing retains distinct invocation occurrences and demands.
pub fn encode_expression_control(
    roots: &PhysicalRootUses,
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
    roots: &PhysicalRootUses,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::ExpressionControl, ControlCodecError> {
    let mut model = encode_model(roots, 0, unbounded())?;
    encode_in(roots, &mut model, &mut |_| Ok(()), false, work).map(|encoded| encoded.0)
}

/// Full Control component sender using the actual immutable fragment and root
/// source, the caller's work scope and one union source invoice. No footer.
pub fn encode_expression_control_observed(
    fragment: &Fragment,
    roots: &PhysicalRootUses,
    source_retained_bytes: usize,
    limits: ControlProjectionLimits,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::ExpressionControl, ControlProjectionFacts), ControlCodecError> {
    let mut m = encode_model(roots, source_retained_bytes, limits)?;
    m.planned_child = physical_expression_roots_header_resource_facts(fragment)?;
    m.gate(admit)?;
    m.floor(roots.source_retained_floor()?)?;
    let mut child = ControlOwnedResourceFacts::default();
    let mut ordinary = None;
    let checked = roots.validate_fragment_in(
        fragment,
        &mut |f| {
            child = *f;
            match m.gate_with(*f, admit) {
                Ok(_) => Ok(()),
                Err(e) => {
                    let c = match &e {
                        ControlCodecError::Control(c) => c.clone(),
                        _ => CompileControlError::from_domain(e.clone()),
                    };
                    ordinary = Some(e);
                    Err(c)
                }
            }
        },
        work,
    );
    if let Some(e) = ordinary {
        return Err(e);
    }
    checked?;
    m.resources.merge(child)?;
    m.planned_child = ControlOwnedResourceFacts::default();
    encode_in(roots, &mut m, admit, true, work)
}
fn encode_model(
    roots: &PhysicalRootUses,
    b: usize,
    limits: ControlProjectionLimits,
) -> Result<Model, ControlCodecError> {
    let f = roots.flow();
    let mut m = Model::new(
        f.domains().len(),
        f.use_reference_count(),
        roots.bindings().len(),
        b,
        limits,
    );
    m.resources
        .buffer::<wire::EvaluationDomain>(f.domains().len(), 1)?;
    m.resources
        .buffer::<wire::ExpressionUse>(f.uses().len(), 1)?;
    m.resources
        .buffer::<wire::RootBinding>(roots.bindings().len(), 1)?;
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
            control_resource_add(f.domains().len(), f.use_reference_count())?,
            roots.bindings().len(),
        )?,
        16,
    )?)?;
    Ok(m)
}
fn encode_in(
    roots: &PhysicalRootUses,
    m: &mut Model,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    observed: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::ExpressionControl, ControlProjectionFacts), ControlCodecError> {
    m.gate(admit)?;
    let flow = roots.flow();
    let mut domains = Vec::new();
    reserve(&mut domains, flow.domains().len(), observed, work)?;
    for domain in flow.domains().values() {
        domains.push(wire::EvaluationDomain {
            id: domain.id.get(),
            parent_domain_id: domain.parent.map(EvaluationDomainId::get),
            guard: domain.guard.map(encode_guard),
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
            domain_id: Some(invocation.context.domain.get()),
            demand: encode_demand(invocation.context.demand),
            control: Some(encode_shape(invocation.control)),
            argument_use_ids: arguments,
        });
        work.step()?;
    }
    let mut bindings = Vec::new();
    reserve(&mut bindings, roots.bindings().len(), observed, work)?;
    for (site, id) in roots.bindings() {
        bindings.push(wire::RootBinding {
            use_id: Some(id.get()),
            site: Some(encode_site(*site)),
        });
        work.step()?;
    }

    let facts = m.gate(admit)?;
    Ok((
        wire::ExpressionControl {
            domains,
            uses,
            roots: bindings,
        },
        facts,
    ))
}

/// Original convenience facade. The new observed component below borrows a
/// parent scope and never invokes another owner through this facade.
pub fn decode_expression_control(
    fragment: &Fragment,
    input: &wire::ExpressionControl,
    control: &dyn PureCompileControl,
) -> Result<PhysicalRootUses, ControlCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let initial = original_decode_header(input);
    if let Err(error) = initial {
        work.finish()?;
        return Err(error);
    }
    let mut m = decode_model(fragment, input, 0, unbounded())?;
    let projected = project_decode_in(input, &mut m, &mut |_| Ok(()), None, false, &mut work);
    if let Err(ControlCodecError::Control(c)) = &projected {
        return Err(c.clone().into());
    }
    work.finish()?;
    let (domains, uses, bindings) = projected?;
    let flow = ExpressionControlFlow::try_new(
        domains,
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        control,
    )?;
    Ok(PhysicalRootUses::try_new(
        fragment, flow, bindings, control,
    )?)
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
    if input.domains.len() > MAX_CONTROL_DEFINITIONS
        || input.uses.len() > MAX_CONTROL_USE_REFERENCES
        || input.roots.len() > MAX_CONTROL_USE_REFERENCES
    {
        return Err(ExpressionControlFlowError::TooManyItems.into());
    }
    Ok(())
}
fn decode_model(
    fragment: &Fragment,
    input: &wire::ExpressionControl,
    b: usize,
    limits: ControlProjectionLimits,
) -> Result<Model, ControlCodecError> {
    let mut model = decode_parts_model(input, b, limits)?;
    let mut future = novarocks_type_contract::ControlResourceCounter::default();
    future.merge(model.future)?;
    future.merge(physical_expression_roots_header_resource_facts(fragment)?)?;
    model.future = future.facts();
    Ok(model)
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
        Some(limits) => Model::new(
            input.domains.len(),
            input.uses.len(),
            input.roots.len(),
            b,
            limits,
        ),
        None => Model::input_derived(input.domains.len(), input.uses.len(), input.roots.len(), b),
    };
    m.resources
        .buffer::<ExpressionEvaluationDomain>(input.domains.len(), 1)?;
    m.resources
        .buffer::<ExpressionInvocation<ExprId>>(input.uses.len(), 1)?;
    m.resources
        .buffer::<(ExpressionRootSite, ExpressionUseId)>(input.roots.len(), 1)?;
    m.resources.work(control_resource_mul(
        control_resource_add(
            control_resource_add(input.domains.len(), input.uses.len())?,
            input.roots.len(),
        )?,
        16,
    )?)?;
    let mut future = novarocks_type_contract::ControlResourceCounter::default();
    future.merge(expression_control_flow_header_resource_facts::<ExprId>(
        input.domains.len(),
        input.uses.len(),
    )?)?;
    future.merge(root_use_binding_header_resource_facts(input.roots.len())?)?;

    m.future = future.facts();
    Ok(m)
}
pub fn decode_expression_control_observed(
    fragment: &Fragment,
    input: &wire::ExpressionControl,
    source_retained_bytes: usize,
    limits: ControlProjectionLimits,
    admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(PhysicalRootUses, ControlProjectionFacts), ControlCodecError> {
    let mut m = decode_model(fragment, input, source_retained_bytes, limits)?;
    m.gate(admit)?;
    let backing_floor = control_resource_add(
        control_resource_add(
            Layout::array::<wire::EvaluationDomain>(input.domains.capacity())
                .map_err(|_| CompileControlError::ResourceExhausted)?
                .size(),
            Layout::array::<wire::ExpressionUse>(input.uses.capacity())
                .map_err(|_| CompileControlError::ResourceExhausted)?
                .size(),
        )?,
        Layout::array::<wire::RootBinding>(input.roots.capacity())
            .map_err(|_| CompileControlError::ResourceExhausted)?
            .size(),
    )?;
    let floor = control_resource_add(
        std::mem::size_of::<wire::ExpressionControl>(),
        backing_floor,
    )?;
    m.floor(floor)?;
    let (domains, uses, bindings) =
        project_decode_in(input, &mut m, admit, Some(floor), true, work)?;
    // Replace the known future geometry by this same author's actual prefix;
    // completed child contributions enter cumulative resources only once.
    let mut root_plan = novarocks_type_contract::ControlResourceCounter::default();
    root_plan.merge(root_use_binding_header_resource_facts(input.roots.len())?)?;
    root_plan.merge(physical_expression_roots_header_resource_facts(fragment)?)?;
    m.future = root_plan.facts();
    let mut planned = novarocks_type_contract::ControlResourceCounter::default();
    planned.merge(expression_control_flow_header_resource_facts::<ExprId>(
        input.domains.len(),
        input.uses.len(),
    )?)?;
    planned.merge(expression_control_flow_edge_resource_facts(
        m.counts[1] - input.uses.len(),
        input.domains.len(),
        input.uses.len(),
    )?)?;
    m.planned_child = planned.facts();
    let mut child = ControlOwnedResourceFacts::default();
    let mut ordinary = None;
    let flow = ExpressionControlFlow::try_new_in(
        domains,
        uses,
        fragment.expressions(),
        &mut |f| {
            child = *f;
            match m.gate_with(*f, admit) {
                Ok(_) => Ok(()),
                Err(e) => {
                    let c = match &e {
                        ControlCodecError::Control(c) => c.clone(),
                        _ => CompileControlError::from_domain(e.clone()),
                    };
                    ordinary = Some(e);
                    Err(c)
                }
            }
        },
        work,
    );
    if let Some(e) = ordinary {
        return Err(e);
    }
    let flow = flow?;
    m.resources.merge(child)?;
    child = ControlOwnedResourceFacts::default();
    m.planned_child = root_plan.facts();
    m.future = ControlOwnedResourceFacts::default();
    let roots = PhysicalRootUses::try_new_in(
        fragment,
        flow,
        bindings,
        &mut |f| {
            child = *f;
            match m.gate_with(*f, admit) {
                Ok(_) => Ok(()),
                Err(e) => {
                    let c = match &e {
                        ControlCodecError::Control(c) => c.clone(),
                        _ => CompileControlError::from_domain(e.clone()),
                    };
                    ordinary = Some(e);
                    Err(c)
                }
            }
        },
        work,
    );
    if let Some(e) = ordinary {
        return Err(e);
    }
    let roots = roots?;
    m.resources.merge(child)?;
    m.planned_child = ControlOwnedResourceFacts::default();
    let facts = m.gate(admit)?;
    Ok((roots, facts))
}
type ControlProjectionParts = (
    Vec<ExpressionEvaluationDomain>,
    Vec<ExpressionInvocation<ExprId>>,
    Vec<(ExpressionRootSite, ExpressionUseId)>,
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
struct CreatedControlProjection<'a, 'c, H> {
    creation: &'a mut novarocks_type_contract::ValueSchemaCreation<'c, H>,
}
impl<H> ControlPartsProjection for CreatedControlProjection<'_, '_, H> {
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
    Vec<ExpressionEvaluationDomain>,
    Vec<ExpressionInvocation<ExprId>>,
    Vec<(ExpressionRootSite, ExpressionUseId)>,
    novarocks_type_contract::ValueTypeStorageLifetime,
);

pub struct CreatedExpressionControlParts {
    domains: Vec<ExpressionEvaluationDomain>,
    uses: Vec<ExpressionInvocation<ExprId>>,
    roots: Vec<(ExpressionRootSite, ExpressionUseId)>,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl CreatedExpressionControlParts {
    pub fn domains(&self) -> &[ExpressionEvaluationDomain] {
        &self.domains
    }
    pub fn uses(&self) -> &[ExpressionInvocation<ExprId>] {
        &self.uses
    }
    pub fn roots(&self) -> &[(ExpressionRootSite, ExpressionUseId)] {
        &self.roots
    }
    pub fn into_parts(self) -> ExpressionControlParts {
        (self.domains, self.uses, self.roots, self.storage)
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
    let (domains, uses, roots) =
        project_decode_core(input, &mut model, &mut projection, Some(floor), true, work)?;
    // Graph/root constructor resources are admitted by the actual downstream
    // constructor. Parts do not allocate or claim those future owners.
    model.future = ControlOwnedResourceFacts::default();
    let facts = model.check_with(ControlOwnedResourceFacts::default())?;
    Ok((
        CreatedExpressionControlParts {
            domains,
            uses,
            roots,
            storage,
        },
        facts,
    ))
}
fn control_source_floor(input: &wire::ExpressionControl) -> Result<usize, ControlCodecError> {
    let mut bytes = std::mem::size_of::<wire::ExpressionControl>();
    for next in [
        Layout::array::<wire::EvaluationDomain>(input.domains.capacity()),
        Layout::array::<wire::ExpressionUse>(input.uses.capacity()),
        Layout::array::<wire::RootBinding>(input.roots.capacity()),
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
                        input.domains.len(),
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
                m.floor(*floor)?;
            }
            work.step().map_err(ControlCodecError::from)?;
        }
        let mut domains = Vec::new();
        projection.reserve(&mut domains, input.domains.len(), observed, work)?;
        for domain in &input.domains {
            domains.push(ExpressionEvaluationDomain {
                id: EvaluationDomainId::new(domain.id),
                parent: domain.parent_domain_id.map(EvaluationDomainId::new),
                guard: domain.guard.as_ref().map(decode_guard).transpose()?,
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
                    domain: EvaluationDomainId::new(required(
                        invocation.domain_id,
                        "use domain is missing",
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
        let mut roots = Vec::new();
        projection.reserve(&mut roots, input.roots.len(), observed, work)?;
        for binding in &input.roots {
            roots.push((
                decode_site(
                    binding
                        .site
                        .as_ref()
                        .ok_or(ControlCodecError::InvalidShape("root site is missing"))?,
                )?,
                ExpressionUseId::new(required(binding.use_id, "root use is missing")?),
            ));
            work.step().map_err(ControlCodecError::from)?;
        }
        Ok((domains, uses, roots))
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
mod created_parts_tests {
    use super::*;
    use novarocks_type_contract::{
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
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn raw() -> wire::ExpressionControl {
        wire::ExpressionControl {
            domains: vec![wire::EvaluationDomain {
                id: 0,
                parent_domain_id: None,
                guard: None,
            }],
            uses: Vec::new(),
            roots: Vec::new(),
        }
    }
    #[test]
    fn created_control_parts_preserve_sparse_zero_and_finite_source_owner_without_graph_claim() {
        let source = raw();
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = Arc::new(Hold(drops.clone()));
        let mut requests = Vec::new();
        let mut exact = |layout: Layout| {
            requests.push(layout);
            Ok::<_, std::convert::Infallible>(())
        };
        let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
        let mut creation = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::held(owner.clone()),
            &mut exact,
            &mut opaque,
        );
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
        let (parts, facts) = decode_expression_control_parts_input_derived_with_creator_in(
            &source,
            control_source_floor(&source).unwrap(),
            &mut creation,
            &mut work,
        )
        .unwrap();
        assert_eq!(parts.domains()[0].id.get(), 0);
        assert!(parts.uses().is_empty());
        assert!(parts.roots().is_empty());
        assert_eq!(facts.domain_count, 1);
        drop(creation);
        drop(owner);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(requests.iter().any(|layout| layout.size() > 0));
        let (_, _, _, storage) = parts.into_parts();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(storage);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn created_control_parts_return_first_original_host_with_no_allocation_tail() {
        use crate::host_projection_v2::ProjectionFailure;
        let source = raw();
        let original = Arc::new(17u64);
        let expected = original.clone();
        let mut count = 0;
        let mut exact = |_: Layout| {
            count += 1;
            assert_eq!(count, 1, "allocation after original refusal");
            Err(original.clone())
        };
        let mut opaque =
            |_: usize| -> Result<(), Arc<u64>> { panic!("opaque tail after original refusal") };
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
        let result = decode_expression_control_parts_input_derived_with_creator_in(
            &source,
            control_source_floor(&source).unwrap(),
            &mut creation,
            &mut work,
        );
        assert!(
            matches!(result,Err(ProjectionFailure::Host(cause)) if Arc::ptr_eq(&cause,&expected))
        );
        drop(creation);
        assert_eq!(count, 1);
    }
}
