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

//! Frozen claims for real relational call occurrences. Scalar invocations and
//! their exact contexts belong exclusively to the owning node's BoundExprSet.
use crate::{
    AggregateBinding, BoundFunction, BoundTableFunction, Fragment, FragmentId, NodeId, NodeKind,
    TopNReduction,
};
use novarocks_type_contract::{
    CallEffects, CallProofScope, CompileCheckpoints, CompileControlError, CompilePhase,
    DecimalOverflowPolicy, EffectContractError, EvaluationDemand, FunctionKind,
    MAX_CONTROL_USE_REFERENCES, PureCompileControl, RelationCallId, RelationEffectContext,
    SemanticParameterRef,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PhysicalCallSite {
    Aggregate { node: NodeId, call: u32 },
    Window { node: NodeId, call: u32 },
    TopNState { node: NodeId, call: u32 },
    WriterPartial { node: NodeId, call: u32 },
    WriterFinal { node: NodeId, call: u32 },
    Table { node: NodeId },
}
impl PhysicalCallSite {
    pub const fn node(self) -> NodeId {
        match self {
            Self::Aggregate { node, .. }
            | Self::Window { node, .. }
            | Self::TopNState { node, .. }
            | Self::WriterPartial { node, .. }
            | Self::WriterFinal { node, .. }
            | Self::Table { node } => node,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenPhysicalCall {
    pub site: PhysicalCallSite,
    pub context: RelationEffectContext,
    /// This call's actual materialized writer input, in its distinct role.
    pub materialized_writer_input: Option<RelationEffectContext>,
    pub effects: CallEffects,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
}
#[derive(Clone, Copy, Debug)]
pub enum PhysicalCallBinding<'a> {
    Window {
        function: &'a BoundFunction,
        aggregate: Option<&'a AggregateBinding>,
    },
    Aggregate(&'a AggregateBinding),
    Table(&'a BoundTableFunction),
}
impl PhysicalCallBinding<'_> {
    pub const fn kind(self) -> FunctionKind {
        match self {
            Self::Window { function, .. } => function.kind,
            Self::Aggregate(binding) => binding.function.kind,
            Self::Table(_) => FunctionKind::Table,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenFragmentCalls {
    fragment: FragmentId,
    entries: Arc<BTreeMap<PhysicalCallSite, FrozenPhysicalCall>>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FrozenCallError {
    MissingLegacyMetadata(crate::MissingLegacyBindingMetadata),
    Control(CompileControlError),
    ResourceSource(&'static str),
    TooManyItems,
    WrongFragment,
    DuplicateSite,
    InvalidSite,
    MissingSite(PhysicalCallSite),
    WrongContext,
    DuplicateIdentity,
    WrongProofScope,
    WrongControl,
    InvalidEffects(EffectContractError),
    ReplicaEquivalence(PhysicalCallSite),
    ReplicaExpression {
        node: NodeId,
        invocation: novarocks_type_contract::ExpressionUseId,
    },
}
impl fmt::Display for FrozenCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid frozen relational calls: {self:?}")
    }
}
impl std::error::Error for FrozenCallError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingLegacyMetadata(source) => Some(source),
            Self::Control(source) => Some(source),
            Self::InvalidEffects(source) => Some(source),
            _ => None,
        }
    }
}
impl From<CompileControlError> for FrozenCallError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
fn finish_frozen_calls<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, FrozenCallError>,
) -> Result<T, FrozenCallError> {
    if matches!(&result, Err(FrozenCallError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
type ResourceAdmission<'a> = dyn FnMut(&novarocks_type_contract::ControlOwnedResourceFacts) -> Result<(), CompileControlError>
    + 'a;
fn resource_error(error: novarocks_type_contract::ControlResourceError) -> FrozenCallError {
    match error {
        novarocks_type_contract::ControlResourceError::Control(cause) => {
            FrozenCallError::Control(cause)
        }
        novarocks_type_contract::ControlResourceError::SourceModel(message) => {
            FrozenCallError::ResourceSource(message)
        }
    }
}
fn resource_control(error: novarocks_type_contract::ControlResourceError) -> CompileControlError {
    match error {
        novarocks_type_contract::ControlResourceError::Control(cause) => cause,
        source @ novarocks_type_contract::ControlResourceError::SourceModel(_) => {
            CompileControlError::from_domain(source)
        }
    }
}
fn admit_resources(
    counter: &novarocks_type_contract::ControlResourceCounter,
    admit: &mut Option<&mut ResourceAdmission<'_>>,
) -> Result<(), FrozenCallError> {
    if let Some(admit) = admit.as_mut() {
        admit(&counter.facts())?;
    }
    Ok(())
}
impl FrozenFragmentCalls {
    /// Explicit unpublished construction state; final publication requires coverage.
    pub(crate) fn unpublished_empty(fragment: FragmentId) -> Self {
        Self {
            fragment,
            entries: Arc::new(BTreeMap::new()),
        }
    }
    pub fn try_new(
        fragment: &Fragment,
        calls: Vec<FrozenPhysicalCall>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FrozenCallError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = Self::try_new_core(fragment, calls, None, &mut work);
        finish_frozen_calls(work, result)
    }
    pub fn construction_resources(
        count: usize,
    ) -> Result<novarocks_type_contract::ControlOwnedResourceFacts, FrozenCallError> {
        Self::construction_resources_with_writer_inputs(count, count)
    }
    pub fn construction_resources_with_writer_inputs(
        count: usize,
        writer_inputs: usize,
    ) -> Result<novarocks_type_contract::ControlOwnedResourceFacts, FrozenCallError> {
        if count > MAX_CONTROL_USE_REFERENCES || writer_inputs > count {
            return Err(FrozenCallError::TooManyItems);
        }
        let mut resources = novarocks_type_contract::ControlResourceCounter::default();
        resources
            .tree::<PhysicalCallSite, FrozenPhysicalCall>(count)
            .map_err(resource_error)?;
        resources
            .arc::<BTreeMap<PhysicalCallSite, FrozenPhysicalCall>>(1)
            .map_err(resource_error)?;
        // Relational calls use their own identity namespace. Materialized inputs
        // are exact roles of the same identity and require no auxiliary set.
        resources
            .tree::<RelationCallId, ()>(count)
            .map_err(resource_error)?;
        Ok(resources.facts())
    }
    pub fn try_new_in(
        fragment: &Fragment,
        calls: Vec<FrozenPhysicalCall>,
        admit: &mut impl FnMut(
            &novarocks_type_contract::ControlOwnedResourceFacts,
        ) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, FrozenCallError> {
        Self::try_new_core(fragment, calls, Some(admit), work)
    }
    fn try_new_core(
        fragment: &Fragment,
        calls: Vec<FrozenPhysicalCall>,
        mut admit: Option<&mut ResourceAdmission<'_>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, FrozenCallError> {
        if calls.len() > MAX_CONTROL_USE_REFERENCES {
            return Err(FrozenCallError::TooManyItems);
        }
        let mut resources = novarocks_type_contract::ControlResourceCounter::default();
        if admit.is_some() {
            resources
                .merge(Self::construction_resources(calls.len())?)
                .map_err(resource_error)?;
            admit_resources(&resources, &mut admit)?;
        }
        let mut entries = BTreeMap::new();
        for call in calls {
            if admit.is_some() {
                work.flush()?;
            }
            if entries.insert(call.site, call).is_some() {
                return Err(FrozenCallError::DuplicateSite);
            }
            work.step()?;
            if admit.is_some() {
                work.flush()?;
            }
        }
        work.flush()?;
        let value = Self {
            fragment: fragment.id(),
            entries: Arc::new(entries),
        };
        work.step()?;
        work.flush()?;
        if let Some(parent) = admit.as_mut() {
            let base = resources.facts();
            value.validate_fragment_core(
                fragment,
                Some(&mut |child| {
                    let mut combined = novarocks_type_contract::ControlResourceCounter::default();
                    combined.merge(base).map_err(resource_control)?;
                    combined.merge(*child).map_err(resource_control)?;
                    parent(&combined.facts())
                }),
                true,
                work,
            )?;
        } else {
            value.validate_fragment_core(fragment, None, false, work)?;
        }
        Ok(value)
    }
    pub fn validate_fragment(
        &self,
        fragment: &Fragment,
        control: &dyn PureCompileControl,
    ) -> Result<(), FrozenCallError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = self.validate_fragment_core(fragment, None, false, &mut work);
        finish_frozen_calls(work, result)
    }
    pub fn validate_fragment_in(
        &self,
        fragment: &Fragment,
        admit: &mut impl FnMut(
            &novarocks_type_contract::ControlOwnedResourceFacts,
        ) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FrozenCallError> {
        self.validate_fragment_core(fragment, Some(admit), false, work)
    }
    fn validate_fragment_core(
        &self,
        fragment: &Fragment,
        mut admit: Option<&mut ResourceAdmission<'_>>,
        precharged: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FrozenCallError> {
        if self.fragment != fragment.id() {
            return Err(FrozenCallError::WrongFragment);
        }
        let mut resources = novarocks_type_contract::ControlResourceCounter::default();
        if admit.is_some() {
            if !precharged {
                resources
                    .tree::<RelationCallId, ()>(self.entries.len())
                    .map_err(resource_error)?;
            }
            let lookups =
                novarocks_type_contract::ControlResourceCounter::lookup_work(self.entries.len())
                    .map_err(resource_error)?;
            resources
                .work(
                    novarocks_type_contract::control_resource_mul(
                        lookups,
                        fragment
                            .nodes()
                            .len()
                            .checked_add(self.entries.len())
                            .ok_or(FrozenCallError::TooManyItems)?,
                    )
                    .map_err(resource_error)?,
                )
                .map_err(resource_error)?;
            admit_resources(&resources, &mut admit)?;
        }
        let mut references = 0usize;
        for node in fragment.nodes().values() {
            if let Some(unit) = &node.expressions {
                references = references
                    .checked_add(unit.control_flow().use_reference_count())
                    .ok_or(FrozenCallError::TooManyItems)?;
            }
            work.step()?;
        }
        if references > MAX_CONTROL_USE_REFERENCES {
            return Err(FrozenCallError::TooManyItems);
        }
        let mut identities = BTreeSet::new();
        let mut visited = 0usize;
        visit_relational_calls_observed(fragment, work, |site, binding, work| {
            let call = self
                .entries
                .get(&site)
                .ok_or(FrozenCallError::MissingSite(site))?;
            if matches!(binding, PhysicalCallBinding::Aggregate(_))
                && binding.kind() != FunctionKind::Aggregate
            {
                return Err(FrozenCallError::InvalidEffects(
                    EffectContractError::KindMismatch,
                ));
            }
            references = references
                .checked_add(1)
                .ok_or(FrozenCallError::TooManyItems)?;
            if references > MAX_CONTROL_USE_REFERENCES {
                return Err(FrozenCallError::TooManyItems);
            }
            if call.context.demand != EvaluationDemand::Value || !call.context.is_call() {
                return Err(FrozenCallError::WrongContext);
            }
            if !identities.insert(call.context.identity.call) {
                return Err(FrozenCallError::DuplicateIdentity);
            }
            let writer = matches!(
                site,
                PhysicalCallSite::WriterPartial { .. } | PhysicalCallSite::WriterFinal { .. }
            );
            match (writer, call.materialized_writer_input) {
                (true, Some(input))
                    if input.demand == EvaluationDemand::Value
                        && input.is_materialized_input_of(call.context) => {}
                (false, None) => {}
                _ => return Err(FrozenCallError::WrongContext),
            }
            if call.effects.proof_scope != CallProofScope::Unconditional
                && call.effects.proof_scope != CallProofScope::Relation(call.context.identity)
            {
                return Err(FrozenCallError::WrongProofScope);
            }
            if admit.is_some() {
                let n = call.effects.environment.len();
                resources
                    .tree::<SemanticParameterRef, ()>(n)
                    .map_err(resource_error)?;
                resources
                    .tree::<novarocks_type_contract::SemanticParameterKey, ()>(n)
                    .map_err(resource_error)?;
                resources
                    .tree::<novarocks_type_contract::SemanticParameterKey, ()>(n)
                    .map_err(resource_error)?;
                resources
                    .buffer::<novarocks_type_contract::SemanticParameterKey>(n, 2)
                    .map_err(resource_error)?;
                admit_resources(&resources, &mut admit)?;
            }
            validate_public_effect_shape(binding.kind(), &call.effects, admit.is_some(), work)?;
            visited += 1;
            Ok(())
        })?;
        if visited != self.entries.len() {
            return Err(FrozenCallError::InvalidSite);
        }
        Ok(())
    }
    pub const fn fragment(&self) -> FragmentId {
        self.fragment
    }
    pub fn entries(&self) -> &BTreeMap<PhysicalCallSite, FrozenPhysicalCall> {
        &self.entries
    }
    pub fn parameter_references(&self) -> impl Iterator<Item = SemanticParameterRef> + '_ {
        self.entries
            .values()
            .flat_map(|call| call.effects.environment.iter().copied())
    }
    pub(crate) fn dynamic_items_observed(
        &self,
        control: &dyn PureCompileControl,
    ) -> Result<usize, FrozenCallError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = self.dynamic_items_in(&mut work);
        finish_frozen_calls(work, result)
    }
    pub(crate) fn dynamic_items_in(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, FrozenCallError> {
        let mut items = self.entries.len();
        for call in self.entries.values() {
            items = items
                .checked_add(call.effects.environment.len())
                .ok_or(FrozenCallError::TooManyItems)?;
            work.step()?;
        }
        Ok(items)
    }
    pub fn binding<'a>(
        &self,
        fragment: &'a Fragment,
        site: PhysicalCallSite,
    ) -> Option<PhysicalCallBinding<'a>> {
        if self.fragment != fragment.id() || !self.entries.contains_key(&site) {
            return None;
        }
        match (site, &fragment.nodes().get(&site.node())?.kind) {
            (PhysicalCallSite::Aggregate { call, .. }, NodeKind::Aggregate { calls, .. }) => Some(
                PhysicalCallBinding::Aggregate(&calls.get(call as usize)?.binding),
            ),
            (
                PhysicalCallSite::TopNState { call, .. },
                NodeKind::TopN {
                    reduction: TopNReduction::GroupedStates { calls, .. },
                    ..
                },
            ) => Some(PhysicalCallBinding::Aggregate(
                &calls.get(call as usize)?.binding,
            )),
            (PhysicalCallSite::WriterPartial { call, .. }, NodeKind::TableWriter { target }) => {
                Some(PhysicalCallBinding::Aggregate(
                    &target.partial_aggregates.get(call as usize)?.binding,
                ))
            }
            (PhysicalCallSite::WriterFinal { call, .. }, NodeKind::TableFinish(finish)) => {
                Some(PhysicalCallBinding::Aggregate(
                    &finish.final_aggregates.get(call as usize)?.binding,
                ))
            }
            (PhysicalCallSite::Window { call, .. }, NodeKind::Window(spec)) => {
                let item = spec.calls.get(call as usize)?;
                Some(PhysicalCallBinding::Window {
                    function: &item.binding,
                    aggregate: item.aggregate_binding.as_deref(),
                })
            }
            (PhysicalCallSite::Table { .. }, NodeKind::TableFunction { function, .. }) => {
                Some(PhysicalCallBinding::Table(function))
            }
            _ => None,
        }
    }
}

fn validate_public_effect_shape(
    kind: FunctionKind,
    effects: &CallEffects,
    observed_resources: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FrozenCallError> {
    // At most one actual reference for each closed environment key. This is
    // only shape validation, never a manufactured implementation declaration.
    let mut references = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for reference in &effects.environment {
        if observed_resources {
            work.flush()?;
        }
        if !references.insert(*reference) || !keys.insert(reference.expected_key) {
            return Err(FrozenCallError::InvalidEffects(
                EffectContractError::InvalidEnvironmentReference,
            ));
        }
        work.step()?;
        if observed_resources {
            work.flush()?;
        }
    }
    if observed_resources {
        work.flush()?;
    }
    if observed_resources {
        work.step()?;
        work.flush()?;
    }
    let result = effects
        .validate_control(kind)
        .map_err(FrozenCallError::InvalidEffects);
    result?;
    if observed_resources {
        work.step()?;
        work.flush()?;
    }
    Ok(())
}

/// Borrow every real relational binding in its original node/call order.
/// The caller owns source and callback admission, entry and completion.
pub fn visit_physical_calls_observed<'a, E: From<FrozenCallError>>(
    fragment: &'a Fragment,
    work: &mut CompileCheckpoints<'_>,
    visit: impl FnMut(
        PhysicalCallSite,
        PhysicalCallBinding<'a>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), E>,
) -> Result<(), E> {
    visit_relational_calls_observed(fragment, work, visit)
}

pub fn visit_relational_calls_observed<'a, E: From<FrozenCallError>>(
    fragment: &'a Fragment,
    work: &mut CompileCheckpoints<'_>,
    visit: impl FnMut(
        PhysicalCallSite,
        PhysicalCallBinding<'a>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), E>,
) -> Result<(), E> {
    visit_relational_node_calls_observed(fragment.nodes().values(), work, visit)
}

/// Same ONE relational call walker for the owning unpublished builders.
/// This borrows definitions only; no completion/source/kernel facts are minted.
pub(crate) fn visit_relational_node_calls_observed<'a, E: From<FrozenCallError>>(
    nodes: impl Iterator<Item = &'a crate::PhysicalNode>,
    work: &mut CompileCheckpoints<'_>,
    mut visit: impl FnMut(
        PhysicalCallSite,
        PhysicalCallBinding<'a>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), E>,
) -> Result<(), E> {
    for node in nodes {
        match &node.kind {
            NodeKind::Aggregate { calls, .. } => {
                for (call, item) in calls.iter().enumerate() {
                    visit(
                        PhysicalCallSite::Aggregate {
                            node: node.id,
                            call: ordinal(call)?,
                        },
                        PhysicalCallBinding::Aggregate(&item.binding),
                        work,
                    )?;
                    work.step().map_err(FrozenCallError::Control)?;
                }
            }
            NodeKind::TopN {
                reduction: TopNReduction::GroupedStates { calls, .. },
                ..
            } => {
                for (call, item) in calls.iter().enumerate() {
                    visit(
                        PhysicalCallSite::TopNState {
                            node: node.id,
                            call: ordinal(call)?,
                        },
                        PhysicalCallBinding::Aggregate(&item.binding),
                        work,
                    )?;
                    work.step().map_err(FrozenCallError::Control)?;
                }
            }
            NodeKind::TableWriter { target } => {
                for (call, item) in target.partial_aggregates.iter().enumerate() {
                    visit(
                        PhysicalCallSite::WriterPartial {
                            node: node.id,
                            call: ordinal(call)?,
                        },
                        PhysicalCallBinding::Aggregate(&item.binding),
                        work,
                    )?;
                    work.step().map_err(FrozenCallError::Control)?;
                }
            }
            NodeKind::TableFinish(finish) => {
                for (call, item) in finish.final_aggregates.iter().enumerate() {
                    visit(
                        PhysicalCallSite::WriterFinal {
                            node: node.id,
                            call: ordinal(call)?,
                        },
                        PhysicalCallBinding::Aggregate(&item.binding),
                        work,
                    )?;
                    work.step().map_err(FrozenCallError::Control)?;
                }
            }
            NodeKind::Window(spec) => {
                for (call, item) in spec.calls.iter().enumerate() {
                    visit(
                        PhysicalCallSite::Window {
                            node: node.id,
                            call: ordinal(call)?,
                        },
                        PhysicalCallBinding::Window {
                            function: &item.binding,
                            aggregate: item.aggregate_binding.as_deref(),
                        },
                        work,
                    )?;
                    work.step().map_err(FrozenCallError::Control)?;
                }
            }
            NodeKind::TableFunction { function, .. } => visit(
                PhysicalCallSite::Table { node: node.id },
                PhysicalCallBinding::Table(function),
                work,
            )?,
            NodeKind::Scan { .. }
            | NodeKind::Filter { .. }
            | NodeKind::Project { .. }
            | NodeKind::HashJoin { .. }
            | NodeKind::NestLoopJoin { .. }
            | NodeKind::Sort { .. }
            | NodeKind::TopN {
                reduction: TopNReduction::Rows,
                ..
            }
            | NodeKind::Limit { .. }
            | NodeKind::SetOp { .. }
            | NodeKind::Values { .. }
            | NodeKind::SystemRelation { .. }
            | NodeKind::Repeat { .. }
            | NodeKind::Unpivot { .. }
            | NodeKind::GenerateSeries { .. }
            | NodeKind::AssertOneRow(_)
            | NodeKind::ChangeEventExpand { .. }
            | NodeKind::PositionShard(_)
            | NodeKind::WriteLayoutKey { .. }
            | NodeKind::ExchangeSource { .. } => {}
        }
        work.step().map_err(FrozenCallError::Control)?;
    }
    Ok(())
}
fn ordinal(value: usize) -> Result<u32, FrozenCallError> {
    u32::try_from(value).map_err(|_| FrozenCallError::TooManyItems)
}

#[cfg(test)]
mod tests;

mod property_proof;
pub(crate) mod replica;
pub(crate) use property_proof::OccurrencePropertyProof;
pub use property_proof::{PropertyProofProjectionFacts, PropertyProofProjectionLimits};

impl Fragment {
    /// Install the original completed table without cloning its owned entries.
    /// Recheck it against this exact fragment through the same frozen author;
    /// the caller retains source/storage admission and final plan publication.
    pub fn with_frozen_calls_observed(
        mut self,
        calls: FrozenFragmentCalls,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FrozenCallError> {
        calls.validate_fragment(&self, control)?;
        self.calls = calls;
        Ok(self)
    }
    pub fn with_calls_observed(
        mut self,
        calls: Vec<FrozenPhysicalCall>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FrozenCallError> {
        self.calls = FrozenFragmentCalls::try_new(&self, calls, control)?;
        Ok(self)
    }
    pub fn with_calls_in(
        mut self,
        calls: Vec<FrozenPhysicalCall>,
        admit: &mut impl FnMut(
            &novarocks_type_contract::ControlOwnedResourceFacts,
        ) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, FrozenCallError> {
        self.calls = FrozenFragmentCalls::try_new_in(&self, calls, admit, work)?;
        Ok(self)
    }
}
