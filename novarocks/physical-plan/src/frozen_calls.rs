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

//! Complete call-site effect claims carried by one immutable fragment.
//!
//! This table validates occurrence coverage, context and public shape only.
//! It is not a proof from an installed owner: admission must recompute every
//! complete fact with the exact selected overload before preparing a kernel.

use crate::{
    AggregateBinding, BoundFunction, BoundTableFunction, ExprKind, Fragment, FragmentId, NodeId,
    NodeKind, PhysicalRootUses, RootUseBindingError, TopNReduction,
};
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompileControlError,
    CompilePhase, DecimalOverflowPolicy, EffectContractError, EvaluationDemand, EvaluationDomainId,
    ExpressionEffectContext, ExpressionUseId, FunctionEffectDeclaration, FunctionKind,
    MAX_CONTROL_USE_REFERENCES, PureCompileControl, SemanticParameterRef,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

/// A position in the actual frozen definition, not an overload name or a
/// flattened argument ordinal. Relational calls are not scalar invocations.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PhysicalCallSite {
    Expression(ExpressionUseId),
    Aggregate { node: NodeId, call: u32 },
    TopNState { node: NodeId, call: u32 },
    WriterPartial { node: NodeId, call: u32 },
    WriterFinal { node: NodeId, call: u32 },
    Table { node: NodeId },
}

/// Claims from the FE's exact owner. Expression contexts must match the
/// control graph. Relational use IDs are disjoint from expression use IDs;
/// their unguarded domain is explicitly present in the same domain table.
/// The compiler separately proves that domain's operator/lifecycle meaning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenPhysicalCall {
    pub site: PhysicalCallSite,
    pub context: ExpressionEffectContext,
    /// Original materialized Value occurrence authored by the writer journal.
    /// It is a root domain, but deliberately not an expression invocation.
    /// Required for WriterPartial/WriterFinal and absent for every other site.
    pub materialized_writer_input: Option<ExpressionEffectContext>,
    pub effects: CallEffects,
    /// Authored by this call's SQL scope, independently of effect claims and
    /// environment parameters. Compilation must not infer a package default.
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    /// Exact original emitted source roles and independent use occurrences.
    /// Required only by nominal TemporalSource controls; absence is no fact.
    pub temporal_source: Option<novarocks_type_contract::TemporalSourcePlan<crate::ExprId>>,
    /// Present only for the exact REGEXP_COUNT scalar; Dynamic is positive
    /// authored evidence, never the default for an absent source receipt.
    pub regexp_count_pattern_source: Option<novarocks_type_contract::RegexpCountPatternSource>,
    /// Exact immediate emitted-source receipt; Ordinary must be positively authored.
    pub to_base64_byte_source: Option<novarocks_type_contract::ToBase64ByteSource>,
}

/// Borrow the real binding; this table never copies a second signature DSL.
#[derive(Clone, Copy, Debug)]
pub enum PhysicalCallBinding<'a> {
    Scalar(&'a BoundFunction),
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
            Self::Scalar(function) | Self::Window { function, .. } => function.kind,
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
    Roots(RootUseBindingError),
    TooManyItems,
    WrongFragment,
    DuplicateSite,
    InvalidSite,
    MissingSite(PhysicalCallSite),
    WrongContext,
    SharedUse,
    InvalidDomain,
    WrongProofScope,
    WrongControl,
    InvalidEffects(EffectContractError),
    /// A claimed broadcast output repeats an invocation whose complete
    /// occurrence facts do not establish replica equivalence.
    ReplicaEquivalence(PhysicalCallSite),
}
impl fmt::Display for FrozenCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid frozen call table: {self:?}")
    }
}
impl std::error::Error for FrozenCallError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingLegacyMetadata(source) => Some(source),
            Self::Control(source) => Some(source),
            Self::Roots(source) => Some(source),
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

// Ordinary validation exits observe their completed tail. A refusal from this
// or an exact nested owner is already primary and never receives another call.
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
fn root_error(error: RootUseBindingError) -> FrozenCallError {
    match error {
        RootUseBindingError::Control(cause) => FrozenCallError::Control(cause),
        error => FrozenCallError::Roots(error),
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
    pub fn try_new(
        fragment: &Fragment,
        uses: &PhysicalRootUses,
        calls: Vec<FrozenPhysicalCall>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FrozenCallError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = Self::try_new_core(fragment, uses, calls, None, &mut work);
        finish_frozen_calls(work, result)
    }

    /// Numerical backing for the actual constructor containers, before any
    /// map insertion or correspondence callback. Count is not a semantic seal.
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
        resources
            .tree::<ExpressionUseId, ()>(
                count
                    .checked_add(writer_inputs)
                    .ok_or(FrozenCallError::TooManyItems)?,
            )
            .map_err(resource_error)?;
        if writer_inputs != 0 {
            resources
                .tree::<EvaluationDomainId, ()>(
                    count
                        .checked_add(writer_inputs)
                        .ok_or(FrozenCallError::TooManyItems)?,
                )
                .map_err(resource_error)?;
        }
        Ok(resources.facts())
    }

    /// Construct using the caller's scope and cumulative owned-request observer.
    /// Facts include this constructor and its exact nested root correspondence;
    /// they do not authenticate effects or provide a host allocation grant.
    pub fn try_new_in(
        fragment: &Fragment,
        uses: &PhysicalRootUses,
        calls: Vec<FrozenPhysicalCall>,
        admit: &mut impl FnMut(
            &novarocks_type_contract::ControlOwnedResourceFacts,
        ) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, FrozenCallError> {
        Self::try_new_core(fragment, uses, calls, Some(admit), work)
    }

    fn try_new_core(
        fragment: &Fragment,
        uses: &PhysicalRootUses,
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
                .merge(Self::construction_resources_with_writer_inputs(
                    calls.len(),
                    calls
                        .iter()
                        .filter(|call| {
                            matches!(
                                call.site,
                                PhysicalCallSite::WriterPartial { .. }
                                    | PhysicalCallSite::WriterFinal { .. }
                            )
                        })
                        .count(),
                )?)
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
        if admit.is_some() {
            work.flush()?;
        }
        let value = Self {
            fragment: fragment.id(),
            entries: Arc::new(entries),
        };
        if admit.is_some() {
            work.step()?;
        }
        work.flush()?;
        if let Some(parent) = admit.as_mut() {
            let base = resources.facts();
            value.validate_fragment_core(
                fragment,
                uses,
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
            value.validate_fragment(fragment, uses, work.control())?;
        }
        Ok(value)
    }

    /// Recheck claims against this package's current snapshot. Changing a
    /// selected overload with the same public shape can pass this structural
    /// check; exact owner refinement and frozen comparison remain mandatory.
    pub fn validate_fragment(
        &self,
        fragment: &Fragment,
        uses: &PhysicalRootUses,
        control: &dyn PureCompileControl,
    ) -> Result<(), FrozenCallError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = self.validate_fragment_core(fragment, uses, None, false, &mut work);
        finish_frozen_calls(work, result)
    }

    /// Validate the same immutable source without creating a nested scope.
    pub fn validate_fragment_in(
        &self,
        fragment: &Fragment,
        uses: &PhysicalRootUses,
        admit: &mut impl FnMut(
            &novarocks_type_contract::ControlOwnedResourceFacts,
        ) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FrozenCallError> {
        self.validate_fragment_core(fragment, uses, Some(admit), false, work)
    }

    fn validate_fragment_core(
        &self,
        fragment: &Fragment,
        uses: &PhysicalRootUses,
        mut admit: Option<&mut ResourceAdmission<'_>>,
        special_precharged: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FrozenCallError> {
        if uses.roots().fragment() != fragment.id() {
            return Err(FrozenCallError::Roots(RootUseBindingError::WrongFragment));
        }
        if self.fragment != fragment.id() {
            return Err(FrozenCallError::WrongFragment);
        }
        let mut writer_inputs = 0usize;
        for call in self.entries.values() {
            work.step()?;
            writer_inputs += usize::from(matches!(
                call.site,
                PhysicalCallSite::WriterPartial { .. } | PhysicalCallSite::WriterFinal { .. }
            ));
        }
        let mut resources = novarocks_type_contract::ControlResourceCounter::default();
        if let Some(parent) = admit.as_mut() {
            // This is an upper bound for the actual relational-ID set. Each
            // environment collection is charged at its captured occurrence.
            if !special_precharged {
                resources
                    .tree::<ExpressionUseId, ()>(
                        self.entries
                            .len()
                            .checked_add(writer_inputs)
                            .ok_or(FrozenCallError::TooManyItems)?,
                    )
                    .map_err(resource_error)?;
            }
            if writer_inputs != 0 && !special_precharged {
                resources
                    .tree::<EvaluationDomainId, ()>(
                        self.entries
                            .len()
                            .checked_add(writer_inputs)
                            .ok_or(FrozenCallError::TooManyItems)?,
                    )
                    .map_err(resource_error)?;
            }
            if writer_inputs != 0 {
                resources
                    .tree::<EvaluationDomainId, ()>(uses.flow().uses().len())
                    .map_err(resource_error)?;
            }
            let lookups =
                novarocks_type_contract::ControlResourceCounter::lookup_work(self.entries.len())
                    .map_err(resource_error)?;
            resources
                .work(
                    novarocks_type_contract::control_resource_mul(
                        lookups,
                        uses.flow()
                            .uses()
                            .len()
                            .checked_add(fragment.nodes().len())
                            .and_then(|n| n.checked_add(self.entries.len()))
                            .ok_or(FrozenCallError::Control(
                                CompileControlError::ResourceExhausted,
                            ))?,
                    )
                    .map_err(resource_error)?,
                )
                .map_err(resource_error)?;
            parent(&resources.facts())?;
            let base = resources.facts();
            let mut child = novarocks_type_contract::ControlOwnedResourceFacts::default();
            uses.validate_fragment_in(
                fragment,
                &mut |next| {
                    let mut combined = novarocks_type_contract::ControlResourceCounter::default();
                    combined.merge(base).map_err(resource_control)?;
                    combined.merge(*next).map_err(resource_control)?;
                    child = *next;
                    parent(&combined.facts())
                },
                work,
            )
            .map_err(root_error)?;
            resources.merge(child).map_err(resource_error)?;
        } else {
            work.flush()?;
            uses.validate_fragment(fragment, work.control())
                .map_err(root_error)?;
        }
        // The same invocation budget covers scalar graph references and real
        // relational calls. It does not multiply by an arena's maximum ID.
        let mut references = uses.flow().use_reference_count();
        let mut special_ids = BTreeSet::new();
        let mut special_domains = BTreeSet::new();
        let mut expression_domains = BTreeSet::new();
        if writer_inputs != 0 {
            for invocation in uses.flow().uses().values() {
                work.step()?;
                expression_domains.insert(invocation.context.domain);
            }
        }
        // Reserve relational domains before validating writer Values so call
        // order cannot disguise borrowing a later call's root domain.
        if writer_inputs != 0 {
            for call in self.entries.values() {
                work.step()?;
                if !matches!(call.site, PhysicalCallSite::Expression(_)) {
                    special_domains.insert(call.context.domain);
                }
            }
        }
        let mut visited = 0usize;
        visit_calls(fragment, uses, work, |site, binding, work| {
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
            match site {
                PhysicalCallSite::Expression(id) => {
                    let invocation = &uses.flow().uses()[&id];
                    if call.context != invocation.context {
                        return Err(FrozenCallError::WrongContext);
                    }
                    match binding {
                        PhysicalCallBinding::Scalar(function) => {
                            let regexp_count =
                                function.function_id.as_str() == "builtin.scalar/regexp_count/v1";
                            if regexp_count {
                                if invocation.control
                                    != novarocks_type_contract::ControlShape::Eager
                                {
                                    return Err(FrozenCallError::WrongControl);
                                }
                                let source = fragment
                                    .expressions()
                                    .get(invocation.definition)
                                    .ok_or(FrozenCallError::WrongControl)?;
                                let ExprKind::FunctionCall { args, .. } = &source.kind else {
                                    return Err(FrozenCallError::WrongControl);
                                };
                                let expected = crate::regexp_count_pattern_source_observed(
                                    fragment.expressions(),
                                    args,
                                    work,
                                )
                                .map_err(|error| match error {
                                    crate::TemporalSourceProjectionError::Control(cause) => {
                                        FrozenCallError::Control(cause)
                                    }
                                    crate::TemporalSourceProjectionError::Invalid(_) => {
                                        FrozenCallError::WrongControl
                                    }
                                })?;
                                if call.regexp_count_pattern_source != Some(expected) {
                                    return Err(FrozenCallError::WrongControl);
                                }
                            } else if call.regexp_count_pattern_source.is_some() {
                                return Err(FrozenCallError::WrongControl);
                            }
                            let to_base64_byte =
                                function.function_id.as_str() == "builtin.scalar/to_base64/v1";
                            if to_base64_byte {
                                if invocation.control
                                    != novarocks_type_contract::ControlShape::Eager
                                {
                                    return Err(FrozenCallError::WrongControl);
                                }
                                let source = fragment
                                    .expressions()
                                    .get(invocation.definition)
                                    .ok_or(FrozenCallError::WrongControl)?;
                                let ExprKind::FunctionCall { args, .. } = &source.kind else {
                                    return Err(FrozenCallError::WrongControl);
                                };
                                let expected = crate::to_base64_byte_source_observed(
                                    fragment.expressions(),
                                    args,
                                    work,
                                )
                                .map_err(|error| match error {
                                    crate::TemporalSourceProjectionError::Control(cause) => {
                                        FrozenCallError::Control(cause)
                                    }
                                    crate::TemporalSourceProjectionError::Invalid(_) => {
                                        FrozenCallError::WrongControl
                                    }
                                })?;
                                if call.to_base64_byte_source != Some(expected) {
                                    return Err(FrozenCallError::WrongControl);
                                }
                            } else if call.to_base64_byte_source.is_some() {
                                return Err(FrozenCallError::WrongControl);
                            }
                            match (invocation.control, &call.temporal_source) {
                                (
                                    novarocks_type_contract::ControlShape::TemporalSource(shape),
                                    Some(plan),
                                ) => {
                                    let source = fragment
                                        .expressions()
                                        .get(invocation.definition)
                                        .ok_or(FrozenCallError::WrongControl)?;
                                    let ExprKind::FunctionCall { args, .. } = &source.kind else {
                                        return Err(FrozenCallError::WrongControl);
                                    };
                                    if admit.is_some() {
                                        // Projector scratch is fixed stack. Its published
                                        // trace/definition buffers are bounded by the
                                        // closed grammar, never an unverified wire claim.
                                        resources
                                            .buffer::<novarocks_type_contract::TemporalCastKind>(
                                                novarocks_type_contract::MAX_CONTROL_DEPTH,
                                                1,
                                            )
                                            .map_err(resource_error)?;
                                        resources
                                            .buffer::<crate::ExprId>(3, 1)
                                            .map_err(resource_error)?;
                                        admit_resources(&resources, &mut admit)?;
                                    }
                                    let definitions = crate::temporal_source_definitions_observed(
                                        shape.kind(),
                                        fragment.expressions(),
                                        args,
                                        work,
                                    )
                                    .map_err(|error| match error {
                                        crate::TemporalSourceProjectionError::Control(cause) => {
                                            FrozenCallError::Control(cause)
                                        }
                                        crate::TemporalSourceProjectionError::Invalid(_) => {
                                            FrozenCallError::WrongControl
                                        }
                                    })?;
                                    plan.validate(&definitions)
                                        .map_err(|_| FrozenCallError::WrongControl)?;
                                    if plan.sources.len() != invocation.arguments.len() {
                                        return Err(FrozenCallError::WrongControl);
                                    }
                                    for (source, edge) in
                                        plan.sources.iter().zip(&invocation.arguments)
                                    {
                                        if source.use_id != *edge
                                            || source.definition
                                                != uses.flow().uses()[edge].definition
                                        {
                                            return Err(FrozenCallError::WrongControl);
                                        }
                                        work.step()?;
                                    }
                                }
                                (
                                    novarocks_type_contract::ControlShape::TemporalSource(_),
                                    None,
                                ) => return Err(FrozenCallError::WrongControl),
                                (_, Some(_)) => return Err(FrozenCallError::WrongControl),
                                (_, None) => {}
                            }
                            if !call
                                .effects
                                .argument_control
                                .matches_scalar_shape(invocation.control)
                            {
                                return Err(FrozenCallError::WrongControl);
                            }
                        }
                        PhysicalCallBinding::Window { .. } => {
                            if call.temporal_source.is_some()
                                || call.regexp_count_pattern_source.is_some()
                                || call.to_base64_byte_source.is_some()
                            {
                                return Err(FrozenCallError::WrongControl);
                            }
                            if !matches!(
                                call.effects.argument_control,
                                ArgumentControl::Aggregate | ArgumentControl::Window
                            ) {
                                return Err(FrozenCallError::WrongControl);
                            }
                        }
                        PhysicalCallBinding::Aggregate(_) | PhysicalCallBinding::Table(_) => {
                            unreachable!()
                        }
                    }
                }
                PhysicalCallSite::Aggregate { .. }
                | PhysicalCallSite::TopNState { .. }
                | PhysicalCallSite::WriterPartial { .. }
                | PhysicalCallSite::WriterFinal { .. }
                | PhysicalCallSite::Table { .. } => {
                    if call.temporal_source.is_some()
                        || call.regexp_count_pattern_source.is_some()
                        || call.to_base64_byte_source.is_some()
                    {
                        return Err(FrozenCallError::WrongControl);
                    }
                    references = references
                        .checked_add(1)
                        .ok_or(FrozenCallError::TooManyItems)?;
                    if references > MAX_CONTROL_USE_REFERENCES {
                        return Err(FrozenCallError::TooManyItems);
                    }
                    if call.context.demand != EvaluationDemand::Value {
                        return Err(FrozenCallError::WrongContext);
                    }
                    if uses.flow().uses().contains_key(&call.context.use_id)
                        || !special_ids.insert(call.context.use_id)
                    {
                        return Err(FrozenCallError::SharedUse);
                    }
                    let domain = uses
                        .flow()
                        .domains()
                        .get(&call.context.domain)
                        .ok_or(FrozenCallError::InvalidDomain)?;
                    if domain.parent.is_some() || domain.guard.is_some() {
                        return Err(FrozenCallError::InvalidDomain);
                    }
                }
            }
            let writer = matches!(
                call.site,
                PhysicalCallSite::WriterPartial { .. } | PhysicalCallSite::WriterFinal { .. }
            );
            match (writer, call.materialized_writer_input) {
                (true, Some(input)) => {
                    if input.demand != EvaluationDemand::Value
                        || input.domain == call.context.domain
                        || input.use_id == call.context.use_id
                    {
                        return Err(FrozenCallError::WrongContext);
                    }
                    if uses.flow().uses().contains_key(&input.use_id)
                        || !special_ids.insert(input.use_id)
                    {
                        return Err(FrozenCallError::SharedUse);
                    }
                    let domain = uses
                        .flow()
                        .domains()
                        .get(&input.domain)
                        .ok_or(FrozenCallError::InvalidDomain)?;
                    if domain.parent.is_some() || domain.guard.is_some() {
                        return Err(FrozenCallError::InvalidDomain);
                    }
                    // Actual source domains are retained; their uses are not
                    // expression invocations. One indexed pass avoids a scan
                    // of the whole graph for each writer channel.
                    if expression_domains.contains(&input.domain)
                        || !special_domains.insert(input.domain)
                    {
                        return Err(FrozenCallError::InvalidDomain);
                    }
                }
                (false, None) => {}
                _ => return Err(FrozenCallError::WrongContext),
            }
            if call.effects.proof_scope != CallProofScope::Unconditional
                && call.effects.proof_scope != CallProofScope::Domain(call.context.domain)
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
                // FunctionEffectDeclaration::validate constructs its original
                // independent duplicate-key set inside the opaque law owner.
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

    /// Count the same immutable environment references in the caller's scope.
    /// This lends observations only; source admission and the ordinary/success
    /// tail remain with the caller, without a nested entry or completion.
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

    /// Lookup the real binding for this occurrence. This is not admission; the
    /// caller still validates this complete table against the same snapshot.
    pub fn binding<'a>(
        &self,
        fragment: &'a Fragment,
        uses: &PhysicalRootUses,
        site: PhysicalCallSite,
    ) -> Option<PhysicalCallBinding<'a>> {
        if self.fragment != fragment.id() || !self.entries.contains_key(&site) {
            return None;
        }
        match site {
            PhysicalCallSite::Expression(id) => {
                let definition = fragment
                    .expressions()
                    .get(uses.flow().uses().get(&id)?.definition)?;
                match &definition.kind {
                    ExprKind::FunctionCall { function, .. } => {
                        Some(PhysicalCallBinding::Scalar(function))
                    }
                    ExprKind::WindowCall {
                        function,
                        aggregate_binding,
                        ..
                    } => Some(PhysicalCallBinding::Window {
                        function,
                        aggregate: aggregate_binding.as_deref(),
                    }),
                    _ => None,
                }
            }
            PhysicalCallSite::Aggregate { node, call } => {
                match &fragment.nodes().get(&node)?.kind {
                    NodeKind::Aggregate { calls, .. } => Some(PhysicalCallBinding::Aggregate(
                        &calls.get(call as usize)?.binding,
                    )),
                    _ => None,
                }
            }
            PhysicalCallSite::TopNState { node, call } => {
                match &fragment.nodes().get(&node)?.kind {
                    NodeKind::TopN {
                        reduction: TopNReduction::GroupedStates { calls, .. },
                        ..
                    } => Some(PhysicalCallBinding::Aggregate(
                        &calls.get(call as usize)?.binding,
                    )),
                    _ => None,
                }
            }
            PhysicalCallSite::WriterPartial { node, call } => {
                match &fragment.nodes().get(&node)?.kind {
                    NodeKind::TableWriter { target } => Some(PhysicalCallBinding::Aggregate(
                        &target.partial_aggregates.get(call as usize)?.binding,
                    )),
                    _ => None,
                }
            }
            PhysicalCallSite::WriterFinal { node, call } => {
                match &fragment.nodes().get(&node)?.kind {
                    NodeKind::TableFinish(finish) => Some(PhysicalCallBinding::Aggregate(
                        &finish.final_aggregates.get(call as usize)?.binding,
                    )),
                    _ => None,
                }
            }
            PhysicalCallSite::Table { node } => match &fragment.nodes().get(&node)?.kind {
                NodeKind::TableFunction { function, .. } => {
                    Some(PhysicalCallBinding::Table(function))
                }
                _ => None,
            },
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
    let shape = FunctionEffectDeclaration {
        value_stability: effects.value_stability,
        own_row_error: effects.own_row_error,
        failure_behavior: effects.failure_behavior,
        null_behavior: effects.null_behavior,
        argument_control: effects.argument_control,
        instance_state: effects.instance_state,
        observable_effects: effects.observable_effects,
        environment_dependencies: keys.into_iter().collect(),
    };
    if observed_resources {
        work.step()?;
        work.flush()?;
    }
    let result = shape
        .validate(kind)
        .map_err(FrozenCallError::InvalidEffects);
    result?;
    if observed_resources {
        work.step()?;
        work.flush()?;
    }
    Ok(())
}

/// Visit each actual call occurrence using the caller's original work scope.
///
/// Root-use correspondence is rechecked against this fragment before any call
/// is exposed. Bindings are borrowed from that fragment, in the same order as
/// the frozen-call validator. This port does not infer effects or create call
/// facts, and root-use correspondence is not a content-identity proof.
///
/// The caller admits source, delegated root-validation scratch and callback
/// allocations, owns entry and finish, and propagates a typed control refusal
/// immediately without a later finish.
/// Delegated root validation retains the same control; no budget is reset.
pub fn visit_physical_calls_observed<'a, E: From<FrozenCallError>>(
    fragment: &'a Fragment,
    uses: &PhysicalRootUses,
    work: &mut CompileCheckpoints<'_>,
    visit: impl FnMut(
        PhysicalCallSite,
        PhysicalCallBinding<'a>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), E>,
) -> Result<(), E> {
    if uses.roots().fragment() != fragment.id() {
        return Err(FrozenCallError::Roots(RootUseBindingError::WrongFragment).into());
    }
    work.flush().map_err(FrozenCallError::Control)?;
    let checked = uses
        .validate_fragment(fragment, work.control())
        .map_err(|error| match error {
            RootUseBindingError::Control(error) => FrozenCallError::Control(error),
            error => FrozenCallError::Roots(error),
        });
    if let Err(FrozenCallError::Control(error)) = checked {
        return Err(FrozenCallError::Control(error).into());
    }
    work.flush().map_err(FrozenCallError::Control)?;
    checked?;
    visit_calls(fragment, uses, work, visit)
}

fn visit_calls<'a, E: From<FrozenCallError>>(
    fragment: &'a Fragment,
    uses: &PhysicalRootUses,
    work: &mut CompileCheckpoints<'_>,
    mut visit: impl FnMut(
        PhysicalCallSite,
        PhysicalCallBinding<'a>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), E>,
) -> Result<(), E> {
    for (id, invocation) in uses.flow().uses() {
        let expression = fragment
            .expressions()
            .get(invocation.definition)
            .ok_or(FrozenCallError::InvalidSite)?;
        match &expression.kind {
            ExprKind::FunctionCall { function, .. } => visit(
                PhysicalCallSite::Expression(*id),
                PhysicalCallBinding::Scalar(function),
                work,
            )?,
            ExprKind::WindowCall {
                function,
                aggregate_binding,
                ..
            } => visit(
                PhysicalCallSite::Expression(*id),
                PhysicalCallBinding::Window {
                    function,
                    aggregate: aggregate_binding.as_deref(),
                },
                work,
            )?,
            ExprKind::Value(_)
            | ExprKind::LambdaParameter { .. }
            | ExprKind::Literal(_)
            | ExprKind::Constant(_)
            | ExprKind::Unary { .. }
            | ExprKind::Binary { .. }
            | ExprKind::Conjunction { .. }
            | ExprKind::Disjunction { .. }
            | ExprKind::Lambda { .. }
            | ExprKind::Cast { .. }
            | ExprKind::IsNull { .. }
            | ExprKind::InList { .. }
            | ExprKind::Between { .. }
            | ExprKind::Like { .. }
            | ExprKind::Case { .. }
            | ExprKind::IsTruthValue { .. } => {}
        }
        work.step().map_err(FrozenCallError::Control)?;
    }
    visit_relational_calls_observed(fragment, work, visit)
}

/// Visit the original non-expression call sites before root-flow construction.
/// This is the same relational half of the complete physical call visitor:
/// zero-argument calls and materialized writer channels still have occurrences.
/// It borrows actual bindings only; structure, phase, selected owner facts and
/// runtime domains remain separate obligations. The caller admits the source
/// and callback allocations, owns entry/finish and preserves the first refusal.
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
            | NodeKind::Window(_)
            | NodeKind::SetOp { .. }
            | NodeKind::Values { .. }
            | NodeKind::Repeat { .. }
            | NodeKind::Unpivot { .. }
            | NodeKind::GenerateSeries { .. }
            | NodeKind::AssertOneRow(_)
            | NodeKind::ChangeEventExpand { .. }
            | NodeKind::PositionShard(_)
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
mod replica;
pub(crate) use property_proof::OccurrencePropertyProof;
pub use property_proof::{PropertyProofProjectionFacts, PropertyProofProjectionLimits};
