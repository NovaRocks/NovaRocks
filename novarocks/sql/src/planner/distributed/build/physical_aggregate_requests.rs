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

//! Actual aggregate update requests, separate from merge-state inputs.

use std::{alloc::Layout, sync::Arc};

use novarocks_functions::{
    AggregateKernelPhase, AggregateOrderKey, AggregatePreparationOptions, ConstantPolicy,
    FunctionArgument, FunctionBindingRequest, FunctionBindingSelection, MAX_CALL_EFFECT_ARGUMENTS,
    PureCallPreparation, ScopedRelationalEffects,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionKind,
};
use {
    novarocks_expression_contract::BoundFunction,
    novarocks_physical_plan::{
        AggregateBinding, AggregateCall, AggregatePhase, ConstantPools, ExprId, Fragment, NodeKind,
        NullOrdering, PhysicalCallSite, PhysicalNode, SortDirection, TopNReduction,
    },
};

use super::{
    lowered_draft::CanonicalAggregateOperationalRequest,
    physical_call_arguments::{PhysicalArgumentError, author_physical_argument_observed},
    physical_scalar_requests::author_scalar_result_selection_observed,
};

#[derive(Debug)]
pub(crate) enum PhysicalAggregateRequestError {
    Control(CompileControlError),
    Argument(PhysicalArgumentError),
    InvalidSource(&'static str),
    MissingArgument(ExprId),
    MissingRoot(novarocks_expression_contract::RootOrdinal),
    MissingLogicalSource(AggregatePhase),
}
impl From<CompileControlError> for PhysicalAggregateRequestError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<novarocks_type_contract::ValueTypeError> for PhysicalAggregateRequestError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        PhysicalArgumentError::from(error).into()
    }
}
impl From<PhysicalArgumentError> for PhysicalAggregateRequestError {
    fn from(error: PhysicalArgumentError) -> Self {
        match error {
            PhysicalArgumentError::Control(cause) => Self::Control(cause),
            other => Self::Argument(other),
        }
    }
}

/// Borrows the actual immutable node and call. The same selected Arc feeds
/// refinement and fresh preparation; signature facts grant no capability.
#[derive(Debug)]
pub(crate) struct AuthoredPhysicalAggregateUpdateRequest<'a> {
    source: &'a AggregateCall,
    node: &'a PhysicalNode,
    site: PhysicalCallSite,
    selected: Arc<FunctionBindingSelection>,
    arguments: AggregateUpdateArguments<'a>,
    options: AggregatePreparationOptions,
}
#[derive(Debug)]
enum AggregateUpdateArguments<'a> {
    OwnedPhysical(Vec<FunctionArgument>),
    Canonical {
        captured: &'a crate::binding::CapturedAggregateLogicalRequest,
        canonical: &'a CanonicalAggregateOperationalRequest,
    },
}
impl AuthoredPhysicalAggregateUpdateRequest<'_> {
    pub const fn source(&self) -> &AggregateCall {
        self.source
    }
    pub const fn node(&self) -> &PhysicalNode {
        self.node
    }
    pub const fn site(&self) -> PhysicalCallSite {
        self.site
    }
    pub const fn binding(&self) -> &AggregateBinding {
        &self.source.binding
    }
    pub const fn function(&self) -> &BoundFunction {
        &self.source.binding.function
    }
    pub const fn selected(&self) -> &Arc<FunctionBindingSelection> {
        &self.selected
    }
    pub fn request(&self) -> FunctionBindingRequest<'_> {
        match &self.arguments {
            AggregateUpdateArguments::OwnedPhysical(arguments) => FunctionBindingRequest {
                arguments,
                logical_argument_count: self.source.arguments.len(),
                // A Partial output carries state; the selected SQL result remains
                // the binding's final result, independently of the output layout.
                expected_result_type: Some(&self.source.binding.function.result_type),
            },
            AggregateUpdateArguments::Canonical { canonical, .. } => canonical.request(),
        }
    }
    /// None identifies the independent direct-physical component. It supplies
    /// no inferred policy for callers requiring an authenticated journal loan.
    pub fn captured_decimal_overflow_policy(
        &self,
    ) -> Option<novarocks_type_contract::DecimalOverflowPolicy> {
        match &self.arguments {
            AggregateUpdateArguments::OwnedPhysical(_) => None,
            AggregateUpdateArguments::Canonical { captured, .. } => {
                Some(captured.binding().decimal_overflow_policy())
            }
        }
    }
    pub fn captured_constant_policy(&self) -> Option<ConstantPolicy> {
        match &self.arguments {
            AggregateUpdateArguments::OwnedPhysical(_) => None,
            AggregateUpdateArguments::Canonical { captured, .. } => {
                Some(captured.constant_policy())
            }
        }
    }
    pub fn preparation(&self, arguments: ScopedRelationalEffects) -> PureCallPreparation {
        PureCallPreparation::Aggregate {
            arguments,
            options: self.options.clone(),
        }
    }
}

/// Preserve logical channels followed by function ORDER BY channels, with
/// every occurrence authored from its actual expression. Single/Partial only:
/// a merge state's signature cannot recover the original logical request.
/// Existing phase, input/domain and sequence laws remain with their mandatory
/// physical and installed owners. No legacy effects or environment are used.
///
/// The caller owns entry, ordinary/success footer, per-call policy/environment
/// and admission of source and opaque nested clones. Layout checks and fallible
/// reserves do not provide a host grant. Control/resource refusals return
/// directly on the original meter, without a later observation here.
pub(crate) fn author_physical_aggregate_update_request_observed<'a>(
    source: &'a AggregateCall,
    node: &'a PhysicalNode,
    site: PhysicalCallSite,
    fragment: &Fragment,
    pools: &ConstantPools,
    literal_policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalAggregateUpdateRequest<'a>, PhysicalAggregateRequestError> {
    work.flush()?;
    let original = fragment.nodes().get(&node.id);
    work.step()?;
    work.flush()?;
    if !original.is_some_and(|original| std::ptr::eq(original, node)) {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "aggregate request node is not the original fragment node",
        ));
    }
    let actual = match (site, &node.kind) {
        (PhysicalCallSite::Aggregate { node: id, call }, NodeKind::Aggregate { calls, .. })
            if id == node.id =>
        {
            usize::try_from(call)
                .ok()
                .and_then(|ordinal| calls.get(ordinal))
        }
        (
            PhysicalCallSite::TopNState { node: id, call },
            NodeKind::TopN {
                reduction: TopNReduction::GroupedStates { calls, .. },
                ..
            },
        ) if id == node.id => usize::try_from(call)
            .ok()
            .and_then(|ordinal| calls.get(ordinal)),
        _ => None,
    };
    let associated = actual.is_some_and(|actual| std::ptr::eq(actual, source));
    work.step()?;
    if !associated {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "aggregate request call differs from its exact node site",
        ));
    }
    let aggregate = source.binding.function.kind == FunctionKind::Aggregate;
    work.step()?;
    if !aggregate {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "aggregate request carries a different function kind",
        ));
    }
    let phase = match source.binding.phase {
        AggregatePhase::Single => Ok(AggregateKernelPhase::Single),
        AggregatePhase::Partial { .. } => Ok(AggregateKernelPhase::Partial),
        phase @ (AggregatePhase::Intermediate { .. } | AggregatePhase::Final { .. }) => {
            Err(PhysicalAggregateRequestError::MissingLogicalSource(phase))
        }
    };
    work.step()?;
    let phase = phase?;
    let count = source
        .arguments
        .len()
        .checked_add(source.order_by.len())
        .ok_or(CompileControlError::ResourceExhausted)?;
    if count > MAX_CALL_EFFECT_ARGUMENTS
        || source.binding.function.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
    {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let logical = usize::try_from(source.binding.logical_argument_count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    let exact_count =
        logical == source.arguments.len() && count == source.binding.function.argument_types.len();
    work.step()?;
    if !exact_count {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "aggregate logical and ORDER BY counts differ from its selected signature",
        ));
    }
    Layout::array::<FunctionArgument>(count)
        .and_then(|_| Layout::array::<AggregateOrderKey>(source.order_by.len()))
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    let mut arguments = Vec::new();
    arguments
        .try_reserve_exact(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    for id in source
        .arguments
        .iter()
        .copied()
        .chain(source.order_by.iter().map(|key| key.expr))
    {
        let argument = super::unit_source::UnitDefinitions::from_node(node).root_definition(id);
        work.step()?;
        let argument = argument.ok_or(PhysicalAggregateRequestError::MissingRoot(id))?;
        arguments.push(author_physical_argument_observed(
            argument,
            pools,
            literal_policy,
            CompilePhase::FunctionSpecialization,
            work,
        )?);
        work.step()?;
    }
    work.flush()?;
    let mut keys = Vec::new();
    keys.try_reserve_exact(source.order_by.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    for key in source.order_by.iter() {
        keys.push(AggregateOrderKey {
            ascending: key.direction == SortDirection::Ascending,
            nulls_first: key.null_ordering == NullOrdering::First,
        });
        work.step()?;
    }
    let selected = author_scalar_result_selection_observed(
        &source.binding.function,
        Some(&source.binding),
        work,
    )?;
    work.flush()?;
    let options = AggregatePreparationOptions {
        state_interpretation: source
            .binding
            .state_interpretation
            .as_ref()
            .map(|value| value.clone_observed(work))
            .transpose()?
            .map(Arc::new),
        phase,
        distinct: source.distinct,
        order_keys: keys.into(),
        state_input_type: None,
    };
    work.flush()?;
    Ok(AuthoredPhysicalAggregateUpdateRequest {
        source,
        node,
        site,
        selected,
        arguments: AggregateUpdateArguments::OwnedPhysical(arguments),
        options,
    })
}

/// Borrow the original admitted logical request for a producer-certified
/// Single/Partial update. Both require the same-emission operational request
/// and selected Arc, associated with the original capture revision and binding.
/// The original capture retains source and policy. Physical expression channels
/// identify runtime roots; they never recreate logical source provenance. ORDER
/// flags come from the exact actual call, and policies remain with the capture.
///
/// Caller entry, ordinary footer and opaque clone/coexistence admission remain
/// mandatory; original typed control refusals return without a footer here.
pub(crate) fn author_physical_aggregate_update_request_from_journal_observed<'source>(
    entry: &super::lowered_draft::CheckedAggregateLogicalSourceEntry<'source>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalAggregateUpdateRequest<'source>, PhysicalAggregateRequestError> {
    use super::lowered_draft::AggregateRuntimeDemand;
    let source = entry.source();
    let phase = match entry.phase() {
        AggregatePhase::Single => Ok(AggregateKernelPhase::Single),
        AggregatePhase::Partial { .. } => Ok(AggregateKernelPhase::Partial),
        phase => Err(PhysicalAggregateRequestError::MissingLogicalSource(phase)),
    };
    work.step()?;
    let phase = phase?;
    let update =
        entry.runtime() == AggregateRuntimeDemand::Update && source.binding.phase == entry.phase();
    work.step()?;
    if !update {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "journal update request differs from its original runtime demand",
        ));
    }
    let captured = entry.captured();
    let canonical = entry.canonical();
    work.step()?;
    let canonical = canonical.ok_or(PhysicalAggregateRequestError::InvalidSource(
        "aggregate update journal has no same-emission operational request",
    ))?;
    let belongs = canonical.belongs_to(captured);
    work.step()?;
    if !belongs {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "aggregate update operational request has a different capture revision or binding",
        ));
    }
    let request = canonical.request();
    let count = source
        .arguments
        .len()
        .checked_add(source.order_by.len())
        .ok_or(CompileControlError::ResourceExhausted)?;
    if count > MAX_CALL_EFFECT_ARGUMENTS
        || request.arguments.len() > MAX_CALL_EFFECT_ARGUMENTS
        || source.binding.function.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
    {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let matching = request.logical_argument_count == source.arguments.len()
        && request.arguments.len() == count
        && source.binding.function.argument_types.len() == count;
    work.step()?;
    if !matching {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "journal update logical and ORDER channels differ from the captured request",
        ));
    }
    Layout::array::<AggregateOrderKey>(source.order_by.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    selected_correspondence_observed(
        captured,
        canonical.selected(),
        request.logical_argument_count,
        source,
        work,
    )?;
    if let Some(facts) = captured.binding().aggregate_state_source() {
        let original = source.binding.state_interpretation.as_ref().ok_or(
            PhysicalAggregateRequestError::InvalidSource(
                "aggregate update has no original state interpretation",
            ),
        )?;
        if !facts.matches_observed(original, || work.step())? || facts.distinct != source.distinct {
            return Err(PhysicalAggregateRequestError::InvalidSource(
                "aggregate physical update changes its logical source interpretation",
            ));
        }
    }
    // Retain the exact same-emission Arc for refinement and preparation.
    // An Arc clone is not a second selected-signature author or host grant.
    work.flush()?;
    let selected = Arc::clone(canonical.selected());
    work.step()?;
    work.flush()?;
    work.flush()?;
    let mut keys = Vec::new();
    keys.try_reserve_exact(source.order_by.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    for key in source.order_by.iter() {
        keys.push(AggregateOrderKey {
            ascending: key.direction == SortDirection::Ascending,
            nulls_first: key.null_ordering == NullOrdering::First,
        });
        work.step()?;
    }
    work.flush()?;
    let options = AggregatePreparationOptions {
        state_interpretation: source
            .binding
            .state_interpretation
            .as_ref()
            .map(|value| value.clone_observed(work))
            .transpose()?
            .map(Arc::new),
        phase,
        distinct: source.distinct,
        order_keys: keys.into(),
        state_input_type: None,
    };
    work.flush()?;
    let arguments = AggregateUpdateArguments::Canonical {
        captured,
        canonical: canonical.as_ref(),
    };
    Ok(AuthoredPhysicalAggregateUpdateRequest {
        source,
        node: entry.node(),
        site: entry.site(),
        selected,
        arguments,
        options,
    })
}

/// A merge borrows its original producer journal and logical request. Runtime
/// state is a separate source loan; it never becomes a selected SQL argument.
/// The one authored selected Arc is shared by refinement and fresh preparation.
pub(crate) struct AuthoredPhysicalAggregateMergeRequest<'entry, 'source> {
    entry: &'entry super::lowered_draft::CheckedAggregateLogicalSourceEntry<'source>,
    state_inputs: super::lowered_draft::CheckedAggregateStateInputs<'source>,
    state_id: ExprId,
    state: &'source novarocks_expression_contract::ExprNode,
    state_interpretation: Option<Arc<novarocks_type_contract::AggregateStateInterpretation>>,
    phase: AggregateKernelPhase,
    selected: Arc<FunctionBindingSelection>,
}
impl AuthoredPhysicalAggregateMergeRequest<'_, '_> {
    pub const fn state_inputs(&self) -> &super::lowered_draft::CheckedAggregateStateInputs<'_> {
        &self.state_inputs
    }
    pub fn source(&self) -> &AggregateCall {
        self.entry.source()
    }
    pub fn node(&self) -> &PhysicalNode {
        self.entry.node()
    }
    pub fn fragment(&self) -> &Fragment {
        self.entry.fragment()
    }
    pub fn site(&self) -> PhysicalCallSite {
        self.entry.site()
    }
    pub fn function(&self) -> &BoundFunction {
        &self.source().binding.function
    }
    pub const fn selected(&self) -> &Arc<FunctionBindingSelection> {
        &self.selected
    }
    pub const fn state_id(&self) -> ExprId {
        self.state_id
    }
    pub fn state_root(&self) -> novarocks_expression_contract::RootOrdinal {
        self.source().arguments[0]
    }
    pub const fn state(&self) -> &novarocks_expression_contract::ExprNode {
        self.state
    }
    pub const fn phase(&self) -> AggregateKernelPhase {
        self.phase
    }
    pub fn request(&self) -> FunctionBindingRequest<'_> {
        match self.entry.canonical() {
            Some(canonical) => canonical.request(),
            None => self.entry.captured().request(),
        }
    }
    pub fn decimal_overflow_policy(&self) -> novarocks_type_contract::DecimalOverflowPolicy {
        self.entry.captured().binding().decimal_overflow_policy()
    }
    pub fn preparation(&self, arguments: ScopedRelationalEffects) -> PureCallPreparation {
        PureCallPreparation::Aggregate {
            arguments,
            options: AggregatePreparationOptions {
                state_interpretation: self.state_interpretation.clone(),
                phase: self.phase,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: Some(self.state.ty.clone()),
            },
        }
    }
}

/// Selected correspondence checks signature facts only. Source certification
/// comes exclusively from the sealed original producer journal. The caller
/// admits exact-type comparison scratch and opaque metadata/Arc clones, and
/// owns entry and the ordinary footer on this same meter.
pub(crate) fn author_physical_aggregate_merge_request_observed<'entry, 'source>(
    entry: &'entry super::lowered_draft::CheckedAggregateLogicalSourceEntry<'source>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalAggregateMergeRequest<'entry, 'source>, PhysicalAggregateRequestError> {
    use super::lowered_draft::AggregateRuntimeDemand;
    let source = entry.source();
    let phase = match entry.phase() {
        AggregatePhase::Intermediate { .. } => Ok(AggregateKernelPhase::Intermediate),
        AggregatePhase::Final { .. } => Ok(AggregateKernelPhase::Final),
        _ => Err(PhysicalAggregateRequestError::InvalidSource(
            "merge request has an update phase",
        )),
    };
    work.step()?;
    let phase = phase?;
    let state_id = match entry.runtime() {
        AggregateRuntimeDemand::ExpressionState(id) => Some(id),
        _ => None,
    };
    work.step()?;
    let state_id = state_id.ok_or(PhysicalAggregateRequestError::InvalidSource(
        "merge request has no original expression state",
    ))?;
    let definitions = super::unit_source::UnitDefinitions::from_node(entry.node());
    let channels = source.arguments.len() == 1
        && definitions
            .root(source.arguments[0])
            .is_some_and(|root| root.definition == state_id)
        && source.order_by.is_empty()
        && !source.distinct
        && source.binding.phase == entry.phase();
    work.step()?;
    if !channels {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "merge request differs from its original state channels",
        ));
    }
    let state = definitions.definition(state_id);
    work.step()?;
    let state = state.ok_or(PhysicalAggregateRequestError::MissingArgument(state_id))?;
    let selected = checked_merge_selected_observed(entry, work)?;
    // The original source graph lends every actual producer. This validates
    // each producer against its own request; cross-phase interpretation is
    // still the aggregate implementation/state owner's separate obligation.
    let source_error = |error| match error {
        super::lowered_draft::SqlSourceJournalError::Control(cause) => {
            PhysicalAggregateRequestError::Control(cause)
        }
        super::lowered_draft::SqlSourceJournalError::MissingLogicalSource => {
            PhysicalAggregateRequestError::MissingLogicalSource(entry.phase())
        }
        super::lowered_draft::SqlSourceJournalError::InvalidSource(detail) => {
            PhysicalAggregateRequestError::InvalidSource(detail)
        }
        _ => {
            PhysicalAggregateRequestError::InvalidSource("aggregate state journal entry is absent")
        }
    };
    let state_inputs = entry.state_inputs_observed(work).map_err(source_error)?;
    state_inputs
        .visit_observed(
            work,
            |producer, _, work| {
                checked_aggregate_state_semantics_observed(
                    entry.captured(),
                    &source.binding,
                    producer.captured(),
                    &producer.source().binding,
                    work,
                )?;
                let result = if producer.phase().consumes_logical_arguments() {
                    author_physical_aggregate_update_request_from_journal_observed(&producer, work)
                        .map(|_| ())
                } else {
                    checked_merge_selected_observed(&producer, work).map(|_| ())
                };
                result.map_err(|error| match error {
                    PhysicalAggregateRequestError::Control(cause) => {
                        super::lowered_draft::SqlSourceJournalError::Control(cause)
                    }
                    _ => super::lowered_draft::SqlSourceJournalError::InvalidSource(
                        "aggregate state producer differs from its own selected request",
                    ),
                })
            },
            |_, _, _, _| Ok(()),
        )
        .map_err(source_error)?;
    work.flush()?;
    let state_interpretation = source
        .binding
        .state_interpretation
        .as_ref()
        .map(|value| value.clone_observed(work).map(Arc::new))
        .transpose()?;
    work.flush()?;
    Ok(AuthoredPhysicalAggregateMergeRequest {
        state_interpretation,
        entry,
        state_inputs,
        state_id,
        state,
        phase,
        selected,
    })
}

/// ONE original ordinary state semantic compatibility author. Source identity
/// is certified independently for each actual contributor; no shared lineage,
/// constant equality or Writer-only policy is inferred for ordinary states.
pub(in crate::planner::distributed::build) fn checked_aggregate_state_semantics_observed(
    consumer: &crate::binding::CapturedAggregateLogicalRequest,
    consumer_binding: &AggregateBinding,
    producer: &crate::binding::CapturedAggregateLogicalRequest,
    producer_binding: &AggregateBinding,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), super::lowered_draft::SqlSourceJournalError> {
    let expected = consumer.binding().group_concat_source();
    let actual = producer.binding().group_concat_source();
    let same_semantics = match (expected, actual) {
        (None, None) => true,
        (Some(a), Some(b)) => a.legacy == b.legacy && a.max_len == b.max_len,
        _ => false,
    };
    let same_source_receipt = match (
        consumer.binding().aggregate_state_source(),
        producer.binding().aggregate_state_source(),
    ) {
        (None, None) => true,
        (Some(a), Some(b)) => a.matches_observed(b, || work.step())?,
        _ => false,
    };
    let same_state = match (
        &consumer_binding.state_interpretation,
        &producer_binding.state_interpretation,
    ) {
        (None, None) => true,
        (Some(a), Some(b)) => a.matches_observed(b, || work.step())?,
        _ => false,
    };
    if !same_semantics || !same_source_receipt || !same_state {
        return Err(super::lowered_draft::SqlSourceJournalError::InvalidSource(
            "aggregate state producer changes its original interpretation or semantic source",
        ));
    }
    work.step()?;
    Ok(())
}

fn checked_merge_selected_observed(
    entry: &super::lowered_draft::CheckedAggregateLogicalSourceEntry<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<FunctionBindingSelection>, PhysicalAggregateRequestError> {
    if let Some(canonical) = entry.canonical() {
        work.step()?;
        if !canonical.belongs_to(entry.captured()) {
            return Err(PhysicalAggregateRequestError::InvalidSource(
                "merge canonical request has a foreign original source revision",
            ));
        }
        selected_correspondence_observed(
            entry.captured(),
            canonical.selected(),
            canonical.request().logical_argument_count,
            entry.source(),
            work,
        )?;
        work.flush()?;
        let selected = Arc::clone(canonical.selected());
        work.step()?;
        work.flush()?;
        Ok(selected)
    } else {
        captured_selected_correspondence_observed(entry.captured(), entry.source(), work)
    }
}

/// The one signature comparison author serves journal updates and merges.
/// Equality verifies selected correspondence; only the original sealed journal
/// establishes logical source provenance. This helper does not inspect state
/// expression domains or infer nullable coercions.
fn captured_selected_correspondence_observed(
    captured: &crate::binding::CapturedAggregateLogicalRequest,
    source: &AggregateCall,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<FunctionBindingSelection>, PhysicalAggregateRequestError> {
    selected_correspondence_observed(
        captured,
        &captured.binding().resolved().selected,
        captured.request().logical_argument_count,
        source,
        work,
    )?;
    // Merge retains its original physical signature projection until actual
    // state-producer provenance is integrated. Updates borrow their exact Arc.
    Ok(author_scalar_result_selection_observed(
        &source.binding.function,
        Some(&source.binding),
        work,
    )?)
}

fn selected_correspondence_observed(
    captured: &crate::binding::CapturedAggregateLogicalRequest,
    selected: &FunctionBindingSelection,
    logical_argument_count: usize,
    source: &AggregateCall,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), PhysicalAggregateRequestError> {
    selected_binding_correspondence_observed(
        captured,
        selected,
        logical_argument_count,
        &source.binding,
        work,
    )
}

/// Shared exact selected-signature author. Actual source and phase loans stay
/// with their respective ordinary or Writer journal owners.
pub(crate) fn selected_binding_correspondence_observed(
    captured: &crate::binding::CapturedAggregateLogicalRequest,
    selected: &FunctionBindingSelection,
    logical_argument_count: usize,
    binding: &AggregateBinding,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), PhysicalAggregateRequestError> {
    use novarocks_functions::FunctionResultType;
    let resolved = captured.binding().resolved();
    let function = &binding.function;
    if selected.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
        || function.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
    {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let identity = resolved.function_id == function.function_id
        && resolved.kind == FunctionKind::Aggregate
        && function.kind == FunctionKind::Aggregate
        && selected.overload == function.overload
        && selected.overload == resolved.selected.overload
        && usize::try_from(binding.logical_argument_count).ok()
            == Some(resolved.logical_argument_count)
        && resolved.logical_argument_count == logical_argument_count
        && selected.argument_types.len() == function.argument_types.len();
    work.step()?;
    if !identity {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "merge binding differs from its captured logical identity or arity",
        ));
    }
    for (original, actual) in selected.argument_types.iter().zip(&function.argument_types) {
        work.flush()?;
        let matching = super::physical_call_arguments::argument_types_exact_observed::<
            PhysicalAggregateRequestError,
        >(original, actual, work)?;
        work.flush()?;
        if !matching {
            return Err(PhysicalAggregateRequestError::InvalidSource(
                "merge binding differs from its captured full argument signature",
            ));
        }
    }
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "merge source selected a relation result",
        ));
    };
    work.flush()?;
    let matching = result
        .exactly_equals_observed::<PhysicalAggregateRequestError>(&function.result_type, || {
            work.step().map_err(PhysicalAggregateRequestError::from)
        })?;
    work.flush()?;
    if !matching {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "merge binding differs from its captured final result type",
        ));
    }
    let aggregate = selected.aggregate.as_ref();
    work.step()?;
    let aggregate = aggregate.ok_or(PhysicalAggregateRequestError::InvalidSource(
        "merge logical source has no aggregate state contract",
    ))?;
    let format = aggregate.state_format == binding.state_format
        && aggregate.state_argument_contract == binding.state_argument_contract;
    work.step()?;
    if !format {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "merge binding differs from its captured state format or argument contract",
        ));
    }
    work.flush()?;
    let matching = aggregate
        .intermediate_type
        .exactly_equals_observed::<PhysicalAggregateRequestError>(
            &binding.intermediate_type,
            || work.step().map_err(PhysicalAggregateRequestError::from),
        )?;
    work.flush()?;
    if !matching {
        return Err(PhysicalAggregateRequestError::InvalidSource(
            "merge binding differs from its captured intermediate type",
        ));
    }
    // No local state-domain/nullability inference: the original Functions owner
    // validates the actual state loan and aligns its cloned options exactly.
    Ok(())
}

#[cfg(test)]
#[path = "physical_aggregate_journal_update_tests.rs"]
mod journal_update_tests;
