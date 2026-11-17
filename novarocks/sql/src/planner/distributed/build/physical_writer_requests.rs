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

//! Original Writer logical requests and all-contributor state loans.

use std::sync::Arc;

use novarocks_functions::{
    AggregateKernelPhase, AggregatePreparationOptions, ConstantPolicy, FunctionBindingError,
    FunctionBindingRequest, FunctionBindingSelection, MAX_CALL_EFFECT_ARGUMENTS,
    PureCallPreparation, ScopedRelationalEffects,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, DecimalOverflowPolicy, FunctionKind,
};
use {
    novarocks_expression_contract::BoundFunction,
    novarocks_physical_plan::{
        AggregateBinding, AggregatePhase, Fragment, PhysicalCallSite, PhysicalNode, ValueDef,
        ValueId, WriterAggregateCall,
    },
};

use super::{
    lowered_draft::{
        AggregateRuntimeDemand, CanonicalAggregateOperationalRequest,
        CheckedWriterAggregateLogicalSourceEntry, CheckedWriterAggregateStateInputs,
        SqlSourceJournalError,
    },
    physical_aggregate_requests::{
        PhysicalAggregateRequestError, selected_binding_correspondence_observed,
    },
    physical_scalar_requests::author_scalar_result_selection_observed,
};

#[derive(Debug)]
pub(crate) enum PhysicalWriterRequestError {
    Control(CompileControlError),
    Journal(SqlSourceJournalError),
    Aggregate(PhysicalAggregateRequestError),
    Binding(FunctionBindingError),
    InvalidSource(&'static str),
}
impl From<CompileControlError> for PhysicalWriterRequestError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<SqlSourceJournalError> for PhysicalWriterRequestError {
    fn from(error: SqlSourceJournalError) -> Self {
        match error {
            SqlSourceJournalError::Control(cause) => Self::Control(cause),
            other => Self::Journal(other),
        }
    }
}
impl From<PhysicalAggregateRequestError> for PhysicalWriterRequestError {
    fn from(error: PhysicalAggregateRequestError) -> Self {
        match error {
            PhysicalAggregateRequestError::Control(cause) => Self::Control(cause),
            other => Self::Aggregate(other),
        }
    }
}
impl From<FunctionBindingError> for PhysicalWriterRequestError {
    fn from(error: FunctionBindingError) -> Self {
        match error {
            FunctionBindingError::Control(cause) => Self::Control(cause),
            other => Self::Binding(other),
        }
    }
}

enum WriterRequestChannels<'source> {
    Update(&'source CanonicalAggregateOperationalRequest),
    Merge(CheckedWriterAggregateStateInputs<'source>),
}

/// A ValueId-based Writer lifecycle, never an ordinary AggregateCall or ExprId.
/// Every selected Arc belongs to this emission. Source/capture membership is
/// supplied by the sealed journal; metadata equality cannot create membership.
pub(crate) struct AuthoredPhysicalWriterRequest<'entry, 'source> {
    entry: &'entry CheckedWriterAggregateLogicalSourceEntry<'source>,
    selected: Arc<FunctionBindingSelection>,
    input: &'source ValueDef,
    phase: AggregateKernelPhase,
    channels: WriterRequestChannels<'source>,
}
impl<'entry, 'source> AuthoredPhysicalWriterRequest<'entry, 'source> {
    pub const fn entry(&self) -> &'entry CheckedWriterAggregateLogicalSourceEntry<'source> {
        self.entry
    }
    pub fn source(&self) -> &'source WriterAggregateCall {
        self.entry.source()
    }
    pub fn node(&self) -> &'source PhysicalNode {
        self.entry.node()
    }
    pub fn fragment(&self) -> &'source Fragment {
        self.entry.fragment()
    }
    pub fn site(&self) -> PhysicalCallSite {
        self.entry.site()
    }
    pub fn binding(&self) -> &'source AggregateBinding {
        &self.source().binding
    }
    pub fn function(&self) -> &'source BoundFunction {
        &self.binding().function
    }
    pub const fn selected(&self) -> &Arc<FunctionBindingSelection> {
        &self.selected
    }
    pub const fn input(&self) -> &'source ValueDef {
        self.input
    }
    pub fn input_id(&self) -> ValueId {
        self.source().input
    }
    pub const fn phase(&self) -> AggregateKernelPhase {
        self.phase
    }
    pub fn state_inputs(&self) -> Option<&CheckedWriterAggregateStateInputs<'source>> {
        match &self.channels {
            WriterRequestChannels::Merge(inputs) => Some(inputs),
            WriterRequestChannels::Update(_) => None,
        }
    }
    pub fn request(&self) -> FunctionBindingRequest<'_> {
        match &self.channels {
            WriterRequestChannels::Update(canonical) => canonical.request(),
            WriterRequestChannels::Merge(_) => match self.entry.canonical() {
                Some(canonical) => canonical.request(),
                None => self.entry.captured().request(),
            },
        }
    }
    pub fn decimal_overflow_policy(&self) -> DecimalOverflowPolicy {
        self.entry.captured().binding().decimal_overflow_policy()
    }
    pub fn constant_policy(&self) -> ConstantPolicy {
        self.entry.captured().constant_policy()
    }
    /// The caller admits the opaque full-type clone and owns its original
    /// scope footer. This options loan is not an allocation or runtime grant.
    pub fn preparation(&self, arguments: ScopedRelationalEffects) -> PureCallPreparation {
        PureCallPreparation::Aggregate {
            arguments,
            options: AggregatePreparationOptions {
                state_interpretation: None,
                phase: self.phase,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: match &self.channels {
                    WriterRequestChannels::Update(_) => None,
                    WriterRequestChannels::Merge(_) => Some(self.input.ty.clone()),
                },
            },
        }
    }
}

/// Borrow only an original checked Writer site. The caller owns entry and the
/// ordinary/success footer, whole-source admission and opaque clone resources.
/// Every producer is checked against its OWN request before pair compatibility;
/// independent sources sharing an auxiliary channel need not share lineage.
pub(crate) fn author_physical_writer_request_observed<'entry, 'source>(
    entry: &'entry CheckedWriterAggregateLogicalSourceEntry<'source>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalWriterRequest<'entry, 'source>, PhysicalWriterRequestError> {
    let source = entry.source();
    let input = entry.fragment().values().get(&source.input);
    work.step()?;
    let input = input.ok_or(PhysicalWriterRequestError::InvalidSource(
        "Writer request input value is absent",
    ))?;
    let matching = source.binding.phase == entry.phase()
        && source.binding.function.kind == FunctionKind::Aggregate;
    work.step()?;
    if !matching {
        return Err(PhysicalWriterRequestError::InvalidSource(
            "Writer request differs from its checked phase or kind",
        ));
    }
    let (selected, phase, channels) = match (entry.phase(), entry.runtime()) {
        (AggregatePhase::Partial { .. }, AggregateRuntimeDemand::Update) => {
            let canonical = checked_update_observed(entry, work)?;
            work.flush()?;
            let selected = Arc::clone(canonical.selected());
            work.step()?;
            work.flush()?;
            (
                selected,
                AggregateKernelPhase::Partial,
                WriterRequestChannels::Update(canonical),
            )
        }
        (AggregatePhase::Final { .. }, AggregateRuntimeDemand::WriterState(value))
            if value == source.input =>
        {
            let request = if let Some(canonical) = entry.canonical() {
                work.step()?;
                if !canonical.belongs_to(entry.captured()) {
                    return Err(PhysicalWriterRequestError::InvalidSource(
                        "Writer merge canonical request has a foreign original source revision",
                    ));
                }
                canonical.request()
            } else {
                entry.captured().request()
            };
            checked_request_shape_observed(source, request, work)?;
            let selected = match entry.canonical() {
                Some(canonical) => canonical.selected().as_ref(),
                None => &entry.captured().binding().resolved().selected,
            };
            selected_binding_correspondence_observed(
                entry.captured(),
                selected,
                request.logical_argument_count,
                &source.binding,
                work,
            )?;
            validate_installed_request_observed(entry, request, work)?;
            let selected = if let Some(canonical) = entry.canonical() {
                work.flush()?;
                let selected = Arc::clone(canonical.selected());
                work.step()?;
                work.flush()?;
                selected
            } else {
                author_scalar_result_selection_observed(
                    &source.binding.function,
                    Some(&source.binding),
                    work,
                )
                .map_err(PhysicalAggregateRequestError::from)?
            };
            let inputs = entry.state_inputs_observed(work)?;
            // The original visitor retains its error ABI. Store the exact
            // callback failure before its immediate sentinel return; the walk
            // exits at that callback and the adapter restores it without work.
            let mut callback_failure = None;
            let traversal = inputs.visit_observed(
                work,
                |producer, _, work| {
                    let result = (|| {
                        checked_update_observed(&producer, work)?;
                        checked_captured_requests_observed(entry, &producer, work)?;
                        let compatible = novarocks_physical_plan::aggregate_bindings_match_observed(
                            &source.binding, &producer.source().binding, work,
                        )?;
                        if !compatible {
                            return Err(PhysicalWriterRequestError::InvalidSource(
                                "Writer state contributor differs from the consumer's complete state contract",
                            ));
                        }
                        Ok(())
                    })();
                    match result {
                        Ok(()) => Ok(()),
                        Err(error) => {
                            let exit = match &error {
                                PhysicalWriterRequestError::Control(cause) => SqlSourceJournalError::Control(cause.clone()),
                                _ => SqlSourceJournalError::InvalidSource("Writer state contributor request rejected"),
                            };
                            callback_failure = Some(error);
                            Err(exit)
                        }
                    }
                },
                |_, _, _, _| Ok(()),
                |_, _| Ok(()),
            );
            if let Some(error) = callback_failure {
                return Err(error);
            }
            traversal?;
            (
                selected,
                AggregateKernelPhase::Final,
                WriterRequestChannels::Merge(inputs),
            )
        }
        _ => {
            return Err(PhysicalWriterRequestError::InvalidSource(
                "Writer request has no admitted Partial update or Final state lifecycle",
            ));
        }
    };
    Ok(AuthoredPhysicalWriterRequest {
        entry,
        selected,
        input,
        phase,
        channels,
    })
}

fn checked_request_shape_observed(
    source: &WriterAggregateCall,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), PhysicalWriterRequestError> {
    if request.arguments.len() > MAX_CALL_EFFECT_ARGUMENTS
        || source.binding.function.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
    {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let matching = request.logical_argument_count == 1
        && request.arguments.len() == 1
        && source.binding.logical_argument_count == 1
        && source.binding.function.argument_types.len() == 1
        && matches!(
            request.arguments.first(),
            Some(novarocks_functions::FunctionArgument::Value { .. })
        )
        && matches!(
            source.binding.function.argument_types.first(),
            Some(novarocks_type_contract::FunctionArgumentType::Value(_))
        );
    work.step()?;
    if !matching {
        return Err(PhysicalWriterRequestError::InvalidSource(
            "Writer request differs from its original single logical Value channel",
        ));
    }
    Ok(())
}

fn checked_update_observed<'source>(
    entry: &CheckedWriterAggregateLogicalSourceEntry<'source>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'source CanonicalAggregateOperationalRequest, PhysicalWriterRequestError> {
    let update = matches!(entry.phase(), AggregatePhase::Partial { .. })
        && entry.runtime() == AggregateRuntimeDemand::Update
        && entry.source().binding.phase == entry.phase();
    work.step()?;
    if !update {
        return Err(PhysicalWriterRequestError::InvalidSource(
            "Writer state emission is not an original Partial update",
        ));
    }
    let canonical = entry.canonical();
    work.step()?;
    let canonical = canonical.ok_or(PhysicalWriterRequestError::InvalidSource(
        "Writer update has no original operational request",
    ))?;
    let belongs = canonical.belongs_to(entry.captured());
    work.step()?;
    if !belongs {
        return Err(PhysicalWriterRequestError::InvalidSource(
            "Writer operational request has a different original capture revision or binding",
        ));
    }
    checked_request_shape_observed(entry.source(), canonical.request(), work)?;
    let input = entry.fragment().values().get(&entry.source().input);
    work.step()?;
    let input = input.ok_or(PhysicalWriterRequestError::InvalidSource(
        "Writer update input value is absent",
    ))?;
    let request = canonical.request();
    let novarocks_functions::FunctionArgument::Value { value_type, .. } = &request.arguments[0]
    else {
        return Err(PhysicalWriterRequestError::InvalidSource(
            "Writer update has no logical Value channel",
        ));
    };
    work.flush()?;
    let matching = value_type
        .exactly_equals_observed::<PhysicalAggregateRequestError>(&input.ty, || {
            work.step().map_err(PhysicalAggregateRequestError::from)
        })?;
    work.flush()?;
    if !matching {
        return Err(PhysicalWriterRequestError::InvalidSource(
            "Writer update operational type differs from its actual input value",
        ));
    }
    selected_binding_correspondence_observed(
        entry.captured(),
        canonical.selected(),
        canonical.request().logical_argument_count,
        &entry.source().binding,
        work,
    )?;
    validate_installed_request_observed(entry, canonical.request(), work)?;
    Ok(canonical.as_ref())
}

/// Metadata-only fixed selection verifies the actual owner request, not a
/// guessed pure effect or a fresh lifecycle. The retained catalogue is borrowed
/// from the original checked source; no caller-supplied replacement is accepted.
fn validate_installed_request_observed(
    entry: &CheckedWriterAggregateLogicalSourceEntry<'_>,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), PhysicalWriterRequestError> {
    let logical_count = request.logical_argument_count;
    work.flush()?;
    let selected = entry.function_catalog().select_exact_overload_observed(
        &entry.source().binding.function.function_id,
        FunctionKind::Aggregate,
        &entry.source().binding.function.overload,
        request,
        work.control(),
    );
    if matches!(selected, Err(FunctionBindingError::Control(_))) {
        return selected.map(|_| ()).map_err(Into::into);
    }
    work.step()?;
    work.flush()?;
    let selected = selected?;
    selected_binding_correspondence_observed(
        entry.captured(),
        &selected,
        logical_count,
        &entry.source().binding,
        work,
    )?;
    Ok(())
}

/// The shared-channel producer keys use exact original logical requests.
/// Check those requests independently of the operational nullable projection;
/// canonical None result constraints never replace either original capture.
/// Some constants need their original state-compatibility author and refuse
/// here until that author can supply a complete observed comparison.
fn checked_captured_requests_observed(
    consumer: &CheckedWriterAggregateLogicalSourceEntry<'_>,
    producer: &CheckedWriterAggregateLogicalSourceEntry<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), PhysicalWriterRequestError> {
    checked_captured_writer_sources_observed(consumer.captured(), producer.captured(), work)
}

/// The ONE original full captured Writer pair-compatibility author. Construction
/// and publication borrow each contributor's own source; shared lineage is not
/// part of this original Writer policy.
pub(in crate::planner::distributed::build) fn checked_captured_writer_sources_observed(
    consumer: &crate::binding::CapturedAggregateLogicalRequest,
    producer: &crate::binding::CapturedAggregateLogicalRequest,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), PhysicalWriterRequestError> {
    let left = consumer.request();
    let right = producer.request();
    let same = left.logical_argument_count == right.logical_argument_count
        && left.arguments.len() == right.arguments.len()
        && consumer.constant_policy() == producer.constant_policy()
        && consumer.binding().decimal_overflow_policy()
            == producer.binding().decimal_overflow_policy();
    work.step()?;
    if !same {
        return Err(PhysicalWriterRequestError::InvalidSource(
            "Writer contributor changes its original logical request policy or arity",
        ));
    }
    for (a, b) in left.arguments.iter().zip(right.arguments) {
        let (
            novarocks_functions::FunctionArgument::Value {
                value_type: a,
                constant: ac,
            },
            novarocks_functions::FunctionArgument::Value {
                value_type: b,
                constant: bc,
            },
        ) = (a, b)
        else {
            return Err(PhysicalWriterRequestError::InvalidSource(
                "Writer captured state requests have a non-Value channel",
            ));
        };
        let nonconstant = ac.is_none() && bc.is_none();
        work.step()?;
        if !nonconstant {
            return Err(PhysicalWriterRequestError::InvalidSource(
                "Writer constant state requests require their original compatibility author",
            ));
        }
        work.flush()?;
        let same = a.exactly_equals_observed::<PhysicalAggregateRequestError>(b, || {
            work.step().map_err(PhysicalAggregateRequestError::from)
        })?;
        work.flush()?;
        if !same {
            return Err(PhysicalWriterRequestError::InvalidSource(
                "Writer contributor changes its original full logical request channel",
            ));
        }
    }
    let same_result = match (left.expected_result_type, right.expected_result_type) {
        (Some(a), Some(b)) => {
            work.flush()?;
            let same = a.exactly_equals_observed::<PhysicalAggregateRequestError>(b, || {
                work.step().map_err(PhysicalAggregateRequestError::from)
            })?;
            work.flush()?;
            same
        }
        (None, None) => true,
        _ => false,
    };
    work.step()?;
    if !same_result {
        return Err(PhysicalWriterRequestError::InvalidSource(
            "Writer contributor changes its original captured result constraint",
        ));
    }
    Ok(())
}
