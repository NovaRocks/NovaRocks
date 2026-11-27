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

//! Complete fresh call-table composition for supported physical lifecycles.

use std::collections::BTreeMap;

use novarocks_functions::ConstantValidation;
use novarocks_physical_plan::{
    AggregateCall, AggregatePhase, ConstantPools, Fragment, FrozenCallError, FrozenFragmentCalls,
    NodeKind, PhysicalCallBinding, PhysicalCallSite, PhysicalNode, WriterAggregateCall,
    visit_relational_calls_observed,
};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    MAX_CONTROL_USE_REFERENCES, PureCompileControl, SemanticParameterRef, SemanticParameters,
};

use super::{
    expression_occurrences::{AuthoredPhysicalOccurrences, ExpressionOccurrenceError},
    lowered_draft::{SqlRelationalSourceFacts, SqlSourceJournalError},
    physical_aggregate_occurrences::{
        PhysicalAggregateMergeOccurrenceInput, PhysicalAggregateOccurrenceError,
        PhysicalAggregateOccurrenceInput, prepare_physical_aggregate_merge_occurrence_observed,
        prepare_physical_aggregate_occurrence_observed,
    },
    physical_aggregate_requests::{
        PhysicalAggregateRequestError, author_physical_aggregate_merge_request_observed,
        author_physical_aggregate_update_request_from_journal_observed,
        author_physical_aggregate_update_request_observed,
    },
    physical_table_occurrences::{
        PhysicalTableOccurrenceError, PhysicalTableOccurrenceInput,
        prepare_physical_table_occurrence_observed,
    },
    physical_table_requests::{
        PhysicalTableRequestError, author_physical_table_request_from_journal_observed,
        author_physical_table_request_observed,
    },
    physical_window_occurrences::{
        PhysicalWindowOccurrenceError, PhysicalWindowOccurrenceInput,
        prepare_physical_window_occurrence_observed,
    },
    physical_window_requests::{
        PhysicalWindowRequestError, author_physical_window_request_from_journal_observed,
        author_physical_window_request_observed,
    },
    physical_writer_occurrences::{
        PhysicalWriterOccurrenceError, PhysicalWriterOccurrenceInput,
        prepare_physical_writer_occurrence_observed,
    },
    physical_writer_requests::{
        PhysicalWriterRequestError, author_physical_writer_request_observed,
    },
};
use crate::compiler::SqlFunctionCatalog;

pub(crate) struct PhysicalRelationalCallSourceScope<'a> {
    pub source: &'a PhysicalNode,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub environment: &'a [SemanticParameterRef],
    pub proof_scope: CallProofScope,
}

pub(crate) struct PhysicalFragmentEffectsInput<'a> {
    pub fragment: &'a Fragment,
    pub occurrences: &'a AuthoredPhysicalOccurrences<'a>,
    pub constants: &'a ConstantPools,
    pub parameters: &'a SemanticParameters,
    pub literal_policy: ConstantValidation,
    pub node_effects: &'a BTreeMap<
        novarocks_physical_plan::NodeId,
        super::contract_lowering::NodeExpressionEffects,
    >,
    pub relational_scopes: &'a BTreeMap<PhysicalCallSite, PhysicalRelationalCallSourceScope<'a>>,
}

/// This immutable call table covers every actual call, or composition fails.
/// It is not complete source admission or a PhysicalPlan/Package publication.
#[derive(Debug)]
pub(crate) struct AuthoredPhysicalFragmentEffects {
    pub calls: FrozenFragmentCalls,
}

#[derive(Debug)]
pub(crate) enum PhysicalFragmentEffectsError {
    Control(CompileControlError),
    Occurrence(ExpressionOccurrenceError),
    AggregateRequest(PhysicalAggregateRequestError),
    Journal(SqlSourceJournalError),
    Aggregate(PhysicalAggregateOccurrenceError),
    WindowRequest(PhysicalWindowRequestError),
    Window(PhysicalWindowOccurrenceError),
    TableRequest(PhysicalTableRequestError),
    Table(PhysicalTableOccurrenceError),
    WriterRequest(PhysicalWriterRequestError),
    Writer(PhysicalWriterOccurrenceError),
    Calls(FrozenCallError),
    MissingScope(PhysicalCallSite),
    InvalidSource(&'static str),
    UnsupportedLifecycle(PhysicalCallSite),
}
impl From<CompileControlError> for PhysicalFragmentEffectsError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
macro_rules! nested_error {
    ($error:ident, $variant:ident) => {
        impl From<$error> for PhysicalFragmentEffectsError {
            fn from(error: $error) -> Self {
                match error {
                    $error::Control(cause) => Self::Control(cause),
                    other => Self::$variant(other),
                }
            }
        }
    };
}
nested_error!(ExpressionOccurrenceError, Occurrence);
nested_error!(PhysicalAggregateRequestError, AggregateRequest);
nested_error!(SqlSourceJournalError, Journal);
nested_error!(PhysicalAggregateOccurrenceError, Aggregate);
nested_error!(PhysicalWindowRequestError, WindowRequest);
nested_error!(PhysicalWindowOccurrenceError, Window);
nested_error!(PhysicalTableRequestError, TableRequest);
nested_error!(PhysicalTableOccurrenceError, Table);
nested_error!(PhysicalWriterRequestError, WriterRequest);
nested_error!(PhysicalWriterOccurrenceError, Writer);
nested_error!(FrozenCallError, Calls);

/// Compose the original expression roots first and then each actual relational
/// call from the sole visitor. Missing, extra or foreign source scopes refuse;
/// unsupported lifecycle owners never receive a fabricated scalar/table path.
/// Only the original complete call-table author publishes exact coverage.
///
/// Source/static/result/all-definition and global plan gates remain mandatory.
/// The caller supplies the same immutable topology and admits opaque maps,
/// clones, retained facts, nested validation scratch and preparation coexistence.
/// This component grants no runtime capability, storage allowance or host grant.
/// Success and ordinary failures observe the footer; nested control is primary.
pub(crate) fn author_physical_fragment_effects_observed(
    input: PhysicalFragmentEffectsInput<'_>,
    functions: &dyn SqlFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalFragmentEffects, PhysicalFragmentEffectsError> {
    compose_fragment_effects_observed(
        input,
        PhysicalCallSources::DirectPhysical,
        functions,
        control,
    )
}

/// Require the actual SQL producer's journal for every expression, table and
/// aggregate lifecycle. Captured constants and true nonconstants are borrowed
/// from the original request, independently of physical expression folding.
/// The supplied fragment, constants and parameters must loan this exact plan.
/// Fresh preparation borrows the retained original immutable Functions Arc.
/// Per-use source scopes remain mandatory caller obligations. This composes
/// fresh calls; it does not replace full static or Package publication gates.
pub(crate) fn author_sql_fragment_effects_observed(
    owner: SqlRelationalSourceFacts<'_>,
    input: PhysicalFragmentEffectsInput<'_>,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalFragmentEffects, PhysicalFragmentEffectsError> {
    compose_fragment_effects_observed(
        input,
        PhysicalCallSources::SqlJournal(owner),
        owner.function_catalog().as_ref(),
        control,
    )
}

enum PhysicalCallSources<'a> {
    DirectPhysical,
    SqlJournal(SqlRelationalSourceFacts<'a>),
}

fn compose_fragment_effects_observed(
    input: PhysicalFragmentEffectsInput<'_>,
    sources: PhysicalCallSources<'_>,
    functions: &dyn SqlFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalFragmentEffects, PhysicalFragmentEffectsError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        if let PhysicalCallSources::SqlJournal(owner) = &sources {
            let original = owner.fragments().get(&input.fragment.id());
            work.step()?;
            let same = original.is_some_and(|value| std::ptr::eq(value, input.fragment))
                && std::ptr::eq(owner.constants(), input.constants)
                && std::ptr::eq(owner.parameters(), input.parameters);
            work.step()?;
            if !same {
                return Err(PhysicalFragmentEffectsError::InvalidSource(
                    "SQL aggregate composition loans a foreign plan, pool or parameter source",
                ));
            }
        }
        if input.relational_scopes.len() > MAX_CONTROL_USE_REFERENCES {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        // Validate exact relational scope coverage against the original call
        // grammar before preparing any expression or relational owner.
        let mut relational_count = 0usize;
        visit_relational_calls_observed(input.fragment, &mut work, |site, binding, work| {
            let node = site.node();
            let actual = input.fragment.nodes().get(&node);
            work.step()?;
            let scope = input.relational_scopes.get(&site);
            work.step()?;
            let scope = scope.ok_or(PhysicalFragmentEffectsError::MissingScope(site))?;
            let original_binding = match binding {
                PhysicalCallBinding::Table(function) => {
                    matches!(&scope.source.kind, NodeKind::TableFunction { function: original, .. } if std::ptr::eq(original, function))
                }
                PhysicalCallBinding::Window {
                    function,
                    aggregate,
                } => window_source(scope.source, site).is_ok_and(|call| {
                    std::ptr::eq(&call.binding, function)
                        && match (call.aggregate_binding.as_deref(), aggregate) {
                            (None, None) => true,
                            (Some(left), Some(right)) => std::ptr::eq(left, right),
                            _ => false,
                        }
                }),
                PhysicalCallBinding::Aggregate(binding) => {
                    if matches!(
                        site,
                        PhysicalCallSite::WriterPartial { .. }
                            | PhysicalCallSite::WriterFinal { .. }
                    ) {
                        writer_source(scope.source, site)
                            .is_ok_and(|source| std::ptr::eq(&source.binding, binding))
                    } else {
                        aggregate_source(scope.source, site)
                            .is_ok_and(|source| std::ptr::eq(&source.binding, binding))
                    }
                }
            };
            let same =
                actual.is_some_and(|actual| std::ptr::eq(actual, scope.source)) && original_binding;
            work.step()?;
            if !same {
                return Err(PhysicalFragmentEffectsError::InvalidSource(
                    "relational scope does not loan this actual source call",
                ));
            }
            relational_count = relational_count
                .checked_add(1)
                .filter(|&count| count <= MAX_CONTROL_USE_REFERENCES)
                .ok_or(CompileControlError::ResourceExhausted)?;
            work.step()?;
            Ok(())
        })?;
        let exact_scopes = relational_count == input.relational_scopes.len();
        work.step()?;
        if !exact_scopes {
            return Err(PhysicalFragmentEffectsError::InvalidSource(
                "relational source scopes contain an extra call site",
            ));
        }
        // Each temporary summary owner must loan this exact installed unit.
        let mut unit_count = 0usize;
        for node in input.fragment.nodes().values() {
            work.step()?;
            if let Some(unit) = node.expressions.as_deref() {
                let effects = input.node_effects.get(&node.id);
                work.step()?;
                if !effects.is_some_and(|effects| std::ptr::eq(unit, effects.unit())) {
                    return Err(PhysicalFragmentEffectsError::InvalidSource(
                        "relation composition has missing or foreign node-unit summaries",
                    ));
                }
                unit_count = unit_count
                    .checked_add(1)
                    .ok_or(CompileControlError::ResourceExhausted)?;
            }
        }
        if unit_count != input.node_effects.len() {
            return Err(PhysicalFragmentEffectsError::InvalidSource(
                "relation composition has extra node-unit summaries",
            ));
        }
        work.flush()?;
        let mut calls = Vec::new();
        calls
            .try_reserve_exact(relational_count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.flush()?;
        visit_relational_calls_observed(input.fragment, &mut work, |site, binding, work| {
            let scope = input.relational_scopes.get(&site);
            work.step()?;
            let scope = scope.ok_or(PhysicalFragmentEffectsError::MissingScope(site))?;
            let frozen = match binding {
                PhysicalCallBinding::Window { .. } => {
                    let source = window_source(scope.source, site)?;
                    let request = match &sources {
                        PhysicalCallSources::DirectPhysical => {
                            author_physical_window_request_observed(
                                source,
                                scope.source,
                                input.constants,
                                input.literal_policy,
                                work,
                            )?
                        }
                        PhysicalCallSources::SqlJournal(owner) => {
                            let entry = owner.checked_window_source_observed(
                                input.fragment,
                                scope.source,
                                site,
                                source,
                                work,
                            )?;
                            let request = author_physical_window_request_from_journal_observed(
                                &entry,
                                input.constants,
                                work,
                            )?;
                            let same = request.captured_decimal_overflow_policy()
                                == Some(scope.decimal_overflow_policy);
                            work.step()?;
                            if !same {
                                return Err(PhysicalFragmentEffectsError::InvalidSource(
                                    "SQL Window occurrence changes its original captured policy",
                                ));
                            }
                            request
                        }
                    };
                    prepare_physical_window_occurrence_observed(
                        PhysicalWindowOccurrenceInput {
                            node: scope.source,
                            site,
                            source,
                            request: &request,
                            occurrences: input.occurrences,
                            child_effects: input.node_effects.get(&scope.source.id),
                            parameters: input.parameters,
                            environment: scope.environment,
                            decimal_overflow_policy: scope.decimal_overflow_policy,
                            proof_scope: scope.proof_scope,
                        },
                        functions,
                        work,
                    )?
                    .frozen
                }
                PhysicalCallBinding::Table(_) => {
                    let request = match &sources {
                        PhysicalCallSources::DirectPhysical => {
                            author_physical_table_request_observed(
                                scope.source,
                                input.fragment,
                                input.constants,
                                input.literal_policy,
                                work,
                            )?
                        }
                        PhysicalCallSources::SqlJournal(owner) => {
                            let entry = owner.checked_table_source_observed(
                                input.fragment,
                                scope.source,
                                work,
                            )?;
                            let request =
                                author_physical_table_request_from_journal_observed(&entry, work)?;
                            let same = request.captured_decimal_overflow_policy()
                                == Some(scope.decimal_overflow_policy);
                            work.step()?;
                            if !same {
                                return Err(PhysicalFragmentEffectsError::InvalidSource(
                                    "SQL table occurrence changes its original captured policy",
                                ));
                            }
                            request
                        }
                    };
                    let fresh = prepare_physical_table_occurrence_observed(
                        PhysicalTableOccurrenceInput {
                            fragment: input.fragment,
                            source: scope.source,
                            request: &request,
                            occurrences: input.occurrences,
                            child_effects: input.node_effects.get(&scope.source.id),
                            parameters: input.parameters,
                            environment: scope.environment,
                            decimal_overflow_policy: scope.decimal_overflow_policy,
                            proof_scope: scope.proof_scope,
                        },
                        functions,
                        work,
                    )?;
                    fresh.frozen
                }
                PhysicalCallBinding::Aggregate(_)
                    if matches!(
                        site,
                        PhysicalCallSite::WriterPartial { .. }
                            | PhysicalCallSite::WriterFinal { .. }
                    ) =>
                {
                    let PhysicalCallSources::SqlJournal(owner) = &sources else {
                        return Err(PhysicalFragmentEffectsError::UnsupportedLifecycle(site));
                    };
                    let source = writer_source(scope.source, site)?;
                    let entry = owner.checked_writer_aggregate_source_observed(
                        input.fragment,
                        scope.source,
                        site,
                        source,
                        work,
                    )?;
                    let request = author_physical_writer_request_observed(&entry, work)?;
                    prepare_physical_writer_occurrence_observed(
                        PhysicalWriterOccurrenceInput {
                            fragment: input.fragment,
                            node: scope.source,
                            source,
                            request: &request,
                            occurrences: input.occurrences,
                            parameters: input.parameters,
                            environment: scope.environment,
                            decimal_overflow_policy: scope.decimal_overflow_policy,
                            proof_scope: scope.proof_scope,
                        },
                        functions,
                        work,
                    )?
                    .frozen
                }
                PhysicalCallBinding::Aggregate(_) => {
                    let source = aggregate_source(scope.source, site)?;
                    match &sources {
                        PhysicalCallSources::DirectPhysical => {
                            let request = author_physical_aggregate_update_request_observed(
                                source,
                                scope.source,
                                site,
                                input.fragment,
                                input.constants,
                                input.literal_policy,
                                work,
                            )?;
                            prepare_physical_aggregate_occurrence_observed(
                                PhysicalAggregateOccurrenceInput {
                                    fragment: input.fragment,
                                    node: scope.source,
                                    source,
                                    request: &request,
                                    occurrences: input.occurrences,
                                    child_effects: input.node_effects.get(&scope.source.id),
                                    parameters: input.parameters,
                                    environment: scope.environment,
                                    decimal_overflow_policy: scope.decimal_overflow_policy,
                                    proof_scope: scope.proof_scope,
                                },
                                functions,
                                work,
                            )?
                            .frozen
                        }
                        PhysicalCallSources::SqlJournal(owner) => {
                            let entry = owner.checked_aggregate_source_observed(
                                input.fragment,
                                scope.source,
                                site,
                                source,
                                work,
                            )?;
                            match entry.phase() {
                                AggregatePhase::Single | AggregatePhase::Partial { .. } => {
                                    let request = author_physical_aggregate_update_request_from_journal_observed(
                                        &entry, work,
                                    )?;
                                    prepare_physical_aggregate_occurrence_observed(
                                        PhysicalAggregateOccurrenceInput {
                                            fragment: input.fragment,
                                            node: scope.source,
                                            source,
                                            request: &request,
                                            occurrences: input.occurrences,
                                            child_effects: input.node_effects.get(&scope.source.id),
                                            parameters: input.parameters,
                                            environment: scope.environment,
                                            decimal_overflow_policy: scope.decimal_overflow_policy,
                                            proof_scope: scope.proof_scope,
                                        },
                                        functions,
                                        work,
                                    )?
                                    .frozen
                                }
                                AggregatePhase::Intermediate { .. }
                                | AggregatePhase::Final { .. } => {
                                    let request = author_physical_aggregate_merge_request_observed(
                                        &entry, work,
                                    )?;
                                    prepare_physical_aggregate_merge_occurrence_observed(
                                        PhysicalAggregateMergeOccurrenceInput {
                                            fragment: input.fragment,
                                            node: scope.source,
                                            source,
                                            request: &request,
                                            occurrences: input.occurrences,
                                            child_effects: input.node_effects.get(&scope.source.id),
                                            parameters: input.parameters,
                                            environment: scope.environment,
                                            decimal_overflow_policy: scope.decimal_overflow_policy,
                                            proof_scope: scope.proof_scope,
                                        },
                                        functions,
                                        work,
                                    )?
                                    .frozen
                                }
                            }
                        }
                    }
                }
            };
            calls.push(frozen);
            work.step()?;
            Ok(())
        })?;
        work.flush()?;
        let calls = FrozenFragmentCalls::try_new(input.fragment, calls, control)?;
        work.flush()?;
        Ok(AuthoredPhysicalFragmentEffects { calls })
    })();
    work.finish_result(result, PhysicalFragmentEffectsError::from)
}

/// Borrow the original call selected by the sole visitor's node/ordinal site.
/// The sealed request still verifies this exact call/node/source association.
pub(super) fn window_source(
    node: &PhysicalNode,
    site: PhysicalCallSite,
) -> Result<&novarocks_physical_plan::WindowCallSpec, PhysicalFragmentEffectsError> {
    match (site, &node.kind) {
        (PhysicalCallSite::Window { node: owner, call }, NodeKind::Window(spec))
            if owner == node.id =>
        {
            spec.calls
                .get(call as usize)
                .ok_or(PhysicalFragmentEffectsError::InvalidSource(
                    "Window site has no actual ordered call",
                ))
        }
        _ => Err(PhysicalFragmentEffectsError::InvalidSource(
            "Window site differs from its original node kind",
        )),
    }
}

pub(super) fn aggregate_source(
    node: &PhysicalNode,
    site: PhysicalCallSite,
) -> Result<&AggregateCall, PhysicalFragmentEffectsError> {
    let (ordinal, calls) = match (site, &node.kind) {
        (PhysicalCallSite::Aggregate { node: id, call }, NodeKind::Aggregate { calls, .. })
            if id == node.id =>
        {
            (call, calls)
        }
        (
            PhysicalCallSite::TopNState { node: id, call },
            NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::GroupedStates { calls, .. },
                ..
            },
        ) if id == node.id => (call, calls),
        _ => {
            return Err(PhysicalFragmentEffectsError::InvalidSource(
                "aggregate site differs from the original node kind",
            ));
        }
    };
    usize::try_from(ordinal)
        .ok()
        .and_then(|ordinal| calls.get(ordinal))
        .ok_or(PhysicalFragmentEffectsError::InvalidSource(
            "aggregate site has no original ordered call",
        ))
}

#[cfg(test)]
#[path = "physical_fragment_sql_aggregate_tests.rs"]
mod sql_aggregate_tests;

/// Writer calls loan actual Value channels and cannot be converted to ordinary
/// aggregate calls or expression-based argument roots.
pub(super) fn writer_source(
    node: &PhysicalNode,
    site: PhysicalCallSite,
) -> Result<&WriterAggregateCall, PhysicalFragmentEffectsError> {
    let source = match (site, &node.kind) {
        (PhysicalCallSite::WriterPartial { node: id, call }, NodeKind::TableWriter { target })
            if id == node.id =>
        {
            target.partial_aggregates.get(call as usize)
        }
        (PhysicalCallSite::WriterFinal { node: id, call }, NodeKind::TableFinish(finish))
            if id == node.id =>
        {
            finish.final_aggregates.get(call as usize)
        }
        _ => None,
    };
    source.ok_or(PhysicalFragmentEffectsError::InvalidSource(
        "Writer site has no original ordered call",
    ))
}

#[cfg(test)]
#[path = "physical_fragment_sql_writer_tests.rs"]
mod sql_writer_tests;

#[cfg(test)]
#[path = "physical_fragment_sql_call_tests.rs"]
mod sql_call_tests;
