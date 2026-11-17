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

//! Author actual expression uses from one original physical definition graph.
//! Relational call facts, selected preparation and final package publication
//! remain separate obligations; this graph alone authorizes no execution.

use novarocks_functions::FunctionSpecializationFailure;
use novarocks_physical_plan::{
    ExprId, ExprKind, ExpressionRootError, Fragment, FrozenCallError, NodeKind,
    PhysicalCallBinding, PhysicalCallSite, PhysicalExpressionRoots, PhysicalRootUses,
    RootUseBindingError, ValueDef, visit_relational_calls_observed,
};
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompileControlError, CompilePhase, ControlShape,
    EvaluationRegionId, ExpressionControlFlow, ExpressionControlFlowError, ExpressionEffectContext,
    ExpressionEvaluationRegion, ExpressionInvocation, ExpressionUseId, MAX_CONTROL_DEFINITIONS,
    MAX_CONTROL_DEPTH, MAX_CONTROL_USE_REFERENCES, PureCompileControl, RegionGuard,
    control_argument_semantics,
};

use super::lowered_draft::{SqlAuthoredPhysicalPlan, SqlSourceJournalError};
use super::physical_temporal_sources::{self, SourceError};
use crate::compiler::SqlFunctionCatalog;
use novarocks_type_contract::{
    TemporalSourceDefinitions, TemporalSourceOccurrence, TemporalSourcePlan,
};
use std::collections::BTreeMap;

#[derive(Debug)]
pub(crate) enum ExpressionOccurrenceError {
    Control(CompileControlError),
    Roots(ExpressionRootError),
    RootBinding(RootUseBindingError),
    Flow(ExpressionControlFlowError),
    Function(FunctionSpecializationFailure),
    MissingDefinition(ExprId),
    InvalidFunctionControl(ExprId),
    TooManyItems,
    Calls(FrozenCallError),
    InvalidRelationalControl(PhysicalCallSite),
    TemporalSource(&'static str),
    Journal(SqlSourceJournalError),
}
impl From<CompileControlError> for ExpressionOccurrenceError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<FrozenCallError> for ExpressionOccurrenceError {
    fn from(value: FrozenCallError) -> Self {
        match value {
            FrozenCallError::Control(error) => Self::Control(error),
            error => Self::Calls(error),
        }
    }
}

/// Expression and relational contexts authored from the same original source.
/// These identities and regions are topology, not refined call-effect facts.
#[derive(Clone, Debug)]
pub(crate) struct AuthoredPhysicalOccurrences<'a> {
    pub root_uses: PhysicalRootUses,
    pub relational_contexts: Vec<(PhysicalCallSite, ExpressionEffectContext)>,
    fragment: &'a Fragment,
    writer_values: Vec<WriterValueUse<'a>>,
    pub(crate) temporal_sources: BTreeMap<ExpressionUseId, TemporalSourcePlan<ExprId>>,
}

/// Materialized values have actual use/region facts, without expression
/// invocations or substituted expression roots. The original source is loaned.
#[derive(Clone, Copy, Debug)]
struct WriterValueUse<'a> {
    site: PhysicalCallSite,
    value: &'a ValueDef,
    context: ExpressionEffectContext,
}

impl<'a> AuthoredPhysicalOccurrences<'a> {
    pub(crate) fn fragment(&self) -> &'a Fragment {
        self.fragment
    }
    pub(crate) fn writer_value_uses(
        &self,
    ) -> impl Iterator<Item = (PhysicalCallSite, &'a ValueDef, ExpressionEffectContext)> + '_ {
        self.writer_values
            .iter()
            .map(|entry| (entry.site, entry.value, entry.context))
    }
}

impl ExpressionOccurrenceError {
    fn flow(error: ExpressionControlFlowError) -> Self {
        match error {
            ExpressionControlFlowError::Control(error) => Self::Control(error),
            error => Self::Flow(error),
        }
    }
    pub(super) fn function(error: FunctionSpecializationFailure) -> Self {
        match error {
            FunctionSpecializationFailure::Control(error) => Self::Control(error),
            FunctionSpecializationFailure::Binding(
                novarocks_functions::FunctionBindingError::Control(error),
            ) => Self::Control(error),
            FunctionSpecializationFailure::Kernel(
                novarocks_functions::KernelFailure::CompileControl(error),
            ) => Self::Control(error),
            FunctionSpecializationFailure::Kernel(
                novarocks_functions::KernelFailure::Cancelled,
            ) => Self::Control(CompileControlError::Cancelled),
            FunctionSpecializationFailure::Kernel(
                novarocks_functions::KernelFailure::DeadlineExceeded,
            ) => Self::Control(CompileControlError::DeadlineExceeded),
            FunctionSpecializationFailure::Kernel(
                novarocks_functions::KernelFailure::ResourceExhausted,
            ) => Self::Control(CompileControlError::ResourceExhausted),
            error => Self::Function(error),
        }
    }
}

/// Author every actual root and ordered child occurrence, using the original
/// root/child grammar and exact installed scalar overload declarations. Shared
/// definitions are expanded into independent uses; no maximum ExprId indexing
/// or cross-use cache is involved. Static TypeOnly arguments create no runtime
/// invocation. All caller-owned maps, vectors and owner lookups require prior
/// admission; bounded cooperative work is not a memory funding grant.
pub(crate) fn author_physical_occurrences_observed<'a>(
    fragment: &'a Fragment,
    functions: &dyn SqlFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalOccurrences<'a>, ExpressionOccurrenceError> {
    author_physical_occurrences_in(fragment, functions, None, control)
}

/// Temporal facts are authored only from a call authenticated by the original
/// same-emission journal. Raw physical test helpers grant no default facts.
pub(crate) fn author_physical_occurrences_from_journal_observed<'a>(
    owner: &SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalOccurrences<'a>, ExpressionOccurrenceError> {
    author_physical_occurrences_in(
        fragment,
        owner.function_catalog().as_ref(),
        Some(owner),
        control,
    )
}
fn author_physical_occurrences_in<'a>(
    fragment: &'a Fragment,
    functions: &dyn SqlFunctionCatalog,
    source_owner: Option<&SqlAuthoredPhysicalPlan>,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalOccurrences<'a>, ExpressionOccurrenceError> {
    let mut author = Author {
        fragment,
        functions,
        source_owner,
        temporal_sources: BTreeMap::new(),
        work: CompileCheckpoints::try_new(control, CompilePhase::Validate)?,
        regions: Vec::new(),
        uses: Vec::new(),
        references: 0,
    };
    let result = (|| {
        let definitions = fragment.expressions().len();
        author.work.step()?;
        if definitions > MAX_CONTROL_DEFINITIONS {
            return Err(ExpressionOccurrenceError::TooManyItems);
        }
        author.work.flush()?;
        let roots =
            PhysicalExpressionRoots::try_new(fragment, control).map_err(|error| match error {
                ExpressionRootError::Control(error) => ExpressionOccurrenceError::Control(error),
                error => ExpressionOccurrenceError::Roots(error),
            })?;
        let mut bindings = Vec::new();
        bindings
            .try_reserve_exact(roots.sites().len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for (site, root) in roots.sites() {
            let region = author.region(None, None)?;
            let id = author.invocation(root.expr, region, root.demand, 1)?;
            bindings.push((*site, id));
            author.work.step()?;
        }
        let mut relational_contexts = Vec::new();
        let mut next_use = author.uses.len();
        let mut writer_values = Vec::new();
        let regions = &mut author.regions;
        visit_relational_calls_observed(fragment, &mut author.work, |site, binding, work| {
            if relational_contexts.len() == MAX_CONTROL_USE_REFERENCES
                || regions.len() == MAX_CONTROL_DEFINITIONS
            {
                return Err(ExpressionOccurrenceError::TooManyItems);
            }
            let (function_id, kind, overload, expected_control) = match binding {
                PhysicalCallBinding::Aggregate(binding) => (
                    &binding.function.function_id,
                    binding.function.kind,
                    &binding.function.overload,
                    ArgumentControl::Aggregate,
                ),
                PhysicalCallBinding::Table(function) => (
                    &function.function_id,
                    novarocks_type_contract::FunctionKind::Table,
                    &function.overload,
                    ArgumentControl::Table,
                ),
                _ => return Err(ExpressionOccurrenceError::InvalidRelationalControl(site)),
            };
            work.flush()?;
            let declaration = functions
                .pure_overload_declaration_observed(function_id, kind, overload, control)
                .map_err(ExpressionOccurrenceError::function)?;
            let matches_control = declaration.effects().argument_control == expected_control;
            work.step()?;
            if !matches_control {
                return Err(ExpressionOccurrenceError::InvalidRelationalControl(site));
            }
            append_relational_context_observed(
                fragment,
                site,
                regions,
                &mut relational_contexts,
                &mut writer_values,
                &mut next_use,
                work,
            )?;
            Ok(())
        })?;
        author.work.flush()?;
        let flow = ExpressionControlFlow::try_new(
            std::mem::take(&mut author.regions),
            std::mem::take(&mut author.uses),
            fragment.expressions(),
            CompilePhase::Validate,
            control,
        )
        .map_err(ExpressionOccurrenceError::flow)?;
        author.work.flush()?;
        let root_uses =
            PhysicalRootUses::try_new(fragment, flow, bindings, control).map_err(|error| {
                match error {
                    RootUseBindingError::Control(error) => {
                        ExpressionOccurrenceError::Control(error)
                    }
                    error => ExpressionOccurrenceError::RootBinding(error),
                }
            })?;
        Ok(AuthoredPhysicalOccurrences {
            root_uses,
            relational_contexts,
            fragment,
            writer_values,
            temporal_sources: std::mem::take(&mut author.temporal_sources),
        })
    })();
    if matches!(&result, Err(ExpressionOccurrenceError::Control(_))) {
        return result;
    }
    author.work.finish()?;
    result
}

fn append_relational_context_observed<'a>(
    fragment: &'a Fragment,
    site: PhysicalCallSite,
    regions: &mut Vec<ExpressionEvaluationRegion>,
    relational_contexts: &mut Vec<(PhysicalCallSite, ExpressionEffectContext)>,
    writer_values: &mut Vec<WriterValueUse<'a>>,
    next_use: &mut usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ExpressionOccurrenceError> {
    if relational_contexts.len() == MAX_CONTROL_USE_REFERENCES
        || regions.len() == MAX_CONTROL_DEFINITIONS
    {
        return Err(ExpressionOccurrenceError::TooManyItems);
    }
    let region = EvaluationRegionId::new(regions.len() as u32);
    // Expression, relational and materialized channel counts are
    // bounded independently. Allocate every use in one checked namespace.
    let id = ExpressionUseId::new(
        u32::try_from(*next_use).map_err(|_| CompileControlError::ResourceExhausted)?,
    );
    *next_use = (*next_use)
        .checked_add(1)
        .ok_or(CompileControlError::ResourceExhausted)?;
    regions
        .try_reserve(1)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    relational_contexts
        .try_reserve(1)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    regions.push(ExpressionEvaluationRegion {
        id: region,
        parent: None,
        guard: None,
    });
    relational_contexts.push((
        site,
        ExpressionEffectContext {
            use_id: id,
            region,
            demand: novarocks_type_contract::EvaluationDemand::Value,
        },
    ));
    work.step()?;
    let channel = match site {
        PhysicalCallSite::WriterPartial { node, call } => {
            match fragment.nodes().get(&node).map(|node| &node.kind) {
                Some(NodeKind::TableWriter { target }) => {
                    target.partial_aggregates.get(call as usize)
                }
                _ => None,
            }
        }
        PhysicalCallSite::WriterFinal { node, call } => {
            match fragment.nodes().get(&node).map(|node| &node.kind) {
                Some(NodeKind::TableFinish(finish)) => finish.final_aggregates.get(call as usize),
                _ => None,
            }
        }
        _ => None,
    };
    work.step()?;
    if let Some(channel) = channel {
        let value = fragment.values().get(&channel.input);
        work.step()?;
        let value = value.ok_or(ExpressionOccurrenceError::InvalidRelationalControl(site))?;
        if writer_values.len() == MAX_CONTROL_USE_REFERENCES
            || regions.len() == MAX_CONTROL_DEFINITIONS
        {
            return Err(ExpressionOccurrenceError::TooManyItems);
        }
        let region = EvaluationRegionId::new(
            u32::try_from(regions.len()).map_err(|_| CompileControlError::ResourceExhausted)?,
        );
        let use_id = ExpressionUseId::new(
            u32::try_from(*next_use).map_err(|_| CompileControlError::ResourceExhausted)?,
        );
        *next_use = (*next_use)
            .checked_add(1)
            .ok_or(CompileControlError::ResourceExhausted)?;
        work.flush()?;
        regions
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        writer_values
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.flush()?;
        regions.push(ExpressionEvaluationRegion {
            id: region,
            parent: None,
            guard: None,
        });
        writer_values.push(WriterValueUse {
            site,
            value,
            context: ExpressionEffectContext {
                use_id,
                region,
                demand: novarocks_type_contract::EvaluationDemand::Value,
            },
        });
        work.step()?;
    } else if matches!(
        site,
        PhysicalCallSite::WriterPartial { .. } | PhysicalCallSite::WriterFinal { .. }
    ) {
        return Err(ExpressionOccurrenceError::InvalidRelationalControl(site));
    }
    Ok(())
}

/// A borrowed role-specific context view, not an owning or synthetic graph.
pub(crate) struct WriterOccurrenceContexts<'a, 'source> {
    fragment: &'source Fragment,
    root_fragment: novarocks_physical_plan::FragmentId,
    pub relational_contexts: &'a [(PhysicalCallSite, ExpressionEffectContext)],
    writer_values: &'a [WriterValueUse<'source>],
    flow: &'a ExpressionControlFlow<ExprId>,
}

impl<'a> AuthoredPhysicalOccurrences<'a> {
    pub(crate) fn writer_contexts(&self) -> WriterOccurrenceContexts<'_, 'a> {
        WriterOccurrenceContexts {
            fragment: self.fragment,
            root_fragment: self.root_uses.roots().fragment(),
            relational_contexts: &self.relational_contexts,
            writer_values: &self.writer_values,
            flow: self.root_uses.flow(),
        }
    }
}
impl WriterOccurrenceContexts<'_, '_> {
    pub(crate) fn fragment(&self) -> &Fragment {
        self.fragment
    }
    pub(crate) fn root_fragment(&self) -> novarocks_physical_plan::FragmentId {
        self.root_fragment
    }
    pub(crate) fn writer_value_uses(
        &self,
    ) -> impl Iterator<Item = (PhysicalCallSite, &ValueDef, ExpressionEffectContext)> {
        self.writer_values
            .iter()
            .map(|entry| (entry.site, entry.value, entry.context))
    }
    pub(crate) fn region(&self, id: EvaluationRegionId) -> Option<&ExpressionEvaluationRegion> {
        self.flow.regions().get(&id)
    }
    pub(crate) fn contains_expression_use(&self, id: ExpressionUseId) -> bool {
        self.flow.uses().contains_key(&id)
    }
    pub(crate) fn expression_invocations(
        &self,
    ) -> impl Iterator<Item = &ExpressionInvocation<ExprId>> {
        self.flow.uses().values()
    }
}

#[cfg(test)]
fn author_expression_occurrences_observed(
    fragment: &Fragment,
    functions: &dyn SqlFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<PhysicalRootUses, ExpressionOccurrenceError> {
    author_physical_occurrences_observed(fragment, functions, control).map(|value| value.root_uses)
}

struct Author<'a> {
    fragment: &'a Fragment,
    functions: &'a dyn SqlFunctionCatalog,
    source_owner: Option<&'a SqlAuthoredPhysicalPlan>,
    temporal_sources: BTreeMap<ExpressionUseId, TemporalSourcePlan<ExprId>>,
    work: CompileCheckpoints<'a>,
    regions: Vec<ExpressionEvaluationRegion>,
    uses: Vec<ExpressionInvocation<ExprId>>,
    references: usize,
}
impl Author<'_> {
    fn reference(&mut self) -> Result<(), ExpressionOccurrenceError> {
        if self.references == MAX_CONTROL_USE_REFERENCES {
            return Err(ExpressionOccurrenceError::TooManyItems);
        }
        self.references += 1;
        self.work.step()?;
        Ok(())
    }
    fn region(
        &mut self,
        parent: Option<EvaluationRegionId>,
        guard: Option<RegionGuard>,
    ) -> Result<EvaluationRegionId, ExpressionOccurrenceError> {
        if self.regions.len() == MAX_CONTROL_DEFINITIONS {
            return Err(ExpressionOccurrenceError::TooManyItems);
        }
        let id = EvaluationRegionId::new(self.regions.len() as u32);
        self.regions
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.regions
            .push(ExpressionEvaluationRegion { id, parent, guard });
        self.work.step()?;
        Ok(id)
    }
    fn invocation(
        &mut self,
        definition: ExprId,
        region: EvaluationRegionId,
        demand: novarocks_type_contract::EvaluationDemand,
        depth: usize,
    ) -> Result<ExpressionUseId, ExpressionOccurrenceError> {
        if depth > MAX_CONTROL_DEPTH {
            return Err(ExpressionOccurrenceError::Flow(
                ExpressionControlFlowError::TooDeep,
            ));
        }
        self.reference()?;
        let node = self.fragment.expressions().get(definition);
        self.work.step()?;
        let node = node.ok_or(ExpressionOccurrenceError::MissingDefinition(definition))?;
        let mut source_definitions: Option<TemporalSourceDefinitions<ExprId>> = None;
        let shape = if let Some(shape) = node
            .kind
            .intrinsic_control_shape()
            .map_err(ExpressionOccurrenceError::flow)?
        {
            shape
        } else {
            let ExprKind::FunctionCall { function, args } = &node.kind else {
                return Err(ExpressionOccurrenceError::InvalidFunctionControl(
                    definition,
                ));
            };
            self.work.flush()?;
            let declaration = self
                .functions
                .pure_overload_declaration_observed(
                    &function.function_id,
                    function.kind,
                    &function.overload,
                    self.work.control(),
                )
                .map_err(ExpressionOccurrenceError::function)?;
            if function.function_id.as_str() == "builtin.scalar/regexp_count/v1" {
                let owner = self
                    .source_owner
                    .ok_or(ExpressionOccurrenceError::TemporalSource(
                        "regexp_count source has no original SQL emission journal",
                    ))?;
                self.work.flush()?;
                owner
                    .checked_expression_call_source_observed(self.fragment, node, &mut self.work)
                    .map_err(|error| match error {
                        SqlSourceJournalError::Control(cause) => {
                            ExpressionOccurrenceError::Control(cause)
                        }
                        error => ExpressionOccurrenceError::Journal(error),
                    })?;
                // SAME emitted-definition source author as codec/validator/BE.
                self.work.flush()?;
                novarocks_physical_plan::regexp_count_pattern_source_observed(
                    self.fragment.expressions(),
                    args,
                    &mut self.work,
                )
                .map_err(|error| match error {
                    SourceError::Control(cause) => ExpressionOccurrenceError::Control(cause),
                    SourceError::Invalid(message) => {
                        ExpressionOccurrenceError::TemporalSource(message)
                    }
                })?;
            }
            let selected_control = if declaration.implementation().abi
                == novarocks_functions::PureKernelAbi::ScalarInvocationV1
            {
                let owner = self
                    .source_owner
                    .ok_or(ExpressionOccurrenceError::TemporalSource(
                        "selected invocation demand has no original SQL emission owner",
                    ))?;
                self.work.flush()?;
                let original = owner
                    .checked_expression_call_source_observed(self.fragment, node, &mut self.work)
                    .map_err(|error| match error {
                        SqlSourceJournalError::Control(cause) => {
                            ExpressionOccurrenceError::Control(cause)
                        }
                        error => ExpressionOccurrenceError::Journal(error),
                    })?;
                let canonical = original.canonical_operational().ok_or(
                    ExpressionOccurrenceError::TemporalSource(
                        "selected invocation demand has no same-emission canonical request",
                    ),
                )?;
                declaration
                    .selected_argument_control_observed(
                        &function.function_id,
                        canonical.selected().as_ref(),
                        canonical.request().logical_argument_count,
                        self.work.control(),
                    )
                    .map_err(|error| ExpressionOccurrenceError::function(error.into()))?
            } else {
                declaration.effects().argument_control
            };
            let shape = if let ArgumentControl::TemporalSource(kind) = selected_control {
                let owner = self
                    .source_owner
                    .ok_or(ExpressionOccurrenceError::TemporalSource(
                        "temporal source control has no original SQL emission owner",
                    ))?;
                self.work.flush()?;
                owner
                    .checked_expression_call_source_observed(self.fragment, node, &mut self.work)
                    .map_err(|error| match error {
                        SqlSourceJournalError::Control(cause) => {
                            ExpressionOccurrenceError::Control(cause)
                        }
                        error => ExpressionOccurrenceError::Journal(error),
                    })?;
                let definitions = physical_temporal_sources::author(
                    kind,
                    self.fragment.expressions(),
                    args,
                    &mut self.work,
                )
                .map_err(|error| match error {
                    SourceError::Control(cause) => ExpressionOccurrenceError::Control(cause),
                    SourceError::Invalid(message) => {
                        ExpressionOccurrenceError::TemporalSource(message)
                    }
                })?;
                let shape = ControlShape::TemporalSource(definitions.facts.shape());
                source_definitions = Some(definitions);
                Some(shape)
            } else {
                scalar_shape(selected_control, args.len())
            };
            self.work.step()?;
            shape.ok_or(ExpressionOccurrenceError::InvalidFunctionControl(
                definition,
            ))?
        };
        let id = ExpressionUseId::new(self.uses.len() as u32);
        self.uses
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                region,
                demand,
            },
            definition,
            control: shape,
            arguments: Box::default(),
        });
        self.work.step()?;
        let mut children = Vec::new();
        if let Some(definitions) = &source_definitions {
            self.work.flush()?;
            children
                .try_reserve_exact(definitions.definitions.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            self.work.flush()?;
            for definition in &definitions.definitions {
                self.reference()?;
                children.push(*definition);
                self.work.step()?;
            }
        } else if !matches!(shape, ControlShape::TypeOnly | ControlShape::NoArguments) {
            node.kind.invocation_references_observed(|child| {
                self.reference()?;
                children
                    .try_reserve(1)
                    .map_err(|_| CompileControlError::ResourceExhausted)?;
                children.push(child);
                Ok::<_, ExpressionOccurrenceError>(())
            })?;
        }
        let mut arguments = Vec::new();
        arguments
            .try_reserve_exact(children.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for (ordinal, child) in children.iter().enumerate() {
            let (child_demand, guard) =
                control_argument_semantics(shape, children.len(), ordinal, demand)
                    .map_err(ExpressionOccurrenceError::flow)?;
            let child_region = if let Some(kind) = guard {
                self.region(Some(region), Some(RegionGuard { owner: id, kind }))?
            } else {
                region
            };
            let child_use = self.invocation(*child, child_region, child_demand, depth + 1)?;
            arguments.push(child_use);
            self.work.step()?;
        }
        if let Some(definitions) = source_definitions {
            self.work.flush()?;
            let mut sources = Vec::new();
            sources
                .try_reserve_exact(arguments.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            self.work.flush()?;
            let roles = definitions.facts.shape().roles();
            for (ordinal, (&definition, &use_id)) in
                definitions.definitions.iter().zip(&arguments).enumerate()
            {
                sources.push(TemporalSourceOccurrence {
                    role: roles[ordinal].ok_or(ExpressionOccurrenceError::TemporalSource(
                        "temporal source role is absent",
                    ))?,
                    definition,
                    use_id,
                });
                self.work.step()?;
            }
            self.work.flush()?;
            let plan = TemporalSourcePlan {
                facts: definitions.facts,
                sources: sources.into_boxed_slice(),
            };
            self.work.flush()?;
            plan.validate_structure().map_err(|_| {
                ExpressionOccurrenceError::TemporalSource(
                    "temporal source occurrence grammar is invalid",
                )
            })?;
            self.work.flush()?;
            self.temporal_sources.insert(id, plan);
            self.work.flush()?;
        }
        self.uses[id.get() as usize].arguments = arguments.into_boxed_slice();
        self.work.step()?;
        Ok(id)
    }
}

pub(super) use crate::compiler::selected_scalar_call::scalar_shape;

#[cfg(test)]
#[path = "expression_occurrences_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "relational_occurrences_tests.rs"]
mod relational_tests;
