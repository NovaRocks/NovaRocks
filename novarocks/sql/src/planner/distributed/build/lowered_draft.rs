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

//! SQL-owned lowering sources retained beside the immutable physical plan.

use std::{collections::BTreeMap, sync::Arc};

use novarocks_physical_plan::{
    AggregateCall, AggregateCallId, AggregatePhase, ExprId, Fragment, FragmentId, NodeKind,
    PhysicalCallSite, PhysicalNode, PhysicalPlan, PlanAnnotation, PlanBuilder,
    PlanConstructionError, ValueId, WriterAggregateCall,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

use crate::binding::{CapturedAggregateLogicalRequest, CapturedLogicalCallArguments};

mod dependency_loan;
mod result_declaration;
use crate::compiler::SqlPhysicalEmissionMode;
pub use dependency_loan::{
    SqlCallDependencyLoan, SqlCallDependencyProvenance, SqlCallDependencySite,
    SqlCanonicalDependencyLoan,
};
use result_declaration::PublishedSqlResultDeclaration;
pub use result_declaration::{
    CheckedSqlResultDeclaration, ResultDeclarationError, SqlResultDeclaration,
};

/// Closed completed publication; an invalid mode/receipt pair cannot survive here.
#[derive(Clone, Debug)]
enum PublishedResultSource {
    OriginalNativeV1,
    Exact(Arc<PublishedSqlResultDeclaration>),
}
impl PublishedResultSource {
    fn publish(
        mode: SqlPhysicalEmissionMode,
        declaration: Option<SqlResultDeclaration>,
        plan: &PhysicalPlan,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ResultDeclarationError> {
        match (mode, declaration) {
            (SqlPhysicalEmissionMode::OriginalNativeV1, None) => Ok(Self::OriginalNativeV1),
            (SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration, Some(declaration)) => {
                let port = plan
                    .result_port()
                    .ok_or(ResultDeclarationError::Association(
                        "exact SQL emission has no actual computed result port",
                    ))?;
                let declaration = declaration.publish(plan.version(), port, control)?;
                Ok(Self::Exact(Arc::new(declaration)))
            }
            _ => Err(ResultDeclarationError::Association(
                "SQL emission mode differs from its original result declaration presence",
            )),
        }
    }
    const fn mode(&self) -> SqlPhysicalEmissionMode {
        match self {
            Self::OriginalNativeV1 => SqlPhysicalEmissionMode::OriginalNativeV1,
            Self::Exact(_) => SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
        }
    }
}
fn result_declaration_construction_error(error: ResultDeclarationError) -> PlanConstructionError {
    match error {
        ResultDeclarationError::Control(cause) => PlanConstructionError::Constants(
            novarocks_physical_plan::ConstantReferenceError::Control(cause),
        ),
        ResultDeclarationError::Type(cause) => PlanConstructionError::Constants(cause.into()),
        ResultDeclarationError::Association(detail) => PlanConstructionError::Constants(
            novarocks_physical_plan::ConstantReferenceError::InvalidConsumer(detail),
        ),
    }
}

mod operational_channels;
pub(super) mod state_sources;
pub(crate) use operational_channels::SqlOperationalProjectionError;
pub(super) use operational_channels::{
    CapturedOperationalSource, EmittedOperationalCall, LoweredOperationalChannel,
    SqlOperationalChannelRole, project_emitted_call_arguments_observed,
    project_emitted_writer_value_observed,
};
pub(super) use state_sources::{
    AggregateStateEndpoint, AggregateStateLink, AggregateStateSources, AggregateStateTransport,
};
pub(crate) use state_sources::{CheckedAggregateStateInputs, CheckedWriterAggregateStateInputs};

/// A completed SQL source owner. Its physical view is read-only; consuming a
/// statement or DML result transfers this entire owner, including its original
/// logical-source journal. This is not certification of fresh kernel coverage,
/// a neutral wire source, a FragmentPackage, or a runtime allocation grant.
#[derive(Clone)]
pub struct SqlAuthoredPhysicalPlan {
    plan: Arc<PhysicalPlan>,
    call_sources: Arc<SqlLogicalSourceJournal>,
    functions: Arc<dyn crate::compiler::SqlFunctionCatalog>,
    public_result_source: PublishedResultSource,
}
impl std::fmt::Debug for SqlAuthoredPhysicalPlan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqlAuthoredPhysicalPlan")
            .field("plan", &self.plan)
            .field("call_sources", &self.call_sources)
            .field("emission_mode", &self.emission_mode())
            .field("functions", &"retained immutable catalogue")
            .finish()
    }
}
impl SqlAuthoredPhysicalPlan {
    /// Frozen at the first host request and sealed by the same emission publication.
    pub const fn emission_mode(&self) -> SqlPhysicalEmissionMode {
        self.public_result_source.mode()
    }

    /// The immutable publication proof loans the original port without reconstructing
    /// another schema or choosing a mode at this consumer boundary.
    pub fn original_public_result_declaration(&self) -> Option<CheckedSqlResultDeclaration<'_>> {
        match &self.public_result_source {
            PublishedResultSource::OriginalNativeV1 => self
                .plan
                .result_port()
                .map(CheckedSqlResultDeclaration::from_original_port),
            PublishedResultSource::Exact(declaration) => Some(declaration.checked_loan()),
        }
    }
    /// Re-observe the association against this owner's actual immutable plan.
    pub fn checked_original_result_declaration_observed(
        &self,
        control: &dyn PureCompileControl,
    ) -> Result<Option<CheckedSqlResultDeclaration<'_>>, ResultDeclarationError> {
        match &self.public_result_source {
            PublishedResultSource::OriginalNativeV1 => {
                let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
                work.step()?;
                let loan = self.original_public_result_declaration();
                work.finish()?;
                Ok(loan)
            }
            PublishedResultSource::Exact(declaration) => {
                let port = self
                    .plan
                    .result_port()
                    .ok_or(ResultDeclarationError::Association(
                        "published exact SQL source has no computed port",
                    ))?;
                declaration
                    .recheck_observed(self.plan.version(), port, control)
                    .map(Some)
            }
        }
    }

    /// The original completion snapshot moves with this source owner. Fresh
    /// preparation borrows it directly; it must never capture a second one.
    pub(crate) const fn function_catalog(&self) -> &Arc<dyn crate::compiler::SqlFunctionCatalog> {
        &self.functions
    }

    pub fn plan(&self) -> &PhysicalPlan {
        &self.plan
    }
    pub const fn plan_arc(&self) -> &Arc<PhysicalPlan> {
        &self.plan
    }

    /// Freeze only the final application's output purpose while consuming this
    /// same SQL source. Aliases, journal and original public declaration stay paired.
    pub fn with_root_output(
        mut self,
        output: novarocks_result_contract::RootOutputContract,
    ) -> Result<Self, String> {
        let plan = Arc::try_unwrap(self.plan).map_err(|_| {
            "root purpose must be frozen before plan aliases are published".to_string()
        })?;
        self.plan = Arc::new(
            plan.with_root_output(output)
                .map_err(|error| error.to_string())?,
        );

        Ok(self)
    }

    /// Loan an ordinary SQL call's original request from its exact emission.
    /// A physical expression's constant shape does not reconstruct presence.
    /// This does not provide late typed projection, use/domain scope or effects.
    pub(crate) fn checked_scalar_source_observed<'a>(
        &'a self,
        fragment: &'a Fragment,
        source: &'a novarocks_physical_plan::ExprNode,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedExpressionLogicalSourceEntry<'a>, SqlSourceJournalError> {
        self.checked_expression_source_observed(
            fragment,
            source,
            Some(SqlExpressionCallKind::Scalar),
            work,
        )
    }

    /// Original synthetic conversion request at its actual intermediate call.
    /// This is distinct from an ordinary typed FunctionCall producer.
    pub(crate) fn checked_conversion_source_observed<'a>(
        &'a self,
        fragment: &'a Fragment,
        source: &'a novarocks_physical_plan::ExprNode,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedExpressionLogicalSourceEntry<'a>, SqlSourceJournalError> {
        self.checked_expression_source_observed(
            fragment,
            source,
            Some(SqlExpressionCallKind::ValueConversion),
            work,
        )
    }

    /// A derived descriptor's original request at its exact emission site.
    pub(crate) fn checked_variant_source_observed<'a>(
        &'a self,
        fragment: &'a Fragment,
        source: &'a novarocks_physical_plan::ExprNode,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedExpressionLogicalSourceEntry<'a>, SqlSourceJournalError> {
        self.checked_expression_source_observed(
            fragment,
            source,
            Some(SqlExpressionCallKind::DerivedVariant),
            work,
        )
    }

    /// Window positional arguments followed by function ORDER BY channels.
    /// Partition/order/frame expressions have separate static owners.
    pub(crate) fn checked_window_source_observed<'a>(
        &'a self,
        fragment: &'a Fragment,
        source: &'a novarocks_physical_plan::ExprNode,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedExpressionLogicalSourceEntry<'a>, SqlSourceJournalError> {
        self.checked_expression_source_observed(
            fragment,
            source,
            Some(SqlExpressionCallKind::Window),
            work,
        )
    }

    /// Dispatch from the sealed original journal, never from a function name
    /// or by trying another producer after a refusal.
    pub(crate) fn checked_expression_call_source_observed<'a>(
        &'a self,
        fragment: &'a Fragment,
        source: &'a novarocks_physical_plan::ExprNode,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedExpressionLogicalSourceEntry<'a>, SqlSourceJournalError> {
        self.checked_expression_source_observed(fragment, source, None, work)
    }

    fn checked_expression_source_observed<'a>(
        &'a self,
        fragment: &'a Fragment,
        source: &'a novarocks_physical_plan::ExprNode,
        kind: Option<SqlExpressionCallKind>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedExpressionLogicalSourceEntry<'a>, SqlSourceJournalError> {
        work.flush()?;
        let same_fragment = self
            .plan
            .fragments()
            .get(&fragment.id())
            .is_some_and(|original| std::ptr::eq(original, fragment));
        work.step()?;
        let same_expression = fragment
            .expressions()
            .get(source.id)
            .is_some_and(|original| std::ptr::eq(original, source));
        work.step()?;
        if !same_fragment || !same_expression {
            return Err(SqlSourceJournalError::InvalidSource(
                "expression journal loans a foreign plan or expression",
            ));
        }
        let entry = self
            .call_sources
            .expression_entries
            .get(&(fragment.id(), source.id));
        work.step()?;
        let entry = entry.ok_or(SqlSourceJournalError::MissingEntry)?;
        let same_kind = kind.is_none_or(|kind| entry.kind == kind);
        work.step()?;
        if !same_kind {
            return Err(SqlSourceJournalError::InvalidSource(
                "expression journal loan uses a different call lifecycle",
            ));
        }
        validate_expression_source_entry_observed(entry, source, work)?;
        work.flush()?;
        Ok(CheckedExpressionLogicalSourceEntry {
            owner: self,
            entry,
            fragment,
            source,
        })
    }

    /// Only the actual Table lowering producer can loan this relation request.
    /// Its selected relation remains whole; no scalar result is invented.
    pub(crate) fn checked_table_source_observed<'a>(
        &'a self,
        fragment: &'a Fragment,
        source: &'a PhysicalNode,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedTableLogicalSourceEntry<'a>, SqlSourceJournalError> {
        work.flush()?;
        let same_fragment = self
            .plan
            .fragments()
            .get(&fragment.id())
            .is_some_and(|original| std::ptr::eq(original, fragment));
        work.step()?;
        let same_node = fragment
            .nodes()
            .get(&source.id)
            .is_some_and(|original| std::ptr::eq(original, source));
        work.step()?;
        if !same_fragment || !same_node {
            return Err(SqlSourceJournalError::InvalidSource(
                "table journal loans a foreign plan or node",
            ));
        }
        let entry = self
            .call_sources
            .table_entries
            .get(&(fragment.id(), source.id));
        work.step()?;
        let entry = entry.ok_or(SqlSourceJournalError::MissingEntry)?;
        validate_table_source_entry_observed(entry, source, work)?;
        work.flush()?;
        Ok(CheckedTableLogicalSourceEntry {
            owner: self,
            entry,
            fragment,
            source,
        })
    }

    fn check_aggregate_owner_observed(
        &self,
        fragment: &Fragment,
        node: &PhysicalNode,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError> {
        work.flush()?;
        let original_fragment = self.plan.fragments().get(&fragment.id());
        let same_fragment = original_fragment.is_some_and(|value| std::ptr::eq(value, fragment));
        work.step()?;
        let same_node = fragment
            .nodes()
            .get(&node.id)
            .is_some_and(|value| std::ptr::eq(value, node));
        work.step()?;
        if !same_fragment || !same_node {
            return Err(SqlSourceJournalError::InvalidSource(
                "aggregate journal loans a foreign plan or node",
            ));
        }
        Ok(())
    }

    /// Only an entry sealed by the actual lowering producer can loan an
    /// authenticated request. Same-signature foreign plans cannot supply it.
    pub(crate) fn checked_aggregate_source_observed<'a>(
        &'a self,
        fragment: &'a Fragment,
        node: &'a PhysicalNode,
        site: PhysicalCallSite,
        source: &'a AggregateCall,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedAggregateLogicalSourceEntry<'a>, SqlSourceJournalError> {
        self.check_aggregate_owner_observed(fragment, node, work)?;
        let actual = match (site, &node.kind) {
            (PhysicalCallSite::Aggregate { node: id, call }, NodeKind::Aggregate { calls, .. })
                if id == node.id =>
            {
                calls.get(call as usize)
            }
            (
                PhysicalCallSite::TopNState { node: id, call },
                NodeKind::TopN {
                    reduction: novarocks_physical_plan::TopNReduction::GroupedStates { calls, .. },
                    ..
                },
            ) if id == node.id => calls.get(call as usize),
            _ => None,
        };
        let same_call = actual.is_some_and(|value| std::ptr::eq(value, source));
        work.step()?;
        if !same_call {
            return Err(SqlSourceJournalError::InvalidSource(
                "aggregate journal call differs from its original site",
            ));
        }
        let entry = self.call_sources.entries.get(&(fragment.id(), site));
        work.step()?;
        let entry = entry.ok_or(SqlSourceJournalError::MissingEntry)?;
        let same_phase = entry.phase == source.binding.phase;
        let same_call_id = entry.target == AggregateSourceTarget::Aggregate(source.id);
        work.step()?;
        if !same_phase || !same_call_id {
            return Err(SqlSourceJournalError::InvalidSource(
                "aggregate journal phase or producer association differs",
            ));
        }
        let expected_state = match entry.phase {
            AggregatePhase::Single | AggregatePhase::Partial { .. } => {
                AggregateRuntimeDemand::Update
            }
            AggregatePhase::Intermediate { .. } | AggregatePhase::Final { .. } => {
                if source.arguments.len() != 1 || !source.order_by.is_empty() || source.distinct {
                    return Err(SqlSourceJournalError::InvalidSource(
                        "aggregate merge has invalid physical state channels",
                    ));
                }
                AggregateRuntimeDemand::ExpressionState(source.arguments[0])
            }
        };
        work.step()?;
        if entry.runtime != expected_state {
            return Err(SqlSourceJournalError::InvalidSource(
                "aggregate journal runtime state differs from its producer",
            ));
        }
        let captured = match &entry.logical {
            LoweredAggregateLogicalSource::Captured(captured) => captured,
            LoweredAggregateLogicalSource::Uncertified => {
                return Err(SqlSourceJournalError::MissingLogicalSource);
            }
        };
        work.step()?;
        work.flush()?;
        Ok(CheckedAggregateLogicalSourceEntry {
            owner: self,
            captured,
            canonical: entry.canonical.as_ref(),
            phase: entry.phase,
            runtime: entry.runtime,
            fragment,
            node,
            site,
            source,
        })
    }

    /// A Writer loans its actual ValueId input and original request directly.
    /// This authenticates the emission; it grants no state-contribution proof.
    pub(crate) fn checked_writer_aggregate_source_observed<'a>(
        &'a self,
        fragment: &'a Fragment,
        node: &'a PhysicalNode,
        site: PhysicalCallSite,
        source: &'a WriterAggregateCall,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedWriterAggregateLogicalSourceEntry<'a>, SqlSourceJournalError> {
        self.check_aggregate_owner_observed(fragment, node, work)?;
        let actual = match (site, &node.kind) {
            (
                PhysicalCallSite::WriterPartial { node: id, call },
                NodeKind::TableWriter { target },
            ) if id == node.id => target.partial_aggregates.get(call as usize),
            (PhysicalCallSite::WriterFinal { node: id, call }, NodeKind::TableFinish(finish))
                if id == node.id =>
            {
                finish.final_aggregates.get(call as usize)
            }
            _ => None,
        };
        let same_call = actual.is_some_and(|value| std::ptr::eq(value, source));
        work.step()?;
        if !same_call {
            return Err(SqlSourceJournalError::InvalidSource(
                "writer journal call differs from its original site",
            ));
        }
        let entry = self.call_sources.entries.get(&(fragment.id(), site));
        work.step()?;
        let entry = entry.ok_or(SqlSourceJournalError::MissingEntry)?;
        let same_target = entry.target == AggregateSourceTarget::Writer(source.output)
            && entry.phase == source.binding.phase;
        work.step()?;
        if !same_target {
            return Err(SqlSourceJournalError::InvalidSource(
                "writer journal phase or output association differs",
            ));
        }
        let runtime = match (site, entry.phase) {
            (PhysicalCallSite::WriterPartial { .. }, AggregatePhase::Partial { .. }) => {
                Ok(AggregateRuntimeDemand::Update)
            }
            (PhysicalCallSite::WriterFinal { .. }, AggregatePhase::Final { .. }) => {
                Ok(AggregateRuntimeDemand::WriterState(source.input))
            }
            _ => Err(SqlSourceJournalError::InvalidSource(
                "writer journal phase differs from its original lifecycle",
            )),
        };
        work.step()?;
        let runtime = runtime?;
        let same_runtime = entry.runtime == runtime;
        work.step()?;
        if !same_runtime {
            return Err(SqlSourceJournalError::InvalidSource(
                "writer journal runtime differs from its original input",
            ));
        }
        let captured = entry
            .logical
            .captured()
            .ok_or(SqlSourceJournalError::MissingLogicalSource);
        work.step()?;
        let captured = captured?;
        work.flush()?;
        Ok(CheckedWriterAggregateLogicalSourceEntry {
            owner: self,
            captured,
            canonical: entry.canonical.as_ref(),
            phase: entry.phase,
            runtime,
            fragment,
            node,
            site,
            source,
        })
    }
}

/// A loan from the original Writer call; an ordinary AggregateCall cannot
/// stand in for this ValueId-based lifecycle.
pub(crate) struct CheckedWriterAggregateLogicalSourceEntry<'a> {
    owner: &'a SqlAuthoredPhysicalPlan,
    captured: &'a CapturedAggregateLogicalRequest,
    canonical: Option<&'a Arc<CanonicalAggregateOperationalRequest>>,
    phase: AggregatePhase,
    runtime: AggregateRuntimeDemand,
    fragment: &'a Fragment,
    node: &'a PhysicalNode,
    site: PhysicalCallSite,
    source: &'a WriterAggregateCall,
}
impl<'a> CheckedWriterAggregateLogicalSourceEntry<'a> {
    /// Borrow exactly the immutable catalog retained by this source owner.
    pub(crate) fn function_catalog(&self) -> &'a Arc<dyn crate::compiler::SqlFunctionCatalog> {
        self.owner.function_catalog()
    }

    pub(crate) fn state_inputs_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedWriterAggregateStateInputs<'a>, SqlSourceJournalError> {
        state_sources::check_writer_state_inputs_observed(self, work)
    }
    pub(crate) const fn captured(&self) -> &'a CapturedAggregateLogicalRequest {
        self.captured
    }
    pub(crate) const fn canonical(&self) -> Option<&'a Arc<CanonicalAggregateOperationalRequest>> {
        self.canonical
    }
    pub(crate) const fn phase(&self) -> AggregatePhase {
        self.phase
    }
    pub(crate) const fn runtime(&self) -> AggregateRuntimeDemand {
        self.runtime
    }
    pub(crate) const fn fragment(&self) -> &'a Fragment {
        self.fragment
    }
    pub(crate) const fn node(&self) -> &'a PhysicalNode {
        self.node
    }
    pub(crate) const fn site(&self) -> PhysicalCallSite {
        self.site
    }
    pub(crate) const fn source(&self) -> &'a WriterAggregateCall {
        self.source
    }
}

/// Only the source owner can create this original emission loan. There is no
/// conversion from a binding, a selected signature or captured data alone.
pub(crate) struct CheckedExpressionLogicalSourceEntry<'a> {
    owner: &'a SqlAuthoredPhysicalPlan,
    entry: &'a LoweredExpressionSourceEntry,
    fragment: &'a Fragment,
    source: &'a novarocks_physical_plan::ExprNode,
}
impl<'a> CheckedExpressionLogicalSourceEntry<'a> {
    pub(crate) fn function_catalog(&self) -> &'a Arc<dyn crate::compiler::SqlFunctionCatalog> {
        self.owner.function_catalog()
    }
    pub(super) const fn kind(&self) -> SqlExpressionCallKind {
        self.entry.kind
    }
    pub(crate) fn captured(&self) -> &'a CapturedLogicalCallArguments {
        self.entry.captured.captured()
    }
    pub(crate) const fn fragment(&self) -> &'a Fragment {
        self.fragment
    }
    pub(crate) const fn source(&self) -> &'a novarocks_physical_plan::ExprNode {
        self.source
    }
    pub(crate) fn arguments(&self) -> &'a [ExprId] {
        &self.entry.arguments
    }
    pub(crate) fn canonical_operational(&self) -> Option<&'a Arc<CanonicalCallOperationalRequest>> {
        self.entry.canonical_operational.as_ref()
    }
    /// The original owner selection obtained before this actual emission.
    /// Other lifecycles still have no canonical author receipt here. This
    /// metadata loan does not authenticate effects, uses or kernel coverage.
    pub(crate) fn canonical_selection(
        &self,
    ) -> Option<&'a Arc<novarocks_functions::FunctionBindingSelection>> {
        self.entry
            .canonical_operational
            .as_ref()
            .map(|request| request.selected())
    }
}

pub(super) fn validate_expression_source_entry_observed(
    entry: &LoweredExpressionSourceEntry,
    source: &novarocks_physical_plan::ExprNode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), SqlSourceJournalError> {
    let same_origin = matches!(
        (&entry.captured, entry.kind),
        (
            LoweredExpressionLogicalSource::DerivedVariant(_),
            SqlExpressionCallKind::DerivedVariant
        ) | (
            LoweredExpressionLogicalSource::Owned(_),
            SqlExpressionCallKind::Scalar
                | SqlExpressionCallKind::Window
                | SqlExpressionCallKind::ValueConversion
        )
    );
    work.step()?;
    if !same_origin {
        return Err(SqlSourceJournalError::InvalidSource(
            "expression journal has a different source producer",
        ));
    }
    let same_scope = source.owner == entry.owner && source.lambda_scope == entry.lambda_scope;
    work.step()?;
    let (args, order) = match (&source.kind, entry.kind) {
        (
            novarocks_physical_plan::ExprKind::FunctionCall { args, .. },
            SqlExpressionCallKind::Scalar
            | SqlExpressionCallKind::ValueConversion
            | SqlExpressionCallKind::DerivedVariant,
        ) => (args.as_ref(), &[][..]),
        (
            novarocks_physical_plan::ExprKind::WindowCall {
                args,
                function_order_by,
                ..
            },
            SqlExpressionCallKind::Window,
        ) => (args.as_ref(), function_order_by.as_ref()),
        _ => {
            return Err(SqlSourceJournalError::InvalidSource(
                "expression journal site has a different call lifecycle",
            ));
        }
    };
    let count = args
        .len()
        .checked_add(order.len())
        .ok_or(CompileControlError::ResourceExhausted)?;
    let request = entry.captured.request();
    let same_count = count == entry.arguments.len()
        && count == entry.channels.len()
        && count == request.arguments.len()
        && args.len() == request.logical_argument_count;
    work.step()?;
    if !same_scope || !same_count {
        return Err(SqlSourceJournalError::InvalidSource(
            "expression journal scope or channel count differs from its emission",
        ));
    }
    for ((actual, original), channel) in args
        .iter()
        .copied()
        .chain(order.iter().map(|key| key.expr))
        .zip(entry.arguments.iter().copied())
        .zip(entry.channels.iter())
    {
        let same_argument = actual == original && actual == channel.expression;
        work.step()?;
        if !same_argument {
            return Err(SqlSourceJournalError::InvalidSource(
                "expression journal channel differs from its original expression identity",
            ));
        }
    }
    Ok(())
}

/// Original table producer loan; constructors remain private to this journal.
pub(crate) struct CheckedTableLogicalSourceEntry<'a> {
    owner: &'a SqlAuthoredPhysicalPlan,
    entry: &'a LoweredTableSourceEntry,
    fragment: &'a Fragment,
    source: &'a PhysicalNode,
}
impl<'a> CheckedTableLogicalSourceEntry<'a> {
    pub(crate) fn function_catalog(&self) -> &'a Arc<dyn crate::compiler::SqlFunctionCatalog> {
        self.owner.function_catalog()
    }
    pub(crate) fn canonical_operational(&self) -> &'a Arc<CanonicalCallOperationalRequest> {
        &self.entry.canonical_operational
    }

    pub(crate) fn captured(&self) -> &'a CapturedLogicalCallArguments {
        &self.entry.captured
    }
    pub(crate) const fn fragment(&self) -> &'a Fragment {
        self.fragment
    }
    pub(crate) const fn source(&self) -> &'a PhysicalNode {
        self.source
    }
    pub(crate) fn arguments(&self) -> &'a [ExprId] {
        &self.entry.arguments
    }
    /// Metadata authored by the original owner before this exact table site.
    /// This is not an effects, runtime-use or pure implementation certificate.
    pub(crate) fn canonical_selection(
        &self,
    ) -> &'a Arc<novarocks_functions::FunctionBindingSelection> {
        self.entry.canonical_operational.selected()
    }
}

pub(super) fn validate_table_source_entry_observed(
    entry: &LoweredTableSourceEntry,
    source: &PhysicalNode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), SqlSourceJournalError> {
    let NodeKind::TableFunction { arguments, .. } = &source.kind else {
        return Err(SqlSourceJournalError::InvalidSource(
            "table journal site is not its original producer",
        ));
    };
    let request = entry.captured.request();
    let same_count = arguments.len() == entry.arguments.len()
        && arguments.len() == entry.channels.len()
        && arguments.len() == request.arguments.len()
        && arguments.len() == request.logical_argument_count;
    work.step()?;
    if !same_count {
        return Err(SqlSourceJournalError::InvalidSource(
            "table journal channel count differs from its emission",
        ));
    }
    for ((actual, original), channel) in arguments
        .iter()
        .zip(entry.arguments.iter())
        .zip(entry.channels.iter())
    {
        let same_argument = actual == original && *actual == channel.expression;
        work.step()?;
        if !same_argument {
            return Err(SqlSourceJournalError::InvalidSource(
                "table journal channel differs from its original expression identity",
            ));
        }
    }
    Ok(())
}

#[derive(Debug)]
pub enum SqlSourceJournalError {
    Control(CompileControlError),
    MissingEntry,
    MissingLogicalSource,
    InvalidSource(&'static str),
}
impl From<CompileControlError> for SqlSourceJournalError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}

/// A borrowed original producer association. Construction is private to this
/// journal; type/signature equality never creates one. Selected correspondence
/// and actual runtime state-domain validation remain their sole owners.
pub(crate) struct CheckedAggregateLogicalSourceEntry<'a> {
    owner: &'a SqlAuthoredPhysicalPlan,
    captured: &'a CapturedAggregateLogicalRequest,
    canonical: Option<&'a Arc<CanonicalAggregateOperationalRequest>>,
    phase: AggregatePhase,
    runtime: AggregateRuntimeDemand,
    fragment: &'a Fragment,
    node: &'a PhysicalNode,
    site: PhysicalCallSite,
    source: &'a AggregateCall,
}
impl<'a> CheckedAggregateLogicalSourceEntry<'a> {
    pub(crate) fn state_inputs_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedAggregateStateInputs<'a>, SqlSourceJournalError> {
        state_sources::check_state_inputs_observed(self, work)
    }
    pub(crate) const fn captured(&self) -> &'a CapturedAggregateLogicalRequest {
        self.captured
    }
    pub(crate) const fn canonical(&self) -> Option<&'a Arc<CanonicalAggregateOperationalRequest>> {
        self.canonical
    }
    pub(crate) const fn phase(&self) -> AggregatePhase {
        self.phase
    }
    pub(crate) const fn runtime(&self) -> AggregateRuntimeDemand {
        self.runtime
    }
    pub(crate) const fn fragment(&self) -> &'a Fragment {
        self.fragment
    }
    pub(crate) const fn node(&self) -> &'a PhysicalNode {
        self.node
    }
    pub(crate) const fn site(&self) -> PhysicalCallSite {
        self.site
    }
    pub(crate) const fn source(&self) -> &'a AggregateCall {
        self.source
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AggregateSourceTarget {
    Aggregate(AggregateCallId),
    Writer(ValueId),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateRuntimeDemand {
    Update,
    ExpressionState(ExprId),
    WriterState(ValueId),
}
#[derive(Debug)]
pub(super) enum LoweredAggregateLogicalSource {
    Captured(Arc<CapturedAggregateLogicalRequest>),
    /// Existing manual/structural IR explicitly lacks an authenticated update.
    /// Retention cannot upgrade it; a fresh-source loan returns MissingLogicalSource.
    Uncertified,
}
impl LoweredAggregateLogicalSource {
    pub(super) fn captured(&self) -> Option<&CapturedAggregateLogicalRequest> {
        match self {
            Self::Captured(source) => Some(source),
            Self::Uncertified => None,
        }
    }
}
#[derive(Debug)]
pub(super) struct LoweredAggregateSourceEntry {
    pub(super) logical: LoweredAggregateLogicalSource,
    /// Updates are authored from their actual emitted channels. State-consuming
    /// families retain their existing source path until producer lending lands.
    pub(super) canonical: Option<Arc<CanonicalAggregateOperationalRequest>>,
    pub(super) phase: AggregatePhase,
    pub(super) runtime: AggregateRuntimeDemand,
    pub(super) target: AggregateSourceTarget,
}

/// Same-emission operational metadata, independent of the original capture.
/// Private SQL publication retains the actual request and selected Arc together;
/// this record is not an effect, kernel, phase or runtime-state certificate.
#[derive(Debug)]
pub(crate) struct CanonicalAggregateOperationalRequest {
    pub(super) binding: crate::binding::SqlFunctionBinding,
    pub(super) identity: crate::binding::AggregateLogicalSourceIdentity,
    pub(super) arguments: Box<[novarocks_functions::FunctionArgument]>,
    pub(super) logical_count: usize,
    pub(super) selected: Arc<novarocks_functions::FunctionBindingSelection>,
    pub(super) result_constraint: Option<novarocks_functions::FunctionValueType>,
}
impl CanonicalAggregateOperationalRequest {
    pub(crate) fn request(&self) -> novarocks_functions::FunctionBindingRequest<'_> {
        novarocks_functions::FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.logical_count,
            expected_result_type: self.result_constraint.as_ref(),
        }
    }
    pub(crate) const fn selected(&self) -> &Arc<novarocks_functions::FunctionBindingSelection> {
        &self.selected
    }
    pub(crate) fn belongs_to(&self, captured: &CapturedAggregateLogicalRequest) -> bool {
        self.identity.same_revision(captured.logical_identity())
            && std::ptr::eq(self.binding.resolved(), captured.binding().resolved())
    }
}
/// The exact original emitter projection and fixed selection travel together.
/// This pure data is not a fresh capability, occurrence proof or funding grant.
#[derive(Debug)]
pub(crate) struct CanonicalCallOperationalRequest {
    pub(super) binding: crate::binding::SqlFunctionBinding,
    pub(super) arguments: Box<[novarocks_functions::FunctionArgument]>,
    pub(super) logical_count: usize,
    pub(super) selected: Arc<novarocks_functions::FunctionBindingSelection>,
    pub(super) result_constraint: Option<novarocks_functions::FunctionValueType>,
}
impl CanonicalCallOperationalRequest {
    pub(crate) fn belongs_to(&self, captured: &CapturedLogicalCallArguments) -> bool {
        std::ptr::eq(self.binding.resolved(), captured.binding().resolved())
    }
    pub(crate) fn request(&self) -> novarocks_functions::FunctionBindingRequest<'_> {
        novarocks_functions::FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.logical_count,
            expected_result_type: self.result_constraint.as_ref(),
        }
    }
    pub(crate) const fn selected(&self) -> &Arc<novarocks_functions::FunctionBindingSelection> {
        &self.selected
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlExpressionCallKind {
    Scalar,
    Window,
    ValueConversion,
    DerivedVariant,
}
#[derive(Debug)]
pub(super) struct LoweredTableSourceEntry {
    pub(super) canonical_operational: Arc<CanonicalCallOperationalRequest>,
    pub(super) captured: CapturedLogicalCallArguments,
    pub(super) arguments: Box<[ExprId]>,
    pub(super) channels: Box<[LoweredOperationalChannel]>,
}
pub(super) enum LoweredExpressionLogicalSource {
    Owned(CapturedLogicalCallArguments),
    DerivedVariant(Arc<crate::common::variant_source::DerivedVariantSource>),
}
impl LoweredExpressionLogicalSource {
    pub(super) fn captured(&self) -> &CapturedLogicalCallArguments {
        match self {
            Self::Owned(original) => original,
            Self::DerivedVariant(source) => source.captured(),
        }
    }
    pub(super) fn request(&self) -> novarocks_functions::FunctionBindingRequest<'_> {
        self.captured().request()
    }
}
impl std::fmt::Debug for LoweredExpressionLogicalSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Owned(source) => f.debug_tuple("Owned").field(source).finish(),
            Self::DerivedVariant(_) => f.write_str("DerivedVariantSource"),
        }
    }
}
#[derive(Debug)]
pub(super) struct LoweredExpressionSourceEntry {
    pub(super) kind: SqlExpressionCallKind,
    pub(super) canonical_operational: Option<Arc<CanonicalCallOperationalRequest>>,
    pub(super) captured: LoweredExpressionLogicalSource,
    pub(super) owner: novarocks_physical_plan::NodeId,
    pub(super) lambda_scope: Option<ExprId>,
    pub(super) arguments: Box<[ExprId]>,
    pub(super) channels: Box<[LoweredOperationalChannel]>,
}
#[derive(Debug)]
pub(super) struct SqlLogicalSourceJournal {
    pub(super) state_sources: AggregateStateSources,
    // Original ordinary, Window, conversion and derived descriptor emissions.
    // Retained source validation does not certify fresh effects or full coverage.
    pub(super) expression_entries: BTreeMap<(FragmentId, ExprId), LoweredExpressionSourceEntry>,
    pub(super) table_entries:
        BTreeMap<(FragmentId, novarocks_physical_plan::NodeId), LoweredTableSourceEntry>,
    pub(super) entries: BTreeMap<(FragmentId, PhysicalCallSite), LoweredAggregateSourceEntry>,
}

/// Exact support refusal remains distinct from original plan construction.
#[derive(Debug)]
pub(crate) enum SqlPublicationError {
    Construction(PlanConstructionError),
    Support(super::source_support::SqlSourceSupportError),
}

/// Sole actual lowering result. Only the original visitor constructs this;
/// there is no raw-builder conversion or consuming builder accessor.
pub(crate) struct LoweredSqlPhysicalDraft {
    builder: PlanBuilder,
    call_sources: SqlLogicalSourceJournal,
    functions: Arc<dyn crate::compiler::SqlFunctionCatalog>,
    emission_mode: SqlPhysicalEmissionMode,
    result_declaration: Option<SqlResultDeclaration>,
}
impl LoweredSqlPhysicalDraft {
    pub(super) fn from_lowering(
        builder: PlanBuilder,
        call_sources: SqlLogicalSourceJournal,
        functions: Arc<dyn crate::compiler::SqlFunctionCatalog>,
        emission_mode: SqlPhysicalEmissionMode,
        result_declaration: Option<SqlResultDeclaration>,
    ) -> Self {
        Self {
            builder,
            call_sources,
            functions,
            emission_mode,
            result_declaration,
        }
    }
    pub(crate) fn add_annotation(&mut self, annotation: PlanAnnotation) {
        self.builder.add_annotation(annotation);
    }
    /// Run the sole publication once, then loan its complete admitted source.
    /// An absent observer preserves the original meter and publication exactly.
    pub(crate) fn finish_with_dependency_observer_observed(
        self,
        control: &crate::compiler::SqlCompileControl,
    ) -> Result<SqlAuthoredPhysicalPlan, SqlPublicationError> {
        let source = self
            .finish_observed(control)
            .map_err(SqlPublicationError::Construction)?;
        super::source_support::admit_all_call_definitions_observed(&source, control)
            .map_err(SqlPublicationError::Support)?;
        if let Some(observer) = control.fold_dependency_observer() {
            observer
                .observe_published_source_observed(&source, control)
                .map_err(|cause| {
                    SqlPublicationError::Construction(PlanConstructionError::Constants(
                        novarocks_physical_plan::ConstantReferenceError::Control(cause),
                    ))
                })?;
        }
        Ok(source)
    }

    pub(crate) fn finish_observed(
        self,
        control: &dyn PureCompileControl,
    ) -> Result<SqlAuthoredPhysicalPlan, PlanConstructionError> {
        let control_error = |cause| {
            PlanConstructionError::Constants(
                novarocks_physical_plan::ConstantReferenceError::Control(cause),
            )
        };
        let mut work =
            CompileCheckpoints::try_new(control, CompilePhase::Validate).map_err(control_error)?;
        let result = (|| {
            work.flush().map_err(control_error)?;
            if !matches!(
                (&self.emission_mode, &self.result_declaration),
                (SqlPhysicalEmissionMode::OriginalNativeV1, None)
                    | (
                        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
                        Some(_)
                    )
            ) {
                return Err(result_declaration_construction_error(
                    ResultDeclarationError::Association(
                        "SQL emission mode differs from its original result declaration presence",
                    ),
                ));
            }
            let plan = self.builder.finish_observed(control)?;
            let public_result_source = PublishedResultSource::publish(
                self.emission_mode,
                self.result_declaration,
                &plan,
                control,
            )
            .map_err(result_declaration_construction_error)?;
            work.flush().map_err(control_error)?;
            let plan = Arc::new(plan);
            work.step().map_err(control_error)?;
            work.flush().map_err(control_error)?;
            let call_sources = Arc::new(self.call_sources);
            work.step().map_err(control_error)?;
            // The source journal moves with this exact immutable plan. The
            // two opaque Arc allocations retain caller admission obligations.
            Ok(SqlAuthoredPhysicalPlan {
                plan,
                call_sources,
                functions: self.functions,
                public_result_source,
            })
        })();
        if matches!(
            &result,
            Err(PlanConstructionError::Constants(
                novarocks_physical_plan::ConstantReferenceError::Control(_)
            ))
        ) {
            return result;
        }
        work.finish().map_err(control_error)?;
        result
    }
}

#[cfg(test)]
#[path = "lowered_draft_publication_tests.rs"]
mod publication_tests;
