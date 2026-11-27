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

//! Borrow the actual original emission before mandatory source admission.
//! This test-only observation cannot finish a plan or skip a production gate.
use super::*;

pub(in crate::planner::distributed::build) trait OriginalEmissionObserver {
    fn observe(
        &mut self,
        view: OriginalEmissionView<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ContractLoweringError>;
}

pub(in crate::planner::distributed::build) struct OriginalEmissionView<'source> {
    pub(super) fragments: &'source BTreeMap<FragmentId, FragmentBuilder>,
    pub(super) journal: &'source SqlLogicalSourceJournal,
    pub(super) completions: &'source BTreeMap<FragmentId, (NodeId, FragmentSink)>,
    pub(super) edges: &'source BTreeMap<EdgeId, Edge>,
    pub(super) constants: &'source novarocks_physical_plan::ConstantPools,
    pub(super) parameters: &'source SemanticParameters,
    pub(super) functions: &'source Arc<dyn crate::compiler::SqlFunctionCatalog>,
}
/// Raw original Window emission. This carries no Frozen-call or publication receipt.
/// Its fields are borrowed only from the original observer's actual builder/journal.
pub(in crate::planner::distributed::build) struct OriginalWindowEmission<'source> {
    builder: &'source FragmentBuilder,
    node: &'source novarocks_physical_plan::PhysicalNode,
    source: &'source WindowCallSpec,
    entry: &'source LoweredWindowSourceEntry,
    constants: &'source novarocks_physical_plan::ConstantPools,
}
impl<'source> OriginalWindowEmission<'source> {
    pub(in crate::planner::distributed::build) fn builder(&self) -> &'source FragmentBuilder {
        self.builder
    }
    pub(in crate::planner::distributed::build) fn node(
        &self,
    ) -> &'source novarocks_physical_plan::PhysicalNode {
        self.node
    }
    pub(in crate::planner::distributed::build) fn source(&self) -> &'source WindowCallSpec {
        self.source
    }
    pub(in crate::planner::distributed::build) fn captured(
        &self,
    ) -> &'source CapturedLogicalCallArguments {
        &self.entry.captured
    }
    pub(in crate::planner::distributed::build) fn canonical(
        &self,
    ) -> &'source Arc<CanonicalCallOperationalRequest> {
        &self.entry.canonical_operational
    }
    pub(in crate::planner::distributed::build) fn arguments(&self) -> &'source [ExprId] {
        &self.entry.arguments
    }
    pub(in crate::planner::distributed::build) fn definitions(
        &self,
    ) -> super::super::unit_source::UnitDefinitions<'source> {
        super::super::unit_source::UnitDefinitions::from_draft(self.builder, self.node.id)
    }
    pub(in crate::planner::distributed::build) fn constants(
        &self,
    ) -> &'source novarocks_physical_plan::ConstantPools {
        self.constants
    }
    pub(in crate::planner::distributed::build) fn operational_arguments(
        &self,
        control: &dyn PureCompileControl,
    ) -> Result<Box<[novarocks_functions::FunctionArgument]>, SqlOperationalProjectionError> {
        project_emitted_call_arguments_observed(
            EmittedOperationalCall {
                expressions: self.definitions(),
                pools: self.constants,
                owner: self.definitions().owner(),
                lambda_scope: None,
                kind: Some(SqlExpressionCallKind::Window),
                captured: CapturedOperationalSource::call(&self.entry.captured),
                derived: None,
                arguments: &self.entry.arguments,
                channels: &self.entry.channels,
            },
            control,
        )
    }
    pub(in crate::planner::distributed::build) fn emitted_constant(
        &self,
        id: ExprId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<novarocks_constant_contract::ConstantValue, ContractLoweringError> {
        let definition = self
            .definitions()
            .definition(id)
            .expect("actual emitted constant definition");
        let ContractExprKind::Constant(reference) = definition.kind else {
            panic!("actual emitted constant");
        };
        self.reference_constant(reference, work)
    }
    pub(in crate::planner::distributed::build) fn reference_constant(
        &self,
        reference: novarocks_constant_contract::ConstantReference,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<novarocks_constant_contract::ConstantValue, ContractLoweringError> {
        Ok(self.constants.resolve_source_observed(reference, work)?)
    }
}

impl<'source> OriginalEmissionView<'source> {
    /// Borrow the actual construction slot and its raw journal record. Reuse
    /// the original draft-capable validator; do not fabricate a Frozen source.
    pub(in crate::planner::distributed::build) fn window_source(
        &self,
        fragment: FragmentId,
        site: novarocks_physical_plan::PhysicalCallSite,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<OriginalWindowEmission<'source>, super::super::lowered_draft::SqlSourceJournalError>
    {
        use super::super::lowered_draft::{
            SqlSourceJournalError, validate_window_source_entry_observed,
        };
        let absent =
            || SqlSourceJournalError::InvalidSource("original Window emission slot is absent");
        let novarocks_physical_plan::PhysicalCallSite::Window { node, call } = site else {
            return Err(SqlSourceJournalError::InvalidSource(
                "original Window emission names another lifecycle",
            ));
        };
        let builder = self.fragments.get(&fragment).ok_or_else(absent)?;
        let actual = builder.construction_node(node).ok_or_else(absent)?;
        let NodeKind::Window(spec) = &actual.kind else {
            return Err(SqlSourceJournalError::InvalidSource(
                "original Window emission has another node kind",
            ));
        };
        let source = spec.calls.get(call as usize).ok_or_else(absent)?;
        let entry = self
            .journal
            .window_entries
            .get(&(fragment, site))
            .ok_or(SqlSourceJournalError::MissingEntry)?;
        validate_window_source_entry_observed(
            entry,
            super::super::unit_source::UnitDefinitions::from_draft(builder, node),
            source,
            work,
        )?;
        work.flush()?;
        Ok(OriginalWindowEmission {
            builder,
            node: actual,
            source,
            entry,
            constants: self.constants,
        })
    }

    pub(in crate::planner::distributed::build) fn fragments(
        &self,
    ) -> &'source BTreeMap<FragmentId, FragmentBuilder> {
        self.fragments
    }
    /// Raw original entries carry no Frozen-call or publication receipt.
    pub(in crate::planner::distributed::build) fn journal(
        &self,
    ) -> &'source SqlLogicalSourceJournal {
        self.journal
    }
    pub(in crate::planner::distributed::build) fn constants(
        &self,
    ) -> &'source novarocks_physical_plan::ConstantPools {
        self.constants
    }
    pub(in crate::planner::distributed::build) fn parameters(&self) -> &'source SemanticParameters {
        self.parameters
    }
    pub(in crate::planner::distributed::build) fn functions(
        &self,
    ) -> &'source Arc<dyn crate::compiler::SqlFunctionCatalog> {
        self.functions
    }
    /// Borrow the existing original construction graph. Its visitor validates
    /// raw emission/transport facts; it does not mint completed source receipts.
    pub(in crate::planner::distributed::build) fn state_graph(
        &self,
    ) -> super::super::lowered_draft::state_sources::ConstructionStateGraph<'source> {
        super::super::lowered_draft::state_sources::ConstructionStateGraph::borrow(
            self.journal,
            self.fragments,
            self.completions,
            self.edges,
        )
    }
    /// Borrow only the actual construction node; no completed Fragment exists.
    pub(in crate::planner::distributed::build) fn actual_node(
        &self,
        fragment: FragmentId,
        node: NodeId,
    ) -> Option<&'source novarocks_physical_plan::PhysicalNode> {
        self.fragments.get(&fragment)?.construction_node(node)
    }
    /// The original parent module alone can assemble this source loan.
    /// Lookup itself adds no work event; its checker remains the original core.
    pub(in crate::planner::distributed::build) fn scalar_loan(
        &self,
        fragment: FragmentId,
        node: NodeId,
    ) -> Result<SqlUnitSourceLoan<'source>, super::super::lowered_draft::SqlSourceJournalError>
    {
        use super::super::lowered_draft::SqlSourceJournalError;
        let builder = self
            .fragments
            .get(&fragment)
            .ok_or(SqlSourceJournalError::InvalidSource(
                "original emission observer names an absent fragment",
            ))?;
        if builder.construction_node(node).is_none() || builder.construction_unit(node).is_none() {
            return Err(SqlSourceJournalError::InvalidSource(
                "original emission observer names an absent scalar node unit",
            ));
        }
        Ok(SqlUnitSourceLoan {
            site: ScalarSourceSite::Node { fragment, node },
            definitions: super::super::unit_source::UnitDefinitions::from_draft(builder, node),
            constants: self.constants,
            parameters: self.parameters,
            journal: self.journal,
            functions: self.functions,
        })
    }
    pub(in crate::planner::distributed::build) fn checked_scalar_source(
        &self,
        fragment: FragmentId,
        node: NodeId,
        definition: ExprId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<
        super::super::lowered_draft::CheckedExpressionLogicalSourceEntry<'source>,
        super::super::lowered_draft::SqlSourceJournalError,
    > {
        use super::super::lowered_draft::SqlSourceJournalError;
        let loan = self.scalar_loan(fragment, node)?;
        let source = loan.definitions().definition(definition).ok_or(
            SqlSourceJournalError::InvalidSource(
                "original emission observer names an absent scalar definition",
            ),
        )?;
        loan.checked_expression_source_observed(source, work)
    }
}

/// Register one test callback on the ordinary read's original driver.
/// A successful callback does not bypass admission or turn refusal into a plan.
pub(in crate::planner::distributed::build) fn lower_read_with_original_emission_observer<
    'control,
>(
    plan: &PhysicalPlanNode,
    version: PlanVersionId,
    dop_domain: PipelineDopDomain,
    reads: Option<FinalizedProviderReadSet>,
    functions: Arc<dyn crate::compiler::SqlFunctionCatalog>,
    constant_policy: novarocks_constant_contract::ConstantValidation,
    control: &'control dyn PureCompileControl,
    observer: &'control mut dyn OriginalEmissionObserver,
) -> Result<LoweredSqlPhysicalDraft, ContractLoweringError> {
    let mut visitor: ContractLoweringVisitor<'control> = ContractLoweringVisitor::new(
        version,
        dop_domain,
        reads,
        functions,
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        constant_policy,
        control,
    )?;
    visitor.original_emission_observer = Some(observer);
    drive_read_emission(visitor, plan, None, None)
}

/// The ordinary writer's input check and full topology driver remain shared.
pub(in crate::planner::distributed::build) fn lower_write_with_original_emission_observer<
    'control,
>(
    plan: &PhysicalPlanNode,
    version: PlanVersionId,
    dop_domain: PipelineDopDomain,
    input: FinalWriteLowering<'_>,
    functions: Arc<dyn crate::compiler::SqlFunctionCatalog>,
    constant_policy: novarocks_constant_contract::ConstantValidation,
    control: &'control dyn PureCompileControl,
    observer: &'control mut dyn OriginalEmissionObserver,
) -> Result<LoweredSqlPhysicalDraft, ContractLoweringError> {
    let FinalWriteLowering {
        reads,
        write,
        write_target_ordinal,
        auxiliary,
        targets,
    } = require_ordinary_write_input(input)?;
    let mut visitor: ContractLoweringVisitor<'control> = ContractLoweringVisitor::new(
        version,
        dop_domain,
        reads,
        functions,
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        constant_policy,
        control,
    )?;
    visitor.original_emission_observer = Some(observer);
    drive_ordinary_write_emission(
        visitor,
        plan,
        write,
        write_target_ordinal,
        auxiliary,
        targets,
    )
}
