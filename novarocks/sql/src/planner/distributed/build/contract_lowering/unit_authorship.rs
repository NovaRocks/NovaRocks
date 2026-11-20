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

//! Original lowering authors every node unit before relation preparation.
use super::super::package_semantics::PackageSemanticsError;
use super::*;
use novarocks_functions::PureCallLifecycle;

pub(super) struct FragmentUnitAuthorship {
    pub effects: BTreeMap<NodeId, NodeExpressionEffects>,
    pub relations: Vec<(
        novarocks_physical_plan::PhysicalCallSite,
        novarocks_expression_contract::PhysicalCallRequest,
    )>,
}
/// The same completed unit and child-first summaries, without a Node/Fragment
/// association. The original producer retains the source/environment owner.
pub(super) struct AuthoredScalarUnit {
    pub(super) unit: Arc<novarocks_expression_contract::BoundExprSet>,
    pub(super) summaries: BTreeMap<
        novarocks_type_contract::ExpressionUseId,
        novarocks_functions::ScopedExpressionEffects,
    >,
}

/// The original static source gate, before request transfer or fresh effects.
/// The presence projection is created once by the caller; the source loan
/// retains the original immutable catalogue rather than replacing it.
pub(super) fn admit_scalar_unit_sources_observed<'source>(
    source_loan: &SqlUnitSourceLoan<'source>,
    draft: &'source novarocks_expression_contract::BoundExprSetBuilder,
    presence: &dyn crate::compiler::SqlFunctionCatalog,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ContractLoweringError> {
    for source in draft.definitions() {
        work.step()?;
        if !matches!(source.kind, ContractExprKind::FunctionCall { .. }) {
            continue;
        }
        let entry = source_loan.checked_expression_source_observed(source, work)?;
        let canonical =
            entry
                .canonical_operational()
                .ok_or(PackageSemanticsError::InvalidSource(
                    "original scalar source has no canonical request",
                ))?;
        if !canonical.belongs_to(entry.captured()) {
            return Err(PackageSemanticsError::InvalidSource(
                "scalar canonical admission has a foreign original capture",
            )
            .into());
        }
        super::super::source_support::admit_original_binding_observed(
            presence,
            &canonical.binding,
            entry.captured().binding(),
            source_loan.parameters(),
            PureCallLifecycle::Scalar,
            control,
        )?;
    }
    Ok(())
}

/// Project referenced input values in the caller's declared layout order.
pub(super) fn standalone_input_ports_observed(
    draft: &novarocks_expression_contract::BoundExprSetBuilder,
    inputs: &[(ColumnId, ValueId, ValueType)],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<novarocks_expression_contract::ExprPort>, ContractLoweringError> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }
    let mut used = BTreeSet::new();
    for definition in draft.definitions() {
        work.step()?;
        if let ContractExprKind::Value(value) = definition.kind {
            used.insert(value);
        }
    }
    if used.is_empty() {
        return Ok(Vec::new());
    }
    let mut values = Vec::new();
    work.flush()?;
    values
        .try_reserve_exact(used.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    for (_, value, ty) in inputs {
        work.step()?;
        if used.contains(value) {
            values.push((*value, ty.clone()));
        }
    }
    Ok(vec![novarocks_expression_contract::ExprPort {
        values: values.into_boxed_slice(),
    }])
}

/// Consume one actual draft through the expression-layer call author.
/// SQL still pairs each call with its source journal and statement policy.
/// Ports are authored at their original point after definition controls; this
/// closure supplies declarations only, never capacity or source credentials.
pub(super) fn finish_scalar_unit_observed(
    site: ScalarSourceSite,
    mut draft: novarocks_expression_contract::BoundExprSetBuilder,
    ports: impl FnOnce() -> Result<Vec<novarocks_expression_contract::ExprPort>, ContractLoweringError>,
    constants: &novarocks_physical_plan::ConstantPools,
    parameters: &SemanticParameters,
    journal: &SqlLogicalSourceJournal,
    functions: &Arc<dyn crate::compiler::SqlFunctionCatalog>,
    sealed_functions: &novarocks_functions::PureEngineFunctionCatalog,
    policy: novarocks_constant_contract::ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredScalarUnit, ContractLoweringError> {
    {
        let source = SqlUnitSourceLoan {
            site: site.clone(),
            definitions: super::super::unit_source::UnitDefinitions::from_builder(
                site.owner(),
                &draft,
            ),
            constants,
            parameters,
            journal,
            functions,
        };
        for ordinal in 0..draft.len() {
            let id = novarocks_expression_contract::ExprId::new(
                u32::try_from(ordinal).map_err(|_| CompileControlError::ResourceExhausted)?,
            );
            let Some(definition) = draft.definition(id) else {
                return Err(PackageSemanticsError::InvalidSource(
                    "scalar draft is missing a definition",
                )
                .into());
            };
            work.step()?;
            if !matches!(definition.kind, ContractExprKind::FunctionCall { .. }) {
                continue;
            }
            source.checked_expression_source_observed(definition, work)?;
        }
    }
    let empty_decimal = BTreeMap::new();
    let control_inputs = novarocks_expression::CallAuthorInputs {
        environment: novarocks_expression::EvaluationEnvironment {
            constants,
            parameters,
        },
        constant_policy: policy,
        functions: sealed_functions,
        decimal_overflow: &empty_decimal,
    };
    let controls = novarocks_expression::author_call_controls(&draft, &control_inputs, work);
    for (definition, shape) in controls.map_err(package_author_error)? {
        draft.definition_control(definition, shape)?;
        work.step()?;
    }
    let ports = ports()?;
    let sealed = draft.seal(ports, work)?;
    let source = SqlUnitSourceLoan {
        site: site.clone(),
        definitions: super::super::unit_source::UnitDefinitions::from_sealed(site.owner(), &sealed),
        constants,
        parameters,
        journal,
        functions,
    };
    let mut decimal = BTreeMap::new();
    for (&id, invocation) in sealed.uses() {
        let definition = sealed.definition(invocation.definition).ok_or(
            PackageSemanticsError::InvalidSource("sealed unit invocation has no actual definition"),
        )?;
        work.step()?;
        if !matches!(definition.kind, ContractExprKind::FunctionCall { .. }) {
            continue;
        }
        let entry = source.checked_expression_source_observed(definition, work)?;
        if entry.captured().constant_policy() != policy {
            return Err(match &site {
                ScalarSourceSite::Node { node, .. } => {
                    PackageSemanticsError::ExpressionPolicyMismatch {
                        node: *node,
                        invocation: id,
                    }
                }
                ScalarSourceSite::Standalone(_) => {
                    PackageSemanticsError::StandaloneExpressionPolicyMismatch { invocation: id }
                }
            }
            .into());
        }
        work.flush()?;
        decimal.insert(
            invocation.definition,
            entry.captured().binding().decimal_overflow_policy(),
        );
        work.step()?;
    }
    let effect_inputs = novarocks_expression::CallAuthorInputs {
        environment: novarocks_expression::EvaluationEnvironment {
            constants,
            parameters,
        },
        constant_policy: policy,
        functions: sealed_functions,
        decimal_overflow: &decimal,
    };
    let authored = novarocks_expression::author_unit_calls(&sealed, &effect_inputs, work);
    let authored = authored.map_err(package_author_error)?;
    let unit = Arc::new(sealed.finish(authored.claims, work)?);
    Ok(AuthoredScalarUnit {
        unit,
        summaries: authored.summaries,
    })
}

pub(in crate::planner::distributed::build) fn package_author_error(
    error: novarocks_expression::CallAuthorError,
) -> PackageSemanticsError {
    use novarocks_expression::{CallAuthorError, CapabilityRefusal, InvariantRefusal};
    match error {
        CallAuthorError::Control(cause) => PackageSemanticsError::Control(cause),
        CallAuthorError::Capability(CapabilityRefusal::ConstantLimit(cause)) => {
            PackageSemanticsError::ConstantSource(cause)
        }
        CallAuthorError::Capability(CapabilityRefusal::Occurrence(detail)) => {
            PackageSemanticsError::Occurrence(detail)
        }
        CallAuthorError::Capability(CapabilityRefusal::Effects(detail)) => {
            PackageSemanticsError::Effects(detail)
        }
        CallAuthorError::Capability(CapabilityRefusal::Unsupported(detail)) => {
            PackageSemanticsError::UnsupportedCapability(detail)
        }
        CallAuthorError::Capability(CapabilityRefusal::Primitive { source, cast }) => {
            PackageSemanticsError::PrimitiveSource { source, cast }
        }
        CallAuthorError::Invariant(InvariantRefusal::ConstantPolicy) => {
            PackageSemanticsError::Effects(
                "InvalidSource(\"SQL expression occurrence changes its original captured policy\")"
                    .into(),
            )
        }
        CallAuthorError::Invariant(InvariantRefusal::MissingOverflow(definition)) => {
            PackageSemanticsError::Effects(format!(
                "call definition {definition:?} has no decimal overflow policy"
            ))
        }
        CallAuthorError::Invariant(InvariantRefusal::Source(detail)) => {
            PackageSemanticsError::InvalidSource(detail)
        }
    }
}

impl ContractLoweringVisitor<'_> {
    /// Admit all stored sources before fresh preparation, using the same
    /// original binding/environment author as completed-source inspection.
    pub(super) fn admit_original_definitions(
        &mut self,
        parameters: &SemanticParameters,
    ) -> Result<(), ContractLoweringError> {
        let functions = self.functions.snapshot_for_scalar_presence();
        let mut source_groups = BTreeMap::new();
        for (&(fragment, node), entry) in &self.call_sources.table_entries {
            insert_relation_binding(
                &mut source_groups,
                fragment,
                novarocks_physical_plan::PhysicalCallSite::Table { node },
                &entry.canonical_operational.binding,
                entry.captured.binding(),
                PureCallLifecycle::Table,
                &mut self.work,
            )?;
        }
        for (&(fragment, site), entry) in &self.call_sources.window_entries {
            let kind = entry.captured.binding().resolved().kind;
            let lifecycle = if kind == novarocks_type_contract::FunctionKind::Aggregate {
                PureCallLifecycle::AggregateWindow
            } else {
                PureCallLifecycle::Window
            };
            insert_relation_binding(
                &mut source_groups,
                fragment,
                site,
                &entry.canonical_operational.binding,
                entry.captured.binding(),
                lifecycle,
                &mut self.work,
            )?;
        }
        for (&(fragment, site), entry) in &self.call_sources.entries {
            let captured = entry
                .logical
                .captured()
                .ok_or(PackageSemanticsError::InvalidSource(
                    "relation admission lacks its original capture",
                ))?;
            let binding = entry
                .canonical
                .as_ref()
                .map(|canonical| &canonical.binding)
                .unwrap_or_else(|| captured.binding());
            insert_relation_binding(
                &mut source_groups,
                fragment,
                site,
                binding,
                captured.binding(),
                PureCallLifecycle::Aggregate,
                &mut self.work,
            )?;
        }
        for (&fragment_id, builder) in &self.fragments {
            for (&node_id, _) in builder.construction_nodes() {
                let Some(unit) = builder.construction_unit(node_id) else {
                    continue;
                };
                let loan = SqlUnitSourceLoan {
                    site: ScalarSourceSite::Node {
                        fragment: fragment_id,
                        node: node_id,
                    },
                    definitions: super::super::unit_source::UnitDefinitions::from_draft(
                        builder, node_id,
                    ),
                    constants: self.constant_emission.pools(),
                    parameters,
                    journal: &self.call_sources,
                    functions: &self.functions,
                };
                admit_scalar_unit_sources_observed(
                    &loan,
                    unit,
                    functions.as_ref(),
                    self.control,
                    &mut self.work,
                )?;
            }
            if let Some(sources) = source_groups.remove(&fragment_id) {
                for (_, (binding, original, lifecycle)) in sources {
                    self.work.flush()?;
                    super::super::source_support::admit_original_binding_observed(
                        functions.as_ref(),
                        binding,
                        original,
                        parameters,
                        lifecycle,
                        self.control,
                    )?;
                }
            }
        }
        if let Some((&fragment, _)) = source_groups.first_key_value() {
            return Err(ContractLoweringError::UnknownFragmentCompletion { fragment });
        }
        Ok(())
    }

    pub(super) fn seal_node_units(
        &mut self,
        builder: &mut FragmentBuilder,
        parameters: &SemanticParameters,
        requests: Vec<(
            neutral_requests::RequestTarget,
            novarocks_expression_contract::PhysicalCallRequest,
        )>,
    ) -> Result<FragmentUnitAuthorship, ContractLoweringError> {
        let fragment = builder.id();
        let mut relations = Vec::new();
        self.work.flush()?;
        relations
            .try_reserve_exact(requests.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for (target, request) in requests {
            self.work.step()?;
            match target {
                neutral_requests::RequestTarget::Unit { node, definition } => {
                    let actual = builder
                        .construction_unit(node)
                        .and_then(|unit| unit.definition(definition));
                    if !actual.is_some_and(|source| {
                        matches!(source.kind, ContractExprKind::FunctionCall { .. })
                    }) {
                        return Err(PackageSemanticsError::InvalidSource(
                            "original scalar request has no actual node definition",
                        )
                        .into());
                    }
                    builder.unit(node)?.request(definition, request)?;
                }
                neutral_requests::RequestTarget::Relation(site) => relations.push((site, request)),
            }
        }
        let mut nodes = Vec::new();
        for (&node, _) in builder.construction_nodes() {
            if builder.construction_unit(node).is_some() {
                self.work.flush()?;
                nodes
                    .try_reserve(1)
                    .map_err(|_| CompileControlError::ResourceExhausted)?;
                nodes.push(node);
                self.work.step()?;
            }
        }
        let mut effects = BTreeMap::new();
        for node in nodes {
            let draft = builder.take_unit_builder(node)?;
            let authored = finish_scalar_unit_observed(
                ScalarSourceSite::Node { fragment, node },
                draft,
                || {
                    builder
                        .node_ports(node)
                        .map_err(ContractLoweringError::from)
                },
                self.constant_emission.pools(),
                parameters,
                &self.call_sources,
                &self.functions,
                self.sealed_functions.as_ref(),
                self.constant_policy,
                &mut self.work,
            )?;
            let unit = authored.unit;
            builder.install_unit(node, Arc::clone(&unit))?;
            self.work.flush()?;
            effects.insert(
                node,
                NodeExpressionEffects {
                    unit,
                    summaries: authored.summaries,
                },
            );
            self.work.step()?;
        }
        Ok(FragmentUnitAuthorship { effects, relations })
    }
}

type OriginalRelationBindings<'a> = BTreeMap<
    FragmentId,
    BTreeMap<
        novarocks_physical_plan::PhysicalCallSite,
        (
            &'a SqlFunctionBinding,
            &'a SqlFunctionBinding,
            PureCallLifecycle,
        ),
    >,
>;
fn insert_relation_binding<'a>(
    groups: &mut OriginalRelationBindings<'a>,
    fragment: FragmentId,
    site: novarocks_physical_plan::PhysicalCallSite,
    binding: &'a SqlFunctionBinding,
    original: &'a SqlFunctionBinding,
    lifecycle: PureCallLifecycle,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ContractLoweringError> {
    work.flush()?;
    let repeated = groups
        .entry(fragment)
        .or_default()
        .insert(site, (binding, original, lifecycle))
        .is_some();
    work.step()?;
    if repeated {
        return Err(PackageSemanticsError::InvalidSource(
            "original relation source repeats an actual call site",
        )
        .into());
    }
    Ok(())
}

impl ContractLoweringVisitor<'_> {
    pub(super) fn author_relation_tables(
        &mut self,
        fragments: &BTreeMap<FragmentId, Fragment>,
        node_effects: &BTreeMap<FragmentId, BTreeMap<NodeId, NodeExpressionEffects>>,
        parameters: &SemanticParameters,
    ) -> Result<
        BTreeMap<FragmentId, novarocks_physical_plan::FrozenFragmentCalls>,
        ContractLoweringError,
    > {
        use super::super::physical_fragment_effects::{
            PhysicalFragmentEffectsInput, PhysicalRelationalCallSourceScope, aggregate_source,
            author_sql_fragment_effects_observed, window_source, writer_source,
        };
        use novarocks_physical_plan::PhysicalCallSite;
        let loan = SqlRelationSourceLoan {
            fragments,
            edges: &self.edges,
            constants: self.constant_emission.pools(),
            parameters,
            journal: &self.call_sources,
            functions: &self.functions,
        };
        let facts = super::super::lowered_draft::SqlRelationalSourceFacts::from_original(&loan);
        let mut output = BTreeMap::new();
        for (&fragment_id, fragment) in fragments {
            let occurrences =
                super::super::expression_occurrences::author_physical_occurrences_observed(
                    fragment,
                    self.functions.as_ref(),
                    self.control,
                )
                .map_err(PackageSemanticsError::from)?;
            let mut policies = BTreeMap::new();
            let mut environments = BTreeMap::new();
            for &(site, _) in &occurrences.relational_contexts {
                let node = fragment.nodes().get(&site.node()).ok_or(
                    PackageSemanticsError::InvalidSource("relation site has no actual node"),
                )?;
                let (binding, policy) = match site {
                    PhysicalCallSite::Table { .. } => {
                        let entry =
                            facts.checked_table_source_observed(fragment, node, &mut self.work)?;
                        (
                            entry.captured().binding(),
                            entry.captured().constant_policy(),
                        )
                    }
                    PhysicalCallSite::Window { .. } => {
                        let source =
                            window_source(node, site).map_err(PackageSemanticsError::from)?;
                        let entry = facts.checked_window_source_observed(
                            fragment,
                            node,
                            site,
                            source,
                            &mut self.work,
                        )?;
                        (
                            entry.captured().binding(),
                            entry.captured().constant_policy(),
                        )
                    }
                    PhysicalCallSite::Aggregate { .. } | PhysicalCallSite::TopNState { .. } => {
                        let source =
                            aggregate_source(node, site).map_err(PackageSemanticsError::from)?;
                        let entry = facts.checked_aggregate_source_observed(
                            fragment,
                            node,
                            site,
                            source,
                            &mut self.work,
                        )?;
                        (
                            entry.captured().binding(),
                            entry.captured().constant_policy(),
                        )
                    }
                    PhysicalCallSite::WriterPartial { .. }
                    | PhysicalCallSite::WriterFinal { .. } => {
                        let source =
                            writer_source(node, site).map_err(PackageSemanticsError::from)?;
                        let entry = facts.checked_writer_aggregate_source_observed(
                            fragment,
                            node,
                            site,
                            source,
                            &mut self.work,
                        )?;
                        (
                            entry.captured().binding(),
                            entry.captured().constant_policy(),
                        )
                    }
                };
                if policy != self.constant_policy {
                    return Err(PackageSemanticsError::PolicyMismatch(site).into());
                }
                self.work.flush()?;
                policies.insert(site, binding.decimal_overflow_policy());
                self.work.step()?;
                if let Some(source) = binding.group_concat_source() {
                    let references = source.environment();
                    for reference in references {
                        parameters.require(reference).map_err(|_| {
                            PackageSemanticsError::InvalidSource(
                                "group_concat admitted parameter is absent",
                            )
                        })?;
                        self.work.step()?;
                    }
                    self.work.flush()?;
                    environments.insert(site, references);
                    self.work.step()?;
                }
            }
            let mut scopes = BTreeMap::new();
            for &(site, context) in &occurrences.relational_contexts {
                let node = fragment.nodes().get(&site.node()).ok_or(
                    PackageSemanticsError::InvalidSource("relation scope has no actual node"),
                )?;
                let decimal =
                    policies
                        .get(&site)
                        .copied()
                        .ok_or(PackageSemanticsError::InvalidSource(
                            "relation scope has no original policy",
                        ))?;
                self.work.flush()?;
                scopes.insert(
                    site,
                    PhysicalRelationalCallSourceScope {
                        source: node,
                        decimal_overflow_policy: decimal,
                        environment: environments
                            .get(&site)
                            .map_or(&[][..], |references| references.as_slice()),
                        proof_scope: novarocks_type_contract::CallProofScope::Relation(
                            context.identity,
                        ),
                    },
                );
                self.work.step()?;
            }
            let effects =
                node_effects
                    .get(&fragment_id)
                    .ok_or(PackageSemanticsError::InvalidSource(
                        "fragment has no original unit-authorship result",
                    ))?;
            let authored = author_sql_fragment_effects_observed(
                facts,
                PhysicalFragmentEffectsInput {
                    fragment,
                    occurrences: &occurrences,
                    constants: self.constant_emission.pools(),
                    parameters,
                    literal_policy: self.constant_policy,
                    node_effects: effects,
                    relational_scopes: &scopes,
                },
                self.control,
            )
            .map_err(PackageSemanticsError::from)?;
            self.work.flush()?;
            output.insert(fragment_id, authored.calls);
            self.work.step()?;
        }
        Ok(output)
    }
}
