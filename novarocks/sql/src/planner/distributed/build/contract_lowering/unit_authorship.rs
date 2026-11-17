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
                for source in unit.definitions() {
                    self.work.step()?;
                    if !matches!(source.kind, ContractExprKind::FunctionCall { .. }) {
                        continue;
                    }
                    let entry = loan.checked_expression_source_observed(source, &mut self.work)?;
                    let canonical = entry.canonical_operational().ok_or(
                        PackageSemanticsError::InvalidSource(
                            "original scalar source has no canonical request",
                        ),
                    )?;
                    if !canonical.belongs_to(entry.captured()) {
                        return Err(PackageSemanticsError::InvalidSource(
                            "scalar canonical admission has a foreign original capture",
                        )
                        .into());
                    }
                    super::super::source_support::admit_original_binding_observed(
                        functions.as_ref(),
                        &canonical.binding,
                        entry.captured().binding(),
                        parameters,
                        PureCallLifecycle::Scalar,
                        self.control,
                    )?;
                }
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
            let controls = {
                let source = SqlUnitSourceLoan {
                    site: ScalarSourceSite::Node { fragment, node },
                    definitions: super::super::unit_source::UnitDefinitions::from_draft(
                        builder, node,
                    ),
                    constants: self.constant_emission.pools(),
                    parameters,
                    journal: &self.call_sources,
                    functions: &self.functions,
                };
                super::super::expression_occurrences::author_original_unit_controls_observed(
                    &source,
                    &mut self.work,
                )
                .map_err(PackageSemanticsError::from)?
            };
            for (definition, control) in controls {
                builder
                    .unit(node)?
                    .definition_control(definition, control)?;
                self.work.step()?;
            }
            let ports = builder.node_ports(node)?;
            let sealed = builder
                .take_unit_builder(node)?
                .seal(ports, &mut self.work)?;
            let source = SqlUnitSourceLoan {
                site: ScalarSourceSite::Node { fragment, node },
                definitions: super::super::unit_source::UnitDefinitions::from_sealed(
                    ScalarUnitOwner::Node(node),
                    &sealed,
                ),
                constants: self.constant_emission.pools(),
                parameters,
                journal: &self.call_sources,
                functions: &self.functions,
            };
            let mut scopes = BTreeMap::new();
            for (&id, invocation) in sealed.uses() {
                let definition = sealed.definition(invocation.definition).ok_or(
                    PackageSemanticsError::InvalidSource(
                        "sealed unit invocation has no actual definition",
                    ),
                )?;
                self.work.step()?;
                if !matches!(definition.kind, ContractExprKind::FunctionCall { .. }) {
                    continue;
                }
                let entry =
                    source.checked_expression_source_observed(definition, &mut self.work)?;
                if entry.captured().constant_policy() != self.constant_policy {
                    return Err(PackageSemanticsError::ExpressionPolicyMismatch {
                        node,
                        invocation: id,
                    }
                    .into());
                }
                self.work.flush()?;
                scopes.insert(
                    id,
                    super::super::physical_expression_effects::PhysicalCallSourceScope {
                        source: definition,
                        decimal_overflow_policy: entry
                            .captured()
                            .binding()
                            .decimal_overflow_policy(),
                        environment: &[],
                        proof_scope: novarocks_type_contract::CallProofScope::Region(
                            invocation.context.region,
                        ),
                    },
                );
                self.work.step()?;
            }
            let authored =
                super::super::physical_expression_effects::author_sql_expression_effects_observed(
                    &source,
                    super::super::physical_expression_effects::PhysicalExpressionEffectsInput {
                        owner: ScalarUnitOwner::Node(node),
                        unit: &sealed,
                        constants: self.constant_emission.pools(),
                        parameters,
                        literal_policy: self.constant_policy,
                        call_scopes: &scopes,
                    },
                    self.control,
                )
                .map_err(PackageSemanticsError::from)?;
            let unit = Arc::new(sealed.finish(authored.calls, &mut self.work)?);
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
