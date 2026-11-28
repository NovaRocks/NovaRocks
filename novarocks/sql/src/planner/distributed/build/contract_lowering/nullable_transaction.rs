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

//! One consumed lowering visitor owns this monotone construction transaction.
//! Failure drops every partially revised builder and journal. No intermediate
//! graph, source owner, output contract or package is published.
use super::*;
use novarocks_physical_plan::{NullabilityRebindError, PhysicalCallBinding, PhysicalCallSite};

impl From<NullabilityRebindError> for ContractLoweringError {
    fn from(error: NullabilityRebindError) -> Self {
        match error {
            NullabilityRebindError::Control(cause) => Self::Control(cause),
            error => Self::InvalidFunctionBinding {
                detail: format!("unpublished nullable transaction: {error:?}"),
            },
        }
    }
}
fn invalid(detail: &'static str) -> ContractLoweringError {
    ContractLoweringError::InvalidAggregate { detail }
}

impl ContractLoweringVisitor<'_> {
    fn construction_expression(
        &self,
        fragment: FragmentId,
        node: NodeId,
        expression: ExprId,
    ) -> Result<&novarocks_expression_contract::ExprNode, ContractLoweringError> {
        self.fragments
            .get(&fragment)
            .and_then(|builder| builder.construction_unit(node))
            .and_then(|unit| unit.definition(expression))
            .ok_or_else(|| invalid("nullable transaction expression is absent"))
    }
    fn construction_root_definition(
        &self,
        fragment: FragmentId,
        node: NodeId,
        root: RootOrdinal,
    ) -> Result<ExprId, ContractLoweringError> {
        self.fragments
            .get(&fragment)
            .and_then(|builder| builder.construction_unit(node))
            .and_then(|unit| unit.roots().get(root.get() as usize))
            .map(|root| root.definition)
            .ok_or_else(|| invalid("nullable transaction root is absent from its node unit"))
    }
    fn construction_value(
        &self,
        fragment: FragmentId,
        value: ValueId,
    ) -> Result<&novarocks_physical_plan::ValueDef, ContractLoweringError> {
        self.fragments
            .get(&fragment)
            .and_then(|builder| builder.value(value))
            .ok_or_else(|| invalid("nullable transaction value is absent"))
    }
    fn construction_node(
        &self,
        fragment: FragmentId,
        node: NodeId,
    ) -> Result<&novarocks_physical_plan::PhysicalNode, ContractLoweringError> {
        self.fragments
            .get(&fragment)
            .and_then(|builder| builder.construction_node(node))
            .ok_or_else(|| invalid("nullable transaction node is absent"))
    }
    fn current_constraint(
        &mut self,
        binding: &SqlFunctionBinding,
        arguments: &[novarocks_functions::FunctionArgument],
    ) -> Result<Option<novarocks_functions::FunctionValueType>, ContractLoweringError> {
        super::scalar_nullability::current_constraint(binding, arguments, &mut self.work)
    }

    fn rebind_window_entry(
        &mut self,
        fragment: FragmentId,
        site: PhysicalCallSite,
    ) -> Result<bool, ContractLoweringError> {
        let PhysicalCallSite::Window { node, call } = site else {
            return Err(invalid("Window rebind names another lifecycle"));
        };
        self.work.flush()?;
        let mut entry = self
            .call_sources
            .window_entries
            .remove(&(fragment, site))
            .ok_or_else(|| invalid("Window rebind lacks its original source entry"))?;
        let NodeKind::Window(spec) = &self.construction_node(fragment, node)?.kind else {
            return Err(invalid("Window rebind node has another kind"));
        };
        let old = spec
            .calls
            .get(call as usize)
            .ok_or_else(|| invalid("Window rebind call ordinal is absent"))?
            .clone();
        self.work.flush()?;
        super::super::lowered_draft::validate_window_source_entry_observed(
            &entry,
            super::super::unit_source::UnitDefinitions::from_draft(
                &self.fragments[&fragment],
                node,
            ),
            &old,
            &mut self.work,
        )?;
        self.work.flush()?;
        let projected = project_emitted_call_arguments_observed(
            EmittedOperationalCall {
                expressions: super::super::unit_source::UnitDefinitions::from_draft(
                    &self.fragments[&fragment],
                    node,
                ),
                pools: self.constant_emission.pools(),
                owner: ScalarUnitOwner::Node(node),
                lambda_scope: None,
                kind: Some(SqlExpressionCallKind::Window),
                captured: CapturedOperationalSource::call(&entry.captured),
                derived: None,
                arguments: &entry.arguments,
                channels: &entry.channels,
            },
            self.control,
        )
        .map_err(ContractLoweringError::from);
        let arguments = self.completed_specialization_result(projected)?;
        let binding = entry.captured.binding();
        let constraint = self.current_constraint(binding, &arguments)?;
        let logical_count = entry.captured.request().logical_argument_count;
        let selected = self.select_canonical_request_with_constraint(
            binding,
            logical_count,
            &arguments,
            constraint.as_ref(),
        )?;
        let novarocks_functions::FunctionResultType::Scalar(result) = &selected.result_type else {
            return Err(invalid("Window rebind acquired a relation result"));
        };
        let revised = bound_function_from_selection(binding.resolved(), &selected, result);
        let aggregate = if let Some(original) = old.aggregate_binding.as_deref() {
            Some(Box::new(lower_captured_aggregate_binding(
                binding,
                logical_count,
                old.order_by.len(),
                &selected,
                original.phase,
                &mut self.work,
            )?))
        } else {
            None
        };
        self.work.flush()?;
        let changed = old.binding != revised || old.aggregate_binding != aggregate;
        self.work.step()?;
        self.work.flush()?;
        self.fragments
            .get_mut(&fragment)
            .expect("checked actual Window construction")
            .nullability_editor()
            .window_signature(
                site,
                &old.binding,
                old.aggregate_binding.as_deref(),
                revised,
                aggregate,
                &mut self.work,
            )?;
        self.work.flush()?;
        entry.canonical_operational = Arc::new(CanonicalCallOperationalRequest {
            binding: binding.clone(),
            arguments,
            logical_count,
            selected,
            result_constraint: constraint,
        });
        self.work.step()?;
        self.call_sources
            .window_entries
            .insert((fragment, site), entry);
        self.work.step()?;
        Ok(changed)
    }
    fn rebind_table_entry(
        &mut self,
        fragment: FragmentId,
        node: NodeId,
    ) -> Result<bool, ContractLoweringError> {
        let mut entry = self
            .call_sources
            .table_entries
            .remove(&(fragment, node))
            .ok_or_else(|| invalid("table rebind lacks its original source entry"))?;
        let old = self.construction_node(fragment, node)?.clone();
        validate_table_source_entry_observed(
            &entry,
            super::super::unit_source::UnitDefinitions::from_draft(
                &self.fragments[&fragment],
                node,
            ),
            &old,
            &mut self.work,
        )?;
        self.work.flush()?;
        let projected = project_emitted_call_arguments_observed(
            EmittedOperationalCall {
                expressions: super::super::unit_source::UnitDefinitions::from_draft(
                    &self.fragments[&fragment],
                    node,
                ),
                pools: self.constant_emission.pools(),
                owner: ScalarUnitOwner::Node(node),
                lambda_scope: None,
                kind: None,
                captured: CapturedOperationalSource::call(&entry.captured),
                derived: None,
                arguments: &entry.arguments,
                channels: &entry.channels,
            },
            self.control,
        )
        .map_err(ContractLoweringError::from);
        let arguments = self.completed_specialization_result(projected)?;
        let binding = entry.captured.binding();
        let logical_count = entry.captured.request().logical_argument_count;
        let constraint = self.current_constraint(binding, &arguments)?;
        let selected = self.select_canonical_request_with_constraint(
            binding,
            logical_count,
            &arguments,
            constraint.as_ref(),
        )?;
        let novarocks_functions::FunctionResultType::Relation(results) = &selected.result_type
        else {
            return Err(invalid("table rebind acquired a scalar result"));
        };
        let NodeKind::TableFunction { function, .. } = &old.kind else {
            return Err(invalid("table rebind changed actual node kind"));
        };
        let mut revised = function.clone();
        revised.argument_types = selected
            .argument_types
            .clone_observed(|| self.work.step())?;
        revised.result_types = results.clone_observed(|| self.work.step())?;
        revised.overload = selected.overload.clone();
        self.work.flush()?;
        let changed = *function != revised;
        self.work.step()?;
        self.work.flush()?;
        self.fragments
            .get_mut(&fragment)
            .expect("checked above")
            .nullability_editor()
            .table_signature(node, function, revised, &mut self.work)?;
        self.work.flush()?;
        entry.canonical_operational = Arc::new(CanonicalCallOperationalRequest {
            binding: binding.clone(),
            arguments,
            logical_count,
            selected,
            result_constraint: constraint,
        });
        self.work.step()?;
        self.call_sources
            .table_entries
            .insert((fragment, node), entry);
        self.work.step()?;
        Ok(changed)
    }
    /// Every pass reads the same owned graph. Root admission is monotone and
    /// finite: an expression/value can change false to true at most once.
    pub(super) fn rebind_unpublished_nullability(&mut self) -> Result<(), ContractLoweringError> {
        let mut dirty_expressions = BTreeSet::new();
        let mut dirty_values = BTreeSet::new();
        loop {
            let mut changed = false;
            let mut emissions = Vec::new();
            for (&fragment, order) in &self.definition_order {
                for &event in &order.events {
                    self.work.flush()?;
                    emissions
                        .try_reserve(1)
                        .map_err(|_| CompileControlError::ResourceExhausted)?;
                    emissions.push((fragment, event));
                    self.work.step()?;
                }
            }
            for (fragment, event) in emissions {
                let (node, id) = match event {
                    NullableEmission::Definition { node, definition } => (node, definition),
                    NullableEmission::Window { site } => {
                        let node = site.node();
                        let PhysicalCallSite::Window { call, .. } = site else {
                            return Err(invalid("nullable Window emission has another lifecycle"));
                        };
                        let actual_node = self
                            .fragments
                            .get(&fragment)
                            .and_then(|builder| builder.construction_node(node))
                            .ok_or_else(|| invalid("nullable Window emission node is absent"))?;
                        let NodeKind::Window(spec) = &actual_node.kind else {
                            return Err(invalid("nullable Window emission node has another kind"));
                        };
                        let source = spec
                            .calls
                            .get(call as usize)
                            .ok_or_else(|| invalid("nullable Window emission call is absent"))?;
                        let definitions = super::super::unit_source::UnitDefinitions::from_draft(
                            &self.fragments[&fragment],
                            node,
                        );
                        let mut dependent = false;
                        for root in source
                            .arguments
                            .iter()
                            .copied()
                            .chain(source.order_by.iter().map(|key| key.expr))
                        {
                            let definition = definitions
                                .root(root)
                                .ok_or_else(|| invalid("nullable Window parameter root is absent"))?
                                .definition;
                            dependent |= dirty_expressions.contains(&(fragment, node, definition));
                            self.work.step()?;
                        }
                        if dependent {
                            changed |= self.rebind_window_entry(fragment, site)?;
                        }
                        continue;
                    }
                };
                let contract_changed = self.scalar_emission_at(fragment, node).rebind_definition(
                    id,
                    |definition| dirty_expressions.contains(&(fragment, node, definition)),
                    |value| dirty_values.contains(&(fragment, value)),
                )?;
                if contract_changed {
                    dirty_expressions.insert((fragment, node, id));
                    self.work.step()?;
                    changed = true;
                }
            }
            self.work.flush()?;
            let mut tables = Vec::new();
            tables
                .try_reserve_exact(self.call_sources.table_entries.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            for &site in self.call_sources.table_entries.keys() {
                self.work.step()?;
                tables.push(site);
            }
            for (fragment, node) in tables {
                let old = self
                    .fragments
                    .get(&fragment)
                    .and_then(|builder| builder.construction_node(node))
                    .ok_or_else(|| invalid("table construction node is absent"))?;
                let NodeKind::TableFunction { arguments, .. } = &old.kind else {
                    return Err(invalid("table source has a different actual node kind"));
                };
                let mut dependent = false;
                for &id in arguments.iter() {
                    self.work.step()?;
                    let definition = self.construction_root_definition(fragment, node, id)?;
                    dependent |= dirty_expressions.contains(&(fragment, node, definition));
                }
                if dependent {
                    changed |= self.rebind_table_entry(fragment, node)?;
                }
            }
            changed |= self.rebind_relational_aggregates(&dirty_expressions, &dirty_values)?;
            let mut values = Vec::new();
            for (&fragment, builder) in &self.fragments {
                for (&id, _) in builder.construction_values() {
                    self.work.flush()?;
                    values
                        .try_reserve(1)
                        .map_err(|_| CompileControlError::ResourceExhausted)?;
                    values.push((fragment, id));
                    self.work.step()?;
                }
            }
            for (fragment, id) in values {
                self.work.flush()?;
                let old = self.construction_value(fragment, id)?.clone();
                self.work.flush()?;
                self.work.step()?;
                let revised = self.derived_construction_value_type(
                    fragment,
                    &old,
                    &dirty_expressions,
                    &dirty_values,
                )?;
                if old.ty.nullable() != revised.nullable() {
                    self.fragments
                        .get_mut(&fragment)
                        .expect("checked above")
                        .nullability_editor()
                        .value_type(id, &old.ty, revised, &mut self.work)?;
                    dirty_values.insert((fragment, id));
                    self.work.step()?;
                    changed = true;
                }
            }
            self.rebind_writer_relation_schemas()?;
            // CTE source metadata is read by later preparation from these actual
            // values; it is not an independently inferred declaration.
            for producer in self.cte_producers.values_mut() {
                for (value, ty) in producer.outputs.values_mut() {
                    self.work.step()?;
                    *ty = self.fragments[&producer.fragment]
                        .value(*value)
                        .ok_or_else(|| invalid("CTE producer value is absent"))?
                        .ty
                        .clone();
                }
            }
            if !changed {
                break;
            }
        }
        self.work.flush()?;
        Ok(())
    }
    pub(super) fn computed_result_port(
        &mut self,
        original: &ResultPort,
    ) -> Result<ResultPort, ContractLoweringError> {
        let node = self.construction_node(original.fragment, original.output.node)?;
        if node.output.columns != original.output.columns {
            return Err(invalid("computed result changed ordered output identity"));
        }
        self.work.flush()?;
        let mut fields = Vec::new();
        fields
            .try_reserve_exact(original.fields.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for field in original.fields.iter() {
            self.work.step()?;
            let ty = self
                .construction_value(original.fragment, field.value)?
                .ty
                .clone();
            self.work.flush()?;
            fields.push(ResultField {
                domain: field.domain,
                name: field.name.clone(),
                alias: field.alias.clone(),
                value: field.value,
                ty,
            });
            self.work.step()?;
        }
        let scalar_schema =
            match (&original.scalar_schema, fields.as_slice()) {
                (Some(schema), [result]) => {
                    self.work.flush()?;
                    let mut field = schema.field().clone();
                    field.nullable = result.ty.nullable();
                    if !result.domain.matches_scalar(&field.value_type)
                    || !novarocks_type_contract::result_scalar_type::scalar_field_matches_storage(
                        &field,
                        result.ty.data_type(),
                        result.ty.nullable(),
                    )
                {
                    return Err(invalid("computed result differs from its captured scalar shape"));
                }
                    self.work.step()?;
                    Some(
                        novarocks_result_contract::ScalarSchema::try_new(field)
                            .map_err(|_| invalid("computed scalar schema is invalid"))?,
                    )
                }
                (Some(_), _) => {
                    return Err(invalid(
                        "computed scalar result changed its occurrence count",
                    ));
                }
                (None, _) => None,
            };
        Ok(ResultPort {
            scalar_schema,
            fragment: original.fragment,
            output: original.output.clone(),
            fields: fields.into_boxed_slice(),
        })
    }
}

impl ContractLoweringVisitor<'_> {
    fn rebind_relational_aggregates(
        &mut self,
        dirty_expressions: &BTreeSet<(FragmentId, NodeId, ExprId)>,
        dirty_values: &BTreeSet<(FragmentId, ValueId)>,
    ) -> Result<bool, ContractLoweringError> {
        enum Call {
            Ordinary(ContractAggregateCall),
            Writer(WriterAggregateCall),
        }
        let mut calls = Vec::new();
        for (&fragment, builder) in &self.fragments {
            builder.visit_construction_relational_calls_observed(
                &mut self.work,
                |site, binding, work| {
                    let node_id = site.node();
                    let node = builder
                        .construction_node(node_id)
                        .ok_or_else(|| invalid("relational call construction node is absent"))?;
                    let actual = match (site, &node.kind) {
                        (
                            PhysicalCallSite::Aggregate { call, .. },
                            NodeKind::Aggregate { calls, .. },
                        )
                        | (
                            PhysicalCallSite::TopNState { call, .. },
                            NodeKind::TopN {
                                reduction:
                                    novarocks_physical_plan::TopNReduction::GroupedStates {
                                        calls, ..
                                    },
                                ..
                            },
                        ) => Call::Ordinary(
                            calls
                                .get(call as usize)
                                .ok_or_else(|| invalid("aggregate construction ordinal is absent"))?
                                .clone(),
                        ),
                        (
                            PhysicalCallSite::WriterPartial { call, .. },
                            NodeKind::TableWriter { target },
                        ) => Call::Writer(
                            target
                                .partial_aggregates
                                .get(call as usize)
                                .ok_or_else(|| invalid("Writer partial ordinal is absent"))?
                                .clone(),
                        ),
                        (
                            PhysicalCallSite::WriterFinal { call, .. },
                            NodeKind::TableFinish(spec),
                        ) => Call::Writer(
                            spec.final_aggregates
                                .get(call as usize)
                                .ok_or_else(|| invalid("Writer final ordinal is absent"))?
                                .clone(),
                        ),
                        (PhysicalCallSite::Table { .. } | PhysicalCallSite::Window { .. }, _) => {
                            return Ok(());
                        }
                        _ => {
                            return Err(invalid(
                                "actual relational call site differs from its node",
                            ));
                        }
                    };
                    if !matches!(binding, PhysicalCallBinding::Aggregate(_)) {
                        return Err(invalid(
                            "aggregate visitor carries a different binding lifecycle",
                        ));
                    }
                    work.flush()?;
                    calls
                        .try_reserve(1)
                        .map_err(|_| CompileControlError::ResourceExhausted)?;
                    calls.push((fragment, node_id, site, actual));
                    work.step()?;
                    Ok::<_, ContractLoweringError>(())
                },
            )?;
        }
        // Dependency identity comes only from the ONE checked original state
        // visitor. Fragment IDs and missing canonical records are not ordering.
        let mut by_site = BTreeMap::new();
        let mut dependencies = Vec::new();
        self.work.flush()?;
        dependencies
            .try_reserve_exact(calls.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for (index, (fragment, _, site, _)) in calls.iter().enumerate() {
            self.work.flush()?;
            if by_site.insert((*fragment, *site), index).is_some() {
                return Err(invalid("actual aggregate dependency site is duplicated"));
            }
            dependencies.push(BTreeSet::new());
            self.work.step()?;
        }
        for (index, (fragment, _, site, call)) in calls.iter().enumerate() {
            let binding = match call {
                Call::Ordinary(c) => &c.binding,
                Call::Writer(c) => &c.binding,
            };
            self.work.step()?;
            if binding.phase.consumes_logical_arguments() {
                continue;
            }
            let graph = super::super::lowered_draft::state_sources::ConstructionStateGraph::borrow(
                &self.call_sources,
                &self.fragments,
                &self.completions,
                &self.edges,
            );
            graph
                .visit_merge_observed(
                    *fragment,
                    *site,
                    &mut self.work,
                    |producer, endpoint, work| {
                        let producer_index =
                            by_site.get(&(endpoint.fragment, producer.site)).copied();
                        work.step()?;
                        let producer_index = producer_index.ok_or(
                            super::super::lowered_draft::SqlSourceJournalError::InvalidSource(
                                "actual state producer has no relational construction call",
                            ),
                        )?;
                        if producer_index == index {
                            return Err(
                                super::super::lowered_draft::SqlSourceJournalError::InvalidSource(
                                    "actual aggregate dependency refers to its own consumer",
                                ),
                            );
                        }
                        work.flush()?;
                        dependencies[index].insert(producer_index);
                        work.step()?;
                        Ok(())
                    },
                )
                .map_err(Self::state_source_error)?;
        }
        let mut completed = BTreeSet::new();
        let mut pending = Vec::new();
        let mut ordered = Vec::new();
        self.work.flush()?;
        pending
            .try_reserve_exact(calls.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        ordered
            .try_reserve_exact(calls.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for call in calls {
            pending.push(Some(call));
            self.work.step()?;
        }
        while ordered.len() < pending.len() {
            let mut ready = None;
            for (index, call) in pending.iter().enumerate() {
                self.work.step()?;
                if call.is_none() {
                    continue;
                }
                let mut available = true;
                for producer in &dependencies[index] {
                    self.work.step()?;
                    available &= completed.contains(producer);
                }
                if available {
                    ready = Some(index);
                    break;
                }
            }
            let index =
                ready.ok_or_else(|| invalid("actual aggregate state dependencies are cyclic"))?;
            ordered.push(
                pending[index]
                    .take()
                    .expect("ready construction call checked above"),
            );
            self.work.flush()?;
            completed.insert(index);
            self.work.step()?;
        }
        let mut changed = false;
        for (fragment, node, site, call) in ordered {
            let old_binding = match &call {
                Call::Ordinary(c) => &c.binding,
                Call::Writer(c) => &c.binding,
            };
            let update = old_binding.phase.consumes_logical_arguments();
            let dependent = match &call {
                Call::Ordinary(c) => {
                    let mut dirty = false;
                    for &id in c
                        .arguments
                        .iter()
                        .chain(c.order_by.iter().map(|key| &key.expr))
                    {
                        self.work.step()?;
                        let definition = self.construction_root_definition(fragment, node, id)?;
                        dirty |= dirty_expressions.contains(&(fragment, node, definition));
                    }
                    dirty
                }
                Call::Writer(c) => {
                    self.work.step()?;
                    dirty_values.contains(&(fragment, c.input))
                }
            };
            if update && !dependent {
                continue;
            }
            // A merge borrows every exact contributor from the ONE original
            // state traversal, retaining repeated links and no-contribution
            // terminals. It never scans for a same-signature aggregate.
            let mut no_contribution_count = 0usize;
            let contributors = if update {
                Vec::new()
            } else {
                let graph =
                    super::super::lowered_draft::state_sources::ConstructionStateGraph::borrow(
                        &self.call_sources,
                        &self.fragments,
                        &self.completions,
                        &self.edges,
                    );
                let mut contributors = Vec::new();
                let consumer = self
                    .call_sources
                    .entries
                    .get(&(fragment, site))
                    .and_then(|entry| entry.logical.captured())
                    .ok_or_else(|| invalid("merge lacks its actual captured logical source"))?;
                let mut writer_callback_failure = None;
                let traversal = graph.visit_merge_sources_observed(fragment, site, &mut self.work, |producer, _, work| {
                    if producer.writer != matches!(&call, Call::Writer(_)) {
                        return Err(super::super::lowered_draft::SqlSourceJournalError::InvalidSource(
                            "merge route changes ordinary/Writer source lifecycle",
                        ));
                    }
                    let canonical = producer.entry.canonical.as_ref().ok_or(
                        super::super::lowered_draft::SqlSourceJournalError::MissingLogicalSource,
                    )?;
                    let captured = producer.entry.logical.captured().ok_or(
                        super::super::lowered_draft::SqlSourceJournalError::MissingLogicalSource,
                    )?;
                    if !canonical.belongs_to(captured) {
                        return Err(super::super::lowered_draft::SqlSourceJournalError::InvalidSource(
                            "state contributor canonical request belongs to a foreign source",
                        ));
                    }
                    if !producer.writer {
                        super::super::physical_aggregate_requests::checked_aggregate_state_semantics_observed(
                            consumer, old_binding, captured, producer.binding, work)?;
                    }
                    if producer.writer
                        && let Err(error) = super::super::physical_writer_requests::checked_captured_writer_sources_observed(consumer, captured, work) {
                            let exit = match &error {
                                super::super::physical_writer_requests::PhysicalWriterRequestError::Control(cause) => super::super::lowered_draft::SqlSourceJournalError::Control(cause.clone()),
                                super::super::physical_writer_requests::PhysicalWriterRequestError::InvalidSource(detail) => super::super::lowered_draft::SqlSourceJournalError::InvalidSource(detail),
                                _ => super::super::lowered_draft::SqlSourceJournalError::InvalidSource("construction Writer captured compatibility rejected"),
                            };
                            writer_callback_failure = Some(error);
                            return Err(exit);
                        }
                    work.flush()?;
                    contributors.try_reserve(1).map_err(|_| CompileControlError::ResourceExhausted)?;
                    contributors.push((Arc::clone(canonical), producer.binding.clone()));
                    work.step()?;
                    work.flush()?;
                    Ok(())
                }, |_, work| {
                    no_contribution_count = no_contribution_count.checked_add(1)
                        .ok_or(CompileControlError::ResourceExhausted)?;
                    work.step()?;
                    Ok(())
                });
                if let Some(error) = writer_callback_failure {
                    return Err(match error {
                        super::super::physical_writer_requests::PhysicalWriterRequestError::Control(cause) => ContractLoweringError::Control(cause),
                        super::super::physical_writer_requests::PhysicalWriterRequestError::InvalidSource(detail) => invalid(detail),
                        error => ContractLoweringError::InvalidFunctionBinding { detail: format!("construction Writer captured compatibility: {error:?}") },
                    });
                }
                traversal.map_err(Self::state_source_error)?;
                contributors
            };
            let mut entry = self
                .call_sources
                .entries
                .remove(&(fragment, site))
                .ok_or_else(|| {
                    invalid("aggregate rebind lacks its original source journal entry")
                })?;
            let captured = entry
                .logical
                .captured()
                .ok_or_else(|| invalid("aggregate rebind lacks its original logical source"))?;
            if entry.phase != old_binding.phase {
                return Err(invalid("aggregate rebind changes original source phase"));
            }
            if !update && entry.canonical.is_none() {
                // Validate the consumer's ORIGINAL contract before current NULL
                // facts can select a different signature. The merge keeps its
                // own capture; a fresh selection cannot repair malformed input.
                let request = captured.request();
                let original_selected = self.select_canonical_operational_request(
                    captured.binding(),
                    request.logical_argument_count,
                    request.arguments,
                )?;
                let order_count = request
                    .arguments
                    .len()
                    .checked_sub(request.logical_argument_count)
                    .ok_or_else(|| {
                        invalid("aggregate original request has an invalid logical prefix")
                    })?;
                let original_binding = lower_captured_aggregate_binding(
                    captured.binding(),
                    request.logical_argument_count,
                    order_count,
                    &captured.binding().resolved().selected,
                    entry.phase,
                    &mut self.work,
                )?;
                match super::super::physical_aggregate_requests::selected_binding_correspondence_observed(
                    captured,
                    &original_selected,
                    request.logical_argument_count,
                    &original_binding,
                    &mut self.work,
                ) {
                    Ok(()) => {}
                    Err(super::super::physical_aggregate_requests::PhysicalAggregateRequestError::Control(cause)) => {
                        return Err(ContractLoweringError::Control(cause));
                    }
                    Err(_) => return Err(invalid(
                        "aggregate original binding differs from its installed logical request",
                    )),
                }
            }
            let (arguments, constraint, selected) = if update {
                let previous = self.current_fragment;
                self.current_fragment = fragment;
                let authored = match &call {
                    Call::Ordinary(c) => {
                        self.work.flush()?;
                        let mut channels = Vec::new();
                        let channel_count = c
                            .arguments
                            .len()
                            .checked_add(c.order_by.len())
                            .ok_or(CompileControlError::ResourceExhausted)?;
                        channels
                            .try_reserve_exact(channel_count)
                            .map_err(|_| CompileControlError::ResourceExhausted)?;
                        for (ordinal, id) in c
                            .arguments
                            .iter()
                            .chain(c.order_by.iter().map(|key| &key.expr))
                            .enumerate()
                        {
                            channels.push(self.aggregate_operational_channel(
                                self.construction_root_definition(fragment, node, *id)?,
                                ordinal,
                                captured,
                            )?);
                            self.work.step()?;
                        }
                        self.author_canonical_update_aggregate(node, captured, &channels)
                    }
                    Call::Writer(c) => self.author_canonical_writer_update(captured, c.input),
                };
                self.current_fragment = previous;
                let canonical = authored?;
                (
                    canonical.arguments.clone(),
                    canonical.result_constraint.clone(),
                    Arc::clone(&canonical.selected),
                )
            } else {
                if contributors.is_empty()
                    && (!matches!(&call, Call::Writer(_)) || no_contribution_count == 0)
                {
                    return Err(invalid(
                        "merge has no actual state contribution or Writer terminal",
                    ));
                }
                // The consumer retains its OWN original logical/control/ORDER
                // channels and constants. Producers never elect these values.
                let request = captured.request();
                let logical_count = request.logical_argument_count;
                self.work.flush()?;
                let mut arguments = Vec::new();
                arguments
                    .try_reserve_exact(request.arguments.len())
                    .map_err(|_| CompileControlError::ResourceExhausted)?;
                for argument in request.arguments {
                    self.work.flush()?;
                    arguments.push(argument.clone());
                    self.work.step()?;
                }
                for (producer, binding) in &contributors {
                    self.work.step()?;
                    // The original semantic/state author already checked this
                    // independently captured source. Only the declared state
                    // law can transfer root nullability into matching logical
                    // Value domains. A constant retains its OWN complete FVT and
                    // checked backing; no constant or control value is replaced.
                    // Exact state signatures may read the current root fact
                    // of their OWN logical source revision. Independent sources
                    // retain their own request and original compatibility law.
                    let own_source = producer.identity.same_revision(captured.logical_identity());
                    self.work.step()?;
                    if binding.merge_state.as_ref().is_none_or(|state| state.state_argument_contract != novarocks_type_contract::AggregateStateArgumentContract::ValueRootNullabilityIndependent)
                        && !own_source
                    {
                        continue;
                    }
                    for (ordinal, (left, right)) in arguments
                        .iter_mut()
                        .zip(producer.arguments.iter())
                        .enumerate()
                    {
                        self.work.step()?;
                        // Only the positive SAME revision receipt can update
                        // its OWN ORDER metadata. Independent states never elect
                        // a consumer's ORDER value, constant or type.
                        if ordinal >= logical_count && !own_source {
                            continue;
                        }
                        if let (
                            novarocks_functions::FunctionArgument::Value {
                                value_type: a,
                                constant: None,
                            },
                            novarocks_functions::FunctionArgument::Value { value_type: b, .. },
                        ) = (left, right)
                        {
                            self.work.flush()?;
                            let mut root = b.clone();
                            root = root
                                .with_nullable(a.nullable())
                                .expect("validated root NULL contract");
                            let same_domain = a
                                .exactly_equals_observed::<ContractLoweringError>(&root, || {
                                    self.work.step().map_err(ContractLoweringError::from)
                                })?;
                            if same_domain {
                                *a = a
                                    .with_nullable(a.nullable() || (b.nullable()))
                                    .expect("validated root NULL contract");
                            }
                        }
                    }
                }
                let constraint = self.current_constraint(captured.binding(), &arguments)?;
                let selected = self.select_canonical_request_with_constraint(
                    captured.binding(),
                    logical_count,
                    &arguments,
                    constraint.as_ref(),
                )?;
                self.work.flush()?;
                let arguments = arguments.into_boxed_slice();
                self.work.step()?;
                (arguments, constraint, selected)
            };
            let logical_count = captured.request().logical_argument_count;
            let order_count = arguments.len().checked_sub(logical_count).ok_or_else(|| {
                invalid("aggregate current channel count is smaller than its logical source")
            })?;
            let revised = lower_captured_aggregate_binding(
                captured.binding(),
                logical_count,
                order_count,
                &selected,
                old_binding.phase,
                &mut self.work,
            )?;
            for (_, producer) in &contributors {
                // Writer has the original complete binding compatibility law.
                // Ordinary pair semantics are authored by its ONE original
                // state owner above, not by a Writer rule or source equality.
                if matches!(&call, Call::Writer(_))
                    && !novarocks_physical_plan::aggregate_bindings_match_observed(
                        &revised,
                        producer,
                        &mut self.work,
                    )?
                {
                    return Err(invalid(
                        "aggregate state contributor differs from current consumer contract",
                    ));
                }
            }
            self.work.flush()?;
            let different = *old_binding != revised;
            self.work.step()?;
            self.work.flush()?;
            if different {
                self.fragments
                    .get_mut(&fragment)
                    .expect("checked above")
                    .nullability_editor()
                    .aggregate_signature(site, old_binding, revised, &mut self.work)?;
                changed = true;
            }
            self.work.flush()?;
            entry.canonical = Some(Arc::new(CanonicalAggregateOperationalRequest {
                binding: captured.binding().clone(),
                identity: captured.logical_identity().clone(),
                arguments,
                logical_count,
                selected,
                result_constraint: constraint,
            }));
            self.work.step()?;
            self.call_sources.entries.insert((fragment, site), entry);
            self.work.step()?;
        }
        Ok(changed)
    }
    fn derived_construction_value_type(
        &mut self,
        fragment: FragmentId,
        value: &novarocks_physical_plan::ValueDef,
        dirty_expressions: &BTreeSet<(FragmentId, NodeId, ExprId)>,
        dirty_values: &BTreeSet<(FragmentId, ValueId)>,
    ) -> Result<ValueType, ContractLoweringError> {
        self.work.flush()?;
        let mut revised = value.ty.clone();
        let origin = value.origin.clone();
        self.work.flush()?;
        match origin {
            // Independently produced counters have no row-input NULL propagation.
            ValueOrigin::JoinMark { .. }
            | ValueOrigin::Summary { .. }
            | ValueOrigin::ProviderField { .. } => {}
            ValueOrigin::Expr { node, root } => {
                let expr = self.construction_root_definition(fragment, node, root)?;
                if dirty_expressions.contains(&(fragment, node, expr)) {
                    revised = published_value_type(
                        &value.ty,
                        &self.construction_expression(fragment, node, expr)?.ty,
                    );
                }
            }
            ValueOrigin::NullExtended { of, .. } => {
                revised =
                    published_value_type(&value.ty, &self.construction_value(fragment, of)?.ty);
                revised = revised
                    .with_nullable(true)
                    .expect("validated root NULL contract");
            }
            ValueOrigin::ExchangeImport { edge, source_value } => {
                let sender = self
                    .edges
                    .get(&edge)
                    .ok_or_else(|| invalid("exchange import has no original edge"))?
                    .source
                    .fragment;
                if dirty_values.contains(&(sender, source_value)) {
                    revised = published_value_type(
                        &value.ty,
                        &self.construction_value(sender, source_value)?.ty,
                    );
                }
            }
            ValueOrigin::CteImport {
                edge,
                producer_fragment,
                producer_value,
            } => {
                let edge = self
                    .edges
                    .get(&edge)
                    .ok_or_else(|| invalid("CTE import has no original edge"))?;
                if edge.source.fragment != producer_fragment {
                    return Err(invalid(
                        "CTE import differs from its original producer fragment",
                    ));
                }
                if dirty_values.contains(&(producer_fragment, producer_value)) {
                    revised = published_value_type(
                        &value.ty,
                        &self
                            .construction_value(producer_fragment, producer_value)?
                            .ty,
                    );
                }
            }
            ValueOrigin::AggregateState { call, .. } | ValueOrigin::AggregateResult { call } => {
                let state = matches!(value.origin, ValueOrigin::AggregateState { .. });
                let mut found = None;
                for (_, node) in self.fragments[&fragment].construction_nodes() {
                    if let Some((_, calls)) = node.kind.aggregate_contract() {
                        for actual in calls {
                            self.work.step()?;
                            if actual.id == call {
                                if actual.output != value.id {
                                    return Err(invalid(
                                        "aggregate value differs from its exact call output",
                                    ));
                                }
                                if state
                                    && !matches!(value.origin,ValueOrigin::AggregateState{phase:actual_phase,..}if actual_phase==actual.binding.phase)
                                {
                                    return Err(invalid(
                                        "aggregate state value changes its actual phase",
                                    ));
                                }
                                found = Some(if state {
                                    actual
                                        .binding
                                        .intermediate_type()
                                        .ok_or_else(|| {
                                            invalid("aggregate state value has no merge state")
                                        })?
                                        .clone()
                                } else {
                                    actual.binding.function.result_type.clone()
                                });
                            }
                        }
                    }
                }
                let found =
                    found.ok_or_else(|| invalid("aggregate value has no actual producer"))?;
                revised = published_value_type(&value.ty, &found);
            }
            ValueOrigin::NodeOutput {
                node,
                output_ordinal,
            } => {
                self.work.flush()?;
                let actual = self.construction_node(fragment, node)?.clone();
                self.work.flush()?;
                let ordinal = output_ordinal as usize;
                if actual.output.columns.get(ordinal) != Some(&value.id) {
                    return Err(invalid(
                        "NodeOutput value differs from its ordered original port",
                    ));
                }
                match &actual.kind {
                    NodeKind::Values { rows } => {
                        for row in rows.iter() {
                            self.work.step()?;
                            let expr = *row.get(ordinal).ok_or_else(|| {
                                invalid("VALUES row differs from its original output width")
                            })?;
                            let expr = self.construction_root_definition(fragment, node, expr)?;
                            if dirty_expressions.contains(&(fragment, node, expr)) {
                                revised = revised
                                    .with_nullable(
                                        revised.nullable()
                                            || (self
                                                .construction_expression(fragment, node, expr)?
                                                .ty
                                                .nullable()),
                                    )
                                    .expect("validated root NULL contract");
                            }
                        }
                    }
                    NodeKind::Window(spec) => {
                        let input = actual
                            .inputs
                            .first()
                            .copied()
                            .ok_or_else(|| invalid("Window output has no actual input"))?;
                        let prefix = self
                            .construction_node(fragment, input)?
                            .output
                            .columns
                            .len();
                        let call = ordinal
                            .checked_sub(prefix)
                            .and_then(|ordinal| spec.calls.get(ordinal))
                            .ok_or_else(|| invalid("Window output has no actual call producer"))?;
                        self.work.step()?;
                        if call.output != value.id {
                            return Err(invalid(
                                "Window output differs from its actual call ordinal",
                            ));
                        }
                        revised = published_value_type(&value.ty, &call.binding.result_type);
                    }
                    NodeKind::SetOp { input_mappings, .. } => {
                        for input in input_mappings.iter() {
                            self.work.step()?;
                            let input = *input.get(ordinal).ok_or_else(|| {
                                invalid("set mapping differs from its original output width")
                            })?;
                            if dirty_values.contains(&(fragment, input)) {
                                revised = revised
                                    .with_nullable(
                                        revised.nullable()
                                            || (self
                                                .construction_value(fragment, input)?
                                                .ty
                                                .nullable()),
                                    )
                                    .expect("validated root NULL contract");
                            }
                        }
                    }
                    NodeKind::TableFunction {
                        function,
                        outputs,
                        left_outer,
                        ..
                    } => {
                        match outputs
                            .get(ordinal)
                            .ok_or_else(|| invalid("table output occurrence is absent"))?
                        {
                            TableFunctionOutput::PassThrough(input) => {
                                revised = published_value_type(
                                    &value.ty,
                                    &self.construction_value(fragment, *input)?.ty,
                                );
                            }
                            TableFunctionOutput::FunctionResult {
                                result_ordinal,
                                value: output,
                            } => {
                                if *output != value.id {
                                    return Err(invalid(
                                        "table result occurrence changes value identity",
                                    ));
                                }
                                let result = function
                                    .result_types
                                    .get(*result_ordinal as usize)
                                    .ok_or_else(|| {
                                        invalid("table relation result ordinal is absent")
                                    })?;
                                revised = published_value_type(&value.ty, result);
                                revised = revised
                                    .with_nullable(revised.nullable() || (*left_outer))
                                    .expect("validated root NULL contract");
                            }
                        }
                    }
                    NodeKind::Unpivot { spec } => {
                        if value.id == spec.value_output {
                            for mapping in spec.mappings.iter() {
                                self.work.step()?;
                                revised = revised
                                    .with_nullable(
                                        revised.nullable()
                                            || (self
                                                .construction_value(fragment, mapping.input)?
                                                .ty
                                                .nullable()),
                                    )
                                    .expect("validated root NULL contract");
                            }
                        } else if let Some((input, _)) = spec
                            .passthrough
                            .iter()
                            .find(|(_, output)| *output == value.id)
                        {
                            revised = published_value_type(
                                &value.ty,
                                &self.construction_value(fragment, *input)?.ty,
                            );
                        }
                        if let Some(literal_ordinal) = spec
                            .literal_outputs
                            .iter()
                            .position(|output| *output == value.id)
                        {
                            for mapping in spec.mappings.iter() {
                                self.work.step()?;
                                let constant = mapping.constants.get(literal_ordinal)
                                    .ok_or_else(|| invalid("unpivot literal occurrence differs from its original width"))?;
                                if let novarocks_physical_plan::UnpivotConstant::Scalar(expr) =
                                    constant
                                {
                                    self.work.step()?;
                                    let expr =
                                        self.construction_root_definition(fragment, node, *expr)?;
                                    if dirty_expressions.contains(&(fragment, node, expr)) {
                                        revised = revised
                                            .with_nullable(
                                                revised.nullable()
                                                    || (self
                                                        .construction_expression(
                                                            fragment, node, expr,
                                                        )?
                                                        .ty
                                                        .nullable()),
                                            )
                                            .expect("validated root NULL contract");
                                    }
                                }
                            }
                        }
                        // Pool/list/map literals preserve their original checked
                        // constant admission; no NULL fact is guessed from a tag.
                    }
                    NodeKind::ChangeEventExpand {
                        events,
                        effect_output,
                    } => {
                        if value.id != *effect_output {
                            for event in events.iter() {
                                for assignment in event.assignments.iter() {
                                    let (output, expr) =
                                        (&assignment.output, &assignment.expression);
                                    self.work.step()?;
                                    if *output == value.id
                                        && let Some(root) = expr
                                    {
                                        let expression = self
                                            .construction_root_definition(fragment, node, *root)?;
                                        if dirty_expressions.contains(&(fragment, node, expression))
                                        {
                                            revised = revised
                                                .with_nullable(
                                                    revised.nullable()
                                                        || self
                                                            .construction_expression(
                                                                fragment, node, expression,
                                                            )?
                                                            .ty
                                                            .nullable(),
                                                )
                                                .expect("validated root NULL contract");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    NodeKind::Repeat {
                        grouping_values, ..
                    } => {
                        if let Some((input, _)) = grouping_values
                            .iter()
                            .find(|(_, output)| *output == value.id)
                        {
                            revised = published_value_type(
                                &value.ty,
                                &self.construction_value(fragment, *input)?.ty,
                            );
                            revised = revised
                                .with_nullable(true)
                                .expect("validated root NULL contract");
                        }
                    }
                    NodeKind::GenerateSeries { .. }
                    | NodeKind::SystemRelation { .. }
                    | NodeKind::WriteLayoutKey { .. }
                    | NodeKind::ContentApply(_)
                    | NodeKind::PositionShard(_) => {}
                    NodeKind::Scan { .. }
                    | NodeKind::Filter { .. }
                    | NodeKind::Project { .. }
                    | NodeKind::Aggregate { .. }
                    | NodeKind::HashJoin { .. }
                    | NodeKind::NestLoopJoin { .. }
                    | NodeKind::Sort { .. }
                    | NodeKind::TopN { .. }
                    | NodeKind::Limit { .. }
                    | NodeKind::AssertOneRow(_)
                    | NodeKind::ExchangeSource { .. }
                    | NodeKind::TableWriter { .. }
                    | NodeKind::TableFinish(_) => {
                        return Err(invalid(
                            "NodeOutput origin differs from its actual construction family",
                        ));
                    }
                }
            }
            ValueOrigin::WriterDerived { writer_node, kind } => {
                let actual = self.construction_node(fragment, writer_node)?.clone();
                match &actual.kind {
                    NodeKind::TableWriter { target } => {
                        for call in target.partial_aggregates.iter() {
                            self.work.step()?;
                            if call.output == value.id {
                                revised = published_value_type(
                                    &value.ty,
                                    call.binding.intermediate_type().ok_or_else(|| {
                                        invalid("writer aggregate has no merge state")
                                    })?,
                                );
                            }
                        }
                    }
                    NodeKind::TableFinish(spec) => {
                        for call in spec.final_aggregates.iter() {
                            self.work.step()?;
                            if call.output == value.id {
                                revised = published_value_type(
                                    &value.ty,
                                    &call.binding.function.result_type,
                                );
                            }
                        }
                        if let Some(unpivot) = &spec.grouped_unpivot {
                            if value.id == unpivot.value_output {
                                for mapping in unpivot.mappings.iter() {
                                    self.work.step()?;
                                    revised = revised
                                        .with_nullable(
                                            revised.nullable()
                                                || (self
                                                    .construction_value(fragment, mapping.input)?
                                                    .ty
                                                    .nullable()),
                                        )
                                        .expect("validated root NULL contract");
                                }
                            }
                            if let Some(literal_ordinal) = unpivot
                                .literal_outputs
                                .iter()
                                .position(|output| *output == value.id)
                            {
                                for mapping in unpivot.mappings.iter() {
                                    self.work.step()?;
                                    let constant = mapping.constants.get(literal_ordinal)
                                        .ok_or_else(|| invalid("Writer unpivot literal occurrence differs from its original width"))?;
                                    if let novarocks_physical_plan::UnpivotConstant::Scalar(expr) =
                                        constant
                                    {
                                        self.work.step()?;
                                        let expr = self.construction_root_definition(
                                            fragment,
                                            writer_node,
                                            *expr,
                                        )?;
                                        if dirty_expressions.contains(&(
                                            fragment,
                                            writer_node,
                                            expr,
                                        )) {
                                            revised = revised
                                                .with_nullable(
                                                    revised.nullable()
                                                        || (self
                                                            .construction_expression(
                                                                fragment,
                                                                writer_node,
                                                                expr,
                                                            )?
                                                            .ty
                                                            .nullable()),
                                                )
                                                .expect("validated root NULL contract");
                                        }
                                    }
                                }
                            }
                            if value.id == unpivot.grouping_output {
                                revised = published_value_type(
                                    &value.ty,
                                    &self
                                        .construction_value(fragment, unpivot.grouping_input)?
                                        .ty,
                                );
                            }
                        }
                    }
                    _ => {
                        return Err(invalid(
                            "WriterDerived value differs from its original Writer lifecycle",
                        ));
                    }
                }
                let _ = kind; // Role-specific constant fields retain the original exact type.
            }
        }
        Ok(revised)
    }
    fn rebind_writer_relation_schemas(&mut self) -> Result<(), ContractLoweringError> {
        let mut fields = Vec::new();
        for (&fragment, builder) in &self.fragments {
            for (&node, actual) in builder.construction_nodes() {
                let schemas: Vec<_> = match &actual.kind {
                    NodeKind::TableWriter { target } => vec![(false, &target.output_schema)],
                    NodeKind::TableFinish(spec) => {
                        vec![(true, &spec.input_schema), (false, &spec.output_schema)]
                    }
                    _ => Vec::new(),
                };
                for (input, schema) in schemas {
                    for (ordinal, field) in schema.fields.iter().enumerate() {
                        self.work.step()?;
                        let revised = builder
                            .value(field.value)
                            .ok_or_else(|| {
                                invalid("Writer schema field has no actual value definition")
                            })?
                            .ty
                            .clone();
                        if field.ty.nullable() != revised.nullable() {
                            self.work.flush()?;
                            fields
                                .try_reserve(1)
                                .map_err(|_| CompileControlError::ResourceExhausted)?;
                            fields.push((
                                fragment,
                                node,
                                input,
                                ordinal,
                                field.ty.clone(),
                                revised,
                            ));
                            self.work.step()?;
                        }
                    }
                }
            }
        }
        for (fragment, node, input, ordinal, old, revised) in fields {
            self.fragments
                .get_mut(&fragment)
                .expect("checked above")
                .nullability_editor()
                .writer_relation_field_type(node, input, ordinal, &old, revised, &mut self.work)?;
        }
        Ok(())
    }
}
