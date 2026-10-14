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

//! Direct SQL physical-plan lowering into the final physical-plan contract.
//!
//! This visitor deliberately consumes SQL's planner-private physical tree. It
//! never accepts the legacy sealed distributed representation, so extending
//! operator coverage cannot accidentally turn that carrier into a second plan
//! authority.

//! Author intermediate group reductions from the actual preaggregation owner.
//! Original captures are loaned by Arc, never rebuilt from state carriers.

use super::*;
use novarocks_physical_plan::{PhysicalCallSite, PhysicalNode, TopNReduction};

impl ContractLoweringVisitor<'_> {
    pub(super) fn requires_grouped_topn(
        &mut self,
        child: &LoweredNode,
        phase: ContractTopNPhase,
    ) -> Result<bool, ContractLoweringError> {
        if self.emission_mode
            != crate::compiler::SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration
            || !matches!(phase, ContractTopNPhase::Partial { .. })
        {
            return Ok(false);
        }
        self.work.flush()?;
        let producer = self
            .fragments
            .get(&child.fragment)
            .and_then(|fragment| fragment.construction_node(child.node));
        let direct = match producer.map(|node| &node.kind) {
            Some(NodeKind::Aggregate {
                grouping: AggregateGrouping::Partial,
                calls,
                ..
            }) => Some(!calls.is_empty()),
            Some(NodeKind::Aggregate {
                grouping: AggregateGrouping::Complete,
                ..
            }) => Some(false),
            _ => None,
        };
        self.work.step()?;
        if let Some(direct) = direct {
            return Ok(direct);
        }
        // Ordinary Rows have no state routes. A transported state input is
        // outside this SQL author, and cannot fall back to a row-count quota.
        for value in &child.output {
            let endpoint = AggregateStateEndpoint {
                fragment: child.fragment,
                node: child.node,
                value: *value,
            };
            if self
                .call_sources
                .state_sources
                .contains_observed(endpoint, &mut self.work)
                .map_err(Self::state_source_error)?
            {
                return Err(grouped_shape(
                    "GroupedStates TopN requires a direct partial aggregate producer",
                ));
            }
        }
        Ok(false)
    }

    pub(super) fn append_grouped_topn(
        &mut self,
        node: NodeId,
        mut child: LoweredNode,
        order_by: Box<[SortExpr]>,
        limit: u64,
        offset: u64,
        phase: ContractTopNPhase,
    ) -> Result<LoweredNode, ContractLoweringError> {
        if offset != 0 || !matches!(phase, ContractTopNPhase::Partial { .. }) {
            return Err(grouped_shape(
                "GroupedStates TopN requires partial phase and zero offset",
            ));
        }
        if child.fragment != self.current_fragment {
            return Err(grouped_shape(
                "GroupedStates TopN input belongs to another fragment",
            ));
        }
        require_single_copy_input("GroupedStates TopN", &child.properties)?;
        self.work.flush()?;
        let producer = self
            .fragments
            .get(&child.fragment)
            .and_then(|fragment| fragment.construction_node(child.node))
            .ok_or_else(|| {
                grouped_shape("GroupedStates TopN lacks its direct preaggregation owner")
            })?;
        let NodeKind::Aggregate {
            group_by: original_keys,
            calls: original_calls,
            grouping: AggregateGrouping::Partial,
        } = &producer.kind
        else {
            return Err(grouped_shape(
                "GroupedStates TopN input is not a partial grouping aggregate",
            ));
        };
        let keys = original_keys
            .iter()
            .map(|(_, value)| *value)
            .collect::<Vec<_>>();
        let originals = original_calls.clone();
        self.work.step()?;
        if keys.is_empty() {
            return Err(grouped_shape(
                "GroupedStates TopN requires nonempty complete grouping keys",
            ));
        }
        let key_set = keys.iter().copied().collect::<BTreeSet<_>>();
        let expected = keys
            .iter()
            .copied()
            .chain(originals.iter().map(|call| call.output))
            .collect::<Vec<_>>();
        if key_set.len() != keys.len() || child.output.as_ref() != expected.as_slice() {
            return Err(grouped_shape(
                "GroupedStates TopN input must contain exactly complete keys and state outputs",
            ));
        }
        let mut ordered = BTreeSet::new();
        let mut ordering = Vec::new();
        for key in &order_by {
            self.work.flush()?;
            let expression = self
                .fragment_mut()
                .expressions()
                .get(key.expr)
                .ok_or_else(|| grouped_shape("GroupedStates TopN ordering expression is absent"))?;
            let ContractExprKind::Value(value) = expression.kind else {
                return Err(grouped_shape(
                    "GroupedStates TopN rank is not a direct complete grouping key",
                ));
            };
            if !key_set.contains(&value) || !ordered.insert(value) {
                return Err(grouped_shape(
                    "GroupedStates TopN order must cover every complete key exactly once",
                ));
            }
            ordering.push(OrderingKey {
                value,
                direction: key.direction,
                null_ordering: key.null_ordering,
            });
            self.work.step()?;
        }
        if ordered != key_set {
            return Err(grouped_shape(
                "GroupedStates TopN order omits a complete grouping key",
            ));
        }
        let comparator = OrderedComparisonAlgorithm::NativeScalarOrderV1;
        let mut group_by = Vec::new();
        for value in &keys {
            let ty = self.value_declared_type(*value)?;
            if !comparator.supports_value_type(&ty)
                || !novarocks_type_contract::PartitionHashAlgorithm::NativeExchangeV1
                    .supports_partition_key(&ty.data_type)
            {
                return Err(grouped_shape(
                    "GroupedStates TopN grouping key lacks NativeScalarOrderV1 comparison equivalence",
                ));
            }
            let expression =
                self.fragment_mut()
                    .add_expression(node, ty, ContractExprKind::Value(*value))?;
            group_by.push((expression, *value));
            self.work.step()?;
        }
        let mut output = keys;
        let mut calls = Vec::new();
        for (ordinal, original) in originals.iter().enumerate() {
            self.work.step()?;
            let sequence = match original.binding.phase {
                AggregatePhase::Partial { sequence }
                | AggregatePhase::Intermediate { sequence } => sequence,
                _ => {
                    return Err(grouped_shape(
                        "GroupedStates TopN input contains a final aggregate result",
                    ));
                }
            };
            let source_site = PhysicalCallSite::Aggregate {
                node: child.node,
                call: u32::try_from(ordinal).map_err(|_| CompileControlError::ResourceExhausted)?,
            };
            self.work.flush()?;
            let source = self
                .call_sources
                .entries
                .get(&(child.fragment, source_site))
                .ok_or_else(|| {
                    grouped_shape("GroupedStates TopN has no original aggregate source entry")
                })?;
            if source.phase != original.binding.phase
                || source.target != AggregateSourceTarget::Aggregate(original.id)
            {
                return Err(grouped_shape(
                    "GroupedStates TopN source differs from its direct producer",
                ));
            }
            let LoweredAggregateLogicalSource::Captured(captured) = &source.logical else {
                return Err(grouped_shape(
                    "GroupedStates TopN lacks an authenticated logical capture",
                ));
            };
            let canonical = source.canonical.as_ref().ok_or_else(|| {
                grouped_shape("GroupedStates TopN lacks its producer's canonical request")
            })?;
            if !canonical.belongs_to(captured) {
                return Err(grouped_shape(
                    "GroupedStates TopN canonical request belongs to another source",
                ));
            }
            let captured = Arc::clone(captured);
            let canonical = Arc::clone(canonical);
            self.work.step()?;
            let input = self.state_endpoint(child.node, original.output);
            if !self
                .call_sources
                .state_sources
                .contains_observed(input, &mut self.work)
                .map_err(Self::state_source_error)?
            {
                return Err(grouped_shape(
                    "GroupedStates TopN input lacks its original emitted state",
                ));
            }
            let ty = self.value_declared_type(original.output)?;
            self.work.flush()?;
            if !ty.exactly_equals_observed::<ContractLoweringError>(
                &original.binding.intermediate_type,
                || self.work.step().map_err(ContractLoweringError::from),
            )? {
                return Err(grouped_shape(
                    "GroupedStates TopN state differs from the exact intermediate type",
                ));
            }
            let argument = self.fragment_mut().add_expression(
                node,
                ty,
                ContractExprKind::Value(original.output),
            )?;
            let id = self.allocate_aggregate_call()?;
            let phase = AggregatePhase::Intermediate { sequence };
            self.work.flush()?;
            let mut binding = original.binding.clone();
            binding.phase = phase;
            let value = self.fragment_mut().add_value(
                binding.intermediate_type.clone(),
                ValueOrigin::AggregateState { call: id, phase },
            )?;
            let site = PhysicalCallSite::TopNState {
                node,
                call: u32::try_from(ordinal).map_err(|_| CompileControlError::ResourceExhausted)?,
            };
            self.record_aggregate_source(
                site,
                LoweredAggregateSourceEntry {
                    logical: LoweredAggregateLogicalSource::Captured(captured),
                    canonical: Some(canonical),
                    phase,
                    runtime: AggregateRuntimeDemand::ExpressionState(argument),
                    target: AggregateSourceTarget::Aggregate(id),
                },
            )?;
            self.call_sources
                .state_sources
                .input_observed(self.current_fragment, site, input, &mut self.work)
                .map_err(Self::state_source_error)?;
            let endpoint = self.state_endpoint(node, value);
            self.call_sources
                .state_sources
                .emission_observed(endpoint, site, &mut self.work)
                .map_err(Self::state_source_error)?;
            for mapped in child.columns.values_mut() {
                self.work.step()?;
                if *mapped == original.output {
                    *mapped = value;
                }
            }
            output.push(value);
            calls.push(ContractAggregateCall {
                id,
                binding,
                arguments: Box::from([argument]),
                distinct: false,
                order_by: Box::default(),
                output: value,
            });
        }
        let properties = PhysicalProperties {
            distribution: child.properties.distribution.clone(),
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: ordering.into_boxed_slice(),
        };
        self.fragment_mut().insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([child.node]),
            required_inputs: Box::from([PhysicalProperties {
                distribution: child.properties.distribution.clone(),
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            }]),
            output_properties: properties.clone(),
            output: OutputPort {
                node,
                columns: output.clone().into_boxed_slice(),
            },
            kind: NodeKind::TopN {
                order_by,
                limit,
                offset,
                phase,
                reduction: TopNReduction::GroupedStates {
                    group_by: group_by.into_boxed_slice(),
                    calls: calls.into_boxed_slice(),
                    comparator,
                },
            },
        })?;
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns: child.columns,
            properties,
            display_names: child.display_names,
        })
    }
}

fn grouped_shape(detail: &'static str) -> ContractLoweringError {
    ContractLoweringError::UnsupportedTopNShape { detail }
}
