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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use arrow_schema::DataType;
use novarocks_type_contract::SemanticParameters;

use crate::{
    Edge, EdgeId, ExprArena, ExprId, ExprKind, ExprNode, Fragment, FragmentId, FragmentParts,
    FragmentSink, NodeId, NodeKind, OutputPort, PhysicalNode, PhysicalPlan, PhysicalPlanParts,
    PipelineDopDomain, PlanAnnotation, PlanVersionId, RequiredContracts, ResultPort, RuntimeFilter,
    RuntimeFilterId, ValidationErrors, ValueDef, ValueId, ValueOrigin, ValueType,
    validate_fragment_definition, validate_plan,
};

mod nullability_rebind;
mod structure;
pub use nullability_rebind::{NullabilityRebindError, UnpublishedNullabilityEditor};
pub use structure::FragmentStructureError;
pub(crate) use structure::admit_structure_counts;

#[cfg(test)]
#[path = "builder/structure_tests.rs"]
mod structure_tests;

/// Mutable construction state. It cannot be encoded or scheduled as a complete
/// plan. A structural definition still requires actual call/property proofs
/// and final package publication; finishing a construction stage is not that
/// publication.
pub struct FragmentBuilder {
    id: FragmentId,
    next_value: u32,
    next_expr: u32,
    next_node: Option<u32>,
    values: BTreeMap<ValueId, ValueDef>,
    expressions: ExprArena,
    nodes: BTreeMap<NodeId, PhysicalNode>,
    contains_scan: bool,
    runtime_filters: BTreeSet<RuntimeFilterId>,
    summaries: Box<[crate::SummaryOutput]>,
}

impl FragmentBuilder {
    pub fn new(id: FragmentId) -> Self {
        Self {
            id,
            next_value: 0,
            next_expr: 0,
            next_node: Some(0),
            values: BTreeMap::new(),
            expressions: ExprArena::default(),
            nodes: BTreeMap::new(),
            contains_scan: false,
            runtime_filters: BTreeSet::new(),
            summaries: Box::default(),
        }
    }

    /// Install explicit side-output declarations before structural publication.
    /// Full validation checks unique kinds, exact fields and legal authors.
    pub fn set_summaries(&mut self, summaries: Box<[crate::SummaryOutput]>) {
        self.summaries = summaries;
    }

    pub fn contains_scan(&self) -> bool {
        self.contains_scan
    }

    pub const fn id(&self) -> FragmentId {
        self.id
    }

    pub const fn expressions(&self) -> &ExprArena {
        &self.expressions
    }

    /// One value this fragment has already defined.
    pub fn value(&self, id: ValueId) -> Option<&ValueDef> {
        self.values.get(&id)
    }

    pub fn reserve_node_id(&mut self) -> Result<NodeId, BuildError> {
        let next = self
            .next_node
            .ok_or(BuildError::IdentitySpaceExhausted("node"))?;
        let id = NodeId::new(next);
        self.next_node = next.checked_add(1);
        Ok(id)
    }

    pub fn reserve_expression_id(&mut self) -> Result<ExprId, BuildError> {
        let id = ExprId::new(self.next_expr);
        self.next_expr = self
            .next_expr
            .checked_add(1)
            .ok_or(BuildError::IdentitySpaceExhausted("expression"))?;
        Ok(id)
    }

    pub fn add_value(&mut self, ty: ValueType, origin: ValueOrigin) -> Result<ValueId, BuildError> {
        let id = ValueId::new(self.next_value);
        self.next_value = self
            .next_value
            .checked_add(1)
            .ok_or(BuildError::IdentitySpaceExhausted("value"))?;
        self.insert_value(ValueDef { id, ty, origin })?;
        Ok(id)
    }

    pub fn insert_value(&mut self, value: ValueDef) -> Result<(), BuildError> {
        let id = value.id;
        let next_value = self.next_value.max(
            id.get()
                .checked_add(1)
                .ok_or(BuildError::IdentitySpaceExhausted("value"))?,
        );
        match self.values.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(value);
                self.next_value = next_value;
            }
            Entry::Occupied(_) => return Err(BuildError::DuplicateValue(id)),
        }
        Ok(())
    }

    pub fn add_expression(
        &mut self,
        owner: NodeId,
        ty: ValueType,
        kind: ExprKind,
    ) -> Result<ExprId, BuildError> {
        self.add_expression_in_scope(owner, None, ty, kind)
    }

    pub fn add_expression_in_scope(
        &mut self,
        owner: NodeId,
        lambda_scope: Option<ExprId>,
        ty: ValueType,
        kind: ExprKind,
    ) -> Result<ExprId, BuildError> {
        let id = self.reserve_expression_id()?;
        self.insert_expression(ExprNode {
            id,
            owner,
            lambda_scope,
            ty,
            kind,
        })?;
        Ok(id)
    }

    pub fn insert_expression(&mut self, expression: ExprNode) -> Result<(), BuildError> {
        let id = expression.id;
        let next_expr = self.next_expr.max(
            id.get()
                .checked_add(1)
                .ok_or(BuildError::IdentitySpaceExhausted("expression"))?,
        );
        match self.expressions.get(id) {
            Some(_) => return Err(BuildError::DuplicateExpression(id)),
            None => {
                self.expressions.insert(expression);
                self.next_expr = next_expr;
            }
        }
        Ok(())
    }

    /// Adds a filter over `input`, retaining rows for which every predicate
    /// holds.
    ///
    /// The caller supplies only what it actually decides: which rows to keep.
    /// The output port, the input requirement and the output properties all
    /// follow from the input and the predicates, so building them by hand was
    /// four separate chances to state something the contract then had to catch.
    /// `node` is reserved first because the predicates are owned by it.
    pub fn add_filter(
        &mut self,
        node: NodeId,
        input: NodeId,
        predicates: Box<[ExprId]>,
    ) -> Result<(), BuildError> {
        if predicates.is_empty() {
            return Err(BuildError::FilterWithoutPredicate(node));
        }
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        let columns = source.output.columns.clone();
        let input_properties = source.output_properties.clone();
        for predicate in &predicates {
            let expression = self.require_owned_expression(node, *predicate)?;
            if *expression.ty.data_type() != DataType::Boolean {
                return Err(BuildError::PredicateIsNotBoolean(*predicate));
            }
        }
        let replica_deterministic = crate::expressions_are_replica_deterministic(
            &self.expressions,
            predicates.iter().copied(),
            true,
        )
        .map_err(BuildError::MissingLegacyMetadata)?;
        let output_properties =
            crate::derive_filter_output_properties(&input_properties, replica_deterministic);
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([crate::passthrough_requirement(&input_properties)]),
            output_properties,
            output: OutputPort { node, columns },
            kind: NodeKind::Filter { predicates },
        })
    }

    /// Adds a global limit over `input`.
    ///
    /// A global limit is only meaningful over a single stream of single-copy
    /// rows, so the requirement is checked here instead of being restated by
    /// the caller and then re-derived by the contract. Row order survives;
    /// nothing else about the input can.
    pub fn add_limit(
        &mut self,
        node: NodeId,
        input: NodeId,
        limit: Option<u64>,
        offset: u64,
    ) -> Result<(), BuildError> {
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        if source.output_properties.distribution != crate::Distribution::Singleton
            || source.output_properties.row_multiplicity != crate::RowMultiplicity::SingleCopy
        {
            return Err(BuildError::LimitInputIsNotGlobal(node));
        }
        let columns = source.output.columns.clone();
        let output_properties = crate::PhysicalProperties {
            distribution: crate::Distribution::Singleton,
            row_multiplicity: crate::RowMultiplicity::SingleCopy,
            ordering: source.output_properties.ordering.clone(),
        };
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([crate::PhysicalProperties {
                distribution: crate::Distribution::Singleton,
                row_multiplicity: crate::RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            }]),
            output_properties,
            output: OutputPort { node, columns },
            kind: NodeKind::Limit { limit, offset },
        })
    }

    /// Adds a cardinality assertion over `input`.
    ///
    /// The assertion counts rows, so it needs each logical row to exist once;
    /// it constrains nothing else and changes nothing it passes on.
    pub fn add_assert_one_row(
        &mut self,
        node: NodeId,
        input: NodeId,
        spec: crate::RowCountAssertionSpec,
    ) -> Result<(), BuildError> {
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        if source.output_properties.row_multiplicity != crate::RowMultiplicity::SingleCopy {
            return Err(BuildError::AssertionOverReplicatedRows(node));
        }
        let columns = source.output.columns.clone();
        let output_properties = source.output_properties.clone();
        let required = crate::PhysicalProperties {
            distribution: output_properties.distribution.clone(),
            row_multiplicity: crate::RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([required]),
            output_properties,
            output: OutputPort { node, columns },
            kind: NodeKind::AssertOneRow(spec),
        })
    }

    /// Adds a sort over `input`.
    ///
    /// The ordering a sort establishes is not an independent fact to be stated
    /// alongside the sort keys - it *is* the sort keys, read as values. Having
    /// the caller supply both invited them to disagree, so the builder derives
    /// the ordering and only the keys are supplied. A key the statement wrote
    /// as an expression still orders the rows and still leaves nothing for a
    /// reader above to name, so such a sort establishes no ordering at all.
    ///
    /// A global sort needs one single-copy stream. An analytic sort leaves the
    /// input layout alone, because it orders within partitions that the layout
    /// already colocates.
    pub fn add_sort(
        &mut self,
        node: NodeId,
        input: NodeId,
        order_by: Box<[crate::SortExpr]>,
        mode: crate::SortMode,
    ) -> Result<(), BuildError> {
        let partition_by: &[crate::SortExpr] = match &mode {
            crate::SortMode::Global => &[],
            crate::SortMode::Analytic { partition_by }
            | crate::SortMode::PartitionTopN { partition_by, .. } => partition_by,
        };
        // A sort orders by its partition keys and then within them, so it has
        // keys as long as one of the two does: a window with `PARTITION BY`
        // and no `ORDER BY` still needs its partitions grouped.
        if order_by.is_empty() && partition_by.is_empty() {
            return Err(BuildError::OrderingWithoutKeys(node));
        }
        let ordering = crate::ordering_keys(&self.expressions, partition_by, &order_by)
            .unwrap_or_default()
            .into_boxed_slice();
        let (output_properties, required) =
            self.passthrough_ordering_properties(node, input, &mode, ordering)?;
        let source = self.nodes.get(&input).expect("checked above");
        let columns = source.output.columns.clone();
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([required]),
            output_properties,
            output: OutputPort { node, columns },
            kind: NodeKind::Sort { order_by, mode },
        })
    }

    /// Adds a top-N over `input`.
    ///
    /// Whether one stream is required follows from the phase rather than being
    /// stated beside it: a partial phase runs where the rows already are and
    /// its result is reduced later, while every other phase produces the answer
    /// and so needs one single-copy stream.
    pub fn add_top_n(
        &mut self,
        node: NodeId,
        input: NodeId,
        order_by: Box<[crate::SortExpr]>,
        limit: u64,
        offset: u64,
        phase: crate::TopNPhase,
    ) -> Result<(), BuildError> {
        let require_singleton = !matches!(phase, crate::TopNPhase::Partial { .. });
        if order_by.is_empty() {
            return Err(BuildError::OrderingWithoutKeys(node));
        }
        let ordering = crate::ordering_keys(&self.expressions, &[], &order_by)
            .unwrap_or_default()
            .into_boxed_slice();
        let mode = if require_singleton {
            crate::SortMode::Global
        } else {
            crate::SortMode::Analytic {
                partition_by: Box::default(),
            }
        };
        let (output_properties, required) =
            self.passthrough_ordering_properties(node, input, &mode, ordering)?;
        let source = self.nodes.get(&input).expect("checked above");
        let columns = source.output.columns.clone();
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([required]),
            output_properties,
            output: OutputPort { node, columns },
            kind: NodeKind::TopN {
                reduction: crate::TopNReduction::Rows,
                order_by,
                limit,
                offset,
                phase,
            },
        })
    }

    /// Adds a projection over `input`.
    ///
    /// `output` stays a caller decision because an output port is an ordered
    /// list of occurrences that may repeat a value, which the expression list
    /// cannot express: each expression defines its value exactly once. What
    /// follows from those two is the input requirement and which of the input's
    /// distribution and ordering survive, so the builder derives both.
    pub fn add_project(
        &mut self,
        node: NodeId,
        input: NodeId,
        expressions: Box<[(ExprId, ValueId)]>,
        output: Box<[ValueId]>,
    ) -> Result<(), BuildError> {
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        let input_properties = source.output_properties.clone();
        for (expression, value) in &expressions {
            self.require_owned_expression(node, *expression)?;
            if !self.values.contains_key(value) {
                return Err(BuildError::UndefinedValue(*value));
            }
        }
        for value in &output {
            if !self.values.contains_key(value) {
                return Err(BuildError::UndefinedValue(*value));
            }
        }
        let replica_deterministic = crate::expressions_are_replica_deterministic(
            &self.expressions,
            expressions.iter().map(|(expression, _)| *expression),
            true,
        )
        .map_err(BuildError::MissingLegacyMetadata)?;
        let output_properties = crate::derive_project_output_properties(
            &input_properties,
            &output,
            replica_deterministic,
        );
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([crate::passthrough_requirement(&input_properties)]),
            output_properties,
            output: OutputPort {
                node,
                columns: output,
            },
            kind: NodeKind::Project { expressions },
        })
    }

    /// Adds the receiving end of an exchange edge.
    ///
    /// An exchange source is a leaf in its own fragment: its rows come from
    /// another fragment over `edge`, so it has no inputs and no input
    /// requirement, and every value it offers must be one it imported. It also
    /// cannot claim an ordering - rows arrive interleaved from senders this
    /// release does not merge - so the caller states only the layout and row
    /// multiplicity the edge delivers.
    pub fn add_exchange_source(
        &mut self,
        node: NodeId,
        edge: EdgeId,
        imports: Box<[(ValueId, ValueId)]>,
        output: Box<[ValueId]>,
        distribution: crate::Distribution,
        row_multiplicity: crate::RowMultiplicity,
    ) -> Result<(), BuildError> {
        let imported = imports
            .iter()
            .map(|(_, destination)| *destination)
            .collect::<BTreeSet<_>>();
        for (_, destination) in &imports {
            if !self.values.contains_key(destination) {
                return Err(BuildError::UndefinedValue(*destination));
            }
        }
        for value in &output {
            if !imported.contains(value) {
                return Err(BuildError::ExchangeOutputWasNotImported {
                    node,
                    value: *value,
                });
            }
        }
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: crate::PhysicalProperties {
                distribution,
                row_multiplicity,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: output,
            },
            kind: NodeKind::ExchangeSource { edge, imports },
        })
    }

    /// Adds inline rows.
    ///
    /// Literal rows exist in one place and are not distributed, so the layout
    /// is not a caller decision. Every row must fill the output port exactly:
    /// a short or long row is a column that has no value or no home.
    pub fn add_values(
        &mut self,
        node: NodeId,
        rows: Box<[Box<[ExprId]>]>,
        output: Box<[ValueId]>,
    ) -> Result<(), BuildError> {
        for row in &rows {
            if row.len() != output.len() {
                return Err(BuildError::ValuesRowWidthMismatch {
                    node,
                    expected: output.len(),
                    actual: row.len(),
                });
            }
            for expression in row {
                self.require_owned_expression(node, *expression)?;
            }
        }
        for value in &output {
            if !self.values.contains_key(value) {
                return Err(BuildError::UndefinedValue(*value));
            }
        }
        self.insert_leaf(node, output, NodeKind::Values { rows })
    }

    /// Adds one immutable metadata snapshot at single-copy singleton placement.
    pub fn add_system_relation(
        &mut self,
        node: NodeId,
        output: Box<[ValueId]>,
        output_fields: std::sync::Arc<[arrow_schema::FieldRef]>,
        data: crate::SystemRelationData,
    ) -> Result<(), BuildError> {
        if output.len() != output_fields.len() {
            return Err(BuildError::SystemRelation(
                "system relation output width differs",
            ));
        }
        for (id, field) in output.iter().zip(output_fields.iter()) {
            let value = self.values.get(id).ok_or(BuildError::UndefinedValue(*id))?;
            if value.ty.data_type() != field.data_type()
                || value.ty.nullable() != field.is_nullable()
            {
                return Err(BuildError::SystemRelation(
                    "system relation field differs from its complete output type",
                ));
            }
        }
        data.validate_schema(&output_fields)
            .map_err(BuildError::SystemRelation)?;
        self.insert_leaf(
            node,
            output,
            NodeKind::SystemRelation {
                output_fields,
                data,
            },
        )
    }

    /// Adds a generated integer series.
    pub fn add_generate_series(
        &mut self,
        node: NodeId,
        start: ExprId,
        stop: ExprId,
        step: Option<ExprId>,
        value: ValueId,
    ) -> Result<(), BuildError> {
        for bound in [Some(start), Some(stop), step].into_iter().flatten() {
            self.require_owned_expression(node, bound)?;
        }
        if !self.values.contains_key(&value) {
            return Err(BuildError::UndefinedValue(value));
        }
        self.insert_leaf(
            node,
            Box::from([value]),
            NodeKind::GenerateSeries { start, stop, step },
        )
    }

    /// Adds a grouping-set expansion over `input`.
    ///
    /// Repeat re-emits each input row once per grouping set, so the values it
    /// passes through keep their identity and the values it rewrites do not.
    /// The surviving layout and ordering follow from exactly that split, which
    /// the builder computes from the input and output ports rather than taking
    /// on trust.
    pub fn add_repeat(
        &mut self,
        node: NodeId,
        input: NodeId,
        rollup_keys: Box<[ValueId]>,
        grouping_sets: Box<[Box<[ValueId]>]>,
        grouping_values: Box<[(ValueId, ValueId)]>,
        grouping_outputs: Box<[crate::GroupingOutput]>,
        output: Box<[ValueId]>,
    ) -> Result<(), BuildError> {
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        let input_properties = source.output_properties.clone();
        let passthrough = source
            .output
            .columns
            .iter()
            .copied()
            .zip(output.iter().copied())
            .filter(|(input_value, output_value)| input_value == output_value)
            .collect::<BTreeMap<_, _>>();
        let output_properties =
            crate::remap_properties_through_values(&input_properties, &passthrough);
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([crate::passthrough_requirement(&input_properties)]),
            output_properties,
            output: OutputPort {
                node,
                columns: output,
            },
            kind: NodeKind::Repeat {
                rollup_keys,
                grouping_sets,
                grouping_values,
                grouping_outputs,
            },
        })
    }

    /// Adds a join of `left` and `right`.
    ///
    /// A join result owns each logical row once, which is why a broadcast-build
    /// join is sound at all: the replicated side is consumed against a
    /// single-copy side that anchors ownership. Both sides replicated has no
    /// anchor, so the result would carry copies the contract then forbids
    /// downstream. Stating `SingleCopy` at the call site let that mistake be
    /// written; deriving it here means the anchor is checked instead.
    ///
    /// A join also establishes no ordering, so none is carried forward.
    pub fn add_join(
        &mut self,
        node: NodeId,
        sides: [NodeId; 2],
        required_inputs: Box<[crate::PhysicalProperties]>,
        output: Box<[ValueId]>,
        distribution: crate::Distribution,
        kind: NodeKind,
    ) -> Result<(), BuildError> {
        debug_assert!(
            matches!(
                kind,
                NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. }
            ),
            "add_join is for join kinds"
        );
        let mut single_copy = false;
        for input in sides {
            let source = self
                .nodes
                .get(&input)
                .ok_or(BuildError::UndefinedInput { node, input })?;
            single_copy |=
                source.output_properties.row_multiplicity == crate::RowMultiplicity::SingleCopy;
        }
        if !single_copy {
            return Err(BuildError::JoinWithoutOwnershipAnchor(node));
        }
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from(sides),
            required_inputs,
            output_properties: crate::PhysicalProperties {
                distribution,
                row_multiplicity: crate::RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: output,
            },
            kind,
        })
    }

    /// Adds a provider scan.
    ///
    /// A scan's properties are the relation's own: the provider stated what its
    /// read delivers, so restating it on the node is only a chance to disagree
    /// with it.
    pub fn add_scan(
        &mut self,
        node: NodeId,
        kind: NodeKind,
        output: Box<[ValueId]>,
    ) -> Result<(), BuildError> {
        let NodeKind::Scan { relation, .. } = &kind else {
            return Err(BuildError::WrongNodeKindForConstructor {
                node,
                expected: "Scan",
            });
        };
        let output_properties = relation.provided_properties().clone();
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties,
            output: OutputPort {
                node,
                columns: output,
            },
            kind,
        })
    }

    /// Adds an operator that consumes every logical row exactly once.
    ///
    /// A complete aggregate, a set operation, a writer and a table finish all
    /// share one requirement the contract states and each call site restated:
    /// they must not see execution copies, because each copy would be counted,
    /// combined or written again. They also establish no ordering. The caller
    /// supplies the layout the operator produces; the rest follows.
    pub fn add_row_consuming(
        &mut self,
        node: NodeId,
        inputs: Box<[NodeId]>,
        required_inputs: RequiredInputs,
        output_distribution: crate::Distribution,
        output: Box<[ValueId]>,
        kind: NodeKind,
    ) -> Result<(), BuildError> {
        let mut derived = Vec::with_capacity(inputs.len());
        for input in &inputs {
            let source = self.nodes.get(input).ok_or(BuildError::UndefinedInput {
                node,
                input: *input,
            })?;
            if source.output_properties.row_multiplicity != crate::RowMultiplicity::SingleCopy {
                return Err(BuildError::ReplicatedRowsConsumedOnce {
                    node,
                    input: *input,
                });
            }
            let distribution = match &required_inputs {
                RequiredInputs::Singleton => crate::Distribution::Singleton,
                RequiredInputs::AsProduced | RequiredInputs::Exact(_) => {
                    source.output_properties.distribution.clone()
                }
            };
            derived.push(crate::PhysicalProperties {
                distribution,
                row_multiplicity: crate::RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            });
        }
        let required = match required_inputs {
            RequiredInputs::Exact(exact) if exact.len() == inputs.len() => exact.into_vec(),
            RequiredInputs::Exact(exact) => {
                return Err(BuildError::RequirementCountMismatch {
                    node,
                    inputs: inputs.len(),
                    requirements: exact.len(),
                });
            }
            _ => derived,
        };
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs,
            required_inputs: required.into_boxed_slice(),
            output_properties: crate::PhysicalProperties {
                distribution: output_distribution,
                row_multiplicity: crate::RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: output,
            },
            kind,
        })
    }

    /// Adds an operator that adds columns to the rows it passes through.
    ///
    /// A window leaves layout, multiplicity and ordering exactly as it found
    /// them - it only widens rows - so all three are taken from the input
    /// rather than restated.
    pub fn add_row_widening(
        &mut self,
        node: NodeId,
        input: NodeId,
        output: Box<[ValueId]>,
        kind: NodeKind,
    ) -> Result<(), BuildError> {
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        let properties = source.output_properties.clone();
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([properties.clone()]),
            output_properties: properties,
            output: OutputPort {
                node,
                columns: output,
            },
            kind,
        })
    }

    /// Adds an operator that rewrites the rows it reads, keeping the values in
    /// `passthrough` and replacing the rest.
    ///
    /// Whatever layout or ordering the input had survives only for values that
    /// survive, which is precisely what `passthrough` records, so the builder
    /// remaps rather than taking the result on trust. Passing `None` states
    /// that nothing survives.
    pub fn add_row_rewriting(
        &mut self,
        node: NodeId,
        input: NodeId,
        passthrough: Option<&BTreeMap<ValueId, ValueId>>,
        output: Box<[ValueId]>,
        kind: NodeKind,
    ) -> Result<(), BuildError> {
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        let input_properties = source.output_properties.clone();
        let output_properties = match passthrough {
            Some(passthrough) => {
                crate::remap_properties_through_values(&input_properties, passthrough)
            }
            None => crate::PhysicalProperties {
                distribution: crate::Distribution::Unconstrained,
                row_multiplicity: input_properties.row_multiplicity,
                ordering: Box::default(),
            },
        };
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([crate::passthrough_requirement(&input_properties)]),
            output_properties,
            output: OutputPort {
                node,
                columns: output,
            },
            kind,
        })
    }

    /// Adds an operator that appends generated rows to each input row while
    /// leaving the input's own rows intact.
    ///
    /// A lateral table function keeps each input row's ordering and copies and
    /// only narrows the layout when the function is not replica-deterministic,
    /// which is a property of the function rather than a caller choice.
    pub fn add_row_expanding(
        &mut self,
        node: NodeId,
        input: NodeId,
        distribution: crate::Distribution,
        output: Box<[ValueId]>,
        kind: NodeKind,
    ) -> Result<(), BuildError> {
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        let input_properties = source.output_properties.clone();
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([crate::passthrough_requirement(&input_properties)]),
            output_properties: crate::PhysicalProperties {
                distribution,
                row_multiplicity: input_properties.row_multiplicity,
                ordering: input_properties.ordering.clone(),
            },
            output: OutputPort {
                node,
                columns: output,
            },
            kind,
        })
    }

    /// Inserts a node that produces rows without reading any.
    fn insert_leaf(
        &mut self,
        node: NodeId,
        output: Box<[ValueId]>,
        kind: NodeKind,
    ) -> Result<(), BuildError> {
        self.insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: crate::PhysicalProperties {
                distribution: crate::Distribution::Singleton,
                row_multiplicity: crate::RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: output,
            },
            kind,
        })
    }

    fn require_owned_expression(
        &self,
        node: NodeId,
        expression: ExprId,
    ) -> Result<&ExprNode, BuildError> {
        let found = self
            .expressions
            .get(expression)
            .ok_or(BuildError::UndefinedExpression(expression))?;
        if found.owner != node {
            return Err(BuildError::ExpressionOutsideOwner {
                expr: expression,
                owner: found.owner,
                node,
            });
        }
        Ok(found)
    }

    /// Output and input properties for an operator that orders rows it passes
    /// through.
    fn passthrough_ordering_properties(
        &self,
        node: NodeId,
        input: NodeId,
        mode: &crate::SortMode,
        ordering: Box<[crate::OrderingKey]>,
    ) -> Result<(crate::PhysicalProperties, crate::PhysicalProperties), BuildError> {
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        let input_properties = &source.output_properties;
        if matches!(mode, crate::SortMode::Global) {
            if input_properties.distribution != crate::Distribution::Singleton
                || input_properties.row_multiplicity != crate::RowMultiplicity::SingleCopy
            {
                return Err(BuildError::GlobalOrderOverManyStreams(node));
            }
            return Ok((
                crate::PhysicalProperties {
                    distribution: crate::Distribution::Singleton,
                    row_multiplicity: crate::RowMultiplicity::SingleCopy,
                    ordering,
                },
                crate::PhysicalProperties {
                    distribution: crate::Distribution::Singleton,
                    row_multiplicity: crate::RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                },
            ));
        }
        Ok((
            crate::PhysicalProperties {
                distribution: input_properties.distribution.clone(),
                row_multiplicity: input_properties.row_multiplicity,
                ordering,
            },
            crate::PhysicalProperties {
                distribution: input_properties.distribution.clone(),
                row_multiplicity: input_properties.row_multiplicity,
                ordering: Box::default(),
            },
        ))
    }

    /// Properties a node in this fragment produces.
    ///
    /// Callers that let the builder derive properties still need to read them
    /// back to describe the node to their own caller.
    pub fn node_output_properties(&self, node: NodeId) -> Option<&crate::PhysicalProperties> {
        self.nodes.get(&node).map(|node| &node.output_properties)
    }

    /// Inserts a fully-stated node without deriving or checking anything.
    ///
    /// The planner does not use this: every node family has a constructor that
    /// derives what the caller does not decide and refuses what it cannot mean.
    /// It remains reachable so tests can build the states those constructors
    /// exist to reject, which is the only way to prove they are rejected.
    pub fn insert_node_unchecked(&mut self, node: PhysicalNode) -> Result<(), BuildError> {
        let id = node.id;
        let next_node = self
            .next_node
            .and_then(|next| id.get().checked_add(1).map(|after| next.max(after)));
        match self.nodes.entry(id) {
            Entry::Vacant(entry) => {
                self.contains_scan |= matches!(node.kind, NodeKind::Scan { .. });
                entry.insert(node);
                self.next_node = next_node;
            }
            Entry::Occupied(_) => return Err(BuildError::DuplicateNode(id)),
        }
        Ok(())
    }

    pub fn attach_runtime_filter(&mut self, id: RuntimeFilterId) -> Result<(), BuildError> {
        if !self.runtime_filters.insert(id) {
            return Err(BuildError::DuplicateRuntimeFilter(id));
        }
        Ok(())
    }

    /// Finish a locally valid fragment definition.
    ///
    /// Cross-fragment imports, exchange properties, runtime-filter endpoints,
    /// and upstream provenance remain incomplete until `PlanBuilder::finish`
    /// derives and validates the exact cuts. This result is therefore a plan
    /// construction input, not an executable publication.
    pub fn finish_definition(
        self,
        root: NodeId,
        sink: FragmentSink,
        dop_domain: PipelineDopDomain,
        stage_placement: crate::StagePlacementPolicy,
    ) -> Result<Fragment, ValidationErrors> {
        let (mut parts, runtime_filters) =
            self.into_fragment_parts(root, sink, dop_domain, stage_placement);
        parts.runtime_filters = runtime_filters.into_iter().collect();
        let fragment = Fragment::from(parts);
        validate_fragment_definition(&fragment)?;
        Ok(fragment)
    }

    fn into_fragment_parts(
        self,
        root: NodeId,
        sink: FragmentSink,
        dop_domain: PipelineDopDomain,
        stage_placement: crate::StagePlacementPolicy,
    ) -> (FragmentParts, BTreeSet<RuntimeFilterId>) {
        let parts = FragmentParts {
            id: self.id,
            root,
            values: self.values,
            expressions: self.expressions,
            nodes: self.nodes,
            sink,
            summaries: self.summaries,
            dop_domain,
            stage_placement,
            // The author installs the exact ordered references before this
            // unpublished construction input reaches any validator.
            runtime_filters: Box::default(),
            call_requests: crate::FragmentCallRequests::unpublished_empty(self.id),
        };
        (parts, self.runtime_filters)
    }
}

pub struct PlanBuilder {
    constants: crate::ConstantPools,
    parameters: SemanticParameters,
    version: PlanVersionId,
    next_edge: u32,
    fragments: BTreeMap<FragmentId, Fragment>,
    edges: BTreeMap<EdgeId, Edge>,
    runtime_filters: BTreeMap<RuntimeFilterId, RuntimeFilter>,
    result_port: Option<ResultPort>,
    required: RequiredContracts,
    annotations: Vec<PlanAnnotation>,
}

impl PlanBuilder {
    pub fn new(version: PlanVersionId) -> Self {
        Self {
            constants: crate::ConstantPools::empty(),
            // An empty table supplies no semantic value or implicit flag.
            parameters: SemanticParameters::default(),
            version,
            next_edge: 0,
            fragments: BTreeMap::new(),
            edges: BTreeMap::new(),
            runtime_filters: BTreeMap::new(),
            result_port: None,
            required: RequiredContracts::default(),
            annotations: Vec::new(),
        }
    }

    pub fn constants(&self) -> &crate::ConstantPools {
        &self.constants
    }

    pub fn with_constant_pools(mut self, constants: crate::ConstantPools) -> Self {
        self.constants = constants;
        self
    }

    pub fn insert_constant_pool(
        &mut self,
        id: crate::ConstantPoolId,
        pool: crate::ConstantPool,
    ) -> Result<(), crate::ConstantReferenceError> {
        self.constants.insert(id, pool)
    }

    /// Install the authored parameter values consumed by exact expression refs.
    pub fn with_semantic_parameters(mut self, parameters: SemanticParameters) -> Self {
        self.parameters = parameters;
        self
    }

    pub fn with_required_contracts(mut self, required: RequiredContracts) -> Self {
        self.required = required;
        self
    }

    pub fn add_fragment(&mut self, fragment: Fragment) -> Result<(), BuildError> {
        let id = fragment.id();
        match self.fragments.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(fragment);
            }
            Entry::Occupied(_) => return Err(BuildError::DuplicateFragment(id)),
        }
        Ok(())
    }

    pub fn reserve_edge_id(&mut self) -> Result<EdgeId, BuildError> {
        let id = EdgeId::new(self.next_edge);
        self.next_edge = self
            .next_edge
            .checked_add(1)
            .ok_or(BuildError::IdentitySpaceExhausted("edge"))?;
        Ok(id)
    }

    pub fn add_edge(&mut self, edge: Edge) -> Result<(), BuildError> {
        let id = edge.id;
        let next_edge = self.next_edge.max(
            id.get()
                .checked_add(1)
                .ok_or(BuildError::IdentitySpaceExhausted("edge"))?,
        );
        match self.edges.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(edge);
                self.next_edge = next_edge;
            }
            Entry::Occupied(_) => return Err(BuildError::DuplicateEdge(id)),
        }
        Ok(())
    }

    pub fn add_runtime_filter(&mut self, filter: RuntimeFilter) -> Result<(), BuildError> {
        let id = filter.id;
        match self.runtime_filters.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(filter);
            }
            Entry::Occupied(_) => return Err(BuildError::DuplicateRuntimeFilter(id)),
        }
        Ok(())
    }

    pub fn set_result_port(&mut self, result: ResultPort) -> Result<(), BuildError> {
        if self.result_port.is_some() {
            return Err(BuildError::DuplicateResultPort);
        }
        self.result_port = Some(result);
        Ok(())
    }

    pub fn add_annotation(&mut self, annotation: PlanAnnotation) {
        self.annotations.push(annotation);
    }

    // Design: ADR-0153 (docs/adr/ADR-0153-completed-physical-plan-is-the-static-execution-authority.md)
    pub fn finish_observed(
        self,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<PhysicalPlan, PlanConstructionError> {
        let plan = PhysicalPlan::from(PhysicalPlanParts {
            constants: self.constants,
            parameters: self.parameters,
            version: self.version,
            fragments: self.fragments,
            edges: self.edges,
            runtime_filters: self.runtime_filters,
            result_port: self.result_port,
            required: self.required,
            annotations: self.annotations.into_boxed_slice(),
        });
        validate_plan_observed(&plan, control)?;
        Ok(plan)
    }
    pub fn finish(self) -> Result<PhysicalPlan, ValidationErrors> {
        if self.fragments.values().any(|fragment| {
            !fragment.call_requests().entries().is_empty()
                || fragment.expressions().iter().any(|(_, node)| {
                    matches!(
                        node.kind,
                        crate::ExprKind::FunctionCall { .. } | crate::ExprKind::WindowCall { .. }
                    )
                })
                || fragment
                    .expressions()
                    .iter()
                    .any(|(_, node)| matches!(node.kind, crate::ExprKind::Constant(_)))
                || fragment.nodes().values().any(|node| match &node.kind {
                    crate::NodeKind::Aggregate { calls, .. } => !calls.is_empty(),
                    crate::NodeKind::TopN {
                        reduction: crate::TopNReduction::GroupedStates { calls, .. },
                        ..
                    } => !calls.is_empty(),
                    crate::NodeKind::TableWriter { target } => {
                        !target.partial_aggregates.is_empty()
                    }
                    crate::NodeKind::TableFinish(finish) => !finish.final_aggregates.is_empty(),
                    crate::NodeKind::TableFunction { .. } => true,
                    _ => false,
                })
                || fragment.nodes().values().any(|node| {
                    let special = |constant: &crate::UnpivotConstant| {
                        crate::constants::collection_reference(constant).is_some()
                    };
                    match &node.kind {
                        crate::NodeKind::Unpivot { spec } => spec
                            .mappings
                            .iter()
                            .any(|mapping| mapping.constants.iter().any(special)),
                        crate::NodeKind::TableFinish(spec) => {
                            spec.grouped_unpivot.as_ref().is_some_and(|grouped| {
                                grouped
                                    .mappings
                                    .iter()
                                    .any(|mapping| mapping.constants.iter().any(special))
                            })
                        }
                        _ => false,
                    }
                })
        }) || !self.constants.entries().is_empty()
        {
            let mut errors = crate::validation::ValidationContext::new();
            errors.push(crate::ValidationError::new(
                "constants",
                "checked constants and original call requests require caller-observed plan publication",
            ));
            return Err(crate::ValidationErrors::from_collector(errors));
        }
        let plan = PhysicalPlan::from(PhysicalPlanParts {
            constants: self.constants,
            parameters: self.parameters,
            version: self.version,
            fragments: self.fragments,
            edges: self.edges,
            runtime_filters: self.runtime_filters,
            result_port: self.result_port,
            required: self.required,
            annotations: self.annotations.into_boxed_slice(),
        });
        validate_plan(&plan)?;
        Ok(plan)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BuildError {
    MissingLegacyMetadata(crate::MissingLegacyBindingMetadata),
    IdentitySpaceExhausted(&'static str),
    DuplicateFragment(FragmentId),
    DuplicateNode(NodeId),
    DuplicateValue(ValueId),
    DuplicateExpression(ExprId),
    DuplicateEdge(EdgeId),
    DuplicateRuntimeFilter(RuntimeFilterId),
    DuplicateResultPort,
    /// A node names an input that has not been inserted yet. Fragments are
    /// built bottom up, so this is always an ordering mistake.
    UndefinedInput {
        node: NodeId,
        input: NodeId,
    },
    UndefinedExpression(ExprId),
    UndefinedValue(ValueId),
    /// Expressions belong to exactly one node's evaluation scope, so using one
    /// under a different node is a scope violation rather than a type error.
    ExpressionOutsideOwner {
        expr: ExprId,
        owner: NodeId,
        node: NodeId,
    },
    PredicateIsNotBoolean(ExprId),
    FilterWithoutPredicate(NodeId),
    /// A global limit was built over rows that are not one single-copy stream,
    /// where "the first N rows" has no single meaning.
    LimitInputIsNotGlobal(NodeId),
    /// A cardinality assertion was built over replicated rows, where counting
    /// them counts execution copies rather than logical rows.
    AssertionOverReplicatedRows(NodeId),
    OrderingWithoutKeys(NodeId),
    /// A sort key reads a computation rather than a value, so no downstream
    /// operator could rely on the ordering it claims.
    /// A global order was built over rows spread across more than one stream,
    /// where "the first N in order" has no single meaning.
    GlobalOrderOverManyStreams(NodeId),
    /// An exchange source offered a value it did not import, which no sender
    /// could have produced.
    ExchangeOutputWasNotImported {
        node: NodeId,
        value: ValueId,
    },
    /// An inline row does not fill the output port, leaving a column with no
    /// value or a value with no column.
    SystemRelation(&'static str),
    ValuesRowWidthMismatch {
        node: NodeId,
        expected: usize,
        actual: usize,
    },
    /// Both join inputs are replicated, so nothing anchors ownership of a
    /// logical row and the result would carry execution copies.
    JoinWithoutOwnershipAnchor(NodeId),
    /// An operator that consumes each logical row once was given replicated
    /// rows, where every execution copy would be counted or written again.
    ReplicatedRowsConsumedOnce {
        node: NodeId,
        input: NodeId,
    },
    WrongNodeKindForConstructor {
        node: NodeId,
        expected: &'static str,
    },
    RequirementCountMismatch {
        node: NodeId,
        inputs: usize,
        requirements: usize,
    },
}

/// What an operator needs of its inputs' layout.
///
/// `Singleton` and `AsProduced` are derived per input, so the caller states an
/// intent rather than a value. `Exact` exists for operators whose requirement
/// depends on a choice the planner made - a join's build side, a set
/// operation's strategy - which the contract cannot re-derive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RequiredInputs {
    /// All rows on one lane.
    Singleton,
    /// Whatever each input already produces, unchanged.
    AsProduced,
    /// Exactly these, one per input.
    Exact(Box<[crate::PhysicalProperties]>),
}

impl fmt::Display for BuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingLegacyMetadata(error) => error.fmt(formatter),
            Self::IdentitySpaceExhausted(kind) => {
                write!(formatter, "{kind} identity space exhausted")
            }
            Self::DuplicateFragment(id) => write!(formatter, "duplicate fragment {}", id.get()),
            Self::DuplicateNode(id) => write!(formatter, "duplicate node {}", id.get()),
            Self::DuplicateValue(id) => write!(formatter, "duplicate value {}", id.get()),
            Self::DuplicateExpression(id) => write!(formatter, "duplicate expression {}", id.get()),
            Self::DuplicateEdge(id) => write!(formatter, "duplicate edge {}", id.get()),
            Self::DuplicateRuntimeFilter(id) => {
                write!(formatter, "duplicate runtime filter {}", id.get())
            }
            Self::UndefinedInput { node, input } => write!(
                formatter,
                "node {} names undefined input {}",
                node.get(),
                input.get()
            ),
            Self::UndefinedExpression(id) => {
                write!(formatter, "expression {} is not defined", id.get())
            }
            Self::UndefinedValue(id) => write!(formatter, "value {} is not defined", id.get()),
            Self::ExpressionOutsideOwner { expr, owner, node } => write!(
                formatter,
                "expression {} belongs to node {}, not node {}",
                expr.get(),
                owner.get(),
                node.get()
            ),
            Self::PredicateIsNotBoolean(id) => {
                write!(
                    formatter,
                    "predicate expression {} is not boolean",
                    id.get()
                )
            }
            Self::FilterWithoutPredicate(id) => {
                write!(formatter, "filter node {} has no predicate", id.get())
            }
            Self::LimitInputIsNotGlobal(id) => write!(
                formatter,
                "limit node {} requires one single-copy singleton input",
                id.get()
            ),
            Self::AssertionOverReplicatedRows(id) => write!(
                formatter,
                "assertion node {} cannot count replicated rows",
                id.get()
            ),
            Self::OrderingWithoutKeys(id) => {
                write!(formatter, "ordering node {} has no sort key", id.get())
            }
            Self::GlobalOrderOverManyStreams(id) => write!(
                formatter,
                "node {} requires one single-copy singleton input for a global order",
                id.get()
            ),
            Self::ExchangeOutputWasNotImported { node, value } => write!(
                formatter,
                "exchange source {} offers value {} without importing it",
                node.get(),
                value.get()
            ),
            Self::SystemRelation(message) => formatter.write_str(message),
            Self::ValuesRowWidthMismatch {
                node,
                expected,
                actual,
            } => write!(
                formatter,
                "values node {} has a row of {actual} expressions for {expected} columns",
                node.get()
            ),
            Self::JoinWithoutOwnershipAnchor(id) => write!(
                formatter,
                "join {} has no single-copy input to anchor row ownership",
                id.get()
            ),
            Self::ReplicatedRowsConsumedOnce { node, input } => write!(
                formatter,
                "node {} consumes each row once but input {} is replicated",
                node.get(),
                input.get()
            ),
            Self::WrongNodeKindForConstructor { node, expected } => write!(
                formatter,
                "node {} was built with the {expected} constructor for another kind",
                node.get()
            ),
            Self::RequirementCountMismatch {
                node,
                inputs,
                requirements,
            } => write!(
                formatter,
                "node {} has {inputs} inputs but {requirements} input requirements",
                node.get()
            ),
            Self::DuplicateResultPort => formatter.write_str("result port is already set"),
        }
    }
}

impl std::error::Error for BuildError {}

#[cfg(test)]
mod sparse_node_identity_tests {
    use super::*;

    fn add_empty_values(builder: &mut FragmentBuilder, id: u32) {
        builder
            .add_values(
                NodeId::new(id),
                Box::from([Box::<[ExprId]>::default()]),
                Box::default(),
            )
            .unwrap();
    }

    #[test]
    fn explicit_max_node_is_valid_and_only_later_reservation_is_exhausted() {
        let mut builder = FragmentBuilder::new(FragmentId::new(7));
        add_empty_values(&mut builder, u32::MAX);
        for _ in 0..2 {
            assert!(matches!(
                builder.reserve_node_id(),
                Err(BuildError::IdentitySpaceExhausted("node"))
            ));
        }
        let fragment = builder
            .finish_definition(
                NodeId::new(u32::MAX),
                FragmentSink::Noop,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap();
        assert_eq!(fragment.root().get(), u32::MAX);
        assert_eq!(fragment.nodes().len(), 1);
    }

    #[test]
    fn monotone_reservation_returns_both_last_legal_ids_without_wraparound() {
        let mut builder = FragmentBuilder::new(FragmentId::new(7));
        add_empty_values(&mut builder, u32::MAX - 2);
        assert_eq!(builder.reserve_node_id().unwrap().get(), u32::MAX - 1);
        assert_eq!(builder.reserve_node_id().unwrap().get(), u32::MAX);
        assert!(matches!(
            builder.reserve_node_id(),
            Err(BuildError::IdentitySpaceExhausted("node"))
        ));
        // Explicit sparse insertion does not reopen automatic allocation.
        add_empty_values(&mut builder, 0);
        assert!(matches!(
            builder.reserve_node_id(),
            Err(BuildError::IdentitySpaceExhausted("node"))
        ));
    }

    #[test]
    fn duplicate_max_node_reports_duplicate_and_keeps_the_original_node() {
        let mut builder = FragmentBuilder::new(FragmentId::new(7));
        add_empty_values(&mut builder, u32::MAX);
        assert!(matches!(
            builder.add_values(NodeId::new(u32::MAX), Box::default(), Box::default()),
            Err(BuildError::DuplicateNode(id)) if id.get() == u32::MAX
        ));
        let fragment = builder
            .finish_definition(
                NodeId::new(u32::MAX),
                FragmentSink::Noop,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap();
        let NodeKind::Values { rows } = &fragment.nodes()[&fragment.root()].kind else {
            panic!("original node was replaced");
        };
        assert_eq!(rows.len(), 1);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanConstructionError {
    Requests(crate::CallRequestError),
    Constants(crate::ConstantReferenceError),
    Structure(crate::ValidationErrors),
}
impl From<crate::CallRequestError> for PlanConstructionError {
    fn from(error: crate::CallRequestError) -> Self {
        match error {
            crate::CallRequestError::Control(cause) => {
                Self::Constants(crate::ConstantReferenceError::Control(cause))
            }
            other => Self::Requests(other),
        }
    }
}
impl From<crate::ConstantReferenceError> for PlanConstructionError {
    fn from(error: crate::ConstantReferenceError) -> Self {
        Self::Constants(error)
    }
}
impl From<crate::ValidationErrors> for PlanConstructionError {
    fn from(error: crate::ValidationErrors) -> Self {
        Self::Structure(error)
    }
}
impl fmt::Display for PlanConstructionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Requests(error) => error.fmt(formatter),
            Self::Constants(error) => error.fmt(formatter),
            Self::Structure(error) => error.fmt(formatter),
        }
    }
}
impl std::error::Error for PlanConstructionError {}

/// Publish/consume a complete source using the caller's original observation.
/// The unobserved structural validator alone does not admit checked sources.
pub fn validate_plan_observed(
    plan: &PhysicalPlan,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(), PlanConstructionError> {
    for fragment in plan.fragments().values() {
        fragment
            .call_requests()
            .validate_fragment(fragment, control)
            .map_err(PlanConstructionError::from)?;
    }
    crate::constants::validate_plan_constants_observed(plan, control)?;
    // The shared wait graph observes bounded work using this original scope.
    // Other structural passes retain their opaque legacy boundary; neither
    // observation nor the plan's shape grants scratch allocation capacity.
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::Validate,
    )
    .map_err(crate::ConstantReferenceError::from)?;
    let result =
        crate::validation::validate_plan_with_limits_in(plan, crate::PlanLimits::FROZEN, &mut work);
    if result.is_ok() {
        work.step().map_err(crate::ConstantReferenceError::from)?;
    }
    work.finish_result(result, |cause| {
        crate::ConstantReferenceError::from(cause).into()
    })
}
