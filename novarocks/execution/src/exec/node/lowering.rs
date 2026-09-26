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

//! Consume a transient decoder plan into one bounded, task-independent graph.
//! A scan recipe is supplied by the provider compiler; the old ScanSource is
//! never inspected to guess a relation or retained by the program.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use novarocks_local_program as lp;

use super::{ExecNode, ExecNodeKind, ExecPlan};
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::ExprId;
use crate::exec::expr::static_program::freeze_field_schema;
use crate::exec::node::runtime_filter::RuntimeFilterConsumerBinding;
use crate::runtime_filter as rf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalProgramLoweringError(String);

impl LocalProgramLoweringError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for LocalProgramLoweringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LocalProgramLoweringError {}

type Result<T> = std::result::Result<T, LocalProgramLoweringError>;

/// Only sink edges outside the root tree may be supplied by the fragment
/// envelope. All node-owned requirements are derived from the frozen nodes.
pub enum ExternalSinkRequirement {
    Result,
    ExchangeOutput { branch: usize },
}

/// The exact runtime capabilities removed while a decoded plan is frozen.
/// They are keyed by local node identity and never enter LocalProgram.
pub struct LocalRuntimeBindings {
    pub(crate) scans: BTreeMap<lp::ProgramNodeId, Arc<dyn super::scan::ScanSource>>,
    pub(crate) writers: BTreeMap<lp::ProgramNodeId, super::table_writer::TableWriterRuntimeBinding>,
    pub(crate) finishers:
        BTreeMap<lp::ProgramNodeId, super::table_finish::TableFinishRuntimeBinding>,
}

impl LocalRuntimeBindings {
    fn new() -> Self {
        Self {
            scans: BTreeMap::new(),
            writers: BTreeMap::new(),
            finishers: BTreeMap::new(),
        }
    }

    pub fn scan_count(&self) -> usize {
        self.scans.len()
    }

    pub fn writer_count(&self) -> usize {
        self.writers.len()
    }

    pub fn finish_count(&self) -> usize {
        self.finishers.len()
    }
}

impl ExecPlan {
    /// Derive a specialization profile from the decoded root's exact slot
    /// metadata and the effective pipeline width for this task.
    pub fn local_compile_profile(
        &self,
        pipeline_dop: NonZeroUsize,
        root_sink_dop: Option<NonZeroUsize>,
    ) -> Result<lp::CompileProfile> {
        let schema = crate::exec::pipeline::builder::output_chunk_schema_for_node(&self.root)
            .ok_or_else(|| LocalProgramLoweringError::new("root has no output chunk schema"))?;
        let identity = layout(&schema)?
            .identity()
            .map_err(|error| LocalProgramLoweringError::new(error.to_string()))?;
        Ok(lp::CompileProfile::new(
            pipeline_dop,
            root_sink_dop,
            identity,
            lp::KernelAbiVersion::CURRENT,
        ))
    }

    #[cfg(test)]
    pub fn into_local_program(
        self,
        profile: lp::CompileProfile,
        scan_sources: BTreeMap<i32, lp::StaticConnectorScan>,
        sink_requirements: Vec<ExternalSinkRequirement>,
    ) -> Result<lp::LocalProgram> {
        self.lower_with_optional_sink(profile, scan_sources, sink_requirements, None)
            .map(|(program, _runtime)| program)
    }

    /// Freeze a temporary decoder tree and return its runtime capabilities as
    /// a separate Task binding. Production callers must retain the second
    /// result until the instance has either started or failed closed.
    pub fn into_local_program_and_bindings(
        self,
        profile: lp::CompileProfile,
        scan_sources: BTreeMap<i32, lp::StaticConnectorScan>,
        sink_requirements: Vec<ExternalSinkRequirement>,
        sink: lp::StaticSinkProgram,
    ) -> Result<(lp::LocalProgram, LocalRuntimeBindings)> {
        self.lower_with_optional_sink(profile, scan_sources, sink_requirements, Some(sink))
    }

    fn lower_with_optional_sink(
        self,
        profile: lp::CompileProfile,
        mut scan_sources: BTreeMap<i32, lp::StaticConnectorScan>,
        sink_requirements: Vec<ExternalSinkRequirement>,
        sink: Option<lp::StaticSinkProgram>,
    ) -> Result<(lp::LocalProgram, LocalRuntimeBindings)> {
        preflight(&self.root)?;
        let ExecPlan { arena, root } = self;
        let expressions = Arc::new(arena.into_immutable().map_err(|error| {
            LocalProgramLoweringError::new(format!("freeze local expressions: {error}"))
        })?);
        let mut lowering = Lowering {
            nodes: Vec::new(),
            requirements: Vec::new(),
            filter_bindings: BTreeSet::new(),
            scan_sources: &mut scan_sources,
            runtime: LocalRuntimeBindings::new(),
        };
        let root = lowering.node(root)?;
        if !lowering.scan_sources.is_empty() {
            return Err(LocalProgramLoweringError::new(format!(
                "unused static scan recipes for node IDs {:?}",
                lowering.scan_sources.keys().collect::<Vec<_>>()
            )));
        }
        let layout = lowering.nodes[root.index()].output_layout().clone();
        for sink_requirement in sink_requirements {
            lowering.requirements.push(match sink_requirement {
                ExternalSinkRequirement::Result => lp::BindingRequirement::ResultSink {
                    layout: layout.clone(),
                },
                ExternalSinkRequirement::ExchangeOutput { branch } => {
                    let branch_layout = if let Some(sink) = &sink {
                        let branch_program = sink.branches().get(branch).ok_or_else(|| {
                            LocalProgramLoweringError::new(
                                "exchange output branch has no static sink branch",
                            )
                        })?;
                        layout
                            .project_by_slots(branch_program.output_columns())
                            .map_err(|error| {
                                LocalProgramLoweringError::new(format!(
                                    "{error}: branch {branch} output slots {:?}, root output slots {:?}",
                                    branch_program.output_columns(),
                                    layout.slots()
                                ))
                            })?
                    } else {
                        layout.clone()
                    };
                    lp::BindingRequirement::ExchangeOutput {
                        branch,
                        layout: branch_layout,
                    }
                }
            });
        }
        let requirements = lp::BindingRequirements::try_new(lowering.requirements)
            .map_err(|error| LocalProgramLoweringError::new(error.to_string()))?;
        let program = lp::LocalProgram::try_new_with_sink(
            lowering.nodes,
            root,
            expressions,
            profile,
            requirements,
            sink,
        )
        .map_err(|error| LocalProgramLoweringError::new(error.to_string()))?;
        Ok((program, lowering.runtime))
    }
}

/// Bound input before a recursive consume. This keeps malformed hand-built
/// trees from overflowing the call stack or allocating a giant graph.
fn preflight(root: &ExecNode) -> Result<()> {
    let mut pending = vec![(root, 1usize)];
    let mut count = 0usize;
    while let Some((node, depth)) = pending.pop() {
        count += 1;
        if count > lp::MAX_PROGRAM_NODES || depth > lp::MAX_PROGRAM_NODE_DEPTH {
            return Err(LocalProgramLoweringError::new(
                "local plan exceeds node/depth limit",
            ));
        }
        macro_rules! push {
            ($child:expr) => {
                pending.push(($child, depth + 1))
            };
        }
        match &node.kind {
            ExecNodeKind::AssertNumRows(n) => push!(&n.input),
            ExecNodeKind::Project(n) => push!(&n.input),
            ExecNodeKind::Unpivot(n) => push!(&n.input),
            ExecNodeKind::Filter(n) => push!(&n.input),
            ExecNodeKind::Repeat(n) => push!(&n.input),
            ExecNodeKind::ChangeEventExpand(n) => push!(&n.input),
            ExecNodeKind::Limit(n) => push!(&n.input),
            ExecNodeKind::Aggregate(n) => push!(&n.input),
            ExecNodeKind::Sort(n) => push!(&n.input),
            ExecNodeKind::TableFunction(n) => push!(&n.input),
            ExecNodeKind::Analytic(n) => push!(&n.input),
            ExecNodeKind::RuntimeFilterConsumer(n) => push!(&n.input),
            ExecNodeKind::TableWriter(n) => push!(&n.input),
            ExecNodeKind::UnionAll(n) => {
                pending.extend(n.inputs.iter().map(|child| (child, depth + 1)))
            }
            ExecNodeKind::SetOp(n) => {
                pending.extend(n.inputs.iter().map(|child| (child, depth + 1)))
            }
            ExecNodeKind::TableFinish(n) => {
                pending.extend(n.inputs.iter().map(|child| (child, depth + 1)))
            }
            ExecNodeKind::Join(n) => {
                push!(&n.left);
                push!(&n.right);
            }
            ExecNodeKind::NestedLoopJoin(n) => {
                push!(&n.left);
                push!(&n.right);
            }
            ExecNodeKind::Values(_) | ExecNodeKind::ExchangeSource(_) | ExecNodeKind::Scan(_) => {}
        }
    }
    Ok(())
}

struct Lowering<'a> {
    nodes: Vec<lp::ProgramNode>,
    requirements: Vec<lp::BindingRequirement>,
    filter_bindings: BTreeSet<i32>,
    scan_sources: &'a mut BTreeMap<i32, lp::StaticConnectorScan>,
    runtime: LocalRuntimeBindings,
}

impl Lowering<'_> {
    fn node(&mut self, node: ExecNode) -> Result<lp::ProgramNodeId> {
        let schema = crate::exec::pipeline::builder::output_chunk_schema_for_node(&node)
            .ok_or_else(|| LocalProgramLoweringError::new("node has no output chunk schema"))?;
        let output = layout(&schema)?;
        let (node_id, kind) = self.kind(node.kind, &output)?;
        let id = lp::ProgramNodeId::new(self.nodes.len());
        self.nodes.push(lp::ProgramNode::new(node_id, kind, output));
        Ok(id)
    }

    fn filter_requirement(&mut self, id: u32) -> Result<()> {
        let id = i32::try_from(id).map_err(|_| {
            LocalProgramLoweringError::new("runtime filter binding ID overflows i32")
        })?;
        if self.filter_bindings.insert(id) {
            self.requirements
                .push(lp::BindingRequirement::RuntimeFilter { binding_id: id });
        }
        Ok(())
    }

    fn consumers(
        &mut self,
        bindings: Vec<RuntimeFilterConsumerBinding>,
    ) -> Result<Vec<lp::FilterConsumerAtExpr>> {
        bindings
            .into_iter()
            .map(|binding| {
                self.filter_requirement(binding.binding_id())?;
                let consumer = freeze_consumer(&binding)?;
                Ok(lp::FilterConsumerAtExpr {
                    expr_id: expr(binding.expr_id),
                    consumer,
                })
            })
            .collect()
    }

    fn producers(
        &mut self,
        bindings: impl IntoIterator<Item = (ExprId, usize, rf::RuntimeFilterProducerContract)>,
    ) -> Result<Vec<lp::FilterProducerAtExpr>> {
        bindings
            .into_iter()
            .map(|(expr_id, key_ordinal, contract)| {
                self.filter_requirement(contract.binding_id().get())?;
                Ok(lp::FilterProducerAtExpr {
                    expr_id: expr(expr_id),
                    key_ordinal,
                    producer: freeze_producer(&contract)?,
                })
            })
            .collect()
    }

    fn kind(
        &mut self,
        node: ExecNodeKind,
        output: &lp::StaticLayout,
    ) -> Result<(i32, lp::ProgramNodeKind)> {
        use lp::ProgramNodeKind as P;
        let mapped = match node {
            ExecNodeKind::AssertNumRows(n) => {
                let input = self.node(*n.input)?;
                let mode = match n.mode {
                    super::assert::AssertNumRowsMode::Global {
                        desired_num_rows,
                        assertion,
                        subquery_string,
                    } => lp::AssertRowsMode::Global {
                        desired_num_rows,
                        assertion: match assertion {
                            super::assert::Assertion::Eq => lp::RowAssertion::Eq,
                            super::assert::Assertion::Ne => lp::RowAssertion::Ne,
                            super::assert::Assertion::Lt => lp::RowAssertion::Lt,
                            super::assert::Assertion::Le => lp::RowAssertion::Le,
                            super::assert::Assertion::Gt => lp::RowAssertion::Gt,
                            super::assert::Assertion::Ge => lp::RowAssertion::Ge,
                        },
                        subquery_string: subquery_string.map(Arc::from),
                    },
                    super::assert::AssertNumRowsMode::PerKeyAtMostOne {
                        key_slots,
                        key_labels,
                        message_prefix,
                    } => lp::AssertRowsMode::PerKeyAtMostOne {
                        key_slots,
                        key_labels: key_labels.into_iter().map(Arc::from).collect(),
                        message_prefix: Arc::from(message_prefix),
                    },
                };
                (n.node_id, P::AssertNumRows { input, mode })
            }
            ExecNodeKind::Values(n) => {
                let values = lp::StaticValues::try_new(n.chunk.batch, output.clone())
                    .map_err(|error| LocalProgramLoweringError::new(error.to_string()))?;
                (n.node_id, P::Values { values })
            }
            ExecNodeKind::Project(n) => {
                let input = self.node(*n.input)?;
                let expr_slot_schemas = n.expr_slot_schemas.map(|slots| {
                    slots
                        .into_iter()
                        .map(|slot| lp::ProjectExpressionSlot {
                            slot_id: slot.slot_id(),
                            field: slot.field().clone(),
                            field_schema: freeze_field_schema(slot.field_schema().clone()),
                            unique_id: slot.unique_id(),
                        })
                        .collect()
                });
                (
                    n.node_id,
                    P::Project {
                        input,
                        is_subordinate: n.is_subordinate,
                        exprs: n.exprs.into_iter().map(expr).collect(),
                        expr_slot_ids: n.expr_slot_ids,
                        expr_slot_schemas,
                        output_indices: n.output_indices,
                    },
                )
            }
            ExecNodeKind::Unpivot(n) => {
                let input = self.node(*n.input)?;
                (
                    n.node_id,
                    P::Unpivot {
                        input,
                        passthrough_columns: n
                            .passthrough_columns
                            .into_iter()
                            .map(|col| lp::UnpivotPassthrough {
                                input_slot_id: col.input_slot_id,
                                output_slot_id: col.output_slot_id,
                            })
                            .collect(),
                        value_output_slot_id: n.value_output_slot_id,
                        literal_output_slot_ids: n.literal_output_slot_ids,
                        value_mappings: n
                            .value_mappings
                            .into_iter()
                            .map(|mapping| lp::UnpivotMapping {
                                input_value_slot_id: mapping.input_value_slot_id,
                                constants: mapping
                                    .constants
                                    .into_iter()
                                    .map(freeze_unpivot_constant)
                                    .collect(),
                            })
                            .collect(),
                        max_output_rows: n.max_output_rows,
                        max_output_bytes: n.max_output_bytes,
                    },
                )
            }
            ExecNodeKind::Filter(n) => {
                let input = self.node(*n.input)?;
                (
                    n.node_id,
                    P::Filter {
                        input,
                        predicate: expr(n.predicate),
                    },
                )
            }
            ExecNodeKind::Repeat(n) => {
                let input = self.node(*n.input)?;
                (
                    n.node_id,
                    P::Repeat {
                        input,
                        null_slot_ids: n.null_slot_ids,
                        grouping_slot_ids: n.grouping_slot_ids,
                        grouping_list: n.grouping_list,
                        repeat_times: n.repeat_times,
                    },
                )
            }
            ExecNodeKind::ChangeEventExpand(n) => {
                let input = self.node(*n.input)?;
                (
                    n.node_id,
                    P::ChangeEventExpand {
                        input,
                        events: n
                            .events
                            .into_iter()
                            .map(|event| lp::ChangeEventSpec {
                                predicate: event.predicate.map(expr),
                                effect: event.effect,
                                assignments: event
                                    .assignments
                                    .into_iter()
                                    .map(|assignment| lp::ChangeEventOutputExpr {
                                        output_slot_id: assignment.output_slot_id,
                                        expr: assignment.expr.map(expr),
                                    })
                                    .collect(),
                            })
                            .collect(),
                        output_slot_ids: n.output_slot_ids,
                        effect_slot_id: n.effect_slot_id,
                    },
                )
            }
            ExecNodeKind::UnionAll(n) => (
                n.node_id,
                P::UnionAll {
                    inputs: n
                        .inputs
                        .into_iter()
                        .map(|input| self.node(input))
                        .collect::<Result<_>>()?,
                },
            ),
            ExecNodeKind::Limit(n) => {
                let input = self.node(*n.input)?;
                (
                    n.node_id,
                    P::Limit {
                        input,
                        limit: n.limit,
                        offset: n.offset,
                    },
                )
            }
            other => return self.kind_remaining(other, output),
        };
        Ok(mapped)
    }

    fn kind_remaining(
        &mut self,
        node: ExecNodeKind,
        output: &lp::StaticLayout,
    ) -> Result<(i32, lp::ProgramNodeKind)> {
        use lp::ProgramNodeKind as P;
        let mapped = match node {
            ExecNodeKind::Scan(n) => {
                let (
                    runtime_source,
                    (node_id, filter_specs, conjunct_predicate, io_tasks, limit, accept_empty),
                ) = n.into_static_fields_with_source();
                let node_id = node_id
                    .ok_or_else(|| LocalProgramLoweringError::new("scan has no native node ID"))?;
                let source = self.scan_sources.remove(&node_id).ok_or_else(|| {
                    LocalProgramLoweringError::new(format!(
                        "missing static scan recipe for node {node_id}"
                    ))
                })?;
                let relation = source.recipe().draft().relation().table().header().clone();
                let id = lp::ProgramNodeId::new(self.nodes.len());
                self.runtime.scans.insert(id, runtime_source);
                self.requirements.push(lp::BindingRequirement::Scan {
                    node: id,
                    kind: lp::ScanSourceKind::TypedConnector { relation },
                    layout: output.clone(),
                });
                let runtime_filters = self.consumers(filter_specs)?;
                (
                    node_id,
                    P::Scan {
                        source,
                        runtime_filters,
                        conjunct_predicate: conjunct_predicate.map(expr),
                        connector_io_tasks_per_scan_operator: io_tasks,
                        limit,
                        accept_empty_scan_ranges: accept_empty,
                    },
                )
            }
            ExecNodeKind::ExchangeSource(n) => {
                let id = lp::ProgramNodeId::new(self.nodes.len());
                self.requirements
                    .push(lp::BindingRequirement::ExchangeInput {
                        node: id,
                        layout: output.clone(),
                    });
                let runtime_filters = self.consumers(n.native_runtime_filter_specs)?;
                (
                    n.node_id,
                    P::ExchangeSource {
                        timeout: n.timeout,
                        runtime_filters,
                        hash_partition_exprs: n
                            .hash_partition_exprs
                            .into_iter()
                            .map(expr)
                            .collect(),
                    },
                )
            }
            ExecNodeKind::Aggregate(n) => {
                let input = self.node(*n.input)?;
                if n.functions.len() != n.resolved_aggregates.len() {
                    return Err(LocalProgramLoweringError::new(
                        "aggregate calls and resolved signatures differ",
                    ));
                }
                let functions = n
                    .functions
                    .into_iter()
                    .zip(n.resolved_aggregates)
                    .map(|(function, resolved)| lp::StaticAggregateCall {
                        name: Arc::from(function.name),
                        inputs: function.inputs.into_iter().map(expr).collect(),
                        input_is_intermediate: function.input_is_intermediate,
                        types: function
                            .types
                            .map(|types| lp::StaticAggregateTypeSignature {
                                intermediate_type: types.intermediate_type,
                                output_type: types.output_type,
                                input_arg_type: types.input_arg_type,
                            }),
                        order: lp::StaticAggregateOrder {
                            is_asc_order: function.order.is_asc_order,
                            nulls_first: function.order.nulls_first,
                            is_distinct: function.order.is_distinct,
                            group_concat_max_len: function.order.group_concat_max_len,
                        },
                        resolved,
                    })
                    .collect();
                let topn_filters = n
                    .runtime_filter_spec
                    .topn_producers
                    .into_iter()
                    .map(|producer| {
                        self.filter_requirement(producer.binding_id())?;
                        Ok(lp::AggregateTopNFilter {
                            group_key_expr: expr(producer.group_key_expr_id),
                            group_key_ordinal: producer.group_key_ordinal,
                            limit: producer.limit,
                            producer: freeze_producer(&producer.contract)?,
                        })
                    })
                    .collect::<Result<_>>()?;
                (n.node_id, P::Aggregate { input, group_by: n.group_by.into_iter().map(expr).collect(), functions,
                    need_finalize: n.need_finalize, input_is_intermediate: n.input_is_intermediate, topn_filters,
                    streaming_preaggregation_mode: n.streaming_preaggregation_mode.map(|mode| match mode {
                        super::aggregate::StreamingPreaggregationMode::Auto => lp::StreamingPreaggregationMode::Auto,
                        super::aggregate::StreamingPreaggregationMode::ForceStreaming => lp::StreamingPreaggregationMode::ForceStreaming,
                        super::aggregate::StreamingPreaggregationMode::ForcePreaggregation => lp::StreamingPreaggregationMode::ForcePreaggregation,
                        super::aggregate::StreamingPreaggregationMode::LimitedMem => lp::StreamingPreaggregationMode::LimitedMem,
                    }) })
            }
            ExecNodeKind::Join(n) => {
                let left = self.node(*n.left)?;
                let right = self.node(*n.right)?;
                let runtime_filters =
                    self.producers(n.runtime_filter_execution.producers.into_iter().map(
                        |producer| {
                            (
                                producer.build_expr_id,
                                producer.build_key_index,
                                producer.contract,
                            )
                        },
                    ))?;
                (
                    n.node_id,
                    P::Join {
                        left,
                        right,
                        join_type: match n.join_type {
                            super::join::JoinType::Inner => lp::JoinType::Inner,
                            super::join::JoinType::LeftOuter => lp::JoinType::LeftOuter,
                            super::join::JoinType::RightOuter => lp::JoinType::RightOuter,
                            super::join::JoinType::FullOuter => lp::JoinType::FullOuter,
                            super::join::JoinType::LeftSemi => lp::JoinType::LeftSemi,
                            super::join::JoinType::RightSemi => lp::JoinType::RightSemi,
                            super::join::JoinType::LeftAnti => lp::JoinType::LeftAnti,
                            super::join::JoinType::RightAnti => lp::JoinType::RightAnti,
                            super::join::JoinType::NullAwareLeftAnti => {
                                lp::JoinType::NullAwareLeftAnti
                            }
                        },
                        distribution_mode: match n.distribution_mode {
                            super::join::JoinDistributionMode::Broadcast => {
                                lp::JoinDistributionMode::Broadcast
                            }
                            super::join::JoinDistributionMode::Partitioned => {
                                lp::JoinDistributionMode::Partitioned
                            }
                        },
                        left_layout: layout(&n.left_chunk_schema)?,
                        right_layout: layout(&n.right_chunk_schema)?,
                        join_scope_layout: layout(&n.join_scope_chunk_schema)?,
                        probe_keys: n.probe_keys.into_iter().map(expr).collect(),
                        build_keys: n.build_keys.into_iter().map(expr).collect(),
                        eq_null_safe: n.eq_null_safe,
                        residual_predicate: n.residual_predicate.map(expr),
                        runtime_filters,
                    },
                )
            }
            ExecNodeKind::NestedLoopJoin(n) => {
                let left = self.node(*n.left)?;
                let right = self.node(*n.right)?;
                (
                    n.node_id,
                    P::NestedLoopJoin {
                        left,
                        right,
                        join_type: match n.join_type {
                            super::nljoin::NestedLoopJoinType::Inner => {
                                lp::NestedLoopJoinType::Inner
                            }
                            super::nljoin::NestedLoopJoinType::Cross => {
                                lp::NestedLoopJoinType::Cross
                            }
                            super::nljoin::NestedLoopJoinType::LeftOuter => {
                                lp::NestedLoopJoinType::LeftOuter
                            }
                            super::nljoin::NestedLoopJoinType::RightOuter => {
                                lp::NestedLoopJoinType::RightOuter
                            }
                            super::nljoin::NestedLoopJoinType::FullOuter => {
                                lp::NestedLoopJoinType::FullOuter
                            }
                            super::nljoin::NestedLoopJoinType::LeftSemi => {
                                lp::NestedLoopJoinType::LeftSemi
                            }
                            super::nljoin::NestedLoopJoinType::LeftAnti => {
                                lp::NestedLoopJoinType::LeftAnti
                            }
                            super::nljoin::NestedLoopJoinType::NullAwareLeftAnti => {
                                lp::NestedLoopJoinType::NullAwareLeftAnti
                            }
                        },
                        join_conjunct: n.join_conjunct.map(expr),
                        left_layout: layout(&n.left_chunk_schema)?,
                        right_layout: layout(&n.right_chunk_schema)?,
                        join_scope_layout: layout(&n.join_scope_chunk_schema)?,
                    },
                )
            }
            ExecNodeKind::Sort(n) => {
                let input = self.node(*n.input)?;
                let sorts = |items: Vec<super::sort::SortExpression>| {
                    items
                        .into_iter()
                        .map(|item| lp::SortExpression {
                            expr: expr(item.expr),
                            asc: item.asc,
                            nulls_first: item.nulls_first,
                        })
                        .collect()
                };
                (
                    n.node_id,
                    P::Sort {
                        input,
                        use_top_n: n.use_top_n,
                        order_by: sorts(n.order_by),
                        limit: n.limit,
                        offset: n.offset,
                        topn_type: match n.topn_type {
                            super::sort::SortTopNType::RowNumber => lp::SortTopNType::RowNumber,
                            super::sort::SortTopNType::Rank => lp::SortTopNType::Rank,
                            super::sort::SortTopNType::DenseRank => lp::SortTopNType::DenseRank,
                        },
                        max_buffered_rows: n.max_buffered_rows,
                        max_buffered_bytes: n.max_buffered_bytes,
                        partition_exprs: sorts(n.partition_exprs),
                        partition_limit: n.partition_limit,
                    },
                )
            }
            ExecNodeKind::TableFunction(n) => {
                let input = self.node(*n.input)?;
                (
                    n.node_id,
                    P::TableFunction {
                        input,
                        function_name: Arc::from(n.function_name),
                        param_slots: n.param_slots,
                        outer_slots: n.outer_slots,
                        fn_result_slots: n.fn_result_slots,
                        fn_result_required: n.fn_result_required,
                        is_left_join: n.is_left_join,
                        param_types: n.param_types,
                        ret_types: n.ret_types,
                        output_slot_sources: n
                            .output_slot_sources
                            .into_iter()
                            .map(|source| match source {
                                super::table_function::TableFunctionOutputSlot::Outer { slot } => {
                                    lp::TableFunctionOutputSlot::Outer { slot }
                                }
                                super::table_function::TableFunctionOutputSlot::Result {
                                    index,
                                } => lp::TableFunctionOutputSlot::Result { index },
                            })
                            .collect(),
                    },
                )
            }
            ExecNodeKind::Analytic(n) => {
                let input = self.node(*n.input)?;
                (
                    n.node_id,
                    P::Analytic {
                        input,
                        partition_exprs: n.partition_exprs.into_iter().map(expr).collect(),
                        order_by_exprs: n.order_by_exprs.into_iter().map(expr).collect(),
                        functions: n
                            .functions
                            .into_iter()
                            .map(freeze_window_function)
                            .collect(),
                        window: n.window.map(freeze_window_frame),
                        output_columns: n
                            .output_columns
                            .into_iter()
                            .map(|col| match col {
                                super::analytic::AnalyticOutputColumn::InputSlotId(slot) => {
                                    lp::AnalyticOutputColumn::InputSlotId(slot)
                                }
                                super::analytic::AnalyticOutputColumn::Window(index) => {
                                    lp::AnalyticOutputColumn::Window(index)
                                }
                            })
                            .collect(),
                    },
                )
            }
            ExecNodeKind::SetOp(n) => {
                let inputs = n
                    .inputs
                    .into_iter()
                    .map(|node| self.node(node))
                    .collect::<Result<_>>()?;
                (
                    n.node_id,
                    P::SetOp {
                        kind: match n.kind {
                            super::set_op::SetOpKind::Intersect => lp::SetOpKind::Intersect,
                            super::set_op::SetOpKind::Except => lp::SetOpKind::Except,
                        },
                        inputs,
                    },
                )
            }
            ExecNodeKind::RuntimeFilterConsumer(n) => {
                let input = self.node(*n.input)?;
                let bindings = self.consumers(n.bindings)?;
                (
                    n.owner_node_id,
                    P::RuntimeFilterConsumer { input, bindings },
                )
            }
            ExecNodeKind::TableWriter(n) => {
                let node_id = n.node_id;
                let (
                    runtime_binding,
                    (child, target, expected_schema, projection, writer_schema, aggregate),
                ) = n.into_static_parts_with_binding();
                let input = self.node(*child)?;
                let id = lp::ProgramNodeId::new(self.nodes.len());
                self.runtime.writers.insert(id, runtime_binding);
                self.requirements.push(lp::BindingRequirement::TableWriter {
                    node: id,
                    layout: output.clone(),
                });
                let exact_projection_layout = layout(projection.chunk_schema())?;
                if exact_projection_layout.schema().as_ref() != expected_schema.as_ref() {
                    return Err(LocalProgramLoweringError::new(
                        "writer projection layout differs from expected writer schema",
                    ));
                }
                let mut projection = projection
                    .into_static()
                    .map_err(LocalProgramLoweringError::new)?;
                projection.layout = exact_projection_layout.clone();
                let expected_layout = exact_projection_layout;
                (
                    node_id,
                    P::TableWriter {
                        input,
                        target,
                        expected_layout,
                        projection,
                        writer_multiplex_layout: layout(writer_schema.chunk_schema())?,
                        partial_aggregates: aggregate
                            .calls
                            .into_iter()
                            .map(|call| lp::WriterPartialAggregateCall {
                                input_slot_id: call.input_slot_id,
                                function_name: call.function_name,
                                resolved: call.resolved,
                                intermediate_slot_id: call.intermediate_slot_id,
                            })
                            .collect(),
                    },
                )
            }
            ExecNodeKind::TableFinish(n) => {
                let node_id = n.node_id;
                let (
                    runtime_binding,
                    (children, expected_targets, writer_schema, root_schema, final_aggregate),
                ) = n.into_static_parts_with_binding();
                let inputs = children
                    .into_iter()
                    .map(|node| self.node(node))
                    .collect::<Result<_>>()?;
                let id = lp::ProgramNodeId::new(self.nodes.len());
                self.runtime.finishers.insert(id, runtime_binding);
                self.requirements.push(lp::BindingRequirement::TableFinish {
                    node: id,
                    layout: output.clone(),
                });
                (
                    node_id,
                    P::TableFinish {
                        inputs,
                        expected_targets,
                        writer_multiplex_layout: layout(writer_schema.chunk_schema())?,
                        root_result_layout: layout(root_schema.chunk_schema())?,
                        final_aggregates: lp::WriterFinalAggregatePlan {
                            calls: final_aggregate
                                .calls
                                .into_iter()
                                .map(|call| lp::WriterFinalAggregateCall {
                                    function_name: call.function_name,
                                    resolved: call.resolved,
                                    intermediate_input_slot_id: call.intermediate_input_slot_id,
                                    final_output_slot_id: call.final_output_slot_id,
                                })
                                .collect(),
                            unpivot: final_aggregate.unpivot.map(|unpivot| {
                                lp::WriterGroupedUnpivotPlan {
                                    grouping_input_slot_id: unpivot.grouping_input_slot_id,
                                    grouping_output_slot_id: unpivot.grouping_output_slot_id,
                                    passthrough_output_slot_id: unpivot.passthrough_output_slot_id,
                                    value_output_slot_id: unpivot.value_output_slot_id,
                                    literal_output_slot_ids: unpivot.literal_output_slot_ids,
                                    mappings: unpivot
                                        .mappings
                                        .into_iter()
                                        .map(|mapping| lp::WriterGroupedUnpivotMapping {
                                            grouping_key: mapping.grouping_key,
                                            input_value_slot_id: mapping.input_value_slot_id,
                                            constants: mapping
                                                .constants
                                                .into_iter()
                                                .map(freeze_unpivot_constant)
                                                .collect(),
                                        })
                                        .collect(),
                                    max_output_rows: unpivot.max_output_rows,
                                    max_output_bytes: unpivot.max_output_bytes,
                                }
                            }),
                        },
                    },
                )
            }
            ExecNodeKind::AssertNumRows(_)
            | ExecNodeKind::Values(_)
            | ExecNodeKind::Project(_)
            | ExecNodeKind::Unpivot(_)
            | ExecNodeKind::Filter(_)
            | ExecNodeKind::Repeat(_)
            | ExecNodeKind::ChangeEventExpand(_)
            | ExecNodeKind::UnionAll(_)
            | ExecNodeKind::Limit(_) => {
                unreachable!("the first lowering match already consumed this variant")
            }
        };
        Ok(mapped)
    }
}

fn layout(schema: &ChunkSchemaRef) -> Result<lp::StaticLayout> {
    lp::StaticLayout::try_new_exact(
        schema.arrow_schema_ref(),
        Arc::from(schema.slot_ids()),
        schema
            .slots()
            .iter()
            .map(|slot| {
                (
                    freeze_field_schema(slot.field_schema().clone()),
                    slot.unique_id(),
                )
            })
            .collect(),
    )
    .map_err(|error| LocalProgramLoweringError::new(error.to_string()))
}

fn expr(id: ExprId) -> lp::ProgramExprId {
    lp::ProgramExprId::new(id.0)
}

fn freeze_unpivot_constant(value: super::unpivot::UnpivotConstant) -> lp::UnpivotConstant {
    match value {
        super::unpivot::UnpivotConstant::Scalar { expr_id, nullable } => {
            lp::UnpivotConstant::Scalar {
                expr_id: expr(expr_id),
                nullable,
            }
        }
        super::unpivot::UnpivotConstant::Int32List(values) => {
            lp::UnpivotConstant::Int32List(values)
        }
        super::unpivot::UnpivotConstant::Utf8Map(values) => lp::UnpivotConstant::Utf8Map(
            values
                .into_iter()
                .map(|(key, value)| (Arc::from(key), Arc::from(value)))
                .collect(),
        ),
    }
}

fn freeze_filter_contract(
    contract: &rf::RuntimeFilterExecutionContract,
) -> lp::StaticFilterContract {
    match contract {
        rf::RuntimeFilterExecutionContract::Membership(schema) => {
            lp::StaticFilterContract::Membership {
                data_type: schema.data_type().clone(),
                null_semantics: match schema.null_semantics() {
                    rf::RuntimeFilterNullSemantics::NeverMatches => {
                        lp::FilterNullSemantics::NeverMatches
                    }
                    rf::RuntimeFilterNullSemantics::NullSafeEqual => {
                        lp::FilterNullSemantics::NullSafeEqual
                    }
                },
                digest: schema.digest(),
            }
        }
        rf::RuntimeFilterExecutionContract::Ordered(order) => lp::StaticFilterContract::Ordered {
            keys: order
                .keys()
                .iter()
                .map(|key| lp::FilterOrderKey {
                    data_type: key.data_type().clone(),
                    direction: match key.direction() {
                        rf::contribution::RuntimeOrderSortDirection::Ascending => {
                            lp::FilterSortDirection::Ascending
                        }
                        rf::contribution::RuntimeOrderSortDirection::Descending => {
                            lp::FilterSortDirection::Descending
                        }
                    },
                    null_order: match key.null_order() {
                        rf::contribution::RuntimeOrderNullOrder::First => {
                            lp::FilterNullOrder::First
                        }
                        rf::contribution::RuntimeOrderNullOrder::Last => lp::FilterNullOrder::Last,
                    },
                })
                .collect(),
            comparator_digest: order.comparator_digest(),
            contract_digest: order.digest(),
        },
    }
}

fn freeze_reduction(reduction: rf::RuntimeFilterReduction) -> Result<lp::FilterReduction> {
    Ok(match reduction {
        rf::RuntimeFilterReduction::SetUnion => lp::FilterReduction::SetUnion,
        rf::RuntimeFilterReduction::TightenOrderedBound => lp::FilterReduction::TightenOrderedBound,
        rf::RuntimeFilterReduction::MergeTopKSummary { k, .. } => {
            lp::FilterReduction::MergeTopKSummary {
                k: std::num::NonZeroU32::new(k)
                    .ok_or_else(|| LocalProgramLoweringError::new("zero top-K runtime filter"))?,
            }
        }
    })
}

fn freeze_consumer(binding: &RuntimeFilterConsumerBinding) -> Result<lp::StaticFilterConsumer> {
    let contract = binding.contract();
    let activation = match contract.activation() {
        rf::ConsumerActivation::BlockingSnapshot => lp::FilterConsumerActivation::BlockingSnapshot,
        rf::ConsumerActivation::NonBlockingLive { late_apply } => {
            lp::FilterConsumerActivation::NonBlockingLive {
                late_apply: match late_apply {
                    rf::RuntimeFilterLateApplyGranularity::Row => {
                        lp::FilterLateApplyGranularity::Row
                    }
                    rf::RuntimeFilterLateApplyGranularity::Batch => {
                        lp::FilterLateApplyGranularity::Batch
                    }
                    rf::RuntimeFilterLateApplyGranularity::RowGroup => {
                        lp::FilterLateApplyGranularity::RowGroup
                    }
                    rf::RuntimeFilterLateApplyGranularity::Split => {
                        lp::FilterLateApplyGranularity::Split
                    }
                    rf::RuntimeFilterLateApplyGranularity::File => {
                        lp::FilterLateApplyGranularity::File
                    }
                },
            }
        }
    };
    let scan_domain = binding
        .scan_domain
        .as_ref()
        .map(|domain| lp::FilterScanDomainTarget {
            field_ordinal: domain.target().field_ordinal(),
            data_type: domain.target().data_type().clone(),
            nullable: domain.target().nullable(),
        });
    lp::StaticFilterConsumer::try_new(
        contract.binding_id().get(),
        contract.channel_id().get(),
        activation,
        freeze_filter_contract(contract.contract()),
        freeze_reduction(contract.reduction())?,
        scan_domain,
    )
    .map_err(|error| LocalProgramLoweringError::new(error.to_string()))
}

fn freeze_producer(
    contract: &rf::RuntimeFilterProducerContract,
) -> Result<lp::StaticFilterProducer> {
    let kind = match contract.kind() {
        rf::RuntimeFilterProducerKind::Membership => lp::FilterProducerKind::Membership,
        rf::RuntimeFilterProducerKind::OrderedBound => lp::FilterProducerKind::OrderedBound,
        rf::RuntimeFilterProducerKind::TopKSummary => lp::FilterProducerKind::TopKSummary,
        rf::RuntimeFilterProducerKind::FinalDomain => lp::FilterProducerKind::FinalDomain,
    };
    lp::StaticFilterProducer::try_new(
        contract.binding_id().get(),
        contract.channel_id().get(),
        kind,
        freeze_filter_contract(contract.contract()),
        freeze_reduction(contract.reduction())?,
    )
    .map_err(|error| LocalProgramLoweringError::new(error.to_string()))
}

fn freeze_window_frame(frame: super::analytic::WindowFrame) -> lp::WindowFrame {
    let boundary = |boundary| match boundary {
        super::analytic::WindowBoundary::CurrentRow => lp::WindowBoundary::CurrentRow,
        super::analytic::WindowBoundary::Preceding(value) => lp::WindowBoundary::Preceding(value),
        super::analytic::WindowBoundary::Following(value) => lp::WindowBoundary::Following(value),
    };
    lp::WindowFrame {
        start: frame.start.map(boundary),
        end: frame.end.map(boundary),
        window_type: match frame.window_type {
            super::analytic::WindowType::Rows => lp::WindowType::Rows,
            super::analytic::WindowType::Range => lp::WindowType::Range,
        },
    }
}

fn freeze_window_function(
    function: super::analytic::WindowFunctionSpec,
) -> lp::StaticWindowFunction {
    use super::analytic::WindowFunctionKind as W;
    lp::StaticWindowFunction {
        kind: match function.kind {
            W::RowNumber => lp::WindowFunctionKind::RowNumber,
            W::Rank => lp::WindowFunctionKind::Rank,
            W::DenseRank => lp::WindowFunctionKind::DenseRank,
            W::CumeDist => lp::WindowFunctionKind::CumeDist,
            W::PercentRank => lp::WindowFunctionKind::PercentRank,
            W::Ntile => lp::WindowFunctionKind::Ntile,
            W::FirstValue { ignore_nulls } => lp::WindowFunctionKind::FirstValue { ignore_nulls },
            W::FirstValueRewrite { ignore_nulls } => {
                lp::WindowFunctionKind::FirstValueRewrite { ignore_nulls }
            }
            W::LastValue { ignore_nulls } => lp::WindowFunctionKind::LastValue { ignore_nulls },
            W::Lead { ignore_nulls } => lp::WindowFunctionKind::Lead { ignore_nulls },
            W::Lag { ignore_nulls } => lp::WindowFunctionKind::Lag { ignore_nulls },
            W::SessionNumber => lp::WindowFunctionKind::SessionNumber,
            W::Count => lp::WindowFunctionKind::Count,
            W::Sum => lp::WindowFunctionKind::Sum,
            W::Avg => lp::WindowFunctionKind::Avg,
            W::Min => lp::WindowFunctionKind::Min,
            W::Max => lp::WindowFunctionKind::Max,
            W::BitmapUnion => lp::WindowFunctionKind::BitmapUnion,
            W::BitmapUnionCount => lp::WindowFunctionKind::BitmapUnionCount,
            W::MaxBy => lp::WindowFunctionKind::MaxBy,
            W::MinBy => lp::WindowFunctionKind::MinBy,
            W::VarianceSamp => lp::WindowFunctionKind::VarianceSamp,
            W::StddevSamp => lp::WindowFunctionKind::StddevSamp,
            W::BoolOr => lp::WindowFunctionKind::BoolOr,
            W::CovarPop => lp::WindowFunctionKind::CovarPop,
            W::CovarSamp => lp::WindowFunctionKind::CovarSamp,
            W::Corr => lp::WindowFunctionKind::Corr,
            W::ArrayAgg {
                is_distinct,
                is_asc_order,
                nulls_first,
            } => lp::WindowFunctionKind::ArrayAgg {
                is_distinct,
                is_asc_order,
                nulls_first,
            },
            W::ApproxTopK => lp::WindowFunctionKind::ApproxTopK,
        },
        args: function.args.into_iter().map(expr).collect(),
        return_type: function.return_type,
        aggregate_binding: function
            .aggregate_binding
            .map(|binding| (Arc::from(binding.function_name), binding.resolved)),
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroUsize};

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_types::SlotId;

    use super::*;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use crate::exec::node::filter::FilterNode;
    use crate::exec::node::limit::LimitNode;
    use crate::exec::node::repeat::RepeatNode;
    use crate::exec::node::values::ValuesNode;

    fn values() -> (ExecNode, lp::StaticLayout) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap();
        let layout = super::layout(&chunk.chunk_schema_ref()).unwrap();
        (
            ExecNode {
                kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 1 }),
            },
            layout,
        )
    }

    fn profile(layout: &lp::StaticLayout) -> lp::CompileProfile {
        lp::CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            layout.identity().unwrap(),
            lp::KernelAbiVersion::new(NonZeroU32::new(1).unwrap()),
        )
    }

    #[test]
    fn lowering_consumes_values_and_keeps_expression_and_sink_exact() {
        let (input, layout) = values();
        let mut arena = ExprArena::default();
        let predicate = arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(true)),
            DataType::Boolean,
        );
        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Filter(FilterNode {
                    input: Box::new(input),
                    node_id: 2,
                    predicate,
                }),
            },
        };
        let program = plan
            .into_local_program(
                profile(&layout),
                BTreeMap::new(),
                vec![ExternalSinkRequirement::Result],
            )
            .unwrap();
        assert_eq!(program.nodes().len(), 2);
        assert!(matches!(
            program.nodes()[0].kind(),
            lp::ProgramNodeKind::Values { .. }
        ));
        assert!(
            matches!(program.nodes()[1].kind(), lp::ProgramNodeKind::Filter { predicate, .. } if *predicate == lp::ProgramExprId::new(0))
        );
        assert_eq!(program.requirements().entries().len(), 1);
        assert!(matches!(
            program.requirements().entries()[0],
            lp::BindingRequirement::ResultSink { .. }
        ));
    }

    #[test]
    fn lowering_rejects_excess_depth_before_recursive_consume() {
        let (mut root, layout) = values();
        for id in 2..=66 {
            root = ExecNode {
                kind: ExecNodeKind::Limit(LimitNode {
                    input: Box::new(root),
                    node_id: id,
                    limit: Some(1),
                    offset: 0,
                }),
            };
        }
        let error = ExecPlan {
            arena: ExprArena::default(),
            root,
        }
        .into_local_program(profile(&layout), BTreeMap::new(), vec![])
        .unwrap_err();
        assert!(error.to_string().contains("node/depth limit"));
    }

    #[test]
    fn repeat_grouping_slot_is_available_to_static_exchange_sink() {
        let (input, _) = values();
        let plan = ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::Repeat(RepeatNode {
                    input: Box::new(input),
                    node_id: 2,
                    null_slot_ids: vec![vec![SlotId::new(1)], vec![], vec![SlotId::new(1)]],
                    grouping_slot_ids: vec![SlotId::new(11)],
                    grouping_list: vec![vec![0, 1, 2]],
                    repeat_times: 3,
                }),
            },
        };
        let profile = plan
            .local_compile_profile(NonZeroUsize::new(1).unwrap(), None)
            .unwrap();
        let branch = lp::StaticStreamBranch::try_new(
            3,
            novarocks_execution_contract::DataStreamPartitionType::Unpartitioned,
            vec![],
            vec![SlotId::new(1), SlotId::new(11)],
            None,
        )
        .unwrap();
        let sink = lp::StaticSinkProgram::try_data_stream(
            branch,
            Arc::new(
                lp::ImmutableExpressions::try_new(Vec::new(), false, Default::default(), None)
                    .unwrap(),
            ),
        )
        .unwrap();
        let (program, _) = plan
            .into_local_program_and_bindings(
                profile,
                BTreeMap::new(),
                vec![ExternalSinkRequirement::ExchangeOutput { branch: 0 }],
                sink,
            )
            .unwrap();
        assert_eq!(
            program.nodes()[program.root().index()]
                .output_layout()
                .slots(),
            &[SlotId::new(1), SlotId::new(11)]
        );
        assert!(
            program.nodes()[program.root().index()]
                .output_layout()
                .schema()
                .field(0)
                .is_nullable()
        );
    }
}
