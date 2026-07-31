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

// Boundary columns, occurrence identity, and the query-scoped allocator are the
// authoritative membership catalog that CGO-9C/Task 3 will consume. Until those
// tasks read every field and accessor, allow the not-yet-consumed surface.
#![allow(dead_code)]

//! Planner-native boundary contract for a sealed distributed plan.
//!
//! A *boundary* is a place where columns cross a plan seam that later stages
//! (codegen, coordinator) must agree on exactly: the query result root, an
//! Exchange sender/receiver, an Iceberg write sink input, or a change-stream
//! router input. This module derives the authoritative *membership* of every
//! such boundary purely from already-constructed planner artifacts
//! (`PlanFragment`, `FragmentEdge`, `DataSink`), assigning each column
//! occurrence a query-scoped [`ExecutionColumnId`].
//!
//! Occurrence identity is the core idea: the same logical planner [`ColumnId`]
//! can appear at several boundaries (e.g. once at an Exchange sender and again
//! at the matching receiver). Those are distinct *occurrences* and each is given
//! its own [`ExecutionColumnId`]; `column_id` + `output_ordinal` preserve the
//! logical provenance but are deliberately NOT the occurrence key.
//!
//! There is exactly one [`ExecutionColumnIdAllocator`] per query. The sealing
//! artifact stores its final state so CGO-9C can keep allocating ids for
//! internal (non-boundary) occurrences without rebuilding the allocator or
//! re-deriving occurrence identity from `ColumnId`.
//!
//! This module depends only on planner and arrow types: no protobuf, no
//! coordinator, no runtime handles.

use std::fmt;

use arrow::datatypes::DataType;

use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;

use super::write::sink::ConnectorWriteInputBinding;
use super::{
    DataSink, DistributedNode, DistributedNodeKind, ExchangeReceiver, FragmentEdge, FragmentId,
    PlanFragment,
};

/// Query-scoped identity for a single boundary column *occurrence*.
///
/// Unlike [`ColumnId`] (logical provenance, shared across occurrences), an
/// `ExecutionColumnId` is unique for every occurrence within one query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ExecutionColumnId(u32);

impl ExecutionColumnId {
    /// The raw occurrence value. Occurrence ids are assigned densely from 1.
    pub(crate) fn value(self) -> u32 {
        self.0
    }
}

/// The single query-scoped allocator of [`ExecutionColumnId`] occurrences.
///
/// Task 1 uses it to number authoritative boundary members. Its final state is
/// preserved in the sealed plan so CGO-9C can resume allocating internal
/// occurrences from exactly where boundary derivation stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExecutionColumnIdAllocator {
    next: u32,
}

impl ExecutionColumnIdAllocator {
    /// A fresh allocator. Occurrence ids start at 1; 0 is reserved as "unset".
    pub(crate) fn new() -> Self {
        Self { next: 1 }
    }

    /// Allocate the next unique occurrence id.
    pub(crate) fn allocate(&mut self) -> ExecutionColumnId {
        let id = ExecutionColumnId(self.next);
        self.next = self
            .next
            .checked_add(1)
            .expect("execution column id space exhausted");
        id
    }

    /// The value the next [`allocate`](Self::allocate) call would return,
    /// without allocating it. CGO-9C resumes allocation from here.
    pub(crate) fn peek_next(&self) -> u32 {
        self.next
    }
}

impl Default for ExecutionColumnIdAllocator {
    fn default() -> Self {
        Self::new()
    }
}

/// One column at a boundary, carrying both its occurrence identity and its
/// logical planner provenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BoundaryColumn {
    /// Query-scoped occurrence identity (NEW; unique per boundary member).
    pub execution_column_id: ExecutionColumnId,
    /// Logical planner provenance (shared across occurrences of the column).
    pub column_id: ColumnId,
    /// Position of this column within *this* boundary (0-based).
    pub output_ordinal: usize,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_internal: bool,
}

/// The kind of seam a [`BoundaryContract`] describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum BoundaryKind {
    /// The query result root output (root fragment with a result sink).
    ResultOutput,
    /// The sender side of an Exchange edge (stream, CTE multicast, or router).
    ///
    /// Its columns are sourced from the matching receiver's `output_columns`
    /// (which already carry the source fragment's provenance) so the send and
    /// receive boundaries stay symmetric. CGO-9C must consult
    /// `edge.output_slot_ids` when binding these send occurrences back to their
    /// source-fragment slots.
    ExchangeSend,
    /// The receiver side of an Exchange edge.
    ExchangeReceive,
    /// The input columns feeding an Iceberg write sink.
    IcebergWriteInput,
    /// The full input feeding an Iceberg change-stream router sink.
    ChangeStreamRouterInput,
}

impl fmt::Display for BoundaryKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ResultOutput => "result-output",
            Self::ExchangeSend => "exchange-send",
            Self::ExchangeReceive => "exchange-receive",
            Self::IcebergWriteInput => "iceberg-write-input",
            Self::ChangeStreamRouterInput => "change-stream-router-input",
        })
    }
}

/// The membership contract for one boundary.
///
/// `node_id` is `None` for fragment-level sink boundaries (result / Iceberg
/// write / change-stream router input). For Exchange send/receive it is the
/// destination Exchange node id: the receiver is that node, and the sender's
/// sink is addressed to it, so both sides share the id and can be paired.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BoundaryContract {
    pub fragment_id: FragmentId,
    pub node_id: Option<i32>,
    pub kind: BoundaryKind,
    pub columns: Vec<BoundaryColumn>,
}

/// The full set of boundary contracts for a sealed distributed plan, in
/// deterministic derivation order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BoundaryCatalog {
    contracts: Vec<BoundaryContract>,
}

impl BoundaryCatalog {
    /// All contracts in canonical derivation order (fragment sink boundaries in
    /// fragment order, then per-edge send/receive boundaries in edge order).
    pub(crate) fn contracts(&self) -> &[BoundaryContract] {
        &self.contracts
    }
}

/// A reason boundary derivation refused to guess.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::sql::planner::distributed) enum BoundaryError {
    /// A referenced output ordinal does not exist in the source fragment's
    /// output columns. Fail fast instead of inventing a projection.
    OutputOrdinalOutOfRange {
        fragment_id: FragmentId,
        kind: BoundaryKind,
        ordinal: usize,
        available: usize,
    },
    /// A non-root fragment carries a result sink. Structural validation forbids
    /// this; deriving boundaries re-checks it rather than silently skipping the
    /// fragment, so a future invariant regression fails loudly.
    NonRootResultSink { fragment_id: FragmentId },
    /// An edge's target Exchange node could not be resolved to an
    /// [`ExchangeReceiver`]. Structural validation already guarantees this
    /// resolves for sealed plans; deriving boundaries re-checks it rather than
    /// unwrapping, so a future invariant regression fails loudly instead of
    /// panicking or guessing.
    UnresolvedTargetExchange {
        target_fragment_id: FragmentId,
        target_exchange_node_id: i32,
    },
}

impl fmt::Display for BoundaryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputOrdinalOutOfRange {
                fragment_id,
                kind,
                ordinal,
                available,
            } => write!(
                formatter,
                "distributed plan {kind} boundary for fragment id={fragment_id} references output ordinal {ordinal} but only {available} output columns exist"
            ),
            Self::NonRootResultSink { fragment_id } => write!(
                formatter,
                "distributed plan non-root fragment id={fragment_id} carries a result sink"
            ),
            Self::UnresolvedTargetExchange {
                target_fragment_id,
                target_exchange_node_id,
            } => write!(
                formatter,
                "distributed plan boundary could not resolve target exchange node_id={target_exchange_node_id} in fragment id={target_fragment_id}"
            ),
        }
    }
}

/// Derive the authoritative boundary membership catalog from already-constructed
/// fragments, edges, and sinks.
///
/// Runs inside `seal_draft` after structural and runtime-filter-graph
/// validation, so the plan shape is already known-sound. This function only
/// *derives*: it never repairs edges or invents schema, and fails fast on any
/// column reference that does not resolve.
///
/// Determinism: fragment sink boundaries are emitted in fragment order, then
/// per-edge send/receive boundaries in edge order; within a boundary, columns
/// keep their source order. The single `allocator` therefore assigns occurrence
/// ids in a fully input-determined sequence.
pub(in crate::sql::planner::distributed) fn build_boundary_catalog(
    fragments: &[PlanFragment],
    root_fragment_id: FragmentId,
    edges: &[FragmentEdge],
    allocator: &mut ExecutionColumnIdAllocator,
) -> Result<BoundaryCatalog, BoundaryError> {
    let mut contracts = Vec::new();

    // Fragment sink boundaries, in fragment declaration order.
    for fragment in fragments {
        match &fragment.sink {
            DataSink::Result => {
                // Structural validation guarantees only the root fragment carries
                // a result sink, and that boundary is the query result output.
                // Re-check it here and fail fast rather than silently skipping a
                // non-root result sink, mirroring `resolve_target_exchange`.
                if fragment.fragment_id != root_fragment_id {
                    return Err(BoundaryError::NonRootResultSink {
                        fragment_id: fragment.fragment_id,
                    });
                }
                contracts.push(BoundaryContract {
                    fragment_id: fragment.fragment_id,
                    node_id: None,
                    kind: BoundaryKind::ResultOutput,
                    columns: occurrence_columns(&fragment.output_columns, allocator),
                });
            }
            DataSink::Noop => {}
            DataSink::Statistics(_) => {}
            DataSink::ConnectorWrite(sink) => {
                let columns = connector_write_input_columns(
                    fragment.fragment_id,
                    &fragment.output_columns,
                    &sink.input,
                    allocator,
                )?;
                contracts.push(BoundaryContract {
                    fragment_id: fragment.fragment_id,
                    node_id: None,
                    kind: BoundaryKind::IcebergWriteInput,
                    columns,
                });
            }
            DataSink::ChangeStreamRouter(_) => {
                contracts.push(BoundaryContract {
                    fragment_id: fragment.fragment_id,
                    node_id: None,
                    kind: BoundaryKind::ChangeStreamRouterInput,
                    columns: occurrence_columns(&fragment.output_columns, allocator),
                });
            }
        }
    }

    // Exchange send/receive boundaries, in edge declaration order. Both sides
    // carry the same logical columns (the receiver's, which already hold the
    // source fragment's provenance) at distinct occurrences and locations.
    for edge in edges {
        let receiver = resolve_target_exchange(fragments, edge)?;
        contracts.push(BoundaryContract {
            fragment_id: edge.source_fragment_id,
            node_id: Some(edge.target_exchange_node_id),
            kind: BoundaryKind::ExchangeSend,
            columns: occurrence_columns(&receiver.output_columns, allocator),
        });
        contracts.push(BoundaryContract {
            fragment_id: edge.target_fragment_id,
            node_id: Some(edge.target_exchange_node_id),
            kind: BoundaryKind::ExchangeReceive,
            columns: occurrence_columns(&receiver.output_columns, allocator),
        });
    }

    Ok(BoundaryCatalog { contracts })
}

/// Number each output column as a fresh occurrence, in source order.
fn occurrence_columns(
    outputs: &[OutputColumn],
    allocator: &mut ExecutionColumnIdAllocator,
) -> Vec<BoundaryColumn> {
    outputs
        .iter()
        .enumerate()
        .map(|(output_ordinal, column)| BoundaryColumn {
            execution_column_id: allocator.allocate(),
            column_id: column.column_id,
            output_ordinal,
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            is_internal: column.is_internal,
        })
        .collect()
}

/// Resolve the input columns of an Iceberg write sink from its binding, failing
/// fast on any ordinal that does not exist in the fragment's output columns.
fn iceberg_write_input_columns(
    fragment_id: FragmentId,
    outputs: &[OutputColumn],
    binding: &ConnectorWriteInputBinding,
    allocator: &mut ExecutionColumnIdAllocator,
) -> Result<Vec<BoundaryColumn>, BoundaryError> {
    match binding {
        ConnectorWriteInputBinding::RootOutputByOrdinal => {
            Ok(occurrence_columns(outputs, allocator))
        }
        ConnectorWriteInputBinding::OutputOrdinals(ordinals) => {
            let mut columns = Vec::with_capacity(ordinals.len());
            for (output_ordinal, source_ordinal) in ordinals.iter().enumerate() {
                let column =
                    outputs
                        .get(*source_ordinal)
                        .ok_or(BoundaryError::OutputOrdinalOutOfRange {
                            fragment_id,
                            kind: BoundaryKind::IcebergWriteInput,
                            ordinal: *source_ordinal,
                            available: outputs.len(),
                        })?;
                columns.push(BoundaryColumn {
                    execution_column_id: allocator.allocate(),
                    column_id: column.column_id,
                    output_ordinal,
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                    is_internal: column.is_internal,
                });
            }
            Ok(columns)
        }
    }
}

/// Resolve the input columns of a provider-neutral connector sink.  Keep this
/// separate from the Iceberg compatibility helper so the generic carrier does
/// not depend on a provider-named input binding.
fn connector_write_input_columns(
    fragment_id: FragmentId,
    outputs: &[OutputColumn],
    binding: &ConnectorWriteInputBinding,
    allocator: &mut ExecutionColumnIdAllocator,
) -> Result<Vec<BoundaryColumn>, BoundaryError> {
    match binding {
        ConnectorWriteInputBinding::RootOutputByOrdinal => {
            Ok(occurrence_columns(outputs, allocator))
        }
        ConnectorWriteInputBinding::OutputOrdinals(ordinals) => {
            let mut columns = Vec::with_capacity(ordinals.len());
            for (output_ordinal, source_ordinal) in ordinals.iter().enumerate() {
                let column =
                    outputs
                        .get(*source_ordinal)
                        .ok_or(BoundaryError::OutputOrdinalOutOfRange {
                            fragment_id,
                            kind: BoundaryKind::IcebergWriteInput,
                            ordinal: *source_ordinal,
                            available: outputs.len(),
                        })?;
                columns.push(BoundaryColumn {
                    execution_column_id: allocator.allocate(),
                    column_id: column.column_id,
                    output_ordinal,
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                    is_internal: column.is_internal,
                });
            }
            Ok(columns)
        }
    }
}

/// Resolve the `ExchangeReceiver` an edge points at. Structural validation
/// already guarantees this resolves for sealed plans; this re-checks rather
/// than unwrapping so a future invariant regression fails loudly.
fn resolve_target_exchange<'a>(
    fragments: &'a [PlanFragment],
    edge: &FragmentEdge,
) -> Result<&'a ExchangeReceiver, BoundaryError> {
    let unresolved = || BoundaryError::UnresolvedTargetExchange {
        target_fragment_id: edge.target_fragment_id,
        target_exchange_node_id: edge.target_exchange_node_id,
    };
    let target = fragments
        .iter()
        .find(|fragment| fragment.fragment_id == edge.target_fragment_id)
        .ok_or_else(unresolved)?;
    let node = find_node(&target.root, edge.target_exchange_node_id).ok_or_else(unresolved)?;
    match &node.payload {
        DistributedNodeKind::Exchange(receiver) => Ok(receiver),
        _ => Err(unresolved()),
    }
}

/// Depth-first search for a node by id within a fragment's node tree.
fn find_node(node: &DistributedNode, node_id: i32) -> Option<&DistributedNode> {
    if node.node_id == node_id {
        return Some(node);
    }
    node.children
        .iter()
        .find_map(|child| find_node(child, node_id))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::sql::analysis::OutputColumn;
    use crate::sql::analysis::cte::CteId;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder;
    use crate::sql::planner::distributed::write::change_stream::{
        ChangeStreamWriteBranchSpec, ChangeStreamWriteDagSpec,
    };
    use crate::sql::planner::distributed::write::plan::finalize_iceberg_change_stream_test_plan;
    use crate::sql::planner::distributed::write::sink::test_support::{
        simple_sink_spec, unpartitioned_metadata_json,
    };
    use crate::sql::planner::distributed::write::sink::{
        ConnectorWriteInputBinding, IcebergWritePlanInput,
    };
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan,
        ExchangeFlavor, ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
        PlanFragment,
    };
    use crate::sql::planner::payload::PlanValuesNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};
    use novarocks_catalog::schema::ColumnDef;

    use super::build_boundary_catalog;
    use super::{BoundaryContract, BoundaryError, BoundaryKind, ExecutionColumnIdAllocator};

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: Default::default(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn output_col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn internal_col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: true,
        }
    }

    fn values_node(fragment_id: u32, node_id: i32, columns: Vec<OutputColumn>) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: vec![node_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns,
            }),
        }
    }

    fn find_boundary(plan: &DistributedPlan, kind: BoundaryKind) -> &BoundaryContract {
        plan.boundaries()
            .contracts()
            .iter()
            .find(|contract| contract.kind == kind)
            .unwrap_or_else(|| panic!("expected a {kind} boundary in the catalog"))
    }

    fn result_plan(columns: Vec<OutputColumn>) -> DistributedPlan {
        DistributedPlanDraftBuilder::new(
            vec![PlanFragment {
                fragment_id: 0,
                root: values_node(0, 10, columns.clone()),
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns: columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            Some(0),
            Vec::new(),
            Default::default(),
        )
        .seal()
        .expect("result plan seals")
    }

    fn stream_plan() -> DistributedPlan {
        let columns = vec![output_col(1, "k")];
        let producer_fragment_id = 1;
        let consumer_fragment_id = 0;
        let exchange_node_id = 20;
        let producer = PlanFragment {
            fragment_id: producer_fragment_id,
            root: values_node(producer_fragment_id, 10, columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let consumer = PlanFragment {
            fragment_id: consumer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: consumer_fragment_id,
                tuple_ids: vec![exchange_node_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: producer_fragment_id,
                    output_columns: columns.clone(),
                    output_qualifier: None,
                    flavor: ExchangeFlavor::Distribution,
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        DistributedPlanDraftBuilder::new(
            vec![producer, consumer],
            Some(consumer_fragment_id),
            vec![FragmentEdge {
                source_fragment_id: producer_fragment_id,
                target_fragment_id: consumer_fragment_id,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![1],
            }],
            Default::default(),
        )
        .seal()
        .expect("stream plan seals")
    }

    fn cte_plan() -> DistributedPlan {
        let cte_id: CteId = 7;
        let producer_columns = vec![
            output_col(1, "k"),
            output_col(2, "v"),
            output_col(3, "payload"),
        ];
        let receive_columns = vec![producer_columns[0].clone(), producer_columns[2].clone()];
        let receive_producer_column_ids =
            vec![producer_columns[0].column_id, producer_columns[2].column_id];
        let producer_fragment_id = 1;
        let consumer_fragment_id = 0;
        let exchange_node_id = 20;
        let producer = PlanFragment {
            fragment_id: producer_fragment_id,
            root: values_node(producer_fragment_id, 10, producer_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: producer_columns,
            cte_id: Some(cte_id),
            cte_exchange_nodes: Vec::new(),
        };
        let consumer = PlanFragment {
            fragment_id: consumer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: consumer_fragment_id,
                tuple_ids: vec![exchange_node_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: producer_fragment_id,
                    output_columns: receive_columns.clone(),
                    output_qualifier: Some("c".to_string()),
                    flavor: ExchangeFlavor::CteMulticast {
                        cte_id,
                        receive_producer_column_ids: receive_producer_column_ids.clone(),
                    },
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: receive_columns,
            cte_id: None,
            cte_exchange_nodes: vec![(
                cte_id,
                exchange_node_id,
                receive_producer_column_ids.clone(),
            )],
        };
        DistributedPlanDraftBuilder::new(
            vec![producer, consumer],
            Some(consumer_fragment_id),
            vec![FragmentEdge {
                source_fragment_id: producer_fragment_id,
                target_fragment_id: consumer_fragment_id,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::CteMulticast {
                    cte_id,
                    receive_producer_column_ids,
                },
                output_slot_ids: vec![1, 3],
            }],
            Default::default(),
        )
        .seal()
        .expect("cte plan seals")
    }

    fn router_plan() -> DistributedPlan {
        let output_columns = vec![
            output_col(1, "op"),
            output_col(2, "route"),
            output_col(3, "delete_id"),
        ];
        let builder = DistributedPlanDraftBuilder::new(
            vec![PlanFragment {
                fragment_id: 0,
                root: values_node(0, 10, output_columns.clone()),
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            Some(0),
            Vec::new(),
            Default::default(),
        );
        let mut branch = ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![2]);
        branch.output_partition_ordinals = vec![2];
        branch.sink_spec.iceberg.serialized_metadata = Some(unpartitioned_metadata_json());
        let dag = ChangeStreamWriteDagSpec::for_test(Some(0), None, vec![branch]);
        finalize_iceberg_change_stream_test_plan(builder, "test_db", dag)
            .expect("router plan seals")
    }

    fn iceberg_write_plan(
        input: ConnectorWriteInputBinding,
        columns: Vec<OutputColumn>,
    ) -> Result<DistributedPlan, String> {
        // Give the sink a target schema whose arity matches the bound input so
        // the plan is a valid write plan for the seal-time write-contract
        // finalization (the target arity is incidental to what these boundary
        // tests assert).
        let target_arity = match &input {
            ConnectorWriteInputBinding::RootOutputByOrdinal => columns.len(),
            ConnectorWriteInputBinding::OutputOrdinals(ordinals) => ordinals.len(),
        };
        let mut spec = simple_sink_spec();
        spec.target_columns = (0..target_arity)
            .map(|idx| ColumnDef {
                name: format!("t{idx}"),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            })
            .collect();
        let draft = DistributedPlanDraftBuilder::new(
            vec![PlanFragment {
                fragment_id: 0,
                root: values_node(0, 10, columns.clone()),
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns: columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            Some(0),
            Vec::new(),
            Default::default(),
        )
        .into_draft();
        let draft = crate::sql::planner::distributed::write::plan::with_iceberg_write_sink(
            draft,
            IcebergWritePlanInput {
                descriptor_database: "test_db".to_string(),
                spec,
                input,
            },
        )?;
        crate::sql::planner::distributed::seal::seal_draft(draft).map_err(|error| error.to_string())
    }

    #[test]
    fn result_root_boundary_lists_fragment_output_columns() {
        let plan = result_plan(vec![output_col(1, "k"), internal_col(2, "__marker")]);
        let boundary = find_boundary(&plan, BoundaryKind::ResultOutput);

        assert_eq!(boundary.fragment_id, 0);
        assert_eq!(boundary.node_id, None);
        assert_eq!(boundary.columns.len(), 2);
        assert_eq!(boundary.columns[0].column_id, ColumnId::new_for_test(1));
        assert_eq!(boundary.columns[0].output_ordinal, 0);
        assert_eq!(boundary.columns[0].name, "k");
        assert!(!boundary.columns[0].is_internal);
        assert_eq!(boundary.columns[1].column_id, ColumnId::new_for_test(2));
        assert_eq!(boundary.columns[1].output_ordinal, 1);
        assert!(boundary.columns[1].is_internal);
        assert_ne!(
            boundary.columns[0].execution_column_id,
            boundary.columns[1].execution_column_id
        );
    }

    #[test]
    fn stream_sender_boundary_is_located_at_source_fragment() {
        let plan = stream_plan();
        let send = find_boundary(&plan, BoundaryKind::ExchangeSend);

        assert_eq!(send.fragment_id, 1);
        assert_eq!(send.node_id, Some(20));
        assert_eq!(send.columns.len(), 1);
        assert_eq!(send.columns[0].column_id, ColumnId::new_for_test(1));
        assert_eq!(send.columns[0].name, "k");
    }

    #[test]
    fn exchange_receiver_boundary_is_located_at_target_exchange_node() {
        let plan = stream_plan();
        let receive = find_boundary(&plan, BoundaryKind::ExchangeReceive);

        assert_eq!(receive.fragment_id, 0);
        assert_eq!(receive.node_id, Some(20));
        assert_eq!(receive.columns.len(), 1);
        assert_eq!(receive.columns[0].column_id, ColumnId::new_for_test(1));
    }

    #[test]
    fn same_logical_column_gets_distinct_occurrence_ids_at_send_and_receive() {
        let plan = stream_plan();
        let send = find_boundary(&plan, BoundaryKind::ExchangeSend);
        let receive = find_boundary(&plan, BoundaryKind::ExchangeReceive);

        // Same logical provenance ...
        assert_eq!(send.columns[0].column_id, receive.columns[0].column_id);
        // ... but distinct query-scoped occurrence identity.
        assert_ne!(
            send.columns[0].execution_column_id,
            receive.columns[0].execution_column_id
        );
    }

    #[test]
    fn cte_sender_boundary_preserves_producer_column_provenance() {
        let plan = cte_plan();
        let send = find_boundary(&plan, BoundaryKind::ExchangeSend);

        assert_eq!(send.fragment_id, 1);
        assert_eq!(send.node_id, Some(20));
        let column_ids: Vec<u32> = send
            .columns
            .iter()
            .map(|column| column.column_id.0)
            .collect();
        assert_eq!(column_ids, vec![1, 3]);
        assert_eq!(send.columns[0].name, "k");
        assert_eq!(send.columns[1].name, "payload");
        assert_eq!(send.columns[0].output_ordinal, 0);
        assert_eq!(send.columns[1].output_ordinal, 1);
    }

    #[test]
    fn router_sender_boundary_projects_branch_columns() {
        let plan = router_plan();
        let send = find_boundary(&plan, BoundaryKind::ExchangeSend);

        assert_eq!(send.fragment_id, 0);
        assert!(send.node_id.is_some());
        assert_eq!(send.columns.len(), 1);
        assert_eq!(send.columns[0].column_id, ColumnId::new_for_test(3));
        assert_eq!(send.columns[0].name, "delete_id");
    }

    #[test]
    fn change_stream_router_input_boundary_lists_full_router_input() {
        let plan = router_plan();
        let input = find_boundary(&plan, BoundaryKind::ChangeStreamRouterInput);

        assert_eq!(input.fragment_id, 0);
        assert_eq!(input.node_id, None);
        let names: Vec<&str> = input
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(names, vec!["op", "route", "delete_id"]);
    }

    #[test]
    fn iceberg_write_input_boundary_lists_bound_input_columns() {
        let plan = iceberg_write_plan(
            ConnectorWriteInputBinding::RootOutputByOrdinal,
            vec![output_col(1, "id_col")],
        )
        .expect("iceberg write plan seals");
        let input = find_boundary(&plan, BoundaryKind::IcebergWriteInput);

        assert_eq!(input.fragment_id, 0);
        assert_eq!(input.node_id, None);
        assert_eq!(input.columns.len(), 1);
        assert_eq!(input.columns[0].column_id, ColumnId::new_for_test(1));
        assert_eq!(input.columns[0].name, "id_col");
    }

    #[test]
    fn iceberg_write_input_boundary_projects_output_ordinals_with_local_positions() {
        // Two source columns; the binding selects source ordinal 1 then 0, so the
        // boundary carries them at boundary-local ordinals 0 and 1.
        let plan = iceberg_write_plan(
            ConnectorWriteInputBinding::OutputOrdinals(vec![1, 0]),
            vec![output_col(1, "a"), output_col(2, "b")],
        )
        .expect("iceberg write plan seals");
        let input = find_boundary(&plan, BoundaryKind::IcebergWriteInput);

        assert_eq!(input.columns.len(), 2);
        assert_eq!(input.columns[0].column_id, ColumnId::new_for_test(2));
        assert_eq!(input.columns[0].output_ordinal, 0);
        assert_eq!(input.columns[1].column_id, ColumnId::new_for_test(1));
        assert_eq!(input.columns[1].output_ordinal, 1);
    }

    #[test]
    fn iceberg_write_input_boundary_fails_fast_on_out_of_range_ordinal() {
        let error = iceberg_write_plan(
            ConnectorWriteInputBinding::OutputOrdinals(vec![99]),
            vec![output_col(1, "id")],
        )
        .expect_err("out-of-range write input ordinal must not seal");

        assert!(error.contains("references output ordinal 99"), "{error}");
        assert!(error.contains("iceberg-write-input"), "{error}");
    }

    #[test]
    fn build_boundary_catalog_fails_fast_when_edge_target_is_not_an_exchange() {
        // Bypass the seal path to exercise the defensive lookup directly: an edge
        // whose target node is a Values node, not an Exchange receiver.
        let columns = vec![output_col(1, "k")];
        let fragment = PlanFragment {
            fragment_id: 0,
            root: values_node(0, 10, columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let edge = FragmentEdge {
            source_fragment_id: 0,
            target_fragment_id: 0,
            target_exchange_node_id: 10,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: vec![1],
        };
        let mut allocator = ExecutionColumnIdAllocator::new();

        let error = build_boundary_catalog(&[fragment], 0, &[edge], &mut allocator)
            .expect_err("a non-exchange edge target must fail fast");

        assert_eq!(
            error,
            BoundaryError::UnresolvedTargetExchange {
                target_fragment_id: 0,
                target_exchange_node_id: 10,
            }
        );
    }

    #[test]
    fn build_boundary_catalog_fails_fast_on_non_root_result_sink() {
        // A non-root fragment with a result sink is forbidden by structural
        // validation; derive directly to exercise the defensive re-check.
        let columns = vec![output_col(1, "k")];
        let fragment = PlanFragment {
            fragment_id: 1,
            root: values_node(1, 10, columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let mut allocator = ExecutionColumnIdAllocator::new();

        let error = build_boundary_catalog(&[fragment], 0, &[], &mut allocator)
            .expect_err("a non-root result sink must fail fast");

        assert_eq!(error, BoundaryError::NonRootResultSink { fragment_id: 1 });
    }

    #[test]
    fn boundary_catalog_derivation_is_deterministic_and_ids_are_contiguous() {
        let first = router_plan();
        let second = router_plan();

        // A change-stream router plan exercises router input, one send, one
        // receive, and the writer's Iceberg write input: four boundaries.
        assert_eq!(first.boundaries().contracts().len(), 4);
        // Same draft shape derives an identical catalog, occurrence ids included.
        assert_eq!(first.boundaries(), second.boundaries());

        let ids: Vec<u32> = first
            .boundaries()
            .contracts()
            .iter()
            .flat_map(|contract| {
                contract
                    .columns
                    .iter()
                    .map(|column| column.execution_column_id.value())
            })
            .collect();
        let expected: Vec<u32> = (1..=ids.len() as u32).collect();
        assert_eq!(ids, expected, "occurrence ids must be dense and ordered");
    }

    #[test]
    fn sealed_plan_preserves_final_allocator_state_for_resumption() {
        let plan = router_plan();
        let total_columns: usize = plan
            .boundaries()
            .contracts()
            .iter()
            .map(|contract| contract.columns.len())
            .sum();
        assert_eq!(total_columns, 6);

        // The single allocator's final state is stored in the sealed plan: its
        // next id is one past the last boundary occurrence. CGO-9C resumes from
        // here without rebuilding the allocator or re-deriving occurrence
        // identity from `ColumnId`.
        assert_eq!(
            plan.execution_column_id_allocator().peek_next(),
            (total_columns as u32) + 1
        );
    }
}
