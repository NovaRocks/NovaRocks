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

// The node-output catalog is the planner-native record of every covered physical
// node's execution output (joins, scan, set-op, sort, and — as of CGO-9C Task 4 —
// hash-aggregate). CGO-9C Task 1 populates it and converts the native encoder onto
// exact reads of it; Tasks 2-5 (fragment/stream projection, write/router, guards)
// consume the occurrence mapping. Until every field and accessor is read, allow
// the not-yet-consumed surface, mirroring `boundary.rs`.
#![allow(dead_code)]

//! Planner-native node execution-output contract for a sealed distributed plan.
//!
//! Where [`super::boundary`] records *which columns cross each plan seam*, this
//! module records *what each execution node outputs*. It finalizes the output
//! columns of the physical nodes whose output the native encoder historically
//! re-derived or repaired: joins (`HashJoin` / `NestLoopJoin`), `Scan`, set
//! operations (`SetOp`), `Sort`, and `HashAggregate`. For a `HashAggregate` the
//! finalized execution output is the visible-or-full aggregate output columns
//! with per-mode intermediate aggregate-state types applied, and the full
//! group-key + aggregate-state wire layout is finalized alongside it (the encoder
//! maps that layout 1:1 into the `HashAggregateNode` payload).
//!
//! Non-join covered outputs (`Scan`, `SetOp`, `Sort`) are *read* from the
//! planner's already-computed physical payloads. Join outputs (`HashJoin` /
//! `NestLoopJoin`) are *reconciled against the join's children* rather than read
//! verbatim: a join's payload `output_columns` carry the planner-logical columns
//! selected for the join, but after fragmentation and scan column pruning those
//! ids can diverge from what the children actually produce at execution (for
//! example a marker/anti join whose probe scan pruned metadata columns, or a
//! join whose logical output lists a column no child emits). The BE builds the
//! join's output chunk from the concatenation of its children's schemas, so this
//! module recomputes the join's execution output from the children's outputs
//! (per join type, preserving the nullable side and any internal marker column)
//! and keeps the payload list only when it already matches. This is the
//! planner-side successor of the native encoder's now-removed join output
//! repair; the encoder maps the sealed contract 1:1.
//!
//! Every covered output is finalized with **unique wire column ids**: the BE
//! rejects duplicate `OutputColumn.column_id`s in a node schema, so a repeated
//! logical [`ColumnId`] within one covered node output is deduplicated here
//! (keeping the first occurrence). Re-materializing a column at several output
//! positions (`SELECT a, a`) is a boundary/projection concern, not a covered
//! node concern.
//!
//! Finalization also *validates* that each covered node carries a complete
//! output (non-empty after reconciliation, and, for `SetOp`, a per-child schema
//! that lines up with the node's children) and fails fast otherwise.
//!
//! Occurrence identity reuses the boundary catalog's work: a covered node that
//! is a fragment root whose fragment-level sink boundary carries exactly that
//! node's output (result / change-stream router / by-ordinal Iceberg write)
//! reuses the boundary's [`ExecutionColumnId`] occurrences. Every other node
//! occurrence is *internal* and is numbered from the SAME query-scoped
//! [`ExecutionColumnIdAllocator`], continued from where boundary derivation
//! stopped. The allocator is never rebuilt and occurrence identity is never
//! derived from [`ColumnId`] (which is shared across occurrences).
//!
//! This module depends only on planner and arrow types: no protobuf, no
//! coordinator, no runtime handles.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::common::expr::JoinKind;
use crate::sql::planner::payload::{PlanGenerateSeriesNode, PlanProjectNode, PlanScanNode};
use crate::sql::planner::physical::{
    PhysicalHashAggregateNode, aggregate_intermediate_type, hash_aggregate_outputs_intermediate,
};
use novarocks_catalog::schema::ColumnDef;

use super::boundary::{
    BoundaryCatalog, BoundaryContract, ExecutionColumnId, ExecutionColumnIdAllocator,
};
use super::write::change_stream::ChangeStreamBranchRoute;
use super::write::sink::{ConnectorWriteInputBinding, IcebergWritePlanInput};
use super::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, ExchangeReceiver, FragmentEdge,
    FragmentEdgeKind, FragmentId, PlanFragment,
};

/// Fragment root nodes keyed by fragment id, used to resolve an exchange
/// receiver's execution output to what its source fragment actually sends.
type FragmentRoots<'a> = BTreeMap<FragmentId, &'a DistributedNode>;

/// The physical node kinds whose execution output this contract finalizes.
///
/// `HashAggregate` is covered (CGO-9C Task 4): its execution output is the
/// visible-or-full aggregate output columns with per-mode intermediate aggregate
/// state types applied, and its group-key + aggregate-state wire layout is
/// finalized alongside (see [`FinalizedAggregateLayout`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum NodeExecutionKind {
    Scan,
    HashJoin,
    NestLoopJoin,
    SetOp,
    Sort,
    HashAggregate,
}

impl fmt::Display for NodeExecutionKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Scan => "scan",
            Self::HashJoin => "hash-join",
            Self::NestLoopJoin => "nest-loop-join",
            Self::SetOp => "set-op",
            Self::Sort => "sort",
            Self::HashAggregate => "hash-aggregate",
        })
    }
}

/// One output column of a finalized execution node, carrying both its
/// query-scoped occurrence identity and its logical planner provenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeExecutionColumn {
    /// Query-scoped occurrence identity (reused from the boundary catalog when
    /// this node output participates in a boundary, otherwise freshly allocated).
    pub execution_column_id: ExecutionColumnId,
    /// Logical planner provenance (shared across occurrences of the column).
    pub column_id: ColumnId,
    /// Position of this column within *this* node's output (0-based).
    pub output_ordinal: usize,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_internal: bool,
}

/// The finalized execution output of a single covered node, keyed by the node's
/// `(fragment_id, node_id)` identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeExecutionOutput {
    pub fragment_id: FragmentId,
    pub node_id: i32,
    pub kind: NodeExecutionKind,
    pub columns: Vec<NodeExecutionColumn>,
}

/// The finalized wire layout of a `HashAggregate` node: the group-key columns and
/// the aggregate-state columns, with per-mode intermediate aggregate-state types
/// already applied to the aggregate columns (for partial modes). The native
/// encoder maps this 1:1 into the `HashAggregateNode.output_layout` payload; the
/// aggregate's covered [`NodeExecutionOutput`] separately carries its
/// visible-or-full execution output (the layout is *not* recoverable from that
/// execution output when a visible projection subsets it).
///
/// (`OutputColumn` derives only `Clone, Debug` — not `PartialEq`/`Eq` — so this,
/// and the [`NodeOutputCatalog`] that holds it, are `Clone, Debug` only.)
#[derive(Clone, Debug)]
pub(crate) struct FinalizedAggregateLayout {
    pub group_key_columns: Vec<OutputColumn>,
    pub aggregate_columns: Vec<OutputColumn>,
}

/// The full set of finalized node execution outputs for a sealed distributed
/// plan, in deterministic derivation order (fragment declaration order, then a
/// pre-order walk of each fragment's node tree).
#[derive(Clone, Debug)]
pub(crate) struct NodeOutputCatalog {
    outputs: Vec<NodeExecutionOutput>,
    index: BTreeMap<(FragmentId, i32), usize>,
    /// Finalized `HashAggregate` wire layouts, keyed by `(fragment_id, node_id)`.
    /// Present for every covered `HashAggregate` node.
    aggregate_layouts: BTreeMap<(FragmentId, i32), FinalizedAggregateLayout>,
}

impl NodeOutputCatalog {
    /// All finalized node outputs in canonical derivation order.
    pub(crate) fn outputs(&self) -> &[NodeExecutionOutput] {
        &self.outputs
    }

    /// The finalized output of the node identified by `(fragment_id, node_id)`,
    /// or `None` if that node is not a covered kind.
    pub(crate) fn output_for(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<&NodeExecutionOutput> {
        self.index
            .get(&(fragment_id, node_id))
            .map(|&index| &self.outputs[index])
    }

    /// The finalized `HashAggregate` wire layout of the node identified by
    /// `(fragment_id, node_id)`, or `None` if that node is not a `HashAggregate`.
    pub(crate) fn aggregate_layout(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<&FinalizedAggregateLayout> {
        self.aggregate_layouts.get(&(fragment_id, node_id))
    }
}

/// A reason node-output finalization refused to seal the plan.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::sql::planner::distributed) enum NodeOutputError {
    /// A covered node carries no output columns. The encoder used to fall back
    /// to a child- or type-derived schema here; finalization fails fast instead.
    MissingOutputColumns {
        fragment_id: FragmentId,
        node_id: i32,
        kind: NodeExecutionKind,
    },
    /// A `SetOp` node's per-child output schema list does not line up with its
    /// children (arity mismatch). The encoder used to re-derive a child's
    /// columns here; finalization fails fast instead.
    SetOpChildArityMismatch {
        fragment_id: FragmentId,
        node_id: i32,
        children: usize,
        child_output_columns: usize,
    },
    /// Two distinct nodes share a `(fragment_id, node_id)` identity, so the
    /// output cannot be keyed unambiguously. Structural invariants keep node ids
    /// unique within a fragment; this re-checks rather than silently overwriting.
    DuplicateNodeKey {
        fragment_id: FragmentId,
        node_id: i32,
    },
    /// A join reconciliation reached a child node kind whose execution output
    /// cannot be derived (mirrors the native encoder's fail-fast for the same
    /// kinds). No valid plan places such a node under a join.
    NonDerivableChildOutput {
        fragment_id: FragmentId,
        node_id: i32,
        kind: &'static str,
    },
    /// A unary passthrough node reached while deriving a join's execution output
    /// does not have exactly one child, so its output cannot be forwarded.
    PassthroughArityMismatch {
        fragment_id: FragmentId,
        node_id: i32,
        children: usize,
    },
    /// A stream/CTE/router edge projection references a source slot the source
    /// fragment root does not produce, and cannot be reconciled by the ordinal /
    /// wholesale-root fallbacks. The encoder used to fail here (its
    /// "stream schema reselection"); finalization fails fast at seal instead.
    StreamEdgeProjectionUnresolved {
        source_fragment_id: FragmentId,
        target_exchange_node_id: i32,
        slot_id: i32,
    },
    /// A stream/CTE/router edge slot id is negative and cannot be interpreted as a
    /// column id. Mirrors the encoder's slot-id conversion failure.
    StreamEdgeSlotIdOutOfRange {
        source_fragment_id: FragmentId,
        target_exchange_node_id: i32,
        slot_id: i32,
    },
    /// A `HashAggregate` node's wire output cannot be finalized: its
    /// `output_layout` aggregate-column count disagrees with its aggregate count, a
    /// partial-mode (`Local`/`Distinct*`) aggregate call exposes no intermediate
    /// type, or a visible output column is absent from the layout. Mirrors the
    /// native encoder's `hash_aggregate_wire_output_columns` fail-fasts, which
    /// CGO-9C Task 4 consolidated into [`finalize_hash_aggregate_wire`].
    AggregateIntermediateType {
        fragment_id: FragmentId,
        node_id: i32,
        detail: String,
    },
}

impl fmt::Display for NodeOutputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingOutputColumns {
                fragment_id,
                node_id,
                kind,
            } => write!(
                formatter,
                "distributed plan {kind} node fragment_id={fragment_id} node_id={node_id} has no execution output columns"
            ),
            Self::SetOpChildArityMismatch {
                fragment_id,
                node_id,
                children,
                child_output_columns,
            } => write!(
                formatter,
                "distributed plan set-op node fragment_id={fragment_id} node_id={node_id} declares {child_output_columns} child output schemas but has {children} children"
            ),
            Self::DuplicateNodeKey {
                fragment_id,
                node_id,
            } => write!(
                formatter,
                "distributed plan node fragment_id={fragment_id} node_id={node_id} is declared more than once"
            ),
            Self::NonDerivableChildOutput {
                fragment_id,
                node_id,
                kind,
            } => write!(
                formatter,
                "distributed plan {kind} node fragment_id={fragment_id} node_id={node_id} has no derivable execution output as a join child"
            ),
            Self::PassthroughArityMismatch {
                fragment_id,
                node_id,
                children,
            } => write!(
                formatter,
                "distributed plan passthrough node fragment_id={fragment_id} node_id={node_id} expected one child for output columns but has {children}"
            ),
            Self::StreamEdgeProjectionUnresolved {
                source_fragment_id,
                target_exchange_node_id,
                slot_id,
            } => write!(
                formatter,
                "distributed plan stream edge from fragment id={source_fragment_id} to exchange node_id={target_exchange_node_id} has a missing projection: source slot id {slot_id} is not produced by the source fragment root"
            ),
            Self::StreamEdgeSlotIdOutOfRange {
                source_fragment_id,
                target_exchange_node_id,
                slot_id,
            } => write!(
                formatter,
                "distributed plan stream edge from fragment id={source_fragment_id} to exchange node_id={target_exchange_node_id} has an out-of-range source slot id {slot_id}"
            ),
            Self::AggregateIntermediateType {
                fragment_id,
                node_id,
                detail,
            } => write!(
                formatter,
                "distributed plan hash-aggregate node fragment_id={fragment_id} node_id={node_id} cannot finalize its wire output: {detail}"
            ),
        }
    }
}

/// The finalized covered output of a node: its covered kind, its deduplicated
/// execution-output columns, and — for a `HashAggregate` — the full group-key +
/// aggregate-state wire layout the flat execution output does not capture.
struct CoveredNodeOutput {
    kind: NodeExecutionKind,
    columns: Vec<OutputColumn>,
    /// `Some` only for a `HashAggregate`. The finalized wire layout stored in the
    /// catalog alongside the execution output.
    aggregate_layout: Option<FinalizedAggregateLayout>,
}

/// Return the covered kind and finalized execution-output columns of a node, or
/// `None` for a node whose output this contract does not finalize.
///
/// Non-join covered outputs (`Scan`, `SetOp`, `Sort`) are read from the
/// planner-computed payload. Join outputs are *reconciled against the node's
/// children* (see [`derive_join_execution_output`]) rather than read verbatim,
/// because a join's payload `output_columns` can list ids no child produces at
/// execution. A `HashAggregate` output is finalized from its group-key +
/// aggregate-state layout with per-mode intermediate types applied, preferring
/// its visible `output_columns` when non-empty (see
/// [`finalize_hash_aggregate_wire`]). Every covered output is deduplicated by
/// column id so the wire schema the encoder emits has unique
/// `OutputColumn.column_id`s.
fn covered_node_output(
    node: &DistributedNode,
    fragment_roots: &FragmentRoots<'_>,
) -> Result<Option<CoveredNodeOutput>, NodeOutputError> {
    let (kind, columns, aggregate_layout) = match &node.payload {
        DistributedNodeKind::Scan(scan) => (
            NodeExecutionKind::Scan,
            scan_execution_output_columns(scan)?,
            None,
        ),
        DistributedNodeKind::HashJoin(join) => (
            NodeExecutionKind::HashJoin,
            derive_join_execution_output(
                join.join_type,
                &join.output_columns,
                node,
                fragment_roots,
            )?,
            None,
        ),
        DistributedNodeKind::NestLoopJoin(join) => (
            NodeExecutionKind::NestLoopJoin,
            derive_join_execution_output(
                join.join_type,
                &join.output_columns,
                node,
                fragment_roots,
            )?,
            None,
        ),
        DistributedNodeKind::SetOp(set_op) => (
            NodeExecutionKind::SetOp,
            set_op.output_columns.clone(),
            None,
        ),
        DistributedNodeKind::Sort(sort) => {
            (NodeExecutionKind::Sort, sort.output_columns.clone(), None)
        }
        DistributedNodeKind::HashAggregate(aggregate) => {
            let wire = finalize_hash_aggregate_wire(node, aggregate)?;
            (
                NodeExecutionKind::HashAggregate,
                wire.output_columns,
                Some(FinalizedAggregateLayout {
                    group_key_columns: wire.group_key_columns,
                    aggregate_columns: wire.aggregate_columns,
                }),
            )
        }
        _ => return Ok(None),
    };
    Ok(Some(CoveredNodeOutput {
        kind,
        columns: deduplicate_output_columns_by_id(columns),
        aggregate_layout,
    }))
}

/// Reconcile a join's execution output against its children.
///
/// The BE materializes a join's output chunk from the concatenation of its
/// children's execution schemas (with the null-able side made nullable for outer
/// joins, and only one side kept for semi/anti joins). The join's payload
/// `output_columns` are the planner-logical columns selected for it, but after
/// fragmentation and scan column pruning those ids can reference columns no child
/// emits. This recomputes the join output from the children's actual outputs and
/// keeps the payload list only when it already lists exactly the derived ids in
/// order (which preserves the payload's names/nullability); otherwise the derived
/// output wins. A join with anything other than two children keeps its payload
/// list verbatim (there is nothing to reconcile against).
///
/// This mirrors the semantics of the native encoder's now-removed
/// `normalize_join_output_columns` / `derive_join_output_columns`.
fn derive_join_execution_output(
    join_type: JoinKind,
    requested: &[OutputColumn],
    node: &DistributedNode,
    fragment_roots: &FragmentRoots<'_>,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    let [left, right] = node.children.as_slice() else {
        return Ok(requested.to_vec());
    };
    let left = node_execution_output_columns(left, fragment_roots)?;
    let right = node_execution_output_columns(right, fragment_roots)?;
    let derived = join_output_columns_from_children(join_type, left, right);
    if requested.is_empty() || !same_output_column_ids(requested, &derived) {
        Ok(derived)
    } else {
        Ok(requested.to_vec())
    }
}

/// Compute the logical execution-output columns a distributed node produces at
/// the BE. Used to reconcile a join against its children, so it must match the
/// BE's per-node output exactly. Fails fast on a node kind whose execution output
/// cannot be derived rather than guessing a default (CGO-9C deleted the encoder's
/// wire-side read walk, so this planner walk is the sole owner of that contract).
fn node_execution_output_columns(
    node: &DistributedNode,
    fragment_roots: &FragmentRoots<'_>,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    match &node.payload {
        DistributedNodeKind::Scan(scan) => scan_execution_output_columns(scan),
        DistributedNodeKind::Values(values) => Ok(values.columns.clone()),
        // Unary passthrough nodes forward their child's execution output.
        DistributedNodeKind::Filter(_)
        | DistributedNodeKind::Sort(_)
        | DistributedNodeKind::TopN(_)
        | DistributedNodeKind::AssertOneRow(_) => {
            unary_passthrough_output_columns(node, fragment_roots)
        }
        DistributedNodeKind::Project(project) => Ok(project_execution_output_columns(project)),
        DistributedNodeKind::HashAggregate(aggregate) => {
            // The aggregate's execution output is its visible-or-full output
            // columns with per-mode intermediate aggregate-state types applied
            // (the same finalization the covered node output uses), so a parent
            // join reconciled against it sees exactly what the BE aggregate emits.
            // `finalize_hash_aggregate_wire` prefers the visible `output_columns`
            // (a subset-by-id, possibly reordered, of the full layout — the
            // projection introduced by #551 "Project visible aggregate output
            // columns"), falling back to the full group-key + aggregate layout only
            // when it is empty.
            Ok(finalize_hash_aggregate_wire(node, aggregate)?.output_columns)
        }
        DistributedNodeKind::Window(window) => Ok(window.output_columns.clone()),
        DistributedNodeKind::GenerateSeries(generate_series) => {
            Ok(vec![generate_series_output_column(generate_series)])
        }
        DistributedNodeKind::TableFunction(table_function) => {
            let mut columns = unary_passthrough_output_columns(node, fragment_roots)?;
            columns.extend(table_function.output_columns.iter().cloned());
            Ok(columns)
        }
        DistributedNodeKind::SetOp(set_op) => Ok(set_op.output_columns.clone()),
        DistributedNodeKind::ChangeEventExpand(expand) => Ok(expand.output_columns.clone()),
        DistributedNodeKind::HashJoin(join) => {
            derive_join_execution_output(join.join_type, &join.output_columns, node, fragment_roots)
        }
        DistributedNodeKind::NestLoopJoin(join) => {
            derive_join_execution_output(join.join_type, &join.output_columns, node, fragment_roots)
        }
        DistributedNodeKind::Exchange(exchange) => {
            exchange_execution_output_columns(exchange, fragment_roots)
        }
        DistributedNodeKind::Repeat(_) => Err(NodeOutputError::NonDerivableChildOutput {
            fragment_id: node.fragment_id,
            node_id: node.node_id,
            kind: "repeat",
        }),
    }
}

/// An exchange receiver delivers exactly what its source fragment sends, so its
/// execution output is the source fragment root's execution output restricted to
/// the ids the receiver actually carries. The receiver's *declared*
/// `output_columns` can over-list ids the source pruned away (for example a probe
/// scan that only materializes its `required_columns`); intersecting with the
/// source root output drops those stale ids while keeping the receiver's declared
/// order and column metadata. Fixing up the receiver's own declared columns is a
/// separate concern (the encoder's exchange-receiver patch); this only computes
/// what a *parent join* actually sees.
fn exchange_execution_output_columns(
    exchange: &ExchangeReceiver,
    fragment_roots: &FragmentRoots<'_>,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    let Some(source_root) = fragment_roots.get(&exchange.source_fragment_id) else {
        // No source fragment in this catalog (should not happen for a sealed
        // plan); fall back to the declared columns rather than fail.
        return Ok(exchange.output_columns.clone());
    };
    let source_ids: HashSet<ColumnId> = node_execution_output_columns(source_root, fragment_roots)?
        .into_iter()
        .map(|column| column.column_id)
        .collect();
    let projected: Vec<OutputColumn> = exchange
        .output_columns
        .iter()
        .filter(|column| source_ids.contains(&column.column_id))
        .cloned()
        .collect();
    // A correct plan always has a non-empty intersection; if the source output
    // could not be reconciled against the declared columns, keep the declared
    // columns verbatim rather than emit an empty exchange output.
    if projected.is_empty() {
        Ok(exchange.output_columns.clone())
    } else {
        Ok(projected)
    }
}

/// A scan produces the payload columns it materializes, restricted to
/// `required_columns` when those prune the projected set (matching the BE read
/// plan). `required_columns` is `None`/empty when the scan materializes every
/// projected column.
///
/// `required_columns` is expected to name a subset of the projected columns. If
/// it matches none of them — an inconsistent scan (e.g. a projected column
/// renamed away from its binding) that a later binding stage rejects with a
/// precise message — fall back to the full projection rather than manufacture an
/// empty execution output here.
fn scan_execution_output_columns(
    scan: &PlanScanNode,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    let required = match &scan.required_columns {
        Some(required) if !required.is_empty() => required,
        _ => return Ok(scan.columns.clone()),
    };
    let required: HashSet<String> = required
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect();
    let pruned: Vec<OutputColumn> = scan
        .columns
        .iter()
        .filter(|column| required.contains(&column.name.to_ascii_lowercase()))
        .cloned()
        .collect();
    if pruned.is_empty() {
        Ok(scan.columns.clone())
    } else {
        Ok(pruned)
    }
}

/// A project produces exactly one output column per item.
fn project_execution_output_columns(project: &PlanProjectNode) -> Vec<OutputColumn> {
    project
        .items
        .iter()
        .map(|item| OutputColumn {
            column_id: item.output_column_id,
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
            is_internal: false,
        })
        .collect()
}

fn generate_series_output_column(generate_series: &PlanGenerateSeriesNode) -> OutputColumn {
    OutputColumn {
        column_id: generate_series.output_column_id,
        name: if generate_series.column_name.is_empty() {
            "generate_series".to_string()
        } else {
            generate_series.column_name.clone()
        },
        data_type: DataType::Int64,
        nullable: false,
        is_internal: false,
    }
}

fn unary_passthrough_output_columns(
    node: &DistributedNode,
    fragment_roots: &FragmentRoots<'_>,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    let [child] = node.children.as_slice() else {
        return Err(NodeOutputError::PassthroughArityMismatch {
            fragment_id: node.fragment_id,
            node_id: node.node_id,
            children: node.children.len(),
        });
    };
    node_execution_output_columns(child, fragment_roots)
}

/// Concatenate the children's outputs per join type: outer joins make the
/// null-able side nullable; semi/anti joins keep only the surviving side.
fn join_output_columns_from_children(
    join_type: JoinKind,
    left: Vec<OutputColumn>,
    right: Vec<OutputColumn>,
) -> Vec<OutputColumn> {
    match join_type {
        JoinKind::Inner | JoinKind::Cross => {
            let mut output = left;
            output.extend(right);
            output
        }
        JoinKind::LeftOuter => {
            let mut output = left;
            output.extend(nullable_output_columns(right));
            output
        }
        JoinKind::RightOuter => {
            let mut output = nullable_output_columns(left);
            output.extend(right);
            output
        }
        JoinKind::FullOuter => {
            let mut output = nullable_output_columns(left);
            output.extend(nullable_output_columns(right));
            output
        }
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => left,
        JoinKind::RightSemi | JoinKind::RightAnti => right,
    }
}

fn nullable_output_columns(mut columns: Vec<OutputColumn>) -> Vec<OutputColumn> {
    for column in &mut columns {
        column.nullable = true;
    }
    columns
}

fn same_output_column_ids(left: &[OutputColumn], right: &[OutputColumn]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| left.column_id == right.column_id)
}

/// Keep the first occurrence of each column id so the wire schema the encoder
/// emits has unique `OutputColumn.column_id`s (the BE rejects duplicates).
fn deduplicate_output_columns_by_id(columns: Vec<OutputColumn>) -> Vec<OutputColumn> {
    let mut seen = HashSet::with_capacity(columns.len());
    columns
        .into_iter()
        .filter(|column| seen.insert(column.column_id))
        .collect()
}

/// Derive the authoritative node execution-output catalog from already-sealed
/// fragments, the boundary catalog, and the continued occurrence allocator.
///
/// Runs inside `seal_draft` after `build_boundary_catalog`, so the boundary
/// occurrences already exist and the `allocator` is positioned one past the last
/// boundary occurrence. Non-join covered outputs are read from planner-computed
/// payloads; join outputs are reconciled against their children (see
/// [`covered_node_output`]). It never rebuilds the allocator.
///
/// Determinism: fragments are visited in declaration order and each fragment's
/// node tree in pre-order, so the single `allocator` assigns internal occurrence
/// ids in a fully input-determined sequence.
pub(in crate::sql::planner::distributed) fn build_node_output_catalog(
    fragments: &[PlanFragment],
    boundaries: &BoundaryCatalog,
    allocator: &mut ExecutionColumnIdAllocator,
) -> Result<NodeOutputCatalog, NodeOutputError> {
    // Fragment-level sink boundaries (result / Iceberg write input / change
    // stream router input) are keyed by fragment id and describe exactly the
    // fragment root's output. A covered root node reuses these occurrences.
    let sink_boundary_by_fragment: BTreeMap<FragmentId, &BoundaryContract> = boundaries
        .contracts()
        .iter()
        .filter(|contract| contract.node_id.is_none())
        .map(|contract| (contract.fragment_id, contract))
        .collect();

    // Fragment roots keyed by id, so an exchange receiver can resolve its
    // execution output to what its source fragment actually sends.
    let fragment_roots: FragmentRoots<'_> = fragments
        .iter()
        .map(|fragment| (fragment.fragment_id, &fragment.root))
        .collect();

    let mut outputs = Vec::new();
    let mut index = BTreeMap::new();
    let mut aggregate_layouts = BTreeMap::new();
    for fragment in fragments {
        let root_sink_boundary = sink_boundary_by_fragment
            .get(&fragment.fragment_id)
            .copied();
        visit_node(
            &fragment.root,
            fragment.fragment_id,
            true,
            root_sink_boundary,
            &fragment_roots,
            allocator,
            &mut outputs,
            &mut index,
            &mut aggregate_layouts,
        )?;
    }

    Ok(NodeOutputCatalog {
        outputs,
        index,
        aggregate_layouts,
    })
}

#[allow(clippy::too_many_arguments)]
fn visit_node(
    node: &DistributedNode,
    fragment_id: FragmentId,
    is_fragment_root: bool,
    root_sink_boundary: Option<&BoundaryContract>,
    fragment_roots: &FragmentRoots<'_>,
    allocator: &mut ExecutionColumnIdAllocator,
    outputs: &mut Vec<NodeExecutionOutput>,
    index: &mut BTreeMap<(FragmentId, i32), usize>,
    aggregate_layouts: &mut BTreeMap<(FragmentId, i32), FinalizedAggregateLayout>,
) -> Result<(), NodeOutputError> {
    if let Some(covered) = covered_node_output(node, fragment_roots)? {
        validate_node_output(fragment_id, node, covered.kind, &covered.columns)?;

        // Reuse the boundary occurrences only when this node is the fragment
        // root and the fragment's sink boundary carries exactly this node's
        // output (same length and per-ordinal logical column id). This holds for
        // result and change-stream router sinks and for by-ordinal Iceberg write
        // input; a reordered write projection or an exchange producer does not
        // match and is numbered as internal.
        let reuse = if is_fragment_root {
            root_sink_boundary
                .filter(|boundary| boundary_matches_node_output(boundary, &covered.columns))
        } else {
            None
        };

        let execution_columns = assign_occurrences(&covered.columns, reuse, allocator);
        let node_key = (fragment_id, node.node_id);
        if index.contains_key(&node_key) {
            return Err(NodeOutputError::DuplicateNodeKey {
                fragment_id,
                node_id: node.node_id,
            });
        }
        // A HashAggregate additionally stores its finalized group-key +
        // aggregate-state wire layout, which the flat execution output above does
        // not capture when a visible projection subsets it.
        if let Some(layout) = covered.aggregate_layout {
            aggregate_layouts.insert(node_key, layout);
        }
        index.insert(node_key, outputs.len());
        outputs.push(NodeExecutionOutput {
            fragment_id,
            node_id: node.node_id,
            kind: covered.kind,
            columns: execution_columns,
        });
    }

    for child in &node.children {
        visit_node(
            child,
            fragment_id,
            false,
            root_sink_boundary,
            fragment_roots,
            allocator,
            outputs,
            index,
            aggregate_layouts,
        )?;
    }
    Ok(())
}

/// Validate that a covered node carries a complete finalized output. The checks
/// are structural (non-empty after reconciliation/dedup, and — for `SetOp` — a
/// per-child schema whose arity matches the node's children).
fn validate_node_output(
    fragment_id: FragmentId,
    node: &DistributedNode,
    kind: NodeExecutionKind,
    columns: &[OutputColumn],
) -> Result<(), NodeOutputError> {
    ensure_output_columns_present(fragment_id, node.node_id, kind, columns)?;

    if let DistributedNodeKind::SetOp(set_op) = &node.payload {
        if set_op.child_output_columns.len() != node.children.len() {
            return Err(NodeOutputError::SetOpChildArityMismatch {
                fragment_id,
                node_id: node.node_id,
                children: node.children.len(),
                child_output_columns: set_op.child_output_columns.len(),
            });
        }
        for child_columns in &set_op.child_output_columns {
            ensure_output_columns_present(fragment_id, node.node_id, kind, child_columns)?;
        }
    }
    Ok(())
}

fn ensure_output_columns_present(
    fragment_id: FragmentId,
    node_id: i32,
    kind: NodeExecutionKind,
    columns: &[OutputColumn],
) -> Result<(), NodeOutputError> {
    if columns.is_empty() {
        return Err(NodeOutputError::MissingOutputColumns {
            fragment_id,
            node_id,
            kind,
        });
    }
    Ok(())
}

/// Whether a fragment-level sink boundary carries exactly the given node output:
/// same length and the same logical column id at every ordinal.
fn boundary_matches_node_output(boundary: &BoundaryContract, columns: &[OutputColumn]) -> bool {
    boundary.columns.len() == columns.len()
        && boundary
            .columns
            .iter()
            .zip(columns.iter())
            .all(|(boundary_column, column)| boundary_column.column_id == column.column_id)
}

/// Number each node output column as an occurrence: reuse the matching boundary
/// occurrence when `reuse` is set, otherwise allocate a fresh id from the shared
/// query-scoped allocator. When `reuse` is set it is guaranteed (by
/// [`boundary_matches_node_output`]) to have the same length as `columns`.
fn assign_occurrences(
    columns: &[OutputColumn],
    reuse: Option<&BoundaryContract>,
    allocator: &mut ExecutionColumnIdAllocator,
) -> Vec<NodeExecutionColumn> {
    columns
        .iter()
        .enumerate()
        .map(|(output_ordinal, column)| {
            let execution_column_id = match reuse {
                Some(boundary) => boundary.columns[output_ordinal].execution_column_id,
                None => allocator.allocate(),
            };
            NodeExecutionColumn {
                execution_column_id,
                column_id: column.column_id,
                output_ordinal,
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                is_internal: column.is_internal,
            }
        })
        .collect()
}

// ===========================================================================
// CGO-9C Task 2: fragment output + stream/CTE/router edge projection finalization
// ===========================================================================
//
// Where [`NodeOutputCatalog`] finalizes each covered execution node's output,
// this catalog finalizes the two remaining wire schemas the native encoder used
// to re-derive or repair at encode time:
//
//   * every fragment's output columns (`plan::PlanFragment.output_columns`), and
//   * every stream edge's projection (the source `DataStreamSink.output_columns`
//     together with the destination Exchange receiver's `output_columns`).
//
// Both are computed here from the sealed fragments, edges, and Task-1 node
// outputs, so the encoder maps the finalized contract 1:1 rather than walking the
// encoded tree, re-selecting a stream schema, or patching the receiver. The
// finalization is a faithful, planner-typed successor of the encoder's removed
// `encode_fragment_execution_output_columns` / `stream_edge_output_columns` /
// `patch_exchange_receiver_output_columns` and fails fast (rather than falling
// back) on an inconsistency.
//
// Iceberg write fragment outputs (their target-schema output columns/exprs) are
// finalized separately by CGO-9C Task 3's `WriteContractCatalog` (see the write
// contract section below), which the encoder likewise maps 1:1; the encoder's own
// write-output synthesis path has been removed.

/// Finalized fragment output columns and per-stream-edge projections for a
/// sealed distributed plan. The native encoder reads both verbatim.
///
/// (`OutputColumn` carries an arrow `DataType` and does not implement `Eq`, so
/// this catalog is compared structurally in tests via its `Debug` form.)
#[derive(Clone, Debug)]
pub(crate) struct FragmentEdgeOutputCatalog {
    /// Finalized `output_columns` per fragment, keyed by fragment id. Iceberg
    /// write fragments are absent (Task 3 owns their output schema).
    fragment_outputs: BTreeMap<FragmentId, Vec<OutputColumn>>,
    /// Finalized stream/CTE/router edge projection, keyed by the destination
    /// Exchange receiver's `(target_fragment_id, target_exchange_node_id)`. The
    /// sender sends exactly these column ids and the receiver carries exactly
    /// these columns, so the two sides are equal by construction.
    stream_edge_projections: BTreeMap<(FragmentId, i32), Vec<OutputColumn>>,
}

impl FragmentEdgeOutputCatalog {
    /// The finalized output columns of the fragment identified by `fragment_id`,
    /// or `None` for an Iceberg write fragment (whose output schema is owned by
    /// the `WriteContractCatalog`, finalized at seal in CGO-9C Task 3; the
    /// encoder only maps that contract 1:1).
    pub(crate) fn fragment_output_columns(
        &self,
        fragment_id: FragmentId,
    ) -> Option<&[OutputColumn]> {
        self.fragment_outputs
            .get(&fragment_id)
            .map(|columns| columns.as_slice())
    }

    #[cfg(test)]
    pub(in crate::sql::planner::distributed) fn remove_fragment_output_for_test(
        &mut self,
        fragment_id: FragmentId,
    ) {
        self.fragment_outputs.remove(&fragment_id);
    }

    /// The finalized projection of the stream edge whose destination Exchange
    /// receiver is `(target_fragment_id, target_exchange_node_id)`, or `None` for
    /// an Exchange node that is not the destination of a finalized stream edge
    /// (e.g. a CTE-multicast or change-stream-router receiver, whose columns come
    /// straight from its declared receiver schema).
    pub(crate) fn stream_edge_projection(
        &self,
        target_fragment_id: FragmentId,
        target_exchange_node_id: i32,
    ) -> Option<&[OutputColumn]> {
        self.stream_edge_projections
            .get(&(target_fragment_id, target_exchange_node_id))
            .map(|columns| columns.as_slice())
    }
}

/// Derive the fragment-output and stream-edge-projection catalog from the sealed
/// fragments, edges, and the already-built node-output catalog.
///
/// Runs inside `seal_draft` after [`build_node_output_catalog`], so covered node
/// outputs are available for the fragment-root wire walk to consume. It only
/// derives (from known-valid inputs) and fails fast on any inconsistency; it
/// never repairs a schema or guesses.
pub(in crate::sql::planner::distributed) fn build_fragment_edge_output_catalog(
    fragments: &[PlanFragment],
    edges: &[FragmentEdge],
    node_outputs: &NodeOutputCatalog,
) -> Result<FragmentEdgeOutputCatalog, NodeOutputError> {
    let mut fragment_outputs = BTreeMap::new();
    for fragment in fragments {
        // Connector writer output schema is sealed separately; skip it here so
        // the native encoder maps that contract 1:1.
        if matches!(
            fragment.sink,
            DataSink::ConnectorWrite(ref sink) if sink.output_contract.is_some()
        ) {
            continue;
        }
        let columns = finalize_fragment_output_columns(fragment, node_outputs);
        fragment_outputs.insert(fragment.fragment_id, columns);
    }

    let fragment_by_id: BTreeMap<FragmentId, &PlanFragment> = fragments
        .iter()
        .map(|fragment| (fragment.fragment_id, fragment))
        .collect();

    let mut stream_edge_projections = BTreeMap::new();
    for edge in edges {
        // Only stream edges have a source `DataStreamSink` whose projection the
        // encoder used to reselect and whose receiver it used to patch. CTE
        // multicast and change-stream-router receivers keep their declared
        // receiver schema (the encoder never patched them), so they are not
        // finalized here.
        if !matches!(edge.edge_kind, FragmentEdgeKind::Stream) {
            continue;
        }
        let source = fragment_by_id.get(&edge.source_fragment_id).ok_or(
            NodeOutputError::StreamEdgeProjectionUnresolved {
                source_fragment_id: edge.source_fragment_id,
                target_exchange_node_id: edge.target_exchange_node_id,
                slot_id: -1,
            },
        )?;
        let target = fragment_by_id.get(&edge.target_fragment_id).ok_or(
            NodeOutputError::StreamEdgeProjectionUnresolved {
                source_fragment_id: edge.source_fragment_id,
                target_exchange_node_id: edge.target_exchange_node_id,
                slot_id: -1,
            },
        )?;
        // Fail fast (like the source/target fragment lookups above) when the
        // edge's target Exchange node is absent. Structural/boundary derivation
        // already guarantees it resolves for a sealed plan, so this is a defensive
        // re-check: a legitimate zero-column stream still yields `Some(receiver)`
        // with empty `output_columns`, so `None` here means a genuinely mis-wired
        // edge, not an empty projection.
        let receiver_columns = find_exchange_receiver(&target.root, edge.target_exchange_node_id)
            .map(|receiver| receiver.output_columns.clone())
            .ok_or(NodeOutputError::StreamEdgeProjectionUnresolved {
                source_fragment_id: edge.source_fragment_id,
                target_exchange_node_id: edge.target_exchange_node_id,
                slot_id: -1,
            })?;
        let projection =
            finalize_stream_edge_projection(source, edge, &receiver_columns, node_outputs)?;
        stream_edge_projections.insert(
            (edge.target_fragment_id, edge.target_exchange_node_id),
            projection,
        );
    }

    Ok(FragmentEdgeOutputCatalog {
        fragment_outputs,
        stream_edge_projections,
    })
}

/// Whether a fragment forwards its execution root output wholesale (a producer
/// fragment feeding an exchange). Mirrors the encoder's `DataSink::Noop`
/// special-casing.
fn fragment_forwards_root_output(fragment: &PlanFragment) -> bool {
    matches!(fragment.sink, DataSink::Noop)
}

/// Finalize a fragment's output columns from its root's wire execution output,
/// reconciled with the fragment's declared output columns. Faithful planner-side
/// successor of the encoder's removed `encode_fragment_execution_output_columns`.
///
/// The declared `output_columns` is the planner's authoritative logical output;
/// the root wire output is a *refinement* that carries the correct metadata and
/// unique wire ids (`SELECT c1, c1`). The refinement is adopted only when it
/// cleanly corresponds to the declared output (same count, or a producer fragment
/// forwarding its root wholesale); otherwise the declared columns stand. This
/// preserves the encoder's behavior on every shape it accepted, including the
/// cases where the root wire output legitimately diverges from the declared
/// output at seal time: an underivable root (a `Repeat` grouping-set producer), a
/// `TableFunction` root whose wire output prepends its passthrough columns, and a
/// binding-driven scan whose physical projection is only resolved at codegen.
fn finalize_fragment_output_columns(
    fragment: &PlanFragment,
    node_outputs: &NodeOutputCatalog,
) -> Vec<OutputColumn> {
    let root_output = wire_node_output_columns(&fragment.root, fragment.fragment_id, node_outputs);
    let declared = &fragment.output_columns;
    if declared.is_empty() {
        // A producer (Noop) fragment forwards its root output wholesale; any other
        // empty declared output stays empty.
        return match root_output {
            Ok(root) if fragment_forwards_root_output(fragment) => root,
            _ => Vec::new(),
        };
    }
    match root_output {
        Ok(root)
            if !root.is_empty()
                && (root.len() == declared.len() || fragment_forwards_root_output(fragment)) =>
        {
            // The root schema corresponds to the declared output: adopt it for its
            // correct metadata and unique wire ids.
            root
        }
        // A count-mismatched, empty, or underivable root at seal time leaves the
        // planner's declared output authoritative. The empty-root and
        // underivable-root cases mirror the retired encoder twin directly. The
        // remaining case (non-empty derivable root whose count differs from the
        // declared output on a non-Noop fragment) was a hard *error* arm in that
        // twin, believed unreachable for real plans; it is folded into this
        // tolerant fallback here because at seal time it is also reached by
        // binding-driven scans whose physical projection is only resolved at
        // codegen. Do not re-add the hard error without accounting for bindings.
        _ => declared.clone(),
    }
}

/// Finalize a stream edge's projection: the source fragment root's wire output,
/// projected onto the destination receiver's declared columns when they resolve,
/// otherwise onto the edge's `output_slot_ids`. This is the planner-side
/// successor of the encoder's removed `stream_edge_output_columns`; the sender
/// (`DataStreamSink.output_columns`) sends these column ids and the receiver
/// carries these columns, so the two sides are equal by construction.
fn finalize_stream_edge_projection(
    source: &PlanFragment,
    edge: &FragmentEdge,
    receiver_columns: &[OutputColumn],
    node_outputs: &NodeOutputCatalog,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    if source.output_columns.is_empty()
        && edge.output_slot_ids.is_empty()
        && receiver_columns.is_empty()
    {
        return Ok(Vec::new());
    }
    let columns = match wire_node_output_columns(&source.root, source.fragment_id, node_outputs) {
        Ok(columns) if !columns.is_empty() => columns,
        Ok(_) if !source.output_columns.is_empty() => source.output_columns.clone(),
        Ok(_) => Vec::new(),
        Err(_) if !source.output_columns.is_empty() => source.output_columns.clone(),
        Err(root_err) => return Err(root_err),
    };
    if !receiver_columns.is_empty()
        && let Some(projected) = project_requested_exchange_columns(
            columns.clone(),
            &source.output_columns,
            receiver_columns,
            edge,
        )?
    {
        return Ok(projected);
    }
    project_edge_output_columns(columns, &source.output_columns, &edge.output_slot_ids, edge)
}

/// Project the source root wire output onto the destination receiver's declared
/// columns, returning `None` when they do not fully resolve so the caller falls
/// back to the edge's `output_slot_ids`. Faithful port of the encoder's
/// `project_output_columns_for_requested_exchange`.
fn project_requested_exchange_columns(
    columns: Vec<OutputColumn>,
    source_output_columns: &[OutputColumn],
    requested_columns: &[OutputColumn],
    edge: &FragmentEdge,
) -> Result<Option<Vec<OutputColumn>>, NodeOutputError> {
    if requested_columns.is_empty() {
        return Ok(None);
    }
    let requested_slot_ids = requested_columns
        .iter()
        .map(|column| {
            i32::try_from(column.column_id.0).map_err(|_| {
                NodeOutputError::StreamEdgeSlotIdOutOfRange {
                    source_fragment_id: edge.source_fragment_id,
                    target_exchange_node_id: edge.target_exchange_node_id,
                    slot_id: -1,
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    match project_edge_output_columns(columns, source_output_columns, &requested_slot_ids, edge) {
        Ok(projected) if projected.len() == requested_columns.len() => Ok(Some(projected)),
        Ok(_) => Ok(None),
        Err(NodeOutputError::StreamEdgeProjectionUnresolved { .. }) => Ok(None),
        Err(other) => Err(other),
    }
}

/// Project the source root wire output onto `output_slot_ids`. Faithful port of
/// the encoder's `project_output_columns_for_edge`: it first tries an
/// ordinal-based resolution through the fragment's declared output columns (so a
/// retagged fragment output still maps to the root schema), then a by-id
/// resolution, and finally falls back to the whole root output when the slot ids
/// are a stale superset; otherwise it fails fast.
fn project_edge_output_columns(
    columns: Vec<OutputColumn>,
    source_output_columns: &[OutputColumn],
    output_slot_ids: &[i32],
    edge: &FragmentEdge,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    if output_slot_ids.is_empty() {
        return Ok(columns);
    }
    let slot_to_column_id = |slot_id: i32| {
        u32::try_from(slot_id).map_err(|_| NodeOutputError::StreamEdgeSlotIdOutOfRange {
            source_fragment_id: edge.source_fragment_id,
            target_exchange_node_id: edge.target_exchange_node_id,
            slot_id,
        })
    };
    if !source_output_columns.is_empty() {
        let mut ordinals_by_column_id: HashMap<u32, Vec<usize>> = HashMap::new();
        for (idx, column) in source_output_columns.iter().enumerate() {
            ordinals_by_column_id
                .entry(column.column_id.0)
                .or_default()
                .push(idx);
        }
        let columns_by_id = columns
            .iter()
            .cloned()
            .map(|column| (column.column_id.0, column))
            .collect::<HashMap<_, _>>();
        let mut next_ordinal_by_column_id = HashMap::new();
        let mut resolved = Vec::with_capacity(output_slot_ids.len());
        let mut resolved_all_by_ordinal = true;
        for slot_id in output_slot_ids {
            let column_id = slot_to_column_id(*slot_id)?;
            let next = next_ordinal_by_column_id.entry(column_id).or_insert(0);
            let Some(ordinals) = ordinals_by_column_id.get(&column_id) else {
                resolved_all_by_ordinal = false;
                break;
            };
            let Some(ordinal) = ordinals.get(*next).copied() else {
                resolved_all_by_ordinal = false;
                break;
            };
            if ordinal >= source_output_columns.len() {
                resolved_all_by_ordinal = false;
                break;
            }
            *next += 1;
            let source_column = &source_output_columns[ordinal];
            let encoded = if ordinals.len() > 1 {
                columns
                    .get(ordinal)
                    .cloned()
                    .or_else(|| columns_by_id.get(&source_column.column_id.0).cloned())
            } else {
                columns_by_id
                    .get(&source_column.column_id.0)
                    .cloned()
                    .or_else(|| {
                        if source_output_columns.len() == columns.len() {
                            columns.get(ordinal).cloned()
                        } else {
                            None
                        }
                    })
            };
            let Some(encoded) = encoded else {
                resolved_all_by_ordinal = false;
                break;
            };
            resolved.push(encoded);
        }
        if resolved_all_by_ordinal {
            return Ok(resolved);
        }
    }
    let columns_by_id = columns
        .iter()
        .cloned()
        .map(|column| (column.column_id.0, column))
        .collect::<HashMap<_, _>>();
    let mut resolved = Vec::with_capacity(output_slot_ids.len());
    let mut missing_slot_id = None;
    for slot_id in output_slot_ids.iter().copied() {
        let column_id = slot_to_column_id(slot_id)?;
        if let Some(column) = columns_by_id.get(&column_id) {
            resolved.push(column.clone());
        } else {
            missing_slot_id = Some(slot_id);
            break;
        }
    }
    if missing_slot_id.is_none() {
        return Ok(resolved);
    }
    if output_slot_ids.len() >= columns.len() {
        return Ok(columns);
    }
    Err(NodeOutputError::StreamEdgeProjectionUnresolved {
        source_fragment_id: edge.source_fragment_id,
        target_exchange_node_id: edge.target_exchange_node_id,
        slot_id: missing_slot_id.expect("checked above"),
    })
}

/// Compute a node's wire execution output columns: the schema the BE materializes
/// for the node, with the boundary/projection uniqueness the wire requires. This
/// is used only to finalize fragment and stream-edge outputs (both start at a
/// fragment root).
///
/// Covered kinds (join / set-op / scan / hash-aggregate) read their finalized
/// output from the [`NodeOutputCatalog`], exactly the sealed schema the encoder
/// emits for them (a partial hash-aggregate's cataloged output already carries its
/// intermediate aggregate-state types). `Sort` (and the other unary passthroughs)
/// forward their child. `Project` re-materialization is made unique here (the
/// boundary/projection concern the node-output catalog defers to Task 2).
///
/// SOLE OWNER: CGO-9C Task 5 deleted the native encoder's read-walk twin
/// (`encoded_node_output_columns` / `encoded_fragment_root_output_columns` in
/// `protocol::native::encode::plan`) and migrated its last consumer -- the
/// coordinator's CTE-multicast sink, which now reads the fragment's sealed
/// `output_columns` directly. This planner walk is now the single implementation
/// of the wire per-node output; the encoder maps the sealed contract 1:1 and no
/// longer re-derives it.
fn wire_node_output_columns(
    node: &DistributedNode,
    fragment_id: FragmentId,
    node_outputs: &NodeOutputCatalog,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    match &node.payload {
        DistributedNodeKind::Project(project) => {
            wire_project_output_columns(node, project, fragment_id, node_outputs)
        }
        DistributedNodeKind::Filter(_)
        | DistributedNodeKind::AssertOneRow(_)
        | DistributedNodeKind::Sort(_)
        | DistributedNodeKind::TopN(_) => {
            wire_unary_passthrough_output_columns(node, fragment_id, node_outputs)
        }
        DistributedNodeKind::HashAggregate(aggregate) => {
            // HashAggregate is a covered kind (CGO-9C Task 4): read its finalized
            // visible-or-full execution output (with per-mode intermediate types
            // applied) from the node-output catalog, exactly the schema the encoder
            // emits for it. Fall back to recomputing the wire only if the aggregate
            // is somehow not cataloged (never in a sealed plan).
            match node_outputs.output_for(fragment_id, node.node_id) {
                Some(output) => Ok(output
                    .columns
                    .iter()
                    .map(node_execution_column_to_output)
                    .collect()),
                None => Ok(finalize_hash_aggregate_wire(node, aggregate)?.output_columns),
            }
        }
        DistributedNodeKind::GenerateSeries(generate_series) => {
            Ok(vec![generate_series_output_column(generate_series)])
        }
        DistributedNodeKind::Scan(scan) => {
            // Scan is a covered kind: read its sealed (required-pruned,
            // deduplicated) output from the Task-1 catalog, exactly the schema
            // the encoder emits for it. Fall back to the pruned projection only
            // if the scan is somehow not cataloged (never in a sealed plan).
            match node_outputs.output_for(fragment_id, node.node_id) {
                Some(output) => Ok(output
                    .columns
                    .iter()
                    .map(node_execution_column_to_output)
                    .collect()),
                None => scan_execution_output_columns(scan),
            }
        }
        DistributedNodeKind::TableFunction(table_function) => {
            let mut columns =
                wire_unary_passthrough_output_columns(node, fragment_id, node_outputs)?;
            columns.extend(table_function.output_columns.iter().cloned());
            Ok(columns)
        }
        DistributedNodeKind::Values(values) => Ok(values.columns.clone()),
        DistributedNodeKind::Window(window) => Ok(window.output_columns.clone()),
        DistributedNodeKind::ChangeEventExpand(expand) => Ok(expand.output_columns.clone()),
        DistributedNodeKind::HashJoin(join) => {
            sealed_covered_output(node, fragment_id, node_outputs, &join.output_columns)
        }
        DistributedNodeKind::NestLoopJoin(join) => {
            sealed_covered_output(node, fragment_id, node_outputs, &join.output_columns)
        }
        DistributedNodeKind::SetOp(set_op) => {
            sealed_covered_output(node, fragment_id, node_outputs, &set_op.output_columns)
        }
        DistributedNodeKind::Exchange(exchange) => Ok(exchange.output_columns.clone()),
        DistributedNodeKind::Repeat(_) => Err(NodeOutputError::NonDerivableChildOutput {
            fragment_id,
            node_id: node.node_id,
            kind: "repeat",
        }),
    }
}

/// The sealed execution output of a covered node (join / set-op), read from the
/// Task-1 catalog so the wire walk sees exactly what the encoder emits for it.
/// Falls back to the node's payload output columns if the node is not cataloged
/// (defensive; covered nodes are always cataloged in a sealed plan).
fn sealed_covered_output(
    node: &DistributedNode,
    fragment_id: FragmentId,
    node_outputs: &NodeOutputCatalog,
    payload_output_columns: &[OutputColumn],
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    match node_outputs.output_for(fragment_id, node.node_id) {
        Some(output) => Ok(output
            .columns
            .iter()
            .map(node_execution_column_to_output)
            .collect()),
        None => Ok(payload_output_columns.to_vec()),
    }
}

fn node_execution_column_to_output(column: &NodeExecutionColumn) -> OutputColumn {
    OutputColumn {
        column_id: column.column_id,
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
        is_internal: column.is_internal,
    }
}

/// The finalized wire encoding of a `HashAggregate` node: its group-key columns,
/// its aggregate-state columns (with per-mode intermediate types applied), and its
/// visible-or-full output columns (with data types remapped from the full layout).
struct FinalizedAggregateWire {
    group_key_columns: Vec<OutputColumn>,
    aggregate_columns: Vec<OutputColumn>,
    output_columns: Vec<OutputColumn>,
}

/// Finalize a `HashAggregate` node's full wire encoding. This is the single
/// planner-side owner of the aggregate intermediate-output determination the
/// native encoder used to perform in `hash_aggregate_wire_output_columns`: a
/// partial-mode aggregate (`Local` / `Distinct*`) emits its aggregate columns as
/// their intermediate aggregate-state type, and the terminal modes
/// (`Single` / `Global`) emit the final result type. The planner-typed aggregate
/// adapters ([`hash_aggregate_outputs_intermediate`] / [`aggregate_intermediate_type`]
/// in `crate::sql::planner::physical`), which delegate to the canonical type
/// contract in `novarocks_types::aggregate`, are reused; no inference is duplicated here.
///
/// The `output_columns` view prefers the aggregate's visible `output_columns`
/// (a subset-by-id, possibly reordered, of the full layout) when non-empty,
/// falling back to the full group-key + aggregate layout, matching the encoder
/// twin exactly. Fails fast (rather than falling back) on an aggregate-column
/// arity mismatch or a visible output column absent from the layout.
fn finalize_hash_aggregate_wire(
    node: &DistributedNode,
    aggregate: &PhysicalHashAggregateNode,
) -> Result<FinalizedAggregateWire, NodeOutputError> {
    // Validate arity unconditionally, matching the encoder twin (which fails fast
    // before any mode branch), so a malformed layout is rejected at seal even in a
    // final/single mode where no per-aggregate zip runs below.
    if aggregate.output_layout.aggregate_columns.len() != aggregate.aggregates.len() {
        return Err(NodeOutputError::AggregateIntermediateType {
            fragment_id: node.fragment_id,
            node_id: node.node_id,
            detail: format!(
                "output_layout aggregate column count {} does not match aggregate count {}",
                aggregate.output_layout.aggregate_columns.len(),
                aggregate.aggregates.len()
            ),
        });
    }
    let group_key_columns = aggregate.output_layout.group_key_columns.clone();
    let mut aggregate_columns = aggregate.output_layout.aggregate_columns.clone();
    if hash_aggregate_outputs_intermediate(aggregate.mode) {
        for (column, call) in aggregate_columns
            .iter_mut()
            .zip(aggregate.aggregates.iter())
        {
            column.data_type = aggregate_intermediate_type(call).map_err(|detail| {
                NodeOutputError::AggregateIntermediateType {
                    fragment_id: node.fragment_id,
                    node_id: node.node_id,
                    detail,
                }
            })?;
        }
    }

    let mut full_output_columns = group_key_columns.clone();
    full_output_columns.extend(aggregate_columns.iter().cloned());

    let output_columns = if aggregate.output_columns.is_empty() {
        full_output_columns
    } else {
        let data_type_by_id = full_output_columns
            .iter()
            .map(|column| (column.column_id, column.data_type.clone()))
            .collect::<HashMap<_, _>>();
        let mut output_columns = aggregate.output_columns.clone();
        for column in &mut output_columns {
            let data_type = data_type_by_id.get(&column.column_id).ok_or_else(|| {
                NodeOutputError::AggregateIntermediateType {
                    fragment_id: node.fragment_id,
                    node_id: node.node_id,
                    detail: format!(
                        "output column {} is missing from output_layout",
                        column.column_id.0
                    ),
                }
            })?;
            column.data_type = data_type.clone();
        }
        output_columns
    };

    Ok(FinalizedAggregateWire {
        group_key_columns,
        aggregate_columns,
        output_columns,
    })
}

fn wire_unary_passthrough_output_columns(
    node: &DistributedNode,
    fragment_id: FragmentId,
    node_outputs: &NodeOutputCatalog,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    let [child] = node.children.as_slice() else {
        return Err(NodeOutputError::PassthroughArityMismatch {
            fragment_id,
            node_id: node.node_id,
            children: node.children.len(),
        });
    };
    wire_node_output_columns(child, fragment_id, node_outputs)
}

struct WireProjectItemOutput {
    preferred_compute_column_id: u32,
    output_column_id: u32,
    can_reuse_input_slot: bool,
    output_name: String,
    data_type: DataType,
    nullable: bool,
}

/// Compute a `Project` node's wire output columns, making re-materialized columns
/// unique. This is now the sole owner of the logic (formerly the encoder's
/// now-removed `encoded_project_output_columns`): `SELECT a, a` allocates a
/// distinct wire id for the repeated output so the BE never sees a duplicate
/// `OutputColumn` id.
fn wire_project_output_columns(
    node: &DistributedNode,
    project: &PlanProjectNode,
    fragment_id: FragmentId,
    node_outputs: &NodeOutputCatalog,
) -> Result<Vec<OutputColumn>, NodeOutputError> {
    let item_outputs = project
        .items
        .iter()
        .map(wire_project_item_output)
        .collect::<Vec<_>>();
    let input_column_ids = match node.children.as_slice() {
        [] => HashSet::new(),
        [child] => wire_node_output_columns(child, fragment_id, node_outputs)?
            .into_iter()
            .map(|column| column.column_id.0)
            .collect::<HashSet<_>>(),
        _ => {
            return Err(NodeOutputError::PassthroughArityMismatch {
                fragment_id,
                node_id: node.node_id,
                children: node.children.len(),
            });
        }
    };
    let output_column_id_candidates = item_outputs
        .iter()
        .map(|item| item.output_column_id)
        .collect::<HashSet<_>>();
    let mut used_output_column_ids = HashSet::new();
    let mut used_compute_column_ids = input_column_ids.clone();
    let mut next_synthetic_column_id = output_column_id_candidates
        .iter()
        .chain(used_compute_column_ids.iter())
        .copied()
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    let mut first_expr_index_by_column_id = HashMap::new();
    let mut computed_columns = Vec::new();
    let mut output_columns = Vec::with_capacity(project.items.len());

    for item in item_outputs {
        let preferred_compute_column_id = item.preferred_compute_column_id;
        let mut compute_column_id = if item.can_reuse_input_slot
            || !input_column_ids.contains(&preferred_compute_column_id)
        {
            preferred_compute_column_id
        } else {
            allocate_project_synthetic_column_id(
                &mut next_synthetic_column_id,
                &mut used_output_column_ids,
                &mut used_compute_column_ids,
            )
        };
        if !item.can_reuse_input_slot && used_compute_column_ids.contains(&compute_column_id) {
            compute_column_id = allocate_project_synthetic_column_id(
                &mut next_synthetic_column_id,
                &mut used_output_column_ids,
                &mut used_compute_column_ids,
            );
        }

        if item.can_reuse_input_slot
            && first_expr_index_by_column_id.contains_key(&compute_column_id)
        {
            // Repeated slot-ref projections share the same computed value but
            // still need distinct visible output slots below.
        } else {
            let computed_idx = computed_columns.len();
            first_expr_index_by_column_id.insert(compute_column_id, computed_idx);
            used_compute_column_ids.insert(compute_column_id);
            computed_columns.push(compute_column_id);
        }

        let output_column_id = if used_output_column_ids.insert(item.output_column_id) {
            item.output_column_id
        } else {
            allocate_project_synthetic_column_id(
                &mut next_synthetic_column_id,
                &mut used_output_column_ids,
                &mut used_compute_column_ids,
            )
        };
        output_columns.push(OutputColumn {
            column_id: ColumnId(output_column_id),
            name: item.output_name,
            data_type: item.data_type,
            nullable: item.nullable,
            is_internal: false,
        });
    }

    Ok(output_columns)
}

fn wire_project_item_output(item: &crate::sql::analysis::ProjectItem) -> WireProjectItemOutput {
    let (preferred_compute_column_id, can_reuse_input_slot) = match &item.expr.kind {
        ExprKind::ColumnRef { column_id, .. } => (column_id.0, true),
        _ => (item.output_column_id.0, false),
    };
    WireProjectItemOutput {
        preferred_compute_column_id,
        output_column_id: item.output_column_id.0,
        can_reuse_input_slot,
        output_name: item.output_name.clone(),
        data_type: item.expr.data_type.clone(),
        nullable: item.expr.nullable,
    }
}

fn allocate_project_synthetic_column_id(
    next_synthetic_column_id: &mut u32,
    used_output_column_ids: &mut HashSet<u32>,
    used_compute_column_ids: &mut HashSet<u32>,
) -> u32 {
    while used_output_column_ids.contains(next_synthetic_column_id)
        || used_compute_column_ids.contains(next_synthetic_column_id)
    {
        *next_synthetic_column_id = next_synthetic_column_id.saturating_add(1);
    }
    let synthetic = *next_synthetic_column_id;
    used_output_column_ids.insert(synthetic);
    used_compute_column_ids.insert(synthetic);
    *next_synthetic_column_id = next_synthetic_column_id.saturating_add(1);
    synthetic
}

/// Depth-first search for the [`ExchangeReceiver`] with `node_id` in a fragment's
/// node tree.
fn find_exchange_receiver(node: &DistributedNode, node_id: i32) -> Option<&ExchangeReceiver> {
    if node.node_id == node_id
        && let DistributedNodeKind::Exchange(receiver) = &node.payload
    {
        return Some(receiver);
    }
    node.children
        .iter()
        .find_map(|child| find_exchange_receiver(child, node_id))
}

// ===========================================================================
// CGO-9C Task 3: Iceberg write output/target-schema + change-stream router
// branch partition finalization
// ===========================================================================
//
// Where [`NodeOutputCatalog`] and [`FragmentEdgeOutputCatalog`] finalize the
// query-path node and fragment/edge wire schemas, this catalog finalizes the
// two write-path semantic facts the native encoder used to *synthesize* or
// *reconstruct* at encode time:
//
//   * every Iceberg write fragment's output expressions and target output
//     schema (the encoder's `encode_iceberg_write_sink_output_columns` and its
//     `output_exprs`/input-binding fallback), and
//   * every change-stream router branch's typed partition expression (the
//     encoder rebuilt this from `output_partition_ordinals` against the router
//     fragment's output columns at encode time).
//
// Both are computed here from the sealed fragments and their sinks, so the
// encoder maps the finalized contract 1:1 rather than synthesizing a schema or
// reconstructing a partition from ordinals. This is a faithful, planner-typed
// successor of the encoder's removed write-output synthesis and router
// ordinal->expression reconstruction: it fails fast (rather than falling back)
// on an inconsistency. The `output_partition_ordinals` still travel on the wire
// unchanged (mapped 1:1 by the encoder); only the semantic reconstruction moves
// here.
//
// This finalization depends only on the sealed fragments/sinks. Unlike the
// query-path catalogs it allocates no new [`ExecutionColumnId`] occurrences: the
// write output expressions and router partition expressions reference existing
// logical [`ColumnId`]s, and the target schema carries positional target-field
// ids, not occurrences. The Iceberg write *input* occurrences it would need are
// already numbered by the boundary catalog's `IcebergWriteInput` /
// `ChangeStreamRouterInput` contracts.

/// One column of a finalized Iceberg write sink's target output schema.
///
/// Unlike a [`NodeExecutionColumn`] or [`OutputColumn`], `column_id` here is the
/// write's positional target-field id (the 1-based ordinal of the column within
/// the target columns), not a logical [`ColumnId`] occurrence: it is the wire id
/// the write sink's output schema carries, matching the target table's field
/// positions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FinalizedWriteTargetColumn {
    pub column_id: u32,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_internal: bool,
}

/// The finalized output contract of a connector write fragment: the expressions
/// the fragment feeds into its write sink, and the sink's target output schema.
/// The native encoder maps both 1:1.
#[derive(Clone, Debug)]
pub(crate) struct ConnectorWriteOutputContract {
    /// The write output expressions, evaluated against the fragment's execution
    /// output. Always populated: either the fragment's declared `output_exprs`
    /// (validated against the target arity) or, when the fragment declares none,
    /// column references into the sink's bound input columns (the encoder's
    /// removed input-binding fallback).
    pub output_exprs: Vec<TypedExpr>,
    /// The write sink's target output schema, carrying positional target-field
    /// ids. Reproduces the encoder's removed `encode_iceberg_write_sink_output_columns`.
    pub target_schema: Vec<FinalizedWriteTargetColumn>,
}

/// Finalized write-path contracts for a sealed distributed plan: Iceberg write
/// fragment outputs and change-stream router branch partitions. The native
/// encoder reads both instead of synthesizing/reconstructing them.
///
/// (`TypedExpr` / `DataPartition` carry an arrow `DataType` and do not implement
/// `Eq`, so this catalog is `Clone, Debug` only, like [`FragmentEdgeOutputCatalog`].)
#[derive(Clone, Debug)]
pub(crate) struct WriteContractCatalog {
    /// Finalized Iceberg write output/target-schema, keyed by fragment id. Only
    /// Connector-write fragments with a frozen output contract are present.
    connector_write_outputs: BTreeMap<FragmentId, ConnectorWriteOutputContract>,
    /// Finalized change-stream router branch partition, keyed by the router
    /// fragment id and the branch id. Only `DataSink::ChangeStreamRouter`
    /// fragments contribute entries.
    router_branch_partitions: BTreeMap<(FragmentId, i32), DataPartition>,
}

impl WriteContractCatalog {
    /// The finalized write output/target-schema of the Iceberg write fragment
    /// identified by `fragment_id`, or `None` for a non-write fragment.
    pub(crate) fn connector_write_output(
        &self,
        fragment_id: FragmentId,
    ) -> Option<&ConnectorWriteOutputContract> {
        self.connector_write_outputs.get(&fragment_id)
    }

    /// The finalized partition of the change-stream router branch identified by
    /// `(fragment_id, branch_id)`, or `None` if that fragment is not a router or
    /// the branch id is unknown.
    pub(crate) fn router_branch_partition(
        &self,
        fragment_id: FragmentId,
        branch_id: i32,
    ) -> Option<&DataPartition> {
        self.router_branch_partitions.get(&(fragment_id, branch_id))
    }
}

/// A reason write-contract finalization refused to seal the plan. Each arm
/// reproduces a fail-fast the native encoder performed while synthesizing the
/// write output or reconstructing a router partition.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::sql::planner::distributed) enum WriteContractError {
    /// An Iceberg write fragment declares `output_exprs` whose count does not
    /// match the sink's target column count. Mirrors the encoder's arity check.
    WriteOutputExprArity {
        fragment_id: FragmentId,
        output_exprs: usize,
        target_columns: usize,
    },
    /// An Iceberg write fragment declares no `output_exprs`, and its sink-bound
    /// input column count does not match the sink's target column count. Mirrors
    /// the encoder's input-binding arity check.
    WriteInputArity {
        fragment_id: FragmentId,
        input_columns: usize,
        target_columns: usize,
    },
    /// An Iceberg write sink `OutputOrdinals` binding references a fragment
    /// output ordinal that does not exist. Mirrors the encoder's fallback
    /// out-of-range check (the boundary catalog also rejects this earlier at
    /// seal; this is the defensive twin for the write finalization).
    WriteInputOrdinalOutOfRange {
        fragment_id: FragmentId,
        ordinal: usize,
        available: usize,
    },
    /// A write sink target column count overflows the positional wire id space.
    WriteTargetColumnIdOverflow {
        fragment_id: FragmentId,
        ordinal: usize,
    },
    /// A change-stream router branch partition ordinal references a router
    /// fragment output column that does not exist. Mirrors the encoder's
    /// ordinal->expression reconstruction out-of-range check.
    RouterPartitionOrdinalOutOfRange {
        fragment_id: FragmentId,
        branch_id: i32,
        ordinal: usize,
        available: usize,
    },
}

impl fmt::Display for WriteContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriteOutputExprArity {
                fragment_id,
                output_exprs,
                target_columns,
            } => write!(
                formatter,
                "distributed plan Iceberg write fragment id={fragment_id} output_exprs count {output_exprs} does not match target column count {target_columns}"
            ),
            Self::WriteInputArity {
                fragment_id,
                input_columns,
                target_columns,
            } => write!(
                formatter,
                "distributed plan Iceberg write fragment id={fragment_id} sink input column count {input_columns} does not match target column count {target_columns}"
            ),
            Self::WriteInputOrdinalOutOfRange {
                fragment_id,
                ordinal,
                available,
            } => write!(
                formatter,
                "distributed plan Iceberg write fragment id={fragment_id} sink input references output ordinal {ordinal} but only {available} output columns exist"
            ),
            Self::WriteTargetColumnIdOverflow {
                fragment_id,
                ordinal,
            } => write!(
                formatter,
                "distributed plan Iceberg write fragment id={fragment_id} target column ordinal {ordinal} overflows the wire column id space"
            ),
            Self::RouterPartitionOrdinalOutOfRange {
                fragment_id,
                branch_id,
                ordinal,
                available,
            } => write!(
                formatter,
                "distributed plan change-stream router fragment id={fragment_id} branch {branch_id} partition references output ordinal {ordinal} but only {available} output columns exist"
            ),
        }
    }
}

/// Derive the write-path finalization catalog from the sealed fragments.
///
/// Runs inside `seal_draft` after the boundary/node/fragment-edge catalogs. It
/// only *derives* from the fragments' sinks and output columns/exprs, and fails
/// fast on any inconsistency; it never synthesizes a schema, reconstructs a
/// partition from ordinals against a guessed source, or falls back.
pub(in crate::sql::planner::distributed) fn build_write_contract_catalog(
    fragments: &[PlanFragment],
) -> Result<WriteContractCatalog, WriteContractError> {
    let mut connector_write_outputs = BTreeMap::new();
    let mut router_branch_partitions = BTreeMap::new();

    for fragment in fragments {
        match &fragment.sink {
            DataSink::ConnectorWrite(sink) => {
                if let Some(contract) = &sink.output_contract {
                    connector_write_outputs.insert(fragment.fragment_id, contract.clone());
                }
            }
            DataSink::ChangeStreamRouter(router) => {
                for branch in &router.branches {
                    let partition = finalize_router_branch_partition(fragment, branch)?;
                    router_branch_partitions
                        .insert((fragment.fragment_id, branch.branch_id), partition);
                }
            }
            DataSink::Result | DataSink::Noop | DataSink::Statistics(_) => {}
        }
    }

    Ok(WriteContractCatalog {
        connector_write_outputs,
        router_branch_partitions,
    })
}

/// Finalize an Iceberg write fragment's output expressions and target schema,
/// reproducing the encoder's removed write-output synthesis.
pub(crate) fn finalize_iceberg_write_output(
    fragment: &PlanFragment,
    sink: &IcebergWritePlanInput,
) -> Result<ConnectorWriteOutputContract, WriteContractError> {
    let target_columns = &sink.spec.target_columns;
    let input_columns =
        iceberg_write_input_columns(fragment.fragment_id, &fragment.output_columns, &sink.input)?;
    let position_delete_mode = matches!(
        sink.spec.mode,
        crate::sql::planner::distributed::write::sink::IcebergWriteSinkMode::PositionDeletes
            | crate::sql::planner::distributed::write::sink::IcebergWriteSinkMode::DeletionVectors
    );
    let position_delete_input =
        position_delete_mode && matches!(sink.input, ConnectorWriteInputBinding::OutputOrdinals(_));

    if position_delete_input {
        // A position/DV writer consumes only the physical row identity columns
        // selected by `OutputOrdinals`.  Its provider handle carries the
        // target-table and partition metadata, so projecting the user table's
        // complete schema here would overwrite the native fragment output with
        // columns that do not exist in the terminal stream.  This remains a
        // generic connector input contract after the placement-time sink patch.
        let output_exprs = match fragment.output_exprs.as_ref() {
            Some(exprs) if exprs.len() == input_columns.len() => exprs.clone(),
            Some(exprs) => {
                return Err(WriteContractError::WriteOutputExprArity {
                    fragment_id: fragment.fragment_id,
                    output_exprs: exprs.len(),
                    target_columns: input_columns.len(),
                });
            }
            None => input_columns.iter().copied().map(column_ref_expr).collect(),
        };
        return Ok(ConnectorWriteOutputContract {
            output_exprs,
            target_schema: finalize_write_input_schema(fragment.fragment_id, &input_columns)?,
        });
    }

    // The write output expressions: the fragment's declared `output_exprs` when
    // present (validated against the target arity), otherwise column references
    // into the sink's bound input columns. This is the planner-side twin of the
    // encoder's `output_exprs`/input-binding branch.
    let output_exprs = match fragment.output_exprs.as_ref() {
        Some(exprs) => {
            if exprs.len() != target_columns.len() {
                return Err(WriteContractError::WriteOutputExprArity {
                    fragment_id: fragment.fragment_id,
                    output_exprs: exprs.len(),
                    target_columns: target_columns.len(),
                });
            }
            exprs.clone()
        }
        None => {
            if input_columns.len() != target_columns.len() {
                return Err(WriteContractError::WriteInputArity {
                    fragment_id: fragment.fragment_id,
                    input_columns: input_columns.len(),
                    target_columns: target_columns.len(),
                });
            }
            input_columns.into_iter().map(column_ref_expr).collect()
        }
    };

    let target_schema = finalize_write_target_schema(fragment.fragment_id, target_columns)?;
    Ok(ConnectorWriteOutputContract {
        output_exprs,
        target_schema,
    })
}

/// Finalize the schema of a narrowed position/DV input stream.  The wire IDs
/// remain positional, while names and types follow the concrete physical
/// stream columns selected by the fragment binding.
fn finalize_write_input_schema(
    fragment_id: FragmentId,
    input_columns: &[&OutputColumn],
) -> Result<Vec<FinalizedWriteTargetColumn>, WriteContractError> {
    input_columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let column_id = idx
                .checked_add(1)
                .and_then(|ordinal| u32::try_from(ordinal).ok())
                .ok_or(WriteContractError::WriteTargetColumnIdOverflow {
                    fragment_id,
                    ordinal: idx,
                })?;
            Ok(FinalizedWriteTargetColumn {
                column_id,
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                is_internal: column.is_internal,
            })
        })
        .collect()
}

/// Select the fragment output columns that feed the write sink, per its input
/// binding. Planner-side twin of the encoder's removed
/// `iceberg_write_sink_columns_for_input`.
fn iceberg_write_input_columns<'a>(
    fragment_id: FragmentId,
    output_columns: &'a [OutputColumn],
    input: &ConnectorWriteInputBinding,
) -> Result<Vec<&'a OutputColumn>, WriteContractError> {
    match input {
        ConnectorWriteInputBinding::RootOutputByOrdinal => Ok(output_columns.iter().collect()),
        ConnectorWriteInputBinding::OutputOrdinals(ordinals) => ordinals
            .iter()
            .copied()
            .map(|ordinal| {
                output_columns
                    .get(ordinal)
                    .ok_or(WriteContractError::WriteInputOrdinalOutOfRange {
                        fragment_id,
                        ordinal,
                        available: output_columns.len(),
                    })
            })
            .collect(),
    }
}

/// Finalize the write sink's target output schema. Positional target-field ids
/// are assigned as `ordinal + 1`, reproducing the encoder's removed
/// `encode_iceberg_write_sink_output_columns`.
fn finalize_write_target_schema(
    fragment_id: FragmentId,
    target_columns: &[ColumnDef],
) -> Result<Vec<FinalizedWriteTargetColumn>, WriteContractError> {
    target_columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let column_id = idx
                .checked_add(1)
                .and_then(|ordinal| u32::try_from(ordinal).ok())
                .ok_or(WriteContractError::WriteTargetColumnIdOverflow {
                    fragment_id,
                    ordinal: idx,
                })?;
            Ok(FinalizedWriteTargetColumn {
                column_id,
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                is_internal: false,
            })
        })
        .collect()
}

/// Finalize a change-stream router branch's partition from the router fragment's
/// output columns and the branch's partition ordinals. Planner-side successor of
/// the encoder's removed ordinal->expression reconstruction: an empty ordinal
/// list is unpartitioned, otherwise a hash partition over column references into
/// the router fragment's output columns.
fn finalize_router_branch_partition(
    fragment: &PlanFragment,
    branch: &ChangeStreamBranchRoute,
) -> Result<DataPartition, WriteContractError> {
    if branch.output_partition_ordinals.is_empty() {
        return Ok(DataPartition::unpartitioned());
    }
    let mut exprs = Vec::with_capacity(branch.output_partition_ordinals.len());
    for ordinal in &branch.output_partition_ordinals {
        let column = fragment.output_columns.get(*ordinal).ok_or(
            WriteContractError::RouterPartitionOrdinalOutOfRange {
                fragment_id: fragment.fragment_id,
                branch_id: branch.branch_id,
                ordinal: *ordinal,
                available: fragment.output_columns.len(),
            },
        )?;
        exprs.push(column_ref_expr(column));
    }
    Ok(DataPartition::hash(exprs))
}

/// A typed column-reference expression for an output column. Planner-side twin
/// of the encoder's removed `column_ref_expr_for_output_column`.
fn column_ref_expr(column: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: column.column_id,
            qualifier: None,
            column: column.name.clone(),
        },
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::NodeExecutionKind;
    use crate::sql::analysis::{
        ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::ChangeStreamBranchKind;
    use crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder;
    use crate::sql::planner::distributed::write::change_stream::{
        ChangeStreamBranchRoute, ChangeStreamRouterSink,
    };
    use crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec;
    use crate::sql::planner::distributed::write::sink::{
        ConnectorWriteInputBinding, IcebergWritePlanInput, IcebergWriteSinkMode,
    };
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan,
        ExchangeFlavor, ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
        PartitionKind, PlanFragment,
    };
    use crate::sql::planner::payload::{
        AggregateCall, PlanProjectNode, PlanScanNode, PlanSortNode, PlanValuesNode,
    };
    use crate::sql::planner::physical::{
        AggMode, AggregateOutputLayout, JoinDistribution, PhysicalHashAggregateNode,
        PhysicalHashJoinNode, PhysicalNestLoopJoinNode, PhysicalPlanStats, PhysicalSetOpNode,
        PlanSetOpKind, PlannerConfidence,
    };
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

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
            nullable: false,
            is_internal: true,
        }
    }

    fn node_in(
        fragment_id: u32,
        node_id: i32,
        children: Vec<DistributedNode>,
        payload: DistributedNodeKind,
    ) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: vec![node_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children,
            stats: stats(),
            payload,
        }
    }

    fn node(
        node_id: i32,
        children: Vec<DistributedNode>,
        payload: DistributedNodeKind,
    ) -> DistributedNode {
        node_in(0, node_id, children, payload)
    }

    fn values_node_in(
        fragment_id: u32,
        node_id: i32,
        columns: Vec<OutputColumn>,
    ) -> DistributedNode {
        node_in(
            fragment_id,
            node_id,
            Vec::new(),
            DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns,
            }),
        )
    }

    fn values_node(node_id: i32, columns: Vec<OutputColumn>) -> DistributedNode {
        values_node_in(0, node_id, columns)
    }

    /// An `AggregateCall` over `arg_types` whose output column id is `output_id`.
    fn agg_call(
        name: &str,
        distinct: bool,
        arg_types: Vec<DataType>,
        output_id: u32,
    ) -> AggregateCall {
        AggregateCall {
            name: name.to_string(),
            args: arg_types
                .into_iter()
                .map(|data_type| TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type,
                    nullable: true,
                })
                .collect(),
            distinct,
            result_type: DataType::Int64,
            order_by: Vec::new(),
            output_column_id: ColumnId::new_for_test(output_id),
        }
    }

    /// A `Single`-mode `HashAggregate` payload whose full group-key + aggregate
    /// layout is `group_key` + `aggregate` but whose visible `output_columns` (what
    /// the BE emits) is `visible`. One placeholder `count` call per aggregate column
    /// keeps the layout/aggregate arity consistent (now that the aggregate is a
    /// covered node whose finalization checks it).
    fn hash_aggregate_payload(
        group_key: Vec<OutputColumn>,
        aggregate: Vec<OutputColumn>,
        visible: Vec<OutputColumn>,
    ) -> DistributedNodeKind {
        let aggregates = aggregate
            .iter()
            .map(|column| agg_call("count", false, Vec::new(), column.column_id.0))
            .collect::<Vec<_>>();
        hash_aggregate_payload_full(AggMode::Single, group_key, aggregate, aggregates, visible)
    }

    /// A `HashAggregate` payload with explicit `mode` and `aggregates`, so tests
    /// can exercise partial-mode intermediate typing and arity/consistency failures.
    fn hash_aggregate_payload_full(
        mode: AggMode,
        group_key: Vec<OutputColumn>,
        aggregate: Vec<OutputColumn>,
        aggregates: Vec<AggregateCall>,
        visible: Vec<OutputColumn>,
    ) -> DistributedNodeKind {
        let is_merge = vec![false; aggregates.len()];
        DistributedNodeKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
            mode,
            group_by: Vec::new(),
            aggregates,
            is_merge,
            output_layout: AggregateOutputLayout::new(group_key, aggregate),
            output_columns: visible,
            topn_runtime_filter_builds: Vec::new(),
        }))
    }

    fn scan_payload(columns: Vec<OutputColumn>) -> DistributedNodeKind {
        scan_payload_with_required(columns, None)
    }

    fn scan_payload_with_required(
        columns: Vec<OutputColumn>,
        required_columns: Option<Vec<String>>,
    ) -> DistributedNodeKind {
        DistributedNodeKind::Scan(PlanScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "t".to_string(),
                columns: Vec::new(),
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::StarRocks {
                    db_id: 1,
                    table_id: 2,
                },
            },
            alias: None,
            columns,
            predicates: Vec::new(),
            required_columns,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        })
    }

    fn sort_payload(output_columns: Vec<OutputColumn>) -> DistributedNodeKind {
        DistributedNodeKind::Sort(PlanSortNode {
            items: Vec::new(),
            analytic_partition_by: Vec::new(),
            output_columns,
            offset: None,
            partition_limit: None,
            topn_type: None,
        })
    }

    fn hash_join_payload(output_columns: Vec<OutputColumn>) -> DistributedNodeKind {
        hash_join_payload_typed(JoinKind::Inner, output_columns)
    }

    fn hash_join_payload_typed(
        join_type: JoinKind,
        output_columns: Vec<OutputColumn>,
    ) -> DistributedNodeKind {
        DistributedNodeKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type,
            eq_conditions: Vec::new(),
            other_condition: None,
            distribution: JoinDistribution::Unknown,
            execution_mode: None,
            build_runtime_filters: Vec::new(),
            output_columns,
        }))
    }

    fn nest_loop_join_payload(output_columns: Vec<OutputColumn>) -> DistributedNodeKind {
        DistributedNodeKind::NestLoopJoin(PhysicalNestLoopJoinNode {
            join_type: JoinKind::Inner,
            condition: None,
            output_columns,
        })
    }

    fn set_op_payload(
        output_columns: Vec<OutputColumn>,
        child_output_columns: Vec<Vec<OutputColumn>>,
    ) -> DistributedNodeKind {
        DistributedNodeKind::SetOp(PhysicalSetOpNode {
            kind: PlanSetOpKind::UnionAll,
            output_columns,
            child_output_columns,
        })
    }

    /// Seal a single-fragment result plan whose root is `root` and whose
    /// fragment output columns are `output_columns`.
    fn seal_single_fragment(
        root: DistributedNode,
        output_columns: Vec<OutputColumn>,
    ) -> Result<DistributedPlan, String> {
        DistributedPlanDraftBuilder::new(
            vec![PlanFragment {
                fragment_id: 0,
                root,
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
        )
        .seal()
    }

    // ----- RED: the seal must reject stale/missing/inconsistent node outputs --

    #[test]
    fn seal_rejects_hash_join_node_with_no_execution_output() {
        // A join's execution output is reconciled from its children. When both
        // children produce nothing, the derived output is empty and the seal must
        // fail fast rather than emit an empty join schema.
        let root = node(
            1,
            vec![values_node(2, Vec::new()), values_node(3, Vec::new())],
            hash_join_payload(Vec::new()),
        );
        let error = seal_single_fragment(root, Vec::new())
            .expect_err("a join node without derivable execution output must not seal");
        assert!(error.contains("hash-join"), "{error}");
        assert!(error.contains("no execution output columns"), "{error}");
    }

    #[test]
    fn seal_rejects_nest_loop_join_node_with_no_execution_output() {
        let root = node(
            1,
            vec![values_node(2, Vec::new()), values_node(3, Vec::new())],
            nest_loop_join_payload(Vec::new()),
        );
        let error = seal_single_fragment(root, Vec::new())
            .expect_err("a nest-loop join without derivable execution output must not seal");
        assert!(error.contains("nest-loop-join"), "{error}");
        assert!(error.contains("no execution output columns"), "{error}");
    }

    #[test]
    fn seal_rejects_scan_node_with_no_execution_output() {
        let root = node(1, Vec::new(), scan_payload(Vec::new()));
        let error = seal_single_fragment(root, Vec::new())
            .expect_err("a scan without execution output must not seal");
        assert!(error.contains("scan"), "{error}");
        assert!(error.contains("no execution output columns"), "{error}");
    }

    #[test]
    fn seal_rejects_set_op_node_with_no_execution_output() {
        let root = node(
            1,
            vec![
                values_node(2, vec![output_col(1, "a")]),
                values_node(3, vec![output_col(1, "a")]),
            ],
            set_op_payload(
                Vec::new(),
                vec![vec![output_col(1, "a")], vec![output_col(1, "a")]],
            ),
        );
        let error = seal_single_fragment(root, Vec::new())
            .expect_err("a set-op without execution output must not seal");
        assert!(error.contains("set-op"), "{error}");
        assert!(error.contains("no execution output columns"), "{error}");
    }

    #[test]
    fn seal_rejects_sort_node_with_no_execution_output() {
        let root = node(
            1,
            vec![values_node(2, vec![output_col(1, "a")])],
            sort_payload(Vec::new()),
        );
        let error = seal_single_fragment(root, Vec::new())
            .expect_err("a sort without execution output must not seal");
        assert!(error.contains("sort"), "{error}");
        assert!(error.contains("no execution output columns"), "{error}");
    }

    #[test]
    fn seal_rejects_set_op_with_child_output_arity_mismatch() {
        let output_columns = vec![output_col(5, "a")];
        let root = node(
            1,
            vec![
                values_node(2, vec![output_col(1, "a")]),
                values_node(3, vec![output_col(2, "b")]),
            ],
            // Two children but only one declared child output schema.
            set_op_payload(output_columns.clone(), vec![vec![output_col(1, "a")]]),
        );
        let error = seal_single_fragment(root, output_columns).expect_err(
            "a set-op whose child schema arity disagrees with its children must not seal",
        );
        assert!(error.contains("set-op"), "{error}");
        assert!(
            error.contains("declares 1 child output schemas but has 2 children"),
            "{error}"
        );
    }

    // ----- GREEN: the seal finalizes covered node outputs --------------------

    /// A result-root Sort over a Scan child. Both covered nodes are cataloged;
    /// their kinds and logical columns follow the planner-computed payloads.
    fn sort_over_scan_plan() -> DistributedPlan {
        let columns = vec![output_col(1, "k"), output_col(2, "v")];
        let scan = node(2, Vec::new(), scan_payload(columns.clone()));
        let sort = node(1, vec![scan], sort_payload(columns.clone()));
        seal_single_fragment(sort, columns).expect("sort-over-scan plan seals")
    }

    #[test]
    fn sealed_plan_catalogs_every_covered_node_output() {
        let plan = sort_over_scan_plan();
        let catalog = plan.node_outputs();

        assert_eq!(catalog.outputs().len(), 2);

        let sort = catalog.output_for(0, 1).expect("sort node output");
        assert_eq!(sort.kind, NodeExecutionKind::Sort);
        assert_eq!(
            sort.columns
                .iter()
                .map(|column| (
                    column.column_id.0,
                    column.name.as_str(),
                    column.output_ordinal
                ))
                .collect::<Vec<_>>(),
            vec![(1, "k", 0), (2, "v", 1)]
        );

        let scan = catalog.output_for(0, 2).expect("scan node output");
        assert_eq!(scan.kind, NodeExecutionKind::Scan);
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.column_id.0)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn output_for_returns_none_for_uncovered_nodes() {
        let plan = sort_over_scan_plan();
        // node 3 does not exist; a covered lookup returns None rather than
        // guessing.
        assert!(plan.node_outputs().output_for(0, 3).is_none());
    }

    #[test]
    fn plan_without_covered_nodes_has_an_empty_catalog() {
        let columns = vec![output_col(1, "k")];
        let plan = seal_single_fragment(values_node(1, columns.clone()), columns)
            .expect("values-only plan seals");
        assert!(plan.node_outputs().outputs().is_empty());
        assert!(plan.node_outputs().output_for(0, 1).is_none());
    }

    #[test]
    fn result_root_covered_node_reuses_boundary_occurrence_ids() {
        let plan = sort_over_scan_plan();
        let result_boundary = plan
            .boundaries()
            .contracts()
            .iter()
            .find(|contract| contract.node_id.is_none())
            .expect("result boundary");
        let boundary_ids = result_boundary
            .columns
            .iter()
            .map(|column| column.execution_column_id)
            .collect::<Vec<_>>();

        // The Sort is the fragment root and its output matches the result
        // boundary, so it reuses the boundary occurrences verbatim.
        let sort = plan.node_outputs().output_for(0, 1).expect("sort output");
        let sort_ids = sort
            .columns
            .iter()
            .map(|column| column.execution_column_id)
            .collect::<Vec<_>>();
        assert_eq!(sort_ids, boundary_ids);
    }

    #[test]
    fn internal_covered_node_gets_fresh_occurrence_ids_continuing_after_boundaries() {
        let plan = sort_over_scan_plan();
        // Two boundary occurrences (the two result columns) are numbered 1 and 2;
        // the internal Scan's occurrences continue densely from 3.
        let scan = plan.node_outputs().output_for(0, 2).expect("scan output");
        let scan_ids = scan
            .columns
            .iter()
            .map(|column| column.execution_column_id.value())
            .collect::<Vec<_>>();
        assert_eq!(scan_ids, vec![3, 4]);
    }

    #[test]
    fn same_logical_column_gets_distinct_occurrences_at_root_and_internal_node() {
        let plan = sort_over_scan_plan();
        let sort = plan.node_outputs().output_for(0, 1).expect("sort output");
        let scan = plan.node_outputs().output_for(0, 2).expect("scan output");

        // Column id 1 flows through both nodes ...
        assert_eq!(sort.columns[0].column_id, scan.columns[0].column_id);
        // ... but each occurrence has its own execution column id.
        assert_ne!(
            sort.columns[0].execution_column_id,
            scan.columns[0].execution_column_id
        );
    }

    #[test]
    fn producer_fragment_covered_root_gets_fresh_occurrence_ids() {
        // A producer fragment (Noop sink feeding an Exchange) has no
        // fragment-level sink boundary, so its covered root cannot reuse a
        // boundary occurrence: the Exchange send/receive boundaries are distinct
        // occurrences of the same logical columns. The root is numbered fresh,
        // continuing after every boundary occurrence.
        let columns = vec![output_col(1, "k"), output_col(2, "v")];
        let exchange_node_id = 20;
        let producer = PlanFragment {
            fragment_id: 1,
            root: node_in(
                1,
                10,
                vec![
                    values_node_in(1, 11, vec![output_col(1, "k")]),
                    values_node_in(1, 12, vec![output_col(2, "v")]),
                ],
                hash_join_payload(columns.clone()),
            ),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let consumer = PlanFragment {
            fragment_id: 0,
            root: node(
                exchange_node_id,
                Vec::new(),
                DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: 1,
                    output_columns: columns.clone(),
                    output_qualifier: None,
                    flavor: ExchangeFlavor::Distribution,
                }),
            ),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let plan = DistributedPlanDraftBuilder::new(
            vec![producer, consumer],
            Some(0),
            vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![1, 2],
            }],
            Default::default(),
        )
        .seal()
        .expect("producer/consumer stream plan seals");

        // Boundaries number six occurrences (result 1-2, send 3-4, receive 5-6);
        // the producer-root join is internal to no boundary and continues from 7.
        let join = plan.node_outputs().output_for(1, 10).expect("join output");
        let boundary_ids = plan
            .boundaries()
            .contracts()
            .iter()
            .flat_map(|contract| {
                contract
                    .columns
                    .iter()
                    .map(|column| column.execution_column_id.value())
            })
            .collect::<Vec<_>>();
        let join_ids = join
            .columns
            .iter()
            .map(|column| column.execution_column_id.value())
            .collect::<Vec<_>>();
        assert!(
            join_ids.iter().all(|id| !boundary_ids.contains(id)),
            "producer root occurrences must be distinct from every boundary occurrence: join={join_ids:?} boundaries={boundary_ids:?}"
        );
        let max_boundary_id = boundary_ids.iter().copied().max().unwrap_or(0);
        assert_eq!(join_ids, vec![max_boundary_id + 1, max_boundary_id + 2]);
    }

    #[test]
    fn set_op_output_is_cataloged_from_its_own_output_columns() {
        let output_columns = vec![output_col(5, "a"), output_col(6, "b")];
        let root = node(
            1,
            vec![
                values_node(2, vec![output_col(1, "a"), output_col(2, "b")]),
                values_node(3, vec![output_col(3, "a"), output_col(4, "b")]),
            ],
            set_op_payload(
                output_columns.clone(),
                vec![
                    vec![output_col(1, "a"), output_col(2, "b")],
                    vec![output_col(3, "a"), output_col(4, "b")],
                ],
            ),
        );
        let plan = seal_single_fragment(root, output_columns).expect("set-op plan seals");
        let set_op = plan.node_outputs().output_for(0, 1).expect("set-op output");
        assert_eq!(set_op.kind, NodeExecutionKind::SetOp);
        assert_eq!(
            set_op
                .columns
                .iter()
                .map(|column| column.column_id.0)
                .collect::<Vec<_>>(),
            vec![5, 6]
        );
    }

    #[test]
    fn seal_deduplicates_repeated_column_ids_in_covered_output() {
        // The BE rejects a node schema with duplicate `OutputColumn.column_id`s,
        // so a covered node output that repeats a logical column id is
        // deduplicated (first occurrence kept) at seal time. Re-materializing a
        // column at several positions (`SELECT c1, c1`) is a boundary/projection
        // concern, not a covered node concern.
        let scan_columns = vec![
            output_col(1, "c1"),
            output_col(2, "c2"),
            output_col(1, "c1"),
        ];
        let root = node(1, Vec::new(), scan_payload(scan_columns));
        let plan = seal_single_fragment(root, vec![output_col(1, "c1"), output_col(2, "c2")])
            .expect("duplicate-column scan plan seals");
        let scan = plan.node_outputs().output_for(0, 1).expect("scan output");
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.column_id.0)
                .collect::<Vec<_>>(),
            vec![1, 2],
            "the repeated logical column id is dropped, keeping the first occurrence"
        );
    }

    #[test]
    fn seal_prunes_scan_covered_output_to_required_columns() {
        // `PruneScanColumns` writes `required_columns` without shrinking the
        // projected `columns`, and the BE materializes only the required set. The
        // catalog is the authoritative execution-output record, so a scan's
        // covered output must be the required-columns-pruned set, not the full
        // projection.
        let scan = scan_payload_with_required(
            vec![
                output_col(1, "k"),
                output_col(2, "v"),
                output_col(3, "_file"),
                output_col(4, "_pos"),
            ],
            Some(vec!["k".to_string(), "v".to_string()]),
        );
        let root = node(1, Vec::new(), scan);
        let plan = seal_single_fragment(root, vec![output_col(1, "k"), output_col(2, "v")])
            .expect("required-columns scan plan seals");
        let scan = plan.node_outputs().output_for(0, 1).expect("scan output");
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| column.column_id.0)
                .collect::<Vec<_>>(),
            vec![1, 2],
            "the scan covered output is pruned to required_columns [k, v]; _file/_pos are dropped"
        );
    }

    #[test]
    fn seal_reconciles_stale_join_output_from_children() {
        // The join payload lists a column (99) no child produces. The seal must
        // ignore the stale payload and take the children-derived output instead,
        // so the sealed join output references only columns in the execution chunk.
        let root = node(
            1,
            vec![
                values_node(2, vec![output_col(1, "a")]),
                values_node(3, vec![output_col(2, "b")]),
            ],
            hash_join_payload(vec![
                output_col(1, "a"),
                output_col(2, "b"),
                output_col(99, "stale"),
            ]),
        );
        let plan = seal_single_fragment(root, vec![output_col(1, "a"), output_col(2, "b")])
            .expect("stale-join plan seals by reconciling against children");
        let join = plan.node_outputs().output_for(0, 1).expect("join output");
        assert_eq!(
            join.columns
                .iter()
                .map(|column| (column.column_id.0, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "a"), (2, "b")],
            "the stale id 99 is dropped; the sealed output is the children-derived schema"
        );
    }

    #[test]
    fn seal_reconciles_marker_join_output_to_pruned_probe_and_nullable_build() {
        // Models a NOT-IN/marker `LEFT OUTER` join: the probe scan projects
        // [k, v, _file, _pos] but only materializes [k, v] (required_columns), and
        // the build side carries a nullable internal marker column. The join
        // payload still lists the pruned probe metadata columns (3, 4). The seal
        // must derive the join output from the children: probe [k, v] plus the
        // nullable build [v, __match_0], dropping the pruned 3/4 and keeping the
        // internal marker made nullable by the outer join.
        let probe = node(
            2,
            Vec::new(),
            scan_payload_with_required(
                vec![
                    output_col(1, "k"),
                    output_col(2, "v"),
                    output_col(3, "_file"),
                    output_col(4, "_pos"),
                ],
                Some(vec!["k".to_string(), "v".to_string()]),
            ),
        );
        let build = values_node(3, vec![output_col(10, "v"), internal_col(13, "__match_0")]);
        let root = node(
            1,
            vec![probe, build],
            hash_join_payload_typed(
                JoinKind::LeftOuter,
                vec![
                    output_col(1, "k"),
                    output_col(2, "v"),
                    output_col(3, "_file"),
                    output_col(4, "_pos"),
                    output_col(10, "v"),
                    internal_col(13, "__match_0"),
                ],
            ),
        );
        let plan = seal_single_fragment(
            root,
            vec![
                output_col(1, "k"),
                output_col(2, "v"),
                output_col(10, "v"),
                internal_col(13, "__match_0"),
            ],
        )
        .expect("marker-join plan seals by reconciling against children");
        let join = plan.node_outputs().output_for(0, 1).expect("join output");
        assert_eq!(
            join.columns
                .iter()
                .map(|column| column.column_id.0)
                .collect::<Vec<_>>(),
            vec![1, 2, 10, 13],
            "pruned probe metadata columns 3/4 are dropped; probe [k, v] and build [v, marker] survive"
        );
        let marker = join
            .columns
            .iter()
            .find(|column| column.column_id.0 == 13)
            .expect("marker column is retained");
        assert!(marker.is_internal, "the marker column stays internal");
        assert!(
            marker.nullable,
            "the outer-join build side (with the marker) is made nullable"
        );
    }

    #[test]
    fn seal_derives_join_output_from_aggregate_child_visible_columns_not_full_layout() {
        // A join whose direct child is a HashAggregate must derive from the
        // aggregate's *visible* `output_columns` (the projected subset the BE
        // actually emits), not from its full group-key + aggregate layout. Here
        // the full layout is [1:g, 2:c, 3:s] but only [1:g, 3:s] are visible, so
        // the sealed Inner-join output must be [1, 3, 5] (visible [1, 3] plus the
        // build's [5]) -- deriving from the full layout would wrongly declare the
        // hidden aggregate column 2 as [1, 2, 3, 5].
        let aggregate = node(
            2,
            vec![values_node(4, vec![output_col(1, "g")])],
            hash_aggregate_payload(
                vec![output_col(1, "g")],
                vec![output_col(2, "c"), output_col(3, "s")],
                vec![output_col(1, "g"), output_col(3, "s")],
            ),
        );
        let build = values_node(3, vec![output_col(5, "x")]);
        let root = node(
            1,
            vec![aggregate, build],
            hash_join_payload(vec![
                output_col(1, "g"),
                output_col(3, "s"),
                output_col(5, "x"),
            ]),
        );
        let plan = seal_single_fragment(
            root,
            vec![output_col(1, "g"), output_col(3, "s"), output_col(5, "x")],
        )
        .expect("join-over-aggregate plan seals from the aggregate's visible output");
        let join = plan.node_outputs().output_for(0, 1).expect("join output");
        assert_eq!(
            join.columns
                .iter()
                .map(|column| column.column_id.0)
                .collect::<Vec<_>>(),
            vec![1, 3, 5],
            "the hidden aggregate column 2 must not appear; only visible [1, 3] plus build [5]"
        );
    }

    // ----- RED: HashAggregate is a covered node with finalized intermediate types

    #[test]
    fn seal_covers_hash_aggregate_with_intermediate_state_types() {
        // A partial (`Local`) aggregate emits its aggregate column as its
        // intermediate aggregate-state type: avg's intermediate is Utf8 even though
        // its final output is Float64. The covered execution output and the stored
        // wire layout must both carry the intermediate type, and the covered output
        // must be numbered with query-scoped occurrence ids.
        let avg = agg_call("avg", false, vec![DataType::Int64], 2);
        let visible = vec![output_col(2, "avg_v")];
        let aggregate = node(
            1,
            vec![values_node(2, vec![output_col(1, "v")])],
            hash_aggregate_payload_full(
                AggMode::Local,
                Vec::new(),
                vec![output_col(2, "avg_v")],
                vec![avg],
                visible.clone(),
            ),
        );
        let plan = seal_single_fragment(aggregate, visible).expect("partial aggregate plan seals");
        let output = plan
            .node_outputs()
            .output_for(0, 1)
            .expect("aggregate is a covered node");
        assert_eq!(output.kind, NodeExecutionKind::HashAggregate);
        assert_eq!(
            output
                .columns
                .iter()
                .map(|column| (column.column_id.0, column.data_type.clone()))
                .collect::<Vec<_>>(),
            vec![(2, DataType::Utf8)],
            "the visible avg output carries the Utf8 intermediate state type, not Float64"
        );
        // Every covered output column is numbered with an occurrence id.
        assert_eq!(output.columns.len(), 1);
        let _occurrence = output.columns[0].execution_column_id;
        // The stored wire layout carries the same intermediate type for the encoder.
        let layout = plan
            .node_outputs()
            .aggregate_layout(0, 1)
            .expect("covered aggregate stores its wire layout");
        assert!(layout.group_key_columns.is_empty());
        assert_eq!(
            layout
                .aggregate_columns
                .iter()
                .map(|column| (column.column_id.0, column.data_type.clone()))
                .collect::<Vec<_>>(),
            vec![(2, DataType::Utf8)]
        );
    }

    #[test]
    fn seal_keeps_final_type_for_terminal_mode_aggregate() {
        // A terminal (`Global`) aggregate emits final result types, so no
        // intermediate repair is applied: avg stays Float64.
        let avg = agg_call("avg", false, vec![DataType::Int64], 2);
        let visible = vec![OutputColumn {
            column_id: ColumnId::new_for_test(2),
            name: "avg_v".to_string(),
            data_type: DataType::Float64,
            nullable: true,
            is_internal: false,
        }];
        let aggregate = node(
            1,
            vec![values_node(2, vec![output_col(1, "v")])],
            hash_aggregate_payload_full(
                AggMode::Global,
                Vec::new(),
                visible.clone(),
                vec![avg],
                visible.clone(),
            ),
        );
        let plan = seal_single_fragment(aggregate, visible).expect("terminal aggregate seals");
        let output = plan
            .node_outputs()
            .output_for(0, 1)
            .expect("aggregate covered");
        assert_eq!(
            output.columns[0].data_type,
            DataType::Float64,
            "a Global aggregate keeps the final result type"
        );
    }

    #[test]
    fn seal_rejects_hash_aggregate_with_aggregate_column_arity_mismatch() {
        // The output_layout declares two aggregate columns but the node has only one
        // aggregate call. The seal must fail fast rather than emit a wire whose
        // aggregate-state columns do not line up with the calls.
        let one_call = agg_call("count", false, Vec::new(), 2);
        let aggregate = node(
            1,
            vec![values_node(2, vec![output_col(1, "v")])],
            hash_aggregate_payload_full(
                AggMode::Local,
                Vec::new(),
                vec![output_col(2, "c0"), output_col(3, "c1")],
                vec![one_call],
                vec![output_col(2, "c0"), output_col(3, "c1")],
            ),
        );
        let error = seal_single_fragment(aggregate, vec![output_col(2, "c0"), output_col(3, "c1")])
            .expect_err("an aggregate whose layout/call arity disagree must not seal");
        assert!(error.contains("hash-aggregate"), "{error}");
        assert!(error.contains("does not match aggregate count"), "{error}");
    }

    #[test]
    fn seal_rejects_hash_aggregate_visible_output_absent_from_layout() {
        // The visible output_columns reference id 99, which is neither a group key
        // nor an aggregate column. The seal cannot type it from the layout and must
        // fail fast.
        let count = agg_call("count", false, Vec::new(), 2);
        let aggregate = node(
            1,
            vec![values_node(2, vec![output_col(1, "v")])],
            hash_aggregate_payload_full(
                AggMode::Local,
                Vec::new(),
                vec![output_col(2, "c0")],
                vec![count],
                vec![output_col(99, "stale")],
            ),
        );
        let error = seal_single_fragment(aggregate, vec![output_col(99, "stale")])
            .expect_err("a visible output column absent from the layout must not seal");
        assert!(error.contains("hash-aggregate"), "{error}");
        assert!(error.contains("is missing from output_layout"), "{error}");
    }

    #[test]
    fn node_output_catalog_derivation_is_deterministic() {
        // `NodeOutputCatalog` holds finalized `OutputColumn`s (arrow `DataType`,
        // no `Eq`), so compare structurally via its `Debug` form.
        assert_eq!(
            format!("{:?}", sort_over_scan_plan().node_outputs()),
            format!("{:?}", sort_over_scan_plan().node_outputs())
        );
    }

    // ---- CGO-9C Task 2: fragment output + stream edge projection finalization ----

    /// A `Project` payload with `items[i]` re-materializing `output_column_id[i]`
    /// from a same-id `ColumnRef` (so `SELECT c1, c1` shares one logical id).
    fn project_payload(output_column_ids: &[u32], names: &[&str]) -> DistributedNodeKind {
        DistributedNodeKind::Project(PlanProjectNode {
            items: output_column_ids
                .iter()
                .zip(names.iter())
                .map(|(id, name)| ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId::new_for_test(*id),
                            qualifier: None,
                            column: (*name).to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: (*name).to_string(),
                    output_column_id: ColumnId::new_for_test(*id),
                })
                .collect(),
            output_qualifier: None,
        })
    }

    /// A two-fragment stream plan: a Noop producer feeding an Exchange receiver in
    /// the Result consumer. `edge_output_slot_ids` selects the edge projection.
    fn stream_plan(
        source_columns: Vec<OutputColumn>,
        receiver_columns: Vec<OutputColumn>,
        edge_output_slot_ids: Vec<i32>,
    ) -> Result<DistributedPlan, String> {
        let exchange_node_id = 20;
        let producer = PlanFragment {
            fragment_id: 1,
            root: values_node_in(1, 10, source_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: source_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let consumer = PlanFragment {
            fragment_id: 0,
            root: node_in(
                0,
                exchange_node_id,
                Vec::new(),
                DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: 1,
                    output_columns: receiver_columns,
                    output_qualifier: None,
                    flavor: ExchangeFlavor::Distribution,
                }),
            ),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        DistributedPlanDraftBuilder::new(
            vec![producer, consumer],
            Some(0),
            vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: edge_output_slot_ids,
            }],
            Default::default(),
        )
        .seal()
    }

    fn column_ids(columns: &[OutputColumn]) -> Vec<u32> {
        columns.iter().map(|column| column.column_id.0).collect()
    }

    fn column_names(columns: &[OutputColumn]) -> Vec<&str> {
        columns.iter().map(|column| column.name.as_str()).collect()
    }

    // ----- RED: the seal must reject incomplete / unresolvable finalizations ----

    #[test]
    fn fragment_output_keeps_declared_columns_when_root_wire_output_diverges() {
        // The declared output is authoritative when the root's seal-time wire
        // output does not correspond to it (a count mismatch here). This mirrors
        // the encoder, which resolved such shapes against the codegen binding or
        // never encoded them; finalization keeps the planner's declared columns
        // rather than failing (a `Repeat` grouping-set producer, a `TableFunction`
        // passthrough root, and binding-driven scans all rely on this).
        let root = values_node(1, vec![output_col(1, "a"), output_col(2, "b")]);
        let plan = seal_single_fragment(
            root,
            vec![output_col(1, "a"), output_col(2, "b"), output_col(3, "c")],
        )
        .expect("a count-mismatched root keeps the declared fragment output");
        let output = plan
            .fragment_edge_outputs()
            .fragment_output_columns(0)
            .expect("fragment 0 output");
        assert_eq!(column_ids(output), vec![1, 2, 3]);
    }

    #[test]
    fn seal_rejects_stream_edge_with_missing_projection() {
        // The edge asks to send source slot 99, which the producer root does not
        // produce, and the receiver declares no columns to reconcile against.
        let error = stream_plan(
            vec![output_col(1, "a"), output_col(2, "b")],
            Vec::new(),
            vec![99],
        )
        .expect_err("a stream edge whose slot id is absent from the source root must not seal");
        assert!(error.contains("missing projection"), "{error}");
        assert!(error.contains("source slot id 99"), "{error}");
    }

    #[test]
    fn seal_rejects_stream_edge_sender_receiver_mismatch() {
        // The receiver declares column id 3, but the producer root only produces
        // [1, 2], and the edge slot ids cannot reconcile it either -- a genuine
        // sender/receiver schema mismatch the seal rejects.
        let error = stream_plan(
            vec![output_col(1, "a"), output_col(2, "b")],
            vec![output_col(3, "c")],
            vec![3],
        )
        .expect_err("a receiver expecting a column the sender cannot produce must not seal");
        assert!(error.contains("missing projection"), "{error}");
        assert!(error.contains("source slot id 3"), "{error}");
    }

    // ----- GREEN: the seal finalizes fragment output + stream edge projections --

    #[test]
    fn finalizes_fragment_output_with_unique_project_ids() {
        // `SELECT c1, c1`: the fragment declares two columns that share logical id
        // 1, and the finalized fragment output re-materializes the repeat as a
        // distinct wire id (1, then a synthetic 3), matching the encoder.
        let child = values_node(2, vec![output_col(1, "c1"), output_col(2, "c2")]);
        let root = node(1, vec![child], project_payload(&[1, 1], &["c1", "c1"]));
        let plan = seal_single_fragment(root, vec![output_col(1, "c1"), output_col(1, "c1")])
            .expect("duplicate-projection fragment seals");
        let output = plan
            .fragment_edge_outputs()
            .fragment_output_columns(0)
            .expect("fragment 0 output");
        assert_eq!(column_ids(output), vec![1, 3]);
        assert_eq!(column_names(output), vec!["c1", "c1"]);
    }

    #[test]
    fn finalizes_stream_edge_projection_in_receiver_order_with_source_metadata() {
        // Mirrors the encoder's stream fixture: the source root produces
        // [1:old, 2:delta]; the receiver requests them reversed as [2, 1]; the
        // finalized projection is [2:delta, 1:old] with the source's metadata.
        let plan = stream_plan(
            vec![output_col(1, "old"), output_col(2, "delta")],
            vec![output_col(2, "delta"), output_col(1, "old")],
            vec![2, 1],
        )
        .expect("stream plan seals");
        let projection = plan
            .fragment_edge_outputs()
            .stream_edge_projection(0, 20)
            .expect("stream edge projection");
        assert_eq!(column_ids(projection), vec![2, 1]);
        assert_eq!(column_names(projection), vec!["delta", "old"]);
    }

    #[test]
    fn finalizes_stream_edge_projection_prefers_receiver_over_stale_slots() {
        // The receiver declares [10, 20] (a prefix of the source) while the edge
        // carries lowered synthetic slot ids [43, 44]; the finalized projection
        // follows the receiver's declared columns with the source's metadata.
        let plan = stream_plan(
            vec![
                output_col(10, "employee_id"),
                output_col(20, "name"),
                output_col(30, "title"),
            ],
            vec![output_col(10, "employee_id"), output_col(20, "name")],
            vec![43, 44],
        )
        .expect("stream plan seals");
        let projection = plan
            .fragment_edge_outputs()
            .stream_edge_projection(0, 20)
            .expect("stream edge projection");
        assert_eq!(column_ids(projection), vec![10, 20]);
        assert_eq!(column_names(projection), vec!["employee_id", "name"]);
    }

    #[test]
    fn fragment_edge_output_finalization_is_deterministic() {
        let first = stream_plan(
            vec![output_col(1, "old"), output_col(2, "delta")],
            vec![output_col(2, "delta"), output_col(1, "old")],
            vec![2, 1],
        )
        .expect("stream plan seals");
        let second = stream_plan(
            vec![output_col(1, "old"), output_col(2, "delta")],
            vec![output_col(2, "delta"), output_col(1, "old")],
            vec![2, 1],
        )
        .expect("stream plan seals");
        assert_eq!(
            format!("{:?}", first.fragment_edge_outputs()),
            format!("{:?}", second.fragment_edge_outputs())
        );
    }

    #[test]
    fn producer_fragment_output_forwards_root_wire_output() {
        // A Noop producer fragment forwards its execution root's wire output.
        let plan = stream_plan(
            vec![output_col(1, "old"), output_col(2, "delta")],
            vec![output_col(1, "old"), output_col(2, "delta")],
            vec![1, 2],
        )
        .expect("stream plan seals");
        let producer_output = plan
            .fragment_edge_outputs()
            .fragment_output_columns(1)
            .expect("producer fragment output");
        assert_eq!(column_ids(producer_output), vec![1, 2]);
    }

    // ----- ported `project_edge_output_columns` coverage (formerly encoder unit
    // tests of `project_output_columns_for_edge`) -------------------------------

    fn stream_edge_for_test() -> FragmentEdge {
        FragmentEdge {
            source_fragment_id: 1,
            target_fragment_id: 0,
            target_exchange_node_id: 20,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }
    }

    #[test]
    fn project_edge_output_columns_uses_root_slots_when_fragment_outputs_are_retagged() {
        // The fragment outputs [5, 6] are retagged from the root's [3, 4]; slot
        // ids [5, 6] resolve through the fragment output ordinals back to the root
        // schema.
        let root_columns = vec![output_col(3, "a"), output_col(4, "b")];
        let source_outputs = vec![output_col(5, "a"), output_col(6, "b")];
        let resolved = super::project_edge_output_columns(
            root_columns,
            &source_outputs,
            &[5, 6],
            &stream_edge_for_test(),
        )
        .expect("resolve stream edge output columns");
        assert_eq!(column_ids(&resolved), vec![3, 4]);
    }

    #[test]
    fn project_edge_output_columns_falls_back_to_root_on_stale_superset_slots() {
        // The requested slots are a stale superset of the root output; the
        // projection falls back to the whole root output rather than failing.
        let root_columns = vec![
            output_col(2, "l_partkey"),
            output_col(4, "l_shipdate"),
            output_col(1, "l_orderkey"),
            output_col(3, "l_suppkey"),
        ];
        let source_outputs = vec![
            output_col(1, "l_orderkey"),
            output_col(2, "l_partkey"),
            output_col(3, "l_suppkey"),
            output_col(4, "l_shipdate"),
            output_col(5, "col1"),
        ];
        let resolved = super::project_edge_output_columns(
            root_columns,
            &source_outputs,
            &[1, 2, 3, 4, 5],
            &stream_edge_for_test(),
        )
        .expect("resolve stream edge output columns");
        assert_eq!(column_ids(&resolved), vec![2, 4, 1, 3]);
    }

    // ---- CGO-9C Task 3: Iceberg write output/target-schema + router partition ----

    fn target_column(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    /// Builds the provider planning input and freezes it into the generic
    /// connector-write contract used by a sealed fragment.
    fn try_write_fragment(
        output_columns: Vec<OutputColumn>,
        output_exprs: Option<Vec<TypedExpr>>,
        input: ConnectorWriteInputBinding,
        target_columns: Vec<ColumnDef>,
        mode: IcebergWriteSinkMode,
    ) -> Result<PlanFragment, super::WriteContractError> {
        let mut spec = simple_sink_spec();
        spec.target_columns = target_columns;
        spec.mode = mode;
        let planning_input = IcebergWritePlanInput {
            descriptor_database: "test_db".to_string(),
            spec,
            input: input.clone(),
        };
        let mut fragment = PlanFragment {
            fragment_id: 0,
            root: values_node(10, output_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::ConnectorWrite(
                crate::sql::planner::distributed::write::sink::ConnectorWriteFragmentSink {
                    handle: None,
                    input: input.clone(),
                    output_contract: None,
                },
            ),
            output_exprs,
            output_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let contract = super::finalize_iceberg_write_output(&fragment, &planning_input)?;
        fragment.sink = DataSink::ConnectorWrite(
            crate::sql::planner::distributed::write::sink::ConnectorWriteFragmentSink {
                handle: None,
                input,
                output_contract: Some(contract),
            },
        );
        Ok(fragment)
    }

    fn write_fragment(
        output_columns: Vec<OutputColumn>,
        output_exprs: Option<Vec<TypedExpr>>,
        input: ConnectorWriteInputBinding,
        target_columns: Vec<ColumnDef>,
    ) -> PlanFragment {
        try_write_fragment(
            output_columns,
            output_exprs,
            input,
            target_columns,
            IcebergWriteSinkMode::Data,
        )
        .expect("write fixture must freeze a valid connector contract")
    }

    /// A single router fragment (root sink `ChangeStreamRouter`) with the
    /// given output columns and branch routes, and no writer edges (mirrors the
    /// encoder's single-fragment router fixture, which seals with vacuous edges).
    fn router_fragment(
        output_columns: Vec<OutputColumn>,
        branches: Vec<ChangeStreamBranchRoute>,
    ) -> PlanFragment {
        PlanFragment {
            fragment_id: 0,
            root: values_node(10, output_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::ChangeStreamRouter(ChangeStreamRouterSink {
                group_id: 0,
                change_op_output_ordinal: 0,
                data_route_output_ordinal: Some(1),
                branches,
            }),
            output_exprs: None,
            output_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }
    }

    fn branch_route(
        branch_id: i32,
        output_ordinals: Vec<usize>,
        output_partition_ordinals: Vec<usize>,
    ) -> ChangeStreamBranchRoute {
        ChangeStreamBranchRoute {
            branch_id,
            branch_kind: ChangeStreamBranchKind::DeleteDv,
            target_fragment_id: 1,
            target_exchange_node_id: 20,
            output_ordinals,
            output_partition_ordinals,
        }
    }

    fn column_ref_id(expr: &TypedExpr) -> u32 {
        match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } => column_id.0,
            other => panic!("expected column ref expr, got {other:?}"),
        }
    }

    #[test]
    fn write_contract_rejects_output_expr_arity_mismatch() {
        // Declared `output_exprs` whose count differs from the target columns must
        // fail fast, reproducing the encoder's arity check.
        let error = try_write_fragment(
            vec![output_col(1, "a"), output_col(2, "b")],
            Some(vec![super::column_ref_expr(&output_col(1, "a"))]),
            ConnectorWriteInputBinding::RootOutputByOrdinal,
            vec![
                target_column("a", DataType::Int64, false),
                target_column("b", DataType::Int64, false),
            ],
            IcebergWriteSinkMode::Data,
        )
        .expect_err("output_exprs arity mismatch must not finalize");
        assert_eq!(
            error,
            super::WriteContractError::WriteOutputExprArity {
                fragment_id: 0,
                output_exprs: 1,
                target_columns: 2,
            }
        );
    }

    #[test]
    fn write_contract_rejects_input_arity_mismatch() {
        // With no declared `output_exprs`, a bound input column count that differs
        // from the target columns must fail fast (encoder's input-binding check).
        let error = try_write_fragment(
            vec![output_col(1, "a"), output_col(2, "b")],
            None,
            ConnectorWriteInputBinding::RootOutputByOrdinal,
            vec![target_column("a", DataType::Int64, false)],
            IcebergWriteSinkMode::Data,
        )
        .expect_err("input arity mismatch must not finalize");
        assert_eq!(
            error,
            super::WriteContractError::WriteInputArity {
                fragment_id: 0,
                input_columns: 2,
                target_columns: 1,
            }
        );
    }

    #[test]
    fn write_contract_preserves_narrowed_dv_input_schema() {
        let fragment = try_write_fragment(
            vec![output_col(10, "file_path"), output_col(11, "row_position")],
            None,
            ConnectorWriteInputBinding::OutputOrdinals(vec![0, 1]),
            vec![
                target_column("id", DataType::Int64, false),
                target_column("g", DataType::Int64, false),
                target_column("v", DataType::Int64, false),
            ],
            IcebergWriteSinkMode::DeletionVectors,
        )
        .expect("DV position input must freeze a connector contract");

        let catalog = super::build_write_contract_catalog(std::slice::from_ref(&fragment))
            .expect("DV position input must seal without projecting user-table columns");
        let contract = catalog
            .connector_write_output(0)
            .expect("DV fragment must have a write contract");
        assert_eq!(contract.output_exprs.len(), 2);
        assert_eq!(contract.target_schema.len(), 2);
        assert_eq!(contract.target_schema[0].name, "file_path");
        assert_eq!(contract.target_schema[1].name, "row_position");
    }

    #[test]
    fn write_contract_rejects_out_of_range_input_ordinal() {
        // An `OutputOrdinals` binding referencing a nonexistent fragment output
        // column must fail fast (the defensive twin of the encoder's check; the
        // boundary catalog also rejects this earlier through a full seal).
        let error = try_write_fragment(
            vec![output_col(1, "a")],
            None,
            ConnectorWriteInputBinding::OutputOrdinals(vec![5]),
            vec![target_column("a", DataType::Int64, false)],
            IcebergWriteSinkMode::Data,
        )
        .expect_err("out-of-range input ordinal must not finalize");
        assert_eq!(
            error,
            super::WriteContractError::WriteInputOrdinalOutOfRange {
                fragment_id: 0,
                ordinal: 5,
                available: 1,
            }
        );
    }

    #[test]
    fn write_contract_rejects_out_of_range_router_partition_ordinal() {
        // A router branch partition ordinal outside the router fragment output
        // must fail fast, reproducing the encoder's reconstruction check.
        let fragment = router_fragment(
            vec![
                output_col(1, "op"),
                output_col(2, "route"),
                output_col(3, "bucket"),
            ],
            vec![branch_route(0, vec![2], vec![5])],
        );
        let error = super::build_write_contract_catalog(std::slice::from_ref(&fragment))
            .expect_err("out-of-range router partition ordinal must not finalize");
        assert_eq!(
            error,
            super::WriteContractError::RouterPartitionOrdinalOutOfRange {
                fragment_id: 0,
                branch_id: 0,
                ordinal: 5,
                available: 3,
            }
        );
    }

    #[test]
    fn seal_finalizes_write_output_exprs_and_positional_target_schema() {
        // RootOutputByOrdinal: the write output expressions are column refs into
        // the fragment output columns (real logical ids), and the target schema
        // carries positional 1-based ids with the target column metadata.
        let plan = DistributedPlanDraftBuilder::new(
            vec![write_fragment(
                vec![output_col(7, "a"), output_col(9, "b")],
                None,
                ConnectorWriteInputBinding::RootOutputByOrdinal,
                vec![
                    target_column("ta", DataType::Int32, false),
                    target_column("tb", DataType::Int64, true),
                ],
            )],
            Some(0),
            Vec::new(),
            Default::default(),
        )
        .seal()
        .expect("write plan seals");

        let contract = plan
            .write_contracts()
            .connector_write_output(0)
            .expect("write fragment has a finalized contract");
        assert_eq!(
            contract
                .output_exprs
                .iter()
                .map(column_ref_id)
                .collect::<Vec<_>>(),
            vec![7, 9]
        );
        assert_eq!(
            contract.target_schema,
            vec![
                super::FinalizedWriteTargetColumn {
                    column_id: 1,
                    name: "ta".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                },
                super::FinalizedWriteTargetColumn {
                    column_id: 2,
                    name: "tb".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ]
        );
    }

    #[test]
    fn seal_finalizes_write_output_exprs_by_output_ordinals() {
        // OutputOrdinals reorders the fragment output columns feeding the sink;
        // the finalized output expressions follow that order.
        let plan = DistributedPlanDraftBuilder::new(
            vec![write_fragment(
                vec![output_col(7, "a"), output_col(9, "b")],
                None,
                ConnectorWriteInputBinding::OutputOrdinals(vec![1, 0]),
                vec![
                    target_column("ta", DataType::Int64, false),
                    target_column("tb", DataType::Int64, false),
                ],
            )],
            Some(0),
            Vec::new(),
            Default::default(),
        )
        .seal()
        .expect("write plan seals");

        let contract = plan
            .write_contracts()
            .connector_write_output(0)
            .expect("write fragment has a finalized contract");
        assert_eq!(
            contract
                .output_exprs
                .iter()
                .map(column_ref_id)
                .collect::<Vec<_>>(),
            vec![9, 7]
        );
    }

    #[test]
    fn seal_finalizes_router_branch_partition_from_fragment_output_columns() {
        // A non-empty partition ordinal list becomes a hash partition over column
        // refs into the router fragment output columns; an empty list is
        // unpartitioned.
        let plan = DistributedPlanDraftBuilder::new(
            vec![router_fragment(
                vec![
                    output_col(1, "op"),
                    output_col(2, "route"),
                    output_col(3, "bucket"),
                ],
                vec![
                    branch_route(0, vec![2], vec![2]),
                    branch_route(1, vec![2], Vec::new()),
                ],
            )],
            Some(0),
            Vec::new(),
            Default::default(),
        )
        .seal()
        .expect("router plan seals");

        let hashed = plan
            .write_contracts()
            .router_branch_partition(0, 0)
            .expect("branch 0 has a finalized partition");
        assert!(matches!(hashed.kind, PartitionKind::Hash));
        assert_eq!(
            hashed.exprs.iter().map(column_ref_id).collect::<Vec<_>>(),
            vec![3]
        );

        let unpartitioned = plan
            .write_contracts()
            .router_branch_partition(0, 1)
            .expect("branch 1 has a finalized partition");
        assert!(matches!(unpartitioned.kind, PartitionKind::Unpartitioned));
        assert!(unpartitioned.exprs.is_empty());
    }

    #[test]
    fn write_contract_catalog_is_empty_for_a_result_plan() {
        let plan = seal_single_fragment(
            values_node(10, vec![output_col(1, "a")]),
            vec![output_col(1, "a")],
        )
        .expect("result plan seals");
        assert!(plan.write_contracts().connector_write_output(0).is_none());
        assert!(
            plan.write_contracts()
                .router_branch_partition(0, 0)
                .is_none()
        );
    }
}
