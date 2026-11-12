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

//! Exhaustive node dispatch through the original prepared component authors.
//!
//! This module creates no scope, resource model, type grammar or capability
//! certificate. The containing Package author owns the cumulative admission,
//! source union, node collection allocations and final original publication.

use crate::{
    physical_aggregate_binding_v2::MaterializedAggregateBindings,
    physical_aggregate_node_v2::AggregateNodeProjectionLimits,
    physical_aggregate_node_v2::{
        PreparedAggregateNodeDecode, PreparedAggregateNodeEncode, prepare_aggregate_node_decode_in,
        prepare_aggregate_node_encode_in,
    },
    physical_assert_rows_v2::{
        PreparedAssertRowsNodeDecode, PreparedAssertRowsNodeEncode,
        prepare_assert_rows_node_decode_in, prepare_assert_rows_node_encode_in,
    },
    physical_binding_v2::{BindingProjectionLimits, MaterializedFunctionBindings},
    physical_change_event_v2::{
        PreparedChangeEventNodeDecode, PreparedChangeEventNodeEncode,
        prepare_change_event_node_decode_in, prepare_change_event_node_encode_in,
    },
    physical_expression_v2::{DecodedExpressions, EncodedExpressions},
    physical_node_v2::{NodeAdmit, NodeCodecError, NodeProjectionFacts, NodeProjectionLimits},
    physical_relation_v2::{DecodedRelations, EncodedRelations, RelationProjectionLimits},
    physical_relational_nodes_v2::{
        PreparedRelationalNodeDecode, PreparedRelationalNodeEncode,
        prepare_relational_node_decode_in, prepare_relational_node_encode_in,
    },
    physical_repeat_v2::{
        PreparedRepeatNodeDecode, PreparedRepeatNodeEncode, prepare_repeat_node_decode_in,
        prepare_repeat_node_encode_in,
    },
    physical_scan_v2::ScanProjectionLimits,
    physical_scan_v2::{
        PreparedScanNodeDecode, PreparedScanNodeEncode, prepare_scan_node_decode_in,
        prepare_scan_node_encode_in,
    },
    physical_simple_nodes_v2::{
        PreparedSimpleNodeDecode, PreparedSimpleNodeEncode, prepare_simple_node_decode_in,
        prepare_simple_node_encode_in,
    },
    physical_table_function_node_v2::TableFunctionNodeProjectionLimits,
    physical_table_function_node_v2::{
        PreparedTableFunctionNodeDecode, PreparedTableFunctionNodeEncode,
        prepare_table_function_node_decode_in, prepare_table_function_node_encode_in,
    },
    physical_table_write_nodes_v2::{
        PreparedTableWriteNodeDecode, PreparedTableWriteNodeEncode,
        prepare_table_write_node_decode_in, prepare_table_write_node_encode_in,
    },
    physical_table_write_nodes_v2::{TableWriteNodeProjectionLimits, TableWriteTypeIds},
    physical_topn_node_v2::TopNNodeProjectionLimits,
    physical_topn_node_v2::{
        PreparedTopNNodeDecode, PreparedTopNNodeEncode, prepare_topn_node_decode_in,
        prepare_topn_node_encode_in,
    },
    physical_unpivot_v2::{
        PreparedUnpivotNodeDecode, PreparedUnpivotNodeEncode, prepare_unpivot_node_decode_in,
        prepare_unpivot_node_encode_in,
    },
    physical_value_v2::EncodedValues,
    physical_writer_schema_v2::WriterSchemaProjectionLimits,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::CompileCheckpoints;

/// Original component limits; no default or independent resource ledger.
#[derive(Clone, Copy, Debug)]
pub(super) struct NodeDispatchLimits {
    pub node: NodeProjectionLimits,
    pub binding: BindingProjectionLimits,
    pub relation: RelationProjectionLimits,
    pub writer_schema: WriterSchemaProjectionLimits,
}

/// Exact sender namespace loans prepared by the containing Package author.
pub(super) struct NodeEncodeContext<'namespace, 'loan, 'source, 'control> {
    pub values: &'namespace EncodedValues<'loan, 'source, 'control>,
    pub expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    pub relations: &'namespace EncodedRelations<'loan, 'source, 'control>,
}

/// Owned binding namespaces retain their original decoded header loans.
pub(super) struct NodeDecodeContext<'namespace, 'loan, 'wire, 'control> {
    pub expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    pub relations: &'namespace DecodedRelations<'loan, 'wire, 'control>,
    pub functions: &'namespace MaterializedFunctionBindings<'loan, 'wire>,
    pub aggregates: &'namespace MaterializedAggregateBindings<'namespace, 'loan, 'wire>,
}

/// Inline original tokens. Any containing token Vec is the parent's request.
pub(super) enum PreparedNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    Simple(PreparedSimpleNodeEncode<'node, 'namespace, 'loan, 'source, 'control>),
    Relational(PreparedRelationalNodeEncode<'node, 'namespace, 'loan, 'source, 'control>),
    Repeat(PreparedRepeatNodeEncode<'node, 'namespace, 'loan, 'source, 'control>),
    AssertRows(PreparedAssertRowsNodeEncode<'node, 'namespace, 'loan, 'source, 'control>),
    ChangeEvent(PreparedChangeEventNodeEncode<'node, 'namespace, 'loan, 'source, 'control>),
    Unpivot(PreparedUnpivotNodeEncode<'node, 'namespace, 'loan, 'source, 'control>),
    Aggregate(PreparedAggregateNodeEncode<'node, 'namespace, 'loan, 'source, 'control>),
    TopN(PreparedTopNNodeEncode<'node, 'namespace, 'loan, 'source, 'control>),
    TableFunction(PreparedTableFunctionNodeEncode<'node, 'namespace, 'loan, 'source, 'control>),
    Scan(PreparedScanNodeEncode<'node, 'namespace, 'loan, 'source, 'control>),
    TableWrite(PreparedTableWriteNodeEncode<'node, 'namespace, 'loan, 'source, 'control>),
}
impl PreparedNodeEncode<'_, '_, '_, '_, '_> {
    #[cfg(test)]
    pub(super) fn facts(&self) -> &NodeProjectionFacts {
        match self {
            Self::Simple(token) => token.facts(),
            Self::Relational(token) => token.facts(),
            Self::Repeat(token) => token.facts(),
            Self::AssertRows(token) => token.facts(),
            Self::ChangeEvent(token) => token.facts(),
            Self::Unpivot(token) => token.facts(),
            Self::Aggregate(token) => token.facts(),
            Self::TopN(token) => token.facts(),
            Self::TableFunction(token) => token.facts(),
            Self::Scan(token) => token.facts(),
            Self::TableWrite(token) => token.facts(),
        }
    }
    /// Delegate on the same original controller after its complete admission.
    pub(super) fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(wire::PhysicalNode, NodeProjectionFacts), NodeCodecError> {
        match self {
            Self::Simple(token) => token.emit_in(admit, work),
            Self::Relational(token) => token.emit_in(admit, work),
            Self::Repeat(token) => token.emit_in(admit, work),
            Self::AssertRows(token) => token.emit_in(admit, work),
            Self::ChangeEvent(token) => token.emit_in(admit, work),
            Self::Unpivot(token) => token.emit_in(admit, work),
            Self::Aggregate(token) => token.emit_in(admit, work),
            Self::TopN(token) => token.emit_in(admit, work),
            Self::TableFunction(token) => token.emit_in(admit, work),
            Self::Scan(token) => token.emit_in(admit, work),
            Self::TableWrite(token) => token.emit_in(admit, work),
        }
    }
}

pub(super) enum PreparedNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    Simple(PreparedSimpleNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>),
    Relational(PreparedRelationalNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>),
    Repeat(PreparedRepeatNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>),
    AssertRows(PreparedAssertRowsNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>),
    ChangeEvent(PreparedChangeEventNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>),
    Unpivot(PreparedUnpivotNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>),
    Aggregate(PreparedAggregateNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>),
    TopN(PreparedTopNNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>),
    TableFunction(PreparedTableFunctionNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>),
    Scan(Box<PreparedScanNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>>),
    TableWrite(PreparedTableWriteNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>),
}
impl PreparedNodeDecode<'_, '_, '_, '_, '_> {
    #[cfg(test)]
    pub(super) fn facts(&self) -> &NodeProjectionFacts {
        match self {
            Self::Simple(token) => token.facts(),
            Self::Relational(token) => token.facts(),
            Self::Repeat(token) => token.facts(),
            Self::AssertRows(token) => token.facts(),
            Self::ChangeEvent(token) => token.facts(),
            Self::Unpivot(token) => token.facts(),
            Self::Aggregate(token) => token.facts(),
            Self::TopN(token) => token.facts(),
            Self::TableFunction(token) => token.facts(),
            Self::Scan(token) => token.facts(),
            Self::TableWrite(token) => token.facts(),
        }
    }
    pub(super) fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(p::PhysicalNode, NodeProjectionFacts), NodeCodecError> {
        match self {
            Self::Simple(token) => token.emit_in(admit, work),
            Self::Relational(token) => token.emit_in(admit, work),
            Self::Repeat(token) => token.emit_in(admit, work),
            Self::AssertRows(token) => token.emit_in(admit, work),
            Self::ChangeEvent(token) => token.emit_in(admit, work),
            Self::Unpivot(token) => token.emit_in(admit, work),
            Self::Aggregate(token) => token.emit_in(admit, work),
            Self::TopN(token) => token.emit_in(admit, work),
            Self::TableFunction(token) => token.emit_in(admit, work),
            Self::Scan(token) => (*token).emit_in(admit, work),
            Self::TableWrite(token) => token.emit_in(admit, work),
        }
    }
}

/// Select only the existing kind author. Writer schema IDs are original
/// occurrence loans supplied by the caller, never inferred from final types.
pub(super) fn prepare_node_encode_in<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    context: &NodeEncodeContext<'namespace, 'loan, 'source, 'control>,
    writer_type_ids: Option<TableWriteTypeIds<'node>>,
    source_retained_bytes: usize,
    limits: NodeDispatchLimits,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, NodeCodecError> {
    if !std::ptr::addr_eq(work.control(), context.values.original_control()) {
        return Err(NodeCodecError::InvalidShape(
            "node dispatcher uses a different original control",
        ));
    }
    let token = match &input.kind {
        p::NodeKind::Filter { .. }
        | p::NodeKind::Project { .. }
        | p::NodeKind::Values { .. }
        | p::NodeKind::SystemRelation { .. }
        | p::NodeKind::Limit { .. }
        | p::NodeKind::GenerateSeries { .. }
        | p::NodeKind::PositionShard(_) => {
            PreparedNodeEncode::Simple(prepare_simple_node_encode_in(
                input,
                context.values,
                context.expressions,
                source_retained_bytes,
                limits.node,
                admit,
                work,
            )?)
        }
        p::NodeKind::HashJoin { .. }
        | p::NodeKind::NestLoopJoin { .. }
        | p::NodeKind::Sort { .. }
        | p::NodeKind::Window(_)
        | p::NodeKind::SetOp { .. }
        | p::NodeKind::ExchangeSource { .. } => {
            PreparedNodeEncode::Relational(prepare_relational_node_encode_in(
                input,
                context.values,
                context.expressions,
                source_retained_bytes,
                limits.node,
                admit,
                work,
            )?)
        }
        p::NodeKind::Repeat { .. } => PreparedNodeEncode::Repeat(prepare_repeat_node_encode_in(
            input,
            context.values,
            source_retained_bytes,
            limits.node,
            admit,
            work,
        )?),
        p::NodeKind::AssertOneRow(_) => {
            PreparedNodeEncode::AssertRows(prepare_assert_rows_node_encode_in(
                input,
                context.values,
                source_retained_bytes,
                limits.node,
                admit,
                work,
            )?)
        }
        p::NodeKind::ChangeEventExpand { .. } => {
            PreparedNodeEncode::ChangeEvent(prepare_change_event_node_encode_in(
                input,
                context.values,
                context.expressions,
                source_retained_bytes,
                limits.node,
                admit,
                work,
            )?)
        }
        p::NodeKind::Unpivot { .. } => PreparedNodeEncode::Unpivot(prepare_unpivot_node_encode_in(
            input,
            context.values,
            context.expressions,
            source_retained_bytes,
            limits.node,
            admit,
            work,
        )?),
        p::NodeKind::Aggregate { .. } => {
            PreparedNodeEncode::Aggregate(prepare_aggregate_node_encode_in(
                input,
                context.values,
                context.expressions,
                source_retained_bytes,
                AggregateNodeProjectionLimits {
                    node: limits.node,
                    binding: limits.binding,
                },
                admit,
                work,
            )?)
        }
        p::NodeKind::TopN { .. } => PreparedNodeEncode::TopN(prepare_topn_node_encode_in(
            input,
            context.values,
            context.expressions,
            source_retained_bytes,
            TopNNodeProjectionLimits {
                node: limits.node,
                binding: limits.binding,
            },
            admit,
            work,
        )?),
        p::NodeKind::TableFunction { .. } => {
            PreparedNodeEncode::TableFunction(prepare_table_function_node_encode_in(
                input,
                context.values,
                context.expressions,
                source_retained_bytes,
                TableFunctionNodeProjectionLimits {
                    node: limits.node,
                    binding: limits.binding,
                },
                admit,
                work,
            )?)
        }
        p::NodeKind::Scan { .. } => PreparedNodeEncode::Scan(prepare_scan_node_encode_in(
            input,
            context.relations,
            context.values,
            context.expressions,
            source_retained_bytes,
            ScanProjectionLimits {
                node: limits.node,
                relation: limits.relation,
            },
            admit,
            work,
        )?),
        p::NodeKind::TableWriter { .. } | p::NodeKind::TableFinish(_) => {
            let type_ids = writer_type_ids.ok_or(NodeCodecError::InvalidShape(
                "writer node dispatcher has no original schema type IDs",
            ))?;
            PreparedNodeEncode::TableWrite(prepare_table_write_node_encode_in(
                input,
                context.values,
                context.expressions,
                type_ids,
                source_retained_bytes,
                TableWriteNodeProjectionLimits {
                    node: limits.node,
                    binding: limits.binding,
                    schema: limits.writer_schema,
                },
                admit,
                work,
            )?)
        }
    };
    Ok(token)
}

/// Wire kind absence remains ordinary malformed input. The match is exhaustive;
/// a new wire vocabulary requires updating this dispatcher at compilation.
pub(super) fn prepare_node_decode_in<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    context: &NodeDecodeContext<'namespace, 'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: NodeDispatchLimits,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, NodeCodecError> {
    if !std::ptr::addr_eq(work.control(), context.expressions.original_control()) {
        return Err(NodeCodecError::InvalidShape(
            "node dispatcher uses a different original control",
        ));
    }
    let token = match input.kind.as_ref() {
        Some(wire::physical_node::Kind::Filter(_))
        | Some(wire::physical_node::Kind::Project(_))
        | Some(wire::physical_node::Kind::Values(_))
        | Some(wire::physical_node::Kind::SystemRelation(_))
        | Some(wire::physical_node::Kind::Limit(_))
        | Some(wire::physical_node::Kind::GenerateSeries(_))
        | Some(wire::physical_node::Kind::PositionShard(_)) => {
            PreparedNodeDecode::Simple(prepare_simple_node_decode_in(
                input,
                context.expressions,
                source_retained_bytes,
                limits.node,
                admit,
                work,
            )?)
        }
        Some(wire::physical_node::Kind::HashJoin(_))
        | Some(wire::physical_node::Kind::NestLoopJoin(_))
        | Some(wire::physical_node::Kind::Sort(_))
        | Some(wire::physical_node::Kind::Window(_))
        | Some(wire::physical_node::Kind::SetOperation(_))
        | Some(wire::physical_node::Kind::ExchangeSource(_)) => {
            PreparedNodeDecode::Relational(prepare_relational_node_decode_in(
                input,
                context.expressions,
                source_retained_bytes,
                limits.node,
                admit,
                work,
            )?)
        }
        Some(wire::physical_node::Kind::Repeat(_)) => {
            PreparedNodeDecode::Repeat(prepare_repeat_node_decode_in(
                input,
                context.expressions.values(),
                source_retained_bytes,
                limits.node,
                admit,
                work,
            )?)
        }
        Some(wire::physical_node::Kind::AssertOneRow(_)) => {
            PreparedNodeDecode::AssertRows(prepare_assert_rows_node_decode_in(
                input,
                context.expressions.values(),
                source_retained_bytes,
                limits.node,
                admit,
                work,
            )?)
        }
        Some(wire::physical_node::Kind::ChangeEventExpand(_)) => {
            PreparedNodeDecode::ChangeEvent(prepare_change_event_node_decode_in(
                input,
                context.expressions,
                source_retained_bytes,
                limits.node,
                admit,
                work,
            )?)
        }
        Some(wire::physical_node::Kind::Unpivot(_)) => {
            PreparedNodeDecode::Unpivot(prepare_unpivot_node_decode_in(
                input,
                context.expressions,
                source_retained_bytes,
                limits.node,
                admit,
                work,
            )?)
        }
        Some(wire::physical_node::Kind::Aggregate(_)) => {
            PreparedNodeDecode::Aggregate(prepare_aggregate_node_decode_in(
                input,
                context.expressions,
                context.aggregates,
                source_retained_bytes,
                AggregateNodeProjectionLimits {
                    node: limits.node,
                    binding: limits.binding,
                },
                admit,
                work,
            )?)
        }
        Some(wire::physical_node::Kind::TopN(_)) => {
            PreparedNodeDecode::TopN(prepare_topn_node_decode_in(
                input,
                context.expressions,
                context.aggregates,
                source_retained_bytes,
                TopNNodeProjectionLimits {
                    node: limits.node,
                    binding: limits.binding,
                },
                admit,
                work,
            )?)
        }
        Some(wire::physical_node::Kind::TableFunction(_)) => {
            PreparedNodeDecode::TableFunction(prepare_table_function_node_decode_in(
                input,
                context.expressions,
                context.functions,
                source_retained_bytes,
                TableFunctionNodeProjectionLimits {
                    node: limits.node,
                    binding: limits.binding,
                },
                admit,
                work,
            )?)
        }
        Some(wire::physical_node::Kind::Scan(_)) => {
            PreparedNodeDecode::Scan(Box::new(prepare_scan_node_decode_in(
                input,
                context.relations,
                context.expressions,
                source_retained_bytes,
                ScanProjectionLimits {
                    node: limits.node,
                    relation: limits.relation,
                },
                admit,
                work,
            )?))
        }
        Some(wire::physical_node::Kind::TableWriter(_))
        | Some(wire::physical_node::Kind::TableFinish(_)) => {
            PreparedNodeDecode::TableWrite(prepare_table_write_node_decode_in(
                input,
                context.expressions,
                context.aggregates,
                source_retained_bytes,
                TableWriteNodeProjectionLimits {
                    node: limits.node,
                    binding: limits.binding,
                    schema: limits.writer_schema,
                },
                admit,
                work,
            )?)
        }
        None => {
            return Err(NodeCodecError::InvalidShape(
                "node dispatcher has no node kind",
            ));
        }
    };
    Ok(token)
}

#[cfg(test)]
mod tests;
