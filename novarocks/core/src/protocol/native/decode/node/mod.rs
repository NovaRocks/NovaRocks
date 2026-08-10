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

//! Native protocol plan-node decoding.

mod aggregate;
mod assert;
mod change_event_expand;
mod common;
mod exchange;
mod filter;
mod generate_series;
mod hash_join;
mod limit;
mod nestloop_join;
mod project;
mod redistribute;
mod repeat;
mod set_op;
mod sort;
mod table_function;
mod topn;
mod values;
mod window;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use self::common::*;

use super::layout::Layout;
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::ExprArena;
use crate::exec::fragment::program::{FragmentNodeId, ScanAssignmentKind};
use crate::exec::node::limit::LimitNode;
use crate::exec::node::scan::BoundScanRanges;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::protocol::common::error::FieldPath;
use crate::protocol::native::test_assembly::{
    NativeExpressionDecoder, NativeExpressionInputLayout, NativeOutputLayout,
    NativeOutputLayoutDecoder,
};
use crate::runtime::exchange::ExchangeKey;
use crate::runtime::fragment::instance::{
    ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
};
use crate::runtime::query_context::QueryId;
use crate::runtime::scan_range::ScanRangeParams;
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_protocol::{novarocks, plan};

#[derive(Clone, Debug)]
pub(crate) struct DecodedNode {
    pub node: ExecNode,
    pub layout: Layout,
    pub output_schema: ChunkSchemaRef,
}

#[derive(Clone)]
pub(crate) struct NativePlanDecodeContext {
    exchange_inputs: ExchangeInputAssignments,
    /// Transient enrichment INPUT: FE-decoded `ScanRangeParams` per scan node.
    /// The scan decoders read these to build connector ranges; the enriched
    /// `BoundScanRanges` output is captured separately below and routed into
    /// the instance's scan assignments (it no longer binds an op at decode).
    raw_scan_ranges: BTreeMap<FragmentNodeId, Vec<ScanRangeParams>>,
    /// Capture slot for the per-node enriched `BoundScanRanges` produced during
    /// scan-node decode. Interior mutability lets `&self` decoders record their
    /// output; the submission builder drains it into the instance assignments.
    captured_scan_ranges: RefCell<BTreeMap<FragmentNodeId, BoundScanRanges>>,
    query_options: Option<QueryOptions>,
    connectors: Option<Arc<crate::connector::ConnectorRegistry>>,
    execution_resolver: Option<Arc<dyn novarocks_spi::connector::ConnectorExecutionResolver>>,
    expression_decoder: Option<Arc<dyn NativeExpressionDecoder>>,
    output_layout_decoder: Option<Arc<dyn NativeOutputLayoutDecoder>>,
    query_id: Option<QueryId>,
    fragment_instance_id: FragmentInstanceId,
}

impl Default for NativePlanDecodeContext {
    fn default() -> Self {
        Self {
            exchange_inputs: ExchangeInputAssignments::default(),
            raw_scan_ranges: BTreeMap::new(),
            captured_scan_ranges: RefCell::new(BTreeMap::new()),
            query_options: None,
            connectors: None,
            execution_resolver: None,
            expression_decoder: None,
            output_layout_decoder: None,
            query_id: None,
            fragment_instance_id: FragmentInstanceId::new(novarocks_types::UniqueId::new(0, 0)),
        }
    }
}

impl NativePlanDecodeContext {
    pub(crate) fn from_parts(
        exchange_inputs: ExchangeInputAssignments,
        raw_scan_ranges: BTreeMap<FragmentNodeId, Vec<ScanRangeParams>>,
        query_options: QueryOptions,
        connectors: Arc<crate::connector::ConnectorRegistry>,
        query_id: QueryId,
        fragment_instance_id: FragmentInstanceId,
    ) -> Self {
        Self {
            exchange_inputs,
            raw_scan_ranges,
            captured_scan_ranges: RefCell::new(BTreeMap::new()),
            query_options: Some(query_options),
            connectors: Some(connectors),
            execution_resolver: None,
            expression_decoder: None,
            output_layout_decoder: None,
            query_id: Some(query_id),
            fragment_instance_id,
        }
    }

    pub(crate) fn with_execution_resolver(
        mut self,
        resolver: Arc<dyn novarocks_spi::connector::ConnectorExecutionResolver>,
    ) -> Self {
        self.execution_resolver = Some(resolver);
        self
    }

    pub(crate) fn with_expression_decoder(
        mut self,
        decoder: Arc<dyn NativeExpressionDecoder>,
    ) -> Self {
        self.expression_decoder = Some(decoder);
        self
    }

    pub(crate) fn with_output_layout_decoder(
        mut self,
        decoder: Arc<dyn NativeOutputLayoutDecoder>,
    ) -> Self {
        self.output_layout_decoder = Some(decoder);
        self
    }

    pub(crate) fn decode_output_layout(
        &self,
        columns: &[novarocks_protocol::common::OutputColumn],
        path: FieldPath,
    ) -> Result<NativeOutputLayout, super::NativeFragmentDecodeError> {
        let Some(decoder) = self.output_layout_decoder.as_ref() else {
            #[cfg(any(test, feature = "query-execution-contract-test-support"))]
            {
                let layout = super::NativeFragmentDecodeError::map_invalid(
                    path.clone(),
                    super::layout::layout_from_output_columns(columns),
                )?;
                let chunk_schema = super::NativeFragmentDecodeError::map_invalid(
                    path.clone(),
                    super::layout::chunk_schema_from_output_columns(columns),
                )?;
                let slot_schemas = super::NativeFragmentDecodeError::map_invalid(
                    path,
                    super::layout::slot_schemas_from_output_columns(columns),
                )?;
                return Ok(NativeOutputLayout::new(
                    layout.order().to_vec(),
                    chunk_schema,
                    slot_schemas,
                ));
            }
            #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
            {
                return Err(super::NativeFragmentDecodeError::unsupported(
                    path,
                    "native output layout decoder must be supplied by the backend runtime",
                ));
            }
        };
        decoder
            .decode_output_layout(columns, path)
            .map_err(super::NativeFragmentDecodeError::from)
    }

    pub(crate) fn decode_expression(
        &self,
        expression: &novarocks_protocol::expr::Expr,
        path: FieldPath,
        arena: &mut ExprArena,
        layout: &Layout,
    ) -> Result<crate::exec::expr::ExprId, super::NativeFragmentDecodeError> {
        let Some(decoder) = self.expression_decoder.as_ref() else {
            #[cfg(any(test, feature = "query-execution-contract-test-support"))]
            {
                return super::expr::decode_expr_at(expression, path, arena, layout);
            }
            #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
            {
                return Err(super::NativeFragmentDecodeError::unsupported(
                    path,
                    "native expression decoder must be supplied by the backend runtime",
                ));
            }
        };
        let input = NativeExpressionInputLayout::from_slot_ids(layout.order().iter().copied());
        decoder
            .decode_expression(expression, path, arena, &input)
            .map_err(super::NativeFragmentDecodeError::from)
    }

    /// Record a scan node's enriched connector ranges (produced during node
    /// decode). Drained later by `take_captured_scan_ranges` into the instance.
    pub(crate) fn capture_scan_ranges(&self, node_id: i32, ranges: BoundScanRanges) {
        self.captured_scan_ranges
            .borrow_mut()
            .insert(FragmentNodeId::new(node_id), ranges);
    }

    /// Drain the captured per-node `BoundScanRanges` for instance assembly.
    pub(crate) fn take_captured_scan_ranges(&self) -> BTreeMap<FragmentNodeId, BoundScanRanges> {
        std::mem::take(&mut self.captured_scan_ranges.borrow_mut())
    }

    /// Test-only: read (clone) the enriched ranges a scan decoder captured for
    /// `node_id`, so tests can materialize the op via `scan.source().bind(..)`.
    #[cfg(test)]
    pub(crate) fn captured_ranges_for_test(&self, node_id: i32) -> BoundScanRanges {
        self.captured_scan_ranges
            .borrow()
            .get(&FragmentNodeId::new(node_id))
            .cloned()
            .expect("captured scan ranges for node")
    }

    #[cfg(test)]
    pub(crate) fn with_exchange_sender_count(mut self, key: ExchangeKey, count: usize) -> Self {
        let count = NonZeroUsize::new(count).expect("test sender count must be positive");
        self.fragment_instance_id = FragmentInstanceId::new(novarocks_types::UniqueId::new(
            key.finst_id_hi,
            key.finst_id_lo,
        ));
        self.exchange_inputs = ExchangeInputAssignments::new(BTreeMap::from([(
            FragmentNodeId::new(key.node_id),
            ExchangeInputAssignment::new(count),
        )]));
        self
    }

    #[cfg(test)]
    pub(crate) fn with_scan_ranges(
        mut self,
        node_id: i32,
        ranges: Vec<novarocks::ScanRangeParams>,
    ) -> Self {
        // Populates the transient enrichment INPUT (raw `ScanRangeParams`); the
        // decoders enrich these and capture the `BoundScanRanges` output.
        let ranges = ranges
            .iter()
            .map(super::decode_scan_range_params)
            .collect::<Result<Vec<_>, _>>()
            .expect("decode test scan ranges");
        self.raw_scan_ranges
            .insert(FragmentNodeId::new(node_id), ranges);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_query_options(mut self, query_options: Option<QueryOptions>) -> Self {
        self.query_options = query_options;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_connector_registry(
        mut self,
        connectors: Arc<crate::connector::ConnectorRegistry>,
    ) -> Self {
        self.connectors = Some(connectors);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_query_id(mut self, query_id: QueryId) -> Self {
        self.query_id = Some(query_id);
        self
    }

    pub(crate) fn scan_ranges(
        &self,
        node_id: i32,
    ) -> Result<&[ScanRangeParams], super::error::NativeFragmentLeafDecodeError> {
        self.raw_scan_ranges
            .get(&FragmentNodeId::new(node_id))
            .map(Vec::as_slice)
            .ok_or_else(|| {
                super::error::NativeFragmentLeafDecodeError::at_field(
                    crate::protocol::common::error::ProtocolErrorKind::MissingField,
                    "scan_ranges",
                    format!("native ScanNode node_id={node_id} missing scan ranges"),
                )
            })
    }

    pub(crate) fn query_options(&self) -> Option<&QueryOptions> {
        self.query_options.as_ref()
    }

    pub(crate) fn query_id(&self) -> Option<QueryId> {
        self.query_id
    }

    pub(crate) fn fragment_instance_id(&self) -> FragmentInstanceId {
        self.fragment_instance_id
    }

    pub(crate) fn connectors(
        &self,
    ) -> Result<&crate::connector::ConnectorRegistry, super::error::NativeFragmentLeafDecodeError>
    {
        self.connectors.as_deref().ok_or_else(|| {
            super::error::NativeFragmentLeafDecodeError::at_field(
                crate::protocol::common::error::ProtocolErrorKind::MissingField,
                "connector_registry",
                "native ScanNode requires ConnectorRegistry in NativePlanDecodeContext",
            )
        })
    }

    pub(crate) fn execution_resolver(
        &self,
    ) -> Result<
        &dyn novarocks_spi::connector::ConnectorExecutionResolver,
        super::error::NativeFragmentLeafDecodeError,
    > {
        self.execution_resolver.as_deref().ok_or_else(|| {
            super::error::NativeFragmentLeafDecodeError::at_field(
                crate::protocol::common::error::ProtocolErrorKind::MissingField,
                "connector_execution_resolver",
                "native ConnectorReadSource requires a query-scoped execution resolver",
            )
        })
    }

    fn exchange_input(
        &self,
        node_id: i32,
    ) -> Result<(ExchangeKey, usize), super::error::NativeFragmentLeafDecodeError> {
        let assignment = self
            .exchange_inputs
            .get(&FragmentNodeId::new(node_id))
            .ok_or_else(|| {
                super::error::NativeFragmentLeafDecodeError::at_field(
                    crate::protocol::common::error::ProtocolErrorKind::MissingField,
                    "exchange_inputs",
                    format!("ExchangeReceiver missing sender count for node_id {node_id}"),
                )
            })?;
        let fragment_instance_id = self.fragment_instance_id.get();
        Ok((
            ExchangeKey {
                finst_id_hi: fragment_instance_id.high(),
                finst_id_lo: fragment_instance_id.low(),
                node_id,
            },
            assignment.sender_count().get(),
        ))
    }
}

pub(super) fn collect_scan_assignment_kinds(
    root: &plan::DistributedNode,
    root_path: FieldPath,
) -> Result<BTreeMap<FragmentNodeId, ScanAssignmentKind>, super::NativeFragmentDecodeError> {
    fn visit(
        node: &plan::DistributedNode,
        path: FieldPath,
        assignments: &mut BTreeMap<FragmentNodeId, ScanAssignmentKind>,
    ) -> Result<(), super::NativeFragmentDecodeError> {
        if let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_ref()
            && let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref()
        {
            let scan_path = path
                .clone()
                .field("payload")
                .field("physical")
                .field("scan");
            let table = scan.table.as_ref().ok_or_else(|| {
                super::NativeFragmentDecodeError::missing(
                    scan_path.clone().field("table"),
                    format!("native ScanNode node_id={} requires table", node.node_id),
                )
            })?;
            let source = table.source.as_ref().ok_or_else(|| {
                super::NativeFragmentDecodeError::missing(
                    scan_path.clone().field("table").field("source"),
                    format!("native ScanNode node_id={} requires source", node.node_id),
                )
            })?;
            let source = source.kind.as_ref().ok_or_else(|| {
                super::NativeFragmentDecodeError::missing(
                    scan_path
                        .clone()
                        .field("table")
                        .field("source")
                        .field("kind"),
                    format!(
                        "native ScanNode node_id={} requires source kind",
                        node.node_id
                    ),
                )
            })?;
            let _ = source;
            let kind = ScanAssignmentKind::File;
            if assignments
                .insert(FragmentNodeId::new(node.node_id), kind)
                .is_some()
            {
                return Err(super::NativeFragmentDecodeError::inconsistent(
                    path.clone().field("node_id"),
                    format!("native plan has duplicate scan node_id={}", node.node_id),
                ));
            }
        }
        for (index, child) in node.children.iter().enumerate() {
            visit(
                child,
                path.clone().field("children").index(index),
                assignments,
            )?;
        }
        Ok(())
    }

    let mut assignments = BTreeMap::new();
    visit(root, root_path, &mut assignments)?;
    Ok(assignments)
}

#[allow(dead_code)]
pub(crate) fn decode_node(
    node: &plan::DistributedNode,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, super::NativeFragmentDecodeError> {
    decode_node_inner(
        node,
        FieldPath::root("plan_fragment").field("root"),
        arena,
        ctx,
    )
}

fn decode_node_inner(
    node: &plan::DistributedNode,
    path: FieldPath,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, super::NativeFragmentDecodeError> {
    validate_distributed_node_children(node, path.clone())?;
    if !node.runtime_filter_binding_ids.is_empty() {
        return Err(super::NativeFragmentDecodeError::unsupported(
            path.clone().field("runtime_filter_binding_ids"),
            "core native test decoder does not decode runtime-filter bindings; use the backend decoder",
        ));
    }
    let mut children = Vec::with_capacity(node.children.len());
    for (index, child) in node.children.iter().enumerate() {
        children.push(decode_node_inner(
            child,
            path.clone().field("children").index(index),
            arena,
            ctx,
        )?);
    }

    let payload = node.payload.as_ref().ok_or_else(|| {
        super::NativeFragmentDecodeError::missing(
            path.clone().field("payload"),
            format!("DistributedNode node_id={} requires payload", node.node_id),
        )
    })?;
    let mut lowered = match payload {
        plan::distributed_node::Payload::Physical(physical) => lower_physical_node(
            node,
            physical,
            path.clone().field("payload").field("physical"),
            path.clone(),
            children,
            arena,
            ctx,
        ),
        plan::distributed_node::Payload::Exchange(exchange) => exchange::lower_exchange_receiver(
            node,
            exchange,
            path.clone().field("payload").field("exchange"),
            children,
            arena,
            ctx,
        ),
    }?;
    apply_distributed_limit_if_needed(node, lowered, path)
}

fn validate_distributed_node_children(
    node: &plan::DistributedNode,
    node_path: FieldPath,
) -> Result<(), super::NativeFragmentDecodeError> {
    let actual = node.children.len();
    let Some(payload) = node.payload.as_ref() else {
        return Ok(());
    };
    match payload {
        plan::distributed_node::Payload::Exchange(_) => {
            require_exact_children(node_path, "ExchangeReceiver", 0, actual)
        }
        plan::distributed_node::Payload::Physical(physical) => {
            let Some(kind) = physical.kind.as_ref() else {
                return Ok(());
            };
            match kind {
                plan::plan_node::Kind::Values(_) => {
                    require_exact_children(node_path, "ValuesNode", 0, actual)
                }
                plan::plan_node::Kind::Project(_) => {
                    require_exact_children(node_path, "ProjectNode", 1, actual)
                }
                plan::plan_node::Kind::Filter(_) => {
                    require_exact_children(node_path, "FilterNode", 1, actual)
                }
                plan::plan_node::Kind::Limit(_) => {
                    require_exact_children(node_path, "LimitNode", 1, actual)
                }
                plan::plan_node::Kind::Sort(_) => {
                    require_exact_children(node_path, "SortNode", 1, actual)
                }
                plan::plan_node::Kind::Topn(_) => {
                    require_exact_children(node_path, "TopNNode", 1, actual)
                }
                plan::plan_node::Kind::SetOp(_) => {
                    require_min_children(node_path, "SetOpNode", 2, actual)
                }
                plan::plan_node::Kind::AssertOneRow(_) => {
                    require_exact_children(node_path, "AssertOneRowNode", 1, actual)
                }
                plan::plan_node::Kind::Scan(_) => {
                    require_exact_children(node_path, "ScanNode", 0, actual)
                }
                plan::plan_node::Kind::HashAggregate(_) => {
                    require_exact_children(node_path, "HashAggregateNode", 1, actual)
                }
                plan::plan_node::Kind::HashJoin(_) => {
                    require_exact_children(node_path, "HashJoinNode", 2, actual)
                }
                plan::plan_node::Kind::NestLoopJoin(_) => {
                    require_exact_children(node_path, "NestLoopJoinNode", 2, actual)
                }
                plan::plan_node::Kind::Window(_) => {
                    require_exact_children(node_path, "WindowNode", 1, actual)
                }
                plan::plan_node::Kind::Repeat(_) => {
                    require_exact_children(node_path, "RepeatNode", 1, actual)
                }
                plan::plan_node::Kind::GenerateSeries(_) => {
                    require_exact_children(node_path, "GenerateSeriesNode", 0, actual)
                }
                plan::plan_node::Kind::TableFunction(_) => {
                    require_exact_children(node_path, "TableFunctionNode", 1, actual)
                }
                plan::plan_node::Kind::ChangeEventExpand(_) => {
                    require_exact_children(node_path, "ChangeEventExpandNode", 1, actual)
                }
                plan::plan_node::Kind::Redistribute(_) => {
                    require_exact_children(node_path, "RedistributeNode", 1, actual)
                }
                plan::plan_node::Kind::Decode(_)
                | plan::plan_node::Kind::CteAnchor(_)
                | plan::plan_node::Kind::CteProduce(_)
                | plan::plan_node::Kind::CteConsume(_) => Ok(()),
            }
        }
    }
}

fn apply_distributed_limit_if_needed(
    node: &plan::DistributedNode,
    mut lowered: DecodedNode,
    path: FieldPath,
) -> Result<DecodedNode, super::NativeFragmentDecodeError> {
    let Some(limit) = super::NativeFragmentDecodeError::map_invalid(
        path.field("limit"),
        parse_distributed_limit(node.limit, "DistributedNode.limit"),
    )?
    else {
        return Ok(lowered);
    };
    if matches!(
        lowered.node.kind,
        ExecNodeKind::Limit(_) | ExecNodeKind::Sort(_)
    ) {
        return Ok(lowered);
    }
    lowered.node = ExecNode {
        kind: ExecNodeKind::Limit(LimitNode {
            input: Box::new(lowered.node),
            node_id: node.node_id,
            limit: Some(limit),
            offset: 0,
        }),
    };
    Ok(lowered)
}

fn lower_physical_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    path: FieldPath,
    node_path: FieldPath,
    children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, super::NativeFragmentDecodeError> {
    let physical_output_path = path.clone().field("output_columns");
    let kind = physical.kind.as_ref().ok_or_else(|| {
        super::NativeFragmentDecodeError::missing(
            path.clone().field("kind"),
            format!("PlanNode node_id={} requires kind", node.node_id),
        )
    })?;
    match kind {
        plan::plan_node::Kind::Values(values) => values::lower_values_node(
            node,
            physical,
            values,
            path.clone().field("values"),
            physical_output_path.clone(),
            children,
            arena,
            ctx,
        ),
        plan::plan_node::Kind::Project(project) => project::lower_project_node(
            node,
            project,
            path.clone().field("project"),
            children,
            arena,
            ctx,
        ),
        plan::plan_node::Kind::Filter(filter) => filter::lower_filter_node(
            node,
            filter,
            path.clone().field("filter"),
            children,
            arena,
            ctx,
        ),
        plan::plan_node::Kind::Limit(limit) => limit::lower_limit_node(
            node,
            limit,
            path.clone().field("limit"),
            node_path,
            children,
        ),
        plan::plan_node::Kind::Sort(sort) => sort::lower_sort_node(
            node,
            physical,
            sort,
            path.clone().field("sort"),
            physical_output_path.clone(),
            children,
            arena,
            ctx,
        ),
        plan::plan_node::Kind::Topn(topn) => {
            topn::lower_topn_node(node, topn, path.clone().field("topn"), children, arena, ctx)
        }
        plan::plan_node::Kind::SetOp(set_op) => set_op::lower_set_op_node(
            node,
            physical,
            set_op,
            path.clone().field("set_op"),
            physical_output_path.clone(),
            children,
            arena,
            ctx,
        ),
        plan::plan_node::Kind::AssertOneRow(assert) => assert::lower_assert_one_row_node(
            node,
            assert,
            path.clone().field("assert_one_row"),
            children,
        ),
        plan::plan_node::Kind::Scan(scan) => super::scan::lower_scan_node(
            node,
            physical,
            scan,
            path.clone().field("scan"),
            ctx,
            arena,
        ),
        plan::plan_node::Kind::HashAggregate(aggregate) => aggregate::lower_hash_aggregate_node(
            node,
            physical,
            aggregate,
            path.clone().field("hash_aggregate"),
            physical_output_path.clone(),
            children,
            arena,
            ctx,
        ),
        plan::plan_node::Kind::HashJoin(join) => hash_join::lower_hash_join_node(
            node,
            physical,
            join,
            path.clone().field("hash_join"),
            node_path.clone(),
            physical_output_path.clone(),
            children,
            arena,
            ctx,
        ),
        plan::plan_node::Kind::NestLoopJoin(join) => nestloop_join::lower_nest_loop_join_node(
            node,
            physical,
            join,
            path.clone().field("nest_loop_join"),
            node_path.clone(),
            physical_output_path.clone(),
            children,
            arena,
            ctx,
        ),
        plan::plan_node::Kind::Window(window) => window::lower_window_node(
            node,
            physical,
            window,
            path.clone().field("window"),
            physical_output_path.clone(),
            children,
            arena,
            ctx,
        ),
        plan::plan_node::Kind::Repeat(repeat) => {
            repeat::lower_repeat_node(node, repeat, path.clone().field("repeat"), children)
        }
        plan::plan_node::Kind::GenerateSeries(generate_series) => {
            generate_series::lower_generate_series_node(
                node,
                generate_series,
                path.clone().field("generate_series"),
                children,
                arena,
                ctx,
            )
        }
        plan::plan_node::Kind::TableFunction(table_function) => {
            table_function::lower_table_function_node(
                node,
                table_function,
                path.clone().field("table_function"),
                children,
                arena,
                ctx,
            )
        }
        plan::plan_node::Kind::Decode(_) => Err(super::NativeFragmentDecodeError::unsupported(
            path.clone().field("decode"),
            "native physical node kind Decode is unsupported",
        )),
        plan::plan_node::Kind::ChangeEventExpand(expand) => {
            change_event_expand::lower_change_event_expand_node(
                node,
                physical,
                expand,
                path.clone().field("change_event_expand"),
                physical_output_path.clone(),
                children,
                arena,
                ctx,
            )
        }
        plan::plan_node::Kind::CteAnchor(_) => Err(super::NativeFragmentDecodeError::unsupported(
            path.clone().field("cte_anchor"),
            "native physical node kind CTEAnchor is unsupported",
        )),
        plan::plan_node::Kind::CteProduce(_) => Err(super::NativeFragmentDecodeError::unsupported(
            path.clone().field("cte_produce"),
            "native physical node kind CTEProduce is unsupported",
        )),
        plan::plan_node::Kind::CteConsume(_) => Err(super::NativeFragmentDecodeError::unsupported(
            path.clone().field("cte_consume"),
            "native physical node kind CTEConsume is unsupported",
        )),
        plan::plan_node::Kind::Redistribute(redistribute) => redistribute::lower_redistribute_node(
            physical,
            redistribute,
            path.clone().field("redistribute"),
            physical_output_path,
            children,
            arena,
            ctx,
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::exec::expr::ExprArena;
    use crate::exec::node::ExecNodeKind;
    use crate::exec::node::assert::{AssertNumRowsMode, Assertion};
    use crate::exec::node::set_op::SetOpKind;
    use crate::protocol::native::type_mapping::encode_type;
    use novarocks_protocol::{common, expr, plan};
    use novarocks_types::SlotId;

    struct DummyScanOp;

    impl crate::exec::node::scan::ScanOp for DummyScanOp {
        fn execute_iter(
            &self,
            _morsel: crate::exec::node::scan::ScanMorsel,
            _profile: Option<novarocks_execution::runtime::profile::RuntimeProfile>,
            _runtime_filters: Option<&crate::exec::node::scan::RuntimeFilterContext>,
        ) -> Result<crate::exec::node::BoxedExecIter, String> {
            Ok(Box::new(std::iter::empty()))
        }

        fn build_morsels(&self) -> Result<crate::exec::node::scan::ScanMorsels, String> {
            Ok(crate::exec::node::scan::ScanMorsels::default())
        }
    }

    pub(super) fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    pub(super) fn output_column_with_nullable(
        column_id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable,
            is_internal: false,
        }
    }

    pub(super) fn output_column(
        column_id: u32,
        name: &str,
        data_type: DataType,
    ) -> common::OutputColumn {
        output_column_with_nullable(column_id, name, data_type, true)
    }

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Int64)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    pub(super) fn string_literal(value: &str) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Utf8)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::StringValue(value.to_string())),
                }),
            })),
        }
    }

    pub(super) fn bool_literal(value: bool) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Boolean)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::BoolValue(value)),
                }),
            })),
        }
    }

    fn null_literal(data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::NullValue(true)),
                }),
            })),
        }
    }

    pub(super) fn column_ref(column_id: u32, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            })),
        }
    }

    pub(super) fn sort_item(column_id: u32) -> expr::SortItem {
        expr::SortItem {
            expr: Some(column_ref(column_id, DataType::Int64)),
            asc: true,
            nulls_first: false,
        }
    }

    pub(super) fn physical_node(
        node_id: i32,
        kind: plan::plan_node::Kind,
        output_columns: Vec<common::OutputColumn>,
        children: Vec<plan::DistributedNode>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(kind),
            })),
        }
    }

    pub(super) fn values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "name", DataType::Utf8),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![
                    plan::ExprList {
                        values: vec![int_literal(10), string_literal("alice")],
                    },
                    plan::ExprList {
                        values: vec![int_literal(20), string_literal("bob")],
                    },
                ],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn one_col_values_node(node_id: i32) -> plan::DistributedNode {
        one_col_values_node_with(node_id, 1, "id", 10)
    }

    pub(super) fn one_col_values_node_with(
        node_id: i32,
        column_id: u32,
        name: &str,
        value: i64,
    ) -> plan::DistributedNode {
        one_col_values_node_with_nullable(node_id, column_id, name, value, true)
    }

    pub(super) fn one_col_values_node_with_nullable(
        node_id: i32,
        column_id: u32,
        name: &str,
        value: i64,
        nullable: bool,
    ) -> plan::DistributedNode {
        let columns = vec![output_column_with_nullable(
            column_id,
            name,
            DataType::Int64,
            nullable,
        )];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(value)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn one_col_values_node_typed(
        node_id: i32,
        column_id: u32,
        name: &str,
        value: i64,
        data_type: DataType,
    ) -> plan::DistributedNode {
        let columns = vec![output_column(column_id, name, data_type)];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(value)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn two_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "a", DataType::Int64),
            output_column(2, "b", DataType::Int64),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10), int_literal(20)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn three_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "a", DataType::Int64),
            output_column(2, "b", DataType::Int64),
            output_column(3, "c", DataType::Int64),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10), int_literal(20), int_literal(30)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn lower(node: &plan::DistributedNode) -> super::DecodedNode {
        let mut arena = ExprArena::default();
        decode_node(node, &mut arena, &NativePlanDecodeContext::default()).expect("lower node")
    }

    fn decode_error(node: &plan::DistributedNode) -> super::super::NativeFragmentDecodeError {
        decode_node(
            node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .expect_err("invalid node must fail")
    }

    fn assert_children_error(node: &plan::DistributedNode) {
        let error = decode_error(node);
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.path().to_string(), "plan_fragment.root.children");
        assert_eq!(
            protocol.kind(),
            crate::protocol::common::error::ProtocolErrorKind::InconsistentFields
        );
    }

    #[test]
    fn rejects_scan_without_context_and_union_distinct() {
        let scan = physical_node(
            50,
            plan::plan_node::Kind::Scan(plan::ScanNode::default()),
            Vec::new(),
            Vec::new(),
        );
        let mut arena = ExprArena::default();
        let err = decode_node(&scan, &mut arena, &NativePlanDecodeContext::default()).unwrap_err();
        assert!(err.contains("Scan"));
        assert!(err.contains("table missing"));

        let union_distinct = physical_node(
            60,
            plan::plan_node::Kind::SetOp(plan::SetOpNode {
                kind: plan::PlanSetOpKind::UnionDistinct as i32,
                output_columns: vec![output_column(1, "id", DataType::Int64)],
                child_output_columns: Vec::new(),
            }),
            Vec::new(),
            vec![one_col_values_node(10), one_col_values_node(11)],
        );
        let err = decode_node(
            &union_distinct,
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap_err();
        assert!(err.contains("UnionDistinct"));
        assert!(err.contains("not implemented"));
    }

    #[test]
    fn lowers_union_all_intersect_except_and_assert_one_row() {
        let output_columns = vec![output_column(1, "id", DataType::Int64)];
        let union_all = physical_node(
            60,
            plan::plan_node::Kind::SetOp(plan::SetOpNode {
                kind: plan::PlanSetOpKind::UnionAll as i32,
                output_columns: output_columns.clone(),
                child_output_columns: Vec::new(),
            }),
            output_columns.clone(),
            vec![one_col_values_node(10), one_col_values_node(11)],
        );
        let lowered = lower(&union_all);
        assert!(matches!(lowered.node.kind, ExecNodeKind::UnionAll(_)));

        for (kind, expected) in [
            (plan::PlanSetOpKind::Intersect, SetOpKind::Intersect),
            (plan::PlanSetOpKind::Except, SetOpKind::Except),
        ] {
            let set_op = physical_node(
                61,
                plan::plan_node::Kind::SetOp(plan::SetOpNode {
                    kind: kind as i32,
                    output_columns: output_columns.clone(),
                    child_output_columns: Vec::new(),
                }),
                output_columns.clone(),
                vec![one_col_values_node(10), one_col_values_node(11)],
            );
            let lowered = lower(&set_op);
            let ExecNodeKind::SetOp(set_op) = lowered.node.kind else {
                panic!("expected SetOp");
            };
            assert_eq!(
                std::mem::discriminant(&set_op.kind),
                std::mem::discriminant(&expected)
            );
            assert_eq!(set_op.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        }

        let assert_one_row = physical_node(
            70,
            plan::plan_node::Kind::AssertOneRow(plan::AssertOneRowNode {
                subquery_text: "select id from t".to_string(),
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                group_key_column_ids: Vec::new(),
                group_key_labels: Vec::new(),
                keyed_message_prefix: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&assert_one_row);
        let ExecNodeKind::AssertNumRows(assert) = lowered.node.kind else {
            panic!("expected AssertNumRows");
        };
        match assert.mode {
            AssertNumRowsMode::Global {
                desired_num_rows,
                assertion,
                subquery_string,
            } => {
                assert_eq!(desired_num_rows, Some(1));
                assert!(matches!(assertion, Assertion::Le));
                assert_eq!(subquery_string.as_deref(), Some("select id from t"));
            }
            AssertNumRowsMode::PerKeyAtMostOne { .. } => panic!("expected global assert"),
        }
    }

    #[test]
    fn lowers_hash_aggregate_and_join_shapes() {
        let output_columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "cnt", DataType::Int64),
        ];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: vec![column_ref(1, DataType::Int64)],
                aggregates: vec![plan::PlanAggregateCall {
                    name: "count".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Int64)),
                    order_by: Vec::new(),
                    output_column_id: 2,
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: vec![output_columns[0].clone()],
                    aggregate_columns: vec![output_columns[1].clone()],
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&aggregate);
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        assert_eq!(aggregate.node_id, 20);
        assert_eq!(aggregate.group_by.len(), 1);
        assert_eq!(aggregate.functions.len(), 1);
        assert!(aggregate.need_finalize);
        assert_eq!(
            aggregate.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );

        let join = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::Inner as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Broadcast as i32,
                execution_mode: None,
            }),
            Vec::new(),
            vec![
                one_col_values_node_with(10, 1, "lhs", 10),
                one_col_values_node_with(11, 2, "rhs", 10),
            ],
        );
        let lowered = lower(&join);
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert_eq!(join.probe_keys.len(), 1);
        assert_eq!(join.build_keys.len(), 1);
        assert_eq!(join.eq_null_safe, vec![false]);
        assert_eq!(
            join.join_scope_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert!(matches!(
            join.join_type,
            crate::exec::node::join::JoinType::Inner
        ));
    }

    #[test]
    fn lowers_repeat_change_event_and_redistribute_shapes() {
        let repeat = physical_node(
            20,
            plan::plan_node::Kind::Repeat(plan::RepeatNode {
                repeat_column_ref_list: Vec::new(),
                repeat_column_ref_ids: vec![
                    plan::UInt32List { values: vec![1] },
                    plan::UInt32List { values: Vec::new() },
                ],
                grouping_ids: vec![0, 1],
                all_rollup_columns: vec!["id".to_string()],
                all_rollup_column_ids: vec![1],
                grouping_key_aliases: Vec::new(),
                grouping_fn_args: Vec::new(),
                grouping_fn_arg_ids: vec![plan::UInt32List { values: vec![1] }],
                grouping_fn_ids: vec![plan::NamedUInt32 {
                    name: "__grouping_fn_0".to_string(),
                    value: 9,
                }],
                virtual_tuple_id: Some(7),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&repeat);
        let ExecNodeKind::Repeat(repeat) = lowered.node.kind else {
            panic!("expected Repeat");
        };
        assert_eq!(repeat.repeat_times, 2);
        assert_eq!(repeat.null_slot_ids, vec![vec![], vec![SlotId::new(1)]]);
        assert_eq!(repeat.grouping_slot_ids, vec![SlotId::new(9)]);
        assert_eq!(repeat.grouping_list, vec![vec![0, 1]]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1), SlotId::new(9)]);
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(9)]
        );

        let change_event = physical_node(
            30,
            plan::plan_node::Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: vec![plan::DistributedChangeEventSpec {
                    predicate: None,
                    effect: plan::RowMutationEffect::Delete as i32,
                    assignments: vec![plan::DistributedChangeEventOutputExpr {
                        output_column_id: 2,
                        expr: None,
                    }],
                }],
                output_columns: vec![
                    output_column(1, "id", DataType::Int64),
                    output_column(2, "op", DataType::Int8),
                ],
                effect_column_id: 2,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&change_event);
        let ExecNodeKind::ChangeEventExpand(change_event) = lowered.node.kind else {
            panic!("expected ChangeEventExpand");
        };
        assert_eq!(
            change_event.output_slot_ids,
            vec![SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(change_event.effect_slot_id, SlotId::new(2));
        assert_eq!(change_event.events.len(), 1);

        let redistribute = physical_node(
            40,
            plan::plan_node::Kind::Redistribute(plan::RedistributeNode {
                mode: Some(plan::RedistributeMode {
                    mode: Some(plan::redistribute_mode::Mode::Gather(true)),
                }),
                partition_exprs: Vec::new(),
                output_columns: vec![output_column(1, "id", DataType::Int64)],
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&redistribute);
        assert!(matches!(lowered.node.kind, ExecNodeKind::Values(_)));
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }
}
