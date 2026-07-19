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

//! Proto node lowering placeholder.

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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use self::common::*;

use super::layout::Layout;
use super::runtime_filter_binding::{
    DecodedBindingRole, DecodedConsumerBindingTarget, DecodedRuntimeFilterBinding,
    DecodedRuntimeFilterContract, DecodedRuntimeFilterReduction, RuntimeFilterBindingLookupLedger,
};
use crate::common::types::UniqueId;
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::ExprArena;
use crate::exec::node::join::{JoinRuntimeFilterExecution, NativeJoinRuntimeFilterProducerSpec};
use crate::exec::node::limit::LimitNode;
use crate::exec::node::runtime_filter::{
    NativeRuntimeFilterAvailability, NativeRuntimeFilterConsumerNode,
    NativeRuntimeFilterConsumerSpec, NativeRuntimeFilterContract, NativeRuntimeFilterReduction,
};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::{novarocks, plan};
use crate::runtime::exchange::ExchangeKey;
use crate::runtime::query_options::QueryOptions;

#[derive(Clone, Debug)]
pub(crate) struct LoweredNode {
    pub node: ExecNode,
    pub layout: Layout,
    pub output_schema: ChunkSchemaRef,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NodeLoweringContext {
    exchange_sender_counts: HashMap<ExchangeKey, usize>,
    scan_ranges: HashMap<i32, Vec<novarocks::ScanRangeParams>>,
    query_options: Option<QueryOptions>,
    connectors: Option<Arc<crate::connector::ConnectorRegistry>>,
    query_id: Option<UniqueId>,
    fragment_instance_hi: i64,
    fragment_instance_lo: i64,
}

impl NodeLoweringContext {
    #[allow(dead_code)]
    pub(crate) fn with_fragment_instance_id(mut self, hi: i64, lo: i64) -> Self {
        self.fragment_instance_hi = hi;
        self.fragment_instance_lo = lo;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_exchange_sender_count(mut self, key: ExchangeKey, count: usize) -> Self {
        self.exchange_sender_counts.insert(key, count);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_scan_ranges(
        mut self,
        node_id: i32,
        ranges: Vec<novarocks::ScanRangeParams>,
    ) -> Self {
        self.scan_ranges.insert(node_id, ranges);
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
    pub(crate) fn with_query_id(mut self, query_id: UniqueId) -> Self {
        self.query_id = Some(query_id);
        self
    }

    pub(crate) fn scan_ranges(
        &self,
        node_id: i32,
    ) -> Result<&[novarocks::ScanRangeParams], String> {
        self.scan_ranges
            .get(&node_id)
            .map(Vec::as_slice)
            .ok_or_else(|| format!("native ScanNode node_id={node_id} missing scan ranges"))
    }

    pub(crate) fn query_options(&self) -> Option<&QueryOptions> {
        self.query_options.as_ref()
    }

    #[cfg(feature = "compat")]
    pub(crate) fn query_id(&self) -> Option<UniqueId> {
        self.query_id
    }

    pub(crate) fn connectors(&self) -> Result<&crate::connector::ConnectorRegistry, String> {
        self.connectors.as_deref().ok_or_else(|| {
            "native ScanNode requires ConnectorRegistry in NodeLoweringContext".to_string()
        })
    }

    fn exchange_key(&self, node_id: i32) -> ExchangeKey {
        ExchangeKey {
            finst_id_hi: self.fragment_instance_hi,
            finst_id_lo: self.fragment_instance_lo,
            node_id,
        }
    }
}

#[allow(dead_code)]
pub(crate) fn lower_proto_node(
    node: &plan::DistributedNode,
    arena: &mut ExprArena,
    ctx: &NodeLoweringContext,
) -> Result<LoweredNode, String> {
    lower_proto_node_inner(node, arena, ctx, None)
}

pub(crate) fn lower_proto_node_with_bindings(
    node: &plan::DistributedNode,
    arena: &mut ExprArena,
    ctx: &NodeLoweringContext,
    ledger: &mut RuntimeFilterBindingLookupLedger,
) -> Result<LoweredNode, String> {
    lower_proto_node_inner(node, arena, ctx, Some(ledger))
}

fn lower_proto_node_inner(
    node: &plan::DistributedNode,
    arena: &mut ExprArena,
    ctx: &NodeLoweringContext,
    mut ledger: Option<&mut RuntimeFilterBindingLookupLedger>,
) -> Result<LoweredNode, String> {
    let mut children = Vec::with_capacity(node.children.len());
    for child in &node.children {
        children.push(lower_proto_node_inner(
            child,
            arena,
            ctx,
            ledger.as_deref_mut(),
        )?);
    }

    let attached = ledger
        .as_deref()
        .map(|ledger| {
            ledger.peek_attached(
                &node.runtime_filter_binding_ids,
                node.node_id,
                node.fragment_id,
            )
        })
        .transpose()?
        .unwrap_or_default();
    let direct_inputs = children
        .iter()
        .map(|child| (child.layout.clone(), child.output_schema.clone()))
        .collect::<Vec<_>>();
    let (consumer_bindings, producer_bindings): (Vec<_>, Vec<_>) = attached
        .into_iter()
        .partition(|binding| matches!(binding.role, DecodedBindingRole::Consumer { .. }));
    if !children.is_empty() {
        attach_direct_input_consumers(node.node_id, &consumer_bindings, &mut children, arena)?;
    }

    let payload = node
        .payload
        .as_ref()
        .ok_or_else(|| format!("DistributedNode node_id={} payload missing", node.node_id))?;
    let mut lowered = match payload {
        plan::distributed_node::Payload::Physical(physical) => {
            lower_physical_node(node, physical, children, arena, ctx)
        }
        plan::distributed_node::Payload::Exchange(exchange) => {
            exchange::lower_exchange_receiver(node, exchange, children, arena, ctx)
        }
    }?;
    if children_are_absent(node) && !consumer_bindings.is_empty() {
        attach_leaf_consumers(node, &consumer_bindings, &mut lowered, arena)?;
    }
    if !producer_bindings.is_empty() {
        attach_hash_join_producers(
            node,
            &producer_bindings,
            &direct_inputs,
            &mut lowered,
            arena,
        )?;
    }
    let lowered = apply_distributed_limit_if_needed(node, lowered)?;
    if let Some(ledger) = ledger {
        ledger.commit_consumed_many(&node.runtime_filter_binding_ids)?;
    }
    Ok(lowered)
}

fn children_are_absent(node: &plan::DistributedNode) -> bool {
    node.children.is_empty()
}

fn attach_direct_input_consumers(
    owner_node_id: i32,
    bindings: &[DecodedRuntimeFilterBinding],
    children: &mut [LoweredNode],
    arena: &mut ExprArena,
) -> Result<(), String> {
    let mut grouped = BTreeMap::<usize, Vec<NativeRuntimeFilterConsumerSpec>>::new();
    for binding in bindings {
        let DecodedBindingRole::Consumer { target, .. } = &binding.role else {
            return Err(format!(
                "native runtime-filter binding_id={} expected consumer role",
                binding.binding_id
            ));
        };
        let DecodedConsumerBindingTarget::DirectInput {
            input_ordinal: index,
        } = *target
        else {
            return Err(format!(
                "native runtime-filter consumer binding_id={} on node_id={owner_node_id} must target a direct input",
                binding.binding_id
            ));
        };
        let child = children.get(index).ok_or_else(|| {
            format!(
                "native runtime-filter consumer binding_id={} on node_id={owner_node_id} targets missing direct input ordinal={index}, input_count={}",
                binding.binding_id,
                children.len()
            )
        })?;
        let expr_id =
            lower_binding_expression(binding, &child.layout, &child.output_schema, arena)?;
        grouped
            .entry(index)
            .or_default()
            .push(consumer_spec(binding, expr_id)?);
    }
    for (index, specs) in grouped {
        let child = &mut children[index];
        let input = child.node.clone();
        child.node = ExecNode {
            kind: ExecNodeKind::NativeRuntimeFilterConsumer(NativeRuntimeFilterConsumerNode {
                input: Box::new(input),
                owner_node_id,
                bindings: specs,
            }),
        };
    }
    Ok(())
}

fn attach_leaf_consumers(
    wire_node: &plan::DistributedNode,
    bindings: &[DecodedRuntimeFilterBinding],
    lowered: &mut LoweredNode,
    arena: &mut ExprArena,
) -> Result<(), String> {
    for binding in bindings {
        let DecodedBindingRole::Consumer { target, .. } = &binding.role else {
            return Err(format!(
                "native runtime-filter binding_id={} expected consumer role",
                binding.binding_id
            ));
        };
        if *target != DecodedConsumerBindingTarget::SourceBoundary {
            return Err(format!(
                "native runtime-filter consumer binding_id={} on leaf node_id={} must target source boundary",
                binding.binding_id, wire_node.node_id
            ));
        }
    }
    let specs = bindings
        .iter()
        .map(|binding| {
            let expr_id =
                lower_binding_expression(binding, &lowered.layout, &lowered.output_schema, arena)?;
            consumer_spec(binding, expr_id)
        })
        .collect::<Result<Vec<_>, String>>()?;
    let payload = wire_node
        .payload
        .as_ref()
        .ok_or_else(|| format!("native node_id={} payload missing", wire_node.node_id))?;
    match payload {
        plan::distributed_node::Payload::Exchange(_) => {
            let exchange = find_exchange_source_mut(&mut lowered.node).ok_or_else(|| {
                format!(
                    "native node_id={} exchange lowering lost ExchangeSource boundary",
                    wire_node.node_id
                )
            })?;
            exchange.set_native_runtime_filter_specs(specs);
        }
        plan::distributed_node::Payload::Physical(physical) => match physical.kind.as_ref() {
            Some(plan::plan_node::Kind::Scan(_)) => {
                set_native_scan_specs(&mut lowered.node, specs).map_err(|_| {
                    format!(
                        "native node_id={} scan lowering lost Scan boundary",
                        wire_node.node_id
                    )
                })?;
            }
            Some(plan::plan_node::Kind::Values(_))
            | Some(plan::plan_node::Kind::GenerateSeries(_)) => {
                wrap_source_boundary(&mut lowered.node, wire_node.node_id, specs);
            }
            kind => {
                return Err(format!(
                    "native runtime-filter consumer binding on leaf node_id={} has unsupported source capability: {kind:?}",
                    wire_node.node_id
                ));
            }
        },
    }
    Ok(())
}

fn wrap_source_boundary(
    node: &mut ExecNode,
    owner_node_id: i32,
    bindings: Vec<NativeRuntimeFilterConsumerSpec>,
) {
    let input = node.clone();
    *node = ExecNode {
        kind: ExecNodeKind::NativeRuntimeFilterConsumer(NativeRuntimeFilterConsumerNode {
            input: Box::new(input),
            owner_node_id,
            bindings,
        }),
    };
}

fn set_native_scan_specs(
    node: &mut ExecNode,
    specs: Vec<NativeRuntimeFilterConsumerSpec>,
) -> Result<(), Vec<NativeRuntimeFilterConsumerSpec>> {
    match &mut node.kind {
        ExecNodeKind::Scan(scan) => {
            scan.set_native_runtime_filter_specs(specs);
            Ok(())
        }
        ExecNodeKind::IcebergDeltaScan(scan) => {
            scan.set_native_runtime_filter_specs(specs);
            Ok(())
        }
        ExecNodeKind::Project(project) if project.is_subordinate => {
            set_native_scan_specs(&mut project.input, specs)
        }
        ExecNodeKind::Filter(filter) => set_native_scan_specs(&mut filter.input, specs),
        _ => Err(specs),
    }
}

fn find_exchange_source_mut(
    node: &mut ExecNode,
) -> Option<&mut crate::exec::node::exchange_source::ExchangeSourceNode> {
    match &mut node.kind {
        ExecNodeKind::ExchangeSource(exchange) => Some(exchange),
        ExecNodeKind::Limit(limit) => find_exchange_source_mut(&mut limit.input),
        ExecNodeKind::Sort(sort) => find_exchange_source_mut(&mut sort.input),
        _ => None,
    }
}

fn attach_hash_join_producers(
    wire_node: &plan::DistributedNode,
    bindings: &[DecodedRuntimeFilterBinding],
    direct_inputs: &[(Layout, ChunkSchemaRef)],
    lowered: &mut LoweredNode,
    arena: &mut ExprArena,
) -> Result<(), String> {
    let ExecNodeKind::Join(join) = &mut lowered.node.kind else {
        return Err(format!(
            "native runtime-filter producer binding is only supported on HashJoin, node_id={}",
            wire_node.node_id
        ));
    };
    if direct_inputs.len() != 2 {
        return Err(format!(
            "native HashJoin node_id={} missing two direct inputs",
            wire_node.node_id
        ));
    }
    let build_input_index = if join.join_type == crate::exec::node::join::JoinType::RightSemi {
        0
    } else {
        1
    };
    let (build_layout, build_schema) = &direct_inputs[build_input_index];
    let plan::distributed_node::Payload::Physical(physical) =
        wire_node.payload.as_ref().ok_or_else(|| {
            format!(
                "native HashJoin node_id={} payload missing",
                wire_node.node_id
            )
        })?
    else {
        return Err(format!(
            "native runtime-filter producer node_id={} is not physical HashJoin",
            wire_node.node_id
        ));
    };
    let Some(plan::plan_node::Kind::HashJoin(wire_join)) = physical.kind.as_ref() else {
        return Err(format!(
            "native runtime-filter producer node_id={} is not HashJoin",
            wire_node.node_id
        ));
    };
    let mut producers = Vec::with_capacity(bindings.len());
    for binding in bindings {
        let DecodedBindingRole::Producer {
            contribution_kinds,
            completion_requirement,
            join_key_ordinal,
        } = &binding.role
        else {
            return Err(format!(
                "native runtime-filter binding_id={} expected producer role",
                binding.binding_id
            ));
        };
        let build_key_index = *join_key_ordinal;
        let condition = wire_join.eq_conditions.get(build_key_index).ok_or_else(|| {
            format!(
                "native runtime-filter producer binding_id={} targets missing join key ordinal={build_key_index}, key_count={}",
                binding.binding_id,
                wire_join.eq_conditions.len()
            )
        })?;
        if condition.null_safe {
            return Err(format!(
                "native runtime-filter producer binding_id={} targets null-safe join key ordinal={build_key_index}",
                binding.binding_id
            ));
        }
        let raw_build = if join.join_type == crate::exec::node::join::JoinType::RightSemi {
            condition.left.as_ref()
        } else {
            condition.right.as_ref()
        }
        .ok_or_else(|| {
            format!(
                "native runtime-filter producer binding_id={} join key ordinal={build_key_index} missing build expression",
                binding.binding_id
            )
        })?;
        if raw_build != &binding.expression {
            return Err(format!(
                "native runtime-filter producer binding_id={} expression does not match join key ordinal={build_key_index}",
                binding.binding_id
            ));
        }
        validate_column_refs_exact(binding.binding_id, raw_build, build_layout, build_schema)?;
        let build_expr_id = lower_binding_expression(binding, build_layout, build_schema, arena)?;
        producers.push(NativeJoinRuntimeFilterProducerSpec {
            binding_id: binding.binding_id,
            channel_id: binding.channel_id,
            build_expr_id,
            build_key_index,
            contribution_kinds: contribution_kinds.clone(),
            completion_requirement: *completion_requirement,
            contract: native_contract(&binding.contract),
            reduction: native_reduction(&binding.reduction),
            availability: NativeRuntimeFilterAvailability::DeploymentNotInstalled,
        });
    }
    join.runtime_filter_execution = JoinRuntimeFilterExecution::Native { producers };
    Ok(())
}

fn consumer_spec(
    binding: &DecodedRuntimeFilterBinding,
    expr_id: crate::exec::expr::ExprId,
) -> Result<NativeRuntimeFilterConsumerSpec, String> {
    let DecodedBindingRole::Consumer {
        capabilities,
        activation,
        ..
    } = &binding.role
    else {
        return Err(format!(
            "native runtime-filter binding_id={} expected consumer role",
            binding.binding_id
        ));
    };
    Ok(NativeRuntimeFilterConsumerSpec {
        binding_id: binding.binding_id,
        channel_id: binding.channel_id,
        expr_id,
        activation: *activation,
        capabilities: capabilities.clone(),
        contract: native_contract(&binding.contract),
        reduction: native_reduction(&binding.reduction),
        availability: NativeRuntimeFilterAvailability::DeploymentNotInstalled,
    })
}

fn native_contract(contract: &DecodedRuntimeFilterContract) -> NativeRuntimeFilterContract {
    match contract {
        DecodedRuntimeFilterContract::Membership {
            canonical_schema,
            schema_digest,
        } => NativeRuntimeFilterContract::Membership {
            canonical_schema: Arc::clone(canonical_schema),
            schema_digest: *schema_digest,
        },
        DecodedRuntimeFilterContract::Ordered {
            keys,
            comparator_digest,
            order_contract_digest,
        } => NativeRuntimeFilterContract::Ordered {
            keys: Arc::clone(keys),
            comparator_digest: *comparator_digest,
            order_contract_digest: *order_contract_digest,
        },
    }
}

fn native_reduction(reduction: &DecodedRuntimeFilterReduction) -> NativeRuntimeFilterReduction {
    match reduction {
        DecodedRuntimeFilterReduction::SetUnion => NativeRuntimeFilterReduction::SetUnion,
        DecodedRuntimeFilterReduction::TightenOrderedBound => {
            NativeRuntimeFilterReduction::TightenOrderedBound
        }
        DecodedRuntimeFilterReduction::MergeTopKSummary { k, contract_digest } => {
            NativeRuntimeFilterReduction::MergeTopKSummary {
                k: *k,
                contract_digest: *contract_digest,
            }
        }
    }
}

fn lower_binding_expression(
    binding: &DecodedRuntimeFilterBinding,
    layout: &Layout,
    schema: &ChunkSchemaRef,
    arena: &mut ExprArena,
) -> Result<crate::exec::expr::ExprId, String> {
    validate_column_refs_exact(binding.binding_id, &binding.expression, layout, schema)?;
    super::expr::lower_proto_expr(&binding.expression, arena, layout).map_err(|error| {
        format!(
            "native runtime-filter binding_id={} expression: {error}",
            binding.binding_id
        )
    })
}

fn validate_column_refs_exact(
    binding_id: u32,
    expression: &crate::proto::expr::Expr,
    layout: &Layout,
    schema: &ChunkSchemaRef,
) -> Result<(), String> {
    use crate::proto::expr::expr::Kind;
    let kind = expression.kind.as_ref().ok_or_else(|| {
        format!("native runtime-filter binding_id={binding_id} expression kind missing")
    })?;
    if let Kind::ColumnRef(column) = kind {
        let slot_id = layout
            .resolve_column_id(column.column_id)
            .map_err(|error| format!("native runtime-filter binding_id={binding_id}: {error}"))?;
        let expected = schema.field_by_slot(slot_id).ok_or_else(|| {
            format!("native runtime-filter binding_id={binding_id} ColumnRef column_id={} has no ChunkSchema field", column.column_id)
        })?;
        let type_desc = expression.r#type.as_ref().ok_or_else(|| {
            format!(
                "native runtime-filter binding_id={binding_id} ColumnRef column_id={} type missing",
                column.column_id
            )
        })?;
        let actual = super::decode_field_type("_runtime_filter_column", expression.nullable, type_desc)
            .map_err(|error| format!("native runtime-filter binding_id={binding_id} ColumnRef column_id={} type: {error}", column.column_id))?;
        if expected.data_type() != actual.data_type()
            || expected.is_nullable() != actual.is_nullable()
            || crate::exec::chunk::ChunkFieldSchema::from_field(expected)?
                != crate::exec::chunk::ChunkFieldSchema::from_field(&actual)?
        {
            return Err(format!(
                "native runtime-filter binding_id={binding_id} ColumnRef column_id={} type/nullability does not exactly match direct input",
                column.column_id
            ));
        }
    }
    let mut visit = |child: &crate::proto::expr::Expr| {
        validate_column_refs_exact(binding_id, child, layout, schema)
    };
    match kind {
        Kind::ColumnRef(_) | Kind::Literal(_) | Kind::LambdaParamRef(_) => Ok(()),
        Kind::BinaryOp(binary) => {
            visit(
                binary
                    .left
                    .as_ref()
                    .ok_or_else(|| "BinaryOp.left missing".to_string())?,
            )?;
            visit(
                binary
                    .right
                    .as_ref()
                    .ok_or_else(|| "BinaryOp.right missing".to_string())?,
            )
        }
        Kind::UnaryOp(unary) => visit(
            unary
                .operand
                .as_ref()
                .ok_or_else(|| "UnaryOp.operand missing".to_string())?,
        ),
        Kind::FunctionCall(call) => call.args.iter().try_for_each(&mut visit),
        Kind::AggregateCall(call) => {
            call.args.iter().try_for_each(&mut visit)?;
            call.order_by.iter().try_for_each(|item| {
                visit(
                    item.expr
                        .as_ref()
                        .ok_or_else(|| "SortItem.expr missing".to_string())?,
                )
            })
        }
        Kind::WindowCall(call) => {
            call.args.iter().try_for_each(&mut visit)?;
            call.partition_by.iter().try_for_each(&mut visit)?;
            call.order_by.iter().try_for_each(|item| {
                visit(
                    item.expr
                        .as_ref()
                        .ok_or_else(|| "SortItem.expr missing".to_string())?,
                )
            })
        }
        Kind::Cast(cast) => visit(
            cast.operand
                .as_ref()
                .ok_or_else(|| "Cast.operand missing".to_string())?,
        ),
        Kind::IsNull(is_null) => visit(
            is_null
                .operand
                .as_ref()
                .ok_or_else(|| "IsNull.operand missing".to_string())?,
        ),
        Kind::InList(in_list) => {
            visit(
                in_list
                    .operand
                    .as_ref()
                    .ok_or_else(|| "InList.operand missing".to_string())?,
            )?;
            in_list.list.iter().try_for_each(&mut visit)
        }
        Kind::Between(between) => {
            visit(
                between
                    .operand
                    .as_ref()
                    .ok_or_else(|| "Between.operand missing".to_string())?,
            )?;
            visit(
                between
                    .low
                    .as_ref()
                    .ok_or_else(|| "Between.low missing".to_string())?,
            )?;
            visit(
                between
                    .high
                    .as_ref()
                    .ok_or_else(|| "Between.high missing".to_string())?,
            )
        }
        Kind::Like(like) => {
            visit(
                like.operand
                    .as_ref()
                    .ok_or_else(|| "Like.operand missing".to_string())?,
            )?;
            visit(
                like.pattern
                    .as_ref()
                    .ok_or_else(|| "Like.pattern missing".to_string())?,
            )
        }
        Kind::CaseExpr(case_expr) => {
            if let Some(operand) = &case_expr.operand {
                visit(operand)?;
            }
            for branch in &case_expr.when_then {
                visit(
                    branch
                        .when
                        .as_ref()
                        .ok_or_else(|| "Case.when missing".to_string())?,
                )?;
                visit(
                    branch
                        .then
                        .as_ref()
                        .ok_or_else(|| "Case.then missing".to_string())?,
                )?;
            }
            if let Some(else_expr) = &case_expr.else_expr {
                visit(else_expr)?;
            }
            Ok(())
        }
        Kind::IsTruth(is_truth) => visit(
            is_truth
                .operand
                .as_ref()
                .ok_or_else(|| "IsTruth.operand missing".to_string())?,
        ),
        Kind::Lambda(lambda) => visit(
            lambda
                .body
                .as_ref()
                .ok_or_else(|| "Lambda.body missing".to_string())?,
        ),
        Kind::Nested(nested) => visit(
            nested
                .inner
                .as_ref()
                .ok_or_else(|| "Nested.inner missing".to_string())?,
        ),
    }
}

fn apply_distributed_limit_if_needed(
    node: &plan::DistributedNode,
    mut lowered: LoweredNode,
) -> Result<LoweredNode, String> {
    let Some(limit) = parse_distributed_limit(node.limit, "DistributedNode.limit")? else {
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
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
    ctx: &NodeLoweringContext,
) -> Result<LoweredNode, String> {
    let kind = physical
        .kind
        .as_ref()
        .ok_or_else(|| format!("PlanNode node_id={} kind missing", node.node_id))?;
    match kind {
        plan::plan_node::Kind::Values(values) => {
            values::lower_values_node(node, physical, values, children, arena)
        }
        plan::plan_node::Kind::Project(project) => {
            project::lower_project_node(node, project, children, arena)
        }
        plan::plan_node::Kind::Filter(filter) => {
            filter::lower_filter_node(node, filter, children, arena)
        }
        plan::plan_node::Kind::Limit(limit) => limit::lower_limit_node(node, limit, children),
        plan::plan_node::Kind::Sort(sort) => {
            sort::lower_sort_node(node, physical, sort, children, arena)
        }
        plan::plan_node::Kind::Topn(topn) => topn::lower_topn_node(node, topn, children, arena),
        plan::plan_node::Kind::SetOp(set_op) => {
            set_op::lower_set_op_node(node, physical, set_op, children, arena)
        }
        plan::plan_node::Kind::AssertOneRow(assert) => {
            assert::lower_assert_one_row_node(node, assert, children)
        }
        plan::plan_node::Kind::Scan(scan) => {
            super::scan::lower_scan_node(node, physical, scan, ctx, arena)
        }
        plan::plan_node::Kind::HashAggregate(aggregate) => {
            aggregate::lower_hash_aggregate_node(node, physical, aggregate, children, arena)
        }
        plan::plan_node::Kind::HashJoin(join) => {
            hash_join::lower_hash_join_node(node, physical, join, children, arena)
        }
        plan::plan_node::Kind::NestLoopJoin(join) => {
            nestloop_join::lower_nest_loop_join_node(node, physical, join, children, arena)
        }
        plan::plan_node::Kind::Window(window) => {
            window::lower_window_node(node, physical, window, children, arena)
        }
        plan::plan_node::Kind::Repeat(repeat) => repeat::lower_repeat_node(node, repeat, children),
        plan::plan_node::Kind::GenerateSeries(generate_series) => {
            generate_series::lower_generate_series_node(node, generate_series, children, arena)
        }
        plan::plan_node::Kind::TableFunction(table_function) => {
            table_function::lower_table_function_node(node, table_function, children, arena)
        }
        plan::plan_node::Kind::Decode(_) => unsupported("Decode"),
        plan::plan_node::Kind::ChangeEventExpand(expand) => {
            change_event_expand::lower_change_event_expand_node(
                node, physical, expand, children, arena,
            )
        }
        plan::plan_node::Kind::CteAnchor(_) => unsupported("CTEAnchor"),
        plan::plan_node::Kind::CteProduce(_) => unsupported("CTEProduce"),
        plan::plan_node::Kind::CteConsume(_) => unsupported("CTEConsume"),
        plan::plan_node::Kind::Redistribute(redistribute) => {
            redistribute::lower_redistribute_node(physical, redistribute, children, arena)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::expr::ExprArena;
    use crate::exec::node::ExecNodeKind;
    use crate::exec::node::assert::{AssertNumRowsMode, Assertion};
    use crate::exec::node::set_op::SetOpKind;
    use crate::proto::{common, expr, plan};
    use crate::types::native_proto::encode_type;

    struct DummyScanOp;

    impl crate::exec::node::scan::ScanOp for DummyScanOp {
        fn execute_iter(
            &self,
            _morsel: crate::exec::node::scan::ScanMorsel,
            _profile: Option<crate::runtime::profile::RuntimeProfile>,
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

    pub(super) fn lower(node: &plan::DistributedNode) -> super::LoweredNode {
        let mut arena = ExprArena::default();
        lower_proto_node(node, &mut arena, &NodeLoweringContext::default()).expect("lower node")
    }

    fn dormant_consumer(
        binding_id: u32,
        node_id: i32,
        column_id: u32,
    ) -> DecodedRuntimeFilterBinding {
        DecodedRuntimeFilterBinding {
            binding_id,
            channel_id: binding_id + 10,
            node_id,
            apply_point: super::super::runtime_filter_binding::DecodedApplyPoint::NodeInput,
            expression: column_ref(column_id, DataType::Int64),
            role: DecodedBindingRole::Consumer {
                capabilities: BTreeSet::from([
                    crate::runtime_filter::model::contract::ArtifactCapability::Membership,
                    crate::runtime_filter::model::contract::ArtifactCapability::EmptyDomain,
                ]),
                activation:
                    crate::runtime_filter::model::contract::ConsumerActivation::BlockingSnapshot,
                target: DecodedConsumerBindingTarget::DirectInput { input_ordinal: 0 },
            },
            contract: DecodedRuntimeFilterContract::Membership {
                canonical_schema: Arc::from([]),
                schema_digest: [0; 32],
            },
            reduction: DecodedRuntimeFilterReduction::SetUnion,
        }
    }

    fn dormant_source_consumer(
        binding_id: u32,
        node_id: i32,
        column_id: u32,
    ) -> DecodedRuntimeFilterBinding {
        let mut binding = dormant_consumer(binding_id, node_id, column_id);
        let DecodedBindingRole::Consumer { target, .. } = &mut binding.role else {
            unreachable!("dormant_consumer always returns a consumer")
        };
        *target = DecodedConsumerBindingTarget::SourceBoundary;
        binding
    }

    fn membership_producer_wire(
        binding_id: u32,
        node_id: i32,
        expression: expr::Expr,
        data_type: &DataType,
    ) -> plan::RuntimeFilterBinding {
        membership_producer_wire_at(binding_id, node_id, expression, data_type, 0)
    }

    fn membership_producer_wire_at(
        binding_id: u32,
        node_id: i32,
        expression: expr::Expr,
        data_type: &DataType,
        join_key_ordinal: u32,
    ) -> plan::RuntimeFilterBinding {
        let schema = crate::runtime_filter::port::artifact::ArtifactMembershipSchema::new(
            data_type,
            crate::runtime_filter::model::contract::NullSemantics::NeverMatches,
        )
        .expect("membership schema");
        plan::RuntimeFilterBinding {
            binding_id,
            channel_id: binding_id + 10,
            node_id,
            apply_point: i32::from(plan::RuntimeFilterApplyPoint::NodeOutput),
            expression: Some(expression),
            contract: Some(plan::RuntimeFilterContract {
                kind: Some(plan::runtime_filter_contract::Kind::Membership(
                    plan::RuntimeFilterMembershipContract {
                        canonical_schema: schema.canonical_bytes().to_vec(),
                        schema_digest: schema.digest().bytes().to_vec(),
                    },
                )),
            }),
            reduction: Some(plan::RuntimeFilterReductionContract {
                kind: Some(plan::runtime_filter_reduction_contract::Kind::SetUnion(
                    true,
                )),
            }),
            role: Some(plan::runtime_filter_binding::Role::Producer(
                plan::RuntimeFilterProducerRole {
                    contribution_kinds: vec![
                        i32::from(plan::RuntimeFilterContributionKind::ValueDomainDelta),
                        i32::from(plan::RuntimeFilterContributionKind::ProducerClosed),
                    ],
                    completion_requirement: i32::from(
                        plan::RuntimeFilterCompletionRequirement::ProducerClosed,
                    ),
                    join_key_ordinal: Some(join_key_ordinal),
                },
            )),
        }
    }

    fn cast_expr(operand: expr::Expr, data_type: DataType, nullable: bool) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable,
            kind: Some(expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                operand: Some(Box::new(operand)),
                target: Some(type_desc(&data_type)),
            }))),
        }
    }

    #[test]
    fn producer_lookup_is_validated_and_consumed_by_interim_dormant_seam() {
        let left_wire = one_col_values_node_with(10, 1, "lhs", 10);
        let right_wire = one_col_values_node_with(11, 2, "rhs", 20);
        let mut wire = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: i32::from(plan::JoinKind::Inner),
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: i32::from(plan::JoinDistribution::Broadcast),
                execution_mode: None,
            }),
            Vec::new(),
            vec![left_wire.clone(), right_wire.clone()],
        );
        wire.runtime_filter_binding_ids = vec![1];
        let table = plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![membership_producer_wire(
                1,
                30,
                column_ref(2, DataType::Int64),
                &DataType::Int64,
            )],
        };
        let mut ledger = RuntimeFilterBindingLookupLedger::decode(1, Some(&table))
            .expect("decode producer table");
        let mut arena = ExprArena::default();
        let lowered = lower_proto_node_with_bindings(
            &wire,
            &mut arena,
            &NodeLoweringContext::default(),
            &mut ledger,
        )
        .expect("producer seam");
        ledger.finish().expect("producer binding consumed");
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("producer seam")
        };
        let JoinRuntimeFilterExecution::Native { producers } = join.runtime_filter_execution else {
            panic!("native producer execution")
        };
        assert_eq!(producers.len(), 1);

        let mut nullable_mismatch = column_ref(2, DataType::Int64);
        nullable_mismatch.nullable = false;
        let invalid_table = plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![membership_producer_wire(
                1,
                30,
                nullable_mismatch,
                &DataType::Int64,
            )],
        };
        let mut ledger = RuntimeFilterBindingLookupLedger::decode(1, Some(&invalid_table))
            .expect("decode invalid lowering table");
        assert!(
            lower_proto_node_with_bindings(
                &wire,
                &mut ExprArena::default(),
                &NodeLoweringContext::default(),
                &mut ledger,
            )
            .is_err()
        );
        assert!(
            ledger.finish().is_err(),
            "failed lowering must not consume binding"
        );
    }

    #[test]
    fn producer_matches_and_references_once_lowered_raw_build_expression() {
        let left = one_col_values_node_typed(10, 1, "lhs", 10, DataType::Int64);
        let right = one_col_values_node_typed(11, 2, "rhs", 20, DataType::Int32);
        let mut wire = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: i32::from(plan::JoinKind::Inner),
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int32)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: i32::from(plan::JoinDistribution::Broadcast),
                execution_mode: None,
            }),
            Vec::new(),
            vec![left, right],
        );
        wire.runtime_filter_binding_ids = vec![1];
        let table = plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![membership_producer_wire(
                1,
                30,
                column_ref(2, DataType::Int32),
                &DataType::Int32,
            )],
        };
        let mut ledger = RuntimeFilterBindingLookupLedger::decode(1, Some(&table))
            .expect("decode producer table");
        let mut arena = ExprArena::default();
        let lowered = lower_proto_node_with_bindings(
            &wire,
            &mut arena,
            &NodeLoweringContext::default(),
            &mut ledger,
        )
        .expect("coerced producer seam");
        ledger.finish().expect("producer binding consumed");
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("producer seam")
        };
        let JoinRuntimeFilterExecution::Native { producers } = join.runtime_filter_execution else {
            panic!("native producer execution")
        };
        let build_expr_id = producers[0].build_expr_id;
        assert_eq!(arena.data_type(build_expr_id), Some(&DataType::Int32));
        assert!(matches!(
            arena.node(build_expr_id),
            Some(crate::exec::expr::ExprNode::SlotId(_))
        ));
    }

    #[test]
    fn distinct_producer_bindings_may_share_one_unique_raw_build_key() {
        let left = one_col_values_node_with(10, 1, "lhs", 10);
        let right = one_col_values_node_with(11, 2, "rhs", 20);
        let mut wire = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: i32::from(plan::JoinKind::Inner),
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: i32::from(plan::JoinDistribution::Broadcast),
                execution_mode: None,
            }),
            Vec::new(),
            vec![left, right],
        );
        wire.runtime_filter_binding_ids = vec![1, 2];
        let table = plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![
                membership_producer_wire(1, 30, column_ref(2, DataType::Int64), &DataType::Int64),
                membership_producer_wire(2, 30, column_ref(2, DataType::Int64), &DataType::Int64),
            ],
        };
        let mut ledger = RuntimeFilterBindingLookupLedger::decode(1, Some(&table))
            .expect("decode shared-key producer table");
        let lowered = lower_proto_node_with_bindings(
            &wire,
            &mut ExprArena::default(),
            &NodeLoweringContext::default(),
            &mut ledger,
        )
        .expect("two channels may bind the same unique raw key");
        ledger.finish().expect("both producer bindings consumed");
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("producer seam")
        };
        let JoinRuntimeFilterExecution::Native { producers } = join.runtime_filter_execution else {
            panic!("native producer execution")
        };
        assert_eq!(
            producers
                .iter()
                .map(|binding| binding.binding_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn duplicate_raw_build_keys_are_disambiguated_by_join_key_ordinal() {
        let left = one_col_values_node_with(10, 1, "lhs", 10);
        let right = one_col_values_node_with(11, 2, "rhs", 20);
        let condition = plan::HashJoinEqCondition {
            left: Some(column_ref(1, DataType::Int64)),
            right: Some(column_ref(2, DataType::Int64)),
            null_safe: false,
        };
        let mut wire = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: i32::from(plan::JoinKind::Inner),
                eq_conditions: vec![condition.clone(), condition],
                other_condition: None,
                distribution: i32::from(plan::JoinDistribution::Broadcast),
                execution_mode: None,
            }),
            Vec::new(),
            vec![left, right],
        );
        wire.runtime_filter_binding_ids = vec![1, 2];
        let table = plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![
                membership_producer_wire_at(
                    1,
                    30,
                    column_ref(2, DataType::Int64),
                    &DataType::Int64,
                    0,
                ),
                membership_producer_wire_at(
                    2,
                    30,
                    column_ref(2, DataType::Int64),
                    &DataType::Int64,
                    1,
                ),
            ],
        };
        let mut ledger = RuntimeFilterBindingLookupLedger::decode(1, Some(&table))
            .expect("decode duplicate-key producer table");
        let lowered = lower_proto_node_with_bindings(
            &wire,
            &mut ExprArena::default(),
            &NodeLoweringContext::default(),
            &mut ledger,
        )
        .expect("exact ordinals disambiguate duplicate raw build keys");
        ledger.finish().expect("both producer bindings consumed");
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("producer seam")
        };
        let JoinRuntimeFilterExecution::Native { producers } = join.runtime_filter_execution else {
            panic!("native producer execution")
        };
        assert_eq!(
            producers
                .iter()
                .map(|binding| binding.build_key_index)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[test]
    fn producer_rejects_nested_intermediate_nullability_mismatch() {
        let raw_build = cast_expr(
            cast_expr(column_ref(2, DataType::Int64), DataType::Int64, true),
            DataType::Int64,
            true,
        );
        let left = one_col_values_node_with(10, 1, "lhs", 10);
        let right = one_col_values_node_with(11, 2, "rhs", 20);
        let mut wire = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: i32::from(plan::JoinKind::Inner),
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(raw_build.clone()),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: i32::from(plan::JoinDistribution::Broadcast),
                execution_mode: None,
            }),
            Vec::new(),
            vec![left, right],
        );
        wire.runtime_filter_binding_ids = vec![1];
        let mut mismatched = raw_build;
        let Some(expr::expr::Kind::Cast(outer)) = mismatched.kind.as_mut() else {
            panic!("outer cast")
        };
        let inner = outer.operand.as_mut().expect("inner cast");
        inner.nullable = false;
        let table = plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![membership_producer_wire(
                1,
                30,
                mismatched,
                &DataType::Int64,
            )],
        };
        let mut ledger = RuntimeFilterBindingLookupLedger::decode(1, Some(&table))
            .expect("decode nested mismatch table");
        assert!(
            lower_proto_node_with_bindings(
                &wire,
                &mut ExprArena::default(),
                &NodeLoweringContext::default(),
                &mut ledger,
            )
            .is_err(),
            "nested intermediate nullability drift must not match"
        );
        assert!(
            ledger.finish().is_err(),
            "failed match must remain unconsumed"
        );
    }

    #[test]
    fn scan_binding_uses_leaf_local_native_consumer_spec() {
        let wire = physical_node(
            10,
            plan::plan_node::Kind::Scan(plan::ScanNode::default()),
            vec![output_column(1, "v", DataType::Int64)],
            Vec::new(),
        );
        let baseline = lower(&one_col_values_node(10));
        let mut lowered = LoweredNode {
            node: ExecNode {
                kind: ExecNodeKind::Scan(crate::exec::node::scan::ScanNode::new(Arc::new(
                    DummyScanOp,
                ))),
            },
            layout: baseline.layout,
            output_schema: baseline.output_schema,
        };
        attach_leaf_consumers(
            &wire,
            &[dormant_source_consumer(1, 10, 1)],
            &mut lowered,
            &mut ExprArena::default(),
        )
        .expect("scan leaf binding");
        let ExecNodeKind::Scan(scan) = &lowered.node.kind else {
            panic!("scan")
        };
        assert_eq!(scan.native_runtime_filter_specs().len(), 1);
        assert!(scan.runtime_filter_specs().is_empty());
    }

    #[test]
    fn exchange_binding_uses_leaf_local_native_consumer_spec() {
        let baseline = lower(&one_col_values_node(10));
        let mut wire = physical_node(
            10,
            plan::plan_node::Kind::Values(plan::ValuesNode::default()),
            Vec::new(),
            Vec::new(),
        );
        wire.payload = Some(plan::distributed_node::Payload::Exchange(
            plan::ExchangeReceiver::default(),
        ));
        let mut lowered = LoweredNode {
            node: ExecNode {
                kind: ExecNodeKind::ExchangeSource(
                    crate::exec::node::exchange_source::ExchangeSourceNode::new(
                        crate::runtime::exchange::ExchangeKey {
                            finst_id_hi: 1,
                            finst_id_lo: 2,
                            node_id: 3,
                        },
                        1,
                        std::time::Duration::from_secs(1),
                        Arc::clone(&baseline.output_schema),
                    ),
                ),
            },
            layout: baseline.layout,
            output_schema: baseline.output_schema,
        };
        attach_leaf_consumers(
            &wire,
            &[dormant_source_consumer(1, 10, 1)],
            &mut lowered,
            &mut ExprArena::default(),
        )
        .expect("exchange leaf binding");
        let ExecNodeKind::ExchangeSource(exchange) = &lowered.node.kind else {
            panic!("exchange")
        };
        assert_eq!(exchange.native_runtime_filter_specs().len(), 1);
        assert!(exchange.runtime_filter_specs().is_empty());
    }

    #[test]
    fn unary_node_wraps_only_its_direct_input() {
        let mut children = vec![lower(&one_col_values_node(10))];
        let mut arena = ExprArena::default();
        attach_direct_input_consumers(20, &[dormant_consumer(1, 20, 1)], &mut children, &mut arena)
            .expect("attach");
        let ExecNodeKind::NativeRuntimeFilterConsumer(consumer) = &children[0].node.kind else {
            panic!("consumer wrapper")
        };
        assert_eq!(consumer.owner_node_id, 20);
        assert!(matches!(consumer.input.kind, ExecNodeKind::Values(_)));
    }

    #[test]
    fn multi_input_requires_exactly_one_matching_direct_input() {
        let mut children = vec![
            lower(&one_col_values_node_with(10, 1, "left", 1)),
            lower(&one_col_values_node_with(11, 2, "right", 2)),
        ];
        let mut arena = ExprArena::default();
        attach_direct_input_consumers(20, &[dormant_consumer(1, 20, 1)], &mut children, &mut arena)
            .expect("unique left input");
        assert!(matches!(
            children[0].node.kind,
            ExecNodeKind::NativeRuntimeFilterConsumer(_)
        ));
        assert!(matches!(children[1].node.kind, ExecNodeKind::Values(_)));

        let mut missing_input = dormant_consumer(2, 20, 1);
        let DecodedBindingRole::Consumer { target, .. } = &mut missing_input.role else {
            unreachable!("consumer")
        };
        *target = DecodedConsumerBindingTarget::DirectInput { input_ordinal: 2 };
        let mut children = vec![
            lower(&one_col_values_node(10)),
            lower(&one_col_values_node(11)),
        ];
        assert!(
            attach_direct_input_consumers(
                20,
                &[missing_input],
                &mut children,
                &mut ExprArena::default()
            )
            .is_err()
        );

        let mut children = vec![
            lower(&one_col_values_node_with(10, 1, "left", 1)),
            lower(&one_col_values_node_with(11, 2, "right", 2)),
        ];
        assert!(
            attach_direct_input_consumers(
                20,
                &[dormant_consumer(3, 20, 3)],
                &mut children,
                &mut ExprArena::default(),
            )
            .is_err()
        );
    }

    #[test]
    fn filter_binding_does_not_move_to_scan_without_scan_binding() {
        let baseline = lower(&one_col_values_node(10));
        let mut children = vec![LoweredNode {
            node: ExecNode {
                kind: ExecNodeKind::Scan(crate::exec::node::scan::ScanNode::new(Arc::new(
                    DummyScanOp,
                ))),
            },
            layout: baseline.layout,
            output_schema: baseline.output_schema,
        }];
        attach_direct_input_consumers(
            20,
            &[dormant_consumer(1, 20, 1)],
            &mut children,
            &mut ExprArena::default(),
        )
        .expect("filter input boundary");
        let ExecNodeKind::NativeRuntimeFilterConsumer(consumer) = &children[0].node.kind else {
            panic!("exact filter input wrapper")
        };
        let ExecNodeKind::Scan(scan) = &consumer.input.kind else {
            panic!("scan remains the direct input")
        };
        assert!(scan.native_runtime_filter_specs().is_empty());
        assert!(scan.runtime_filter_specs().is_empty());
    }

    #[test]
    fn values_binding_wraps_the_source_boundary() {
        let wire = one_col_values_node(10);
        let mut lowered = lower(&wire);
        attach_leaf_consumers(
            &wire,
            &[dormant_source_consumer(1, 10, 1)],
            &mut lowered,
            &mut ExprArena::default(),
        )
        .expect("values source boundary");
        let ExecNodeKind::NativeRuntimeFilterConsumer(consumer) = lowered.node.kind else {
            panic!("consumer")
        };
        assert!(matches!(consumer.input.kind, ExecNodeKind::Values(_)));
    }

    #[test]
    fn generate_series_binding_wraps_the_source_boundary() {
        let wire = physical_node(
            10,
            plan::plan_node::Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: 1,
                end: 3,
                step: 1,
                output_column_id: 1,
                column_name: "v".to_string(),
                alias: None,
            }),
            vec![output_column_with_nullable(1, "v", DataType::Int64, false)],
            Vec::new(),
        );
        let mut lowered = lower(&wire);
        let mut binding = dormant_source_consumer(1, 10, 1);
        binding.expression.nullable = false;
        attach_leaf_consumers(&wire, &[binding], &mut lowered, &mut ExprArena::default())
            .expect("generate series source boundary");
        let ExecNodeKind::NativeRuntimeFilterConsumer(consumer) = lowered.node.kind else {
            panic!("consumer")
        };
        assert!(matches!(
            consumer.input.kind,
            ExecNodeKind::TableFunction(_)
        ));
    }

    #[test]
    fn unsupported_leaf_capability_fails_before_execution() {
        let wire = physical_node(
            10,
            plan::plan_node::Kind::Decode(plan::DecodeNode::default()),
            Vec::new(),
            Vec::new(),
        );
        let mut lowered = lower(&one_col_values_node(10));
        assert!(
            attach_leaf_consumers(
                &wire,
                &[dormant_source_consumer(1, 10, 1)],
                &mut lowered,
                &mut ExprArena::default(),
            )
            .is_err()
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
        let err = lower_proto_node(&scan, &mut arena, &NodeLoweringContext::default()).unwrap_err();
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
        let err = lower_proto_node(&union_distinct, &mut arena, &NodeLoweringContext::default())
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
                    branch_kind: plan::ChangeStreamBranchKind::DeleteDv as i32,
                    assignments: vec![plan::DistributedChangeEventOutputExpr {
                        output_column_id: 2,
                        expr: None,
                    }],
                }],
                output_columns: vec![
                    output_column(1, "id", DataType::Int64),
                    output_column(2, "op", DataType::Int8),
                ],
                change_op_column_id: 2,
                data_route_column_id: None,
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
        assert_eq!(change_event.change_op_slot_id, SlotId::new(2));
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
