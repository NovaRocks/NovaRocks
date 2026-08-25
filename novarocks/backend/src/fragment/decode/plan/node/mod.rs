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

//! Fragment-native protocol plan-node decoding.

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

use self::common::*;
use novarocks_execution::runtime_filter as execution;
use std::collections::BTreeMap;

use crate::fragment::decode::plan::context::NativePlanDecodeContext;
use crate::fragment::decode::plan::error::NativeFragmentDecodeError;
use crate::fragment::decode::plan::layout::Layout;
use crate::fragment::decode::plan::runtime_filter_binding::{
    DecodedBindingRole, DecodedConsumerBindingTarget, DecodedRuntimeFilterBinding,
    NativeRuntimeFilterDecodeLedger, ProducerBindingTarget,
};
use novarocks_execution::exec::chunk::ChunkSchemaRef;
use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::fragment::program::{FragmentNodeId, ScanAssignmentKind};
use novarocks_execution::exec::node::aggregate::{
    AggregateRuntimeFilterSpec, AggregateTopNRuntimeFilterProducerBinding,
};
use novarocks_execution::exec::node::join::{
    JoinRuntimeFilterExecution, JoinRuntimeFilterProducerBinding,
};
use novarocks_execution::exec::node::limit::LimitNode;
use novarocks_execution::exec::node::runtime_filter::{
    RuntimeFilterConsumerBinding, RuntimeFilterConsumerNode,
};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto::FieldPath;
use novarocks_proto::plan;

#[derive(Clone, Debug)]
pub(crate) struct DecodedNode {
    pub(crate) node: ExecNode,
    pub(crate) layout: Layout,
    pub(crate) output_schema: ChunkSchemaRef,
}
#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
pub(super) fn collect_scan_assignment_kinds(
    root: &plan::DistributedNode,
    root_path: FieldPath,
) -> Result<BTreeMap<FragmentNodeId, ScanAssignmentKind>, NativeFragmentDecodeError> {
    fn visit(
        node: &plan::DistributedNode,
        path: FieldPath,
        assignments: &mut BTreeMap<FragmentNodeId, ScanAssignmentKind>,
    ) -> Result<(), NativeFragmentDecodeError> {
        if let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_ref()
            && let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref()
        {
            let scan_path = path
                .clone()
                .field("payload")
                .field("physical")
                .field("scan");
            let table = scan.table.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    scan_path.clone().field("table"),
                    format!("native ScanNode node_id={} requires table", node.node_id),
                )
            })?;
            let source = table.source.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    scan_path.clone().field("table").field("source"),
                    format!("native ScanNode node_id={} requires source", node.node_id),
                )
            })?;
            let source = source.kind.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
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
                return Err(NativeFragmentDecodeError::inconsistent(
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
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    decode_node_inner(
        node,
        FieldPath::root("plan_fragment").field("root"),
        arena,
        ctx,
        None,
    )
}

pub(crate) fn decode_node_with_runtime_filters(
    node: &plan::DistributedNode,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
    ledger: &mut NativeRuntimeFilterDecodeLedger,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    decode_node_inner(
        node,
        FieldPath::root("plan_fragment").field("root"),
        arena,
        ctx,
        Some(ledger),
    )
}

fn decode_node_inner(
    node: &plan::DistributedNode,
    path: FieldPath,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
    mut ledger: Option<&mut NativeRuntimeFilterDecodeLedger>,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    validate_distributed_node_children(node, path.clone())?;
    let mut children = Vec::with_capacity(node.children.len());
    for (index, child) in node.children.iter().enumerate() {
        children.push(decode_node_inner(
            child,
            path.clone().field("children").index(index),
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
        .transpose()
        .map_err(|error| error.into_native(path.clone()))?
        .unwrap_or_default();
    let direct_inputs = children
        .iter()
        .map(|child| (child.layout.clone(), child.output_schema.clone()))
        .collect::<Vec<_>>();
    let (consumer_bindings, producer_bindings): (Vec<_>, Vec<_>) = attached
        .into_iter()
        .partition(|binding| matches!(binding.role, DecodedBindingRole::Consumer { .. }));
    if !children.is_empty() {
        attach_direct_input_consumers(
            node.node_id,
            &consumer_bindings,
            &mut children,
            arena,
            path.clone().field("runtime_filter_binding_ids"),
            ctx,
        )?;
    }

    let payload = node.payload.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
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
    if children_are_absent(node) && !consumer_bindings.is_empty() {
        attach_leaf_consumers(
            node,
            &consumer_bindings,
            &mut lowered,
            arena,
            path.clone(),
            ctx,
        )?;
    }
    let mut lowered = apply_distributed_limit_if_needed(node, lowered, path.clone())?;
    if !producer_bindings.is_empty() {
        attach_producers(
            node,
            &producer_bindings,
            &direct_inputs,
            &mut lowered,
            arena,
            path.clone(),
        )?;
    }
    if let Some(ledger) = ledger {
        ledger
            .commit_consumed_many(&node.runtime_filter_binding_ids)
            .map_err(|error| error.into_native(path))?;
    }
    Ok(lowered)
}

fn validate_distributed_node_children(
    node: &plan::DistributedNode,
    node_path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
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

fn children_are_absent(node: &plan::DistributedNode) -> bool {
    node.children.is_empty()
}

fn attach_direct_input_consumers(
    owner_node_id: i32,
    bindings: &[DecodedRuntimeFilterBinding],
    children: &mut [DecodedNode],
    arena: &mut ExprArena,
    path: FieldPath,
    ctx: &NativePlanDecodeContext,
) -> Result<(), NativeFragmentDecodeError> {
    let mut grouped = BTreeMap::<usize, Vec<RuntimeFilterConsumerBinding>>::new();
    for binding in bindings {
        let DecodedBindingRole::Consumer { target, .. } = &binding.role else {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone(),
                format!(
                    "native runtime-filter binding_id={} expected consumer role",
                    binding.binding_id
                ),
            ));
        };
        let DecodedConsumerBindingTarget::DirectInput {
            input_ordinal: index,
        } = target
        else {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone(),
                format!(
                    "native runtime-filter consumer binding_id={} on node_id={owner_node_id} must target a direct input",
                    binding.binding_id
                ),
            ));
        };
        let child = children.get(*index).ok_or_else(|| {
            NativeFragmentDecodeError::inconsistent(
                path.clone(),
                format!(
                    "native runtime-filter consumer binding_id={} on node_id={owner_node_id} targets missing direct input ordinal={index}, input_count={}",
                    binding.binding_id,
                    children.len()
                ),
            )
        })?;
        let expr_id =
            lower_binding_expression(binding, &child.layout, &child.output_schema, arena, ctx)?;
        grouped
            .entry(*index)
            .or_default()
            .push(NativeFragmentDecodeError::map_invalid(
                path.clone(),
                consumer_spec(binding, expr_id),
            )?);
    }
    for (index, specs) in grouped {
        let child = &mut children[index];
        let input = child.node.clone();
        child.node = ExecNode {
            kind: ExecNodeKind::RuntimeFilterConsumer(RuntimeFilterConsumerNode::new(
                input,
                owner_node_id,
                specs,
            )),
        };
    }
    Ok(())
}

fn attach_leaf_consumers(
    wire_node: &plan::DistributedNode,
    bindings: &[DecodedRuntimeFilterBinding],
    lowered: &mut DecodedNode,
    arena: &mut ExprArena,
    path: FieldPath,
    ctx: &NativePlanDecodeContext,
) -> Result<(), NativeFragmentDecodeError> {
    for binding in bindings {
        let DecodedBindingRole::Consumer { target, .. } = &binding.role else {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("runtime_filter_binding_ids"),
                format!(
                    "native runtime-filter binding_id={} expected consumer role",
                    binding.binding_id
                ),
            ));
        };
        let DecodedConsumerBindingTarget::SourceBoundary { scan_domain } = target else {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("runtime_filter_binding_ids"),
                format!(
                    "native runtime-filter consumer binding_id={} on leaf node_id={} must target source boundary",
                    binding.binding_id, wire_node.node_id
                ),
            ));
        };
        if scan_domain.is_some()
            && !matches!(
                wire_node.payload.as_ref(),
                Some(plan::distributed_node::Payload::Physical(physical))
                    if matches!(physical.kind.as_ref(), Some(plan::plan_node::Kind::Scan(_)))
            )
        {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("runtime_filter_binding_ids"),
                format!(
                    "native runtime-filter binding_id={} scan-domain target must attach to a connector scan leaf",
                    binding.binding_id
                ),
            ));
        }
        if let Some(target) = scan_domain {
            validate_scan_domain_target(binding, target, path.clone())?;
        }
    }
    let specs = bindings
        .iter()
        .map(|binding| {
            let expr_id = lower_binding_expression(
                binding,
                &lowered.layout,
                &lowered.output_schema,
                arena,
                ctx,
            )?;
            consumer_spec(binding, expr_id).map_err(|error| {
                NativeFragmentDecodeError::inconsistent(
                    path.clone().field("runtime_filter_binding_ids"),
                    error,
                )
            })
        })
        .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
    let payload = wire_node.payload.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("payload"),
            format!("native node_id={} payload missing", wire_node.node_id),
        )
    })?;
    match payload {
        plan::distributed_node::Payload::Exchange(_) => {
            let exchange = find_exchange_source_mut(&mut lowered.node).ok_or_else(|| {
                NativeFragmentDecodeError::inconsistent(
                    path.clone().field("payload").field("exchange"),
                    format!(
                        "native node_id={} exchange lowering lost ExchangeSource boundary",
                        wire_node.node_id
                    ),
                )
            })?;
            *exchange = exchange.clone().with_runtime_filter_consumers(specs);
        }
        plan::distributed_node::Payload::Physical(physical) => match physical.kind.as_ref() {
            Some(plan::plan_node::Kind::Scan(_)) => {
                set_native_scan_specs(&mut lowered.node, specs).map_err(|_| {
                    NativeFragmentDecodeError::inconsistent(
                        path.clone()
                            .field("payload")
                            .field("physical")
                            .field("scan"),
                        format!(
                            "native node_id={} scan lowering lost Scan boundary",
                            wire_node.node_id
                        ),
                    )
                })?;
            }
            Some(plan::plan_node::Kind::Values(_))
            | Some(plan::plan_node::Kind::GenerateSeries(_)) => {
                wrap_source_boundary(&mut lowered.node, wire_node.node_id, specs);
            }
            kind => {
                return Err(NativeFragmentDecodeError::unsupported(
                    path.field("runtime_filter_binding_ids"),
                    format!(
                        "native runtime-filter consumer binding on leaf node_id={} has unsupported source capability: {kind:?}",
                        wire_node.node_id
                    ),
                ));
            }
        },
    }
    Ok(())
}

fn validate_scan_domain_target(
    binding: &DecodedRuntimeFilterBinding,
    target: &crate::fragment::decode::plan::runtime_filter_binding::DecodedRuntimeFilterScanDomainTarget,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    use novarocks_proto::expr::expr::Kind;

    if !matches!(&binding.expression.kind, Some(Kind::ColumnRef(_))) {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.field("runtime_filter_binding_ids"),
            format!(
                "native runtime-filter binding_id={} scan-domain target requires an exact ColumnRef consumer expression",
                binding.binding_id
            ),
        ));
    }
    let expression_type = binding.expression.r#type.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            binding.expression_path.clone().field("type"),
            format!(
                "native runtime-filter binding_id={} scan-domain consumer expression is missing type",
                binding.binding_id
            ),
        )
    })?;
    let expression_type = crate::fragment::decode::type_decode::decode_type(expression_type).map_err(|error| {
        NativeFragmentDecodeError::invalid_value(
            binding.expression_path.clone().field("type"),
            format!(
                "native runtime-filter binding_id={} scan-domain consumer expression has invalid type: {error}",
                binding.binding_id
            ),
        )
    })?;
    if expression_type != target.data_type || binding.expression.nullable != target.nullable {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.field("runtime_filter_binding_ids"),
            format!(
                "native runtime-filter binding_id={} scan-domain target type/nullability does not match consumer expression",
                binding.binding_id
            ),
        ));
    }
    Ok(())
}

fn wrap_source_boundary(
    node: &mut ExecNode,
    owner_node_id: i32,
    bindings: Vec<RuntimeFilterConsumerBinding>,
) {
    let input = node.clone();
    *node = ExecNode {
        kind: ExecNodeKind::RuntimeFilterConsumer(RuntimeFilterConsumerNode::new(
            input,
            owner_node_id,
            bindings,
        )),
    };
}

fn set_native_scan_specs(
    node: &mut ExecNode,
    specs: Vec<RuntimeFilterConsumerBinding>,
) -> Result<(), Vec<RuntimeFilterConsumerBinding>> {
    match &mut node.kind {
        ExecNodeKind::Scan(scan) => {
            *scan = scan.clone().with_runtime_filter_consumers(specs);
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
) -> Option<&mut novarocks_execution::exec::node::exchange_source::ExchangeSourceNode> {
    match &mut node.kind {
        ExecNodeKind::ExchangeSource(exchange) => Some(exchange),
        ExecNodeKind::Limit(limit) => find_exchange_source_mut(&mut limit.input),
        ExecNodeKind::Sort(sort) => find_exchange_source_mut(&mut sort.input),
        _ => None,
    }
}

fn attach_producers(
    wire_node: &plan::DistributedNode,
    bindings: &[DecodedRuntimeFilterBinding],
    direct_inputs: &[(Layout, ChunkSchemaRef)],
    lowered: &mut DecodedNode,
    arena: &mut ExprArena,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    let payload = wire_node.payload.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("payload"),
            format!(
                "native runtime-filter producer node_id={} payload missing",
                wire_node.node_id
            ),
        )
    })?;
    let plan::distributed_node::Payload::Physical(physical) = payload else {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("runtime_filter_binding_ids"),
            format!(
                "native runtime-filter producer node_id={} must target a physical HashJoin or HashAggregate",
                wire_node.node_id
            ),
        ));
    };
    match physical.kind.as_ref() {
        Some(plan::plan_node::Kind::HashJoin(wire_join)) => {
            attach_hash_join_producers(wire_node, wire_join, bindings, direct_inputs, lowered, path)
        }
        Some(plan::plan_node::Kind::HashAggregate(wire_aggregate)) => {
            attach_hash_aggregate_producers(
                wire_node,
                wire_aggregate,
                bindings,
                direct_inputs,
                lowered,
                arena,
                path,
            )
        }
        kind => Err(NativeFragmentDecodeError::inconsistent(
            path.field("runtime_filter_binding_ids"),
            format!(
                "native runtime-filter producer node_id={} must target a physical HashJoin or HashAggregate, got {kind:?}",
                wire_node.node_id
            ),
        )),
    }
}

fn attach_hash_join_producers(
    wire_node: &plan::DistributedNode,
    wire_join: &plan::HashJoinNode,
    bindings: &[DecodedRuntimeFilterBinding],
    direct_inputs: &[(Layout, ChunkSchemaRef)],
    lowered: &mut DecodedNode,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    let binding_path = path.clone().field("runtime_filter_binding_ids");
    let join = find_hash_join_mut(&mut lowered.node, wire_node.node_id).ok_or_else(|| {
        NativeFragmentDecodeError::inconsistent(
            binding_path.clone(),
            format!(
                "native runtime-filter producer binding is only supported on HashJoin, node_id={}",
                wire_node.node_id
            ),
        )
    })?;
    if direct_inputs.len() != 2 {
        return Err(NativeFragmentDecodeError::inconsistent(
            binding_path,
            format!(
                "native HashJoin node_id={} missing two direct inputs",
                wire_node.node_id
            ),
        ));
    }
    let build_input_index =
        if join.join_type == novarocks_execution::exec::node::join::JoinType::RightSemi {
            0
        } else {
            1
        };
    let (build_layout, build_schema) = &direct_inputs[build_input_index];
    let mut producers = Vec::with_capacity(bindings.len());
    for binding in bindings {
        let DecodedBindingRole::Producer { contract, target } = &binding.role else {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("runtime_filter_binding_ids"),
                format!(
                    "native runtime-filter binding_id={} expected producer role",
                    binding.binding_id
                ),
            ));
        };
        let ProducerBindingTarget::JoinBuildKey {
            ordinal: build_key_index,
        } = *target
        else {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("runtime_filter_binding_ids"),
                format!(
                    "native runtime-filter producer binding_id={} on HashJoin must target a join build key",
                    binding.binding_id
                ),
            ));
        };
        let join_key_path = path
            .clone()
            .field("payload")
            .field("physical")
            .field("hash_join")
            .field("eq_conditions")
            .index(build_key_index);
        let condition = wire_join.eq_conditions.get(build_key_index).ok_or_else(|| {
            NativeFragmentDecodeError::inconsistent(
                join_key_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} targets missing join key ordinal={build_key_index}, key_count={}",
                    binding.binding_id,
                    wire_join.eq_conditions.len()
                ),
            )
        })?;
        if condition.null_safe {
            return Err(NativeFragmentDecodeError::inconsistent(
                join_key_path.clone().field("null_safe"),
                format!(
                    "native runtime-filter producer binding_id={} targets null-safe join key ordinal={build_key_index}",
                    binding.binding_id
                ),
            ));
        }
        let (raw_build, raw_build_path) =
            if join.join_type == novarocks_execution::exec::node::join::JoinType::RightSemi {
                (condition.left.as_ref(), join_key_path.clone().field("left"))
            } else {
                (
                    condition.right.as_ref(),
                    join_key_path.clone().field("right"),
                )
            };
        let raw_build = raw_build.ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                raw_build_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} join key ordinal={build_key_index} missing build expression",
                    binding.binding_id
                ),
            )
        })?;
        if raw_build != &binding.expression {
            return Err(NativeFragmentDecodeError::inconsistent(
                raw_build_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} expression does not match join key ordinal={build_key_index}",
                    binding.binding_id
                ),
            ));
        }
        validate_column_refs_exact(
            binding.binding_id,
            raw_build,
            build_layout,
            build_schema,
            raw_build_path,
        )?;
        let build_expr_id = *join.build_keys.get(build_key_index).ok_or_else(|| {
            NativeFragmentDecodeError::inconsistent(
                join_key_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} lowered join key ordinal={build_key_index} is missing",
                    binding.binding_id
                ),
            )
        })?;
        producers.push(JoinRuntimeFilterProducerBinding::new(
            build_expr_id,
            build_key_index,
            contract.clone(),
        ));
    }
    join.runtime_filter_execution = JoinRuntimeFilterExecution::new(producers);
    Ok(())
}

fn attach_hash_aggregate_producers(
    wire_node: &plan::DistributedNode,
    wire_aggregate: &plan::HashAggregateNode,
    bindings: &[DecodedRuntimeFilterBinding],
    direct_inputs: &[(Layout, ChunkSchemaRef)],
    lowered: &mut DecodedNode,
    arena: &ExprArena,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    let binding_path = path.clone().field("runtime_filter_binding_ids");
    let [(input_layout, input_schema)] = direct_inputs else {
        return Err(NativeFragmentDecodeError::inconsistent(
            binding_path,
            format!(
                "native HashAggregate node_id={} missing one direct input",
                wire_node.node_id
            ),
        ));
    };
    let aggregate =
        find_hash_aggregate_mut(&mut lowered.node, wire_node.node_id).ok_or_else(|| {
            NativeFragmentDecodeError::inconsistent(
                path.clone()
                    .field("payload")
                    .field("physical")
                    .field("hash_aggregate"),
                format!(
                    "native HashAggregate node_id={} lowering lost its physical aggregate owner",
                    wire_node.node_id
                ),
            )
        })?;
    let mut seen = std::collections::BTreeSet::new();
    let mut producers = Vec::with_capacity(bindings.len());
    for binding in bindings {
        let DecodedBindingRole::Producer { contract, target } = &binding.role else {
            return Err(NativeFragmentDecodeError::inconsistent(
                binding_path.clone(),
                format!(
                    "native runtime-filter binding_id={} expected producer role",
                    binding.binding_id
                ),
            ));
        };
        let ProducerBindingTarget::AggregateTopNKey {
            ordinal: group_key_ordinal,
            limit,
        } = *target
        else {
            return Err(NativeFragmentDecodeError::inconsistent(
                binding_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} on HashAggregate must target an aggregate TopN key",
                    binding.binding_id
                ),
            ));
        };
        if !seen.insert((binding.binding_id, group_key_ordinal)) {
            return Err(NativeFragmentDecodeError::inconsistent(
                binding_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} duplicates aggregate TopN group key ordinal={group_key_ordinal}",
                    binding.binding_id
                ),
            ));
        }
        let group_key_path = path
            .clone()
            .field("payload")
            .field("physical")
            .field("hash_aggregate")
            .field("group_by")
            .index(group_key_ordinal);
        let raw_group_key = wire_aggregate
            .group_by
            .get(group_key_ordinal)
            .ok_or_else(|| {
                NativeFragmentDecodeError::inconsistent(
                    group_key_path.clone(),
                    format!(
                        "native runtime-filter producer binding_id={} targets missing aggregate group key ordinal={group_key_ordinal}, key_count={}",
                        binding.binding_id,
                        wire_aggregate.group_by.len()
                    ),
                )
            })?;
        if raw_group_key != &binding.expression {
            return Err(NativeFragmentDecodeError::inconsistent(
                group_key_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} expression does not match aggregate group key ordinal={group_key_ordinal}",
                    binding.binding_id
                ),
            ));
        }
        validate_column_refs_exact(
            binding.binding_id,
            raw_group_key,
            input_layout,
            input_schema,
            group_key_path.clone(),
        )?;
        let group_key_expr_id =
            *aggregate
                .group_by
                .get(group_key_ordinal)
                .ok_or_else(|| {
                    NativeFragmentDecodeError::inconsistent(
                        group_key_path.clone(),
                        format!(
                            "native runtime-filter producer binding_id={} lowered aggregate group key ordinal={group_key_ordinal} is missing",
                            binding.binding_id
                        ),
                    )
                })?;
        let group_key_type = arena.data_type(group_key_expr_id).ok_or_else(|| {
            NativeFragmentDecodeError::inconsistent(
                group_key_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} aggregate group key ordinal={group_key_ordinal} has no lowered type",
                    binding.binding_id
                ),
            )
        })?;
        let execution::RuntimeFilterExecutionContract::Ordered(order) = contract.contract() else {
            return Err(NativeFragmentDecodeError::inconsistent(
                binding_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} aggregate TopN requires an ordered contract",
                    binding.binding_id
                ),
            ));
        };
        if order.keys().len() != 1 || order.keys()[0].data_type() != group_key_type {
            return Err(NativeFragmentDecodeError::inconsistent(
                binding_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} aggregate TopN ordered contract must contain exactly one key matching group key ordinal={group_key_ordinal} type={group_key_type:?}",
                    binding.binding_id
                ),
            ));
        }
        if contract.kind() != execution::RuntimeFilterProducerKind::OrderedBound {
            return Err(NativeFragmentDecodeError::inconsistent(
                binding_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} aggregate TopN requires TightenOrderedBound reduction",
                    binding.binding_id
                ),
            ));
        }
        producers.push(AggregateTopNRuntimeFilterProducerBinding::new(
            group_key_expr_id,
            group_key_ordinal,
            limit,
            contract.clone(),
        ));
    }
    aggregate.runtime_filter_spec = NativeFragmentDecodeError::map_invalid(
        binding_path,
        AggregateRuntimeFilterSpec::try_new(producers),
    )?;
    Ok(())
}

fn find_hash_aggregate_mut(
    node: &mut ExecNode,
    node_id: i32,
) -> Option<&mut novarocks_execution::exec::node::aggregate::AggregateNode> {
    match &mut node.kind {
        ExecNodeKind::Aggregate(aggregate) if aggregate.node_id == node_id => Some(aggregate),
        ExecNodeKind::Limit(limit) if limit.node_id == node_id => {
            find_hash_aggregate_mut(&mut limit.input, node_id)
        }
        ExecNodeKind::Project(project) if project.is_subordinate && project.node_id == node_id => {
            find_hash_aggregate_mut(&mut project.input, node_id)
        }
        _ => None,
    }
}

fn find_hash_join_mut(
    node: &mut ExecNode,
    node_id: i32,
) -> Option<&mut novarocks_execution::exec::node::join::JoinNode> {
    match &mut node.kind {
        ExecNodeKind::Join(join) if join.node_id == node_id => Some(join),
        ExecNodeKind::Limit(limit) if limit.node_id == node_id => {
            find_hash_join_mut(&mut limit.input, node_id)
        }
        ExecNodeKind::Project(project) if project.is_subordinate && project.node_id == node_id => {
            find_hash_join_mut(&mut project.input, node_id)
        }
        _ => None,
    }
}

fn consumer_spec(
    binding: &DecodedRuntimeFilterBinding,
    expr_id: novarocks_execution::exec::expr::ExprId,
) -> Result<RuntimeFilterConsumerBinding, String> {
    let DecodedBindingRole::Consumer { contract, target } = &binding.role else {
        return Err(format!(
            "native runtime-filter binding_id={} expected consumer role",
            binding.binding_id
        ));
    };
    let scan_domain = match target {
        DecodedConsumerBindingTarget::DirectInput { .. } => None,
        DecodedConsumerBindingTarget::SourceBoundary { scan_domain } => {
            scan_domain.as_ref().map(|target| {
                execution::scan_domain::RuntimeFilterScanDomainBinding::new(
                    execution::RuntimeFilterBindingId::new(binding.binding_id),
                    execution::scan_domain::RuntimeFilterScanDomainTarget::new(
                        target.field_ordinal,
                        target.data_type.clone(),
                        target.nullable,
                    ),
                )
            })
        }
    };
    Ok(RuntimeFilterConsumerBinding::new(
        expr_id,
        contract.clone(),
        scan_domain,
    ))
}

fn lower_binding_expression(
    binding: &DecodedRuntimeFilterBinding,
    layout: &Layout,
    schema: &ChunkSchemaRef,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<novarocks_execution::exec::expr::ExprId, NativeFragmentDecodeError> {
    let expression_path = binding.expression_path.clone();
    validate_column_refs_exact(
        binding.binding_id,
        &binding.expression,
        layout,
        schema,
        expression_path.clone(),
    )?;
    ctx.decode_expression(&binding.expression, expression_path, arena, layout)
}

fn validate_column_refs_exact(
    binding_id: u32,
    expression: &novarocks_proto::expr::Expr,
    layout: &Layout,
    schema: &ChunkSchemaRef,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    use novarocks_proto::expr::expr::Kind;

    let kind = expression.kind.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("kind"),
            format!("native runtime-filter binding_id={binding_id} expression kind missing"),
        )
    })?;
    if let Kind::ColumnRef(column) = kind {
        let column_path = path.clone().field("column_ref");
        let slot_id = layout
            .resolve_column_id(column.column_id)
            .map_err(|error| {
                NativeFragmentDecodeError::invalid_value(
                    column_path.clone().field("column_id"),
                    format!("native runtime-filter binding_id={binding_id}: {error}"),
                )
            })?;
        let expected = schema.field_by_slot(slot_id).ok_or_else(|| {
            NativeFragmentDecodeError::inconsistent(
                column_path.clone().field("column_id"),
                format!(
                    "native runtime-filter binding_id={binding_id} ColumnRef column_id={} has no ChunkSchema field",
                    column.column_id
                ),
            )
        })?;
        let type_desc = expression.r#type.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone().field("type"),
                format!(
                    "native runtime-filter binding_id={binding_id} ColumnRef column_id={} type missing",
                    column.column_id
                ),
            )
        })?;
        let actual = crate::fragment::decode::type_decode::decode_field_type(
            "_runtime_filter_column",
            expression.nullable,
            type_desc,
        )
        .map_err(|error| {
            NativeFragmentDecodeError::invalid_value(
                path.clone().field("type"),
                format!(
                    "native runtime-filter binding_id={binding_id} ColumnRef column_id={} type: {error}",
                    column.column_id
                ),
            )
        })?;
        let expected_schema = novarocks_execution::exec::chunk::ChunkFieldSchema::from_field(
            expected,
        )
        .map_err(|error| NativeFragmentDecodeError::inconsistent(column_path.clone(), error))?;
        let actual_schema = novarocks_execution::exec::chunk::ChunkFieldSchema::from_field(&actual)
            .map_err(|error| {
                NativeFragmentDecodeError::invalid_value(path.clone().field("type"), error)
            })?;
        if expected.data_type() != actual.data_type()
            || expected.is_nullable() != actual.is_nullable()
            || expected_schema != actual_schema
        {
            return Err(NativeFragmentDecodeError::inconsistent(
                column_path,
                format!(
                    "native runtime-filter binding_id={binding_id} ColumnRef column_id={} type/nullability does not exactly match direct input",
                    column.column_id
                ),
            ));
        }
    }

    let visit = |child: &novarocks_proto::expr::Expr, child_path: FieldPath| {
        validate_column_refs_exact(binding_id, child, layout, schema, child_path)
    };
    let missing = |child_path: FieldPath, detail: &'static str| {
        NativeFragmentDecodeError::missing(child_path, detail)
    };
    match kind {
        Kind::ColumnRef(_) | Kind::Literal(_) | Kind::LambdaParamRef(_) => Ok(()),
        Kind::BinaryOp(binary) => {
            let binary_path = path.field("binary_op");
            let left_path = binary_path.clone().field("left");
            visit(
                binary
                    .left
                    .as_ref()
                    .ok_or_else(|| missing(left_path.clone(), "BinaryOp.left missing"))?,
                left_path,
            )?;
            let right_path = binary_path.field("right");
            visit(
                binary
                    .right
                    .as_ref()
                    .ok_or_else(|| missing(right_path.clone(), "BinaryOp.right missing"))?,
                right_path,
            )
        }
        Kind::UnaryOp(unary) => {
            let operand_path = path.field("unary_op").field("operand");
            visit(
                unary
                    .operand
                    .as_ref()
                    .ok_or_else(|| missing(operand_path.clone(), "UnaryOp.operand missing"))?,
                operand_path,
            )
        }
        Kind::FunctionCall(call) => call.args.iter().enumerate().try_for_each(|(index, child)| {
            visit(
                child,
                path.clone()
                    .field("function_call")
                    .field("args")
                    .index(index),
            )
        }),
        Kind::AggregateCall(call) => {
            let call_path = path.clone().field("aggregate_call");
            call.args
                .iter()
                .enumerate()
                .try_for_each(|(index, child)| {
                    visit(child, call_path.clone().field("args").index(index))
                })?;
            call.order_by
                .iter()
                .enumerate()
                .try_for_each(|(index, item)| {
                    let expr_path = call_path
                        .clone()
                        .field("order_by")
                        .index(index)
                        .field("expr");
                    visit(
                        item.expr
                            .as_ref()
                            .ok_or_else(|| missing(expr_path.clone(), "SortItem.expr missing"))?,
                        expr_path,
                    )
                })
        }
        Kind::WindowCall(call) => {
            let call_path = path.clone().field("window_call");
            call.args
                .iter()
                .enumerate()
                .try_for_each(|(index, child)| {
                    visit(child, call_path.clone().field("args").index(index))
                })?;
            call.partition_by
                .iter()
                .enumerate()
                .try_for_each(|(index, child)| {
                    visit(child, call_path.clone().field("partition_by").index(index))
                })?;
            call.order_by
                .iter()
                .enumerate()
                .try_for_each(|(index, item)| {
                    let expr_path = call_path
                        .clone()
                        .field("order_by")
                        .index(index)
                        .field("expr");
                    visit(
                        item.expr
                            .as_ref()
                            .ok_or_else(|| missing(expr_path.clone(), "SortItem.expr missing"))?,
                        expr_path,
                    )
                })
        }
        Kind::Cast(cast) => {
            let operand_path = path.field("cast").field("operand");
            visit(
                cast.operand
                    .as_ref()
                    .ok_or_else(|| missing(operand_path.clone(), "Cast.operand missing"))?,
                operand_path,
            )
        }
        Kind::IsNull(is_null) => {
            let operand_path = path.field("is_null").field("operand");
            visit(
                is_null
                    .operand
                    .as_ref()
                    .ok_or_else(|| missing(operand_path.clone(), "IsNull.operand missing"))?,
                operand_path,
            )
        }
        Kind::InList(in_list) => {
            let list_path = path.clone().field("in_list");
            let operand_path = list_path.clone().field("operand");
            visit(
                in_list
                    .operand
                    .as_ref()
                    .ok_or_else(|| missing(operand_path.clone(), "InList.operand missing"))?,
                operand_path,
            )?;
            in_list
                .list
                .iter()
                .enumerate()
                .try_for_each(|(index, child)| {
                    visit(child, list_path.clone().field("list").index(index))
                })
        }
        Kind::Between(between) => {
            let between_path = path.clone().field("between");
            for (field, child, detail) in [
                (
                    "operand",
                    between.operand.as_ref(),
                    "Between.operand missing",
                ),
                ("low", between.low.as_ref(), "Between.low missing"),
                ("high", between.high.as_ref(), "Between.high missing"),
            ] {
                let child_path = between_path.clone().field(field);
                visit(
                    child.ok_or_else(|| missing(child_path.clone(), detail))?,
                    child_path,
                )?;
            }
            Ok(())
        }
        Kind::Like(like) => {
            let like_path = path.clone().field("like");
            for (field, child, detail) in [
                ("operand", like.operand.as_ref(), "Like.operand missing"),
                ("pattern", like.pattern.as_ref(), "Like.pattern missing"),
            ] {
                let child_path = like_path.clone().field(field);
                visit(
                    child.ok_or_else(|| missing(child_path.clone(), detail))?,
                    child_path,
                )?;
            }
            Ok(())
        }
        Kind::CaseExpr(case_expr) => {
            let case_path = path.clone().field("case_expr");
            if let Some(operand) = &case_expr.operand {
                visit(operand, case_path.clone().field("operand"))?;
            }
            for (index, branch) in case_expr.when_then.iter().enumerate() {
                let branch_path = case_path.clone().field("when_then").index(index);
                let when_path = branch_path.clone().field("when");
                visit(
                    branch
                        .when
                        .as_ref()
                        .ok_or_else(|| missing(when_path.clone(), "Case.when missing"))?,
                    when_path,
                )?;
                let then_path = branch_path.field("then");
                visit(
                    branch
                        .then
                        .as_ref()
                        .ok_or_else(|| missing(then_path.clone(), "Case.then missing"))?,
                    then_path,
                )?;
            }
            if let Some(else_expr) = &case_expr.else_expr {
                visit(else_expr, case_path.field("else_expr"))?;
            }
            Ok(())
        }
        Kind::IsTruth(is_truth) => {
            let operand_path = path.field("is_truth").field("operand");
            visit(
                is_truth
                    .operand
                    .as_ref()
                    .ok_or_else(|| missing(operand_path.clone(), "IsTruth.operand missing"))?,
                operand_path,
            )
        }
        Kind::Lambda(lambda) => {
            let body_path = path.field("lambda").field("body");
            visit(
                lambda
                    .body
                    .as_ref()
                    .ok_or_else(|| missing(body_path.clone(), "Lambda.body missing"))?,
                body_path,
            )
        }
        Kind::Nested(nested) => {
            let inner_path = path.field("nested").field("inner");
            visit(
                nested
                    .inner
                    .as_ref()
                    .ok_or_else(|| missing(inner_path.clone(), "Nested.inner missing"))?,
                inner_path,
            )
        }
    }
}

fn apply_distributed_limit_if_needed(
    node: &plan::DistributedNode,
    mut lowered: DecodedNode,
    path: FieldPath,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let Some(limit) = NativeFragmentDecodeError::map_invalid(
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
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let physical_output_path = path.clone().field("output_columns");
    let kind = physical.kind.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
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
        plan::plan_node::Kind::Decode(_) => Err(NativeFragmentDecodeError::unsupported(
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
        plan::plan_node::Kind::CteAnchor(_) => Err(NativeFragmentDecodeError::unsupported(
            path.clone().field("cte_anchor"),
            "native physical node kind CTEAnchor is unsupported",
        )),
        plan::plan_node::Kind::CteProduce(_) => Err(NativeFragmentDecodeError::unsupported(
            path.clone().field("cte_produce"),
            "native physical node kind CTEProduce is unsupported",
        )),
        plan::plan_node::Kind::CteConsume(_) => Err(NativeFragmentDecodeError::unsupported(
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
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::fragment::decode::type_decode::encode_type;
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_execution::exec::node::assert::{AssertNumRowsMode, Assertion};
    use novarocks_execution::exec::node::set_op::SetOpKind;
    use novarocks_execution::runtime_filter as execution;
    use novarocks_proto::{common, expr, plan};
    use novarocks_types::SlotId;

    #[allow(
        dead_code,
        reason = "Retained for target-specific native integration and regression coverage."
    )]
    struct DummyScanOp;

    impl novarocks_execution::exec::node::scan::ScanOp for DummyScanOp {
        fn execute_iter(
            &self,
            _morsel: novarocks_execution::exec::node::scan::ScanMorsel,
            _profile: Option<novarocks_execution::runtime::profile::RuntimeProfile>,
            _runtime_filters: Option<&novarocks_execution::exec::node::scan::RuntimeFilterContext>,
        ) -> Result<novarocks_execution::exec::node::BoxedExecIter, String> {
            Ok(Box::new(std::iter::empty()))
        }

        fn build_morsels(
            &self,
        ) -> Result<novarocks_execution::exec::node::scan::ScanMorsels, String> {
            Ok(novarocks_execution::exec::node::scan::ScanMorsels::default())
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

    #[allow(
        dead_code,
        reason = "Retained for target-specific native integration and regression coverage."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained for target-specific native integration and regression coverage."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained for target-specific native integration and regression coverage."
    )]
    fn decode_error(
        node: &plan::DistributedNode,
    ) -> crate::fragment::decode::plan::error::NativeFragmentDecodeError {
        decode_node(
            node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .expect_err("invalid node must fail")
    }

    #[allow(
        dead_code,
        reason = "Retained for target-specific native integration and regression coverage."
    )]
    fn assert_children_error(node: &plan::DistributedNode) {
        let error = decode_error(node);
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.path().to_string(), "plan_fragment.root.children");
        assert_eq!(
            protocol.kind(),
            novarocks_proto::ProtocolErrorKind::InconsistentFields
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
            novarocks_execution::exec::node::join::JoinType::Inner
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

    #[test]
    fn execution_contract_retains_ordered_key_semantics() {
        let contract = execution::RuntimeFilterExecutionContract::Ordered(Arc::new(
            execution::contribution::RuntimeOrderContract::from_frozen(
                [execution::contribution::RuntimeOrderKey::with_order(
                    DataType::Int64,
                    execution::contribution::RuntimeOrderSortDirection::Descending,
                    execution::contribution::RuntimeOrderNullOrder::First,
                )],
                [3; 32],
                [4; 32],
            ),
        ));

        let execution::RuntimeFilterExecutionContract::Ordered(order) = contract else {
            panic!("ordered decoded contract must remain ordered")
        };
        assert_eq!(order.keys().len(), 1);
        assert_eq!(order.keys()[0].data_type(), &DataType::Int64);
        assert_eq!(
            order.keys()[0].direction(),
            execution::contribution::RuntimeOrderSortDirection::Descending
        );
        assert_eq!(
            order.keys()[0].null_order(),
            execution::contribution::RuntimeOrderNullOrder::First
        );
        assert_eq!(order.comparator_digest(), [3; 32]);
        assert_eq!(order.digest(), [4; 32]);
    }
}
