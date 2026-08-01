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

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::common::types::UniqueId;
use crate::exec::expr::ExprArena;
use crate::exec::fragment::program::{
    ExchangeInputContract, FragmentContractVersion, FragmentNodeId, FragmentProgram,
    FragmentProgramOptions, FragmentSinkSpec, RuntimeFilterContract, RuntimeFilterId,
    ScanSourceContract,
};
use crate::exec::node::ExecPlan;
use crate::proto::{novarocks, plan};
use crate::protocol::common::error::FieldPath;
use crate::protocol::native::test_assembly::{
    NativeExchangeContractDecoder, NativeExpressionDecoder, NativeFragmentEnvelopeDecoder,
    NativeFragmentInstanceInput, NativeFragmentSinkAssignmentDecoder,
    NativeFragmentSubmissionValidator, NativeOutputLayoutDecoder, NativeScanSourceContractDecoder,
    RuntimeFilterExecutionContractDecoder,
};
use crate::query_execution::contract::QueryId as ExecutionQueryId;
use crate::query_execution::lifecycle::{AttemptId, QueryExecutionId};
use crate::runtime::fragment::instance::{
    BackendNum, ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
    FragmentInstanceSpec, FragmentRuntimeOptions, ScanAssignments,
};
use crate::runtime::fragment::submission::FragmentSubmission;
use crate::runtime::query_context::QueryId;

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
use super::decode_fragment_sink_assignment;
use super::instance::{NativeSubmissionMetadata, decode_scan_range_params_at};
use super::{
    NativeFragmentDecodeError, NativePlanDecodeContext, NativeRuntimeFilterDecodeLedger,
    decode_fragment_sink_program_with_context, decode_node_with_runtime_filters,
    decode_query_options,
};

#[derive(Debug)]
pub(crate) struct DecodedNativeFragment {
    submission: FragmentSubmission,
    metadata: NativeSubmissionMetadata,
}

impl DecodedNativeFragment {
    fn new(submission: FragmentSubmission, metadata: NativeSubmissionMetadata) -> Self {
        Self {
            submission,
            metadata,
        }
    }

    pub(crate) fn into_parts(self) -> (FragmentSubmission, NativeSubmissionMetadata) {
        (self.submission, self.metadata)
    }
}

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub(crate) fn decode_fragment_submission_with_connectors(
    fragment: &plan::PlanFragment,
    instance_params: &novarocks::InstanceParams,
    connectors: Arc<crate::connector::ConnectorRegistry>,
) -> Result<DecodedNativeFragment, NativeFragmentDecodeError> {
    decode_fragment_submission_with_connectors_and_execution_resolver(
        fragment,
        instance_params,
        connectors,
        Arc::new(MissingExecutionResolver),
    )
}

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub(crate) fn decode_fragment_submission_with_connectors_and_execution_resolver(
    fragment: &plan::PlanFragment,
    instance_params: &novarocks::InstanceParams,
    connectors: Arc<crate::connector::ConnectorRegistry>,
    execution_resolver: Arc<dyn novarocks_spi::connector::ConnectorExecutionResolver>,
) -> Result<DecodedNativeFragment, NativeFragmentDecodeError> {
    let instance_parts = decode_instance_parts(instance_params)?;
    assemble_fragment_submission_with_sink_assignment(
        fragment,
        instance_parts,
        connectors,
        execution_resolver,
        |fragment| {
            fragment.root.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    FieldPath::root("plan_fragment").field("root"),
                    "native PlanFragment requires root",
                )
            })
        },
        None,
        |fragment| {
            fragment.sink.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    FieldPath::root("plan_fragment").field("sink"),
                    "native PlanFragment requires sink",
                )
            })
        },
        |sink| decode_fragment_sink_assignment(sink, instance_params),
        None,
        None,
        |root, path| decode_scan_source_contracts(root, path),
        |root, path| decode_exchange_contracts(root, path),
        |fragment| decode_runtime_filter_contract(fragment),
    )
}

pub(crate) fn assemble_fragment_submission_with_connectors_and_execution_resolver(
    fragment: &plan::PlanFragment,
    instance_parts: NativeFragmentInstanceInput,
    instance_params: &novarocks::InstanceParams,
    envelope_decoder: &dyn NativeFragmentEnvelopeDecoder,
    submission_validator: &dyn NativeFragmentSubmissionValidator,
    sink_assignment_decoder: &dyn NativeFragmentSinkAssignmentDecoder,
    expression_decoder: Arc<dyn NativeExpressionDecoder>,
    output_layout_decoder: Arc<dyn NativeOutputLayoutDecoder>,
    scan_source_contract_decoder: &dyn NativeScanSourceContractDecoder,
    exchange_contract_decoder: &dyn NativeExchangeContractDecoder,
    runtime_filter_contract_decoder: &dyn RuntimeFilterExecutionContractDecoder,
    connectors: Arc<crate::connector::ConnectorRegistry>,
    execution_resolver: Arc<dyn novarocks_spi::connector::ConnectorExecutionResolver>,
) -> Result<DecodedNativeFragment, NativeFragmentDecodeError> {
    assemble_fragment_submission_with_sink_assignment(
        fragment,
        instance_parts,
        connectors,
        execution_resolver,
        |fragment| {
            envelope_decoder
                .require_root(fragment)
                .map_err(NativeFragmentDecodeError::from)
        },
        Some(submission_validator),
        |fragment| {
            envelope_decoder
                .require_sink(fragment)
                .map_err(NativeFragmentDecodeError::from)
        },
        |sink| {
            sink_assignment_decoder
                .decode_sink_assignment(sink, instance_params)
                .map_err(NativeFragmentDecodeError::from)
        },
        Some(expression_decoder),
        Some(output_layout_decoder),
        |root, path| {
            scan_source_contract_decoder
                .decode_scan_source_contracts(root, path)
                .map_err(NativeFragmentDecodeError::from)
        },
        |root, path| {
            exchange_contract_decoder
                .decode_exchange_contracts(root, path)
                .map_err(NativeFragmentDecodeError::from)
        },
        |fragment| {
            runtime_filter_contract_decoder
                .decode_runtime_filter_contract(fragment)
                .map_err(NativeFragmentDecodeError::from)
        },
    )
}

fn assemble_fragment_submission_with_sink_assignment<F>(
    fragment: &plan::PlanFragment,
    instance_parts: NativeFragmentInstanceInput,
    connectors: Arc<crate::connector::ConnectorRegistry>,
    execution_resolver: Arc<dyn novarocks_spi::connector::ConnectorExecutionResolver>,
    require_root: impl FnOnce(
        &plan::PlanFragment,
    ) -> Result<&plan::DistributedNode, NativeFragmentDecodeError>,
    submission_validator: Option<&dyn NativeFragmentSubmissionValidator>,
    require_sink: impl FnOnce(&plan::PlanFragment) -> Result<&plan::DataSink, NativeFragmentDecodeError>,
    decode_sink_assignment: F,
    expression_decoder: Option<Arc<dyn NativeExpressionDecoder>>,
    output_layout_decoder: Option<Arc<dyn NativeOutputLayoutDecoder>>,
    decode_scan_source_contracts: impl FnOnce(
        &plan::DistributedNode,
        FieldPath,
    ) -> Result<
        BTreeMap<FragmentNodeId, ScanSourceContract>,
        NativeFragmentDecodeError,
    >,
    decode_exchange_contracts: impl FnOnce(
        &plan::DistributedNode,
        FieldPath,
    ) -> Result<
        BTreeMap<FragmentNodeId, ExchangeInputContract>,
        NativeFragmentDecodeError,
    >,
    decode_runtime_filter_contract: impl FnOnce(
        &plan::PlanFragment,
    ) -> Result<
        RuntimeFilterContract,
        NativeFragmentDecodeError,
    >,
) -> Result<DecodedNativeFragment, NativeFragmentDecodeError>
where
    F: FnOnce(
        &plan::DataSink,
    ) -> Result<
        crate::runtime::fragment::instance::FragmentSinkAssignment,
        NativeFragmentDecodeError,
    >,
{
    let root_path = FieldPath::root("plan_fragment").field("root");
    let root = require_root(fragment)?;
    if let Some(submission_validator) = submission_validator {
        submission_validator
            .validate_root_node(root, root_path.clone())
            .map_err(NativeFragmentDecodeError::from)?;
    } else {
        #[cfg(any(test, feature = "query-execution-contract-test-support"))]
        validate_node_required_fields(root, root_path.clone())?;
        #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
        return Err(NativeFragmentDecodeError::unsupported(
            root_path,
            "native submission validator must be supplied by the backend runtime",
        ));
    }
    let sink_path = FieldPath::root("plan_fragment").field("sink");
    let sink = require_sink(fragment)?;
    if sink.kind.is_none() {
        return Err(NativeFragmentDecodeError::missing(
            sink_path.clone().field("kind"),
            "native DataSink requires kind",
        ));
    }
    if let Some(submission_validator) = submission_validator {
        submission_validator
            .validate_fragment_expressions(fragment)
            .map_err(NativeFragmentDecodeError::from)?;
    } else {
        #[cfg(any(test, feature = "query-execution-contract-test-support"))]
        {
            for (index, expression) in fragment.output_exprs.iter().enumerate() {
                super::expr::validate_proto_expr_shape_at(
                    expression,
                    FieldPath::root("plan_fragment")
                        .field("output_exprs")
                        .index(index),
                )?;
            }
            validate_runtime_filter_binding_expressions(fragment)?;
        }
        #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
        return Err(NativeFragmentDecodeError::unsupported(
            FieldPath::root("plan_fragment"),
            "native submission validator must be supplied by the backend runtime",
        ));
    }

    let scan_sources = decode_scan_source_contracts(root, root_path.clone())?;
    // Preserve the old cross-check: every FE-provided scan-range node must map
    // to a static scan contract. The enriched `BoundScanRanges` are captured
    // during node decode (below) and assembled into the instance afterwards;
    // the raw `ScanRangeParams` here are only the transient enrichment input.
    validate_raw_scan_range_nodes(
        &scan_sources,
        &instance_parts.raw_scan_ranges,
        FieldPath::root("instance_params").field("per_node_scan_ranges"),
    )?;
    let sink_assignment = decode_sink_assignment(sink)?;

    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(instance_parts.query_options.allow_throw_exception);
    let mut context = NativePlanDecodeContext::from_parts(
        instance_parts.exchange_inputs.clone(),
        instance_parts.raw_scan_ranges,
        instance_parts.query_options.clone(),
        connectors,
        instance_parts.query_id,
        instance_parts.fragment_instance_id,
    )
    .with_execution_resolver(execution_resolver);
    if let Some(expression_decoder) = expression_decoder {
        context = context.with_expression_decoder(expression_decoder);
    }
    if let Some(output_layout_decoder) = output_layout_decoder {
        context = context.with_output_layout_decoder(output_layout_decoder);
    }
    let mut runtime_filter_ledger = NativeRuntimeFilterDecodeLedger::decode(
        fragment.fragment_id,
        fragment.runtime_filter_bindings.as_ref(),
    )?;
    let decoded_root =
        decode_node_with_runtime_filters(root, &mut arena, &context, &mut runtime_filter_ledger)?;
    runtime_filter_ledger.finish()?;
    // Drain the per-node enriched `BoundScanRanges` captured during decode into
    // the instance's scan assignments (`materialize_scan_bindings` binds these).
    let scan_assignments = ScanAssignments::try_new(context.take_captured_scan_ranges())
        .map_err(NativeFragmentDecodeError::Binding)?;
    let plan = crate::exec::node::ExecPlanBuilder::new(arena, decoded_root.node).finish()?;
    let sink_program =
        decode_fragment_sink_program_with_context(fragment, &decoded_root.layout, Some(&context))?;
    let sink_spec =
        FragmentSinkSpec::try_new(sink_program).map_err(NativeFragmentDecodeError::Binding)?;
    let exchange_inputs = decode_exchange_contracts(root, root_path)?;
    let runtime_filters = decode_runtime_filter_contract(fragment)?;
    let program = crate::exec::fragment::program::FragmentProgramBuilder::new(
        plan,
        sink_spec,
        FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
    )
    .scan_sources(scan_sources)
    .exchange_inputs(exchange_inputs)
    .runtime_filters(runtime_filters)
    .finish()?;
    let metadata = NativeSubmissionMetadata::new(
        instance_parts.backend_num.get(),
        instance_parts.typed_result_sink,
    );
    let instance = FragmentInstanceSpec::new_native(
        FragmentContractVersion::CURRENT,
        instance_parts.query_id,
        instance_parts.fragment_instance_id,
        scan_assignments,
        instance_parts.exchange_inputs,
        sink_assignment,
        FragmentRuntimeOptions::new(
            instance_parts.query_options,
            instance_parts.typed_result_sink,
        ),
        instance_parts.pipeline_dop,
        instance_parts.backend_num,
    );
    let submission = FragmentSubmission::try_new(Arc::new(program), instance)
        .map_err(NativeFragmentDecodeError::Binding)?;
    Ok(DecodedNativeFragment::new(submission, metadata))
}

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
struct MissingExecutionResolver;

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
impl novarocks_spi::connector::ConnectorExecutionResolver for MissingExecutionResolver {
    fn resolve(
        &self,
        _key: &novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<
        Arc<novarocks_spi::connector::ConnectorExecutionBinding>,
        novarocks_spi::connector::ConnectorError,
    > {
        Err(novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unavailable,
            "native ConnectorReadSource execution resolver is not configured",
        ))
    }
}

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub(crate) fn decode_query_execution_id(
    execution_id: &novarocks::QueryExecutionId,
) -> Result<QueryExecutionId, NativeFragmentDecodeError> {
    let root = FieldPath::root("execution_id");
    let query_id = execution_id.query_id.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            root.clone().field("query_id"),
            "native fragment execution_id requires query_id",
        )
    })?;
    let attempt_id = AttemptId::new(execution_id.attempt_id).map_err(|error| {
        NativeFragmentDecodeError::invalid_value(
            root.clone().field("attempt_id"),
            error.to_string(),
        )
    })?;
    QueryExecutionId::new(ExecutionQueryId::new(query_id.hi, query_id.lo), attempt_id)
        .map_err(|error| NativeFragmentDecodeError::invalid_value(root, error.to_string()))
}

#[cfg(test)]
pub(crate) fn decode_fragment_submission(
    fragment: &plan::PlanFragment,
    instance_params: &novarocks::InstanceParams,
) -> Result<DecodedNativeFragment, NativeFragmentDecodeError> {
    decode_fragment_submission_with_connectors(
        fragment,
        instance_params,
        Arc::new(crate::connector::ConnectorRegistry::new()),
    )
}

fn validate_node_required_fields(
    node: &plan::DistributedNode,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    let payload = node.payload.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("payload"),
            format!("native DistributedNode {} requires payload", node.node_id),
        )
    })?;
    if let plan::distributed_node::Payload::Physical(physical) = payload {
        let kind = physical.kind.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone()
                    .field("payload")
                    .field("physical")
                    .field("kind"),
                format!("native PlanNode {} requires kind", node.node_id),
            )
        })?;
        if let plan::plan_node::Kind::Values(values) = kind {
            for (row_index, row) in values.rows.iter().enumerate() {
                for (value_index, value) in row.values.iter().enumerate() {
                    super::expr::validate_proto_expr_shape_at(
                        value,
                        path.clone()
                            .field("payload")
                            .field("physical")
                            .field("values")
                            .field("rows")
                            .index(row_index)
                            .field("values")
                            .index(value_index),
                    )?;
                }
            }
        }
    }
    for (index, child) in node.children.iter().enumerate() {
        validate_node_required_fields(child, path.clone().field("children").index(index))?;
    }
    Ok(())
}

fn validate_runtime_filter_binding_expressions(
    fragment: &plan::PlanFragment,
) -> Result<(), NativeFragmentDecodeError> {
    let Some(table) = fragment.runtime_filter_bindings.as_ref() else {
        return Ok(());
    };
    for (index, binding) in table.bindings.iter().enumerate() {
        let path = FieldPath::root("plan_fragment")
            .field("runtime_filter_bindings")
            .field("bindings")
            .index(index)
            .field("expression");
        let expression = binding.expression.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone(),
                "native runtime-filter binding requires expression",
            )
        })?;
        super::expr::validate_proto_expr_shape_at(expression, path)?;
    }
    Ok(())
}

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
fn decode_instance_parts(
    src: &novarocks::InstanceParams,
) -> Result<NativeFragmentInstanceInput, NativeFragmentDecodeError> {
    let path = FieldPath::root("instance_params");
    let query_id = src.query_id.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("query_id"),
            "native InstanceParams requires query_id",
        )
    })?;
    let fragment_instance_id = src.fragment_instance_id.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("fragment_instance_id"),
            "native InstanceParams requires fragment_instance_id",
        )
    })?;
    if src.backend_num < 0 {
        return Err(NativeFragmentDecodeError::out_of_range(
            path.clone().field("backend_num"),
            format!("backend_num must be non-negative, got {}", src.backend_num),
        ));
    }
    let backend_num =
        BackendNum::try_new(src.backend_num).map_err(NativeFragmentDecodeError::Binding)?;
    let wire_query_options = src.query_options.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("query_options"),
            "native InstanceParams requires query_options with explicit pipeline_dop",
        )
    })?;
    let query_options = decode_query_options(wire_query_options)?;
    let pipeline_dop = usize::try_from(wire_query_options.pipeline_dop)
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| {
            NativeFragmentDecodeError::out_of_range(
                path.clone().field("query_options").field("pipeline_dop"),
                format!(
                    "pipeline_dop must be explicitly positive, got {}",
                    wire_query_options.pipeline_dop
                ),
            )
        })?;
    let mut scan_keys = src.per_node_scan_ranges.keys().copied().collect::<Vec<_>>();
    scan_keys.sort_unstable();
    let mut raw_scan_ranges = BTreeMap::new();
    for raw_node_id in scan_keys {
        let list_path = path
            .clone()
            .field("per_node_scan_ranges")
            .map_key(raw_node_id.to_string());
        let wire_ranges = &src.per_node_scan_ranges[&raw_node_id];
        let mut ranges = Vec::with_capacity(wire_ranges.ranges.len());
        for (index, range) in wire_ranges.ranges.iter().enumerate() {
            ranges.push(decode_scan_range_params_at(
                range,
                list_path.clone().field("ranges").index(index),
            )?);
        }
        raw_scan_ranges.insert(FragmentNodeId::new(raw_node_id), ranges);
    }

    let mut exchange_keys = src.per_exch_num_senders.keys().copied().collect::<Vec<_>>();
    exchange_keys.sort_unstable();
    let mut exchange_inputs = BTreeMap::new();
    for raw_node_id in exchange_keys {
        let sender_count = src.per_exch_num_senders[&raw_node_id];
        let count = usize::try_from(sender_count)
            .ok()
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                NativeFragmentDecodeError::out_of_range(
                    path.clone()
                        .field("per_exch_num_senders")
                        .map_key(raw_node_id.to_string()),
                    format!("sender count must be positive, got {sender_count}"),
                )
            })?;
        exchange_inputs.insert(
            FragmentNodeId::new(raw_node_id),
            ExchangeInputAssignment::new(count),
        );
    }
    Ok(NativeFragmentInstanceInput::new(
        query_id_from_native(query_id),
        FragmentInstanceId::new(unique_id_from_native(fragment_instance_id)),
        backend_num,
        query_options,
        pipeline_dop,
        raw_scan_ranges,
        ExchangeInputAssignments::new(exchange_inputs),
        src.typed_result_sink,
    ))
}

/// Cross-check that every FE-provided per-node scan-range entry maps to a
/// static scan contract (an "unknown scan node" is rejected here, as the old
/// `bind_scan_assignments` did). The ranges themselves are the transient
/// enrichment input; the instance's `ScanAssignments` are assembled from the
/// enriched `BoundScanRanges` captured during node decode.
fn validate_raw_scan_range_nodes(
    contracts: &BTreeMap<FragmentNodeId, ScanSourceContract>,
    raw_ranges: &BTreeMap<FragmentNodeId, Vec<crate::runtime::scan_range::ScanRangeParams>>,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    for node_id in raw_ranges.keys() {
        if !contracts.contains_key(node_id) {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().map_key(node_id.get().to_string()),
                format!(
                    "scan ranges assigned to unknown scan node {}",
                    node_id.get()
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
fn decode_scan_source_contracts(
    root: &plan::DistributedNode,
    path: FieldPath,
) -> Result<BTreeMap<FragmentNodeId, ScanSourceContract>, NativeFragmentDecodeError> {
    super::node::collect_scan_assignment_kinds(root, path).map(|kinds| {
        kinds
            .into_iter()
            .map(|(node_id, kind)| (node_id, ScanSourceContract::new(kind)))
            .collect()
    })
}

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
fn decode_exchange_contracts(
    root: &plan::DistributedNode,
    path: FieldPath,
) -> Result<BTreeMap<FragmentNodeId, ExchangeInputContract>, NativeFragmentDecodeError> {
    fn visit(
        node: &plan::DistributedNode,
        path: FieldPath,
        contracts: &mut BTreeMap<FragmentNodeId, ExchangeInputContract>,
    ) -> Result<(), NativeFragmentDecodeError> {
        if let Some(plan::distributed_node::Payload::Exchange(exchange)) = node.payload.as_ref() {
            let schema = super::layout::chunk_schema_from_output_columns(&exchange.output_columns)
                .map_err(|error| {
                    error.into_native(
                        path.clone()
                            .field("payload")
                            .field("exchange")
                            .field("output_columns"),
                    )
                })?;
            if contracts
                .insert(
                    FragmentNodeId::new(node.node_id),
                    ExchangeInputContract::new(schema),
                )
                .is_some()
            {
                return Err(NativeFragmentDecodeError::inconsistent(
                    path.clone().field("node_id"),
                    format!("duplicate exchange node_id={}", node.node_id),
                ));
            }
        }
        for (index, child) in node.children.iter().enumerate() {
            visit(
                child,
                path.clone().field("children").index(index),
                contracts,
            )?;
        }
        Ok(())
    }

    let mut contracts = BTreeMap::new();
    visit(root, path, &mut contracts)?;
    Ok(contracts)
}

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
fn decode_runtime_filter_contract(
    fragment: &plan::PlanFragment,
) -> Result<RuntimeFilterContract, NativeFragmentDecodeError> {
    let path = FieldPath::root("plan_fragment").field("runtime_filter_bindings");
    let table = fragment.runtime_filter_bindings.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(path.clone(), "runtime_filter_bindings are required")
    })?;
    let mut build_filters = BTreeSet::new();
    let mut probe_filters = BTreeSet::new();
    for (index, binding) in table.bindings.iter().enumerate() {
        let raw_id = i32::try_from(binding.channel_id).map_err(|_| {
            NativeFragmentDecodeError::out_of_range(
                path.clone()
                    .field("bindings")
                    .index(index)
                    .field("channel_id"),
                format!("channel_id {} exceeds i32 range", binding.channel_id),
            )
        })?;
        match binding.role.as_ref() {
            Some(plan::runtime_filter_binding::Role::Producer(_)) => {
                build_filters.insert(RuntimeFilterId::new(raw_id));
            }
            Some(plan::runtime_filter_binding::Role::Consumer(_)) => {
                probe_filters.insert(RuntimeFilterId::new(raw_id));
            }
            None => {
                return Err(NativeFragmentDecodeError::missing(
                    path.clone().field("bindings").index(index).field("role"),
                    "runtime-filter binding role is required",
                ));
            }
        }
    }
    Ok(RuntimeFilterContract::new(build_filters, probe_filters))
}

fn unique_id_from_native(src: &crate::proto::common::UniqueId) -> UniqueId {
    UniqueId::new(src.hi, src.lo)
}

fn query_id_from_native(src: &crate::proto::common::UniqueId) -> QueryId {
    QueryId::new(src.hi, src.lo)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};

    use arrow::datatypes::DataType;

    use super::*;
    use crate::common::types::UniqueId;
    use crate::exec::fragment::program::FragmentSinkKind;
    use crate::exec::node::ExecNodeKind;
    use crate::proto::{common, novarocks, plan};
    use crate::protocol::common::error::ProtocolErrorKind;
    use crate::protocol::native::type_mapping::encode_type;
    use crate::runtime::exchange::{ExchangeKey, snapshot_receiver_state};
    use crate::runtime::query_context::{QueryId, query_context_manager};
    use crate::runtime::result_buffer::{self, FetchErrorKind, TryFetchResult};
    use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};

    static NEXT_TEST_ID: AtomicI64 = AtomicI64::new(8_600_000_000_000_000_000);

    fn output_column(column_id: u32) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: "value".to_string(),
            r#type: Some(encode_type(&DataType::Int32).expect("encode type")),
            nullable: false,
            is_internal: false,
        }
    }

    fn values_noop_fragment() -> plan::PlanFragment {
        let columns = vec![output_column(1)];
        plan::PlanFragment {
            fragment_id: 7,
            root: Some(plan::DistributedNode {
                node_id: 11,
                fragment_id: 7,
                limit: -1,
                payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                    output_columns: columns.clone(),
                    kind: Some(plan::plan_node::Kind::Values(plan::ValuesNode {
                        rows: Vec::new(),
                        columns: columns.clone(),
                    })),
                })),
                ..Default::default()
            }),
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::Noop(true)),
            }),
            output_columns: columns,
            runtime_filter_bindings: Some(plan::RuntimeFilterBindingTable {
                fragment_id: 7,
                bindings: Vec::new(),
            }),
            ..Default::default()
        }
    }

    fn instance_params(query: UniqueId, finst: UniqueId) -> novarocks::InstanceParams {
        novarocks::InstanceParams {
            query_id: Some(common::UniqueId {
                hi: query.high(),
                lo: query.low(),
            }),
            fragment_instance_id: Some(common::UniqueId {
                hi: finst.high(),
                lo: finst.low(),
            }),
            backend_num: 3,
            query_options: Some(novarocks::QueryOptions {
                pipeline_dop: 1,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn assert_runtime_state_absent(
        query: QueryId,
        finst: UniqueId,
        exchange_key: ExchangeKey,
        rf_key: QueryKey,
    ) {
        let manager = query_context_manager();
        assert!(manager.query_mem_tracker(query).is_none());
        assert!(manager.query_id_by_finst(finst).is_none());
        let TryFetchResult::Error(error) = result_buffer::try_fetch(finst) else {
            panic!("missing result buffer entry must return an error");
        };
        assert!(matches!(error.kind, FetchErrorKind::NotFound));
        assert!(snapshot_receiver_state(exchange_key).is_none());
        assert!(
            RuntimeFilterLifecycleRegistry::global()
                .snapshot(rf_key)
                .is_none()
        );
    }

    #[test]
    fn values_noop_decodes_to_validated_submission() {
        let query = UniqueId::new(11, 12);
        let finst = UniqueId::new(21, 22);

        let decoded =
            decode_fragment_submission(&values_noop_fragment(), &instance_params(query, finst))
                .expect("decode values/noop submission");
        let (submission, metadata) = decoded.into_parts();

        assert_eq!(submission.instance().query_id(), QueryId::new(11, 12));
        assert_eq!(submission.instance().fragment_instance_id().get(), finst);
        assert_eq!(submission.instance().backend_num().get(), 3);
        assert_eq!(metadata.backend_num(), 3);
        assert_eq!(submission.program().sink().kind(), FragmentSinkKind::Noop);
        assert!(matches!(
            submission.program().plan().root.kind,
            ExecNodeKind::Values(_)
        ));
    }

    #[test]
    fn missing_root_fails_before_missing_sink() {
        let fragment = plan::PlanFragment {
            root: None,
            sink: None,
            ..Default::default()
        };
        let error = decode_fragment_submission(
            &fragment,
            &instance_params(UniqueId::new(31, 32), UniqueId::new(41, 42)),
        )
        .expect_err("missing root must fail");

        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.path().to_string(), "plan_fragment.root");
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn invalid_exchange_maps_are_checked_in_sorted_key_order() {
        let mut params = instance_params(UniqueId::new(51, 52), UniqueId::new(61, 62));
        params.per_exch_num_senders.insert(9, 0);
        params.per_exch_num_senders.insert(3, -1);

        let error = decode_fragment_submission(&values_noop_fragment(), &params)
            .expect_err("invalid sender count must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "instance_params.per_exch_num_senders[\"3\"]"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::OutOfRange);
    }

    #[test]
    fn scan_range_errors_include_sorted_map_key_and_range_index() {
        let mut params = instance_params(UniqueId::new(131, 132), UniqueId::new(141, 142));
        params.per_node_scan_ranges.insert(
            11,
            novarocks::ScanRangeList {
                ranges: vec![novarocks::ScanRangeParams::default()],
            },
        );

        let error = decode_fragment_submission(&values_noop_fragment(), &params)
            .expect_err("missing scan range must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "instance_params.per_node_scan_ranges[\"11\"].ranges[0].range"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn missing_child_payload_reports_recursive_node_path() {
        let mut fragment = values_noop_fragment();
        fragment
            .root
            .as_mut()
            .expect("root")
            .children
            .push(plan::DistributedNode::default());

        let error = decode_fragment_submission(
            &fragment,
            &instance_params(UniqueId::new(91, 92), UniqueId::new(101, 102)),
        )
        .expect_err("missing child payload must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.children[0].payload"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn malformed_values_expression_reports_recursive_expr_path() {
        let mut fragment = values_noop_fragment();
        let root = fragment.root.as_mut().expect("root");
        let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_mut()
        else {
            panic!("physical root");
        };
        let Some(plan::plan_node::Kind::Values(values)) = physical.kind.as_mut() else {
            panic!("values root");
        };
        values.rows.push(plan::ExprList {
            values: vec![crate::proto::expr::Expr::default()],
        });

        let error = decode_fragment_submission(
            &fragment,
            &instance_params(UniqueId::new(111, 112), UniqueId::new(121, 122)),
        )
        .expect_err("missing expression type must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.values.rows[0].values[0].type"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn binary_expression_error_includes_oneof_segment() {
        let mut fragment = values_noop_fragment();
        let root = fragment.root.as_mut().expect("root");
        let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_mut()
        else {
            panic!("physical root");
        };
        let Some(plan::plan_node::Kind::Values(values)) = physical.kind.as_mut() else {
            panic!("values root");
        };
        values.rows.push(plan::ExprList {
            values: vec![crate::proto::expr::Expr {
                r#type: Some(encode_type(&DataType::Boolean).expect("encode type")),
                nullable: false,
                kind: Some(crate::proto::expr::expr::Kind::BinaryOp(Box::new(
                    crate::proto::expr::BinaryOpExpr {
                        op: crate::proto::expr::BinaryOp::Eq as i32,
                        left: None,
                        right: None,
                    },
                ))),
            }],
        });

        let error = decode_fragment_submission(
            &fragment,
            &instance_params(UniqueId::new(211, 212), UniqueId::new(221, 222)),
        )
        .expect_err("missing binary left operand must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.values.rows[0].values[0].binary_op.left"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn scan_missing_table_uses_exact_typed_path() {
        let mut fragment = values_noop_fragment();
        let root = fragment.root.as_mut().expect("root");
        let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_mut()
        else {
            panic!("physical root");
        };
        physical.kind = Some(plan::plan_node::Kind::Scan(plan::ScanNode {
            database: "db".to_string(),
            table: None,
            ..Default::default()
        }));

        let error = decode_fragment_submission(
            &fragment,
            &instance_params(UniqueId::new(231, 232), UniqueId::new(241, 242)),
        )
        .expect_err("missing scan table must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.scan.table"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn false_noop_marker_uses_sink_oneof_path() {
        let mut fragment = values_noop_fragment();
        fragment.sink = Some(plan::DataSink {
            kind: Some(plan::data_sink::Kind::Noop(false)),
        });

        let error = decode_fragment_submission(
            &fragment,
            &instance_params(UniqueId::new(251, 252), UniqueId::new(261, 262)),
        )
        .expect_err("false noop marker must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.path().to_string(), "plan_fragment.sink.noop");
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
    }

    #[test]
    fn submission_binding_errors_keep_binding_stage_identity() {
        let mut params = instance_params(UniqueId::new(151, 152), UniqueId::new(161, 162));
        params.per_exch_num_senders.insert(99, 1);

        let error = decode_fragment_submission(&values_noop_fragment(), &params)
            .expect_err("unknown exchange assignment must fail binding");
        let NativeFragmentDecodeError::Binding(binding) = error else {
            panic!("expected binding error stage");
        };
        assert_eq!(
            binding.target(),
            crate::exec::fragment::error::FragmentBindingTarget::ExchangeNode(99)
        );
    }

    #[test]
    fn malformed_submission_has_zero_runtime_side_effects() {
        let unique = NEXT_TEST_ID.fetch_add(10, Ordering::Relaxed);
        let query = UniqueId::new(unique, unique + 1);
        let finst = UniqueId::new(unique + 2, unique + 3);
        let query_id = QueryId::new(query.high(), query.low());
        let exchange_key = ExchangeKey {
            finst_id_hi: finst.high(),
            finst_id_lo: finst.low(),
            node_id: 77,
        };
        let rf_key = QueryKey::from_hi_lo(query.high(), query.low());
        assert_runtime_state_absent(query_id, finst, exchange_key, rf_key);

        let malformed = plan::PlanFragment {
            root: None,
            sink: None,
            ..Default::default()
        };
        let error = decode_fragment_submission(&malformed, &instance_params(query, finst))
            .expect_err("malformed submission must fail");
        assert_eq!(
            error.protocol().expect("protocol error").path().to_string(),
            "plan_fragment.root"
        );

        assert_runtime_state_absent(query_id, finst, exchange_key, rf_key);
    }
}
