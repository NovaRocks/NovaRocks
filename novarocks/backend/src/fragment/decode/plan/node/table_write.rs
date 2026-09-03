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

//! Fragment decoding of the two dataflow write nodes.
//!
//! A `TableWriter` is an ordinary unary processor: it names one exact
//! query-leased catalog runtime, one query-local logical write target, and the
//! immutable recipe copied to every placement serving that target. Decoding it
//! means resolving the exact write role binding this attempt leased, turning
//! the carried recipe back into a provider value through that binding's own
//! decoder, and joining it to the attempt-local facts a driver needs.
//!
//! Two budgets exist and only one belongs here. The single-handle cap is
//! re-verified at this ingress, because this is where an untrusted carrier
//! enters. The query-wide unique-handle total is deliberately *not*: this
//! decoder sees one carrier at a time and can never reconstruct the query's
//! unique set, so restating that budget here could only produce a wrong answer.
//!
//! A `TableFinish` is n-ary — the planner gives it one exchange receiver per
//! writer fragment — and knows only which logical targets are legal. It gets a
//! structural carrier validator and nothing else: no commit handle, no control
//! binding, and no way to turn a fragment into a provider value.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::node::table_finish::TableFinishNode;
use novarocks_execution::exec::node::table_write_relation::{
    WRITE_RELATION_SLOT_IDS, root_relation_chunk_schema, writer_relation_chunk_schema,
    writer_relation_schema,
};
use novarocks_execution::exec::node::table_writer::{
    TableWriterInputProjection, TableWriterNode, TableWriterPhysicalContextTemplate,
};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_execution::runtime::query_options::query_expire_durations;
use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::connector_write::ValidatedWriterHandle;
use novarocks_proto_models::plan;
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_spi::connector::write_stack::{WriteTargetOrdinal, validate_writer_handle_bytes};

use super::DecodedNode;
use crate::connector::write_data_plane::{
    ObservedConnectorWriteExecution, RoleBoundCommitFragmentEncoder,
    RootCommitFragmentCarrierValidator,
};
use crate::fragment::decode::plan::context::NativePlanDecodeContext;
use crate::fragment::decode::plan::error::NativeFragmentDecodeError;
use crate::fragment::decode::plan::layout::Layout;

/// Decode one `TableWriterNode` and bind it to this attempt's write role
/// binding.
pub(super) fn lower_table_writer_node(
    node: &plan::DistributedNode,
    writer: &plan::TableWriterNode,
    path: FieldPath,
    children: Vec<DecodedNode>,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let child = children
        .into_iter()
        .next()
        .expect("table writer child arity is validated before lowering");
    let node_id = node.node_id;

    let runtime = ctx.typed_scan_runtime().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("catalog_handle"),
            format!(
                "native node_id={node_id} table writer requires a query-leased catalog runtime"
            ),
        )
    })?;

    let wire_catalog_handle = writer.catalog_handle.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("catalog_handle"),
            format!("native node_id={node_id} table writer requires an exact catalog handle"),
        )
    })?;
    let catalog_handle = novarocks_proto_codec::catalog::decode_catalog_handle(
        wire_catalog_handle.clone(),
        path.clone().field("catalog_handle"),
    )
    .map_err(NativeFragmentDecodeError::from)?;

    let target = WriteTargetOrdinal::try_new(writer.write_target_ordinal).map_err(|error| {
        NativeFragmentDecodeError::out_of_range(
            path.clone().field("write_target_ordinal"),
            format!("native node_id={node_id} table writer target ordinal: {error}"),
        )
    })?;

    let carrier = writer.handle.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("handle"),
            format!("native node_id={node_id} table writer requires its writer handle"),
        )
    })?;
    // Only the single-handle cap. The query-wide unique-handle budget belongs
    // to the frontend, which is the only owner that can see the unique set.
    validate_writer_handle_bytes(prost::Message::encoded_len(carrier)).map_err(|error| {
        NativeFragmentDecodeError::out_of_range(
            path.clone().field("handle"),
            format!("native node_id={node_id} table writer handle: {error}"),
        )
    })?;
    let validated = ValidatedWriterHandle::parse(carrier.clone(), path.clone().field("handle"))
        .map_err(NativeFragmentDecodeError::from)?;

    let binding = runtime
        .catalog_write_execution(&catalog_handle)
        .map_err(|error| {
            NativeFragmentDecodeError::invalid_value(
                path.clone().field("catalog_handle"),
                format!(
                    "native node_id={node_id} table writer cannot resolve its query-leased connector write runtime: {error}"
                ),
            )
        })?;
    let handle = binding
        .handle_decoder()
        .decode_writer_handle(&validated)
        .map_err(|error| {
            NativeFragmentDecodeError::invalid_value(
                path.clone().field("handle"),
                format!("native node_id={node_id} table writer handle: {error}"),
            )
        })?;

    if writer.target_schema.is_empty() {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("target_schema"),
            format!("native node_id={node_id} table writer requires a target schema"),
        ));
    }
    let expected_schema = ctx
        .decode_output_layout(&writer.target_schema, path.clone().field("target_schema"))?
        .chunk_schema()
        .arrow_schema_ref();

    // The input binding is a sealed description of which execution outputs feed
    // the writer. The projection itself is always the sealed output
    // expressions, so the binding is validated against the real child rather
    // than reinterpreted into a second, competing projection.
    validate_input_binding(
        writer.input.as_ref(),
        &child,
        node_id,
        path.clone().field("input"),
    )?;

    if writer.output_exprs.is_empty() {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("output_exprs"),
            format!("native node_id={node_id} table writer requires its sealed output expressions"),
        ));
    }
    let mut projection_arena = ExprArena::default();
    if let Some(options) = ctx.query_options() {
        projection_arena.set_allow_throw_exception(options.allow_throw_exception());
    }
    let mut exprs = Vec::with_capacity(writer.output_exprs.len());
    for (index, expression) in writer.output_exprs.iter().enumerate() {
        exprs.push(ctx.decode_expression(
            expression,
            path.clone().field("output_exprs").index(index),
            &mut projection_arena,
            &child.layout,
        )?);
    }
    let projection =
        TableWriterInputProjection::try_new(projection_arena, exprs, Arc::clone(&expected_schema))
            .map_err(|error| {
                NativeFragmentDecodeError::inconsistent(
                    path.clone().field("output_exprs"),
                    format!("native node_id={node_id} table writer projection: {error}"),
                )
            })?;

    // The query and attempt come from the execution identity this fragment was
    // admitted under, never from the plan node: a plan carries no attempt, and
    // a replacement attempt must not inherit a predecessor's writer context.
    let execution_id = runtime.execution_id();
    let physical_template = TableWriterPhysicalContextTemplate::new(
        uuid_bytes(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        ),
        execution_id.attempt_id().get(),
        uuid_bytes(
            ctx.fragment_instance_id().get().high(),
            ctx.fragment_instance_id().get().low(),
        ),
        writer.writer_ordinal,
    );

    let (_, query_expire) = query_expire_durations(ctx.query_options());
    let request_context = ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        ctx.connector_cancellation()
            .map_err(|error| error.into_native(path.clone()))?,
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map(|context| context.with_storage_resolver(runtime.storage_resolver()))
    .map_err(|error| {
        NativeFragmentDecodeError::invalid_value(
            path.clone(),
            format!("native node_id={node_id} table writer request context: {error}"),
        )
    })?;

    let execution = Arc::new(ObservedConnectorWriteExecution::new(
        binding.execution(),
        execution_id,
        node_id,
    ));
    let fragment_encoder = Arc::new(RoleBoundCommitFragmentEncoder::new(
        binding.fragment_encoder(),
        execution_id,
        node_id,
    ));

    let lowered = TableWriterNode::try_new(
        Box::new(child.node),
        node_id,
        handle,
        target,
        execution,
        expected_schema,
        projection,
        physical_template,
        request_context,
        fragment_encoder,
    )
    .map_err(|error| {
        NativeFragmentDecodeError::inconsistent(
            path.clone(),
            format!("native node_id={node_id} table writer: {error}"),
        )
    })?;

    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::TableWriter(lowered),
        },
        layout: Layout::for_slots(WRITE_RELATION_SLOT_IDS),
        output_schema: writer_relation_chunk_schema(),
    })
}

/// Decode one n-ary `TableFinishNode`.
pub(super) fn lower_table_finish_node(
    node: &plan::DistributedNode,
    finish: &plan::TableFinishNode,
    path: FieldPath,
    children: Vec<DecodedNode>,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let node_id = node.node_id;
    let ordinals_path = path.clone().field("expected_target_ordinals");
    let mut expected = Vec::with_capacity(finish.expected_target_ordinals.len());
    for (index, ordinal) in finish.expected_target_ordinals.iter().enumerate() {
        expected.push(WriteTargetOrdinal::try_new(*ordinal).map_err(|error| {
            NativeFragmentDecodeError::out_of_range(
                ordinals_path.clone().index(index),
                format!("native node_id={node_id} table finish target ordinal: {error}"),
            )
        })?);
    }

    // Every input must already carry the writer relation. The finish node reads
    // its columns positionally, so a foreign input would otherwise only fail
    // once rows arrive.
    let relation_types = writer_relation_schema()
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect::<Vec<_>>();
    for (index, child) in children.iter().enumerate() {
        let slots = child.output_schema.slots();
        let matches = slots.len() == relation_types.len()
            && slots
                .iter()
                .zip(relation_types.iter())
                .all(|(slot, data_type)| slot.data_type() == data_type);
        if !matches {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("children").index(index),
                format!(
                    "native node_id={node_id} table finish input {index} does not carry the write relation"
                ),
            ));
        }
    }

    // The finish node and its writers are normally in different fragments, so
    // this only fires when a plan places them together. When it does, a writer
    // naming a target the finish node never expects is a self-contradictory
    // plan, not a runtime surprise.
    //
    // Membership is an exact set test: a query's expected set is the targets
    // *this query's* writers feed and need not be dense from zero, so comparing
    // against the highest ordinal would admit a target no writer here compiles.
    let sealed = expected.iter().copied().collect::<BTreeSet<_>>();
    if !sealed.is_empty() {
        let mut offender = None;
        for child in &node.children {
            collect_out_of_set_writer(child, &sealed, &mut offender);
        }
        if let Some((writer_node_id, ordinal)) = offender {
            return Err(NativeFragmentDecodeError::inconsistent(
                ordinals_path.clone(),
                format!(
                    "native node_id={writer_node_id} table writer names write target ordinal {ordinal} outside the finish node's sealed set of {} targets",
                    expected.len()
                ),
            ));
        }
    }

    let validator = Arc::new(RootCommitFragmentCarrierValidator::new(
        ctx.typed_scan_runtime()
            .map(|runtime| runtime.execution_id())
            .ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    path.clone(),
                    format!(
                        "native node_id={node_id} table finish requires a query-leased catalog runtime"
                    ),
                )
            })?,
        node_id,
    ));

    let inputs = children.into_iter().map(|child| child.node).collect();
    let lowered =
        TableFinishNode::try_new(inputs, node_id, expected, validator).map_err(|error| {
            NativeFragmentDecodeError::invalid_value(
                ordinals_path,
                format!("native node_id={node_id} table finish: {error}"),
            )
        })?;

    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::TableFinish(lowered),
        },
        layout: Layout::for_slots(WRITE_RELATION_SLOT_IDS),
        output_schema: root_relation_chunk_schema(),
    })
}

/// Record the first writer under `node` whose target ordinal is outside the
/// dense set bounded by `highest`.
fn collect_out_of_set_writer(
    node: &plan::DistributedNode,
    sealed: &BTreeSet<WriteTargetOrdinal>,
    offender: &mut Option<(i32, u32)>,
) {
    if offender.is_some() {
        return;
    }
    if let Some(plan::distributed_node::Payload::TableWriter(writer)) = node.payload.as_ref()
        && !WriteTargetOrdinal::try_new(writer.write_target_ordinal)
            .is_ok_and(|ordinal| sealed.contains(&ordinal))
    {
        *offender = Some((node.node_id, writer.write_target_ordinal));
        return;
    }
    for child in &node.children {
        collect_out_of_set_writer(child, sealed, offender);
    }
}

/// Validate the sealed input binding against the writer's real child.
fn validate_input_binding(
    input: Option<&plan::ConnectorWriteInputBinding>,
    child: &DecodedNode,
    node_id: i32,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    let input = input.ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone(),
            format!("native node_id={node_id} table writer requires its input binding"),
        )
    })?;
    let width = child.layout.order().len();
    match input.kind.as_ref() {
        None => Err(NativeFragmentDecodeError::missing(
            path.field("kind"),
            format!("native node_id={node_id} table writer input binding requires a kind"),
        )),
        Some(plan::connector_write_input_binding::Kind::RootOutputByOrdinal(true)) => Ok(()),
        Some(plan::connector_write_input_binding::Kind::RootOutputByOrdinal(false)) => {
            Err(NativeFragmentDecodeError::invalid_value(
                path.field("root_output_by_ordinal"),
                format!(
                    "native node_id={node_id} table writer root_output_by_ordinal marker must be true"
                ),
            ))
        }
        Some(plan::connector_write_input_binding::Kind::OutputOrdinals(ordinals)) => {
            let path = path.field("output_ordinals");
            if ordinals.values.is_empty() {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path,
                    format!(
                        "native node_id={node_id} table writer output_ordinals must not be empty"
                    ),
                ));
            }
            let mut seen = std::collections::BTreeSet::new();
            for (index, value) in ordinals.values.iter().enumerate() {
                let ordinal = usize::try_from(*value).map_err(|_| {
                    NativeFragmentDecodeError::out_of_range(
                        path.clone().index(index),
                        format!(
                            "native node_id={node_id} table writer output ordinal {value} is not addressable"
                        ),
                    )
                })?;
                if ordinal >= width {
                    return Err(NativeFragmentDecodeError::out_of_range(
                        path.clone().index(index),
                        format!(
                            "native node_id={node_id} table writer output ordinal {ordinal} is outside its input width {width}"
                        ),
                    ));
                }
                if !seen.insert(ordinal) {
                    return Err(NativeFragmentDecodeError::inconsistent(
                        path.clone().index(index),
                        format!(
                            "native node_id={node_id} table writer repeats output ordinal {ordinal}"
                        ),
                    ));
                }
            }
            Ok(())
        }
    }
}

/// The 16-byte form of a native `(high, low)` identity, in the same big-endian
/// halves its UUID rendering uses.
const fn uuid_bytes(high: i64, low: i64) -> [u8; 16] {
    let high = high.to_be_bytes();
    let low = low.to_be_bytes();
    [
        high[0], high[1], high[2], high[3], high[4], high[5], high[6], high[7], low[0], low[1],
        low[2], low[3], low[4], low[5], low[6], low[7],
    ]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_execution::exec::pipeline::operator_factory::OperatorFactory;
    use novarocks_execution::runtime::execution_runtime::{
        ExecutionRuntime, ExecutionRuntimeConfig, ExecutionSpillStorageConfig,
    };
    use novarocks_execution::runtime::runtime_state::RuntimeState;
    use novarocks_proto_codec::ProtocolErrorKind;
    use novarocks_proto_models::connector_write as write_dto;
    use novarocks_proto_models::plan;
    use novarocks_spi::connector::write_stack::{
        ConnectorOpenWriterRequest, ConnectorWriterPhysicalContext,
        MAX_CONNECTOR_WRITER_HANDLE_BYTES, WriteTargetOrdinal,
    };
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId, UniqueId};

    use super::super::tests::{column_ref, one_col_values_node_with, output_column};
    use super::super::{DecodedNode, NativePlanDecodeContext, decode_node};
    use crate::connector::write_test_support::{
        RecordingWriteExecution, TEST_WRITE_CATALOG, finish_node, iceberg_writer_handle,
        table_writer_payload, test_request_context, test_write_adapter, test_write_binding,
        test_write_catalog_handle, test_write_scan_runtime, wire_catalog_handle, writer_node,
    };
    use crate::fragment::decode::plan::error::NativeFragmentDecodeError;

    const CHILD_COLUMN_ID: u32 = 1;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(0x51, 0x52),
            AttemptId::new(3).expect("attempt"),
        )
        .expect("execution id")
    }

    fn fragment_instance_id() -> UniqueId {
        UniqueId::new(0x61, 0x62)
    }

    fn recording_execution() -> Arc<RecordingWriteExecution> {
        Arc::new(RecordingWriteExecution::new())
    }

    fn writer_runtime_state() -> RuntimeState {
        let runtime = Arc::new(
            ExecutionRuntime::new(ExecutionRuntimeConfig {
                driver_threads: 1,
                scan_threads: 1,
                scan_queue_capacity: 1,
                spill_io_threads: 1,
                spill_io_queue_capacity: 1,
                spill_storage: ExecutionSpillStorageConfig::default(),
                exchange_wait_ms: 120_000,
                exchange_io_threads: 1,
                exchange_io_max_inflight_bytes: 1024,
                exchange_max_transmit_batched_bytes: 1024,
                operator_buffer_chunks: 1,
                local_exchange_buffer_mem_limit_per_driver: 1024,
                local_exchange_max_buffered_rows: 1024,
                connector_io_tasks_per_scan_operator: 1,
                scan_submit_fail_max: 1,
                scan_submit_fail_timeout_ms: 1,
                runtime_filter_scan_wait_time_ms_override: None,
                runtime_filter_wait_timeout_ms_override: None,
                sink_io_worker_threads: 1,
                sink_io_max_blocking_threads: 1,
            })
            .expect("writer execution runtime"),
        );
        RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(runtime),
            None,
        )
    }

    fn write_decode_context(execution: Arc<RecordingWriteExecution>) -> NativePlanDecodeContext {
        NativePlanDecodeContext::default()
            .with_typed_scan_runtime(Some(test_write_scan_runtime(
                execution_id(),
                fragment_instance_id(),
                execution,
            )))
            .with_connector_cancellation(crate::connector::write_test_support::never_cancelled())
            .with_fragment_instance_id(fragment_instance_id())
    }

    fn writer_payload() -> plan::TableWriterNode {
        table_writer_payload(
            column_ref(CHILD_COLUMN_ID, DataType::Int64),
            vec![output_column(7, "id", DataType::Int64)],
        )
    }

    fn simple_writer_plan(writer: plan::TableWriterNode) -> plan::DistributedNode {
        writer_node(
            30,
            writer,
            vec![one_col_values_node_with(10, CHILD_COLUMN_ID, "id", 42)],
        )
    }

    fn decode_with(
        node: &plan::DistributedNode,
        execution: Arc<RecordingWriteExecution>,
    ) -> Result<DecodedNode, NativeFragmentDecodeError> {
        let mut arena = ExprArena::default();
        decode_node(node, &mut arena, &write_decode_context(execution))
    }

    fn decode_error(node: &plan::DistributedNode) -> NativeFragmentDecodeError {
        decode_with(node, recording_execution()).expect_err("invalid write node must fail")
    }

    fn assert_protocol(
        error: &NativeFragmentDecodeError,
        expected_path: &str,
        expected_kind: ProtocolErrorKind,
    ) {
        let protocol = error
            .protocol()
            .unwrap_or_else(|| panic!("expected a protocol error, got {error}"));
        assert_eq!(protocol.path().to_string(), expected_path);
        assert_eq!(protocol.kind(), expected_kind);
    }

    // ----------------------------------------------------------------- positives

    #[test]
    fn a_table_writer_decodes_and_binds_to_its_query_leased_write_runtime() {
        let decoded = decode_with(&simple_writer_plan(writer_payload()), recording_execution())
            .expect("table writer decodes");
        let ExecNodeKind::TableWriter(writer) = &decoded.node.kind else {
            panic!("expected a table writer, got {:?}", decoded.node.kind);
        };
        assert_eq!(writer.node_id, 30);
        assert_eq!(writer.target().get(), 0);
        assert_eq!(
            writer.execution().catalog_handle(),
            &test_write_catalog_handle()
        );
        assert_eq!(
            writer.handle().binding().catalog_handle(),
            &test_write_catalog_handle()
        );
        assert_eq!(
            decoded.output_schema,
            novarocks_execution::exec::node::table_write_relation::writer_relation_chunk_schema()
        );
    }

    #[test]
    fn the_writer_physical_context_comes_from_the_admitted_attempt_not_the_plan() {
        let decoded = decode_with(&simple_writer_plan(writer_payload()), recording_execution())
            .expect("table writer decodes");
        let ExecNodeKind::TableWriter(writer) = &decoded.node.kind else {
            panic!("expected a table writer");
        };
        let physical = writer.physical_template().for_driver(5);
        assert_eq!(
            physical.execution_query_id(),
            super::uuid_bytes(
                execution_id().query_id().high(),
                execution_id().query_id().low()
            )
        );
        assert_eq!(
            physical.execution_attempt_id(),
            execution_id().attempt_id().get()
        );
        assert_eq!(
            physical.fragment_instance_id(),
            super::uuid_bytes(fragment_instance_id().high(), fragment_instance_id().low())
        );
        assert_eq!(physical.driver_id(), 5);
        assert_eq!(physical.writer_ordinal(), 0);
    }

    #[test]
    fn every_driver_opens_its_own_writer_with_its_own_driver_id() {
        let execution = recording_execution();
        let decoded = decode_with(
            &simple_writer_plan(writer_payload()),
            Arc::clone(&execution),
        )
        .expect("table writer decodes");
        let ExecNodeKind::TableWriter(writer) = &decoded.node.kind else {
            panic!("expected a table writer");
        };
        let factory = novarocks_execution::exec::operators::TableWriterOperatorFactory::new(writer);
        assert!(!factory.is_sink(), "a table writer is not a terminal sink");
        let runtime_state = writer_runtime_state();
        let mut operators = Vec::new();
        for driver_id in 0..4 {
            let mut operator = factory.create(4, driver_id);
            operator
                .bind_runtime_state(&runtime_state)
                .expect("bind writer actor");
            operators.push(operator);
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while execution.opened().len() != 4 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            execution.opened(),
            vec![(0, 0, 0), (1, 0, 0), (2, 0, 0), (3, 0, 0)]
        );
    }

    #[test]
    fn a_table_finish_decodes_over_several_writer_inputs() {
        let node = finish_node(
            40,
            vec![0],
            vec![
                simple_writer_plan(writer_payload()),
                simple_writer_plan(writer_payload()),
            ],
        );
        let decoded = decode_with(&node, recording_execution()).expect("table finish decodes");
        let ExecNodeKind::TableFinish(finish) = &decoded.node.kind else {
            panic!("expected a table finish, got {:?}", decoded.node.kind);
        };
        assert_eq!(finish.inputs.len(), 2);
        assert_eq!(finish.expected_targets().len(), 1);
        assert!(finish.accepts_target(WriteTargetOrdinal::try_new(0).expect("bounded ordinal")));
        assert!(!finish.accepts_target(WriteTargetOrdinal::try_new(1).expect("bounded ordinal")));
        assert_eq!(
            decoded.output_schema,
            novarocks_execution::exec::node::table_write_relation::root_relation_chunk_schema()
        );
    }

    // ----------------------------------------------------------------- negatives

    #[test]
    fn a_foreign_catalog_handle_is_refused_at_its_exact_path() {
        let mut writer = writer_payload();
        writer.catalog_handle = Some(wire_catalog_handle("other_catalog"));
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.catalog_handle",
            ProtocolErrorKind::InvalidValue,
        );
        assert!(error.contains("query-leased"), "unexpected detail: {error}");
    }

    #[test]
    fn a_writer_handle_over_the_single_handle_cap_is_refused_at_its_exact_path() {
        let mut writer = writer_payload();
        writer.handle = Some(iceberg_writer_handle(
            "a".repeat(MAX_CONNECTOR_WRITER_HANDLE_BYTES + 1),
        ));
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.handle",
            ProtocolErrorKind::OutOfRange,
        );
        assert!(
            error.contains("frozen single-handle budget"),
            "unexpected detail: {error}"
        );
    }

    #[test]
    fn a_writer_handle_without_a_provider_variant_is_refused_at_its_exact_path() {
        let mut writer = writer_payload();
        writer.handle = Some(write_dto::ConnectorWriterHandle { handle: None });
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.handle",
            ProtocolErrorKind::MissingField,
        );
    }

    #[test]
    fn a_structurally_invalid_writer_handle_is_refused_at_its_exact_provider_field() {
        let mut writer = writer_payload();
        let mut carrier = iceberg_writer_handle("9c2f1f66".to_string());
        if let Some(write_dto::connector_writer_handle::Handle::Iceberg(iceberg)) =
            carrier.handle.as_mut()
        {
            iceberg.table.as_mut().expect("table facts").format_version = 9;
        }
        writer.handle = Some(carrier);
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.handle.iceberg.table.format_version",
            ProtocolErrorKind::OutOfRange,
        );
    }

    #[test]
    fn a_writer_target_ordinal_beyond_the_frozen_bound_is_refused() {
        let mut writer = writer_payload();
        writer.write_target_ordinal = u32::MAX;
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.write_target_ordinal",
            ProtocolErrorKind::OutOfRange,
        );
    }

    #[test]
    fn a_writer_input_ordinal_outside_its_input_width_is_refused() {
        let mut writer = writer_payload();
        writer.input = Some(plan::ConnectorWriteInputBinding {
            kind: Some(plan::connector_write_input_binding::Kind::OutputOrdinals(
                plan::UInt64List { values: vec![7] },
            )),
        });
        let error = decode_error(&simple_writer_plan(writer));
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.input.output_ordinals[0]",
            ProtocolErrorKind::OutOfRange,
        );
    }

    #[test]
    fn a_finish_node_with_repeated_expected_ordinals_is_refused() {
        let mut writer = writer_payload();
        writer.write_target_ordinal = 1;
        let node = finish_node(40, vec![1, 1], vec![simple_writer_plan(writer)]);
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.expected_target_ordinals",
            ProtocolErrorKind::InvalidValue,
        );
        assert!(error.contains("repeats"), "unexpected detail: {error}");
    }

    /// A query set is the targets *this query's* writers feed, so it need not
    /// be dense from zero: a copy-on-write statement compiles one writer per
    /// query, at that group's own ordinal. Denseness stays a property of the
    /// session's sealed set, checked where the session is sealed.
    #[test]
    fn a_finish_node_with_a_single_non_zero_expected_ordinal_decodes() {
        let mut writer = writer_payload();
        writer.write_target_ordinal = 2;
        let node = finish_node(40, vec![2], vec![simple_writer_plan(writer)]);
        let decoded = decode_with(&node, recording_execution()).expect("table finish decodes");
        let ExecNodeKind::TableFinish(finish) = &decoded.node.kind else {
            panic!("expected a table finish, got {:?}", decoded.node.kind);
        };
        assert!(finish.accepts_target(WriteTargetOrdinal::try_new(2).expect("bounded ordinal")));
        // Exact membership, not a bound: ordinal 0 is below the highest
        // expected one and is still not part of this query's set.
        assert!(!finish.accepts_target(WriteTargetOrdinal::try_new(0).expect("bounded ordinal")));
    }

    /// A writer below the highest expected ordinal but outside the set is still
    /// a self-contradictory plan.
    #[test]
    fn a_writer_below_the_highest_expected_ordinal_but_outside_the_set_is_refused() {
        let mut writer = writer_payload();
        writer.write_target_ordinal = 0;
        let node = finish_node(40, vec![2], vec![simple_writer_plan(writer)]);
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.expected_target_ordinals",
            ProtocolErrorKind::InconsistentFields,
        );
        assert!(error.contains("outside"), "unexpected detail: {error}");
    }

    #[test]
    fn a_finish_node_with_no_expected_ordinals_is_refused() {
        let node = finish_node(40, Vec::new(), vec![simple_writer_plan(writer_payload())]);
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.expected_target_ordinals",
            ProtocolErrorKind::InvalidValue,
        );
    }

    #[test]
    fn a_writer_target_outside_the_finish_nodes_sealed_set_is_refused() {
        let mut writer = writer_payload();
        writer.write_target_ordinal = 3;
        let node = finish_node(40, vec![0], vec![simple_writer_plan(writer)]);
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.expected_target_ordinals",
            ProtocolErrorKind::InconsistentFields,
        );
        assert!(error.contains("outside"), "unexpected detail: {error}");
    }

    #[test]
    fn a_limit_on_a_write_dataflow_node_is_refused() {
        // A limit would truncate the write relation and drop commit fragments.
        let mut node = simple_writer_plan(writer_payload());
        node.limit = 1;
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.limit",
            ProtocolErrorKind::InconsistentFields,
        );

        let mut node = finish_node(40, vec![0], vec![simple_writer_plan(writer_payload())]);
        node.limit = 1;
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.limit",
            ProtocolErrorKind::InconsistentFields,
        );
    }

    #[test]
    fn a_finish_input_that_is_not_a_write_relation_is_refused() {
        let node = finish_node(
            40,
            vec![0],
            vec![one_col_values_node_with(10, CHILD_COLUMN_ID, "id", 42)],
        );
        let error = decode_error(&node);
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_finish.children[0]",
            ProtocolErrorKind::InconsistentFields,
        );
    }

    #[test]
    fn a_write_node_without_a_query_leased_runtime_is_refused() {
        let mut arena = ExprArena::default();
        let error = decode_node(
            &simple_writer_plan(writer_payload()),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .expect_err("a write node needs a query-leased runtime");
        assert_protocol(
            &error,
            "plan_fragment.root.payload.table_writer.catalog_handle",
            ProtocolErrorKind::MissingField,
        );
    }

    // -------------------------------------------------- structural write limits

    #[tokio::test]
    async fn the_backend_write_binding_can_only_open_writers_and_move_carriers() {
        // The proof is the binding's own shape: the single member that reaches a
        // provider is `ConnectorWriteExecution`, whose only method is
        // `open_writer`. There is no commit handle, no control binding, and no
        // metadata mutation reachable from here, and the two codec facets it
        // holds run in opposite directions from the frontend's.
        let execution = recording_execution();
        let binding = test_write_binding(Arc::clone(&execution));
        let write_execution = binding.execution();
        assert_eq!(
            write_execution.catalog_handle(),
            &test_write_catalog_handle()
        );
        let mut writer = write_execution
            .open_writer(ConnectorOpenWriterRequest {
                handle: test_write_adapter().wrap_writer_handle(
                    match iceberg_writer_handle("t".to_string()).handle {
                        Some(write_dto::connector_writer_handle::Handle::Iceberg(iceberg)) => {
                            iceberg
                        }
                        None => unreachable!("the fixture always carries a variant"),
                    },
                ),
                target: WriteTargetOrdinal::try_new(0).expect("ordinal"),
                expected_schema: Arc::new(arrow::datatypes::Schema::empty()),
                physical: ConnectorWriterPhysicalContext::new([0; 16], 1, [0; 16], 0, 0),
                context: test_request_context(),
            })
            .await
            .expect("open writer");
        // A finished writer only ever yields commit fragments, never a commit.
        assert!(writer.finish().await.expect("finish").is_empty());
        assert_eq!(binding.handle_decoder().owner(), TEST_WRITE_CATALOG);
        assert_eq!(binding.fragment_encoder().owner(), TEST_WRITE_CATALOG);
    }
}
