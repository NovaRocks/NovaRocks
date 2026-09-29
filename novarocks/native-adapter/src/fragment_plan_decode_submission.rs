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

//! Fragment-owned native fragment submission assembly.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::fragment::program::{
    FragmentContractVersion, FragmentProgram, FragmentProgramOptions,
};
use novarocks_execution::exec::node::ExternalSinkRequirement;
use novarocks_execution::runtime::fragment::{
    FragmentInstanceSpec, FragmentRuntimeOptions, FragmentSubmission, ScanAssignments,
};
use novarocks_execution_contract::task_execution::descriptor::ExchangeTopology;
use novarocks_local_program::StaticSinkProgram;
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::plan;
use novarocks_spi::connector::ConnectorStopView;

use crate::fragment_validation::{validate_fragment_expressions, validate_node_required_fields};

use crate::fragment_decode_context::NativePlanDecodeContext;
use crate::fragment_error::NativeFragmentDecodeError;
use crate::fragment_instance::{NativeFragmentInstanceInput, task_scan_ranges_path};
use crate::fragment_layout::decode_exchange_contracts;
use crate::fragment_plan_decode::decode_node_with_runtime_filters;
use crate::fragment_runtime_filter::decode_runtime_filter_contract;
use crate::fragment_runtime_filter_binding::NativeRuntimeFilterDecodeLedger;
use crate::fragment_sink::decode_fragment_sink_program;
use crate::fragment_submission::{
    decode_fragment_sink_assignment, decode_scan_source_contracts, require_root, require_sink,
    validate_scan_range_nodes,
};

pub(crate) struct DecodedNativeFragment {
    submission: FragmentSubmission,
    backend_num: i32,
}

impl DecodedNativeFragment {
    pub(crate) fn into_parts(self) -> (FragmentSubmission, i32) {
        (self.submission, self.backend_num)
    }
}

/// Decodes one fragment's static plan against one instance of it.
///
/// Every cross-check between the plan and the instance runs here, before
/// anything is prepared: the scan nodes the instance assigns ranges to must be
/// scan nodes of the plan, and each static sink branch must be bound to the
/// outbound edge serving it. A refusal leaves no runtime side effect behind.
pub(crate) fn decode_fragment_submission(
    fragment: &plan::PlanFragment,
    instance: NativeFragmentInstanceInput,
    topology: &ExchangeTopology,
    connector_stop: ConnectorStopView,
    exchange_wait: Duration,
    typed_scan_runtime: Option<novarocks_worker::TypedScanRuntime>,
    function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
) -> Result<DecodedNativeFragment, NativeFragmentDecodeError> {
    let root_path = FieldPath::root("plan_fragment").field("root");
    let root = require_root(fragment).map_err(NativeFragmentDecodeError::from)?;
    validate_node_required_fields(root, root_path.clone())
        .map_err(NativeFragmentDecodeError::from)?;
    let sink = require_sink(fragment).map_err(NativeFragmentDecodeError::from)?;
    if sink.kind.is_none() {
        return Err(NativeFragmentDecodeError::missing(
            FieldPath::root("plan_fragment").field("sink").field("kind"),
            "native DataSink requires kind",
        ));
    }
    validate_fragment_expressions(fragment).map_err(NativeFragmentDecodeError::from)?;

    let scan_sources = decode_scan_source_contracts(root, root_path.clone())
        .map_err(NativeFragmentDecodeError::from)?;
    validate_scan_range_nodes(
        &scan_sources,
        &instance.raw_scan_ranges,
        task_scan_ranges_path(),
    )
    .map_err(NativeFragmentDecodeError::from)?;
    let sink_assignment = decode_fragment_sink_assignment(
        sink,
        &instance.sink_edge_ids,
        instance.fragment_instance_id.get(),
        topology,
    )
    .map_err(NativeFragmentDecodeError::from)?;

    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(instance.query_options.allow_throw_exception());
    let context = NativePlanDecodeContext::from_parts(
        instance.exchange_inputs.clone(),
        instance.raw_scan_ranges,
        instance.query_options.clone(),
        connector_stop,
        instance.query_id,
        instance.fragment_instance_id,
        exchange_wait,
    )
    .with_typed_scan_runtime(typed_scan_runtime)
    .with_function_catalog(function_catalog);
    let mut ledger = NativeRuntimeFilterDecodeLedger::decode(
        fragment.fragment_id,
        fragment.runtime_filter_bindings.as_ref(),
    )?;
    let decoded_root = decode_node_with_runtime_filters(root, &mut arena, &context, &mut ledger)?;
    ledger.finish()?;
    let scan_assignments = ScanAssignments::try_new(context.take_captured_scan_ranges())
        .map_err(NativeFragmentDecodeError::Binding)?;
    let sink_program = decode_fragment_sink_program(fragment, &decoded_root.layout)?;
    let static_sink = sink_program.into_static().map_err(|error| {
        NativeFragmentDecodeError::invalid_value(
            FieldPath::root("plan_fragment").field("sink"),
            error,
        )
    })?;
    let sink_requirements = match &static_sink {
        StaticSinkProgram::Result => vec![ExternalSinkRequirement::Result],
        StaticSinkProgram::Noop => Vec::new(),
        StaticSinkProgram::DataStream { .. }
        | StaticSinkProgram::MultiCastDataStream { .. }
        | StaticSinkProgram::SplitDataStream { .. } => (0..static_sink.branches().len())
            .map(|branch| ExternalSinkRequirement::ExchangeOutput { branch })
            .collect(),
    };
    let plan = novarocks_execution::exec::node::ExecPlanBuilder::new(arena, decoded_root.node)
        .finish()
        .map_err(NativeFragmentDecodeError::from)?;
    let logical_dop = i32::try_from(instance.pipeline_dop.get()).map_err(|_| {
        NativeFragmentDecodeError::invalid_value(
            FieldPath::root("plan_fragment").field("pipeline_dop"),
            "pipeline DOP exceeds the runtime representation",
        )
    })?;
    let effective_dop = novarocks_execution::runtime::exec_env::calc_pipeline_dop(logical_dop);
    let effective_dop = usize::try_from(effective_dop)
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| {
            NativeFragmentDecodeError::invalid_value(
                FieldPath::root("plan_fragment").field("pipeline_dop"),
                "effective pipeline DOP must be positive",
            )
        })?;
    let profile = plan
        .local_compile_profile(effective_dop, None)
        .map_err(|error| {
            NativeFragmentDecodeError::invalid_value(
                FieldPath::root("plan_fragment").field("root"),
                error,
            )
        })?;
    let (local_program, runtime_bindings) = plan
        .into_local_program_and_bindings(
            profile,
            context.take_captured_static_scans(),
            sink_requirements,
            static_sink,
        )
        .map_err(|error| {
            NativeFragmentDecodeError::invalid_value(
                FieldPath::root("plan_fragment").field("root"),
                error,
            )
        })?;
    let program = FragmentProgram::try_new(
        Arc::new(local_program),
        FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
        scan_sources,
        decode_exchange_contracts(root, root_path).map_err(NativeFragmentDecodeError::from)?,
        decode_runtime_filter_contract(fragment).map_err(NativeFragmentDecodeError::from)?,
    )
    .map_err(NativeFragmentDecodeError::Binding)?;
    let backend_num = instance.backend_num.get();
    let fragment_instance = FragmentInstanceSpec::new_native(
        FragmentContractVersion::CURRENT,
        instance.query_id,
        instance.fragment_instance_id,
        scan_assignments,
        instance.exchange_inputs,
        sink_assignment,
        FragmentRuntimeOptions::new(instance.query_options, instance.typed_result_sink),
        instance.pipeline_dop,
        instance.backend_num,
    );
    let submission =
        FragmentSubmission::try_new(Arc::new(program), runtime_bindings, fragment_instance)
            .map_err(NativeFragmentDecodeError::Binding)?;
    Ok(DecodedNativeFragment {
        submission,
        backend_num,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::datatypes::DataType;
    use novarocks_execution::exec::fragment::program::{FragmentNodeId, FragmentSinkKind};
    use novarocks_execution::runtime::fragment::{
        BackendNum, ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
    };
    use novarocks_execution::runtime::query_options::QueryOptions;
    use novarocks_execution_contract::task_execution::descriptor::{
        DataStreamPartitionType, ExchangeDestination, ExchangeEdge, ExchangeTopology,
        RuntimeEndpoint,
    };
    use novarocks_execution_contract::task_execution::domain::ExchangeEdgeId;
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_local_program::ProgramNodeKind;
    use novarocks_proto_codec::ProtocolErrorKind;
    use novarocks_proto_codec::lifecycle::{AttemptId, QueryExecutionId};
    use novarocks_proto_models::{common, expr, plan};
    use novarocks_types::{
        UniqueId,
        identity::{BackendProcessId, QueryId, StageId, TaskId},
    };

    use super::{DecodedNativeFragment, NativeFragmentDecodeError, decode_fragment_submission};
    use crate::fragment_instance::NativeFragmentInstanceInput;
    use novarocks_plan_codec::encode_native_type as encode_type;

    fn output_column(column_id: u32) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: "value".to_string(),
            r#type: Some(encode_type(&DataType::Int32).expect("encode type")),
            nullable: false,
            is_internal: false,
        }
    }

    fn column_ref(column_id: u32) -> expr::Expr {
        expr::Expr {
            r#type: Some(encode_type(&DataType::Int32).expect("encode type")),
            nullable: false,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            })),
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

    /// One kernel instance of the fixture fragment, stated directly: these
    /// cases exercise the plan decoder, not how a creation projects its
    /// instance.
    fn instance(query: UniqueId, finst: UniqueId) -> NativeFragmentInstanceInput {
        NativeFragmentInstanceInput {
            query_id: QueryId::new(query.high(), query.low()),
            fragment_instance_id: FragmentInstanceId::new(finst),
            backend_num: BackendNum::try_new(3).expect("backend num"),
            query_options: QueryOptions {
                pipeline_dop: Some(1),
                ..QueryOptions::default()
            },
            pipeline_dop: NonZeroUsize::new(1).expect("pipeline dop"),
            raw_scan_ranges: BTreeMap::new(),
            exchange_inputs: ExchangeInputAssignments::new(BTreeMap::new()),
            typed_result_sink: false,
            sink_edge_ids: Vec::new(),
        }
    }

    fn decode(
        fragment: &plan::PlanFragment,
        instance: NativeFragmentInstanceInput,
    ) -> Result<DecodedNativeFragment, NativeFragmentDecodeError> {
        decode_with_topology(fragment, instance, &ExchangeTopology::default())
    }

    fn decode_with_topology(
        fragment: &plan::PlanFragment,
        instance: NativeFragmentInstanceInput,
        topology: &ExchangeTopology,
    ) -> Result<DecodedNativeFragment, NativeFragmentDecodeError> {
        decode_fragment_submission(
            fragment,
            instance,
            topology,
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            Duration::from_secs(1),
            None,
            Arc::new(
                novarocks_sql::compiler::build_builtin_engine_function_catalog()
                    .expect("builtin function catalog"),
            ),
        )
    }

    fn expect_decode_error(
        result: Result<DecodedNativeFragment, NativeFragmentDecodeError>,
        message: &str,
    ) -> NativeFragmentDecodeError {
        match result {
            Ok(_) => panic!("{message}"),
            Err(error) => error,
        }
    }

    #[test]
    fn values_noop_decodes_to_validated_submission() {
        let query = UniqueId::new(11, 12);
        let finst = UniqueId::new(21, 22);

        let decoded = decode(&values_noop_fragment(), instance(query, finst))
            .expect("decode values/noop submission");
        let (submission, backend_num) = decoded.into_parts();

        assert_eq!(
            submission.instance().query_id(),
            novarocks_types::QueryId::new(11, 12)
        );
        assert_eq!(submission.instance().fragment_instance_id().get(), finst);
        assert_eq!(submission.instance().backend_num().get(), 3);
        assert_eq!(backend_num, 3);
        assert_eq!(submission.program().sink_kind(), FragmentSinkKind::Noop);
        let local = submission.program().local_program();
        assert!(matches!(
            local.nodes()[local.root().index()].kind(),
            ProgramNodeKind::Values { .. }
        ));
    }

    #[test]
    fn data_stream_hash_sink_resolves_partition_expression_from_root_layout() {
        let mut fragment = values_noop_fragment();
        fragment.sink = Some(plan::DataSink {
            kind: Some(plan::data_sink::Kind::DataStream(plan::DataStreamSink {
                dest_node_id: 17,
                output_partition: Some(plan::DataPartition {
                    kind: plan::PartitionKind::Hash as i32,
                    exprs: vec![column_ref(1)],
                }),
                output_columns: vec![1],
                ..Default::default()
            })),
        });

        let query = QueryId::new(23, 24);
        let destination_task = TaskIdentity::new(
            QueryExecutionId::new(query, AttemptId::new(1).expect("nonzero attempt"))
                .expect("execution id"),
            StageId::new(2).expect("stage"),
            TaskId::new(1).expect("task"),
            BackendProcessId::new_v7(),
        );
        let topology = ExchangeTopology::try_new(
            vec![
                ExchangeEdge::try_new(
                    ExchangeEdgeId::new(1).expect("edge"),
                    novarocks_execution_contract::FragmentNodeId::new(17),
                    DataStreamPartitionType::HashPartitioned,
                    vec![ExchangeDestination::new(
                        destination_task,
                        UniqueId::new(27, 28),
                        RuntimeEndpoint::new("be.local", 8060).expect("endpoint"),
                        novarocks_execution_contract::FragmentNodeId::new(17),
                    )],
                    0,
                    NonZeroU32::new(1).expect("sender count"),
                )
                .expect("edge"),
            ],
            vec![],
        )
        .expect("topology");
        let mut fixture = instance(UniqueId::new(23, 24), UniqueId::new(25, 26));
        fixture.sink_edge_ids = vec![1];
        let decoded = decode_with_topology(&fragment, fixture, &topology)
            .expect("hash sink expression must resolve through the decoded root layout");
        let (submission, _) = decoded.into_parts();
        assert_eq!(
            submission.program().sink_kind(),
            FragmentSinkKind::DataStream
        );
    }

    #[test]
    fn missing_root_fails_before_missing_sink() {
        let error = expect_decode_error(
            decode(
                &plan::PlanFragment::default(),
                instance(UniqueId::new(31, 32), UniqueId::new(41, 42)),
            ),
            "missing root must fail",
        );

        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.path().to_string(), "plan_fragment.root");
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(protocol.detail(), "native PlanFragment requires root");
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

        let error = expect_decode_error(
            decode(
                &fragment,
                instance(UniqueId::new(91, 92), UniqueId::new(101, 102)),
            ),
            "missing child payload must fail",
        );
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.children[0].payload"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            protocol.detail(),
            "native DistributedNode 0 requires payload"
        );
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
            values: vec![expr::Expr::default()],
        });

        let error = expect_decode_error(
            decode(
                &fragment,
                instance(UniqueId::new(111, 112), UniqueId::new(121, 122)),
            ),
            "missing expression type must fail",
        );
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.values.rows[0].values[0].type"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(protocol.detail(), "native Expr requires type");
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
            values: vec![expr::Expr {
                r#type: Some(encode_type(&DataType::Boolean).expect("encode type")),
                nullable: false,
                kind: Some(expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                    decimal_overflow_policy: expr::DecimalOverflowPolicy::OutputNull as i32,
                    op: expr::BinaryOp::Eq as i32,
                    left: None,
                    right: None,
                }))),
            }],
        });

        let error = expect_decode_error(
            decode(
                &fragment,
                instance(UniqueId::new(211, 212), UniqueId::new(221, 222)),
            ),
            "missing binary left operand must fail",
        );
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.values.rows[0].values[0].binary_op.left"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(protocol.detail(), "native Expr requires left");
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

        let error = expect_decode_error(
            decode(
                &fragment,
                instance(UniqueId::new(231, 232), UniqueId::new(241, 242)),
            ),
            "missing scan table must fail",
        );
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.scan.table"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            protocol.detail(),
            "native ScanNode node_id=11 requires table"
        );
    }

    #[test]
    fn false_noop_marker_uses_sink_oneof_path() {
        let mut fragment = values_noop_fragment();
        fragment.sink = Some(plan::DataSink {
            kind: Some(plan::data_sink::Kind::Noop(false)),
        });

        let error = expect_decode_error(
            decode(
                &fragment,
                instance(UniqueId::new(251, 252), UniqueId::new(261, 262)),
            ),
            "false noop marker must fail",
        );
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.path().to_string(), "plan_fragment.sink.noop");
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
        assert_eq!(protocol.detail(), "native NOOP sink marker must be true");
    }

    #[test]
    fn submission_binding_errors_keep_binding_stage_identity() {
        let mut fixture = instance(UniqueId::new(151, 152), UniqueId::new(161, 162));
        fixture.exchange_inputs = ExchangeInputAssignments::new(BTreeMap::from([(
            FragmentNodeId::new(99),
            ExchangeInputAssignment::new(NonZeroUsize::new(1).expect("sender count")),
        )]));

        let error = expect_decode_error(
            decode(&values_noop_fragment(), fixture),
            "unknown exchange assignment must fail binding",
        );
        let NativeFragmentDecodeError::Binding(binding) = error else {
            panic!("expected binding error stage");
        };
        assert_eq!(
            binding.target(),
            novarocks_execution::exec::fragment::error::FragmentBindingTarget::ExchangeNode(99)
        );
    }

    #[test]
    fn malformed_submission_has_zero_runtime_side_effects() {
        let error = expect_decode_error(
            decode(
                &plan::PlanFragment::default(),
                instance(UniqueId::new(271, 272), UniqueId::new(281, 282)),
            ),
            "malformed submission must fail before runtime dependencies are observed",
        );
        assert_eq!(
            error.protocol().expect("protocol error").path().to_string(),
            "plan_fragment.root"
        );
    }
}
