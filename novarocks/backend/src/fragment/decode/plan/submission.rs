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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::connector::ConnectorRegistry;
use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::fragment::program::{
    FragmentContractVersion, FragmentProgramOptions, FragmentSinkSpec, ScanSourceContract,
};
use novarocks_execution::runtime::fragment::{
    FragmentInstanceSpec, FragmentRuntimeOptions, FragmentSubmission, ScanAssignments,
};
use novarocks_proto::FieldPath;
use novarocks_proto::lifecycle::ScanRangeParams;
use novarocks_proto::{novarocks as proto, plan};
use novarocks_spi::connector::{ConnectorCancellation, ConnectorExecutionResolver};

use crate::fragment::decode::envelope::{require_root, require_sink};
use crate::fragment::decode::exchange::decode_exchange_contracts;
use crate::fragment::decode::instance::NativeFragmentInstanceInput;
use crate::fragment::decode::runtime_filter::decode_runtime_filter_contract;
use crate::fragment::decode::scan_contract::decode_scan_source_contracts;
use crate::fragment::decode::sink_assignment::decode_fragment_sink_assignment;
use crate::fragment::decode::submission_validation::{
    validate_fragment_expressions, validate_node_required_fields,
};

use super::context::NativePlanDecodeContext;
use super::error::NativeFragmentDecodeError;
use super::node::decode_node_with_runtime_filters;
use super::runtime_filter_binding::NativeRuntimeFilterDecodeLedger;
use super::sink::decode_fragment_sink_program_with_context;

pub(crate) struct DecodedNativeFragment {
    submission: FragmentSubmission,
    backend_num: i32,
}

impl DecodedNativeFragment {
    pub(crate) fn into_parts(self) -> (FragmentSubmission, i32) {
        (self.submission, self.backend_num)
    }
}

pub(crate) fn decode_fragment_submission(
    fragment: &plan::PlanFragment,
    instance: NativeFragmentInstanceInput,
    instance_params: &proto::InstanceParams,
    connectors: Arc<ConnectorRegistry>,
    execution_resolver: Arc<dyn ConnectorExecutionResolver>,
    connector_cancellation: Arc<dyn ConnectorCancellation>,
    exchange_wait: Duration,
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
    validate_raw_scan_range_nodes(
        &scan_sources,
        &instance.raw_scan_ranges,
        FieldPath::root("instance_params").field("per_node_scan_ranges"),
    )?;
    let sink_assignment = decode_fragment_sink_assignment(sink, instance_params)
        .map_err(NativeFragmentDecodeError::from)?;

    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(instance.query_options.allow_throw_exception());
    let context = NativePlanDecodeContext::from_parts(
        instance.exchange_inputs.clone(),
        instance.raw_scan_ranges,
        instance.query_options.clone(),
        connectors,
        execution_resolver,
        connector_cancellation,
        instance.query_id,
        instance.fragment_instance_id,
        exchange_wait,
    );
    let mut ledger = NativeRuntimeFilterDecodeLedger::decode(
        fragment.fragment_id,
        fragment.runtime_filter_bindings.as_ref(),
    )?;
    let decoded_root = decode_node_with_runtime_filters(root, &mut arena, &context, &mut ledger)?;
    ledger.finish()?;
    let scan_assignments = ScanAssignments::try_new(context.take_captured_scan_ranges())
        .map_err(NativeFragmentDecodeError::Binding)?;
    let sink_program =
        decode_fragment_sink_program_with_context(fragment, &decoded_root.layout, Some(&context))?;
    let sink_spec =
        FragmentSinkSpec::try_new(sink_program).map_err(NativeFragmentDecodeError::Binding)?;
    let plan = novarocks_execution::exec::node::ExecPlanBuilder::new(arena, decoded_root.node)
        .finish()
        .map_err(NativeFragmentDecodeError::from)?;
    let program = novarocks_execution::exec::fragment::program::FragmentProgramBuilder::new(
        plan,
        sink_spec,
        FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
    )
    .scan_sources(scan_sources)
    .exchange_inputs(
        decode_exchange_contracts(root, root_path).map_err(NativeFragmentDecodeError::from)?,
    )
    .runtime_filters(
        decode_runtime_filter_contract(fragment).map_err(NativeFragmentDecodeError::from)?,
    )
    .finish()
    .map_err(NativeFragmentDecodeError::from)?;
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
    let submission = FragmentSubmission::try_new(Arc::new(program), fragment_instance)
        .map_err(NativeFragmentDecodeError::Binding)?;
    Ok(DecodedNativeFragment {
        submission,
        backend_num,
    })
}

fn validate_raw_scan_range_nodes(
    contracts: &BTreeMap<
        novarocks_execution::exec::fragment::program::FragmentNodeId,
        ScanSourceContract,
    >,
    raw_ranges: &BTreeMap<
        novarocks_execution::exec::fragment::program::FragmentNodeId,
        Vec<ScanRangeParams>,
    >,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::connector::ConnectorRegistry;
    use arrow::datatypes::DataType;
    use novarocks_execution::exec::fragment::program::FragmentSinkKind;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_proto::ProtocolErrorKind;
    use novarocks_proto::lifecycle::{AttemptId, QueryExecutionId};
    use novarocks_proto::{common, expr, novarocks as proto, plan};
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorError, ConnectorErrorKind, ConnectorExecutionBinding,
        ConnectorExecutionBindingKey, ConnectorExecutionResolver,
    };
    use novarocks_types::UniqueId;

    use super::{DecodedNativeFragment, NativeFragmentDecodeError, decode_fragment_submission};
    use crate::fragment::decode::instance::decode_instance_params;
    use crate::fragment::decode::request::NativeFragmentRequest;
    use crate::fragment::decode::type_decode::encode_type;

    struct NeverResolved;

    impl ConnectorExecutionResolver for NeverResolved {
        fn resolve(
            &self,
            _key: &ConnectorExecutionBindingKey,
        ) -> Result<Arc<ConnectorExecutionBinding>, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "test resolver must not be used for malformed submissions",
            ))
        }
    }

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

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

    fn instance_params(query: UniqueId, finst: UniqueId) -> proto::InstanceParams {
        proto::InstanceParams {
            query_id: Some(common::UniqueId {
                hi: query.high(),
                lo: query.low(),
            }),
            fragment_instance_id: Some(common::UniqueId {
                hi: finst.high(),
                lo: finst.low(),
            }),
            backend_num: 3,
            query_options: Some(proto::QueryOptions {
                pipeline_dop: 1,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn decode(
        fragment: &plan::PlanFragment,
        params: &proto::InstanceParams,
    ) -> Result<DecodedNativeFragment, NativeFragmentDecodeError> {
        let instance = decode_instance_params(params).expect("valid native test instance");
        decode_fragment_submission(
            fragment,
            instance,
            params,
            Arc::new(ConnectorRegistry::new()),
            Arc::new(NeverResolved),
            Arc::new(NeverCancelled),
            Duration::from_secs(1),
        )
    }

    fn decode_request(
        fragment: plan::PlanFragment,
        params: proto::InstanceParams,
    ) -> Result<NativeFragmentRequest, crate::fragment::ingress::NativeFragmentIngressError> {
        let query_id = params.query_id.as_ref().expect("test query id");
        NativeFragmentRequest::try_decode(
            QueryExecutionId::new(
                novarocks_types::QueryId::new(query_id.hi, query_id.lo),
                AttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("valid execution id"),
            fragment,
            params,
            Arc::new(ConnectorRegistry::new()),
            Duration::from_secs(1),
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

    fn expect_request_error(
        result: Result<NativeFragmentRequest, crate::fragment::ingress::NativeFragmentIngressError>,
        message: &str,
    ) -> crate::fragment::ingress::NativeFragmentIngressError {
        match result {
            Ok(_) => panic!("{message}"),
            Err(error) => error,
        }
    }

    #[test]
    fn values_noop_decodes_to_validated_submission() {
        let query = UniqueId::new(11, 12);
        let finst = UniqueId::new(21, 22);

        let decoded = decode(&values_noop_fragment(), &instance_params(query, finst))
            .expect("decode values/noop submission");
        let (submission, backend_num) = decoded.into_parts();

        assert_eq!(
            submission.instance().query_id(),
            novarocks_types::QueryId::new(11, 12)
        );
        assert_eq!(submission.instance().fragment_instance_id().get(), finst);
        assert_eq!(submission.instance().backend_num().get(), 3);
        assert_eq!(backend_num, 3);
        assert_eq!(submission.program().sink().kind(), FragmentSinkKind::Noop);
        assert!(matches!(
            submission.program().plan().root.kind,
            ExecNodeKind::Values(_)
        ));
    }

    #[test]
    fn missing_root_fails_before_missing_sink() {
        let error = expect_decode_error(
            decode(
                &plan::PlanFragment::default(),
                &instance_params(UniqueId::new(31, 32), UniqueId::new(41, 42)),
            ),
            "missing root must fail",
        );

        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.path().to_string(), "plan_fragment.root");
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(protocol.detail(), "native PlanFragment requires root");
    }

    #[test]
    fn invalid_exchange_maps_are_checked_in_sorted_key_order() {
        let mut params = instance_params(UniqueId::new(51, 52), UniqueId::new(61, 62));
        params.per_exch_num_senders.insert(9, 0);
        params.per_exch_num_senders.insert(3, -1);

        let error = expect_request_error(
            decode_request(values_noop_fragment(), params),
            "invalid sender count must fail",
        );
        assert_eq!(
            error.to_string(),
            "native protocol error at instance_params.per_exch_num_senders[\"3\"] (out of range): sender count must be positive, got -1"
        );
    }

    #[test]
    fn scan_range_errors_include_sorted_map_key_and_range_index() {
        let mut params = instance_params(UniqueId::new(131, 132), UniqueId::new(141, 142));
        params.per_node_scan_ranges.insert(
            11,
            proto::ScanRangeList {
                ranges: vec![proto::ScanRangeParams::default()],
            },
        );

        let error = expect_request_error(
            decode_request(values_noop_fragment(), params),
            "missing scan range must fail",
        );
        assert_eq!(
            error.to_string(),
            "native protocol error at instance_params.per_node_scan_ranges[\"11\"].ranges[0].range (missing field): native ScanRangeParams requires range"
        );
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
                &instance_params(UniqueId::new(91, 92), UniqueId::new(101, 102)),
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
                &instance_params(UniqueId::new(111, 112), UniqueId::new(121, 122)),
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
                    op: expr::BinaryOp::Eq as i32,
                    left: None,
                    right: None,
                }))),
            }],
        });

        let error = expect_decode_error(
            decode(
                &fragment,
                &instance_params(UniqueId::new(211, 212), UniqueId::new(221, 222)),
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
                &instance_params(UniqueId::new(231, 232), UniqueId::new(241, 242)),
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
                &instance_params(UniqueId::new(251, 252), UniqueId::new(261, 262)),
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
        let mut params = instance_params(UniqueId::new(151, 152), UniqueId::new(161, 162));
        params.per_exch_num_senders.insert(99, 1);

        let error = expect_decode_error(
            decode(&values_noop_fragment(), &params),
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
                &instance_params(UniqueId::new(271, 272), UniqueId::new(281, 282)),
            ),
            "malformed submission must fail before runtime dependencies are observed",
        );
        assert_eq!(
            error.protocol().expect("protocol error").path().to_string(),
            "plan_fragment.root"
        );
    }
}
