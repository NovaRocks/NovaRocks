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

//! Backend-owned native fragment submission assembly.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks::connector::ConnectorRegistry;
use novarocks::exec::expr::ExprArena;
use novarocks::exec::fragment::program::{
    FragmentContractVersion, FragmentProgram, FragmentProgramOptions, FragmentSinkSpec,
    ScanSourceContract,
};
use novarocks::exec::node::ExecPlan;
use novarocks::protocol::FieldPath;
use novarocks::runtime::fragment::instance::{
    FragmentInstanceSpec, FragmentRuntimeOptions, ScanAssignments,
};
use novarocks::runtime::fragment::submission::FragmentSubmission;
use novarocks::runtime::scan_range::ScanRangeParams;
use novarocks_protocol::{novarocks as proto, plan};
use novarocks_spi::connector::{ConnectorCancellation, ConnectorExecutionResolver};

use crate::native::envelope::{require_root, require_sink};
use crate::native::exchange::decode_exchange_contracts;
use crate::native::instance::NativeFragmentInstanceInput;
use crate::native::runtime_filter::decode_runtime_filter_contract;
use crate::native::scan_contract::decode_scan_source_contracts;
use crate::native::sink_assignment::decode_fragment_sink_assignment;
use crate::native::submission_validation::{
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
    let program = FragmentProgram::new(
        ExecPlan {
            arena,
            root: decoded_root.node,
        },
        sink_spec,
        FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
        scan_sources,
        decode_exchange_contracts(root, root_path).map_err(NativeFragmentDecodeError::from)?,
        decode_runtime_filter_contract(fragment).map_err(NativeFragmentDecodeError::from)?,
    );
    let backend_num = instance.backend_num.get();
    let fragment_instance = FragmentInstanceSpec::new_native(
        FragmentContractVersion::CURRENT,
        instance.query_id,
        instance.fragment_instance_id,
        scan_assignments,
        instance.exchange_inputs,
        sink_assignment,
        FragmentRuntimeOptions::new(instance.query_options, None, instance.typed_result_sink),
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
    contracts: &BTreeMap<novarocks::exec::fragment::program::FragmentNodeId, ScanSourceContract>,
    raw_ranges: &BTreeMap<novarocks::exec::fragment::program::FragmentNodeId, Vec<ScanRangeParams>>,
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
