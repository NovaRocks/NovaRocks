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

//! Backend fragment-decode boundary.
//!
//! This value owns the production request surface and decodes the instance
//! execution values before invoking the narrow core assembly seam for the
//! shared plan program. It also supplies the backend-owned sink-assignment
//! decoder at the established core assembly validation point.

use std::sync::Arc;
use std::time::Duration;

use novarocks_execution::runtime::fragment::FragmentSubmission;
#[cfg(test)]
use novarocks_proto_codec::lifecycle::decode_query_execution_id;
use novarocks_proto_models::{novarocks as proto, plan};
use novarocks_types::{QueryExecutionId, QueryId, UniqueId};

use crate::fragment::ingress::NativeFragmentIngressError;

use super::instance::{decode_instance_params, decode_instance_params_with_query_options};
use super::plan::submission::decode_fragment_submission;

pub(crate) struct NativeFragmentRequest {
    execution_id: QueryExecutionId,
    submission: FragmentSubmission,
    backend_num: i32,
}

#[cfg(test)]
pub(crate) fn decode_native_query_execution_id(
    execution_id: &proto::QueryExecutionId,
) -> Result<QueryExecutionId, NativeFragmentIngressError> {
    decode_query_execution_id(execution_id).map_err(NativeFragmentIngressError::new)
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
impl NativeFragmentRequest {
    #[cfg(test)]
    pub(crate) fn try_decode(
        execution_id: QueryExecutionId,
        fragment: plan::PlanFragment,
        instance_params: proto::InstanceParams,
        exchange_wait: std::time::Duration,
    ) -> Result<Self, NativeFragmentIngressError> {
        Self::try_decode_with_runtime(
            execution_id,
            fragment,
            instance_params,
            Arc::new(NeverCancelled),
            exchange_wait,
            None,
            Arc::new(
                novarocks_sql::compiler::build_builtin_engine_function_catalog()
                    .expect("builtin function catalog"),
            ),
        )
    }

    pub(crate) fn try_decode_with_runtime(
        execution_id: QueryExecutionId,
        fragment: plan::PlanFragment,
        instance_params: proto::InstanceParams,
        connector_cancellation: Arc<dyn novarocks_spi::connector::ConnectorCancellation>,
        exchange_wait: std::time::Duration,
        typed_scan_runtime: Option<crate::fragment::decode::plan::context::TypedScanRuntime>,
        function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
    ) -> Result<Self, NativeFragmentIngressError> {
        let instance = decode_instance_params(&instance_params)?;
        let decoded = decode_fragment_submission(
            &fragment,
            instance,
            &instance_params,
            connector_cancellation,
            exchange_wait,
            typed_scan_runtime,
            function_catalog,
        )
        .map_err(NativeFragmentIngressError::new)?;
        let (submission, backend_num) = decoded.into_parts();
        if execution_id.query_id().high() != submission.instance().query_id().high()
            || execution_id.query_id().low() != submission.instance().query_id().low()
        {
            return Err(NativeFragmentIngressError::new(
                "native fragment execution_id query_id does not match instance_params query_id",
            ));
        }
        Ok(Self {
            execution_id,
            submission,
            backend_num,
        })
    }

    /// Decodes a task after its wire QueryOptions copy has been proven equal
    /// to the immutable query-context contract.
    ///
    /// Only `query_options` feeds plan lowering and the resulting submission;
    /// the copy in `instance_params` remains a redundant wire witness.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_decode_with_context_options(
        execution_id: QueryExecutionId,
        fragment: plan::PlanFragment,
        instance_params: proto::InstanceParams,
        query_options: novarocks_execution::runtime::query_options::QueryOptions,
        connector_cancellation: Arc<dyn novarocks_spi::connector::ConnectorCancellation>,
        exchange_wait: std::time::Duration,
        typed_scan_runtime: Option<crate::fragment::decode::plan::context::TypedScanRuntime>,
        function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
    ) -> Result<Self, NativeFragmentIngressError> {
        let instance = decode_instance_params_with_query_options(&instance_params, query_options)?;
        let decoded = decode_fragment_submission(
            &fragment,
            instance,
            &instance_params,
            connector_cancellation,
            exchange_wait,
            typed_scan_runtime,
            function_catalog,
        )
        .map_err(NativeFragmentIngressError::new)?;
        let (submission, backend_num) = decoded.into_parts();
        if execution_id.query_id().high() != submission.instance().query_id().high()
            || execution_id.query_id().low() != submission.instance().query_id().low()
        {
            return Err(NativeFragmentIngressError::new(
                "native fragment execution_id query_id does not match instance_params query_id",
            ));
        }
        Ok(Self {
            execution_id,
            submission,
            backend_num,
        })
    }

    pub(crate) const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }
    pub(crate) const fn query_id(&self) -> QueryId {
        self.submission.instance().query_id()
    }
    pub(crate) const fn fragment_instance_id(&self) -> UniqueId {
        self.submission.instance().fragment_instance_id().get()
    }
    pub(crate) const fn backend_num(&self) -> i32 {
        self.backend_num
    }
    pub(crate) fn enable_profile(&self) -> bool {
        self.query_options().enable_profile()
    }
    pub(crate) fn runtime_profile_report_interval_seconds(&self) -> Option<i64> {
        self.query_options().runtime_profile_report_interval()
    }
    pub(crate) fn query_expire_durations(&self) -> (Duration, Duration) {
        novarocks_execution::runtime::query_options::query_expire_durations(Some(
            self.query_options(),
        ))
    }
    pub(crate) fn exec_mem_limit(&self) -> Option<i64> {
        self.query_options().exec_mem_limit()
    }
    pub(crate) fn has_runtime_filter_bindings(&self) -> bool {
        self.submission.program().runtime_filters().has_bindings()
    }
    pub(crate) fn uses_result_sink(&self) -> bool {
        self.submission.program().sink().kind()
            == novarocks_execution::exec::fragment::program::FragmentSinkKind::Result
    }
    pub(crate) fn root_plan_node_id(&self) -> i32 {
        self.submission.program().root_plan_node_id().get()
    }
    pub(crate) fn into_submission(self) -> FragmentSubmission {
        self.submission
    }

    pub(crate) fn query_options(
        &self,
    ) -> &novarocks_execution::runtime::query_options::QueryOptions {
        self.submission.instance().runtime_options().query_options()
    }
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
struct NeverCancelled;

impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use novarocks_execution::runtime::query_options::QueryOptions;
    use novarocks_proto_codec::lifecycle::{AttemptId, QueryExecutionId};
    use novarocks_proto_models::{common, novarocks as proto, plan};
    use novarocks_types::QueryId;

    use super::{NativeFragmentRequest, decode_native_query_execution_id};

    #[test]
    fn execution_identity_decode_preserves_native_error_contract() {
        let missing = decode_native_query_execution_id(&proto::QueryExecutionId::default())
            .expect_err("query id is required");
        assert_eq!(
            missing.to_string(),
            "native protocol error at query_execution_id.query_id (missing field): query id is required"
        );

        let zero_attempt = decode_native_query_execution_id(&proto::QueryExecutionId {
            query_id: Some(common::UniqueId { hi: 7, lo: 8 }),
            attempt_id: 0,
        })
        .expect_err("attempt id is required");
        assert_eq!(
            zero_attempt.to_string(),
            "native protocol error at query_execution_id.attempt_id (invalid value): attempt id must be nonzero"
        );
    }

    #[test]
    fn context_query_options_are_the_only_submission_runtime_authority() {
        let query_id = QueryId::new(41, 42);
        let request = NativeFragmentRequest::try_decode_with_context_options(
            QueryExecutionId::new(query_id, AttemptId::new(1).expect("nonzero attempt"))
                .expect("valid execution id"),
            plan::PlanFragment {
                fragment_id: 7,
                root: Some(plan::DistributedNode {
                    node_id: 10,
                    fragment_id: 7,
                    limit: -1,
                    payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                        output_columns: Vec::new(),
                        kind: Some(plan::plan_node::Kind::Values(plan::ValuesNode {
                            rows: Vec::new(),
                            columns: Vec::new(),
                        })),
                    })),
                    ..Default::default()
                }),
                sink: Some(plan::DataSink {
                    kind: Some(plan::data_sink::Kind::Noop(true)),
                }),
                runtime_filter_bindings: Some(plan::RuntimeFilterBindingTable {
                    fragment_id: 7,
                    bindings: Vec::new(),
                }),
                ..Default::default()
            },
            proto::InstanceParams {
                query_id: Some(common::UniqueId { hi: 41, lo: 42 }),
                fragment_instance_id: Some(common::UniqueId { hi: 51, lo: 52 }),
                backend_num: 3,
                query_options: Some(proto::QueryOptions {
                    pipeline_dop: 1,
                    query_timeout: 0,
                    ..Default::default()
                }),
                ..Default::default()
            },
            QueryOptions {
                pipeline_dop: Some(1),
                query_timeout: Some(9),
                batch_size: Some(2048),
                ..QueryOptions::default()
            },
            Arc::new(super::NeverCancelled),
            Duration::from_secs(1),
            None,
            Arc::new(
                novarocks_sql::compiler::build_builtin_engine_function_catalog()
                    .expect("builtin function catalog"),
            ),
        )
        .expect("decode values request through backend ingress");

        assert_eq!(request.query_id(), query_id);
        assert_eq!(
            request.fragment_instance_id(),
            novarocks_types::UniqueId::new(51, 52)
        );
        assert_eq!(request.backend_num(), 3);
        assert_eq!(request.root_plan_node_id(), 10);
        assert_eq!(request.query_options().query_timeout(), Some(9));
        assert_eq!(request.query_options().batch_size(), Some(2048));
    }
}
