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
//! This value owns the production request surface: it decodes one fragment's
//! static plan against the kernel instance a task's creation winner projected,
//! invoking the narrow core assembly seam for the shared plan program. It also
//! supplies the backend-owned sink-assignment decoder at the established core
//! assembly validation point.

use std::sync::Arc;
use std::time::Duration;

use novarocks_execution::runtime::fragment::FragmentSubmission;
use novarocks_execution_contract::task_execution::descriptor::ExchangeTopology;
#[cfg(test)]
use novarocks_proto_codec::lifecycle::decode_query_execution_id;
#[cfg(test)]
use novarocks_proto_models::novarocks as proto;
use novarocks_proto_models::plan;
use novarocks_types::{QueryExecutionId, QueryId, UniqueId};

use crate::fragment_ingress_error::NativeFragmentIngressError;

use crate::fragment_instance::NativeFragmentInstanceInput;
use crate::fragment_plan_decode_submission::decode_fragment_submission;

pub struct NativeFragmentRequest {
    execution_id: QueryExecutionId,
    submission: FragmentSubmission,
    backend_num: i32,
}

#[cfg(test)]
pub fn decode_native_query_execution_id(
    execution_id: &proto::QueryExecutionId,
) -> Result<QueryExecutionId, NativeFragmentIngressError> {
    decode_query_execution_id(execution_id).map_err(NativeFragmentIngressError::new)
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
impl NativeFragmentRequest {
    /// Decodes one task's static plan against its projected kernel instance.
    ///
    /// The plan is taken by value: it is the one decoded copy the creation
    /// winner owns, and it ends here once the submission is assembled. The
    /// instance was projected from the single owner of each fact, so the only
    /// checks left are the ones between the plan and that instance -- scan
    /// node membership and sink edge binding -- and they run before anything
    /// is prepared.
    #[allow(clippy::too_many_arguments)]
    pub fn try_decode_task(
        execution_id: QueryExecutionId,
        fragment: plan::PlanFragment,
        instance: NativeFragmentInstanceInput,
        topology: &ExchangeTopology,
        connector_stop: novarocks_spi::connector::ConnectorStopView,
        exchange_wait: std::time::Duration,
        typed_scan_runtime: Option<novarocks_worker::TypedScanRuntime>,
        function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
    ) -> Result<Self, NativeFragmentIngressError> {
        let decoded = decode_fragment_submission(
            &fragment,
            instance,
            topology,
            connector_stop,
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
                "native fragment execution_id query_id does not match the instance query_id",
            ));
        }
        Ok(Self {
            execution_id,
            submission,
            backend_num,
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }
    pub const fn query_id(&self) -> QueryId {
        self.submission.instance().query_id()
    }
    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.submission.instance().fragment_instance_id().get()
    }
    pub const fn backend_num(&self) -> i32 {
        self.backend_num
    }
    pub fn enable_profile(&self) -> bool {
        self.query_options().enable_profile()
    }
    pub fn runtime_profile_report_interval_seconds(&self) -> Option<i64> {
        self.query_options().runtime_profile_report_interval()
    }
    pub fn query_expire_durations(&self) -> (Duration, Duration) {
        novarocks_execution::runtime::query_options::query_expire_durations(Some(
            self.query_options(),
        ))
    }
    pub fn exec_mem_limit(&self) -> Option<i64> {
        self.query_options().exec_mem_limit()
    }
    pub fn has_runtime_filter_bindings(&self) -> bool {
        self.submission.program().runtime_filters().has_bindings()
    }
    pub fn uses_result_sink(&self) -> bool {
        self.submission.program().sink_kind()
            == novarocks_execution::exec::fragment::program::FragmentSinkKind::Result
    }
    /// What the decoded program's sink does.
    pub fn sink_kind(&self) -> novarocks_execution::exec::fragment::program::FragmentSinkKind {
        self.submission.program().sink_kind()
    }
    pub fn root_plan_node_id(&self) -> i32 {
        self.submission.program().root_plan_node_id().get()
    }
    pub fn into_submission(self) -> FragmentSubmission {
        self.submission
    }

    pub fn query_options(&self) -> &novarocks_execution::runtime::query_options::QueryOptions {
        self.submission.instance().runtime_options().query_options()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use novarocks_execution::exec::fragment::program::FragmentSinkKind;
    use novarocks_execution::runtime::query_options::QueryOptions;
    use novarocks_execution_contract::task_execution::descriptor::{
        ExchangeTopology, TaskDescriptor,
    };
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_proto_codec::lifecycle::{AttemptId, QueryExecutionId};
    use novarocks_proto_models::{common, novarocks as proto, plan};
    use novarocks_types::QueryId;
    use novarocks_types::identity::{BackendProcessId, StageId, TaskId};

    use super::{NativeFragmentRequest, decode_native_query_execution_id};
    use crate::fragment_instance::project_task_instance;

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
        let execution =
            QueryExecutionId::new(query_id, AttemptId::new(1).expect("nonzero attempt"))
                .expect("valid execution id");
        let descriptor = TaskDescriptor::try_new(
            TaskIdentity::new(
                execution,
                StageId::new(1).expect("stage"),
                TaskId::new(1).expect("task"),
                BackendProcessId::new_v7(),
            ),
            novarocks_types::UniqueId::new(51, 52),
            NonZeroUsize::new(1).expect("dop"),
            Vec::new(),
            ExchangeTopology::default(),
        )
        .expect("legal descriptor");
        let instance = project_task_instance(
            &descriptor,
            proto::TaskAssignment {
                instance_ordinal: 3,
                ..Default::default()
            },
            QueryOptions {
                pipeline_dop: Some(1),
                query_timeout: Some(9),
                batch_size: Some(2048),
                ..QueryOptions::default()
            },
            FragmentSinkKind::Noop,
        )
        .expect("project the task instance");
        let request = NativeFragmentRequest::try_decode_task(
            execution,
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
            instance,
            descriptor.topology(),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
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
        assert_eq!(request.sink_kind(), FragmentSinkKind::Noop);
        assert_eq!(request.query_options().query_timeout(), Some(9));
        assert_eq!(request.query_options().batch_size(), Some(2048));
    }
}
