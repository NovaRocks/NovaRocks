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

use novarocks::cache::CacheOptions;
use novarocks::connector::ConnectorRegistry;
use novarocks::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};
use novarocks::query_execution::lifecycle::{AttemptId, QueryExecutionId};
use novarocks::runtime::fragment::FragmentSubmission;
use novarocks_protocol::{novarocks as proto, plan};
use novarocks_spi::connector::ConnectorExecutionResolver;
use novarocks_types::QueryId as ExecutionQueryId;
use novarocks_types::QueryId;
use novarocks_types::UniqueId;

use super::ingress::NativeFragmentIngressError;
use super::instance::decode_instance_params;
use super::plan_decode::submission::decode_fragment_submission;

pub(crate) struct NativeFragmentRequest {
    execution_id: QueryExecutionId,
    submission: FragmentSubmission,
    backend_num: i32,
}

pub(crate) fn decode_native_query_execution_id(
    execution_id: &proto::QueryExecutionId,
) -> Result<QueryExecutionId, NativeFragmentIngressError> {
    let root = FieldPath::root("execution_id");
    let query_id = execution_id.query_id.as_ref().ok_or_else(|| {
        NativeFragmentIngressError::new(
            ProtocolError::new(
                ProtocolFamily::Native,
                root.clone().field("query_id"),
                ProtocolErrorKind::MissingField,
                "native fragment execution_id requires query_id",
            )
            .to_string(),
        )
    })?;
    let attempt_id = AttemptId::new(execution_id.attempt_id).map_err(|error| {
        NativeFragmentIngressError::new(
            ProtocolError::new(
                ProtocolFamily::Native,
                root.clone().field("attempt_id"),
                ProtocolErrorKind::InvalidValue,
                error.to_string(),
            )
            .to_string(),
        )
    })?;
    QueryExecutionId::new(ExecutionQueryId::new(query_id.hi, query_id.lo), attempt_id).map_err(
        |error| {
            NativeFragmentIngressError::new(
                ProtocolError::new(
                    ProtocolFamily::Native,
                    root,
                    ProtocolErrorKind::InvalidValue,
                    error.to_string(),
                )
                .to_string(),
            )
        },
    )
}

impl NativeFragmentRequest {
    pub(crate) fn try_decode(
        execution_id: QueryExecutionId,
        fragment: plan::PlanFragment,
        instance_params: proto::InstanceParams,
        connectors: Arc<ConnectorRegistry>,
    ) -> Result<Self, NativeFragmentIngressError> {
        Self::try_decode_with_execution_resolver(
            execution_id,
            fragment,
            instance_params,
            connectors,
            Arc::new(MissingExecutionResolver),
            Arc::new(NeverCancelled),
        )
    }

    pub(crate) fn try_decode_with_execution_resolver(
        execution_id: QueryExecutionId,
        fragment: plan::PlanFragment,
        instance_params: proto::InstanceParams,
        connectors: Arc<ConnectorRegistry>,
        execution_resolver: Arc<dyn ConnectorExecutionResolver>,
        connector_cancellation: Arc<dyn novarocks_spi::connector::ConnectorCancellation>,
    ) -> Result<Self, NativeFragmentIngressError> {
        let instance = decode_instance_params(&instance_params)?;
        let decoded = decode_fragment_submission(
            &fragment,
            instance,
            &instance_params,
            connectors,
            execution_resolver,
            connector_cancellation,
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
        novarocks::runtime::query_options::query_expire_durations(Some(self.query_options()))
    }
    pub(crate) fn cache_options(&self) -> Result<CacheOptions, NativeFragmentIngressError> {
        CacheOptions::from_query_options(Some(self.query_options()))
            .map_err(NativeFragmentIngressError::new)
    }
    pub(crate) fn has_runtime_filter_bindings(&self) -> bool {
        self.submission.program().runtime_filters().has_bindings()
    }
    pub(crate) fn uses_result_sink(&self) -> bool {
        self.submission.program().sink().kind()
            == novarocks::exec::fragment::program::FragmentSinkKind::Result
    }
    pub(crate) fn root_plan_node_id(&self) -> i32 {
        self.submission.program().root_plan_node_id().get()
    }
    pub(crate) fn into_submission(self) -> FragmentSubmission {
        self.submission
    }

    fn query_options(&self) -> &novarocks::runtime::query_options::QueryOptions {
        self.submission.instance().runtime_options().query_options()
    }
}

struct MissingExecutionResolver;

impl ConnectorExecutionResolver for MissingExecutionResolver {
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

struct NeverCancelled;

impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::decode_native_query_execution_id;
    use novarocks_protocol::{common, novarocks};

    #[test]
    fn execution_identity_decode_preserves_native_error_contract() {
        let missing = decode_native_query_execution_id(&novarocks::QueryExecutionId::default())
            .expect_err("query id is required");
        assert_eq!(
            missing.to_string(),
            "native protocol error at execution_id.query_id (missing field): native fragment execution_id requires query_id"
        );

        let zero_attempt = decode_native_query_execution_id(&novarocks::QueryExecutionId {
            query_id: Some(common::UniqueId { hi: 7, lo: 8 }),
            attempt_id: 0,
        })
        .expect_err("attempt id is required");
        assert_eq!(
            zero_attempt.to_string(),
            "native protocol error at execution_id.attempt_id (invalid value): InvalidManifest: attempt id must be nonzero"
        );
    }
}
