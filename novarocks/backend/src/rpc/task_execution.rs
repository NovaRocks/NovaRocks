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

//! Native task protocol RPC boundary.
//!
//! This is the only place the task protocol's wire messages meet the backend's
//! own owner. The RPC service stays thin: it hands a decoded request to this
//! port and encodes what comes back, so nothing above it interprets a wire
//! shape and nothing below it names one.
//!
//! Five entry points, matching the five RPCs: one batched mutation, one status
//! subscription, two typed observation reads, and the root result data plane.

use std::pin::Pin;

use novarocks_proto_models::novarocks as proto;
use tokio_stream::Stream;

/// Server-side status event stream of one logical query-by-backend
/// subscription.
pub(crate) type TaskStatusEventStream =
    Pin<Box<dyn Stream<Item = Result<proto::TaskStatusStreamEvent, tonic::Status>> + Send>>;

/// The backend's task protocol port.
///
/// Every method takes the wire request and returns the wire response, because
/// this is the wire boundary. A `tonic::Status` here means the request could
/// not be understood at all; a request that was understood and refused comes
/// back as a typed receipt or outcome inside a successful response, which is
/// what lets a frontend classify it without reading an error message.
#[tonic::async_trait]
pub(crate) trait TaskExecutionIngress: Send + Sync {
    /// Applies a per-backend batch, one receipt per item in request order.
    ///
    /// A batch gives its items no atomicity and no shared verdict: a partial
    /// failure leaves every other item exactly as its own receipt reports.
    fn apply_task_operations(
        &self,
        request: proto::ApplyTaskOperationsRequest,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status>;

    /// Opens one logical subscription, resuming from the given per-task
    /// cursors.
    ///
    /// Observation only: it creates nothing, freezes no task set, and owns no
    /// admission, edge-open, terminal, or cancel authority.
    fn subscribe_task_status(
        &self,
        request: proto::SubscribeTaskStatusRequest,
    ) -> Result<TaskStatusEventStream, tonic::Status>;

    fn fetch_task_dynamic_filters(
        &self,
        request: proto::FetchTaskDynamicFiltersRequest,
    ) -> Result<proto::FetchTaskDynamicFiltersResponse, tonic::Status>;

    fn get_final_task_info(
        &self,
        request: proto::GetFinalTaskInfoRequest,
    ) -> Result<proto::GetFinalTaskInfoResponse, tonic::Status>;

    /// Polls the root task's result stream.
    ///
    /// Unlike the fragment-instance-addressed form it replaces, the request
    /// names an exact task, so it is fenced against a replaced backend
    /// process before it reaches a result buffer.
    async fn fetch_task_result(
        &self,
        request: proto::FetchTaskResultRequest,
    ) -> Result<proto::FetchResultResponse, tonic::Status>;
}
