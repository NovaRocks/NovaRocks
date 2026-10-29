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

//! Resolve the Native role's real admission geometry for the sole Worker
//! Node. This leaf constructs no clock, host, executor or completion owner.

use crate::{
    native_lane::NativeLane,
    native_server::NativeIngressConfig,
    native_transport_admission::{
        TransportRole, incoming_lane_stream_limit, root_result_ingress_owner_limit,
    },
};
use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_task_codec::TransportBudget;
use novarocks_worker::{
    LeaseBounds, METRIC_PUBLISH_MIN_INTERVAL, NODE_CONTEXT_STEP_BUDGET, NODE_CONTROL_STEP_BUDGET,
    NodeAdmissionTicketConfig, NodeContextCoreConfig, NodeControlConfig, NodeGeometry,
    NodeGeometryError, NodeGeometryInputs, NodeWorkers, RequestHorizon,
    TaskInboundCapabilityLimits, TaskPreparationLimits,
    control_runtime_policy::{ControlRuntimePolicy, IngressRoleLimits, PolicyError},
};
use std::{fmt, num::NonZeroUsize, time::Duration};

/// An explicit total owned-record policy: active, retained and physically
/// removing Contexts all hold this same budget until their actual exit.
pub(super) const DEFAULT_TOTAL_CONTEXT_RECORDS: NonZeroUsize =
    NonZeroUsize::new(1024).expect("the explicit Context record policy is positive");

#[derive(Debug)]
pub(super) enum WorkerNodeConfigError {
    RootIngress(std::io::Error),
    Policy(PolicyError),
    Geometry(NodeGeometryError),
    Invalid(&'static str),
}
impl fmt::Display for WorkerNodeConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RootIngress(error) => {
                write!(f, "resolve actual Native Root ingress positions: {error}")
            }
            Self::Policy(error) => write!(f, "resolve Worker request policy: {error}"),
            Self::Geometry(error) => write!(f, "resolve Worker Node geometry: {error}"),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}

/// Pure configuration only. Application composition moves these fields into
/// NodeBootstrapConfig alongside its original identity/epoch/clock/host ports.
/// No placeholder bootstrap or second production owner is manufactured here.
pub(super) struct ResolvedWorkerNodeConfig {
    pub control: NodeControlConfig,
    pub context: NodeContextCoreConfig,
    pub tickets: NodeAdmissionTicketConfig,
    pub geometry: NodeGeometry,
    pub workers: NodeWorkers,
    pub cadence: Duration,
}

impl ResolvedWorkerNodeConfig {
    /// Thread counts are explicit, validated NonZero inputs from application
    /// composition. Each physical pool remains independent even when counts
    /// happen to be equal; queue sizes are derived by the original Node owner.
    pub(super) fn try_new(
        native: NativeIngressConfig,
        preparation: TaskPreparationLimits,
        inbound: TaskInboundCapabilityLimits,
        contexts: NonZeroUsize,
        workers: NodeWorkers,
    ) -> Result<Self, WorkerNodeConfigError> {
        let transport = TransportBudget::DEFAULT;
        let tasks = NonZeroUsize::new(transport.max_tasks_per_context()).ok_or(
            WorkerNodeConfigError::Invalid("Native Task bound must be positive"),
        )?;
        let active = NonZeroUsize::new(transport.max_active_tasks_per_backend()).ok_or(
            WorkerNodeConfigError::Invalid("Native active Task bound must be positive"),
        )?;
        if workers.preparation.get() != preparation.workers() {
            return Err(WorkerNodeConfigError::Invalid(
                "Node preparation workers differ from the configured preparation owner",
            ));
        }
        if preparation.per_context() > tasks.get() {
            return Err(WorkerNodeConfigError::Invalid(
                "per-Context preparation positions exceed the actual Native Task bound",
            ));
        }
        let profile = NativeResultSupportGeometry::V1;
        let root_holders = NonZeroUsize::new(
            usize::try_from(profile.root_live_send_holders).map_err(|_| {
                WorkerNodeConfigError::Invalid(
                    "Native Root holder geometry does not fit the target",
                )
            })?,
        )
        .ok_or(WorkerNodeConfigError::Invalid(
            "Native Root holder geometry must be positive",
        ))?;
        // Root unary ingress has a separate gate and real response-body tails.
        // Its original holders are not covered by ordinary_running/waiting.
        let root_requests = root_result_ingress_owner_limit(TransportRole::Backend, &profile)
            .map_err(WorkerNodeConfigError::RootIngress)?;
        let status_requests =
            incoming_lane_stream_limit(TransportRole::Backend, NativeLane::Observation, &profile)
                .map_err(WorkerNodeConfigError::RootIngress)?;
        let policy = ControlRuntimePolicy::try_new_with_request_sources(
            IngressRoleLimits {
                ordinary_running: native.ordinary_running,
                ordinary_waiting: native.ordinary_waiting,
                control_running: native.control_running,
                control_waiting: native.control_waiting,
                max_batch_items: transport.max_batch_items(),
                root_read_holders: root_holders.get(),
            },
            root_requests,
            status_requests,
        )
        .map_err(WorkerNodeConfigError::Policy)?;
        let tickets = NodeAdmissionTicketConfig::DEFAULT;
        let geometry = NodeGeometry::try_new(NodeGeometryInputs {
            contexts,
            tasks_per_context: tasks,
            active_tasks: active,
            preparing_tasks: NonZeroUsize::new(preparation.tasks()).ok_or(
                WorkerNodeConfigError::Invalid("preparation Task positions must be positive"),
            )?,
            preparing_bytes: NonZeroUsize::new(preparation.bytes()).ok_or(
                WorkerNodeConfigError::Invalid("preparation bytes must be positive"),
            )?,
            ticket_live: NonZeroUsize::new(tickets.max_reservations()).ok_or(
                WorkerNodeConfigError::Invalid("admission ticket positions must be positive"),
            )?,
            ticket_records: NonZeroUsize::new(tickets.max_records()).ok_or(
                WorkerNodeConfigError::Invalid("admission ticket records must be positive"),
            )?,
            inbound,
            policy,
            native_root_data_holders: root_holders,
        })
        .map_err(WorkerNodeConfigError::Geometry)?;
        let request_positions = geometry.request_positions().get();
        let control = NodeControlConfig {
            max_contexts: contexts.get(),
            max_waiters: request_positions,
            max_pending_requests: request_positions,
            step_budget: NODE_CONTROL_STEP_BUDGET,
            retained_contexts: contexts.get(),
            gone_fences: tasks.get(),
            retention: RequestHorizon::DEFAULT.total(),
        };
        let context = NodeContextCoreConfig {
            max_tasks: tasks.get(),
            max_creation_waiters: request_positions,
            max_pending_requests: request_positions,
            max_preparing: preparation.per_context(),
            max_task_bytes: u64::try_from(preparation.bytes()).map_err(|_| {
                WorkerNodeConfigError::Invalid("preparation bytes do not fit the protocol width")
            })?,
            step_budget: NODE_CONTEXT_STEP_BUDGET,
            lease_bounds: LeaseBounds::DEFAULT,
            metric_publish_interval: METRIC_PUBLISH_MIN_INTERVAL,
        };
        Ok(Self {
            control,
            context,
            tickets,
            geometry,
            workers,
            // Same existing preparation stop-observation cadence; the Common
            // protocol timer remains deadline-driven rather than a sweep.
            cadence: Duration::from_millis(50),
        })
    }
}
