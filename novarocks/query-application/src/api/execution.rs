// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc};

use novarocks_workload_control::{
    CancellationReason, WorkCancellationRequester, WorkError, WorkOwner,
};

use crate::preparation::FrozenExecutionDescription;

/// A product-independent query operation.
///
/// The request directly owns SQL's typed, topology-free distributed plan and
/// its validated application contracts. Attempts read those values without a
/// serialization or decoding layer.
pub struct QueryExecutionRequest {
    description: FrozenExecutionDescription,
}

impl QueryExecutionRequest {
    pub const fn kind(&self) -> QueryExecutionKind {
        self.description.kind()
    }

    /// Consume a fully frozen semantic description. Execution may instantiate
    /// attempts from it, but has no callback into observation or compilation.
    pub fn from_frozen_description(description: FrozenExecutionDescription) -> Self {
        Self { description }
    }

    pub const fn description(&self) -> &FrozenExecutionDescription {
        &self.description
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QueryExecutionKind {
    Read,
    Write,
    Statistics,
    Maintenance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QueryExecutionErrorKind {
    InvalidRequest,
    Rejected,
    Cancelled,
    DeadlineExceeded,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryExecutionError {
    kind: QueryExecutionErrorKind,
    message: Arc<str>,
}

impl QueryExecutionError {
    pub fn new(kind: QueryExecutionErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> QueryExecutionErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for QueryExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for QueryExecutionError {}

pub type QueryExecutionFuture =
    Pin<Box<dyn Future<Output = Result<ExecutionHandle, QueryExecutionError>> + Send + 'static>>;

/// Server-supplied query execution implementation.
pub trait QueryExecutionDriver: Send + Sync + 'static {
    fn start(&self, request: QueryExecutionRequest, owner: WorkOwner) -> QueryExecutionFuture;
}

/// Cloneable capability for starting logical executions.
///
/// Starting consumes the single work owner. A caller therefore cannot submit
/// one governed responsibility twice or omit governance accidentally.
///
/// ```compile_fail
/// use novarocks_query_application::api::{QueryExecutionClient, QueryExecutionRequest};
/// fn start_without_governance(
///     client: &QueryExecutionClient,
///     request: QueryExecutionRequest,
/// ) {
///     let _ = client.start(request);
/// }
/// ```
#[derive(Clone)]
pub struct QueryExecutionClient {
    driver: Arc<dyn QueryExecutionDriver>,
}

impl QueryExecutionClient {
    pub fn new(driver: impl QueryExecutionDriver) -> Self {
        Self {
            driver: Arc::new(driver),
        }
    }

    pub fn start(&self, request: QueryExecutionRequest, owner: WorkOwner) -> QueryExecutionFuture {
        self.driver.start(request, owner)
    }
}

/// Driver-owned control of a running logical execution.
pub trait ExecutionControl: Send + 'static {
    fn request_cancel(&self) -> Result<(), QueryExecutionError>;
}

impl ExecutionControl for WorkCancellationRequester {
    fn request_cancel(&self) -> Result<(), QueryExecutionError> {
        self.request(CancellationReason::Requested)
            .map_err(|error| {
                let kind = if matches!(error, WorkError::Released) {
                    QueryExecutionErrorKind::Rejected
                } else {
                    QueryExecutionErrorKind::Failed
                };
                QueryExecutionError::new(
                    kind,
                    format!("request governed logical execution cancellation: {error}"),
                )
            })
    }
}

/// Move-only logical execution handle.
///
/// Dropping a handle is not a stop or resource-release fact. T08 attaches
/// bounded result delivery and convergence observation to this abstraction.
///
/// ```compile_fail
/// use novarocks_query_application::api::ExecutionHandle;
/// fn duplicate(handle: ExecutionHandle) {
///     let _copy = handle.clone();
/// }
/// ```
pub struct ExecutionHandle {
    control: Box<dyn ExecutionControl>,
    output: Option<crate::api::ExecutionOutput>,
}

impl ExecutionHandle {
    pub(crate) fn new(control: impl ExecutionControl, output: crate::api::ExecutionOutput) -> Self {
        Self {
            control: Box::new(control),
            output: Some(output),
        }
    }

    pub fn request_cancel(&self) -> Result<(), QueryExecutionError> {
        self.control.request_cancel()
    }

    /// Transfers the single result consumer to the protocol/application owner.
    /// Cancellation remains available on this handle while that output is in
    /// use, and taking it twice cannot create another consumer.
    pub fn take_output(&mut self) -> Option<crate::api::ExecutionOutput> {
        self.output.take()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;
    use crate::api::ExecutionOutput;
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    struct RecordingControl(Arc<AtomicBool>);

    impl ExecutionControl for RecordingControl {
        fn request_cancel(&self) -> Result<(), QueryExecutionError> {
            self.0.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn execution_handle_transfers_output_once_and_keeps_control() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut handle = ExecutionHandle::new(
            RecordingControl(Arc::clone(&cancelled)),
            ExecutionOutput::Completion,
        );
        assert!(matches!(
            handle.take_output(),
            Some(ExecutionOutput::Completion)
        ));
        assert!(handle.take_output().is_none());
        handle.request_cancel().unwrap();
        assert!(cancelled.load(Ordering::SeqCst));
    }

    #[test]
    fn execution_handle_requests_cancellation_without_owning_work() {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let handle = ExecutionHandle::new(
            work.owner.cancellation_requester(),
            ExecutionOutput::Completion,
        );

        handle.request_cancel().unwrap();
        assert_eq!(
            scope.cancellation().unwrap().reason(),
            Some(CancellationReason::Requested)
        );
        assert_eq!(control.snapshot().root_responsibilities, 1);

        drop(handle);
        work.owner.complete();
        work.business.release();
    }
}
