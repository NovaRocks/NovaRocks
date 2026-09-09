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

use novarocks_workload_control::WorkOwner;

/// A product-independent query operation.
///
/// T07 adds the frozen, topology-free description behind this opaque request.
/// Its representation deliberately does not expose planning or native types.
pub struct QueryExecutionRequest {
    kind: QueryExecutionKind,
    _private: RequestPrivate,
}

struct RequestPrivate;

impl QueryExecutionRequest {
    pub const fn kind(&self) -> QueryExecutionKind {
        self.kind
    }

    #[allow(dead_code)]
    pub(crate) const fn new(kind: QueryExecutionKind) -> Self {
        Self {
            kind,
            _private: RequestPrivate,
        }
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
}

impl ExecutionHandle {
    pub fn new(control: impl ExecutionControl) -> Self {
        Self {
            control: Box::new(control),
        }
    }

    pub fn request_cancel(&self) -> Result<(), QueryExecutionError> {
        self.control.request_cancel()
    }
}
