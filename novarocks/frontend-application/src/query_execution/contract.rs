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

//! Resolved Native execution options and typed Frontend execution failures.

use std::fmt;

use novarocks_execution::runtime::query_options::{
    QueryCacheOptions, QueryOptions as RuntimeQueryOptions,
};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::admitted_query_context::QueryExecutionContext;

/// Query options resolved by core before ownership crosses into frontend.
///
/// The runtime representation stays private; frontend only receives stable
/// scalar views needed to schedule, submit, and time out native work.
pub struct ResolvedQueryOptions {
    runtime: RuntimeQueryOptions,
}

impl ResolvedQueryOptions {
    pub(crate) fn from_upstream(options: Option<QueryOptions>) -> Self {
        let mut runtime = options
            .as_ref()
            .map(reconstruct_runtime_query_options)
            .unwrap_or_default();
        let pipeline_dop = novarocks_execution::runtime::exec_env::calc_pipeline_dop(
            runtime.pipeline_dop.unwrap_or_default(),
        );
        debug_assert!(pipeline_dop > 0, "resolved pipeline DOP must be positive");
        runtime.pipeline_dop = Some(pipeline_dop);
        Self { runtime }
    }

    pub fn timeout_ms(&self) -> i64 {
        self.runtime
            .query_timeout
            .map(|seconds| i64::from(seconds) * 1_000)
            .unwrap_or(300_000)
    }

    pub fn native_submission_options(&self) -> NativeSubmissionOptionsView {
        NativeSubmissionOptionsView {
            pipeline_dop: self
                .runtime
                .pipeline_dop
                .expect("core resolves pipeline DOP before request handoff"),
        }
    }

    pub fn runtime_filter_lifecycle(&self) -> RuntimeFilterLifecycleView {
        let (delivery_expire, query_expire) =
            novarocks_execution::runtime::query_options::query_expire_durations(Some(
                &self.runtime,
            ));
        RuntimeFilterLifecycleView {
            delivery_expire,
            query_expire,
        }
    }

    /// Frozen execution options exposed to the Frontend only for its
    /// role-owned native wire projection.  This does not provide lifecycle
    /// construction or a mutable execution handle.
    pub fn runtime_options(&self) -> &RuntimeQueryOptions {
        &self.runtime
    }
}

/// Project admitted statement semantics for synthetic executions that previously
/// supplied no options. Existing sealed options must not pass through this path.
pub(crate) fn synthetic_statement_query_options(execution: &QueryExecutionContext) -> QueryOptions {
    let mut runtime = RuntimeQueryOptions::default();
    runtime.set_allow_throw_exception(execution.sql_semantics().sql_mode().allow_throw_exception());
    QueryOptions::from_proto(
        crate::native::fragment_encoder::instance::encode_query_options(&runtime),
    )
}

/// Freeze a completed plan's per-instance driver range from the same resolved
/// query DOP that its eventual native request will use. Backend count controls
/// placement, not the number of drivers inside one placed fragment instance.
pub(crate) fn completed_plan_dop_domain(
    options: Option<&QueryOptions>,
) -> Result<novarocks_physical_plan::PipelineDopDomain, String> {
    let resolved = ResolvedQueryOptions::from_upstream(options.cloned())
        .native_submission_options()
        .pipeline_dop();
    let max = u32::try_from(resolved)
        .ok()
        .filter(|dop| (1..=novarocks_physical_plan::MAX_PIPELINE_DOP).contains(dop))
        .ok_or_else(|| format!("resolved query pipeline DOP {resolved} exceeds the plan limit"))?;
    Ok(novarocks_physical_plan::PipelineDopDomain {
        min: 1,
        max,
        requires_power_of_two: false,
    })
}

/// Reconstructs the Frontend-local execution view from the protocol value
/// without creating a second wire representation or decoder.
fn reconstruct_runtime_query_options(options: &QueryOptions) -> RuntimeQueryOptions {
    let src = options.as_proto();
    RuntimeQueryOptions {
        batch_size: (src.batch_size > 0).then_some(src.batch_size),
        query_timeout: (src.query_timeout > 0).then_some(src.query_timeout),
        query_delivery_timeout: (src.query_delivery_timeout > 0)
            .then_some(src.query_delivery_timeout),
        enable_profile: src.enable_profile,
        runtime_profile_report_interval: (src.runtime_profile_report_interval > 0)
            .then_some(src.runtime_profile_report_interval),
        pipeline_dop: (src.pipeline_dop > 0).then_some(src.pipeline_dop),
        exec_mem_limit: (src.query_mem_limit > 0).then_some(src.query_mem_limit),
        orc_use_column_names: src.orc_use_column_names,
        enable_file_metacache: src.enable_file_metacache,
        enable_file_pagecache: src.enable_file_pagecache,
        enable_parquet_reader_page_index: src.enable_parquet_reader_page_index,
        runtime_filter_scan_wait_time_ms: src.runtime_filter_scan_wait_time_ms,
        runtime_filter_wait_timeout_ms: src.runtime_filter_wait_timeout_ms,
        allow_throw_exception: src.allow_throw_exception,
        group_concat_max_len: src.group_concat_max_len,
        enable_join_runtime_bitset_filter: src.enable_join_runtime_bitset_filter,
        global_runtime_filter_build_max_size: (src.global_runtime_filter_build_max_size > 0)
            .then_some(src.global_runtime_filter_build_max_size),
        cache: QueryCacheOptions {
            enable_scan_datacache: src.enable_scan_datacache,
            enable_populate_datacache: src.enable_populate_datacache,
            enable_datacache_async_populate_mode: src.enable_datacache_async_populate_mode,
            enable_datacache_io_adaptor: src.enable_datacache_io_adaptor,
            enable_cache_select: src.enable_cache_select,
            datacache_evict_probability: src.datacache_evict_probability,
            datacache_priority: (src.datacache_priority != 0).then_some(src.datacache_priority),
            datacache_ttl_seconds: (src.datacache_ttl_seconds > 0)
                .then_some(src.datacache_ttl_seconds),
            datacache_sharing_work_period: (src.datacache_sharing_work_period > 0)
                .then_some(src.datacache_sharing_work_period),
        },
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeSubmissionOptionsView {
    pipeline_dop: i32,
}

impl NativeSubmissionOptionsView {
    pub const fn pipeline_dop(self) -> i32 {
        self.pipeline_dop
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterLifecycleView {
    delivery_expire: std::time::Duration,
    query_expire: std::time::Duration,
}

impl RuntimeFilterLifecycleView {
    pub const fn delivery_expire(self) -> std::time::Duration {
        self.delivery_expire
    }

    pub const fn query_expire(self) -> std::time::Duration {
        self.query_expire
    }
}

/// Stable error categories exposed by the coordinator boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DistributedQueryErrorKind {
    ContractViolation,
    Rejected,
    Failed,
}

/// A coordinator failure that core can surface without naming a coordinator
/// implementation or frontend state type.
#[derive(Clone, Debug, Eq, PartialEq)]
enum DistributedQueryErrorBody {
    Coordinator(String),
    Execution(Box<novarocks_query_application::execution_error::ExecutionFailureInput>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedQueryError {
    domain_source: Option<novarocks_execution_contract::failure::SharedFailureCause>,
    kind: DistributedQueryErrorKind,
    body: DistributedQueryErrorBody,
    compile_control: Option<novarocks_type_contract::CompileControlError>,
}

impl DistributedQueryError {
    /// An already selected execution input cannot become preparation text.
    pub(crate) fn from_execution(
        input: novarocks_query_application::execution_error::ExecutionFailureInput,
    ) -> Self {
        Self {
            domain_source: None,
            kind: DistributedQueryErrorKind::Failed,
            body: DistributedQueryErrorBody::Execution(Box::new(input)),
            compile_control: None,
        }
    }

    pub(crate) fn execution_failure(
        &self,
    ) -> Option<&novarocks_query_application::execution_error::ExecutionFailureInput> {
        match &self.body {
            DistributedQueryErrorBody::Execution(input) => Some(input),
            DistributedQueryErrorBody::Coordinator(_) => None,
        }
    }

    pub fn new(kind: DistributedQueryErrorKind, message: impl Into<String>) -> Self {
        Self {
            domain_source: None,
            kind,
            body: DistributedQueryErrorBody::Coordinator(message.into()),
            compile_control: None,
        }
    }

    pub(crate) fn from_source(
        kind: DistributedQueryErrorKind,
        source: novarocks_execution_contract::failure::SharedFailureCause,
    ) -> Self {
        use novarocks_failure_contract::FailureProjection;
        let input = novarocks_query_application::execution_error::ExecutionFailureInput::new(
            source.failure_fact(),
            novarocks_execution_contract::SafeDetail::truncating_display(format_args!(
                "native attempt initialization failed in its owning domain (reason {})",
                source.failure_fact().reason(),
            )),
            novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator,
        );
        let mut error = Self::from_execution(input);
        error.kind = kind;
        error.domain_source = Some(source);
        error
    }

    pub(crate) fn from_compile(error: novarocks_sql::compiler::SqlCompileError) -> Self {
        use novarocks_sql::compiler::SqlCompileError;
        use novarocks_type_contract::CompileControlError;
        match error {
            SqlCompileError::Cancelled => {
                Self::from_compile_control(CompileControlError::Cancelled)
            }
            SqlCompileError::DeadlineExceeded => {
                Self::from_compile_control(CompileControlError::DeadlineExceeded)
            }
            SqlCompileError::ResourceExhausted => {
                Self::from_compile_control(CompileControlError::ResourceExhausted)
            }
            SqlCompileError::Analyze(error) => match error.control_error() {
                Some(error) => Self::from_compile_control(error),
                None => {
                    let message = if error.kind()
                        == novarocks_sql::analyze_error::AnalyzeErrorKind::UnavailableImplementation
                    {
                        // Source admission can discover a metadata-only overload
                        // after SQL analysis. Preserve its exact SQL descriptor
                        // through the existing source-less completion error port.
                        error.to_user_error(None).to_string()
                    } else {
                        error.to_string()
                    };
                    Self::new(DistributedQueryErrorKind::ContractViolation, message)
                }
            },
            SqlCompileError::CompilationSource(source) => {
                Self::from_source(DistributedQueryErrorKind::ContractViolation, source)
            }
            error @ (SqlCompileError::InvalidRequest(_) | SqlCompileError::Compilation(_)) => {
                Self::new(
                    DistributedQueryErrorKind::ContractViolation,
                    error.to_string(),
                )
            }
        }
    }
    pub(crate) fn from_encode(error: novarocks_plan_codec::PhysicalEncodeError) -> Self {
        match error {
            novarocks_plan_codec::PhysicalEncodeError::Control(error) => {
                Self::from_compile_control(error)
            }
            other => Self::from_source(
                DistributedQueryErrorKind::ContractViolation,
                novarocks_failure_contract::SharedFailureCause::new(other),
            ),
        }
    }
    fn from_compile_control(error: novarocks_type_contract::CompileControlError) -> Self {
        let mut failure = match &error {
            novarocks_type_contract::CompileControlError::Source(source) => {
                Self::from_source(DistributedQueryErrorKind::ContractViolation, source.clone())
            }
            _ => Self::new(
                DistributedQueryErrorKind::ContractViolation,
                error.to_string(),
            ),
        };
        failure.compile_control = Some(error);
        failure
    }
    pub fn compile_control_error(&self) -> Option<novarocks_type_contract::CompileControlError> {
        self.compile_control.clone()
    }

    /// The coordinator consumes its own closed state. Foreign domain facts and
    /// already selected root-read input are forwarded without reclassification;
    /// text-only coordinator failures explicitly select this owner's reason.
    pub(crate) fn into_execution_failure_input(
        self,
    ) -> novarocks_query_application::execution_error::ExecutionFailureInput {
        use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
        use novarocks_query_application::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
        };
        let message = match self.body {
            DistributedQueryErrorBody::Execution(input) => return *input,
            DistributedQueryErrorBody::Coordinator(message) => message,
        };
        let site = ExecutionFailureSite::Coordinator;
        if let Some(source) = self.domain_source {
            return ExecutionFailureInput::new(
                source.failure_fact(),
                SafeDetail::truncating_display(format_args!(
                    "native attempt initialization failed in its owning domain (reason {})",
                    source.failure_fact().reason(),
                )),
                site,
            );
        }
        if let Some(control) = self.compile_control {
            return ExecutionFailureInput::new(
                control.failure_fact(),
                SafeDetail::truncating(
                    "native attempt compilation was stopped by its control owner",
                ),
                site,
            );
        }
        let reason = match self.kind {
            DistributedQueryErrorKind::ContractViolation => QueryFailureReason::PreparationProtocol,
            DistributedQueryErrorKind::Rejected | DistributedQueryErrorKind::Failed => {
                QueryFailureReason::PreparationFailed
            }
        };
        tracing::debug!(error = %message, kind = ?self.kind, "coordinator preparation failed");
        ExecutionFailureInput::new(
            reason.failure_fact(),
            SafeDetail::truncating("native attempt initialization failed"),
            site,
        )
    }

    #[cfg(test)]
    pub fn kind(&self) -> DistributedQueryErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        match &self.body {
            DistributedQueryErrorBody::Coordinator(message) => message,
            DistributedQueryErrorBody::Execution(input) => input.detail().as_str(),
        }
    }
}

impl fmt::Display for DistributedQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.message())
    }
}

impl std::error::Error for DistributedQueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.domain_source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests;
