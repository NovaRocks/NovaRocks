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
mod tests {
    fn validate_completed_runtime_options(
        description: &novarocks_query_application::preparation::FrozenExecutionDescription,
        options: &super::ResolvedQueryOptions,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), super::DistributedQueryError> {
        description
            .validate_legacy_intrinsic_allow_throw_exception(
                options.runtime_options().allow_throw_exception,
                control,
            )
            .map_err(super::DistributedQueryError::from_compile)
    }

    use super::{completed_plan_dop_domain, reconstruct_runtime_query_options};
    use novarocks_proto_codec::lifecycle::QueryOptions;
    use novarocks_proto_models::novarocks;

    #[test]
    fn typed_task_and_root_fetch_failures_preserve_engine_identity_without_text_inference() {
        use novarocks_failure_contract::FailureProjection;
        let backend = novarocks_types::BackendProcessId::new_v7();
        let query = novarocks_types::QueryExecutionId::new(
            novarocks_types::QueryId::new(31, 32),
            novarocks_types::AttemptId::new(1).unwrap(),
        )
        .unwrap();
        let identity = novarocks_execution_contract::TaskIdentity::new(
            query,
            novarocks_types::StageId::new(1).unwrap(),
            novarocks_types::TaskId::new(1).unwrap(),
            backend,
        );
        for original in [
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
                "FULL OUTER hash join".into(),
            ),
            novarocks_plan_codec::PhysicalEncodeError::Invalid("FULL OUTER hash join".into()),
        ] {
            let fact = original.failure_fact();
            let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
                identity,
                novarocks_execution_contract::TerminationDetail::Failed(
                    novarocks_execution_contract::TaskFailure::new_in_phase(
                        fact,
                        novarocks_execution_contract::SafeDetail::truncating(
                            "FULL OUTER hash join",
                        ),
                        novarocks_execution_contract::TaskFailurePhase::Preparation,
                    ),
                ),
            );
            let input = novarocks_query_application::execution_error::ExecutionFailureInput::from_selected_task(&selected).unwrap();
            let error = super::DistributedQueryError::from_execution(input.clone());
            assert_eq!(error.clone().into_execution_failure_input(), input);
            let observed =
                novarocks_query_application::coordination::RootResultFetchFailure::from_input(
                    input.clone(),
                );
            let error = super::DistributedQueryError::from_execution(observed.input().clone());
            assert_eq!(error.into_execution_failure_input(), input);
        }
    }

    #[test]
    fn completion_missing_pure_implementation_preserves_only_its_sql_descriptor() {
        use novarocks_functions::{FunctionBindingError, FunctionOverloadId};
        use novarocks_sql::{analyze_error::AnalyzeError, compiler::SqlCompileError};
        use novarocks_type_contract::CompileControlError;

        let compile = SqlCompileError::from(FunctionBindingError::UnavailableImplementation(
            FunctionOverloadId::try_new("fixture/completion/missing-pure").unwrap(),
        ));
        let SqlCompileError::Analyze(ref analyze) = compile else {
            panic!("missing pure implementation must retain SQL analysis identity");
        };
        let expected = analyze.to_user_error(None).to_string();
        let failure = super::DistributedQueryError::from_compile(compile);
        assert_eq!(failure.to_string(), expected);
        assert!(
            failure
                .to_string()
                .starts_with("[sql.analyze.unavailable_implementation] ")
        );
        assert_eq!(failure.compile_control_error(), None);

        let ordinary = super::DistributedQueryError::from_compile(SqlCompileError::Analyze(
            AnalyzeError::internal("unsupported diagnostic"),
        ));
        assert_eq!(ordinary.to_string(), "unsupported diagnostic");
        assert_eq!(ordinary.compile_control_error(), None);
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let failure = super::DistributedQueryError::from_compile(SqlCompileError::Analyze(
                AnalyzeError::control(control.clone()),
            ));
            assert_eq!(failure.compile_control_error(), Some(control));
        }
    }

    #[test]
    fn encode_capability_refusal_is_not_a_contract_violation() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        for original in [
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
                "CTE multicast".into(),
            ),
            novarocks_plan_codec::PhysicalEncodeError::Invalid("unsupported diagnostic".into()),
        ] {
            let expected = original.failure_fact();
            let error = super::DistributedQueryError::from_encode(original);
            assert_eq!(error.execution_failure().unwrap().fact(), expected);
            assert_eq!(
                error.clone().into_execution_failure_input().fact(),
                expected
            );
            assert!(
                error
                    .domain_source
                    .as_ref()
                    .unwrap()
                    .downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>()
                    .is_some()
            );
            assert_eq!(
                expected.class(),
                match error
                    .domain_source
                    .as_ref()
                    .unwrap()
                    .downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>()
                    .unwrap()
                {
                    novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(_) =>
                        FailureClass::Unsupported,
                    novarocks_plan_codec::PhysicalEncodeError::Invalid(_) =>
                        FailureClass::Invariant,
                    other => panic!("unexpected author: {other}"),
                }
            );
        }
    }

    #[test]
    fn synthetic_options_preserve_admitted_root_hints_and_absent_options_defaults() {
        use novarocks_query_application::admitted_query_context::{
            RequestAdmission, RequestContext,
        };
        use novarocks_query_application::api::BackendTopologySnapshot;
        use novarocks_query_application::cancellation::QueryCancellationSource;
        use novarocks_sql::sql_mode::{SqlMode, SqlSemanticSettings, statement_sql_semantics};

        for (session_mode, sql, expected_allow) in [
            ("32", "SELECT 1", false),
            ("ALLOW_THROW_EXCEPTION", "SELECT 1", true),
            (
                "32",
                "SELECT /*+ SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION') */ 1",
                true,
            ),
            (
                "ALLOW_THROW_EXCEPTION",
                "SELECT /*+ SET_VAR(sql_mode=32) */ 1",
                false,
            ),
        ] {
            let session = SqlSemanticSettings::default()
                .with_sql_mode(SqlMode::from_assignment(session_mode));
            let statements = novarocks_parser::parse(sql).unwrap();
            let admitted_semantics = statement_sql_semantics(&session, &statements[0]).unwrap();
            let cancellation = QueryCancellationSource::new();
            let context = RequestContext::admit(RequestAdmission::new(
                None,
                "default".to_owned(),
                novarocks_types::ClusterRole::Fe,
                BackendTopologySnapshot::empty(7),
                None,
                cancellation.view(),
                novarocks_sql::compiler::SessionOptimizerSettings::default(),
                admitted_semantics,
                novarocks_result_contract::ClientPresentationFamily::MysqlText,
            ));
            let options = super::synthetic_statement_query_options(context.execution());
            assert_eq!(options.as_proto().allow_throw_exception, expected_allow);
            let reconstructed = reconstruct_runtime_query_options(&options);
            let mut expected = novarocks_execution::runtime::query_options::QueryOptions::default();
            expected.set_allow_throw_exception(expected_allow);
            assert_eq!(reconstructed, expected);

            let resolved = super::ResolvedQueryOptions::from_upstream(Some(options));
            let mut absent = super::ResolvedQueryOptions::from_upstream(None);
            absent.runtime.set_allow_throw_exception(expected_allow);
            assert_eq!(resolved.runtime_options(), absent.runtime_options());
            validate_completed_runtime_options(
                &intrinsic_runtime_description(expected_allow),
                &resolved,
                &runtime_gate_control(None),
            )
            .unwrap();
        }
    }

    fn intrinsic_runtime_description(
        allow: bool,
    ) -> novarocks_query_application::preparation::FrozenExecutionDescription {
        use arrow::datatypes::DataType;
        use novarocks_physical_plan::{
            ExprKind, FragmentBuilder, FragmentId, FragmentSink, LiteralValue, PipelineDopDomain,
            PlanBuilder, PlanVersionId, ValueOrigin, ValueType,
        };
        use novarocks_query_application::preparation::{
            CompletedPhysicalPlanCandidate, ExecutionResourceRequirements, FrozenCostEstimate,
            FrozenEstimateUnknownReason, FrozenExecutionDescription, OutputContract,
        };
        use novarocks_type_contract::{
            DecimalOverflowPolicy, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
            SemanticParameterValue, SemanticParameters,
        };
        let mut fragment = FragmentBuilder::new(FragmentId::new(13));
        let node = fragment.reserve_node_id().unwrap();
        let ty = ValueType::new(DataType::Int64, false);
        let literal = fragment
            .add_expression(node, ty.clone(), ExprKind::Literal(LiteralValue::Int64(4)))
            .unwrap();
        let cast = fragment
            .add_expression(
                node,
                ty.clone(),
                ExprKind::Cast {
                    expr: literal,
                    target: DataType::Int64,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: SemanticParameterRef {
                        id: SemanticParameterId::new(u32::MAX),
                        expected_key: SemanticParameterKey::AllowThrowException,
                    },
                },
            )
            .unwrap();
        let value = fragment
            .add_value(
                ty,
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        fragment
            .add_values(node, Box::from([Box::from([cast])]), Box::from([value]))
            .unwrap();
        let fragment = fragment
            .finish_definition(
                node,
                FragmentSink::Noop,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap();
        let mut builder = PlanBuilder::new(PlanVersionId::try_new([29; 16]).unwrap())
            .with_semantic_parameters(
                SemanticParameters::try_new([(
                    SemanticParameterId::new(u32::MAX),
                    SemanticParameterValue::AllowThrowException(allow),
                )])
                .unwrap(),
            );
        builder.add_fragment(fragment).unwrap();
        let candidate = CompletedPhysicalPlanCandidate::for_program(
            builder.finish().unwrap(),
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        FrozenExecutionDescription::for_completed_plan(
            novarocks_query_application::api::QueryExecutionKind::Write,
            candidate,
            Vec::new(),
            OutputContract::CompletionOnly,
            novarocks_coordination_contract::ExecutionEffect::None,
            novarocks_coordination_contract::RecoveryMode::NoRecovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap()
    }

    struct RuntimeGateControl {
        calls: std::sync::Mutex<Vec<(novarocks_type_contract::CompilePhase, u32)>>,
        refuse: Option<(usize, novarocks_type_contract::CompileControlError)>,
    }
    impl novarocks_type_contract::PureCompileControl for RuntimeGateControl {
        fn checkpoint(
            &self,
            phase: novarocks_type_contract::CompilePhase,
            units: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            calls.push((phase, units));
            match self.refuse.clone() {
                Some((ordinal, cause)) if calls.len() == ordinal + 1 => Err(cause),
                _ => Ok(()),
            }
        }
    }
    fn runtime_gate_control(
        refuse: Option<(usize, novarocks_type_contract::CompileControlError)>,
    ) -> RuntimeGateControl {
        RuntimeGateControl {
            calls: std::sync::Mutex::new(Vec::new()),
            refuse,
        }
    }

    #[test]
    fn native_runtime_gate_compares_actual_resolved_options_with_the_same_candidate() {
        for actual in [false, true] {
            let options = super::ResolvedQueryOptions::from_upstream(Some(
                QueryOptions::from_proto(novarocks::QueryOptions {
                    allow_throw_exception: actual,
                    ..Default::default()
                }),
            ));
            let description = intrinsic_runtime_description(actual);
            validate_completed_runtime_options(&description, &options, &runtime_gate_control(None))
                .unwrap();
            let error = validate_completed_runtime_options(
                &intrinsic_runtime_description(!actual),
                &options,
                &runtime_gate_control(None),
            )
            .unwrap_err();
            assert_eq!(
                error.kind(),
                super::DistributedQueryErrorKind::ContractViolation
            );
            assert_eq!(error.compile_control_error(), None);
        }
        let default_options = super::ResolvedQueryOptions::from_upstream(None);
        assert!(
            validate_completed_runtime_options(
                &intrinsic_runtime_description(true),
                &default_options,
                &runtime_gate_control(None)
            )
            .is_err()
        );
    }

    #[test]
    fn native_runtime_gate_preserves_all_typed_control_prefixes_in_the_terminal_owner() {
        use novarocks_type_contract::CompileControlError;
        let description = intrinsic_runtime_description(true);
        let options = super::ResolvedQueryOptions::from_upstream(Some(QueryOptions::from_proto(
            novarocks::QueryOptions {
                allow_throw_exception: true,
                ..Default::default()
            },
        )));
        for success in [true, false] {
            let description = if success {
                description.clone()
            } else {
                intrinsic_runtime_description(false)
            };
            let trace = runtime_gate_control(None);
            let result = validate_completed_runtime_options(&description, &options, &trace);
            assert_eq!(result.is_ok(), success);
            let callbacks = trace.calls.into_inner().unwrap();
            assert_eq!(callbacks.len(), 2);
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                for ordinal in 0..callbacks.len() {
                    let trace = runtime_gate_control(Some((ordinal, cause.clone())));
                    let error = validate_completed_runtime_options(&description, &options, &trace)
                        .unwrap_err();
                    assert_eq!(error.compile_control_error(), Some(cause.clone()));
                    assert_eq!(*trace.calls.lock().unwrap(), callbacks[..=ordinal]);
                }
            }
        }
    }

    #[test]
    fn completed_plan_dop_domain_uses_resolved_driver_width_not_backend_count() {
        let explicit = QueryOptions::from_proto(novarocks::QueryOptions {
            pipeline_dop: 5,
            ..Default::default()
        });
        assert_eq!(completed_plan_dop_domain(Some(&explicit)).unwrap().max, 5);
        assert_eq!(
            completed_plan_dop_domain(None).unwrap().max as i32,
            novarocks_execution::runtime::exec_env::calc_pipeline_dop(0)
        );
    }

    #[test]
    fn reconstructed_runtime_options_preserve_protocol_scalars() {
        let protocol = QueryOptions::from_proto(novarocks::QueryOptions {
            batch_size: 4096,
            query_timeout: 60,
            query_delivery_timeout: 30,
            enable_profile: true,
            runtime_profile_report_interval: 7,
            pipeline_dop: 8,
            query_mem_limit: 1 << 20,
            runtime_filter_scan_wait_time_ms: Some(250),
            runtime_filter_wait_timeout_ms: Some(5000),
            allow_throw_exception: true,
            group_concat_max_len: Some(65_535),
            enable_join_runtime_bitset_filter: Some(false),
            global_runtime_filter_build_max_size: 1 << 19,
            orc_use_column_names: true,
            enable_file_metacache: true,
            enable_file_pagecache: true,
            enable_parquet_reader_page_index: true,
            enable_scan_datacache: true,
            enable_populate_datacache: true,
            enable_datacache_async_populate_mode: true,
            enable_datacache_io_adaptor: true,
            enable_cache_select: true,
            datacache_evict_probability: Some(75),
            datacache_priority: 2,
            datacache_ttl_seconds: 3600,
            datacache_sharing_work_period: 10,
        });

        let runtime = reconstruct_runtime_query_options(&protocol);

        assert_eq!(runtime.batch_size, Some(4096));
        assert_eq!(runtime.query_timeout, Some(60));
        assert_eq!(runtime.query_delivery_timeout, Some(30));
        assert!(runtime.enable_profile);
        assert_eq!(runtime.runtime_profile_report_interval, Some(7));
        assert_eq!(runtime.pipeline_dop, Some(8));
        assert_eq!(runtime.exec_mem_limit, Some(1 << 20));
        assert_eq!(runtime.runtime_filter_scan_wait_time_ms, Some(250));
        assert_eq!(runtime.runtime_filter_wait_timeout_ms, Some(5000));
        assert!(runtime.allow_throw_exception);
        assert_eq!(runtime.group_concat_max_len, Some(65_535));
        assert_eq!(runtime.enable_join_runtime_bitset_filter, Some(false));
        assert_eq!(runtime.global_runtime_filter_build_max_size, Some(1 << 19));
        assert!(runtime.orc_use_column_names);
        assert!(runtime.enable_file_metacache);
        assert!(runtime.enable_file_pagecache);
        assert!(runtime.enable_parquet_reader_page_index);
        assert_eq!(runtime.cache.datacache_evict_probability, Some(75));
        assert_eq!(runtime.cache.datacache_priority, Some(2));
        assert_eq!(runtime.cache.datacache_ttl_seconds, Some(3600));
        assert_eq!(runtime.cache.datacache_sharing_work_period, Some(10));
    }
}

#[cfg(test)]
mod typed_compile_source_tests {
    use super::*;
    use novarocks_execution_contract::failure::{
        FailureClass, FailureProjection, SharedFailureCause,
    };
    use std::error::Error;
    #[test]
    fn source_safe_detail_keeps_distinct_dependency_reasons_without_rendering_private_source() {
        use novarocks_query_application::execution_error::ExecutionFailureSite;
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

        // Identical private text (including a misleading reason) cannot decide
        // which source-authored fact reaches the public boundary.
        let private =
            "private-provider-locator (reason 4:8); Variant history may be related ".repeat(32);
        for (kind, reason) in [
            (ConnectorErrorKind::Unavailable, 8),
            (ConnectorErrorKind::PermissionDenied, 3),
            (ConnectorErrorKind::NotFound, 2),
        ] {
            let source = SharedFailureCause::new(ConnectorError::new(kind, private.clone()));
            let original_fact = source.failure_fact();
            let failure = DistributedQueryError::from_source(
                DistributedQueryErrorKind::Failed,
                source.clone(),
            );
            let carried = failure
                .source()
                .unwrap()
                .downcast_ref::<SharedFailureCause>()
                .unwrap();
            assert_eq!(carried, &source);
            assert_eq!(failure.compile_control_error(), None);

            let input = failure.into_execution_failure_input();
            assert_eq!(input.fact(), original_fact);
            assert_eq!(input.fact().class(), FailureClass::Dependency);
            assert_eq!(input.fact().reason().domain(), 4);
            assert_eq!(input.fact().reason().code(), reason);
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(
                input.detail().as_str(),
                format!(
                    "native attempt initialization failed in its owning domain (reason 4:{reason})"
                )
            );
            assert!(
                input.detail().as_str().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES
            );
            let public = input.user_error();
            assert_eq!(public.code().as_str(), "query.execution.dependency");
            assert_eq!(public.message(), input.detail().as_str());
            assert!(!public.to_string().contains("private-provider-locator"));
            assert!(!public.to_string().contains("Variant"));
            assert_eq!(public.message().contains("(reason 4:8)"), reason == 8);
        }
    }

    #[test]
    fn typed_compile_source_remains_original_without_forging_control_or_retry_proof() {
        let source = SharedFailureCause::new(novarocks_functions::ConstantError::Limit(
            "finite constant policy",
        ));
        let failure = DistributedQueryError::from_compile(
            novarocks_sql::compiler::SqlCompileError::CompilationSource(source.clone()),
        );
        let carried = failure
            .source()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        assert_eq!(carried, &source);
        assert_eq!(carried.failure_fact().class(), FailureClass::Resource);
        assert!(
            carried
                .downcast_ref::<novarocks_functions::ConstantError>()
                .is_some()
        );
        assert_eq!(failure.compile_control_error(), None);
    }
}

#[cfg(test)]
mod selected_execution_carrier_tests {
    use super::*;
    use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
    use novarocks_query_application::execution_error::{
        ExecutionFailureInput, ExecutionFailureSite,
    };

    #[test]
    fn selected_execution_carrier_preserves_its_original_input() {
        let source = novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Internal,
            "private provider diagnostic",
        );
        let original = ExecutionFailureInput::new(
            source.failure_fact(),
            SafeDetail::new("original safe task failure").unwrap(),
            ExecutionFailureSite::Coordinator,
        );
        let failure = DistributedQueryError::from_execution(original.clone());
        assert_eq!(failure.execution_failure(), Some(&original));
        assert_eq!(failure.kind(), DistributedQueryErrorKind::Failed);
        assert_eq!(failure.into_execution_failure_input(), original);
    }
}
