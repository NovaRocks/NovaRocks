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

//! Frontend-owned SQL admission and native assembly boundary.

use std::sync::Arc;

use crate::catalog_application::information_schema;
use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::connector::connector_planning_context_for_query_on_runtime;
use crate::mv::domain::readiness::{MvCandidateReader, MvReadinessPort};
use crate::query_execution::compiler::query_catalog_service_snapshot;
use crate::query_execution::completion::PreparedQueryOperation;
use crate::query_execution::kernels::{
    QueryPreparationKernel, SystemTableQueryKernel, ViewExecutionKernel,
};
use crate::query_execution::planning::sql_compile_control_from_execution;
use crate::query_execution::planning::time_travel::{TimeTravelRewriteError, has_time_travel_refs};
use novarocks_parser::ast::{ExplainFormat, ExplainQuery, Query, Statement};
use novarocks_physical_plan::{
    MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, ScanReadBudget,
};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::admitted_query_context::{QueryExecutionContext, RequestContext};
use novarocks_query_application::view::ViewRequestContext;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_sql::analyze_error::AnalyzeError;
use novarocks_sql::compiler::{
    ExplainLevel, SqlCompileError, SqlCompileIntent, SqlPlanningEnvironment, SqlSessionContext,
    SqlStatementInput,
};

/// Application intent is supplied by the caller, never inferred from a
/// physical plan's column count. Wire installation remains a separate cut.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FrontendQueryPurpose {
    ClientRows,
    LocalRows,
    ScalarValue,
}

impl FrontendQueryPurpose {
    pub(crate) fn client_query(query: &Query) -> Self {
        if information_schema::is_local_materialized_views_query(query) {
            Self::LocalRows
        } else {
            Self::ClientRows
        }
    }

    pub(crate) fn window_class(self) -> novarocks_workload_control::ResultWindowClass {
        use novarocks_workload_control::ResultWindowClass;
        match self {
            Self::ClientRows => ResultWindowClass::Client,
            Self::LocalRows => ResultWindowClass::Local,
            Self::ScalarValue => ResultWindowClass::Internal,
        }
    }

    fn validate(self, statement: &Statement) -> Result<(), FrontendQueryCompilerError> {
        let valid = match (self, statement) {
            (Self::ClientRows, Statement::Query(query)) => {
                !information_schema::is_local_materialized_views_query(query)
            }
            (Self::LocalRows, Statement::Query(query)) => {
                information_schema::is_local_materialized_views_query(query)
            }
            (Self::ScalarValue, Statement::Query(_)) => true,
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(FrontendQueryCompilerError::Engine(
                "query preparation purpose does not match the admitted statement".to_owned(),
            ))
        }
    }
}

/// Preserves SQL analyze-domain facts until the session still has the original
/// SQL source required to render a user location.
#[derive(Debug)]
pub(crate) enum FrontendQueryCompilerError {
    Application(novarocks_query_application::session_error::QueryServiceError),
    Engine(String),
    Analyze(AnalyzeError),
    Source(novarocks_failure_contract::SharedFailureCause),
    Control(novarocks_type_contract::CompileControlError),
}

impl FrontendQueryCompilerError {
    pub(crate) fn from_compile(error: SqlCompileError) -> Self {
        match error {
            SqlCompileError::Analyze(error) => match error.control_error() {
                Some(control) => Self::Control(control),
                None => Self::Analyze(error),
            },
            SqlCompileError::Cancelled => {
                Self::Control(novarocks_type_contract::CompileControlError::Cancelled)
            }
            SqlCompileError::DeadlineExceeded => {
                Self::Control(novarocks_type_contract::CompileControlError::DeadlineExceeded)
            }
            SqlCompileError::ResourceExhausted => {
                Self::Control(novarocks_type_contract::CompileControlError::ResourceExhausted)
            }
            SqlCompileError::CompilationSource(source) => Self::Source(source),
            error => Self::Engine(error.to_string()),
        }
    }
    pub(crate) fn from_encode(error: novarocks_plan_codec::PhysicalEncodeError) -> Self {
        match error {
            novarocks_plan_codec::PhysicalEncodeError::Control(error) => Self::Control(error),
            other => Self::Source(novarocks_failure_contract::SharedFailureCause::new(other)),
        }
    }
    pub(crate) fn from_completion(
        error: &novarocks_query_application::preparation::FinalPlanCompletionError,
    ) -> Self {
        use novarocks_query_application::preparation::FinalPlanCompletionError as E;
        match error {
            E::Cancelled { .. } => {
                Self::Control(novarocks_type_contract::CompileControlError::Cancelled)
            }
            E::DeadlineExceeded => {
                Self::Control(novarocks_type_contract::CompileControlError::DeadlineExceeded)
            }
            E::ResourceExhausted => {
                Self::Control(novarocks_type_contract::CompileControlError::ResourceExhausted)
            }
            E::Source { source } => Self::Source(source.clone()),
            E::Analyze { error } => match error.control_error() {
                Some(control) => Self::Control(control),
                None => Self::Analyze(error.clone()),
            },
            error => Self::Engine(error.to_string()),
        }
    }
}

impl From<String> for FrontendQueryCompilerError {
    fn from(error: String) -> Self {
        Self::Engine(error)
    }
}

impl From<TimeTravelRewriteError> for FrontendQueryCompilerError {
    fn from(error: TimeTravelRewriteError) -> Self {
        match error {
            TimeTravelRewriteError::Engine(error) => Self::Engine(error),
            TimeTravelRewriteError::Analyze(error) => Self::Analyze(error),
        }
    }
}

#[derive(Clone)]
pub(crate) struct FrontendQueryCompiler {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    query: QueryPreparationKernel,
    view: ViewExecutionKernel,
    system_tables: SystemTableQueryKernel,
    mv_readiness: Arc<MvReadinessPort>,
    mv_candidate_reader: MvCandidateReader,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    /// The bounded lane every owner that may reach a remote catalog answers
    /// on, so one slow catalog cannot stall the statement threads.
    connector_blocking_io: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
}

mod async_explain;
mod async_read;
pub(crate) use async_explain::{
    ExplainPreparationPorts, PreparedAsyncExplain, PreparedAsyncProfile,
};

impl FrontendQueryCompiler {
    pub(crate) fn prepare_show_views_action(
        &self,
        show: novarocks_parser::ast::ShowViews,
        request: novarocks_query_application::admitted_query_context::RequestContext,
        command: novarocks_query_application::api::CommandContext,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        novarocks_query_application::api::CommandError,
    > {
        crate::query::view_read_action::prepare(
            self.view.clone(),
            &self.connector_blocking_io,
            show,
            request,
            command,
        )
    }

    pub(crate) fn validate_original_read_runtime(
        &self,
    ) -> Result<(), novarocks_workload_control::WorkError> {
        self.connector_blocking_io.validate_original_read_runtime()
    }
    pub(crate) fn connector_runtime(&self) -> &tokio::runtime::Handle {
        self.connector_blocking_io.runtime()
    }

    pub(crate) fn new(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        query: QueryPreparationKernel,
        view: ViewExecutionKernel,
        system_tables: SystemTableQueryKernel,
        mv_readiness: Arc<MvReadinessPort>,
        mv_candidate_reader: MvCandidateReader,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
        connector_blocking_io: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
        constant_policy: novarocks_functions::ConstantPolicy,
    ) -> Self {
        Self {
            functions,
            constant_policy,
            query,
            view,
            system_tables,
            mv_readiness,
            mv_candidate_reader,
            mv_storage_observation,
            connector_blocking_io,
        }
    }

    pub(crate) const fn constant_policy(&self) -> novarocks_functions::ConstantPolicy {
        self.constant_policy
    }

    /// The owners in this process that can answer what completing a statement
    /// asks, and the lane their synchronous calls are admitted through.
    fn completion_fact_owners(
        &self,
    ) -> crate::query_execution::completion_facts::CompletionFactOwners {
        crate::query_execution::completion_facts::CompletionFactOwners::new(
            Arc::new(query_catalog_service_snapshot(&self.query)),
            self.query.catalog_application().cloned(),
            Arc::clone(self.query.typed_connector_control()),
            Arc::clone(self.query.unified_statistics()),
            self.mv_candidate_reader.clone(),
            Arc::clone(&self.mv_storage_observation),
            self.connector_blocking_io.clone(),
        )
    }

    /// Freeze the same per-instance driver width that request construction
    /// will resolve from these query options. Backend count governs placement.
    fn pipeline_dop_domain(
        options: Option<&QueryOptions>,
    ) -> Result<PipelineDopDomain, FrontendQueryCompilerError> {
        crate::query_execution::contract::completed_plan_dop_domain(options)
            .map_err(FrontendQueryCompilerError::Engine)
    }

    /// How much one scan may return in a batch.
    ///
    /// This is a ceiling, not a target. The session may lower the row count;
    /// nothing in this deployment lowers the byte count, so it stays at the
    /// contract's own maximum, which states "unconstrained" rather than a
    /// number someone chose.
    fn scan_read_budget(query_options: Option<&QueryOptions>) -> ScanReadBudget {
        let max_batch_rows = query_options
            .map(|options| options.as_proto().batch_size)
            .and_then(|rows| u64::try_from(rows).ok())
            .filter(|rows| *rows > 0 && *rows <= MAX_SCAN_BATCH_ROWS)
            .unwrap_or(MAX_SCAN_BATCH_ROWS);
        ScanReadBudget {
            max_batch_rows,
            max_batch_bytes: MAX_SCAN_BATCH_BYTES,
        }
    }
}

fn query_options_for_explain_analyze(query_options: Option<QueryOptions>) -> QueryOptions {
    let mut raw = query_options
        .as_ref()
        .map(|options| *options.as_proto())
        .unwrap_or_default();
    raw.enable_profile = true;
    QueryOptions::from_proto(raw)
}

pub(crate) fn explain_mode(explain: &ExplainQuery) -> (ExplainLevel, bool) {
    let level = match explain.format {
        ExplainFormat::Default => ExplainLevel::Normal,
        ExplainFormat::Verbose => ExplainLevel::Verbose,
        ExplainFormat::Costs => ExplainLevel::Costs,
        ExplainFormat::Logical => ExplainLevel::Normal,
        ExplainFormat::Analyze => ExplainLevel::Analyze,
        ExplainFormat::Contract => ExplainLevel::Contract,
    };
    (
        level,
        explain.logical || matches!(explain.format, ExplainFormat::Logical),
    )
}

pub(crate) fn scalar_root_output_from_public_port(
    port: Option<&novarocks_physical_plan::ResultPort>,
) -> Result<novarocks_result_contract::FrozenRootOutput, FrontendQueryCompilerError> {
    novarocks_sql::compiler::validate_public_result_logical_identities(port)
        .map_err(FrontendQueryCompilerError::Engine)?;
    Ok(novarocks_result_contract::FrozenRootOutput::ScalarValue(
        port.and_then(|port| port.scalar_schema.clone())
            .ok_or_else(|| {
                FrontendQueryCompilerError::Engine(
                    "completed scalar query has no frozen SQL scalar schema".into(),
                )
            })?,
    ))
}

#[cfg(test)]
mod compile_control_tests {
    use super::*;
    use novarocks_type_contract::CompileControlError;

    fn service(
        error: FrontendQueryCompilerError,
    ) -> novarocks_query_application::session_error::QueryServiceError {
        super::super::routed_execution_service_error(super::super::routed_compiler_failure(
            error,
            "SELECT value",
        ))
    }

    #[test]
    fn original_repository_failure_keeps_its_domain_without_compile_provenance() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        use novarocks_query_application::session_error::QueryServiceError;
        let original =
            SharedFailureCause::new(novarocks_state_store_api::StateStoreOriginalUnsupported);
        let expected = original.failure_fact();
        let error = QueryServiceError::from_failure_cause(original.clone());
        let governed = service(FrontendQueryCompilerError::Application(error.clone()));
        let routed =
            super::super::routed_execution_service_error(super::super::routed_compiler_failure(
                FrontendQueryCompilerError::Application(error),
                "SELECT * FROM information_schema.materialized_views",
            ));
        for actual in [governed, routed] {
            assert_eq!(actual.failure_cause(), Some(&original));
            assert_eq!(actual.execution_failure().unwrap().fact(), expected);
            assert!(actual.compile_control_error().is_none());
        }
    }

    #[test]
    fn compilation_source_keeps_original_resource_fact_through_the_real_session_consumer() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_query_application::execution_error::ExecutionFailureSite;
        let domain = novarocks_functions::ConstantError::arrow_validation(
            arrow::error::ArrowError::MemoryError("provider-local allocation detail".into()),
        );
        let expected = domain.failure_fact();
        let sql_error = SqlCompileError::from(domain);
        let SqlCompileError::CompilationSource(source) = &sql_error else {
            panic!("the constant author must retain its source")
        };
        let original = source.clone();
        let error = FrontendQueryCompilerError::from_compile(sql_error);
        assert!(
            matches!(&error, FrontendQueryCompilerError::Source(actual) if actual == &original)
        );
        let routed = super::super::routed_compiler_failure(
            FrontendQueryCompilerError::from_compile(SqlCompileError::CompilationSource(
                original.clone(),
            )),
            "EXPLAIN SELECT value",
        );
        let super::super::RoutedExecutionError::Control(control) = routed else {
            panic!("the EXPLAIN consumer must retain its nominal source")
        };
        assert_eq!(control, CompileControlError::Source(original.clone()));
        let routed_service =
            novarocks_query_application::session_error::QueryServiceError::from_compile_control(
                control,
            );
        assert_eq!(routed_service.execution_failure().unwrap().fact(), expected);
        assert_eq!(
            routed_service
                .execution_failure()
                .unwrap()
                .descriptor()
                .errno,
            1041
        );
        let service = service(error);
        let input = service.execution_failure().unwrap();
        assert_eq!(input.fact(), expected);
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(
            (input.descriptor().errno, input.descriptor().sqlstate),
            (1041, "HY000")
        );
        assert_eq!(
            service.compile_control_error(),
            Some(CompileControlError::Source(original))
        );
        assert_eq!(
            service.user_error().unwrap().phase(),
            novarocks_user_error::ErrorPhase::Execution
        );
        assert_eq!(service.user_error().unwrap().location(), None);
        assert!(!service.message().contains("provider-local"));
    }

    #[test]
    fn actual_constant_limit_keeps_its_source_through_the_completion_session_consumer() {
        use arrow::datatypes::{DataType, Field};
        use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
        use novarocks_query_application::execution_error::ExecutionFailureSite;
        use novarocks_query_application::preparation::FinalPlanCompletionError;
        let mut policy = novarocks_plan_codec::physical_package_v2::FROZEN_CONSTANT_POLICY;
        policy.max_rows = 0;
        let cause = novarocks_functions::ConstantValue::from_i64(
            Arc::new(Field::new("value", DataType::Int64, false)),
            novarocks_functions::FunctionValueType::new(DataType::Int64, false),
            1,
            policy,
            novarocks_type_contract::CompilePhase::Validate,
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap_err();
        assert!(matches!(
            cause,
            novarocks_functions::ConstantError::Limit(_)
        ));
        let expected = cause.failure_fact();
        assert_eq!(expected.class(), FailureClass::Resource);
        assert_eq!(expected.reason().domain(), domain::CONSTANT.get());
        assert_eq!(expected.reason().code(), 2);
        let compile = SqlCompileError::from(cause);
        let SqlCompileError::CompilationSource(original) = &compile else {
            panic!("the constant author must retain its source");
        };
        let original = original.clone();
        let completion = FinalPlanCompletionError::Source {
            source: original.clone(),
        };
        for error in [
            FrontendQueryCompilerError::from_compile(compile),
            FrontendQueryCompilerError::from_completion(&completion),
        ] {
            assert!(
                matches!(&error, FrontendQueryCompilerError::Source(source) if source == &original)
            );
            let routed = service(error);
            let input = routed.execution_failure().unwrap();
            assert_eq!(input.fact(), expected);
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(
                (input.descriptor().errno, input.descriptor().sqlstate),
                (1041, "HY000")
            );
            assert_eq!(
                routed.compile_control_error(),
                Some(CompileControlError::Source(original.clone()))
            );
            assert_eq!(
                routed.user_error().unwrap().phase(),
                novarocks_user_error::ErrorPhase::Execution
            );
            assert_eq!(routed.user_error().unwrap().location(), None);
        }
    }

    #[test]
    fn real_checked_decimal_overflow_keeps_1690_through_compiler_and_session() {
        use novarocks_failure_contract::FailureProjection;
        let domain = novarocks_functions::legacy_decimal::pow10_i128(39).unwrap_err();
        let expected = domain.failure_fact();
        let domain = novarocks_functions::ConstantError::from_domain(domain);
        let sql_error = SqlCompileError::from(domain);
        let service = service(FrontendQueryCompilerError::from_compile(sql_error));
        let input = service.execution_failure().unwrap();
        assert_eq!(input.fact(), expected);
        assert_eq!(
            (input.descriptor().errno, input.descriptor().sqlstate),
            (1690, "22003")
        );
        let Some(CompileControlError::Source(source)) = service.compile_control_error() else {
            panic!("the original source must remain available locally")
        };
        assert!(
            source
                .downcast_ref::<novarocks_functions::ConstantError>()
                .is_some()
        );
        assert_eq!(
            service.user_error().unwrap().phase(),
            novarocks_user_error::ErrorPhase::Execution
        );
        assert_eq!(service.user_error().unwrap().location(), None);
    }

    #[test]
    fn ordinary_compile_strings_and_analyze_locations_keep_their_original_routes() {
        let service = service(FrontendQueryCompilerError::from_compile(
            SqlCompileError::Compilation("numeric overflow resource exhausted".into()),
        ));
        assert!(service.execution_failure().is_none());
        assert!(service.user_error().is_none());

        let sql = "SELECT missing";
        let error =
            AnalyzeError::unknown_column("unknown column", novarocks_parser::Span::new(7, 14));
        let expected = error.to_user_error(Some(sql));
        let service =
            super::super::routed_execution_service_error(super::super::routed_compiler_failure(
                FrontendQueryCompilerError::from_compile(SqlCompileError::Analyze(error)),
                sql,
            ));
        assert_eq!(service.user_error(), Some(&expected));
        assert!(service.execution_failure().is_none());
        assert!(service.user_error().unwrap().location().is_some());
    }

    #[test]
    fn encode_capability_refusal_preserves_the_client_engine_code() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        for original in [
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
                "CTE multicast".into(),
            ),
            novarocks_plan_codec::PhysicalEncodeError::Invalid("unsupported diagnostic".into()),
        ] {
            let expected = original.failure_fact();
            let FrontendQueryCompilerError::Source(source) =
                FrontendQueryCompilerError::from_encode(original)
            else {
                panic!("typed encode author must survive")
            };
            assert_eq!(source.failure_fact(), expected);
            assert!(
                source
                    .downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>()
                    .is_some()
            );
            assert_eq!(
                expected.class(),
                match source
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
            let service = super::super::routed_execution_service_error(
                super::super::routed_compiler_failure(
                    FrontendQueryCompilerError::Source(source),
                    "SELECT 1",
                ),
            );
            assert_eq!(service.execution_failure().unwrap().fact(), expected);
            let (code, message) = match expected.class() {
                FailureClass::Unsupported => (
                    "UnsupportedCompiledShape",
                    "[UnsupportedCompiledShape] pure compilation failed in its source domain",
                ),
                FailureClass::Invariant => (
                    "query.execution.invariant",
                    "[query.execution.invariant] pure compilation failed in its source domain",
                ),
                other => panic!("unexpected author class: {other:?}"),
            };
            assert_eq!(service.user_error().unwrap().code().as_str(), code);
            assert_eq!(service.message(), message);
        }
    }

    #[test]
    fn query_compile_encode_and_analyze_preserve_control_category() {
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let errors = [
                FrontendQueryCompilerError::from_compile(control.clone().into()),
                FrontendQueryCompilerError::from_encode(
                    novarocks_plan_codec::PhysicalEncodeError::Control(control.clone()),
                ),
                FrontendQueryCompilerError::from_compile(SqlCompileError::Analyze(
                    AnalyzeError::control(control.clone()),
                )),
            ];
            for error in errors {
                assert!(
                    matches!(error, FrontendQueryCompilerError::Control(actual) if actual == control)
                );
            }
            use novarocks_failure_contract::{FailureClass, FailureProjection};
            let original = novarocks_plan_codec::PhysicalEncodeError::Invalid(control.to_string());
            let expected = original.failure_fact();
            let FrontendQueryCompilerError::Source(source) =
                FrontendQueryCompilerError::from_encode(original)
            else {
                panic!("the original Invalid author must survive")
            };
            assert_eq!(source.failure_fact(), expected);
            assert_eq!(expected.class(), FailureClass::Invariant);
            assert_ne!(expected, control.failure_fact());
            assert!(matches!(
                source.downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>(),
                Some(novarocks_plan_codec::PhysicalEncodeError::Invalid(detail))
                    if detail == &control.to_string()
            ));
            let service = service(FrontendQueryCompilerError::Source(source));
            assert_eq!(service.execution_failure().unwrap().fact(), expected);
            assert_ne!(
                service.kind(),
                novarocks_query_application::session_error::QueryServiceErrorKind::Interrupted
            );
            assert_ne!(
                service.kind(),
                novarocks_query_application::session_error::QueryServiceErrorKind::Timeout
            );
        }
    }
    #[test]
    fn comparison_source_survives_freeze_encode_and_the_actual_session_consumer() {
        use crate::query_execution::package_freeze::PackageFreezeError;
        use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
        use novarocks_functions::ComparisonPrepareError;
        use novarocks_query_application::execution_error::ExecutionFailureSite;
        use novarocks_sql::compiler::PackageSemanticsError;
        for (original, class, public_code) in [
            (
                ComparisonPrepareError::Unsupported,
                FailureClass::Unsupported,
                "query.execution.unsupported",
            ),
            (
                ComparisonPrepareError::TypeMismatch,
                FailureClass::Invariant,
                "query.execution.invariant",
            ),
        ] {
            let original_source = SharedFailureCause::new(original.clone());
            let expected = original_source.failure_fact();
            assert_eq!(expected.class(), class);
            let encoded = novarocks_plan_codec::PhysicalEncodeError::from(
                PackageFreezeError::Semantics(PackageSemanticsError::PrimitiveSource {
                    source: original_source.clone(),
                    cast: None,
                }),
            );
            let error = FrontendQueryCompilerError::from_encode(encoded);
            let FrontendQueryCompilerError::Source(retained) = &error else {
                panic!("the encoded source must remain typed");
            };
            let retained = retained.clone();
            let Some(novarocks_plan_codec::PhysicalEncodeError::Source(freeze_source)) =
                retained.downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>()
            else {
                panic!("retain the actual physical encode source");
            };
            let Some(PackageFreezeError::Semantics(PackageSemanticsError::PrimitiveSource {
                source: actual,
                cast: None,
            })) = freeze_source.downcast_ref::<PackageFreezeError>()
            else {
                panic!("retain the exact comparison source below its wrappers");
            };
            assert_eq!(actual, &original_source);
            assert_eq!(actual.detail(), original_source.detail());
            assert_eq!(
                actual.downcast_ref::<ComparisonPrepareError>(),
                Some(&original)
            );
            let service = service(error);
            let failure = service.execution_failure().unwrap();
            assert_eq!(failure.fact(), expected);
            assert_eq!(failure.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(failure.descriptor().descriptor.code.as_str(), public_code);
            assert_eq!(
                service.message(),
                format!("[{public_code}] pure compilation failed in its source domain")
            );
            assert_eq!(
                service.compile_control_error(),
                Some(CompileControlError::Source(retained))
            );
        }
    }
}

#[cfg(test)]
mod purpose_tests {
    use super::*;

    fn statement(sql: &str) -> Statement {
        novarocks_query_application::sql::parse_single_statement(sql).unwrap()
    }

    #[test]
    fn public_scalar_complete_uuid_is_named_before_the_original_missing_proof_error() {
        use novarocks_physical_plan::{
            ResultField, ResultPort, ResultValueDomain, ValueId, ValueType,
        };
        use novarocks_type_contract::ValueLogicalType;
        let ty = ValueType::try_with_logical_type(
            arrow::datatypes::DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap();
        let port = ResultPort {
            fragment: novarocks_physical_plan::FragmentId::new(1),
            output: novarocks_physical_plan::OutputPort {
                node: novarocks_physical_plan::NodeId::new(1),
                columns: vec![ValueId::new(1)].into(),
            },
            fields: vec![ResultField {
                domain: ResultValueDomain::Plain,
                name: "u".into(),
                alias: None,
                value: ValueId::new(1),
                ty,
            }]
            .into(),
            scalar_schema: None,
        };
        let error = scalar_root_output_from_public_port(Some(&port)).unwrap_err();
        assert!(
            matches!(error, FrontendQueryCompilerError::Engine(ref text) if text.contains("UUID") && text.contains("unsupported"))
        );
        let error = scalar_root_output_from_public_port(None).unwrap_err();
        assert!(
            matches!(error, FrontendQueryCompilerError::Engine(ref text) if text == "completed scalar query has no frozen SQL scalar schema")
        );
    }

    #[test]
    fn client_admission_uses_only_the_exact_local_immediate_shape() {
        for sql in [
            "SELECT * FROM information_schema.materialized_views",
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.MATERIALIZED_VIEWS WHERE IS_ACTIVE = true ORDER BY TABLE_NAME",
        ] {
            let parsed = statement(sql);
            let Statement::Query(query) = &parsed else {
                panic!("expected query")
            };
            assert_eq!(
                FrontendQueryPurpose::client_query(query),
                FrontendQueryPurpose::LocalRows,
                "{sql}"
            );
            assert!(FrontendQueryPurpose::LocalRows.validate(&parsed).is_ok());
            assert!(FrontendQueryPurpose::ClientRows.validate(&parsed).is_err());
        }
        for sql in [
            "SELECT 1",
            "SELECT TABLE_NAME FROM information_schema.tables",
            "SELECT * FROM catalog.information_schema.materialized_views",
            "SELECT * FROM information_schema.materialized_views m JOIN t ON m.TABLE_NAME = t.name",
            "SELECT * FROM information_schema.materialized_views UNION ALL SELECT * FROM information_schema.materialized_views",
            "SELECT * FROM (SELECT * FROM information_schema.materialized_views) m",
        ] {
            let parsed = statement(sql);
            let Statement::Query(query) = &parsed else {
                panic!("expected query")
            };
            assert_eq!(
                FrontendQueryPurpose::client_query(query),
                FrontendQueryPurpose::ClientRows,
                "{sql}"
            );
            assert!(FrontendQueryPurpose::ClientRows.validate(&parsed).is_ok());
            assert!(FrontendQueryPurpose::LocalRows.validate(&parsed).is_err());
        }
    }

    #[test]
    fn synchronous_read_purposes_reject_explain_and_do_not_infer_scalar_output() {
        let select = statement("SELECT 1");
        assert!(FrontendQueryPurpose::ClientRows.validate(&select).is_ok());
        assert!(FrontendQueryPurpose::ScalarValue.validate(&select).is_ok());
        for sql in ["EXPLAIN SELECT 1", "EXPLAIN ANALYZE SELECT 1"] {
            let explain = statement(sql);
            for purpose in [
                FrontendQueryPurpose::ClientRows,
                FrontendQueryPurpose::LocalRows,
                FrontendQueryPurpose::ScalarValue,
            ] {
                assert!(purpose.validate(&explain).is_err(), "{sql}: {purpose:?}");
            }
        }
        let local = statement("SELECT TABLE_NAME FROM information_schema.materialized_views");
        assert!(FrontendQueryPurpose::ScalarValue.validate(&local).is_ok());
    }
}
