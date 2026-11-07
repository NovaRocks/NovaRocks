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

//! One frozen SET computation runs on the original supervised planning CPU.
use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
use novarocks_parser::{Span, ast};
use novarocks_query_application::sql::session::registry::{
    SessionPropertyError, SessionPropertyOrigin, SessionPropertyRegistry, SessionPropertyType,
};
use novarocks_query_application::sql::{
    session::{SessionSqlState, admit_session_set_assignment, session_property_error},
    session_admit::SessionAdmitError,
};
use novarocks_query_application::{
    session_effects::SessionEffects, session_error::QueryServiceError,
};
use novarocks_sql::compiler::{
    RequiredConstantInput, RequiredConstantRequest, RequiredConstantTarget, SqlCompileControl,
    SqlCompileError, SqlConstantEvaluationError, SqlFunctionCatalog, SqlPlannerTableSnapshot,
    prepare_required_constant,
};
use novarocks_workload_control::{ResultWindowAlias, WorkScope};
use std::sync::{Arc, RwLock};

pub(crate) struct SessionPropertyCpuInput {
    pub(crate) source: Arc<str>,
    pub(crate) set: ast::SetStatement,
    pub(crate) staged: SessionSqlState,
    pub(crate) registry: Arc<SessionPropertyRegistry>,
    pub(crate) interpreter: Arc<crate::RequiredConstantInterpreter>,
    pub(crate) control: SqlCompileControl,
    pub(crate) scope: WorkScope,
    pub(crate) window: ResultWindowAlias,
    pub(crate) catalog: Arc<RwLock<novarocks_sql::planning::catalog::PlannerMemoryCatalog>>,
    pub(crate) functions: Arc<dyn SqlFunctionCatalog>,
    pub(crate) policy: novarocks_functions::ConstantPolicy,
}
pub(crate) struct PreparedSessionProperties {
    pub(crate) staged: SessionSqlState,
    pub(crate) effects: SessionEffects,
}
impl SessionPropertyCpuInput {
    pub(crate) fn run(mut self) -> Result<PreparedSessionProperties, QueryServiceError> {
        let mut effects = SessionEffects::default();
        for assignment in &self.set.assignments {
            admit_session_set_assignment(&self.source, assignment)?;
            let span = assignment.value.span();
            let declaration = self
                .registry
                .lookup_system(&assignment.name.value, SessionPropertyOrigin::Sql)
                .map_err(|e| property_error_at(e, &self.source, span))?;
            let target = match declaration.value_type() {
                SessionPropertyType::Boolean => RequiredConstantTarget::Boolean,
                SessionPropertyType::Integer => RequiredConstantTarget::Integer,
                SessionPropertyType::Bigint => RequiredConstantTarget::Bigint,
                SessionPropertyType::Double => RequiredConstantTarget::Double,
                SessionPropertyType::Varchar => RequiredConstantTarget::Varchar,
            };
            let prepared = {
                let catalog = self.catalog.read().map_err(super::poisoned_state)?;
                prepare_required_constant(RequiredConstantRequest {
                    input: RequiredConstantInput::Expression(&assignment.value),
                    target,
                    catalog: &SqlPlannerTableSnapshot::new(&*catalog),
                    current_database: self.staged.current_database(),
                    functions: self.functions.clone(),
                    sql_semantics: self.staged.sql_semantics(),
                    constant_policy: self.policy,
                    control: &self.control,
                    version: crate::query_execution::physical_encoding::mint_plan_version(),
                })
            }
            .map_err(compile_error)?;
            let value = self
                .interpreter
                .evaluate(prepared, &self.scope, &self.window)
                .map_err(|e| evaluation_error(e, &assignment.name.value))?;
            let property = declaration
                .validate_sql_value(&value, &self.control)
                .map_err(|e| property_error_at(e, &self.source, span))?;
            effects.append(SessionEffects::property(property));
        }
        // Projection is still CPU work. The live session is untouched until
        // this whole computation has returned and its original worker exited.
        self.staged.apply_effects(&effects)?;
        Ok(PreparedSessionProperties {
            staged: self.staged,
            effects,
        })
    }
}
fn property_error_at(error: SessionPropertyError, source: &str, span: Span) -> QueryServiceError {
    match error {
        e @ (SessionPropertyError::UnknownProperty { .. }
        | SessionPropertyError::InvalidValue { .. }) => QueryServiceError::from_user_error(
            SessionAdmitError::InvalidSessionProperty.to_user_error(source, span, e.to_string()),
        ),
        other => session_property_error(other),
    }
}
fn compile_error(error: SqlCompileError) -> QueryServiceError {
    use novarocks_type_contract::CompileControlError as C;
    match error {
        SqlCompileError::Cancelled => QueryServiceError::from_compile_control(C::Cancelled),
        SqlCompileError::DeadlineExceeded => {
            QueryServiceError::from_compile_control(C::DeadlineExceeded)
        }
        SqlCompileError::ResourceExhausted => {
            QueryServiceError::from_compile_control(C::ResourceExhausted)
        }
        SqlCompileError::CompilationSource(cause) => QueryServiceError::from_failure_cause(cause),
        SqlCompileError::Analyze(error) => {
            if let Some(control) = error.control_error() {
                return QueryServiceError::from_compile_control(control);
            }
            if let Some(cause) = error.source_failure() {
                return QueryServiceError::from_failure_cause(cause.clone());
            }
            if error.kind() == novarocks_sql::analyze_error::AnalyzeErrorKind::Internal {
                return super::internal_error(error.to_string());
            }
            if error.kind()
                == novarocks_sql::analyze_error::AnalyzeErrorKind::UnavailableImplementation
            {
                return QueryServiceError::from_user_error(error.to_user_error(None));
            }
            QueryServiceError::from_user_error(
                SessionAdmitError::InvalidSessionProperty.without_location(error.to_string()),
            )
        }
        other => super::internal_error(other.to_string()),
    }
}
fn evaluation_error(error: SqlConstantEvaluationError, name: &str) -> QueryServiceError {
    match error {
        SqlConstantEvaluationError::Control(error) => {
            QueryServiceError::from_compile_control(error)
        }
        SqlConstantEvaluationError::Constant(error) => {
            QueryServiceError::from_failure_cause(SharedFailureCause::new(error))
        }
        SqlConstantEvaluationError::Outer(cause) => QueryServiceError::from_failure_cause(cause),
        SqlConstantEvaluationError::Source(cause) => {
            let selected_data = cause.failure_fact().class() == FailureClass::Data
                && cause
                    .downcast_ref::<novarocks_execution::runtime::task::ExecutionFailure>()
                    .is_some_and(|error| {
                        matches!(
                            error.cause(),
                            novarocks_execution::runtime::task::ExecutionFailureCause::RequiredRow(
                                _
                            )
                        )
                    });
            let required_capability = cause.downcast_ref::<crate::query_execution::required_constant_eval::RequiredConstantEligibilityFailure>().is_some();
            if selected_data || required_capability {
                QueryServiceError::from_user_error_with_cause(
                    SessionAdmitError::InvalidSessionProperty.without_location(format!(
                        "Unable to set session property '{name}': {cause}"
                    )),
                    cause,
                )
            } else {
                QueryServiceError::from_failure_cause(cause)
            }
        }
        SqlConstantEvaluationError::Evaluation(message) => super::internal_error(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };

    struct Fixture {
        root: RootWork,
        window: ResultWindowAlias,
        _control: WorkloadControl,
    }
    impl Fixture {
        fn new() -> Self {
            let control = WorkloadControl::try_new(
                WorkloadConfig::default(),
                ResourceConfig {
                    total_bytes: 1 << 20,
                    control_bytes: 1024,
                    per_scope_bytes: (1 << 20) - 1024,
                },
            )
            .unwrap();
            control
                .configure_result_capacity(ResultCapacityConfig::V1)
                .unwrap();
            control.mark_ready().unwrap();
            let (root, grant) = control
                .root_admission()
                .try_begin_root_with_result(
                    WorkRequest::new(WorkClass::Management),
                    novarocks_workload_control::ResultWindowClass::Local,
                )
                .unwrap();
            Self {
                root,
                window: grant.retain_alias(),
                _control: control,
            }
        }
        fn input(&self, sql: &str, staged: SessionSqlState) -> SessionPropertyCpuInput {
            let statements = novarocks_parser::parse(sql).unwrap();
            let [ast::Statement::Session(ast::SessionStatement::Set(set))] = statements.as_slice()
            else {
                panic!("SET fixture");
            };
            SessionPropertyCpuInput {
                source: Arc::from(sql), set: set.clone(), staged,
                registry: Arc::new(SessionPropertyRegistry::new().unwrap()),
                interpreter: Arc::new(crate::query_execution::required_constant_eval::test_required_constant_interpreter()),
                control: SqlCompileControl::unbounded(), scope: self.root.owner.scope(), window: self.window.clone(),
                catalog: Arc::new(RwLock::new(Default::default())),
                functions: Arc::new(novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap()),
                policy: crate::application::test_constant_policy(),
            }
        }
        fn close(self) {
            assert_eq!(self.window.retained_payload_bytes(), 0);
            drop(self.window);
            self.root.owner.complete();
            self.root.business.release();
        }
    }
    #[test]
    fn whole_set_prepares_typed_values_and_effects_without_optional_folding() {
        let fixture = Fixture::new();
        let mut state = SessionSqlState::default();
        state
            .apply_session_property_text(
                &SessionPropertyRegistry::new().unwrap(),
                "disable_optimizer_rules",
                "FoldConstant",
            )
            .unwrap();
        let prepared = fixture
            .input(
                "SET pipeline_dop = 1 + 2, enable_global_runtime_filter = false AND (1 / 0 = 1)",
                state,
            )
            .run()
            .unwrap();
        assert_eq!(prepared.staged.execution_settings().pipeline_dop(), Some(3));
        assert_eq!(
            prepared.effects.set_properties().collect::<Vec<_>>(),
            vec![
                ("pipeline_dop".into(), "3".into()),
                ("enable_global_runtime_filter".into(), "false".into())
            ]
        );
        assert!(fixture.window.retained_payload_bytes() > 0);
        drop(prepared);
        fixture.close();
    }
    #[test]
    fn later_assignment_refusal_does_not_publish_any_earlier_property() {
        let fixture = Fixture::new();
        let state = SessionSqlState::default();
        let error = fixture
            .input(
                "SET pipeline_dop = 1 + 2, trc_unknown = true",
                state.clone(),
            )
            .run()
            .err()
            .unwrap();
        assert_eq!(
            error.user_error().unwrap().code().as_str(),
            "sql.admit.invalid_session_property"
        );
        assert_eq!(state.execution_settings().pipeline_dop(), None);
        assert!(state.render_session_property("pipeline_dop").is_none());
        assert!(error.user_error().unwrap().location().is_some());
        drop(error);
        fixture.close();
    }
    #[test]
    fn property_locations_distinguish_lookup_validation_and_required_evaluation() {
        let fixture = Fixture::new();
        for (sql, location) in [
            ("SET trc_unknown = 1", true),
            ("SET query_timeout = -1", true),
            ("SET pipeline_dop = 1.5", false),
            ("SET pipeline_dop = NULL", false),
            ("SET pipeline_dop = missing_column", false),
            ("SET pipeline_dop = (SELECT 1)", false),
            ("SET pipeline_dop = 1 / 0", false),
        ] {
            let error = fixture
                .input(sql, SessionSqlState::default())
                .run()
                .err()
                .unwrap_or_else(|| panic!("{sql} must fail"));
            let user = error.user_error().unwrap();
            assert_eq!(
                user.code().as_str(),
                "sql.admit.invalid_session_property",
                "{sql}"
            );
            assert_eq!(user.location().is_some(), location, "{sql}");
        }
        fixture.close();
    }

    #[test]
    fn declared_required_ineligibility_is_distinct_from_missing_execution_capability() {
        let original = SharedFailureCause::new(
            novarocks_sql::compiler::PackageSemanticsError::UnsupportedCapability(
                "an uninstalled execution capability".into(),
            ),
        );
        let error = evaluation_error(
            SqlConstantEvaluationError::Source(original.clone()),
            "pipeline_dop",
        );
        assert_eq!(error.failure_cause(), Some(&original));
        assert!(
            error
                .user_error()
                .is_none_or(|error| error.code().as_str() != "sql.admit.invalid_session_property")
        );
        let eligibility = SharedFailureCause::new(
            crate::query_execution::required_constant_eval::RequiredConstantEligibilityFailure::new(
            ),
        );
        let error = evaluation_error(
            SqlConstantEvaluationError::Source(eligibility.clone()),
            "pipeline_dop",
        );
        assert_eq!(
            error.user_error().unwrap().code().as_str(),
            "sql.admit.invalid_session_property"
        );
        assert_eq!(error.failure_cause(), Some(&eligibility));
    }

    #[derive(Debug)]
    struct OriginalData;
    impl std::fmt::Display for OriginalData {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("selected division failure")
        }
    }
    impl std::error::Error for OriginalData {}
    impl FailureProjection for OriginalData {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            novarocks_functions::DataFailureReason::DivisionByZero.failure_fact()
        }
    }
    #[test]
    fn property_business_boundary_preserves_outer_and_control_source_identity() {
        let source = SharedFailureCause::new(OriginalData);
        let source_error = evaluation_error(
            SqlConstantEvaluationError::Source(source.clone()),
            "pipeline_dop",
        );
        assert_eq!(source_error.failure_cause(), Some(&source));
        assert!(
            source_error
                .user_error()
                .is_none_or(|error| error.code().as_str() != "sql.admit.invalid_session_property")
        );
        let compile = compile_error(SqlCompileError::CompilationSource(source.clone()));
        assert_eq!(compile.failure_cause(), Some(&source));
        assert!(
            compile
                .user_error()
                .is_none_or(|error| error.code().as_str() != "sql.admit.invalid_session_property")
        );
        let outer = evaluation_error(
            SqlConstantEvaluationError::Outer(source.clone()),
            "pipeline_dop",
        );
        assert_eq!(outer.failure_cause(), Some(&source));
        assert!(
            outer
                .user_error()
                .is_none_or(|error| error.code().as_str() != "sql.admit.invalid_session_property")
        );
        let control = evaluation_error(
            SqlConstantEvaluationError::Control(
                novarocks_type_contract::CompileControlError::Source(source.clone()),
            ),
            "pipeline_dop",
        );
        assert!(
            control
                .user_error()
                .is_none_or(|error| error.code().as_str() != "sql.admit.invalid_session_property")
        );
        let Some(novarocks_type_contract::CompileControlError::Source(original)) =
            control.compile_control_error()
        else {
            panic!("original control source");
        };
        assert_eq!(original, source);
    }
}
