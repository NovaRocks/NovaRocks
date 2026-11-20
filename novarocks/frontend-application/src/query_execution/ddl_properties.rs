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

//! DDL constants on the caller's original supervised planning CPU turn.
//! All services, SQL settings, control, and result custody are borrowed from
//! that statement. This adapter submits no work and creates no second Root.

use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
use novarocks_functions::{ConstantPolicy, ConstantValue};
use novarocks_parser::ast::Expr;
use novarocks_query_application::{
    session_error::{QueryServiceError, QueryServiceErrorKind},
    sql::property_evaluation::{DdlPropertyConstantError, DdlPropertyConstantPort},
};
use novarocks_sql::{
    analyze_error::AnalyzeErrorKind,
    compiler::{
        PreparedRequiredConstant, RequiredConstantInput, RequiredConstantRequest,
        RequiredConstantTarget, SqlCatalogSnapshot, SqlCompileControl, SqlCompileError,
        SqlConstantEvaluationError, SqlFunctionCatalog, prepare_required_constant,
    },
    sql_mode::SqlSemanticSettings,
};
use novarocks_type_contract::CompileControlError;
use novarocks_workload_control::{ResultWindowAlias, WorkScope};
use std::sync::Arc;

pub(crate) struct DdlPropertyConstantEvaluator<'a> {
    pub(crate) catalog: &'a dyn SqlCatalogSnapshot,
    pub(crate) current_database: &'a str,
    pub(crate) functions: Arc<dyn SqlFunctionCatalog>,
    pub(crate) sql_semantics: &'a SqlSemanticSettings,
    pub(crate) constant_policy: ConstantPolicy,
    pub(crate) control: &'a SqlCompileControl,
    pub(crate) interpreter: &'a super::required_constant_eval::RequiredConstantInterpreter,
    pub(crate) scope: &'a WorkScope,
    pub(crate) window: &'a ResultWindowAlias,
}

impl DdlPropertyConstantEvaluator<'_> {
    /// Catalog snapshot borrowing ends with this pure preparation call. The
    /// original caller may then release its catalog read guard before evaluate.
    pub(crate) fn prepare(
        &self,
        expression: &Expr,
        target: RequiredConstantTarget,
    ) -> Result<PreparedRequiredConstant, DdlPropertyConstantError> {
        prepare_required_constant(RequiredConstantRequest {
            input: RequiredConstantInput::Expression(expression),
            target,
            catalog: self.catalog,
            current_database: self.current_database,
            functions: self.functions.clone(),
            sealed_functions: self.interpreter.sealed_functions(),
            sql_semantics: self.sql_semantics,
            constant_policy: self.constant_policy,
            control: self.control,
        })
        .map_err(compile_error)
    }
}

impl DdlPropertyConstantPort for DdlPropertyConstantEvaluator<'_> {
    fn evaluate(
        &mut self,
        expression: &Expr,
        target: RequiredConstantTarget,
    ) -> Result<ConstantValue, DdlPropertyConstantError> {
        let prepared = self.prepare(expression, target)?;
        self.interpreter
            .evaluate(prepared, self.scope, self.window)
            .map_err(evaluation_error)
    }
}

fn internal(message: impl Into<String>) -> DdlPropertyConstantError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message).into()
}

fn compile_error(error: SqlCompileError) -> DdlPropertyConstantError {
    match error {
        SqlCompileError::Cancelled => {
            QueryServiceError::from_compile_control(CompileControlError::Cancelled).into()
        }
        SqlCompileError::DeadlineExceeded => {
            QueryServiceError::from_compile_control(CompileControlError::DeadlineExceeded).into()
        }
        SqlCompileError::ResourceExhausted => {
            QueryServiceError::from_compile_control(CompileControlError::ResourceExhausted).into()
        }
        SqlCompileError::CompilationSource(cause) => {
            QueryServiceError::from_failure_cause(cause).into()
        }
        SqlCompileError::Analyze(error) => {
            if let Some(control) = error.control_error() {
                return QueryServiceError::from_compile_control(control).into();
            }
            if let Some(cause) = error.source_failure() {
                return QueryServiceError::from_failure_cause(cause.clone()).into();
            }
            match error.kind() {
                AnalyzeErrorKind::Internal => internal(error.to_string()),
                AnalyzeErrorKind::UnavailableImplementation => {
                    QueryServiceError::from_user_error(error.to_user_error(None)).into()
                }
                _ => DdlPropertyConstantError::invalid_value(error.to_string()),
            }
        }
        SqlCompileError::InvalidRequest(message) | SqlCompileError::Compilation(message) => {
            internal(message)
        }
    }
}

pub(crate) fn evaluation_error(error: SqlConstantEvaluationError) -> DdlPropertyConstantError {
    match error {
        SqlConstantEvaluationError::Control(error) => {
            QueryServiceError::from_compile_control(error).into()
        }
        SqlConstantEvaluationError::Constant(error) => {
            QueryServiceError::from_failure_cause(SharedFailureCause::new(error)).into()
        }
        SqlConstantEvaluationError::Outer(cause) => {
            QueryServiceError::from_failure_cause(cause).into()
        }
        SqlConstantEvaluationError::Source(cause) => {
            // Only these two original authors describe invalid required SQL
            // input. A different Data or capability failure keeps its author.
            let selected_row = cause.failure_fact().class() == FailureClass::Data
                && cause
                    .downcast_ref::<novarocks_expression::RequiredExpressionRowError>()
                    .is_some();
            let eligibility = cause
                .downcast_ref::<super::required_constant_eval::RequiredConstantEligibilityFailure>()
                .is_some();
            if selected_row || eligibility {
                DdlPropertyConstantError::invalid_value_with_cause(cause.to_string(), cause)
            } else {
                QueryServiceError::from_failure_cause(cause).into()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actual_cast_capability_and_outer_data_do_not_become_property_input_errors() {
        use arrow::datatypes::DataType;
        use novarocks_functions::{CastOperation, CastPrepareError, PreparedCastRecipe};
        use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
        let refusal = PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &FunctionValueType::new(DataType::Date32, false),
            &FunctionValueType::new(DataType::Int8, true),
            DecimalOverflowPolicy::OutputNull,
            false,
            None,
            &SqlCompileControl::unbounded(),
        )
        .unwrap_err();
        assert_eq!(refusal, CastPrepareError::Unsupported);
        let capability = SharedFailureCause::new(refusal);
        let data = SharedFailureCause::new(novarocks_functions::RowDataError::with_reason(
            0,
            novarocks_functions::DataFailureReason::DivisionByZero,
            "original outer data failure",
        ));
        for cause in [capability, data] {
            for original in [
                SqlConstantEvaluationError::Source(cause.clone()),
                SqlConstantEvaluationError::Outer(cause.clone()),
            ] {
                let DdlPropertyConstantError::Failure(error) = evaluation_error(original) else {
                    panic!("original failure author must survive")
                };
                assert_eq!(error.failure_cause(), Some(&cause));
            }
            let DdlPropertyConstantError::Failure(error) =
                compile_error(SqlCompileError::CompilationSource(cause.clone()))
            else {
                panic!("original compile source must survive")
            };
            assert_eq!(error.failure_cause(), Some(&cause));
            let DdlPropertyConstantError::Failure(error) = evaluation_error(
                SqlConstantEvaluationError::Control(CompileControlError::Source(cause.clone())),
            ) else {
                panic!("original control source must survive")
            };
            assert_eq!(
                error.compile_control_error(),
                Some(CompileControlError::Source(cause))
            );
        }
    }
}
