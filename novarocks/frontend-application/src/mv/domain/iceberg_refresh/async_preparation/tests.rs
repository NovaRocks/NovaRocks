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

use super::{DefinitionAnalysisFailure, RefreshError, definition_analysis_failure};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_sql::{
    analyze_error::{AnalyzeError, AnalyzeErrorKind},
    compiler::{
        SqlCompileControl, SqlCompileError, SqlMvRefreshAnalysisContext, SqlPlannerTableSnapshot,
        analyze_mv_refresh_input, build_builtin_engine_function_catalog,
    },
    planning::catalog::PlannerMemoryCatalog,
};

#[test]
fn definition_analysis_failure_keeps_only_confirmed_effect_rejections() {
    let query = novarocks_parser::parser::parse_query("SELECT IF(false, random(), 0e0) AS id")
        .expect("parse definition");
    let catalog = PlannerMemoryCatalog::default();
    let catalog = SqlPlannerTableSnapshot::new(&catalog);
    let functions = build_builtin_engine_function_catalog().expect("build function catalog");
    let effect = analyze_mv_refresh_input(SqlMvRefreshAnalysisContext {
        query: Box::new(query.clone()),
        current_database: "db".to_owned(),
        catalog: &catalog,
        functions: &functions,
        constant_policy: crate::application::test_constant_policy(),
        control: SqlCompileControl::unbounded(),
    })
    .expect_err("reject the effect in the original definition");
    let SqlCompileError::Analyze(effect) = effect else {
        panic!("expected a typed analysis rejection");
    };
    assert_eq!(effect.kind(), AnalyzeErrorKind::MvDefinitionEffect);
    let unsupported = AnalyzeError::unsupported_expression(effect.message(), query.span);

    let rejected = RefreshError::from(SqlCompileError::Analyze(effect.clone()));
    let DefinitionAnalysisFailure::Deterministic(rejected) = definition_analysis_failure(&rejected)
    else {
        panic!("a confirmed effect rejection invalidates the definition");
    };
    assert_eq!(rejected.0.analyze_error(), Some(&effect));

    let generic = RefreshError::from(SqlCompileError::Analyze(unsupported.clone()));
    let DefinitionAnalysisFailure::Transient(generic) = definition_analysis_failure(&generic)
    else {
        panic!("a generic unsupported expression must preserve its original disposition");
    };
    assert_eq!(generic.0.analyze_error(), Some(&unsupported));

    let cancelled = RefreshError::from(SqlCompileError::Cancelled);
    let DefinitionAnalysisFailure::Transient(cancelled) = definition_analysis_failure(&cancelled)
    else {
        panic!("cancellation must preserve its original disposition");
    };
    assert_eq!(
        cancelled.0.compile_control_error(),
        Some(novarocks_type_contract::CompileControlError::Cancelled)
    );

    let source = RefreshError::from(ConnectorError::new(
        ConnectorErrorKind::Unavailable,
        "refresh source unavailable",
    ));
    let DefinitionAnalysisFailure::Transient(source) = definition_analysis_failure(&source) else {
        panic!("source access failures must preserve their original disposition");
    };
    let connector_error = source
        .0
        .original_failure()
        .and_then(|cause| cause.downcast_ref::<ConnectorError>())
        .expect("original connector error");
    assert_eq!(connector_error.kind(), ConnectorErrorKind::Unavailable);
    assert_eq!(connector_error.message(), "refresh source unavailable");
}
