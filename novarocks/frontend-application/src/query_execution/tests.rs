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

use novarocks_query_application::cancellation::{QueryCancellationReason, QueryCancellationSource};
use novarocks_query_application::publication::LakePublicationRuntimePolicy;

#[test]
fn cancellation_view_observes_injected_flag() {
    let cancelled = QueryCancellationSource::new();
    let view = cancelled.view();

    assert!(!view.is_cancelled());
    let _ = cancelled.request(QueryCancellationReason::ClientDisconnected);
    assert!(view.is_cancelled());
}

#[test]
fn statistics_attempt_timeout_must_be_positive() {
    let policy_for = |attempt_timeout| {
        LakePublicationRuntimePolicy::try_new(
            attempt_timeout,
            std::time::Duration::from_secs(45 * 60),
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(5 * 60),
            std::time::Duration::from_secs(60),
        )
    };
    let policy =
        policy_for(std::time::Duration::from_secs(30 * 60)).expect("positive attempt timeout");
    assert_eq!(
        policy.max_attempt_duration(),
        std::time::Duration::from_secs(30 * 60)
    );
    assert!(
        policy_for(std::time::Duration::from_secs(30 * 60 + 1)).is_ok(),
        "the shared policy accepts longer attempts when its strict GC relationship is satisfied"
    );
    assert!(policy_for(std::time::Duration::ZERO).is_err());
}

#[test]
fn fragment_profile_set_preserves_fragment_profiles() {
    let profile = crate::query_execution::profile::FragmentProfileTree::for_fragment(
        7,
        novarocks_execution::runtime::profile::Profiler::new("fragment-7").to_native_tree(),
    );
    let profiles = crate::query_execution::outcome::FragmentProfileSet::new(vec![profile.clone()]);
    assert_eq!(profiles.into_profiles(), vec![profile]);
}

pub(super) fn dependency_fixture_constant_policy() -> novarocks_functions::ConstantPolicy {
    novarocks_functions::ConstantPolicy {
        max_rows: 64,
        max_array_nodes: 1024,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}

/// Complete genuine table-free SQL with explicit test session settings.
pub(super) fn complete_dependency_fixture(
    sql: &str,
    optimizer_settings: novarocks_sql::compiler::SessionOptimizerSettings,
) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId, ScanReadBudget};
    use novarocks_sql::compiler::{
        DEFAULT_COMPLETION_LIMITS, SqlCompileControl, SqlCompileIntent, SqlCompileProgress,
        SqlCompiler, SqlFinalPlanCompileRequest, SqlPlanningEnvironment, SqlSessionContext,
        SqlStatementInput, builtin_sql_function_catalog,
    };

    let mut statements = novarocks_parser::parse(sql).unwrap();
    assert_eq!(statements.len(), 1);
    let novarocks_parser::ast::Statement::Query(query) = statements.pop().unwrap() else {
        panic!("expected one query");
    };
    let control = SqlCompileControl::unbounded();
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([83; 16]).unwrap(),
        SqlStatementInput::parsed_query(Box::new(query)),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings,
        },
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        dependency_fixture_constant_policy(),
        control.clone(),
        PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        ScanReadBudget {
            max_batch_rows: 64,
            max_batch_bytes: 1 << 20,
        },
        DEFAULT_COMPLETION_LIMITS,
    );
    let result = SqlCompiler::start(request.try_into_completion().unwrap(), &control)
        .unwrap_or_else(|error| panic!("complete actual SQL fixture for {sql}: {error:#?}"));
    match result {
        SqlCompileProgress::Complete(completed) => completed.into_plan(),
        SqlCompileProgress::Incomplete(_) => panic!("table-free actual source must complete"),
    }
}
