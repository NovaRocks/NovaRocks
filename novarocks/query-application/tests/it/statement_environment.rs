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
use novarocks_query_application::{
    admitted_query_context::{RequestAdmission, RequestContext, StatementAdmissionContext},
    api::BackendTopologySnapshot,
    cancellation::{QueryCancellationReason, QueryCancellationSource},
};
use novarocks_sql::{compiler::SessionOptimizerSettings, sql_mode::SqlSemanticSettings};
use novarocks_type_contract::{StatementStartUtc, StatementTimeEnvironment, TimeZoneKey};
use novarocks_types::ClusterRole;
use std::time::{Duration, Instant};

#[test]
fn statement_environment_survives_initial_admission_and_later_topology_rounds() {
    let environment = StatementTimeEnvironment {
        start: StatementStartUtc::try_new(-1, 987_654_321).unwrap(),
        zone: TimeZoneKey::parse("Europe/Warsaw").unwrap(),
    };
    let cancellation = QueryCancellationSource::new();
    let deadline = Some(Instant::now() + Duration::from_secs(30));
    let settings = SqlSemanticSettings::default().with_statement_time(environment);
    let initial = RequestContext::admit(RequestAdmission::new(
        None,
        "db".into(),
        ClusterRole::Fe,
        BackendTopologySnapshot::empty(7),
        deadline,
        cancellation.view(),
        SessionOptimizerSettings::default(),
        settings,
        None,
    ));
    let statement = StatementAdmissionContext::new(
        None,
        initial.session().current_database().to_owned(),
        initial.execution().role(),
        initial.execution().deadline(),
        initial.execution().cancellation().clone(),
        initial.execution().optimizer_settings().clone(),
        initial.session().sql_semantics().clone(),
        None,
    );
    let round = statement.for_topology(BackendTopologySnapshot::empty(8));
    let derived_round = statement
        .clone()
        .for_topology(BackendTopologySnapshot::empty(9));
    cancellation.request(QueryCancellationReason::ClientDisconnected);
    for (context, revision) in [(initial, 7), (round, 8), (derived_round, 9)] {
        assert_eq!(context.execution().topology().revision(), revision);
        assert_eq!(context.execution().deadline(), deadline);
        assert!(context.execution().cancellation().is_cancelled());
        assert_eq!(
            context.session().sql_semantics().statement_time(),
            Some(environment)
        );
        assert_eq!(
            context.execution().sql_semantics().statement_time(),
            Some(environment)
        );
    }
}
