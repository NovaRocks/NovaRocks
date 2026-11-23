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

use super::*;
use crate::session_control::{
    GovernedQueryStatementBeginError, GovernedStatementVisibilitySealOutcome, QueryControlPort,
    QueryControlService,
};
use novarocks_workload_control::{
    CancellationReason, ResourceConfig, WorkClass, WorkloadConfig, WorkloadControl,
};

struct Fixture {
    control: Arc<QueryApplicationControl>,
    service: QueryControlService,
    workload: WorkloadControl,
    created: ClientQueryTime,
}

impl Fixture {
    fn new(history: Option<ClientQueryHistoryConfig>) -> Self {
        let process = novarocks_types::FrontendProcessId::new_v7();
        let control = Arc::new(match history {
            Some(history) => QueryApplicationControl::with_client_query_history(process, history),
            None => QueryApplicationControl::new(process),
        });
        let service = QueryControlService::new(Arc::clone(&control) as Arc<dyn QueryControlPort>);
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        workload.mark_ready().unwrap();
        Self {
            control,
            service,
            workload,
            created: ClientQueryTime::now(),
        }
    }

    fn at(&self, millis: u64) -> ClientQueryTime {
        let delay = Duration::from_millis(millis);
        ClientQueryTime::new(
            self.created.monotonic + delay,
            self.created.wall_clock + delay,
        )
    }

    fn post(&self, text: Option<&ClientQueryText>) -> ClientQueryRegistration {
        self.service
            .register_client_query(
                self.service.mint_client_query_id(),
                "root".into(),
                Some("JDBC".into()),
                text,
                self.created,
            )
            .unwrap()
    }

    fn claim(&self, registration: &ClientQueryRegistration) -> ClientQueryClaim {
        match self.service.claim_client_query(registration, self.at(1)) {
            ClientQueryStart::Claimed(claim) => claim,
            _ => panic!("client query must be claimed once"),
        }
    }

    fn session(
        &self,
        registration: &ClientQueryRegistration,
    ) -> crate::session_control::QuerySessionLease {
        let claim = self.claim(registration);
        self.service
            .register_claimed_session(
                SessionIdentity::client_statement(registration.id().clone(), "root"),
                claim,
            )
            .unwrap()
    }

    fn requester(&self, user: &str) -> SessionToken {
        let requester = self
            .control
            .register_session(SessionIdentity::client_statement(
                self.control.mint_client_query_id(),
                user,
            ))
            .unwrap();
        self.control.begin_statement(requester).unwrap();
        requester
    }

    fn begin(
        &self,
        session: SessionToken,
    ) -> Result<crate::session_control::GovernedQueryStatementOwner, GovernedQueryStatementBeginError>
    {
        self.service.begin_governed_statement(
            session,
            &self.workload.root_admission(),
            WorkClass::Management,
            None,
            None,
            None,
        )
    }

    fn observe(&self, registration: &ClientQueryRegistration, at: u64) -> ClientQueryObservation {
        self.service
            .observe_client_query(
                "root",
                registration.id().as_str(),
                ClientQueryColumns::default(),
                self.at(at),
            )
            .unwrap()
    }
}

fn user_canceled() -> ClientQueryConclusion {
    ClientQueryConclusion::Failed(USER_CANCELED)
}

#[test]
fn client_query_lifecycle_repeated_get_accepts_only_the_original_claim() {
    let fixture = Fixture::new(None);
    let registration = fixture.post(None);
    let id = registration.id();
    assert!(
        fixture
            .control
            .current_session_for_client_query(id.as_str())
            .is_none()
    );
    let claim = fixture.claim(&registration);
    assert!(matches!(
        fixture
            .service
            .claim_client_query(&registration, fixture.at(2)),
        ClientQueryStart::Unavailable
    ));
    assert!(!fixture.service.withdraw_client_query(&registration));
    let identity = SessionIdentity::client_statement(id.clone(), "root");
    assert!(matches!(
        fixture.service.register_session(identity.clone()),
        Err(QueryControlError::ClientQueryIdInUse)
    ));
    let lease = fixture
        .service
        .register_claimed_session(identity, claim)
        .unwrap();
    assert_eq!(
        fixture
            .control
            .current_session_for_client_query(id.as_str()),
        Some(lease.token())
    );
    drop(lease);
    assert!(
        fixture
            .control
            .current_session_for_client_query(id.as_str())
            .is_none()
    );
    assert_eq!(
        fixture.service.settle_client_query(
            &registration,
            ClientQueryConclusion::Finished,
            fixture.at(3)
        ),
        Some(ClientQueryConclusion::Finished)
    );
    assert_eq!(
        fixture
            .service
            .settle_client_query(&registration, user_canceled(), fixture.at(4)),
        Some(ClientQueryConclusion::Finished)
    );
    let foreign = Fixture::new(None);
    assert_eq!(
        foreign
            .service
            .settle_client_query(&registration, user_canceled(), foreign.at(4)),
        None
    );
}

#[tokio::test]
async fn client_query_lifecycle_queued_kill_one_winner_and_original_receipt() {
    let fixture = Fixture::new(None);
    let registration = fixture.post(None);
    let other = fixture.requester("guest");
    let requester = fixture.requester("root");
    let kill = |requester, target, message| {
        fixture
            .service
            .kill_client_query(requester, target, message, fixture.at(2))
    };
    assert_eq!(
        kill(other, registration.id().as_str(), None),
        Err(KillClientQueryError::PermissionDenied)
    );
    assert_eq!(
        kill(requester, "not_a_real_query", None),
        Err(KillClientQueryError::NotFound)
    );
    assert_eq!(
        kill(requester, "NoT_a_real_query", None),
        Err(KillClientQueryError::InvalidId)
    );
    let receipt = kill(requester, registration.id().as_str(), Some("cancel test")).unwrap();
    assert_eq!(receipt.cut(), ManagementCancellationCut::Registered);
    assert_eq!(receipt.query_id(), registration.id());
    assert_eq!(receipt.user(), "root");
    assert_eq!(receipt.requester(), requester);
    assert_eq!(receipt.message(), Some("cancel test"));
    registration.cancellation_requested().await;
    assert!(matches!(
        fixture
            .service
            .claim_client_query(&registration, fixture.at(3)),
        ClientQueryStart::Cancelled
    ));
    assert!(!fixture.service.request_unstarted_client_cancel(
        &registration,
        QueryCancellationReason::ClientDisconnected
    ));
    assert_eq!(
        kill(
            requester,
            registration.id().as_str(),
            Some("different cause")
        ),
        Err(KillClientQueryError::NotRunning)
    );
    assert_eq!(
        fixture
            .service
            .settle_client_query(&registration, user_canceled(), fixture.at(6)),
        Some(ClientQueryConclusion::AdministrativelyKilled(receipt))
    );
}

#[test]
fn client_query_lifecycle_kill_across_claimed_session_prevents_work_install() {
    let fixture = Fixture::new(None);
    let registration = fixture.post(None);
    let lease = fixture.session(&registration);
    let requester = fixture.requester("root");
    let receipt = fixture
        .service
        .kill_client_query(requester, registration.id().as_str(), None, fixture.at(2))
        .unwrap();
    assert_eq!(receipt.cut(), ManagementCancellationCut::Claimed);
    assert!(matches!(
        fixture.begin(lease.token()),
        Err(GovernedQueryStatementBeginError::QueryControl(
            QueryControlError::ClientQueryCancelled
        ))
    ));
    assert_eq!(fixture.workload.snapshot().root_responsibilities, 0);
    assert_eq!(fixture.workload.snapshot().businesses, 0);
}

#[tokio::test]
async fn client_query_lifecycle_work_stop_and_management_receipt_stay_together() {
    let fixture = Fixture::new(None);
    let registration = fixture.post(None);
    let lease = fixture.session(&registration);
    let statement = fixture.begin(lease.token()).unwrap();
    let token = statement.token();
    let requester = fixture.requester("root");
    let original_reason = CancellationReason::ExplicitKill {
        requester_session: requester.key().get(),
    };
    let receipt = std::thread::scope(|scope| {
        let kill = scope.spawn(|| {
            fixture
                .service
                .kill_client_query(
                    requester,
                    registration.id().as_str(),
                    Some("work stopped"),
                    fixture.at(2),
                )
                .unwrap()
        });
        let observed = statement.cancellation();
        // This observer starts from Work's original stop signal. It must see
        // the complete accepted receipt even if Work wakes before publication.
        futures::executor::block_on(observed.cancelled());
        assert_eq!(observed.reason(), Some(original_reason));
        let Some(ClientQueryCancellation::Management(receipt)) =
            fixture.service.client_query_cancellation(&registration)
        else {
            panic!("Work stop must resolve the original managed receipt");
        };
        assert!(Arc::ptr_eq(&receipt, &kill.join().unwrap()));
        receipt
    });
    assert_eq!(receipt.cut(), ManagementCancellationCut::Statement(token));
    assert_eq!(receipt.message(), Some("work stopped"));
    drop(statement);
    assert_eq!(fixture.workload.snapshot().root_responsibilities, 0);
    assert_eq!(
        fixture
            .service
            .settle_client_query(&registration, user_canceled(), fixture.at(3)),
        Some(ClientQueryConclusion::AdministrativelyKilled(receipt))
    );
}

#[test]
fn client_query_lifecycle_sealed_work_can_still_fail_client_delivery() {
    let fixture = Fixture::new(Some(ClientQueryHistoryConfig {
        max_bytes: 16_384,
        max_age: Duration::from_secs(1),
    }));
    let registration = fixture.post(None);
    let lease = fixture.session(&registration);
    let requester = fixture.requester("root");
    let statement = fixture.begin(lease.token()).unwrap();
    assert_eq!(
        fixture
            .control
            .seal_governed_statement_visibility(statement.token()),
        GovernedStatementVisibilitySealOutcome::Sealed
    );
    assert_eq!(
        fixture.observe(&registration, 2).state,
        ClientQueryState::Finishing
    );
    assert_eq!(
        fixture.service.kill_client_query(
            requester,
            registration.id().as_str(),
            None,
            fixture.at(2)
        ),
        Err(KillClientQueryError::NotRunning)
    );
    assert!(statement.cancellation().reason().is_none());
    fixture
        .service
        .settle_client_query(&registration, user_canceled(), fixture.at(3))
        .unwrap();
    let observation = fixture.observe(&registration, 4);
    assert_eq!(observation.state, ClientQueryState::Failed);
    assert_eq!(observation.error, user_canceled().failure());
    assert!(statement.cancellation().reason().is_none());
    drop(statement);
    assert_eq!(fixture.workload.snapshot().root_responsibilities, 0);
}

#[test]
fn recent_query_history_refunds_original_text_after_last_alias_and_expires_independently() {
    let fixture = Fixture::new(Some(ClientQueryHistoryConfig {
        max_bytes: 16_384,
        max_age: Duration::from_millis(9),
    }));
    let original = ClientQueryText::new("SELECT 1".into());
    let registration = fixture.post(Some(&original));
    let mut wake = fixture.service.subscribe_client_query_history().unwrap();
    let text_column = ClientQueryColumns {
        query: true,
        ..Default::default()
    };
    let get_text = |at| {
        fixture
            .service
            .observe_client_query(
                "root",
                registration.id().as_str(),
                text_column,
                fixture.at(at),
            )
            .unwrap()
            .query
            .unwrap()
    };
    assert_eq!(Arc::strong_count(&original.0), 1);
    assert!(fixture.observe(&registration, 1).query.is_none());
    assert_eq!(Arc::strong_count(&original.0), 1);
    let alias = get_text(2);
    assert!(Arc::ptr_eq(&alias.0, &original.0));
    fixture
        .service
        .touch_client_query(&registration, fixture.at(3));
    fixture
        .service
        .settle_client_query(
            &registration,
            ClientQueryConclusion::Finished,
            fixture.at(4),
        )
        .unwrap();
    assert!(wake.has_changed().unwrap());
    assert_eq!(*wake.borrow_and_update(), Some(fixture.at(13).monotonic));
    drop(original);
    let late_alias = get_text(12);
    assert!(Arc::ptr_eq(&alias.0, &late_alias.0));
    assert_eq!(
        fixture.observe(&registration, 12).last_heartbeat,
        Some(fixture.at(3).wall_clock)
    );
    assert_eq!(
        fixture
            .service
            .evict_client_query_history(fixture.at(13).monotonic)
            .rows,
        0
    );
    assert_eq!(*wake.borrow_and_update(), None);
    let text_bytes = alias.retained_bytes().unwrap();
    assert_eq!(
        fixture
            .service
            .evict_client_query_history(fixture.at(14).monotonic)
            .retained_bytes,
        text_bytes
    );
    drop(late_alias);
    assert_eq!(
        fixture
            .service
            .evict_client_query_history(fixture.at(15).monotonic)
            .retained_bytes,
        text_bytes
    );
    drop(alias);
    assert_eq!(
        fixture
            .service
            .evict_client_query_history(fixture.at(16).monotonic)
            .retained_bytes,
        0
    );
    assert_eq!(
        fixture
            .service
            .settle_client_query(&registration, user_canceled(), fixture.at(17)),
        Some(ClientQueryConclusion::Finished)
    );
}

#[test]
fn recent_query_history_pressure_uses_finish_order_and_drops_oversized_diagnostics() {
    let id = QueryApplicationControl::default().mint_client_query_id();
    let backing = |value| ClientQueryText::new(value).retained_bytes().unwrap();
    let principal: Arc<str> = "root".into();
    // Registration stores the principal using its Arc<str> Display path,
    // whose String backing can have spare capacity beyond its UTF-8 length.
    let max_bytes = size_of::<history::HistoricalQuery>()
        + backing(id.as_str().to_owned())
        + backing(principal.to_string());
    let fixture = Fixture::new(Some(ClientQueryHistoryConfig {
        max_bytes,
        max_age: Duration::from_secs(1),
    }));
    let sql = ClientQueryText::new("SELECT ".repeat(1000));
    let first = fixture.post(Some(&sql));
    fixture
        .service
        .settle_client_query(&first, ClientQueryConclusion::Finished, fixture.at(2))
        .unwrap();
    let row = fixture.observe(&first, 3);
    assert_eq!(row.state, ClientQueryState::Finished);
    assert!(
        fixture
            .service
            .observe_client_query(
                "root",
                first.id().as_str(),
                ClientQueryColumns {
                    query: true,
                    ..Default::default()
                },
                fixture.at(3),
            )
            .unwrap()
            .query
            .is_none()
    );
    drop(row);
    let second = fixture.post(None);
    fixture
        .service
        .settle_client_query(&second, ClientQueryConclusion::Finished, fixture.at(4))
        .unwrap();
    let retained = fixture
        .service
        .evict_client_query_history(fixture.at(5).monotonic);
    assert_eq!(retained.rows, 1);
    assert_eq!(retained.oldest_finished, Some(fixture.at(4).monotonic));
    assert!(retained.retained_bytes <= max_bytes);
    let requester = fixture.requester("root");
    assert_eq!(
        fixture
            .service
            .kill_client_query(requester, second.id().as_str(), None, fixture.at(5)),
        Err(KillClientQueryError::NotRunning)
    );
    assert_eq!(
        fixture
            .service
            .kill_client_query(requester, first.id().as_str(), None, fixture.at(5)),
        Err(KillClientQueryError::NotFound)
    );
}

#[test]
fn client_query_lifecycle_missing_phases_stay_null_and_generation_must_match() {
    let fixture = Fixture::new(None);
    let registration = fixture.post(None);
    let lease = fixture.session(&registration);
    let statement = fixture.control.begin_statement(lease.token()).unwrap();
    let token = statement.token();
    let queued = fixture.observe(&registration, 5);
    assert!(queued.analysis.is_none());
    assert!(queued.planning.is_none());
    assert_eq!(queued.claimed, Some(fixture.at(1).wall_clock));
    assert_eq!(queued.queued, Duration::from_millis(5));
    assert!(fixture.service.record_client_query_event(
        token,
        ClientQueryEvent::Admitted,
        fixture.at(2),
    ));
    assert!(!fixture.service.record_client_query_event(
        StatementToken::new(token.session(), token.generation() + 1),
        ClientQueryEvent::PlanningStarted,
        fixture.at(1)
    ));
    assert!(fixture.service.record_client_query_event(
        token,
        ClientQueryEvent::AnalysisStarted,
        fixture.at(2)
    ));
    assert!(fixture.service.record_client_query_event(
        token,
        ClientQueryEvent::AnalysisFinished,
        fixture.at(4)
    ));
    assert!(fixture.service.record_client_query_event(
        token,
        ClientQueryEvent::PlanningStarted,
        fixture.at(5)
    ));
    let observed = fixture.observe(&registration, 8);
    assert_eq!(observed.queued, Duration::from_millis(2));
    assert_eq!(observed.analysis, Some(Duration::from_millis(2)));
    assert_eq!(observed.planning, Some(Duration::from_millis(3)));
    assert_eq!(observed.started, Some(fixture.at(5).wall_clock));
}

#[test]
fn client_query_lifecycle_scan_keeps_the_original_creation_cut_after_history_transfer() {
    let fixture = Fixture::new(Some(ClientQueryHistoryConfig {
        max_bytes: 16_384,
        max_age: Duration::from_secs(1),
    }));
    let first = fixture.post(None);
    let later = fixture
        .service
        .register_client_query(
            fixture.service.mint_client_query_id(),
            "root".into(),
            None,
            None,
            fixture.at(10),
        )
        .unwrap();
    let columns = ClientQueryColumns::default();
    let observe = || {
        fixture
            .service
            .next_client_query("root", Some(first.id().as_str()), columns, fixture.at(5))
    };
    assert!(observe().is_none());
    fixture
        .service
        .settle_client_query(&later, ClientQueryConclusion::Finished, fixture.at(11))
        .unwrap();
    assert!(observe().is_none());
    assert!(
        fixture
            .service
            .observe_client_query("root", later.id().as_str(), columns, fixture.at(5))
            .is_none()
    );
    assert!(
        fixture
            .service
            .observe_client_query("root", later.id().as_str(), columns, fixture.at(12))
            .is_some()
    );
}

#[test]
fn client_query_lifecycle_failed_post_withdraws_only_the_unclaimed_registration() {
    let fixture = Fixture::new(None);
    let text = ClientQueryText::new("SELECT 1".into());
    let registration = fixture.post(Some(&text));
    assert_eq!(Arc::strong_count(&text.0), 1);
    assert!(fixture.service.withdraw_client_query(&registration));
    assert!(matches!(
        fixture
            .service
            .claim_client_query(&registration, fixture.at(1)),
        ClientQueryStart::Unavailable
    ));
    assert!(
        fixture
            .service
            .observe_client_query(
                "root",
                registration.id().as_str(),
                ClientQueryColumns::default(),
                fixture.at(1),
            )
            .is_none()
    );
    assert!(
        fixture
            .service
            .settle_client_query(&registration, user_canceled(), fixture.at(2))
            .is_none()
    );
    assert_eq!(Arc::strong_count(&text.0), 1);
}

#[test]
fn client_query_lifecycle_retired_registration_cannot_install_work_in_its_old_session() {
    let fixture = Fixture::new(None);
    let registration = fixture.post(None);
    let lease = fixture.session(&registration);
    fixture.service.request_unstarted_client_cancel(
        &registration,
        QueryCancellationReason::ClientCancelRequested,
    );
    assert_eq!(
        fixture
            .service
            .settle_client_query(&registration, user_canceled(), fixture.at(2)),
        Some(user_canceled()),
    );
    assert!(matches!(
        fixture.begin(lease.token()),
        Err(GovernedQueryStatementBeginError::QueryControl(
            QueryControlError::StaleSession
        ))
    ));
    assert_eq!(fixture.workload.snapshot().root_responsibilities, 0);
}
