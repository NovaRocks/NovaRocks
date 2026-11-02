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

//! One role-authorized result handoff, before the protocol can emit metadata.

use novarocks_failpoint::scope::{ActionClaim, ControlCancellation, GrantHandle, ProcessBinding};
use novarocks_query_application::{api::ResultFailureView, session_control::StatementToken};
use serde::Deserialize;
use std::sync::{Arc, Mutex};

pub(crate) struct ResultHandoffControl {
    grant: GrantHandle,
    cancellation: ControlCancellation,
    run_token: String,
    state: Mutex<HandoffState>,
}
#[derive(Default)]
struct HandoffState {
    actual: Option<StatementToken>,
    permission: Option<ActionClaim>,
    hit: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Parameters {
    action: String,
    statement_run_token: String,
}
impl ResultHandoffControl {
    pub(crate) const DOMAIN: &'static str = "result-handoff";
    pub(crate) fn from_grants(grants: Vec<GrantHandle>) -> Result<Option<Arc<Self>>, String> {
        if grants.is_empty() {
            return Ok(None);
        }
        if grants.len() != 1 || grants[0].domain() != Self::DOMAIN {
            return Err("result handoff requires exactly one own-domain grant".into());
        }
        let grant = grants.into_iter().next().expect("one grant");
        let parameters: Parameters = serde_json::from_value(grant.parameters().clone())
            .map_err(|_| "result handoff grant parameters are invalid".to_owned())?;
        if parameters.action != "await-terminal-before-protocol"
            || parameters.statement_run_token.is_empty()
            || parameters.statement_run_token.len() > 256
        {
            return Err("result handoff grant action or target is invalid".into());
        }
        let cancellation = grant.cancellation();
        if !matches!(cancellation.process(), ProcessBinding::Frontend(_)) {
            return Err("result handoff requires the original Frontend process".into());
        }
        if !cancellation.is_active() {
            return Err("result handoff grant is no longer active".into());
        }
        Ok(Some(Arc::new(Self {
            grant,
            cancellation,
            run_token: parameters.statement_run_token,
            state: Mutex::new(HandoffState::default()),
        })))
    }

    /// Nomination supplies identity only. The independent original grant is
    /// the authority; unrelated statements keep the ordinary eager handoff.
    pub(crate) async fn before_protocol(
        &self,
        actual: StatementToken,
        nominated: Option<(String, StatementToken, u32)>,
        failure: Option<ResultFailureView>,
    ) -> Result<(), String> {
        let Some((run_token, nominated, connection_id)) = nominated else {
            return Ok(());
        };
        if run_token != self.run_token {
            return Ok(());
        }
        if nominated != actual || actual.generation() == 0 {
            return Err("result handoff nomination names a different statement".into());
        }
        if !self.cancellation.is_active() {
            return Ok(());
        }
        let mut failure = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "result handoff state lock failed".to_owned())?;
            if let Some(bound) = state.actual {
                return if bound == actual {
                    Ok(())
                } else {
                    Err("result handoff grant already belongs to another statement".into())
                };
            }
            let failure = failure
                .ok_or_else(|| "selected result handoff has no original failure view".to_owned())?;
            let permission = match self.grant.claim_once(self.cancellation.process()) {
                Ok(permission) => permission,
                Err(_) if !self.cancellation.is_active() => return Ok(()),
                Err(_) => return Err("result handoff original grant claim was refused".into()),
            };
            state.actual = Some(actual);
            state.permission = Some(permission);
            failure
        };
        // No mutex guard or whole result is held across suspension. This is
        // the original role deadline, not another timeout or business cause.
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => return Ok(()),
            _ = failure.wait() => {},
        }
        // wait() also reports a closed sender. That synthetic observation is
        // not an accepted failure and must never produce this action's hit.
        if failure.current().is_none() || !self.cancellation.is_active() {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| "result handoff state lock failed".to_owned())?;
        if state.hit || !self.cancellation.is_active() {
            return Ok(());
        }
        state
            .permission
            .as_mut()
            .expect("the selected handoff retains its original claim")
            .record_hit()
            .map_err(|_| "result handoff terminal observation could not be recorded".to_owned())?;
        state.hit = true;
        drop(state);
        eprintln!(
            "NOVAROCKS_RESULT_HANDOFF_TERMINAL_OBSERVED token={} statement={}:{}:{}",
            self.run_token.escape_default(),
            connection_id,
            actual.session().key().get(),
            actual.generation()
        );
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use novarocks_execution_contract::failure::FailureProjection;
    use novarocks_failpoint::{
        liveness::{PreparedRoleSession, RoleSessionOwner, RunnerSession},
        session::{DECLARATION_VERSION, DomainGrant, SessionDeclaration, SessionRole},
    };
    use novarocks_query_application::{
        api::{ExecutionHandle, ExecutionOutput, QueryExecutionError, QueryResultStream},
        execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
        session_control::SessionToken,
        test_support::ResultStreamTestProducer,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };
    use novarocks_workload_control::ResourceConfig;
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };

    struct Role {
        role: RoleSessionOwner,
        runner: RunnerSession,
        _directory: tempfile::TempDir,
        closed: bool,
    }
    impl Role {
        fn new(domain: &str, parameters: serde_json::Value, backend: bool) -> Self {
            use std::os::unix::fs::PermissionsExt;
            let directory = tempfile::Builder::new()
                .prefix("sep1-rh-")
                .tempdir_in("/tmp")
                .unwrap();
            std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
                .unwrap();
            let role = if backend {
                SessionRole::Be
            } else {
                SessionRole::Fe
            };
            let declaration = SessionDeclaration {
                schema_version: DECLARATION_VERSION,
                session_id: "handoff-session".into(),
                launch_id: "handoff-launch".into(),
                role,
                lifetime_millis: 20_000,
                liveness_socket: directory.path().join("live.sock"),
                handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
                domain_grants: vec![DomainGrant {
                    domain: domain.into(),
                    grant_id: "handoff".into(),
                    parameters,
                }],
            };
            let path = directory.path().join("session.json");
            declaration.write_private(&path).unwrap();
            let prepared = PreparedRoleSession::read_and_claim(&path, role).unwrap();
            let runner = RunnerSession::start(declaration).unwrap();
            let process = if backend {
                ProcessBinding::Backend(BackendProcessId::new_v7())
            } else {
                ProcessBinding::Frontend(FrontendProcessId::new_v7())
            };
            Self {
                role: prepared.bind(process).unwrap(),
                runner,
                _directory: directory,
                closed: false,
            }
        }
        fn valid() -> Self {
            Self::new(ResultHandoffControl::DOMAIN, parameters("selected"), false)
        }
        fn control(&self) -> Arc<ResultHandoffControl> {
            ResultHandoffControl::from_grants(
                self.role.scope().grants_for(ResultHandoffControl::DOMAIN),
            )
            .unwrap()
            .unwrap()
        }
        fn events(&self, event: &str) -> usize {
            self.role
                .scope()
                .observations()
                .iter()
                .filter(|o| o.domain == ResultHandoffControl::DOMAIN && o.event == event)
                .count()
        }
        fn close(&mut self) {
            if !self.closed {
                self.role.close_and_join().unwrap();
                self.runner.end_and_join().unwrap();
                self.closed = true;
            }
        }
    }
    impl Drop for Role {
        fn drop(&mut self) {
            self.close();
        }
    }
    fn parameters(token: &str) -> serde_json::Value {
        serde_json::json!({"action":"await-terminal-before-protocol","statement_run_token":token})
    }
    fn statement() -> StatementToken {
        StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (17_u64 << 32) | 3_u64,
                )
                .expect("nonzero fixture session"),
            ),
            9,
        )
    }
    fn nomination(actual: StatementToken) -> Option<(String, StatementToken, u32)> {
        Some(("selected".into(), actual, 7))
    }
    fn pending<F: Future>(future: std::pin::Pin<&mut F>) {
        assert!(matches!(
            future.poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
    }
    struct Stream {
        producer: ResultStreamTestProducer,
        execution: ExecutionHandle,
        stream: QueryResultStream,
    }
    impl Stream {
        fn open() -> Self {
            let id =
                QueryExecutionId::new(QueryId::new(71, 1), AttemptId::new(1).unwrap()).unwrap();
            let (producer, mut execution, _resources, _schema_receipt) =
                ResultStreamTestProducer::open(
                    id,
                    Vec::new(),
                    1,
                    ResourceConfig {
                        total_bytes: 1024 * 1024,
                        control_bytes: 1024,
                        per_scope_bytes: 1024 * 1024 - 1024,
                    },
                )
                .unwrap();
            let Some(ExecutionOutput::Rows(stream)) = execution.take_output() else {
                panic!("real producer must hand off a row stream")
            };
            Self {
                producer,
                execution,
                stream,
            }
        }
        fn view(&self) -> Option<ResultFailureView> {
            self.stream.failure_view()
        }
        fn finish(self) {
            drop(self.stream);
            drop(self.execution);
            self.producer.finish();
        }
    }
    fn original() -> QueryExecutionError {
        QueryExecutionError::from_execution(ExecutionFailureInput::new(
            QueryFailureReason::ResultProtocol.failure_fact(),
            novarocks_execution_contract::SafeDetail::truncating(
                "original query-owner result failure",
            ),
            ExecutionFailureSite::Coordinator,
        ))
    }

    #[test]
    fn only_one_live_frontend_own_domain_grant_with_closed_parameters_can_bind() {
        assert!(
            ResultHandoffControl::from_grants(Vec::new())
                .unwrap()
                .is_none()
        );
        let valid = Role::valid();
        let grants = valid.role.scope().grants_for(ResultHandoffControl::DOMAIN);
        assert!(
            ResultHandoffControl::from_grants(vec![grants[0].clone(), grants[0].clone()]).is_err()
        );
        let foreign = Role::new("foreign", parameters("selected"), false);
        assert!(
            ResultHandoffControl::from_grants(foreign.role.scope().grants_for("foreign")).is_err()
        );
        let backend = Role::new(ResultHandoffControl::DOMAIN, parameters("selected"), true);
        assert!(
            ResultHandoffControl::from_grants(
                backend
                    .role
                    .scope()
                    .grants_for(ResultHandoffControl::DOMAIN)
            )
            .is_err()
        );
        for value in [
            parameters(""),
            parameters(&"x".repeat(257)),
            serde_json::json!({"action":"other","statement_run_token":"selected"}),
            serde_json::json!({"action":"await-terminal-before-protocol","statement_run_token":"selected","extra":true}),
        ] {
            let bad = Role::new(ResultHandoffControl::DOMAIN, value, false);
            assert!(
                ResultHandoffControl::from_grants(
                    bad.role.scope().grants_for(ResultHandoffControl::DOMAIN)
                )
                .is_err()
            );
        }
        let mut ended = Role::valid();
        ended.close();
        assert!(
            ResultHandoffControl::from_grants(
                ended.role.scope().grants_for(ResultHandoffControl::DOMAIN)
            )
            .is_err()
        );
        assert_eq!(valid.events("claimed"), 0);
    }

    #[tokio::test]
    async fn unrelated_statements_bypass_and_same_run_foreign_statement_is_rejected_before_claim() {
        let role = Role::valid();
        let control = role.control();
        let stream = Stream::open();
        let actual = statement();
        control.before_protocol(actual, None, None).await.unwrap();
        control
            .before_protocol(actual, Some(("another-run".into(), actual, 7)), None)
            .await
            .unwrap();
        let foreign = StatementToken::new(actual.session(), actual.generation() + 1);
        assert!(
            control
                .before_protocol(actual, nomination(foreign), stream.view())
                .await
                .is_err()
        );
        assert_eq!(role.events("claimed"), 0);
        assert_eq!(role.events("hit"), 0);
        stream.finish();
    }

    #[tokio::test]
    async fn original_failure_view_releases_once_and_preserves_the_exact_failure_for_protocol() {
        let role = Role::valid();
        let control = role.control();
        let stream = Stream::open();
        let actual = statement();
        {
            let mut first =
                std::pin::pin!(control.before_protocol(actual, nomination(actual), stream.view()));
            pending(first.as_mut());
            assert_eq!(role.events("claimed"), 1);
            assert_eq!(role.events("hit"), 0);
            // Duplicate calls cannot consume another permission or await again.
            control
                .before_protocol(actual, nomination(actual), stream.view())
                .await
                .unwrap();
            let expected = original();
            stream.producer.fail(expected.clone());
            first.await.unwrap();
            assert_eq!(stream.view().unwrap().current(), Some(expected));
        }
        control
            .before_protocol(actual, nomination(actual), stream.view())
            .await
            .unwrap();
        assert_eq!(role.events("claimed"), 1);
        assert_eq!(role.events("hit"), 1);
        stream.finish();
    }

    #[tokio::test]
    async fn original_role_exit_releases_a_pending_handoff_without_failure_or_hit() {
        let mut role = Role::valid();
        let control = role.control();
        let stream = Stream::open();
        let actual = statement();
        {
            let mut wait =
                std::pin::pin!(control.before_protocol(actual, nomination(actual), stream.view()));
            pending(wait.as_mut());
            role.close();
            wait.await.unwrap();
        }
        assert_eq!(stream.view().unwrap().current(), None);
        assert_eq!(role.events("claimed"), 1);
        assert_eq!(role.events("hit"), 0);
        stream.finish();
    }

    #[tokio::test]
    async fn failure_sender_exit_is_not_a_terminal_failure_hit() {
        let role = Role::valid();
        let control = role.control();
        let stream = Stream::open();
        let actual = statement();
        let Stream {
            producer,
            execution,
            stream,
        } = stream;
        {
            let mut wait = std::pin::pin!(control.before_protocol(
                actual,
                nomination(actual),
                stream.failure_view()
            ));
            pending(wait.as_mut());
            producer.finish();
            wait.await.unwrap();
        }
        assert_eq!(stream.failure_view().unwrap().current(), None);
        assert_eq!(role.events("claimed"), 1);
        assert_eq!(role.events("hit"), 0);
        drop(stream);
        drop(execution);
    }
}
