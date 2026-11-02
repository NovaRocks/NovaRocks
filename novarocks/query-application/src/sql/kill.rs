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

//! Query-application KILL admission and command execution.

use novarocks_parser::ast::{KillKind, KillStatement, LiteralKind, SessionStatement};

use crate::client_connection::{
    ClientConnectionControlPort, ClientConnectionTerminateOutcome,
    ClientConnectionTerminationReason,
};
use crate::protocol_delivery::QuerySessionOutput;
use crate::session_control::{
    ConnectionKillAuthorization, QueryCancelOutcome, QueryControlService, SessionToken,
};
use crate::session_error::{QueryServiceError, QueryServiceErrorKind};

use super::session_admit::SessionAdmitError;

/// Route only parser-admitted control commands before ordinary root/business
/// admission. Connection admission and the existing authorization still apply.
/// All other session statements retain their normal governed entrypoint.
pub fn execute_control_session_statement(
    source: &str,
    statement: &SessionStatement,
    requester: SessionToken,
    query_control: &QueryControlService,
    connection_control: &dyn ClientConnectionControlPort,
) -> Option<Result<QuerySessionOutput, QueryServiceError>> {
    let SessionStatement::Kill(statement) = statement else {
        return None;
    };
    Some(execute_kill_statement(
        source,
        statement,
        requester,
        query_control,
        connection_control,
    ))
}

/// Executes a parser-admitted KILL statement using the exact session and
/// protocol connection capabilities composed for this query service.
pub fn execute_kill_statement(
    source: &str,
    statement: &KillStatement,
    requester: SessionToken,
    query_control: &QueryControlService,
    connection_control: &dyn ClientConnectionControlPort,
) -> Result<QuerySessionOutput, QueryServiceError> {
    let connection_id = kill_connection_id(statement)?;
    match statement.kind {
        KillKind::Query => match query_control.kill_query(requester, connection_id) {
            QueryCancelOutcome::Requested
            | QueryCancelOutcome::AlreadyRequested(_)
            | QueryCancelOutcome::NoActiveStatement => Ok(QuerySessionOutput::Ok),
            QueryCancelOutcome::Failed(error) => Err(QueryServiceError::new(
                QueryServiceErrorKind::Internal,
                format!("request governed query cancellation failed: {error}"),
            )),
            QueryCancelOutcome::UnknownSession => Err(no_such_connection_error(connection_id)),
            QueryCancelOutcome::PermissionDenied => Err(kill_denied_error(source, statement)),
        },
        KillKind::Default | KillKind::Connection => {
            let target = match query_control.authorize_connection_kill(requester, connection_id) {
                ConnectionKillAuthorization::Authorized(target) => target,
                ConnectionKillAuthorization::UnknownSession => {
                    return Err(no_such_connection_error(connection_id));
                }
                ConnectionKillAuthorization::PermissionDenied => {
                    return Err(kill_denied_error(source, statement));
                }
            };
            match connection_control.terminate(
                target,
                ClientConnectionTerminationReason::ExplicitKillConnection {
                    requester_session: requester.key(),
                },
            ) {
                ClientConnectionTerminateOutcome::Requested
                | ClientConnectionTerminateOutcome::AlreadyTerminating => {
                    Ok(QuerySessionOutput::Ok)
                }
                ClientConnectionTerminateOutcome::Stale => {
                    Err(no_such_connection_error(connection_id))
                }
            }
        }
    }
}

fn kill_connection_id(statement: &KillStatement) -> Result<u32, QueryServiceError> {
    let LiteralKind::Number(connection_id) = &statement.connection_id.kind else {
        return Err(QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            "KILL requires an integer connection id",
        ));
    };
    connection_id.parse::<u32>().map_err(|_| {
        QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            "KILL requires an integer connection id",
        )
    })
}

fn no_such_connection_error(connection_id: u32) -> QueryServiceError {
    QueryServiceError::new(
        QueryServiceErrorKind::NoSuchSession,
        format!("unknown connection {connection_id}"),
    )
}

fn kill_denied_error(source: &str, statement: &KillStatement) -> QueryServiceError {
    QueryServiceError::from_user_error(SessionAdmitError::KillDenied.to_user_error(
        source,
        statement.span,
        "permission denied to kill connection owned by another principal",
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use novarocks_parser::{
        ast::{SessionStatement, Statement},
        parse,
    };

    use super::*;
    use crate::client_connection::{ClientConnectionTerminationReason, ClientConnectionToken};
    use crate::query_control::QueryApplicationControl;
    use crate::session_control::{QuerySessionLease, SessionIdentity};

    fn parsed_kill(source: &str) -> KillStatement {
        let statements = parse(source).expect("KILL statement must parse");
        let [Statement::Session(SessionStatement::Kill(statement))] = statements.as_slice() else {
            panic!("expected KILL statement");
        };
        statement.clone()
    }

    fn register_session(
        control: &QueryControlService,
        connection_id: u32,
        generation: u64,
        principal: &str,
    ) -> QuerySessionLease {
        control
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(connection_id, generation).expect("valid token"),
                principal,
            ))
            .expect("register session")
    }

    struct FixedConnectionControl {
        outcome: ClientConnectionTerminateOutcome,
        calls: Mutex<Vec<(ClientConnectionToken, ClientConnectionTerminationReason)>>,
    }

    impl FixedConnectionControl {
        fn new(outcome: ClientConnectionTerminateOutcome) -> Self {
            Self {
                outcome,
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl ClientConnectionControlPort for FixedConnectionControl {
        fn terminate(
            &self,
            target: ClientConnectionToken,
            reason: ClientConnectionTerminationReason,
        ) -> ClientConnectionTerminateOutcome {
            self.calls
                .lock()
                .expect("connection control calls lock")
                .push((target, reason));
            self.outcome
        }
    }

    #[test]
    fn control_route_cancels_existing_work_while_ordinary_capacity_is_full_and_closed() {
        use novarocks_workload_control::{
            ResourceConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkError,
            WorkRequest, WorkloadConfig, WorkloadControl,
        };
        let control = QueryApplicationControl::service();
        let requester = register_session(&control, 8, 1, "alice");
        let target = register_session(&control, 7, 1, "alice");
        let foreign = register_session(&control, 9, 1, "bob");
        let workload = WorkloadControl::try_new(
            WorkloadConfig {
                root_limit: 1,
                business_limit: 1,
                query_concurrency_limit: 1,
                ..WorkloadConfig::default()
            },
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(ResultCapacityConfig {
                positions: [1; 4],
                client_compute_positions: 1,
                client_short_tail_positions: 0,
                supported_cancel_burst: 0,
                sustained_cancels_per_second: 0,
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        workload.mark_ready().unwrap();
        let statement = control
            .begin_governed_statement_with_result(
                target.token(),
                &workload.root_admission(),
                WorkClass::Management,
                None,
                None,
                None,
                ResultWindowClass::Local,
            )
            .unwrap();
        assert!(matches!(
            workload
                .root_admission()
                .try_begin_root(WorkRequest::new(WorkClass::Management)),
            Err(WorkError::Capacity(_))
        ));
        workload.close_admission();
        let connection_control =
            FixedConnectionControl::new(ClientConnectionTerminateOutcome::Requested);
        let kill = SessionStatement::Kill(parsed_kill("KILL QUERY 7"));
        assert!(
            execute_control_session_statement(
                "KILL QUERY 7",
                &kill,
                foreign.token(),
                &control,
                &connection_control
            )
            .unwrap()
            .is_err()
        );
        assert!(statement.cancellation().reason().is_none());
        assert!(matches!(
            execute_control_session_statement(
                "KILL QUERY 7",
                &kill,
                requester.token(),
                &control,
                &connection_control
            )
            .unwrap()
            .unwrap(),
            QuerySessionOutput::Ok
        ));
        assert!(statement.cancellation().reason().is_some());
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        assert_eq!(workload.snapshot().businesses, 1);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        let ordinary = parse("SET @v = 1").unwrap();
        let [Statement::Session(session)] = ordinary.as_slice() else {
            panic!("session statement");
        };
        assert!(
            execute_control_session_statement(
                "SET @v = 1",
                session,
                requester.token(),
                &control,
                &connection_control
            )
            .is_none()
        );
        drop(statement);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[test]
    fn kill_query_treats_an_idle_authorized_target_as_ok() {
        let control = QueryApplicationControl::service();
        let requester = register_session(&control, 8, 1, "alice");
        let _target = register_session(&control, 7, 1, "alice");
        let connection_control =
            FixedConnectionControl::new(ClientConnectionTerminateOutcome::Stale);

        let result = execute_kill_statement(
            "KILL QUERY 7",
            &parsed_kill("KILL QUERY 7"),
            requester.token(),
            &control,
            &connection_control,
        );

        assert!(matches!(result, Ok(QuerySessionOutput::Ok)));
        assert!(
            connection_control
                .calls
                .lock()
                .expect("calls lock")
                .is_empty()
        );
    }

    #[test]
    fn kill_connection_forms_accept_requested_and_already_terminating() {
        for outcome in [
            ClientConnectionTerminateOutcome::Requested,
            ClientConnectionTerminateOutcome::AlreadyTerminating,
        ] {
            for source in ["KILL 7", "KILL CONNECTION 7"] {
                let control = QueryApplicationControl::service();
                let requester = register_session(&control, 8, 1, "alice");
                let _target = register_session(&control, 7, 11, "alice");
                let connection_control = FixedConnectionControl::new(outcome);

                let result = execute_kill_statement(
                    source,
                    &parsed_kill(source),
                    requester.token(),
                    &control,
                    &connection_control,
                );

                assert!(
                    matches!(result, Ok(QuerySessionOutput::Ok)),
                    "{source}: {outcome:?}"
                );
                assert_eq!(
                    connection_control
                        .calls
                        .lock()
                        .expect("calls lock")
                        .as_slice(),
                    &[(
                        ClientConnectionToken::new(7, 11).expect("valid token"),
                        ClientConnectionTerminationReason::ExplicitKillConnection {
                            requester_session: requester.token().key(),
                        },
                    )]
                );
            }
        }
    }

    #[test]
    fn kill_connection_stale_target_maps_to_no_such_session() {
        let control = QueryApplicationControl::service();
        let requester = register_session(&control, 8, 1, "alice");
        let _target = register_session(&control, 7, 1, "alice");
        let connection_control =
            FixedConnectionControl::new(ClientConnectionTerminateOutcome::Stale);

        let error = execute_kill_statement(
            "KILL CONNECTION 7",
            &parsed_kill("KILL CONNECTION 7"),
            requester.token(),
            &control,
            &connection_control,
        )
        .expect_err("stale protocol target must be rejected");

        assert_eq!(error.kind(), QueryServiceErrorKind::NoSuchSession);
    }

    #[test]
    fn kill_denial_is_a_typed_admit_error_for_query_and_connection() {
        for source in ["KILL QUERY 7", "KILL CONNECTION 7"] {
            let control = QueryApplicationControl::service();
            let requester = register_session(&control, 8, 1, "alice");
            let _target = register_session(&control, 7, 1, "bob");
            let connection_control =
                FixedConnectionControl::new(ClientConnectionTerminateOutcome::Requested);

            let error = execute_kill_statement(
                source,
                &parsed_kill(source),
                requester.token(),
                &control,
                &connection_control,
            )
            .expect_err("cross-principal KILL must be denied");
            let user_error = error.user_error().expect("typed KILL error");

            assert_eq!(user_error.code().as_str(), "sql.admit.kill_denied");
            assert_eq!(user_error.phase(), novarocks_user_error::ErrorPhase::Admit);
            assert_eq!(
                user_error.location().map(|location| location.column()),
                Some(1)
            );
            assert!(
                connection_control
                    .calls
                    .lock()
                    .expect("calls lock")
                    .is_empty()
            );
        }
    }
}
