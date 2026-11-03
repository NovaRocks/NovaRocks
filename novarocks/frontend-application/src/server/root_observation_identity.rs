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

//! Default-off independent original-FE observation, with no MySQL gate owner.
use crate::application::FrontendApplicationError;
use novarocks_native_trust::{NativeProcessIdentity, NativeTrust};
use novarocks_types::FrontendProcessId;
use std::io::{self, Write};

pub(super) const DOMAIN: &str = "root-observation";
pub(super) fn emit(
    trust: &NativeTrust,
    grants: Vec<novarocks_failpoint::scope::GrantHandle>,
) -> Result<Option<novarocks_failpoint::scope::ActionClaim>, FrontendApplicationError> {
    if grants.is_empty() {
        return Ok(None);
    }
    if grants.iter().any(|grant| grant.domain() != DOMAIN) {
        return Err(FrontendApplicationError::server(
            "root observation received a foreign domain grant",
        ));
    }
    if grants.len() != 1 {
        return Err(FrontendApplicationError::server(
            "root observation requires exactly one grant",
        ));
    }
    let grant = grants.into_iter().next().expect("one grant");
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Parameters {
        action: String,
    }
    let parameters: Parameters = serde_json::from_value(grant.parameters().clone())
        .map_err(|_| FrontendApplicationError::server("root observation grant is invalid"))?;
    if parameters.action != "observe" {
        return Err(FrontendApplicationError::server(
            "root observation action is unsupported",
        ));
    }

    let Some(NativeProcessIdentity::Frontend(frontend)) = trust.local_process_identity() else {
        return Err(FrontendApplicationError::server(
            "root observation requires actual FE NativeTrust identity",
        ));
    };
    let mut claim = grant
        .claim_once(novarocks_failpoint::scope::ProcessBinding::Frontend(
            frontend,
        ))
        .map_err(FrontendApplicationError::server_root_observation)?;
    write_marker(&mut io::stdout().lock(), frontend)
        .map_err(FrontendApplicationError::server_root_observation)?;
    claim
        .record_hit()
        .map_err(FrontendApplicationError::server_root_observation)?;
    Ok(Some(claim))
}
fn write_marker(output: &mut impl Write, frontend: FrontendProcessId) -> io::Result<()> {
    let mut bytes = [0; 128];
    let length = {
        let mut cursor = io::Cursor::new(&mut bytes[..]);
        writeln!(
            cursor,
            "{} frontend_process_id={frontend}",
            novarocks_failpoint::observation_marker::MEM_1_M07_ROOT_OBSERVATION_FE
        )?;
        cursor.position() as usize
    };
    output.write_all(&bytes[..length])?;
    output.flush()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_native_uuid_literal_has_no_gate_or_secret_fields() {
        let mut bytes = Vec::new();
        write_marker(
            &mut bytes,
            "01890f6e-7a00-7123-8123-456789abcdef".parse().unwrap(),
        )
        .unwrap();
        assert_eq!(bytes, b"NOVAROCKS_MEM_1_M07_ROOT_OBSERVATION_FE frontend_process_id=01890f6e-7a00-7123-8123-456789abcdef\n");
    }
    #[test]
    fn actual_original_stdout_io_source_is_retained() {
        struct ActualSource;
        impl std::fmt::Debug for ActualSource {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("must not debug original source");
            }
        }
        impl std::fmt::Display for ActualSource {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("must not format original source")
            }
        }
        impl std::error::Error for ActualSource {}
        struct Fail;
        impl Write for Fail {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::other(ActualSource))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let io = write_marker(
            &mut Fail,
            "01890f6e-7a00-7123-8123-456789abcdef".parse().unwrap(),
        )
        .unwrap_err();
        let error = FrontendApplicationError::server_root_observation(io);
        let original = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap();
        assert!(
            original
                .get_ref()
                .unwrap()
                .downcast_ref::<ActualSource>()
                .is_some()
        );
        let debug = format!("{error:?}");
        assert!(debug.contains("source_retained: true"));
        assert!(!debug.contains("ActualSource"));
        assert_eq!(
            error.to_string(),
            "Server: root observation stdout IO failed"
        );
    }
}

#[cfg(all(test, unix))]
mod authorization_tests {
    use super::*;
    use anyhow::{Context, Result, ensure};
    use novarocks_failpoint::liveness::{PreparedRoleSession, RunnerSession};
    use novarocks_failpoint::scope::ProcessBinding;
    use novarocks_failpoint::session::{
        DECLARATION_VERSION, DomainGrant, SessionDeclaration, SessionRole,
    };
    use novarocks_native_trust::{
        DeploymentId, NativeCallerSubject, NativeTransportMode, ValidatedSharedSecret,
    };
    use std::time::{Duration, Instant};

    fn trust(frontend: FrontendProcessId) -> NativeTrust {
        let trust = NativeTrust::new(
            DeploymentId::parse("root-observation-owner-test").unwrap(),
            ValidatedSharedSecret::new(novarocks_secret::SecretValue::new(
                "0123456789abcdef0123456789abcdef",
            ))
            .unwrap(),
            NativeCallerSubject::parse("fe@127.0.0.1:19040").unwrap(),
            NativeTransportMode::Disabled,
        );
        trust
            .bind_process_identity(NativeProcessIdentity::Frontend(frontend))
            .unwrap();
        trust
    }

    #[test]
    fn actual_emit_requires_exact_launch_records_one_hit_and_ends_both_helpers() -> Result<()> {
        let directory = tempfile::Builder::new()
            .prefix("nr-root-auth-")
            .tempdir_in("/tmp")?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let frontend = FrontendProcessId::new_v7();
        let actual_trust = trust(frontend);
        ensure!(emit(&actual_trust, Vec::new())?.is_none());
        let declaration = SessionDeclaration {
            schema_version: DECLARATION_VERSION,
            session_id: "root-session".into(),
            launch_id: "root-launch".into(),
            role: SessionRole::Fe,
            lifetime_millis: 5_000,
            liveness_socket: directory.path().join("live.sock"),
            handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
            domain_grants: vec![DomainGrant {
                domain: DOMAIN.into(),
                grant_id: "root-observe-once".into(),
                parameters: serde_json::json!({"action":"observe"}),
            }],
        };
        let path = directory.path().join("declaration.json");
        declaration.write_private(&path)?;
        let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe)?;
        let mut runner = RunnerSession::start_until(declaration, deadline)?;
        let mut role = match prepared.bind(ProcessBinding::Frontend(frontend)) {
            Ok(role) => role,
            Err(primary) => {
                let cleanup = runner.end_and_join();
                return match cleanup {
                    Ok(()) => Err(primary.into()),
                    Err(cleanup) => Err(anyhow::Error::from(primary)
                        .context(format!("runner helper cleanup failed: {cleanup}"))),
                };
            }
        };
        let scope = role.scope().clone();
        let grants = scope.grants_for(DOMAIN);
        let primary = (|| -> Result<()> {
            ensure!(emit(&trust(FrontendProcessId::new_v7()), grants.clone()).is_err());
            ensure!(
                scope.observations().is_empty(),
                "foreign process consumed the grant"
            );
            let claim = emit(&actual_trust, grants.clone())?
                .context("authorized root observation did not execute")?;
            ensure!(
                emit(&actual_trust, grants.clone()).is_err(),
                "root grant was reused"
            );
            drop(claim);
            let observations = scope.observations();
            for event in ["claimed", "hit", "claim-released"] {
                ensure!(
                    observations
                        .iter()
                        .filter(|item| item.event == event)
                        .count()
                        == 1,
                    "root observation did not record exactly one {event}"
                );
            }
            Ok(())
        })();
        // End the original runner connection, then join the exact receiver.
        // Both cleanup results survive even when the real emit failed.
        let runner_cleanup = runner.end_and_join();
        scope.wait_closed();
        let role_cleanup = role.close_and_join();
        let cleanup = match (runner_cleanup, role_cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Err(secondary)) => Err(anyhow::Error::from(primary)
                .context(format!("role receiver cleanup failed: {secondary}"))),
            (Err(error), _) | (_, Err(error)) => Err(error.into()),
        };
        match (primary, cleanup) {
            (Ok(()), Ok(())) => {}
            (Err(primary), Err(cleanup)) => {
                return Err(primary.context(format!("root session cleanup failed: {cleanup:#}")));
            }
            (Err(error), _) | (_, Err(error)) => return Err(error),
        }
        ensure!(!scope.is_active(), "ended root scope remained active");
        ensure!(
            emit(&actual_trust, grants).is_err(),
            "ended launch regained authority"
        );
        ensure!(!directory.path().join("live.sock").exists());
        Ok(())
    }
}
