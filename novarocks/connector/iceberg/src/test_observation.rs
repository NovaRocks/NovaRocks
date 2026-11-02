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

//! Finite evidence observations selected by existing exact request identities.
use novarocks_failpoint::{
    scope::{ActionClaim, GrantHandle, ProcessBinding},
    target_binding::{
        StatementExecutionBinding, StatementInputNomination, StatementWorkNomination,
    },
};
use serde::Deserialize;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
#[derive(Clone, Default)]
pub struct IcebergObservationSlot(Arc<Mutex<Option<Vec<Observation>>>>);
impl std::fmt::Debug for IcebergObservationSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergObservationSlot")
            .finish_non_exhaustive()
    }
}
#[expect(
    clippy::enum_variant_names,
    reason = "Marker suffixes are part of the finite observation action names consumed by test controls"
)]
#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
enum Parameters {
    ReaderMarker {
        range_scope: (i64, i64, u64, i64, i64, i32),
        max_markers: usize,
    },
    ReaderStatementMarker {
        statement_run_token: String,
        binding_root: PathBuf,
        backend_index: usize,
        #[serde(default)]
        node_id: Option<i32>,
        max_markers: usize,
    },
    WriterMarker {
        write_session_id: uuid::Uuid,
        max_markers: usize,
    },
    WriterStatementMarker {
        statement_run_token: String,
        max_markers: usize,
    },
}
impl Parameters {
    fn marker_bound(&self) -> usize {
        match self {
            Self::ReaderMarker { max_markers, .. }
            | Self::ReaderStatementMarker { max_markers, .. }
            | Self::WriterMarker { max_markers, .. }
            | Self::WriterStatementMarker { max_markers, .. } => *max_markers,
        }
    }
    fn reader(&self) -> bool {
        matches!(
            self,
            Self::ReaderMarker { .. } | Self::ReaderStatementMarker { .. }
        )
    }
}
struct Observation {
    parameters: Parameters,
    grant: GrantHandle,
    claim: Option<ActionClaim>,
    remaining: usize,
    writer_input: Option<StatementInputNomination>,
    writer_nomination: Option<StatementWorkNomination>,
    write_session: Option<[u8; 16]>,
}
fn valid_token(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= 256
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}
fn same_nomination(a: &StatementWorkNomination, b: &StatementWorkNomination) -> bool {
    a.target_session_id == b.target_session_id
        && a.statement_run_token == b.statement_run_token
        && a.frontend_process_id == b.frontend_process_id
        && a.connection_id == b.connection_id
        && a.session_key == b.session_key
        && a.statement_generation == b.statement_generation
}
impl IcebergObservationSlot {
    pub const DOMAIN: &'static str = "iceberg-observation";
    pub fn activate(&self, grants: Vec<GrantHandle>) -> Result<(), &'static str> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| "Iceberg observation slot lock failed")?;
        if state.is_some() {
            return Err("Iceberg observation slot was already activated");
        }
        if grants.iter().any(|grant| grant.domain() != Self::DOMAIN) {
            return Err("Iceberg observation received a foreign domain grant");
        }
        if let Some(first) = grants.first() {
            let original = first.cancellation();
            if grants.iter().any(|grant| {
                let scope = grant.cancellation();
                scope.process() != original.process()
                    || scope.session_id() != original.session_id()
                    || scope.launch_id() != original.launch_id()
                    || scope.deadline() != original.deadline()
            }) {
                return Err("Iceberg observation grants must belong to one exact role launch");
            }
        }
        let mut observations = Vec::with_capacity(grants.len());
        for grant in grants {
            let parameters: Parameters = serde_json::from_value(grant.parameters().clone())
                .map_err(|_| "Iceberg observation grant is invalid")?;
            let remaining = parameters.marker_bound();
            if !(1..=4096).contains(&remaining) {
                return Err("Iceberg observation marker bound is invalid");
            }
            if parameters.reader()
                != matches!(grant.cancellation().process(), ProcessBinding::Backend(_))
            {
                return Err("Iceberg observation action belongs to another Native role");
            }
            match &parameters {
                Parameters::ReaderMarker {
                    range_scope: (qh, ql, attempt, fh, fl, node),
                    ..
                } => {
                    if (*qh == 0 && *ql == 0)
                        || *attempt == 0
                        || (*fh == 0 && *fl == 0)
                        || *node < 0
                    {
                        return Err("Iceberg reader observation needs an exact range scope");
                    }
                }
                Parameters::ReaderStatementMarker {
                    statement_run_token,
                    binding_root,
                    node_id,
                    ..
                } => {
                    if !valid_token(statement_run_token)
                        || !binding_root.is_absolute()
                        || !binding_root.is_dir()
                        || node_id.is_some_and(|n| n < 0)
                    {
                        return Err("Iceberg reader statement target is invalid");
                    }
                }
                Parameters::WriterMarker {
                    write_session_id, ..
                } => {
                    if write_session_id.is_nil() {
                        return Err("Iceberg writer observation needs an exact session");
                    }
                }
                Parameters::WriterStatementMarker {
                    statement_run_token,
                    ..
                } => {
                    if !valid_token(statement_run_token) {
                        return Err("Iceberg writer statement target is invalid");
                    }
                }
            }
            if observations
                .iter()
                .any(|old: &Observation| match (&old.parameters, &parameters) {
                    (
                        Parameters::ReaderMarker { range_scope: a, .. },
                        Parameters::ReaderMarker { range_scope: b, .. },
                    ) => a == b,
                    (
                        Parameters::ReaderStatementMarker {
                            statement_run_token: a,
                            node_id: an,
                            ..
                        },
                        Parameters::ReaderStatementMarker {
                            statement_run_token: b,
                            node_id: bn,
                            ..
                        },
                    ) => a == b && (an == bn || an.is_none() || bn.is_none()),
                    (
                        Parameters::WriterMarker {
                            write_session_id: a,
                            ..
                        },
                        Parameters::WriterMarker {
                            write_session_id: b,
                            ..
                        },
                    ) => a == b,
                    (
                        Parameters::WriterStatementMarker {
                            statement_run_token: a,
                            ..
                        },
                        Parameters::WriterStatementMarker {
                            statement_run_token: b,
                            ..
                        },
                    ) => a == b,
                    (a, b) => {
                        a.reader() == b.reader()
                            && std::mem::discriminant(a) != std::mem::discriminant(b)
                    }
                })
            {
                return Err("Iceberg observation targets duplicate or overlap");
            }
            observations.push(Observation {
                parameters,
                grant,
                claim: None,
                remaining,
                writer_input: None,
                writer_nomination: None,
                write_session: None,
            });
        }
        *state = Some(observations);
        Ok(())
    }
    fn validate_writer_input_scope(
        observation: &Observation,
        input: &StatementInputNomination,
    ) -> Result<(), String> {
        let cancellation = observation.grant.cancellation();
        let ProcessBinding::Frontend(frontend) = cancellation.process() else {
            return Err("Iceberg writer input role mismatch".into());
        };
        if !cancellation.is_active()
            || input.target_session_id != cancellation.session_id()
            || input.target_launch_id != cancellation.launch_id()
            || input.frontend_process_id != frontend.to_string()
        {
            return Err("Iceberg writer input belongs to another role launch".into());
        }
        Ok(())
    }
    /// Pin the actual client and SQL input in its predeclared finite slot.
    /// Observer journal retirement does not release this pin or consume a grant.
    pub fn pin_statement_input(&self, input: &StatementInputNomination) -> Result<(), String> {
        input
            .validate()
            .map_err(|_| "Iceberg writer input is invalid".to_owned())?;
        let mut state = self
            .0
            .lock()
            .map_err(|_| "Iceberg observation slot lock failed".to_owned())?;
        let Some(observations) = state.as_mut() else {
            return Ok(());
        };
        let matches = |observation: &Observation| {
            matches!(
                &observation.parameters,
                Parameters::WriterStatementMarker { statement_run_token, .. }
                    if *statement_run_token == input.statement_run_token
            )
        };
        // Validate every matching slot before making any first-input mutation.
        for observation in observations.iter().filter(|o| matches(o)) {
            Self::validate_writer_input_scope(observation, input)?;
            if observation
                .writer_input
                .as_ref()
                .is_some_and(|original| original != input)
            {
                return Err("Iceberg writer statement input replacement refused".into());
            }
        }
        for observation in observations.iter_mut().filter(|o| matches(o)) {
            if observation.writer_input.is_none() {
                observation.writer_input = Some(input.clone());
            }
        }
        Ok(())
    }
    /// Freeze the exact statement before a writer can reach a phase marker.
    /// The nomination itself conveys no authorization and consumes no product identity.
    pub fn nominate_writer_statement(
        &self,
        input: &StatementInputNomination,
        nomination: &StatementWorkNomination,
    ) -> Result<(), String> {
        input
            .validate_statement(nomination)
            .map_err(|_| "Iceberg writer input does not extend the actual statement".to_owned())?;
        let mut state = self
            .0
            .lock()
            .map_err(|_| "Iceberg observation slot lock failed".to_owned())?;
        let Some(observations) = state.as_mut() else {
            return Ok(());
        };
        let matches = |observation: &Observation| {
            matches!(
                &observation.parameters,
                Parameters::WriterStatementMarker { statement_run_token, .. }
                    if *statement_run_token == input.statement_run_token
            )
        };
        for observation in observations.iter().filter(|o| matches(o)) {
            Self::validate_writer_input_scope(observation, input)?;
            let Some(original_input) = observation.writer_input.as_ref() else {
                return Err("Iceberg writer statement input was not pinned".into());
            };
            if original_input != input {
                return Err("Iceberg writer statement input replacement refused".into());
            }
            if observation
                .writer_nomination
                .as_ref()
                .is_some_and(|original| !same_nomination(original, nomination))
            {
                return Err("Iceberg writer statement generation replacement refused".into());
            }
        }
        for observation in observations.iter_mut().filter(|o| matches(o)) {
            if observation.writer_nomination.is_none() {
                observation.writer_nomination = Some(nomination.clone());
            }
        }
        Ok(())
    }
    pub fn close(&self) {
        if let Ok(mut state) = self.0.lock() {
            *state = Some(Vec::new());
        }
    }
    pub fn cancellation(&self) -> Option<novarocks_failpoint::scope::ControlCancellation> {
        self.0
            .lock()
            .ok()?
            .as_ref()?
            .first()
            .map(|o| o.grant.cancellation())
    }
    pub async fn close_when_cancelled(&self) {
        if let Some(scope) = self.cancellation() {
            scope.cancelled().await;
            self.close();
        }
    }
    fn record(
        &self,
        matches: impl Fn(&Observation) -> bool,
        write_session: Option<[u8; 16]>,
        emit: impl FnOnce(),
    ) {
        let Ok(mut state) = self.0.lock() else {
            return;
        };
        let Some(observations) = state.as_mut() else {
            return;
        };
        let Some(observation) = observations
            .iter_mut()
            .find(|o| o.remaining > 0 && o.grant.cancellation().is_active() && matches(o))
        else {
            return;
        };
        let cancellation = observation.grant.cancellation();
        if !cancellation.is_active() {
            observation.claim = None;
            return;
        }
        if observation.remaining == 0 {
            return;
        }
        if observation.claim.is_none() {
            let Ok(claim) = observation.grant.claim_once(cancellation.process()) else {
                return;
            };
            observation.claim = Some(claim);
        }
        if write_session.is_some() {
            observation.write_session = write_session;
        }
        emit();
        if observation.remaining == observation.parameters.marker_bound() {
            let _ = observation
                .claim
                .as_mut()
                .expect("claimed observation")
                .record_hit();
        }
        observation.remaining -= 1;
    }
    pub(crate) fn reader(
        &self,
        scope: novarocks_spi::connector::ConnectorRangeScope,
        emit: impl FnOnce(),
    ) {
        self.record(
            |observation| match &observation.parameters {
                Parameters::ReaderMarker { range_scope, .. } => *range_scope == scope.parts(),
                Parameters::ReaderStatementMarker {
                    statement_run_token,
                    binding_root,
                    backend_index,
                    node_id,
                    ..
                } => {
                    let cancellation = observation.grant.cancellation();
                    let ProcessBinding::Backend(backend) = cancellation.process() else {
                        return false;
                    };
                    let path =
                        binding_root.join(format!("{statement_run_token}-be{backend_index}.json"));
                    let Ok(binding) = StatementExecutionBinding::read_private(&path) else {
                        return false;
                    };
                    let (qh, ql, attempt, _, _, node) = scope.parts();
                    binding.target_session_id == cancellation.session_id()
                        && binding.statement_run_token == *statement_run_token
                        && binding.backend_index == *backend_index
                        && binding.process().ok() == Some(backend)
                        && binding.query_id_hi == qh
                        && binding.query_id_lo == ql
                        && binding.attempt_id == attempt
                        && node_id.is_none_or(|expected| expected == node)
                }
                _ => false,
            },
            None,
            emit,
        );
    }
    pub(crate) fn writer(
        &self,
        session: [u8; 16],
        context: Option<&novarocks_spi::connector::ConnectorRequestContext>,
        emit: impl FnOnce(),
    ) {
        self.record(
            |observation| match &observation.parameters {
                Parameters::WriterMarker {
                    write_session_id, ..
                } => *write_session_id.as_bytes() == session,
                Parameters::WriterStatementMarker { .. } => {
                    let Some(nominated) = observation.writer_nomination.as_ref() else {
                        return false;
                    };
                    let Some(context) = context else {
                        return false;
                    };
                    let actual = context
                        .request_scope_extension_or_insert_with(StatementWorkNomination::default);
                    actual.validate().is_ok()
                        && same_nomination(nominated, &actual)
                        && observation
                            .write_session
                            .is_none_or(|bound| bound == session)
                }
                _ => false,
            },
            Some(session),
            emit,
        );
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn an_inert_slot_never_emits_or_creates_a_collector() {
        let slot = IcebergObservationSlot::default();
        slot.writer([1; 16], None, || panic!("no grant must emit nothing"));
        assert!(slot.0.lock().unwrap().is_none());
    }
    #[test]
    fn empty_explicit_grants_do_not_authorize_a_marker() {
        let slot = IcebergObservationSlot::default();
        slot.activate(Vec::new()).unwrap();
        slot.writer([1; 16], None, || panic!("empty grants must emit nothing"));
        assert!(slot.activate(Vec::new()).is_err());
        slot.close();
        assert!(slot.activate(Vec::new()).is_err());
    }
}

#[cfg(all(test, unix))]
mod exact_statement_tests {
    use super::*;
    use novarocks_failpoint::{
        liveness::{PreparedRoleSession, RunnerSession},
        session::{DECLARATION_VERSION, DomainGrant, SessionDeclaration, SessionRole},
    };
    use std::os::unix::fs::DirBuilderExt;
    #[test]
    fn writer_nomination_freezes_generation_before_the_real_session_binds() {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let root = PathBuf::from(format!(
            "/tmp/g2writer-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let mut declaration = SessionDeclaration {
            schema_version: DECLARATION_VERSION,
            session_id: "writer-session".into(),
            launch_id: "writer-launch".into(),
            role: SessionRole::Fe,
            lifetime_millis: 10_000,
            liveness_socket: root.join("live.sock"),
            handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
            domain_grants: vec![DomainGrant {
                domain: IcebergObservationSlot::DOMAIN.into(),
                grant_id: "one-writer".into(),
                parameters: serde_json::json!({"action":"writer-statement-marker","statement_run_token":"selected","max_markers":2}),
            }],
        };
        declaration.domain_grants.push(DomainGrant {
            domain: IcebergObservationSlot::DOMAIN.into(),
            grant_id: "fresh-writer".into(),
            parameters: serde_json::json!({"action":"writer-statement-marker","statement_run_token":"fresh","max_markers":1}),
        });
        let mut foreign = declaration.domain_grants[0].clone();
        foreign.domain = "foreign-domain".into();
        foreign.grant_id = "foreign-grant".into();
        declaration.domain_grants.push(foreign);
        let path = root.join("declaration.json");
        declaration.write_private(&path).unwrap();
        let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe).unwrap();
        let mut runner = RunnerSession::start(declaration).unwrap();
        let frontend = novarocks_types::FrontendProcessId::new_v7();
        let mut role = prepared.bind(ProcessBinding::Frontend(frontend)).unwrap();
        assert!(
            IcebergObservationSlot::default()
                .activate(role.scope().grants_for("foreign-domain"))
                .is_err()
        );
        let slot = IcebergObservationSlot::default();
        slot.activate(role.scope().grants_for(IcebergObservationSlot::DOMAIN))
            .unwrap();
        let nomination = StatementWorkNomination {
            target_session_id: "writer-session".into(),
            statement_run_token: "selected".into(),
            frontend_process_id: frontend.to_string(),
            connection_id: 7,
            session_key: 1,
            statement_generation: 3,
        };
        let input = StatementInputNomination {
            target_session_id: "writer-session".into(),
            target_launch_id: "writer-launch".into(),
            statement_run_token: "selected".into(),
            frontend_process_id: frontend.to_string(),
            connection_id: 7,
            session_key: 1,
            sql_sha256: [1; 32],
        };
        // A nominal statement cannot silently invent the missing arm-era input.
        assert!(slot.nominate_writer_statement(&input, &nomination).is_err());
        // Reject a foreign role launch even before this slot has a first input.
        for foreign_input in [
            StatementInputNomination {
                target_session_id: "another-session".into(),
                ..input.clone()
            },
            StatementInputNomination {
                target_launch_id: "another-launch".into(),
                ..input.clone()
            },
            StatementInputNomination {
                frontend_process_id: novarocks_types::FrontendProcessId::new_v7().to_string(),
                ..input.clone()
            },
        ] {
            assert!(slot.pin_statement_input(&foreign_input).is_err());
            assert!(
                slot.0.lock().unwrap().as_ref().unwrap()[0]
                    .writer_input
                    .is_none()
            );
        }
        slot.pin_statement_input(&input).unwrap();
        slot.pin_statement_input(&input).unwrap();
        {
            let state = slot.0.lock().unwrap();
            let observations = state.as_ref().unwrap();
            assert_eq!(observations[0].writer_input.as_ref(), Some(&input));
            assert!(observations[0].writer_nomination.is_none());
            assert!(observations[0].claim.is_none());
            assert_eq!(observations[0].remaining, 2);
            assert!(observations[1].writer_input.is_none());
        }
        // Before any actual statement is nominated, another caller/rearm still
        // sees the original finite-owner input. Journal drain has no slot reset API.
        let rearmed_caller = slot.clone();
        for replacement_input in [
            StatementInputNomination {
                connection_id: 8,
                ..input.clone()
            },
            StatementInputNomination {
                session_key: 2,
                ..input.clone()
            },
            StatementInputNomination {
                sql_sha256: [2; 32],
                ..input.clone()
            },
            StatementInputNomination {
                target_session_id: "another-session".into(),
                ..input.clone()
            },
            StatementInputNomination {
                target_launch_id: "another-launch".into(),
                ..input.clone()
            },
            StatementInputNomination {
                frontend_process_id: novarocks_types::FrontendProcessId::new_v7().to_string(),
                ..input.clone()
            },
        ] {
            assert!(
                rearmed_caller
                    .pin_statement_input(&replacement_input)
                    .is_err()
            );
            let replacement_nomination = StatementWorkNomination {
                target_session_id: replacement_input.target_session_id.clone(),
                statement_run_token: replacement_input.statement_run_token.clone(),
                frontend_process_id: replacement_input.frontend_process_id.clone(),
                connection_id: replacement_input.connection_id,
                session_key: replacement_input.session_key,
                statement_generation: nomination.statement_generation,
            };
            assert!(
                rearmed_caller
                    .nominate_writer_statement(&replacement_input, &replacement_nomination)
                    .is_err()
            );
        }
        let unknown_input = StatementInputNomination {
            statement_run_token: "undeclared".into(),
            ..input.clone()
        };
        slot.pin_statement_input(&unknown_input).unwrap();
        let unknown_nomination = StatementWorkNomination {
            statement_run_token: "undeclared".into(),
            ..nomination.clone()
        };
        slot.nominate_writer_statement(&unknown_input, &unknown_nomination)
            .unwrap();
        {
            let state = slot.0.lock().unwrap();
            let observations = state.as_ref().unwrap();
            assert_eq!(observations.len(), 2);
            assert_eq!(observations[0].writer_input.as_ref(), Some(&input));
            assert!(observations[0].writer_nomination.is_none());
            assert!(observations[1].writer_input.is_none());
        }
        // Fresh declared permission can select a different input independently.
        let fresh_input = StatementInputNomination {
            statement_run_token: "fresh".into(),
            connection_id: 9,
            session_key: 2,
            sql_sha256: [3; 32],
            ..input.clone()
        };
        let fresh_nomination = StatementWorkNomination {
            statement_run_token: "fresh".into(),
            connection_id: 9,
            session_key: 2,
            ..nomination.clone()
        };
        slot.pin_statement_input(&fresh_input).unwrap();
        slot.nominate_writer_statement(&fresh_input, &fresh_nomination)
            .unwrap();
        let different_sql = StatementInputNomination {
            sql_sha256: [2; 32],
            ..input.clone()
        };
        assert!(
            slot.nominate_writer_statement(&different_sql, &nomination)
                .is_err()
        );
        assert!(
            slot.nominate_writer_statement(&fresh_input, &nomination)
                .is_err()
        );
        slot.nominate_writer_statement(&input, &nomination).unwrap();
        slot.nominate_writer_statement(&input, &nomination).unwrap();
        let mut replacement = nomination.clone();
        replacement.statement_generation = 4;
        assert!(
            slot.nominate_writer_statement(&input, &replacement)
                .is_err()
        );
        assert!(
            !role
                .scope()
                .observations()
                .iter()
                .any(|o| matches!(o.event, "hit" | "claimed"))
        );
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let make_context = || {
            novarocks_spi::connector::ConnectorRequestContext::try_new(
                std::time::Instant::now() + std::time::Duration::from_secs(5),
                stop.view(),
                1024,
                4096,
            )
            .unwrap()
        };
        let context = make_context();
        let other = make_context();
        let count = std::sync::atomic::AtomicUsize::new(0);
        let emit = || {
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        };
        slot.writer([1; 16], Some(&context), emit);
        assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 0);
        // A default lookup above cannot be replaced by a late nomination in that
        // old request. Root installs before exposing the admitted context.
        let selected = make_context();
        selected.request_scope_extension_or_insert_with(|| nomination.clone());
        other.request_scope_extension_or_insert_with(|| replacement.clone());
        slot.writer([1; 16], Some(&selected), emit);
        slot.writer([2; 16], Some(&selected), emit);
        slot.writer([1; 16], Some(&other), emit);
        assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1);
        slot.writer([1; 16], Some(&selected), emit);
        slot.writer([1; 16], Some(&selected), emit);
        assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(
            role.scope()
                .observations()
                .iter()
                .filter(|o| o.event == "hit")
                .count(),
            1
        );
        role.close_and_join().unwrap();
        assert!(slot.pin_statement_input(&input).is_err());
        assert!(slot.nominate_writer_statement(&input, &nomination).is_err());
        slot.writer([1; 16], Some(&selected), emit);
        assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 2);
        slot.close();
        runner.end_and_join().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(all(test, unix))]
mod mixed_launch_tests {
    use super::*;
    use novarocks_failpoint::{
        liveness::{PreparedRoleSession, RunnerSession},
        session::{DECLARATION_VERSION, DomainGrant, SessionDeclaration, SessionRole},
    };
    use std::os::unix::fs::DirBuilderExt;
    #[test]
    fn public_slot_rejects_vectors_from_different_process_session_or_launch() {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        for case in 0..3 {
            let root = PathBuf::from(format!(
                "/tmp/g2mix-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&root)
                .unwrap();
            let make_declaration = |index: usize| SessionDeclaration {
                schema_version: DECLARATION_VERSION,
                session_id: if index == 1 && case == 1 {
                    "other-session"
                } else {
                    "same-session"
                }
                .into(),
                launch_id: if index == 1 && case == 2 {
                    "other-launch"
                } else {
                    "same-launch"
                }
                .into(),
                role: SessionRole::Fe,
                lifetime_millis: 10_000,
                liveness_socket: root.join(format!("live{index}.sock")),
                handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
                domain_grants: vec![DomainGrant {
                    domain: IcebergObservationSlot::DOMAIN.into(),
                    grant_id: format!("writer-{index}"),
                    parameters: serde_json::json!({"action":"writer-marker","write_session_id":uuid::Uuid::from_bytes([index as u8+1;16]),"max_markers":1}),
                }],
            };
            let declaration0 = make_declaration(0);
            let declaration1 = make_declaration(1);
            let path0 = root.join("declaration0.json");
            let path1 = root.join("declaration1.json");
            declaration0.write_private(&path0).unwrap();
            declaration1.write_private(&path1).unwrap();
            let prepared0 = PreparedRoleSession::read_and_claim(&path0, SessionRole::Fe).unwrap();
            let prepared1 = PreparedRoleSession::read_and_claim(&path1, SessionRole::Fe).unwrap();
            let mut runner0 = RunnerSession::start(declaration0).unwrap();
            let mut runner1 = RunnerSession::start(declaration1).unwrap();
            let frontend0 = novarocks_types::FrontendProcessId::new_v7();
            let frontend1 = if case == 0 {
                novarocks_types::FrontendProcessId::new_v7()
            } else {
                frontend0
            };
            let mut role0 = prepared0.bind(ProcessBinding::Frontend(frontend0)).unwrap();
            let mut role1 = prepared1.bind(ProcessBinding::Frontend(frontend1)).unwrap();
            let mut mixed = role0.scope().grants_for(IcebergObservationSlot::DOMAIN);
            mixed.extend(role1.scope().grants_for(IcebergObservationSlot::DOMAIN));
            let slot = IcebergObservationSlot::default();
            assert!(slot.activate(mixed).is_err());
            slot.activate(role0.scope().grants_for(IcebergObservationSlot::DOMAIN))
                .unwrap();
            assert!(
                !role0
                    .scope()
                    .observations()
                    .iter()
                    .any(|o| o.event == "claimed")
            );
            slot.close();
            role0.close_and_join().unwrap();
            role1.close_and_join().unwrap();
            runner0.end_and_join().unwrap();
            runner1.end_and_join().unwrap();
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
