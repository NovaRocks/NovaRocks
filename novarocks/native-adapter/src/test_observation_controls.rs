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

//! Finite Native-owned observations and independent catalog fault permissions.

use novarocks_execution::runtime::exchange::TestEventObserver;
use novarocks_failpoint::scope::{ActionClaim, GrantHandle, ProcessBinding};
use novarocks_failpoint::target_binding::{
    RoleProcessReceipt, StatementExecutionBinding, StatementInputNomination,
    StatementWorkNomination,
};
use novarocks_types::{BackendProcessId, QueryExecutionId};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const DOMAIN: &str = "native-observation";
const MAX_EVENTS: usize = 16_384;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Action {
    GrpcFragment,
    Reader,
    Writer,
    CatalogMaterialization,
    CatalogLifecycle,
    ExchangeSnapshot,
    CatalogHold,
    CatalogFailure,
}
impl Action {
    fn parse(value: &str) -> Result<Self, String> {
        Ok(match value {
            "grpc-fragment" => Self::GrpcFragment,
            "reader-marker" => Self::Reader,
            "writer-marker" => Self::Writer,
            "catalog-materialization" => Self::CatalogMaterialization,
            "catalog-lifecycle" => Self::CatalogLifecycle,
            "exchange-snapshot" => Self::ExchangeSnapshot,
            "catalog-hold" => Self::CatalogHold,
            "catalog-failure" => Self::CatalogFailure,
            _ => return Err("unknown or unsupported Native observation action".into()),
        })
    }
    fn name(self) -> &'static str {
        match self {
            Self::GrpcFragment => "grpc-fragment",
            Self::Reader => "reader-marker",
            Self::Writer => "writer-marker",
            Self::CatalogMaterialization => "catalog-materialization",
            Self::CatalogLifecycle => "catalog-lifecycle",
            Self::ExchangeSnapshot => "exchange-snapshot",
            Self::CatalogHold => "catalog-hold",
            Self::CatalogFailure => "catalog-failure",
        }
    }
    fn injection(self) -> bool {
        matches!(self, Self::CatalogHold | Self::CatalogFailure)
    }
}

#[derive(Default)]
struct NominationPin {
    input: Option<StatementInputNomination>,
    statement: Option<StatementWorkNomination>,
}

struct Authorized {
    grant: GrantHandle,
    action: Action,
    bind: bool,
    root: PathBuf,
    backend_index: usize,
    run_token: String,
    target_session_id: String,
    max_events: usize,
    attempt_id: Option<u64>,
    direct_execution: Option<QueryExecutionId>,
    target_process_receipt: Option<PathBuf>,
    frozen_nomination: Mutex<NominationPin>,
    observation: Mutex<Option<Arc<GrantedObservation>>>,
}

pub struct NativeObservationControls {
    process: ProcessBinding,
    actions: Vec<Authorized>,
    nominated_queries: Mutex<HashMap<String, novarocks_types::QueryId>>,
}
impl std::fmt::Debug for NativeObservationControls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeObservationControls")
            .field("process", &self.process)
            .field("actions", &self.actions.len())
            .finish()
    }
}

impl NativeObservationControls {
    pub fn from_grants(grants: Vec<GrantHandle>) -> Result<Self, String> {
        let first = grants
            .first()
            .ok_or("Native observations require finite grants")?
            .cancellation();
        let process = first.process();
        let session = first.session_id().to_string();
        let launch = first.launch_id().to_string();
        let mut actions: Vec<Authorized> = Vec::new();
        for grant in grants {
            if grant.domain() != DOMAIN {
                return Err("grant belongs to another Native observation domain".into());
            }
            let cancellation = grant.cancellation();
            if cancellation.process() != process
                || cancellation.session_id() != session
                || cancellation.launch_id() != launch
            {
                return Err("Native observations cannot combine role launches".into());
            }
            let object = grant
                .parameters()
                .as_object()
                .ok_or("Native observation parameters must be an object")?;
            let text = |key: &str| {
                object
                    .get(key)
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| format!("Native observation grant requires {key}"))
            };
            let name = text("action")?;
            let bind = name.starts_with("bind-");
            let action = Action::parse(name.strip_prefix("bind-").unwrap_or(name))?;
            if bind != matches!(process, ProcessBinding::Frontend(_)) {
                return Err("Native observation action does not match its actual role".into());
            }
            let direct = object.contains_key("query_execution");
            if direct && (bind || action.injection()) {
                return Err("direct Native query identity requires a BE observation action".into());
            }
            let allowed = if direct {
                &[
                    "action",
                    "root",
                    "backend_index",
                    "target_session_id",
                    "max_events",
                    "query_execution",
                ][..]
            } else if bind {
                &[
                    "action",
                    "root",
                    "backend_index",
                    "statement_run_token",
                    "target_session_id",
                    "target_process_receipt",
                    "attempt_id",
                ][..]
            } else if action.injection() {
                &[
                    "action",
                    "root",
                    "backend_index",
                    "statement_run_token",
                    "target_session_id",
                    "attempt_id",
                ][..]
            } else {
                &[
                    "action",
                    "root",
                    "backend_index",
                    "statement_run_token",
                    "target_session_id",
                    "max_events",
                    "attempt_id",
                ][..]
            };
            let target_process_receipt = if bind {
                let path = PathBuf::from(text("target_process_receipt")?);
                if !path.is_absolute() {
                    return Err("target role receipt path must be absolute".into());
                }
                Some(path)
            } else {
                None
            };
            if object.keys().any(|key| !allowed.contains(&key.as_str())) {
                return Err("unknown Native observation parameter".into());
            }
            let root = PathBuf::from(text("root")?);
            if !root.is_absolute() {
                return Err("Native observation root must be absolute".into());
            }
            let backend_index = object
                .get("backend_index")
                .and_then(serde_json::Value::as_u64)
                .and_then(|n| usize::try_from(n).ok())
                .ok_or("Native observation grant requires backend_index")?;
            let direct_execution = if direct {
                let value = object["query_execution"]
                    .as_object()
                    .ok_or("direct Native query_execution must be an object")?;
                if value.len() != 3
                    || value.keys().any(|key| {
                        !["query_id_hi", "query_id_lo", "attempt_id"].contains(&key.as_str())
                    })
                {
                    return Err(
                        "direct Native query_execution requires exact identity fields".into(),
                    );
                }
                let hi = value["query_id_hi"]
                    .as_i64()
                    .ok_or("direct query_id_hi must be an i64")?;
                let lo = value["query_id_lo"]
                    .as_i64()
                    .ok_or("direct query_id_lo must be an i64")?;
                let attempt = value["attempt_id"]
                    .as_u64()
                    .ok_or("direct attempt_id must be positive")?;
                Some(
                    QueryExecutionId::new(
                        novarocks_types::QueryId::new(hi, lo),
                        novarocks_types::AttemptId::new(attempt)
                            .map_err(|error| error.to_string())?,
                    )
                    .map_err(|error| error.to_string())?,
                )
            } else {
                None
            };
            let run_token = if direct {
                String::new()
            } else {
                text("statement_run_token")?.to_string()
            };
            // Optional exact refinement of the existing actual first-attempt
            // binding. Each declared ordinal owns a separate once grant.
            let attempt_id = object
                .get("attempt_id")
                .map(|value| {
                    let id = value
                        .as_u64()
                        .ok_or("Native observation attempt_id must be an integer")?;
                    novarocks_types::AttemptId::new(id)
                        .map_err(|_| "Native observation attempt_id must be positive")?;
                    Ok::<_, &str>(id)
                })
                .transpose()?;
            if run_token.len() > 256
                || !run_token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            {
                return Err("invalid Native observation statement token".into());
            }
            let target_session_id = text("target_session_id")?.to_string();
            if !bind && target_session_id != session {
                return Err("Native observation grant names another backend session".into());
            }
            let max_events = if bind || action.injection() {
                1
            } else {
                object
                    .get("max_events")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|n| usize::try_from(n).ok())
                    .filter(|n| *n > 0 && *n <= MAX_EVENTS)
                    .ok_or("Native observation grant needs a bounded positive max_events")?
            };
            if actions.iter().any(|old| {
                old.action == action
                    && old.bind == bind
                    && old.backend_index == backend_index
                    && old.run_token == run_token
                    && old.direct_execution == direct_execution
                    && (old.attempt_id == attempt_id
                        || old.attempt_id.is_none()
                        || attempt_id.is_none())
            }) {
                return Err("duplicate or conflicting Native observation grant".into());
            }
            actions.push(Authorized {
                grant,
                action,
                bind,
                root,
                backend_index,
                run_token,
                target_session_id,
                max_events,
                attempt_id,
                direct_execution,
                target_process_receipt,
                frozen_nomination: Mutex::new(NominationPin::default()),
                observation: Mutex::new(None),
            });
        }
        Ok(Self {
            process,
            actions,
            nominated_queries: Mutex::new(HashMap::new()),
        })
    }

    pub fn owns_backend(&self, process: BackendProcessId) -> bool {
        self.process == ProcessBinding::Backend(process)
    }

    /// Pin request input in the existing finite slots without creating an
    /// observation, writing evidence, or claiming action permission.
    pub fn pin_statement_input(&self, input: &StatementInputNomination) -> Result<(), String> {
        input.validate().map_err(|error| error.to_string())?;
        let frontend = input
            .frontend_process()
            .map_err(|error| error.to_string())?;
        let matching = self
            .actions
            .iter()
            .filter(|authorized| {
                authorized.bind && authorized.run_token == input.statement_run_token
            })
            .collect::<Vec<_>>();
        let mut pins = Vec::with_capacity(matching.len());
        for authorized in matching {
            Self::validate_input_owner(authorized, input, frontend)?;
            let pin = authorized
                .frozen_nomination
                .lock()
                .map_err(|_| "Native nomination lock failed")?;
            if pin.input.as_ref().is_some_and(|original| original != input) {
                return Err("Native original statement input replacement refused".into());
            }
            pins.push(pin);
        }
        // Keep all matching slot locks until validation completes, so an
        // error cannot leave a partially pinned domain.
        for pin in &mut pins {
            pin.input.get_or_insert_with(|| input.clone());
        }
        Ok(())
    }

    fn validate_input_owner(
        authorized: &Authorized,
        input: &StatementInputNomination,
        frontend: novarocks_types::FrontendProcessId,
    ) -> Result<(), String> {
        let cancellation = authorized.grant.cancellation();
        if !cancellation.is_active() {
            return Err("Native statement nomination session is closed".into());
        }
        if cancellation.process() != ProcessBinding::Frontend(frontend)
            || cancellation.session_id() != input.target_session_id
            || cancellation.launch_id() != input.target_launch_id
        {
            return Err("Native nomination belongs to another FE session".into());
        }
        Ok(())
    }

    /// Backend evidence and action claims remain separate later operations.
    pub fn nominate_statement(
        &self,
        input: &StatementInputNomination,
        nomination: &StatementWorkNomination,
    ) -> Result<(), String> {
        input
            .validate_statement(nomination)
            .map_err(|error| error.to_string())?;
        let frontend = input
            .frontend_process()
            .map_err(|error| error.to_string())?;
        let matching = self
            .actions
            .iter()
            .filter(|authorized| {
                authorized.bind && authorized.run_token == input.statement_run_token
            })
            .collect::<Vec<_>>();
        let mut pins = Vec::with_capacity(matching.len());
        for authorized in matching {
            Self::validate_input_owner(authorized, input, frontend)?;
            let pin = authorized
                .frozen_nomination
                .lock()
                .map_err(|_| "Native nomination lock failed")?;
            match pin.input.as_ref() {
                Some(original) if original == input => {}
                Some(_) => return Err("Native original statement input replacement refused".into()),
                None => {
                    return Err("Native statement input was not pinned before nomination".into());
                }
            }
            if pin
                .statement
                .as_ref()
                .is_some_and(|original| original != nomination)
            {
                return Err("Native nominated statement generation replacement refused".into());
            }
            pins.push(pin);
        }
        for pin in &mut pins {
            pin.statement.get_or_insert_with(|| nomination.clone());
        }
        Ok(())
    }

    /// Called only for the exact armed statement and selected live placement.
    pub fn publish_nominated_binding(
        &self,
        action: &str,
        nomination: &StatementWorkNomination,
        process: BackendProcessId,
        execution: QueryExecutionId,
    ) -> Result<bool, String> {
        nomination
            .validate()
            .map_err(|error| format!("Native publish nomination is invalid: {error}"))?;
        let action = Action::parse(action)?;
        for authorized in &self.actions {
            if !authorized.bind
                || authorized.action != action
                || authorized.run_token != nomination.statement_run_token
                || authorized
                    .attempt_id
                    .is_some_and(|id| id != execution.attempt_id().get())
            {
                continue;
            }
            let frozen = authorized
                .frozen_nomination
                .lock()
                .map_err(|_| "Native nomination lock failed")?;
            if frozen.statement.as_ref() != Some(nomination) {
                return Err("Native publish differs from the frozen original statement".into());
            }
            if !authorized.grant.cancellation().is_active() {
                return Err("Native statement nomination session is closed".into());
            }
            drop(frozen);
            let receipt = RoleProcessReceipt::read_private(
                authorized
                    .target_process_receipt
                    .as_ref()
                    .ok_or("FE binding grant lacks its target role receipt")?,
            )
            .map_err(|error| format!("read exact target role receipt: {error}"))?;
            if receipt.session_id != authorized.target_session_id {
                return Err("target role receipt names another backend session".into());
            }
            if receipt
                .backend_process()
                .map_err(|error| error.to_string())?
                != process
            {
                continue;
            }
            // All finite attempt slots for one nominated statement refine
            // the same logical query, including across backend placements.
            let mut queries = self
                .nominated_queries
                .lock()
                .map_err(|_| "Native nominated logical query lock failed")?;
            if let Some(original) = queries.get(&authorized.run_token) {
                if *original != execution.query_id() {
                    return Err(
                        "Native nominated statement changed its logical query identity".into(),
                    );
                }
            } else {
                queries.insert(authorized.run_token.clone(), execution.query_id());
            }
            drop(queries);
            let mut claim = authorized
                .grant
                .claim_once(self.process)
                .map_err(|error| error.to_string())?;
            let binding = StatementExecutionBinding::new(
                authorized.target_session_id.clone(),
                authorized.run_token.clone(),
                authorized.backend_index,
                process,
                execution,
            )
            .map_err(|error| error.to_string())?;
            let path = authorized.binding_path();
            if let Err(error) = binding.write_private(&path) {
                if error.kind() != std::io::ErrorKind::AlreadyExists {
                    return Err(format!(
                        "publish exact Native observation nomination: {error}"
                    ));
                }
                let existing = StatementExecutionBinding::read_private(&path)
                    .map_err(|error| format!("read existing exact Native nomination: {error}"))?;
                if existing.target_session_id != binding.target_session_id
                    || existing.statement_run_token != binding.statement_run_token
                    || existing.backend_index != binding.backend_index
                    || existing.process().map_err(|error| error.to_string())?
                        != binding.process().map_err(|error| error.to_string())?
                    || existing.execution().map_err(|error| error.to_string())?
                        != binding.execution().map_err(|error| error.to_string())?
                {
                    return Err(
                        "Native nomination conflicts with another generation/session/process"
                            .into(),
                    );
                }
            }
            claim.record_hit().map_err(|error| error.to_string())?;
            return Ok(true);
        }
        Ok(false)
    }

    pub fn observation_for(
        &self,
        action: &str,
        execution: QueryExecutionId,
    ) -> Result<Option<Arc<dyn TestEventObserver>>, String> {
        let action = Action::parse(action)?;
        if action.injection() {
            return Err("catalog injection cannot be granted by observation".into());
        }
        for authorized in &self.actions {
            if authorized.bind
                || authorized.action != action
                || !authorized.matches(self.process, execution)?
            {
                continue;
            }
            let mut cached = authorized
                .observation
                .lock()
                .map_err(|_| "Native observation state lock failed")?;
            let observation = cached
                .get_or_insert_with(|| {
                    Arc::new(GrantedObservation {
                        grant: authorized.grant.clone(),
                        process: self.process,
                        max_events: authorized.max_events,
                        state: Mutex::new(ObservationState {
                            claim: None,
                            events: 0,
                        }),
                        execution,
                    })
                })
                .clone();
            if observation.execution != execution {
                return Err("Native observation was already bound to another query attempt".into());
            }
            return Ok(Some(observation));
        }
        Ok(None)
    }

    pub fn catalog_hold(
        &self,
        execution: QueryExecutionId,
        released: impl Fn() -> bool,
    ) -> Result<(), String> {
        for authorized in &self.actions {
            if authorized.bind
                || authorized.action != Action::CatalogHold
                || !authorized.matches(self.process, execution)?
            {
                continue;
            }
            let file = authorized.root.join(format!(
                "catalog-install-hold-{}.trigger",
                authorized.backend_index
            ));
            if !file.exists() {
                continue;
            }
            let mut claim = match authorized.grant.claim_once(self.process) {
                Ok(claim) => claim,
                Err(_) => continue,
            };
            claim.record_hit().map_err(|error| error.to_string())?;
            while file.exists() {
                if released() {
                    return Err("catalog context released during authorized hold".into());
                }
                if !claim.cancellation().is_active() {
                    return Err("Native test session ended during catalog hold".into());
                }
                std::thread::park_timeout(Duration::from_millis(10));
            }
            if released() {
                return Err("catalog context released during authorized hold".into());
            }
            if !claim.cancellation().is_active() {
                return Err("Native test session ended during catalog hold".into());
            }
        }
        Ok(())
    }

    pub fn catalog_failure(&self, execution: QueryExecutionId) -> Result<bool, String> {
        for authorized in &self.actions {
            if authorized.bind
                || authorized.action != Action::CatalogFailure
                || !authorized.matches(self.process, execution)?
            {
                continue;
            }
            let file = authorized.root.join(format!(
                "catalog-install-failure-{}.trigger",
                authorized.backend_index
            ));
            if !file.exists() {
                continue;
            }
            let mut claim = match authorized.grant.claim_once(self.process) {
                Ok(claim) => claim,
                Err(_) => continue,
            };
            claim.record_hit().map_err(|error| error.to_string())?;
            return Ok(true);
        }
        Ok(false)
    }
}

impl Authorized {
    fn binding_path(&self) -> PathBuf {
        attempt_binding_path(
            &self.root,
            self.action.name(),
            self.backend_index,
            &self.run_token,
            self.attempt_id,
        )
    }
    fn matches(
        &self,
        process: ProcessBinding,
        execution: QueryExecutionId,
    ) -> Result<bool, String> {
        if !self.grant.cancellation().is_active() {
            return Ok(false);
        }
        if let Some(target) = self.direct_execution {
            return Ok(execution == target && self.grant.cancellation().process() == process);
        }
        if self
            .attempt_id
            .is_some_and(|id| id != execution.attempt_id().get())
        {
            return Ok(false);
        }
        let binding = match StatementExecutionBinding::read_private(&self.binding_path()) {
            Ok(binding) => binding,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(format!("read Native observation nomination: {error}")),
        };
        Ok(binding.target_session_id == self.target_session_id
            && binding.statement_run_token == self.run_token
            && binding.backend_index == self.backend_index
            && process
                == ProcessBinding::Backend(binding.process().map_err(|error| error.to_string())?)
            && binding.execution().map_err(|error| error.to_string())? == execution)
    }
}
pub fn binding_path(root: &Path, action: &str, backend_index: usize, run_token: &str) -> PathBuf {
    attempt_binding_path(root, action, backend_index, run_token, None)
}
pub fn attempt_binding_path(
    root: &Path,
    action: &str,
    backend_index: usize,
    run_token: &str,
    attempt_id: Option<u64>,
) -> PathBuf {
    {
        let _ = action;
        match attempt_id {
            Some(attempt) => root.join(format!(
                "{run_token}-be{backend_index}-attempt{attempt}.json"
            )),
            None => root.join(format!("{run_token}-be{backend_index}.json")),
        }
    }
}

struct ObservationState {
    claim: Option<ActionClaim>,
    events: usize,
}
struct GrantedObservation {
    grant: GrantHandle,
    process: ProcessBinding,
    execution: QueryExecutionId,
    max_events: usize,
    state: Mutex<ObservationState>,
}
impl std::fmt::Debug for GrantedObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrantedObservation")
            .field("execution", &self.execution)
            .field("max_events", &self.max_events)
            .finish()
    }
}
impl TestEventObserver for GrantedObservation {
    fn is_active(&self) -> bool {
        self.grant.cancellation().is_active()
            && self
                .state
                .lock()
                .is_ok_and(|state| state.events < self.max_events)
    }
    fn observe(&self, event: &str) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if !self.grant.cancellation().is_active() || state.events >= self.max_events {
            return;
        }
        if state.claim.is_none() {
            let Ok(claim) = self.grant.claim_once(self.process) else {
                return;
            };
            state.claim = Some(claim);
        }
        println!("{event}");
        let _ = std::io::Write::flush(&mut std::io::stdout());
        if state.events == 0
            && let Some(claim) = state.claim.as_mut()
        {
            let _ = claim.record_hit();
        }
        state.events += 1;
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use novarocks_failpoint::{
        liveness::{PreparedRoleSession, RoleSessionOwner, RunnerSession},
        session::{DomainGrant, SessionDeclaration, SessionRole},
    };
    use novarocks_types::{AttemptId, QueryId};
    use serde_json::{Value, json};
    struct Session {
        _directory: tempfile::TempDir,
        root: PathBuf,
        role: RoleSessionOwner,
        runner: RunnerSession,
    }
    impl Session {
        fn new(parameters: Vec<Value>, role: SessionRole) -> Self {
            Self::new_domain(parameters, role, DOMAIN)
        }
        fn new_domain(mut parameters: Vec<Value>, role: SessionRole, domain: &str) -> Self {
            let directory = tempfile::Builder::new()
                .prefix("nr-ob-")
                .tempdir_in("/tmp")
                .unwrap();
            let root = directory.path().to_owned();
            for parameters in &mut parameters {
                parameters["root"] = json!(root);
            }
            let declaration = SessionDeclaration {
                schema_version: 1,
                session_id: "native-be".into(),
                launch_id: "native-launch".into(),
                role,
                lifetime_millis: 3000,
                liveness_socket: root.join("live.sock"),
                handshake_secret: "n".repeat(32),
                domain_grants: parameters
                    .into_iter()
                    .enumerate()
                    .map(|(i, parameters)| DomainGrant {
                        domain: domain.into(),
                        grant_id: format!("native-{i}"),
                        parameters,
                    })
                    .collect(),
            };
            let file = root.join("session.json");
            declaration.write_private(&file).unwrap();
            let prepared = PreparedRoleSession::read_and_claim(&file, role).unwrap();
            let runner = RunnerSession::start(declaration).unwrap();
            let process = match role {
                SessionRole::Be => ProcessBinding::Backend(BackendProcessId::new_v7()),
                SessionRole::Fe => {
                    ProcessBinding::Frontend(novarocks_types::FrontendProcessId::new_v7())
                }
            };
            let role = prepared.bind(process).unwrap();
            Self {
                _directory: directory,
                root,
                role,
                runner,
            }
        }
        fn controls(&self) -> Result<NativeObservationControls, String> {
            NativeObservationControls::from_grants(self.role.scope().grants_for(DOMAIN))
        }
        fn bind(&self, action: &str, execution: QueryExecutionId, process: BackendProcessId) {
            StatementExecutionBinding::new(
                "native-be".into(),
                "statement-run".into(),
                2,
                process,
                execution,
            )
            .unwrap()
            .write_private(&binding_path(&self.root, action, 2, "statement-run"))
            .unwrap();
        }
        fn process(&self) -> BackendProcessId {
            let ProcessBinding::Backend(process) = self.role.scope().process() else {
                panic!("BE")
            };
            process
        }
        fn nomination(&self, run_token: &str, generation: u64) -> StatementWorkNomination {
            StatementWorkNomination {
                target_session_id: self.role.scope().session_id().into(),
                statement_run_token: run_token.into(),
                frontend_process_id: self.role.scope().process().process_id(),
                connection_id: 7,
                session_key: 1,
                statement_generation: generation,
            }
        }
        fn input(
            &self,
            nomination: &StatementWorkNomination,
            sql: &str,
        ) -> StatementInputNomination {
            use sha2::{Digest, Sha256};
            StatementInputNomination {
                target_session_id: nomination.target_session_id.clone(),
                target_launch_id: self.role.scope().launch_id().into(),
                statement_run_token: nomination.statement_run_token.clone(),
                frontend_process_id: nomination.frontend_process_id.clone(),
                connection_id: nomination.connection_id,
                session_key: nomination.session_key,
                sql_sha256: Sha256::digest(sql.as_bytes()).into(),
            }
        }
        fn close(&mut self) {
            self.role.close_and_join().unwrap();
            self.runner.end_and_join().unwrap();
        }
    }
    impl Drop for Session {
        fn drop(&mut self) {
            self.close();
        }
    }
    fn execution(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(attempt).unwrap()).unwrap()
    }
    fn grant(action: &str) -> Value {
        let mut grant = json!({"action":action,"root":"/tmp","backend_index":2,"statement_run_token":"statement-run","target_session_id":"native-be"});
        if !action.starts_with("bind-") && !matches!(action, "catalog-hold" | "catalog-failure") {
            grant["max_events"] = json!(2);
        }
        grant
    }

    fn direct_grant() -> Value {
        json!({"action":"grpc-fragment","root":"/tmp","backend_index":2,
            "target_session_id":"native-be","max_events":2,
            "query_execution":{"query_id_hi":7,"query_id_lo":9,"attempt_id":1}})
    }
    #[test]
    fn direct_request_identity_needs_no_mysql_nomination_and_never_widens() {
        let mut session = Session::new(vec![direct_grant()], SessionRole::Be);
        let controls = session.controls().unwrap();
        assert!(controls.owns_backend(session.process()));
        assert!(!controls.owns_backend(BackendProcessId::new_v7()));
        assert!(
            controls
                .observation_for("grpc-fragment", execution(2))
                .unwrap()
                .is_none()
        );
        let foreign =
            QueryExecutionId::new(QueryId::new(7, 10), AttemptId::new(1).unwrap()).unwrap();
        assert!(
            controls
                .observation_for("grpc-fragment", foreign)
                .unwrap()
                .is_none()
        );
        assert!(
            controls
                .observation_for("reader-marker", execution(1))
                .unwrap()
                .is_none()
        );
        assert!(session.role.scope().observations().is_empty());
        let observer = controls
            .observation_for("grpc-fragment", execution(1))
            .unwrap()
            .unwrap();
        observer.observe("NATIVE_TEST_EVENT direct-request");
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|item| item.event == "hit")
                .count(),
            1
        );
        session.close();
        assert!(!observer.is_active());
        assert!(
            controls
                .observation_for("grpc-fragment", execution(1))
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn direct_request_schema_refuses_mixed_injected_or_foreign_authority() {
        for patch in [
            json!({"statement_run_token":"statement-run"}),
            json!({"attempt_id":1}),
            json!({"action":"catalog-hold"}),
            json!({"target_session_id":"foreign"}),
            json!({"query_execution":{"query_id_hi":7,"query_id_lo":9,"attempt_id":0}}),
            json!({"query_execution":{"query_id_hi":7,"query_id_lo":9,"attempt_id":1,"other":1}}),
        ] {
            let mut parameters = direct_grant();
            for (key, value) in patch.as_object().unwrap() {
                parameters[key] = value.clone();
            }
            let session = Session::new(vec![parameters], SessionRole::Be);
            assert!(session.controls().is_err());
        }
        let mut parameters = direct_grant();
        parameters["action"] = json!("bind-grpc-fragment");
        let session = Session::new(vec![parameters], SessionRole::Fe);
        assert!(session.controls().is_err());
        let session = Session::new(vec![direct_grant(), direct_grant()], SessionRole::Be);
        assert!(session.controls().is_err());
    }

    #[test]
    fn exact_binding_data_does_not_grant_unrelated_actions() {
        let session = Session::new(vec![grant("reader-marker")], SessionRole::Be);
        let controls = session.controls().unwrap();
        session.bind("reader-marker", execution(1), session.process());
        assert!(
            controls
                .observation_for("writer-marker", execution(1))
                .unwrap()
                .is_none()
        );
        assert!(
            controls
                .observation_for("catalog-hold", execution(1))
                .is_err()
        );
        assert!(!controls.catalog_failure(execution(1)).unwrap());
        assert!(session.role.scope().observations().is_empty());
    }

    #[test]
    fn observation_validates_process_attempt_and_event_budget() {
        let session = Session::new(vec![grant("reader-marker")], SessionRole::Be);
        let controls = session.controls().unwrap();
        session.bind("reader-marker", execution(1), session.process());
        assert!(
            controls
                .observation_for("reader-marker", execution(2))
                .unwrap()
                .is_none()
        );
        assert!(session.role.scope().observations().is_empty());
        let sink = controls
            .observation_for("reader-marker", execution(1))
            .unwrap()
            .unwrap();
        sink.observe("NATIVE_TEST_EVENT first");
        assert!(sink.is_active());
        sink.observe("NATIVE_TEST_EVENT second");
        assert!(!sink.is_active());
        sink.observe("NATIVE_TEST_EVENT rejected-third");
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|e| e.event == "claimed")
                .count(),
            1
        );
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|e| e.event == "hit")
                .count(),
            1
        );
        let mismatch = Session::new(vec![grant("reader-marker")], SessionRole::Be);
        mismatch.bind("reader-marker", execution(1), BackendProcessId::new_v7());
        assert!(
            mismatch
                .controls()
                .unwrap()
                .observation_for("reader-marker", execution(1))
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn explicitly_listed_attempts_have_independent_once_budgets() {
        let parameters = [1, 2]
            .into_iter()
            .map(|attempt| {
                let mut value = grant("grpc-fragment");
                value["attempt_id"] = json!(attempt);
                value
            })
            .collect();
        let mut session = Session::new(parameters, SessionRole::Be);
        let controls = session.controls().unwrap();
        for attempt in [1, 2] {
            let binding = StatementExecutionBinding::new(
                "native-be".into(),
                "statement-run".into(),
                2,
                session.process(),
                execution(attempt),
            )
            .unwrap();
            binding
                .write_private(&attempt_binding_path(
                    &session.root,
                    "grpc-fragment",
                    2,
                    "statement-run",
                    Some(attempt),
                ))
                .unwrap();
            let observer = controls
                .observation_for("grpc-fragment", execution(attempt))
                .unwrap()
                .unwrap();
            observer.observe("NATIVE_TEST_EVENT exact-attempt");
        }
        assert!(
            controls
                .observation_for("grpc-fragment", execution(3))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count(),
            2
        );
        session.close();
        assert!(
            controls
                .observation_for("grpc-fragment", execution(1))
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn fe_only_binds_predeclared_attempt_ordinals_of_the_same_statement() {
        let target = Session::new(vec![grant("grpc-fragment")], SessionRole::Be);
        let values = [1, 2]
            .into_iter()
            .map(|attempt| {
                let mut value = grant("bind-grpc-fragment");
                value["attempt_id"] = json!(attempt);
                value["target_process_receipt"] = json!(target.runner.process_receipt_path());
                value
            })
            .collect();
        let session = Session::new(values, SessionRole::Fe);
        let controls = session.controls().unwrap();
        let nominated = session.nomination("statement-run", 1);
        let input = session.input(&nominated, "SELECT 1");
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &nominated).unwrap();
        assert!(
            !controls
                .publish_nominated_binding(
                    "grpc-fragment",
                    &nominated,
                    target.process(),
                    execution(3)
                )
                .unwrap()
        );
        for attempt in [1, 2] {
            if attempt == 2 {
                let foreign =
                    QueryExecutionId::new(QueryId::new(11, 12), AttemptId::new(2).unwrap())
                        .unwrap();
                assert!(
                    controls
                        .publish_nominated_binding(
                            "grpc-fragment",
                            &nominated,
                            target.process(),
                            foreign
                        )
                        .unwrap_err()
                        .contains("logical query identity")
                );
            }
            assert!(
                controls
                    .publish_nominated_binding(
                        "grpc-fragment",
                        &nominated,
                        target.process(),
                        execution(attempt)
                    )
                    .unwrap()
            );
            let path = attempt_binding_path(
                &session.root,
                "grpc-fragment",
                2,
                "statement-run",
                Some(attempt),
            );
            assert_eq!(
                StatementExecutionBinding::read_private(&path)
                    .unwrap()
                    .execution()
                    .unwrap(),
                execution(attempt)
            );
        }
        assert!(
            controls
                .nominate_statement(
                    &session.input(&session.nomination("statement-run", 2), "SELECT 1"),
                    &session.nomination("statement-run", 2)
                )
                .is_err()
        );
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count(),
            2
        );
        let mut first = grant("grpc-fragment");
        first["attempt_id"] = json!(1);
        let ambiguous = Session::new(vec![grant("grpc-fragment"), first], SessionRole::Be);
        assert!(ambiguous.controls().is_err());
        let mut invalid = grant("grpc-fragment");
        invalid["attempt_id"] = json!(0);
        assert!(
            Session::new(vec![invalid], SessionRole::Be)
                .controls()
                .is_err()
        );
    }

    #[test]
    fn closed_session_disables_an_already_bound_observer() {
        let mut session = Session::new(vec![grant("grpc-fragment")], SessionRole::Be);
        let controls = session.controls().unwrap();
        session.bind("grpc-fragment", execution(1), session.process());
        let sink = controls
            .observation_for("grpc-fragment", execution(1))
            .unwrap()
            .unwrap();
        session.close();
        sink.observe("NATIVE_TEST_EVENT closed");
        assert!(!sink.is_active());
        assert!(session.role.scope().observations().is_empty());
    }

    #[test]
    fn invalid_role_session_unbounded_and_unsupported_grants_reject() {
        let other_domain = Session::new_domain(
            vec![grant("reader-marker")],
            SessionRole::Be,
            "other-domain",
        );
        assert!(
            NativeObservationControls::from_grants(
                other_domain.role.scope().grants_for("other-domain")
            )
            .is_err()
        );
        assert!(NativeObservationControls::from_grants(Vec::new()).is_err());
        let role = Session::new(vec![grant("reader-marker")], SessionRole::Fe);
        assert!(role.controls().is_err());
        let mut wrong = grant("reader-marker");
        wrong["target_session_id"] = json!("another-be");
        let other = Session::new(vec![wrong], SessionRole::Be);
        assert!(other.controls().is_err());
        let mut unlimited = grant("reader-marker");
        unlimited["max_events"] = json!(0);
        let invalid = Session::new(vec![unlimited], SessionRole::Be);
        assert!(invalid.controls().is_err());
        let dead = Session::new(vec![grant("exec-node-output")], SessionRole::Be);
        assert!(dead.controls().is_err());
    }

    #[test]
    fn catalog_failure_is_separate_once_authority() {
        let session = Session::new(vec![grant("catalog-failure")], SessionRole::Be);
        let controls = session.controls().unwrap();
        session.bind("catalog-failure", execution(1), session.process());
        std::fs::write(
            session.root.join("catalog-install-failure-2.trigger"),
            "fail",
        )
        .unwrap();
        assert!(!controls.catalog_failure(execution(2)).unwrap());
        assert!(controls.catalog_failure(execution(1)).unwrap());
        assert!(!controls.catalog_failure(execution(1)).unwrap());
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|e| e.event == "hit")
                .count(),
            1
        );
    }

    #[test]
    fn catalogue_hold_observes_session_close_and_real_waiter_joins() {
        let mut session = Session::new(vec![grant("catalog-hold")], SessionRole::Be);
        let controls = session.controls().unwrap();
        session.bind("catalog-hold", execution(1), session.process());
        std::fs::write(session.root.join("catalog-install-hold-2.trigger"), "hold").unwrap();
        let waiter = std::thread::spawn(move || controls.catalog_hold(execution(1), || false));
        let until = std::time::Instant::now() + Duration::from_secs(1);
        while session
            .role
            .scope()
            .observations()
            .iter()
            .all(|e| e.event != "hit")
        {
            assert!(std::time::Instant::now() < until);
            std::thread::yield_now();
        }
        session.close();
        assert!(
            waiter
                .join()
                .unwrap()
                .unwrap_err()
                .contains("session ended")
        );
    }
    #[test]
    fn fe_nomination_is_exact_finite_and_same_record_idempotent_only() {
        let target = Session::new(vec![grant("reader-marker")], SessionRole::Be);
        let bind_grant = |name: &str| {
            let mut value = grant(name);
            value["target_process_receipt"] = json!(target.runner.process_receipt_path());
            value
        };
        let session = Session::new(
            vec![
                bind_grant("bind-reader-marker"),
                bind_grant("bind-catalog-lifecycle"),
                bind_grant("bind-writer-marker"),
            ],
            SessionRole::Fe,
        );
        let controls = session.controls().unwrap();
        let process = target.process();
        let nominated = session.nomination("statement-run", 1);
        let unrelated = session.nomination("unrelated", 1);
        let input = session.input(&nominated, "SELECT 1");
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &nominated).unwrap();

        assert!(
            !controls
                .publish_nominated_binding("reader-marker", &unrelated, process, execution(1))
                .unwrap()
        );
        assert!(
            !controls
                .publish_nominated_binding(
                    "reader-marker",
                    &nominated,
                    BackendProcessId::new_v7(),
                    execution(1)
                )
                .unwrap()
        );
        assert!(session.role.scope().observations().is_empty());
        let path = binding_path(&session.root, "reader-marker", 2, "statement-run");
        assert!(!path.exists());
        assert!(
            controls
                .publish_nominated_binding("reader-marker", &nominated, process, execution(1))
                .unwrap()
        );
        let before = StatementExecutionBinding::read_private(&path).unwrap();
        assert!(
            controls
                .publish_nominated_binding("catalog-lifecycle", &nominated, process, execution(1))
                .unwrap()
        );
        assert!(
            controls
                .publish_nominated_binding("writer-marker", &nominated, process, execution(2))
                .unwrap_err()
                .contains("conflicts")
        );
        assert_eq!(
            StatementExecutionBinding::read_private(&path).unwrap(),
            before
        );
        assert!(
            controls
                .publish_nominated_binding("reader-marker", &nominated, process, execution(1))
                .is_err()
        );
    }
    #[test]
    fn fe_unpublished_first_nomination_never_redirects_to_a_new_generation() {
        let target = Session::new(vec![grant("reader-marker")], SessionRole::Be);
        let mut bind = grant("bind-reader-marker");
        bind["target_process_receipt"] = json!(target.runner.process_receipt_path());
        let session = Session::new(vec![bind], SessionRole::Fe);
        let controls = session.controls().unwrap();
        let a = session.nomination("statement-run", 1);
        let unrelated = session.nomination("unlisted", 1);
        let input = session.input(&unrelated, "SELECT 1");
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &unrelated).unwrap();
        assert!(
            controls.actions[0]
                .frozen_nomination
                .lock()
                .unwrap()
                .statement
                .is_none()
        );
        assert!(
            controls
                .publish_nominated_binding("reader-marker", &a, target.process(), execution(1))
                .unwrap_err()
                .contains("frozen original")
        );
        let input = session.input(&a, "SELECT 1");
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &a).unwrap();
        let input = session.input(&a, "SELECT 1");
        controls.pin_statement_input(&input).unwrap();
        controls.nominate_statement(&input, &a).unwrap();
        let path = binding_path(&session.root, "reader-marker", 2, "statement-run");
        assert!(!path.exists());
        assert!(session.role.scope().observations().is_empty());
        let mut replacements = vec![session.nomination("statement-run", 2), a.clone(), a.clone()];
        replacements[1].session_key = 2;
        replacements[2].connection_id = 8;
        for b in replacements {
            assert!(
                controls
                    .nominate_statement(&session.input(&b, "SELECT 1"), &b)
                    .unwrap_err()
                    .contains("replacement")
            );
            assert!(
                controls
                    .publish_nominated_binding("reader-marker", &b, target.process(), execution(2))
                    .unwrap_err()
                    .contains("frozen original")
            );
        }
        assert!(!path.exists());
        assert!(session.role.scope().observations().is_empty());
        assert!(
            controls
                .publish_nominated_binding("reader-marker", &a, target.process(), execution(1))
                .unwrap()
        );
    }
    #[test]
    fn unbound_input_pin_refuses_client_sql_rearm_but_allows_a_fresh_declared_slot() {
        let mut values = [
            "bind-reader-marker",
            "bind-writer-marker",
            "bind-catalog-lifecycle",
        ]
        .into_iter()
        .map(|action| {
            let mut value = grant(action);
            value["target_process_receipt"] = json!("/tmp/g2-not-started.process.json");
            value
        })
        .collect::<Vec<_>>();
        values[2]["statement_run_token"] = json!("fresh-input");
        let mut session = Session::new(values, SessionRole::Fe);
        let controls = session.controls().unwrap();
        let nomination = session.nomination("statement-run", 1);
        let input = session.input(&nomination, "SELECT 1");
        assert!(
            controls
                .nominate_statement(&input, &nomination)
                .unwrap_err()
                .contains("not pinned")
        );
        let unknown = session.input(&session.nomination("unlisted", 1), "SELECT 1");
        controls.pin_statement_input(&unknown).unwrap();
        assert!(
            controls.actions.iter().all(|slot| slot
                .frozen_nomination
                .lock()
                .unwrap()
                .input
                .is_none())
        );
        controls.pin_statement_input(&input).unwrap();
        controls.pin_statement_input(&input).unwrap();
        assert!(
            controls.actions.iter().all(|slot| slot
                .frozen_nomination
                .lock()
                .unwrap()
                .statement
                .is_none())
        );
        let replacement = StatementWorkNomination {
            session_key: 2,
            ..nomination.clone()
        };
        let replacement_input = session.input(&replacement, "SELECT 1");
        let different_sql = session.input(&nomination, "SELECT 2");
        for changed in [
            replacement_input.clone(),
            different_sql.clone(),
            StatementInputNomination {
                connection_id: 8,
                ..input.clone()
            },
            StatementInputNomination {
                target_launch_id: "another-launch".into(),
                ..input.clone()
            },
            StatementInputNomination {
                target_session_id: "another-session".into(),
                ..input.clone()
            },
            StatementInputNomination {
                frontend_process_id: novarocks_types::FrontendProcessId::new_v7().to_string(),
                ..input.clone()
            },
        ] {
            assert!(controls.pin_statement_input(&changed).is_err());
        }
        assert!(
            controls
                .nominate_statement(&replacement_input, &replacement)
                .is_err()
        );
        assert!(
            controls
                .nominate_statement(&different_sql, &nomination)
                .is_err()
        );
        assert!(controls.nominate_statement(&input, &replacement).is_err());
        let fresh = StatementWorkNomination {
            statement_run_token: "fresh-input".into(),
            ..replacement.clone()
        };
        let fresh_input = session.input(&fresh, "SELECT 2");
        controls.pin_statement_input(&fresh_input).unwrap();
        controls.nominate_statement(&fresh_input, &fresh).unwrap();
        assert!(
            controls.actions[0]
                .frozen_nomination
                .lock()
                .unwrap()
                .statement
                .is_none()
        );
        assert!(
            controls.actions[1]
                .frozen_nomination
                .lock()
                .unwrap()
                .statement
                .is_none()
        );
        assert!(session.role.scope().observations().is_empty());
        assert!(
            controls
                .actions
                .iter()
                .all(|slot| slot.observation.lock().unwrap().is_none())
        );
        assert!(controls.nominated_queries.lock().unwrap().is_empty());
        assert!(!binding_path(&session.root, "reader-marker", 2, "statement-run").exists());
        session.close();
        assert!(
            controls
                .pin_statement_input(&input)
                .unwrap_err()
                .contains("closed")
        );
        assert!(
            controls
                .nominate_statement(&input, &nomination)
                .unwrap_err()
                .contains("closed")
        );
    }
}
