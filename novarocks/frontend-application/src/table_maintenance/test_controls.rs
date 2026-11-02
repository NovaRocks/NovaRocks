// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0 (the
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

//! Exact FE-local authorization for the existing maintenance rendezvous.
use novarocks_failpoint::scope::{ActionClaim, GrantHandle, ProcessBinding};
use novarocks_failpoint::target_binding::{StatementInputNomination, StatementWorkNomination};
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_table_maintenance::{MaintenanceTarget, OptimizeJob};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

pub(crate) const DOMAIN: &str = "table-maintenance";
const HOLD: &str = "hold-before-rebind";
const OBSERVE: &str = "dispatch-count";
const BARRIER_LIMIT: Duration = Duration::from_secs(30);

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Parameters {
    action: String,
    statement_run_token: String,
    root: PathBuf,
    catalog: String,
    namespace: String,
    table: String,
}
impl Parameters {
    fn target(&self) -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: self.catalog.clone(),
            namespace: self.namespace.clone(),
            table: self.table.clone(),
        }
    }
}
#[derive(Default)]
struct State {
    input: Option<StatementInputNomination>,
    nomination: Option<StatementWorkNomination>,
    submission_started: bool,
    captured_object: Option<Vec<u8>>,
    job_id: Option<i64>,
    work_id: Option<u64>,
    abandoned: bool,
}
struct Slot {
    parameters: Parameters,
    hold: GrantHandle,
    observe: GrantHandle,
    state: Mutex<State>,
    changed: Notify,
}
pub(crate) struct MaintenanceTestControls {
    process: ProcessBinding,
    slots: Vec<Arc<Slot>>,
}
impl MaintenanceTestControls {
    pub(crate) fn from_grants(grants: Vec<GrantHandle>) -> Result<Option<Arc<Self>>, String> {
        let Some(first) = grants.first() else {
            return Ok(None);
        };
        let first = first.cancellation();
        let process = first.process();
        if !matches!(process, ProcessBinding::Frontend(_)) {
            return Err("maintenance controls require an actual FE process".into());
        }
        let mut grouped: BTreeMap<String, (Parameters, Option<GrantHandle>, Option<GrantHandle>)> =
            BTreeMap::new();
        for grant in grants {
            let cancellation = grant.cancellation();
            if grant.domain() != DOMAIN
                || cancellation.process() != process
                || cancellation.session_id() != first.session_id()
                || cancellation.launch_id() != first.launch_id()
            {
                return Err("maintenance controls cannot adopt foreign domains or launches".into());
            }
            let parameters: Parameters = serde_json::from_value(grant.parameters().clone())
                .map_err(|error| format!("invalid maintenance control parameters: {error}"))?;
            if !matches!(parameters.action.as_str(), HOLD | OBSERVE)
                || parameters.statement_run_token.is_empty()
                || parameters.statement_run_token.len() > 256
                || !parameters
                    .statement_run_token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
                || [
                    &parameters.catalog,
                    &parameters.namespace,
                    &parameters.table,
                ]
                .iter()
                .any(|name| name.is_empty() || name.len() > 512)
            {
                return Err("unsupported maintenance action or invalid exact target".into());
            }
            private_root(&parameters.root)?;
            let slot = grouped
                .entry(parameters.statement_run_token.clone())
                .or_insert_with(|| (parameters.clone(), None, None));
            if slot.0.root != parameters.root || slot.0.target() != parameters.target() {
                return Err("maintenance actions disagree on their exact target".into());
            }
            let place = if parameters.action == HOLD {
                &mut slot.1
            } else {
                &mut slot.2
            };
            if place.replace(grant).is_some() {
                return Err("duplicate maintenance action for one statement".into());
            }
        }
        let mut slots = Vec::new();
        for (_, (parameters, hold, observe)) in grouped {
            slots.push(Arc::new(Slot {
                parameters,
                hold: hold.ok_or("maintenance rendezvous lacks its hold grant")?,
                observe: observe
                    .ok_or("maintenance rendezvous lacks its dispatch observer grant")?,
                state: Mutex::new(State::default()),
                changed: Notify::new(),
            }));
        }
        Ok(Some(Arc::new(Self { process, slots })))
    }
    pub(crate) fn pin_statement_input(
        &self,
        input: &StatementInputNomination,
    ) -> Result<(), String> {
        input.validate().map_err(|error| error.to_string())?;
        let mut states = Vec::new();
        for slot in &self.slots {
            if slot.parameters.statement_run_token != input.statement_run_token {
                continue;
            }
            self.validate_input(slot, input)?;
            let state = slot
                .state
                .lock()
                .map_err(|_| "maintenance input pin lock failed")?;
            if state.abandoned {
                return Err("maintenance owner is closed for this statement".into());
            }
            if state
                .input
                .as_ref()
                .is_some_and(|original| original != input)
            {
                return Err("maintenance statement input replacement refused".into());
            }
            states.push(state);
        }
        for mut state in states {
            if state.input.is_none() {
                state.input = Some(input.clone());
            }
        }
        Ok(())
    }
    fn validate_input(&self, slot: &Slot, input: &StatementInputNomination) -> Result<(), String> {
        let cancellation = slot.hold.cancellation();
        if self.process
            != ProcessBinding::Frontend(
                input
                    .frontend_process()
                    .map_err(|error| error.to_string())?,
            )
            || cancellation.session_id() != input.target_session_id
            || cancellation.launch_id() != input.target_launch_id
            || !cancellation.is_active()
        {
            return Err("maintenance input names a foreign or closed FE launch".into());
        }
        Ok(())
    }
    pub(crate) fn nominate_statement(
        &self,
        input: &StatementInputNomination,
        nomination: &StatementWorkNomination,
    ) -> Result<(), String> {
        input
            .validate_statement(nomination)
            .map_err(|error| error.to_string())?;
        let mut states = Vec::new();
        for slot in &self.slots {
            if slot.parameters.statement_run_token != input.statement_run_token {
                continue;
            }
            self.validate_input(slot, input)?;
            let state = slot
                .state
                .lock()
                .map_err(|_| "maintenance nomination lock failed")?;
            if state.abandoned {
                return Err("maintenance owner is closed for this statement".into());
            }
            match state.input.as_ref() {
                None => return Err("maintenance statement input was not pinned".into()),
                Some(original) if original != input => {
                    return Err("maintenance statement input replacement refused".into());
                }
                Some(_) => {}
            }
            if state
                .nomination
                .as_ref()
                .is_some_and(|original| original != nomination)
            {
                return Err("maintenance nomination changed its exact statement generation".into());
            }
            states.push(state);
        }
        for mut state in states {
            if state.nomination.is_none() {
                state.nomination = Some(nomination.clone());
            }
        }
        Ok(())
    }
    pub(crate) fn begin_submission(
        self: &Arc<Self>,
        nomination: &StatementWorkNomination,
        target: &MaintenanceTarget,
    ) -> Result<Option<SubmissionReceipt>, String> {
        for slot in &self.slots {
            if slot.parameters.statement_run_token != nomination.statement_run_token {
                continue;
            }
            let mut state = slot
                .state
                .lock()
                .map_err(|_| "maintenance submission lock failed")?;
            if state.abandoned
                || slot.parameters.target() != *target
                || state.nomination.as_ref() != Some(nomination)
                || !slot.hold.cancellation().is_active()
            {
                return Err(
                    "maintenance submission differs from its frozen statement target".into(),
                );
            }
            if state.submission_started {
                return Err("maintenance statement submission was already consumed".into());
            }
            state.submission_started = true;
            return Ok(Some(SubmissionReceipt {
                slot: slot.clone(),
                finished: false,
            }));
        }
        Ok(None)
    }
    pub(crate) async fn before_rebind(
        &self,
        job: &OptimizeJob,
        capacity: &QueryResultCapacityBinding,
    ) -> Result<Option<AuthorizedJob>, String> {
        'slots: for slot in &self.slots {
            if slot.parameters.target() != job.target {
                continue;
            }
            let deadline = {
                let state = slot
                    .state
                    .lock()
                    .map_err(|_| "maintenance association lock failed")?;
                // Publication cannot precede capture. Only this captured incarnation
                // may wait for its already-running submit operation to return its ID.
                if state.captured_object.as_deref() != Some(job.object_id.as_slice())
                    || state.abandoned
                {
                    continue;
                }
                (Instant::now() + BARRIER_LIMIT).min(slot.hold.cancellation().deadline())
            };
            loop {
                // Register before reading State so bind/close cannot be lost
                // between checking the original receipt and yielding this poll.
                let changed = slot.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let bound = {
                    let mut state = slot
                        .state
                        .lock()
                        .map_err(|_| "maintenance association lock failed")?;
                    if state.job_id.is_none() && !state.abandoned {
                        check_live(slot, capacity, deadline)?;
                        false
                    } else {
                        if state.abandoned || state.job_id != Some(job.job_id) {
                            continue 'slots;
                        }
                        check_live(slot, capacity, deadline)?;
                        let work_id = capacity.scope().id().get();
                        if work_id == 0 || state.work_id.replace(work_id).is_some() {
                            return Err("maintenance job was already bound to a work scope".into());
                        }
                        true
                    }
                };
                if bound {
                    break;
                }
                tokio::select! {
                    _ = changed => {},
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                }
            }
            let hold = slot
                .hold
                .claim_once(self.process)
                .map_err(|error| error.to_string())?;
            let observe = slot
                .observe
                .claim_once(self.process)
                .map_err(|error| error.to_string())?;
            let mut authorized = AuthorizedJob {
                slot: slot.clone(),
                job: job.clone(),
                capacity: capacity.clone(),
                hold,
                observe,
                dispatches: 0,
            };
            authorized.wait_before_rebind(deadline).await?;
            return Ok(Some(authorized));
        }
        Ok(None)
    }
    pub(crate) fn close(&self) -> Result<(), String> {
        for slot in &self.slots {
            let mut state = slot
                .state
                .lock()
                .map_err(|_| "maintenance close lock failed")?;
            state.abandoned = true;
            drop(state);
            slot.changed.notify_waiters();
        }
        Ok(())
    }
}

pub(crate) struct SubmissionReceipt {
    slot: Arc<Slot>,
    finished: bool,
}
impl SubmissionReceipt {
    pub(crate) fn capture(&self, object: &[u8]) -> Result<(), String> {
        if object.is_empty()
            || object.len() > novarocks_spi::connector::MAX_CONNECTOR_TABLE_OBJECT_ID_BYTES
        {
            return Err("maintenance captured an empty object identity".into());
        }
        let mut state = self
            .slot
            .state
            .lock()
            .map_err(|_| "maintenance capture lock failed")?;
        if state.abandoned
            || state.captured_object.is_some()
            || !self.slot.hold.cancellation().is_active()
        {
            return Err("maintenance capture was already consumed or closed".into());
        }
        state.captured_object = Some(object.to_vec());
        Ok(())
    }
    pub(crate) fn bind(mut self, job_id: i64) -> Result<(), String> {
        let mut state = self
            .slot
            .state
            .lock()
            .map_err(|_| "maintenance receipt lock failed")?;
        if job_id <= 0
            || state.captured_object.is_none()
            || state.job_id.is_some()
            || state.abandoned
            || !self.slot.hold.cancellation().is_active()
        {
            return Err("maintenance receipt lacks its exact captured submission".into());
        }
        state.job_id = Some(job_id);
        self.finished = true;
        drop(state);
        self.slot.changed.notify_waiters();
        Ok(())
    }
}
impl Drop for SubmissionReceipt {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Ok(mut state) = self.slot.state.lock() {
            state.abandoned = true;
            drop(state);
            self.slot.changed.notify_waiters();
        }
    }
}

pub(crate) struct AuthorizedJob {
    slot: Arc<Slot>,
    job: OptimizeJob,
    capacity: QueryResultCapacityBinding,
    hold: ActionClaim,
    observe: ActionClaim,
    dispatches: u64,
}
impl AuthorizedJob {
    fn path(&self, suffix: &str) -> PathBuf {
        self.slot.parameters.root.join(format!(
            "{}.{}",
            self.slot.parameters.statement_run_token, suffix
        ))
    }
    async fn wait_before_rebind(&mut self, deadline: Instant) -> Result<(), String> {
        check_live(&self.slot, &self.capacity, deadline)?;
        if self.path("before-rebind.resume").exists() {
            return Err("maintenance resume existed before its exact barrier".into());
        }
        write_fresh(&self.path("dispatch-count"), b"0\n")?;
        self.hold.record_hit().map_err(|error| error.to_string())?;
        let nomination = self
            .slot
            .state
            .lock()
            .map_err(|_| "maintenance receipt nomination lock failed")?
            .nomination
            .clone()
            .ok_or("maintenance receipt lost its exact nomination")?;
        let ready = serde_json::json!({
            "schema_version":1,"job_id":self.job.job_id,"work_id":self.capacity.scope().id().get(),
            "phase":"after-claim-before-rebind","catalog":self.job.target.catalog,
            "namespace":self.job.target.namespace,"table":self.job.target.table,
            "object_id":self.job.object_id,
            "launch_id":self.slot.hold.cancellation().launch_id(),
            "nomination": {
                "target_session_id": nomination.target_session_id,
                "statement_run_token": nomination.statement_run_token,
                "frontend_process_id": nomination.frontend_process_id,
                "connection_id": nomination.connection_id,
                "session_key": nomination.session_key,
                "statement_generation": nomination.statement_generation,
            },
        });
        let ready_bytes = serde_json::to_vec(&ready).map_err(|error| error.to_string())?;
        let staged_ready = self.path("before-rebind.ready.pending");
        write_fresh(&staged_ready, &ready_bytes)?;
        // Publish only complete immutable bytes. The canonical name is exclusive;
        // the staging hard link remains in this attempt as original evidence.
        fs::hard_link(&staged_ready, self.path("before-rebind.ready"))
            .map_err(|error| format!("publish exact maintenance receipt: {error}"))?;
        while !self.path("before-rebind.resume").exists() {
            let changed = self.slot.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            check_live(&self.slot, &self.capacity, deadline)?;
            {
                let state = self
                    .slot
                    .state
                    .lock()
                    .map_err(|_| "maintenance barrier lock failed")?;
                if state.abandoned {
                    return Err("maintenance observation closed during hold".into());
                }
            }
            // Resume is an external file; retain the original finite 10 ms
            // observation policy without blocking the common Action poll.
            tokio::select! {
                _ = changed => {},
                _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
        }
        check_live(&self.slot, &self.capacity, deadline)
    }
    pub(crate) fn record_dispatch(
        &mut self,
        job: &OptimizeJob,
        capacity: &QueryResultCapacityBinding,
    ) -> Result<(), String> {
        if self.job.job_id != job.job_id
            || self.job.target != job.target
            || self.job.object_id != job.object_id
            || !capacity.window_alias().is_for_scope(self.capacity.scope())
            || capacity.scope().id() != self.capacity.scope().id()
        {
            return Err("maintenance dispatch differs from its exact job/work owner".into());
        }
        if self
            .slot
            .state
            .lock()
            .map_err(|_| "maintenance dispatch state lock failed")?
            .abandoned
        {
            return Err("maintenance owner closed before dispatch observation".into());
        }
        check_live(
            &self.slot,
            capacity,
            self.slot.observe.cancellation().deadline(),
        )?;
        self.dispatches = self
            .dispatches
            .checked_add(1)
            .ok_or("maintenance dispatch counter overflow")?;
        fs::write(
            self.path("dispatch-count"),
            format!("{}\n", self.dispatches),
        )
        .map_err(|error| format!("write exact maintenance dispatch count: {error}"))?;
        if self.dispatches == 1 {
            self.observe
                .record_hit()
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}
fn check_live(
    slot: &Slot,
    capacity: &QueryResultCapacityBinding,
    deadline: Instant,
) -> Result<(), String> {
    if Instant::now() >= deadline {
        return Err("timed out waiting for exact maintenance resume or submission receipt".into());
    }
    if !slot.hold.cancellation().is_active() || !slot.observe.cancellation().is_active() {
        return Err("maintenance test session closed".into());
    }
    capacity
        .scope()
        .check()
        .map_err(|error| error.to_string())?;
    if capacity
        .scope()
        .cancellation()
        .map_err(|error| error.to_string())?
        .reason()
        .is_some()
    {
        return Err("maintenance work was cancelled during its authorized hold".into());
    }
    Ok(())
}
fn private_root(root: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(root)
        .map_err(|error| format!("read maintenance private root: {error}"))?;
    if !root.is_absolute() || !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("maintenance transport root must be a private regular directory".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("maintenance transport root is not private".into());
        }
    }
    Ok(())
}
fn write_fresh(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .and_then(|mut file| file.write_all(bytes))
        .map_err(|error| {
            format!(
                "write exact maintenance observation {}: {error}",
                path.display()
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_failpoint::liveness::{PreparedRoleSession, RoleSessionOwner, RunnerSession};
    use novarocks_failpoint::session::{
        DECLARATION_VERSION, DomainGrant, SessionDeclaration, SessionRole,
    };
    use novarocks_table_maintenance::job_service::{
        CapturedOptimizeTarget, OptimizeJobService, OptimizeTargetCapturePort,
    };
    use novarocks_workload_control::{
        CancellationReason, PendingQueryRoot, QueryConcurrencyPermit, ResourceConfig,
        ResultCapacityConfig, ResultWindowClass, ResultWindowGrant, WorkClass, WorkRequest,
        WorkloadConfig, WorkloadControl,
    };

    struct TestSession {
        root: tempfile::TempDir,
        nomination: StatementWorkNomination,
        role: RoleSessionOwner,
        runner: RunnerSession,
    }
    impl TestSession {
        fn new(lifetime_millis: u64, actions: &[&str]) -> Self {
            Self::with_tokens(lifetime_millis, actions, &["selected-statement"])
        }
        fn with_tokens(lifetime_millis: u64, actions: &[&str], tokens: &[&str]) -> Self {
            let root = tempfile::tempdir().unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
            }
            let process = novarocks_types::FrontendProcessId::new_v7();
            let grants = tokens
                .iter()
                .flat_map(|token| {
                    let root = &root;
                    actions.iter().map(move |action| DomainGrant {
                        domain: DOMAIN.into(),
                        grant_id: if tokens.len() == 1 {
                            (*action).into()
                        } else {
                            format!("{token}-{action}")
                        },
                        parameters: serde_json::json!({
                            "action":action,"statement_run_token":token,"root":root.path(),
                            "catalog":"catalog","namespace":"ns","table":"orders",
                        }),
                    })
                })
                .collect();
            let declaration = SessionDeclaration {
                schema_version: DECLARATION_VERSION,
                session_id: "maintenance-session".into(),
                launch_id: "maintenance-launch".into(),
                role: SessionRole::Fe,
                lifetime_millis,
                liveness_socket: root.path().join("live.sock"),
                handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
                domain_grants: grants,
            };
            let path = root.path().join("declaration.json");
            declaration.write_private(&path).unwrap();
            let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe).unwrap();
            let runner = RunnerSession::start(declaration).unwrap();
            let role = prepared.bind(ProcessBinding::Frontend(process)).unwrap();
            let nomination = StatementWorkNomination {
                target_session_id: "maintenance-session".into(),
                statement_run_token: "selected-statement".into(),
                frontend_process_id: process.to_string(),
                connection_id: 17,
                session_key: 3,
                statement_generation: 2,
            };
            Self {
                root,
                nomination,
                role,
                runner,
            }
        }
        fn input(&self) -> StatementInputNomination {
            use sha2::Digest;
            StatementInputNomination {
                target_session_id: self.nomination.target_session_id.clone(),
                target_launch_id: self.role.scope().launch_id().into(),
                statement_run_token: self.nomination.statement_run_token.clone(),
                frontend_process_id: self.nomination.frontend_process_id.clone(),
                connection_id: self.nomination.connection_id,
                session_key: self.nomination.session_key,
                sql_sha256: sha2::Sha256::digest(b"ALTER TABLE orders OPTIMIZE").into(),
            }
        }
        fn grants(&self) -> Vec<GrantHandle> {
            self.role.scope().grants_for(DOMAIN)
        }
    }
    impl Drop for TestSession {
        fn drop(&mut self) {
            self.role.close_and_join().unwrap();
            self.runner.end_and_join().unwrap();
        }
    }
    struct Fixture {
        session: TestSession,
        controls: Arc<MaintenanceTestControls>,
    }
    impl std::ops::Deref for Fixture {
        type Target = TestSession;
        fn deref(&self) -> &TestSession {
            &self.session
        }
    }
    impl Fixture {
        fn new() -> Self {
            Self::with_lifetime(10_000)
        }
        fn with_lifetime(lifetime_millis: u64) -> Self {
            let session = TestSession::new(lifetime_millis, &[HOLD, OBSERVE]);
            let controls = MaintenanceTestControls::from_grants(session.grants())
                .unwrap()
                .unwrap();
            controls.pin_statement_input(&session.input()).unwrap();
            controls
                .nominate_statement(&session.input(), &session.nomination)
                .unwrap();
            Self { session, controls }
        }
        fn ready(&self) -> PathBuf {
            self.root
                .path()
                .join("selected-statement.before-rebind.ready")
        }
        fn resume(&self) -> PathBuf {
            self.root
                .path()
                .join("selected-statement.before-rebind.resume")
        }
        fn count(&self) -> PathBuf {
            self.root.path().join("selected-statement.dispatch-count")
        }
        fn wait_until_held(&self) {
            let failure_watchdog =
                (Instant::now() + Duration::from_secs(2)).min(self.role.scope().deadline());
            while !self.ready().exists() && Instant::now() < failure_watchdog {
                std::thread::sleep(Duration::from_millis(2));
            }
            assert!(
                self.ready().exists(),
                "actual maintenance hold was not reached"
            );
        }
    }
    fn drive<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future)
    }
    fn target(table: &str) -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: "catalog".into(),
            namespace: "ns".into(),
            table: table.into(),
        }
    }
    fn new_workload() -> WorkloadControl {
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        workload
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        workload.mark_ready().unwrap();
        workload
    }
    struct Admitted {
        _workload: WorkloadControl,
        root: Option<PendingQueryRoot>,
        query_concurrency: Option<QueryConcurrencyPermit>,
        window: Option<ResultWindowGrant>,
        capacity: Option<QueryResultCapacityBinding>,
    }
    impl Admitted {
        fn new() -> Self {
            let workload = new_workload();
            let root = workload
                .root_admission()
                .begin_warehouse_root(WorkRequest::new(WorkClass::TableMaintenance))
                .unwrap();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let (query_concurrency, window) = runtime
                .block_on(
                    root.owner
                        .scope()
                        .admit_query_with_result(ResultWindowClass::Internal)
                        .unwrap(),
                )
                .unwrap();
            let capacity =
                QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias())
                    .unwrap();
            Self {
                _workload: workload,
                root: Some(root),
                query_concurrency: Some(query_concurrency),
                window: Some(window),
                capacity: Some(capacity),
            }
        }
        fn capacity(&self) -> &QueryResultCapacityBinding {
            self.capacity.as_ref().unwrap()
        }
        fn cancel(&self) {
            self.root
                .as_ref()
                .unwrap()
                .owner
                .cancel(CancellationReason::ServerShutdown);
        }
    }
    impl Drop for Admitted {
        fn drop(&mut self) {
            drop(self.query_concurrency.take());
            let root = self.root.take().unwrap();
            root.owner.complete();
            drop(self.capacity.take());
            drop(self.window.take());
        }
    }
    struct Capture<'a> {
        receipt: Option<&'a SubmissionReceipt>,
    }
    impl OptimizeTargetCapturePort for Capture<'_> {
        fn capture(&self, _: &MaintenanceTarget) -> Result<CapturedOptimizeTarget, String> {
            let object_id = vec![1, 2, 3];
            if let Some(receipt) = self.receipt {
                receipt.capture(&object_id)?;
            }
            Ok(CapturedOptimizeTarget {
                object_id,
                base_snapshot_id: -1,
                completion: novarocks_table_maintenance::OptimizeCompletionMode::Detached,
            })
        }
    }
    struct RealJobs {
        service: OptimizeJobService,
        runtime: tokio::runtime::Runtime,
    }
    impl RealJobs {
        fn new() -> Self {
            Self {
                service: OptimizeJobService::new(),
                runtime: tokio::runtime::Runtime::new().unwrap(),
            }
        }
        fn claim(&self, table: &str, receipt: Option<&SubmissionReceipt>) -> OptimizeJob {
            let submission = self
                .runtime
                .block_on(
                    self.service
                        .submit_optimize(target(table), &Capture { receipt }),
                )
                .unwrap();
            let job = self
                .runtime
                .block_on(self.service.runtime().claim_next(1))
                .unwrap()
                .unwrap();
            assert_eq!(job.job_id, submission.handle().unwrap().job_id());
            job
        }
    }
    fn pending(fixture: &Fixture, jobs: &RealJobs) -> (SubmissionReceipt, OptimizeJob) {
        let receipt = fixture
            .controls
            .begin_submission(&fixture.nomination, &target("orders"))
            .unwrap()
            .unwrap();
        let job = jobs.claim("orders", Some(&receipt));
        (receipt, job)
    }
    #[test]
    fn constructor_rejects_mixed_unknown_and_observation_without_hold() {
        assert!(
            MaintenanceTestControls::from_grants(vec![])
                .unwrap()
                .is_none()
        );
        let first = TestSession::new(10_000, &[HOLD, OBSERVE]);
        let second = TestSession::new(10_000, &[HOLD, OBSERVE]);
        let mut mixed = first.grants();
        mixed.extend(second.grants());
        let mixed_error = MaintenanceTestControls::from_grants(mixed).err().unwrap();
        assert!(
            mixed_error.contains("foreign domains or launches"),
            "{mixed_error}"
        );
        let unknown = TestSession::new(10_000, &["not-a-maintenance-action"]);
        let unknown_error = MaintenanceTestControls::from_grants(unknown.grants())
            .err()
            .unwrap();
        assert!(
            unknown_error.contains("unsupported maintenance action"),
            "{unknown_error}"
        );
        let observer = TestSession::new(10_000, &[OBSERVE]);
        let observer_error = MaintenanceTestControls::from_grants(observer.grants())
            .err()
            .unwrap();
        assert!(
            observer_error.contains("lacks its hold grant"),
            "{observer_error}"
        );
        assert!(
            !observer
                .root
                .path()
                .join("selected-statement.before-rebind.ready")
                .exists()
        );
    }
    #[test]
    fn finite_input_pin_refuses_client_sql_and_launch_replacement_before_any_job() {
        let mut session = TestSession::with_tokens(
            10_000,
            &[HOLD, OBSERVE],
            &["selected-statement", "fresh-statement"],
        );
        let controls = MaintenanceTestControls::from_grants(session.grants())
            .unwrap()
            .unwrap();
        let original = session.input();
        assert!(
            controls
                .nominate_statement(&original, &session.nomination)
                .is_err()
        );
        let mut foreign_session = original.clone();
        foreign_session.target_session_id = "foreign-session".into();
        let mut foreign_process = original.clone();
        foreign_process.frontend_process_id =
            novarocks_types::FrontendProcessId::new_v7().to_string();
        for foreign in [foreign_session, foreign_process] {
            assert!(controls.pin_statement_input(&foreign).is_err());
        }
        assert!(
            controls
                .slots
                .iter()
                .all(|slot| slot.state.lock().unwrap().input.is_none())
        );
        controls.pin_statement_input(&original).unwrap();
        controls.pin_statement_input(&original).unwrap();
        let mut client = original.clone();
        client.session_key += 1;
        let mut connection = original.clone();
        connection.connection_id += 1;
        let mut sql = original.clone();
        sql.sql_sha256[0] ^= 0xff;
        let mut launch = original.clone();
        launch.target_launch_id = "foreign-launch".into();
        for replacement in [client, connection, sql, launch] {
            assert!(controls.pin_statement_input(&replacement).is_err());
            assert!(
                controls
                    .nominate_statement(&replacement, &session.nomination)
                    .is_err()
            );
        }
        let selected = controls
            .slots
            .iter()
            .find(|slot| slot.parameters.statement_run_token == "selected-statement")
            .unwrap();
        assert_eq!(
            selected.state.lock().unwrap().input.as_ref(),
            Some(&original)
        );
        assert!(selected.state.lock().unwrap().nomination.is_none());
        let mut unknown = original.clone();
        unknown.statement_run_token = "not-declared".into();
        controls.pin_statement_input(&unknown).unwrap();
        assert_eq!(controls.slots.len(), 2);
        let mut fresh = original.clone();
        fresh.statement_run_token = "fresh-statement".into();
        fresh.session_key += 1;
        let mut fresh_nom = session.nomination.clone();
        fresh_nom.statement_run_token = fresh.statement_run_token.clone();
        fresh_nom.session_key = fresh.session_key;
        controls.pin_statement_input(&fresh).unwrap();
        controls.nominate_statement(&fresh, &fresh_nom).unwrap();
        controls
            .nominate_statement(&original, &session.nomination)
            .unwrap();
        assert!(session.role.scope().observations().is_empty());
        assert!(
            !session
                .root
                .path()
                .join("selected-statement.before-rebind.ready")
                .exists()
        );
        session.role.close_and_join().unwrap();
        assert!(controls.pin_statement_input(&original).is_err());
        assert!(
            controls
                .nominate_statement(&original, &session.nomination)
                .is_err()
        );
    }

    #[test]
    fn closed_maintenance_owner_cannot_repin_an_unsubmitted_input() {
        let session = TestSession::new(10_000, &[HOLD, OBSERVE]);
        let controls = MaintenanceTestControls::from_grants(session.grants())
            .unwrap()
            .unwrap();
        controls.pin_statement_input(&session.input()).unwrap();
        controls.close().unwrap();
        assert!(session.role.scope().is_active());
        assert!(controls.pin_statement_input(&session.input()).is_err());
        assert!(
            controls
                .nominate_statement(&session.input(), &session.nomination)
                .is_err()
        );
        assert!(session.role.scope().observations().is_empty());
    }

    #[test]
    fn abandoned_submission_and_new_generation_cannot_redirect_unhit_permission() {
        let fixture = Fixture::new();
        let receipt = fixture
            .controls
            .begin_submission(&fixture.nomination, &target("orders"))
            .unwrap()
            .unwrap();
        receipt.capture(&[1, 2, 3]).unwrap();
        drop(receipt);
        let mut later = fixture.nomination.clone();
        later.statement_generation += 1;
        assert!(
            fixture
                .controls
                .nominate_statement(&fixture.input(), &later)
                .is_err()
        );
        assert!(
            fixture
                .controls
                .begin_submission(&fixture.nomination, &target("orders"))
                .is_err()
        );
        assert!(!fixture.ready().exists());
    }
    #[test]
    fn pending_receipt_binds_real_job_scope_and_closed_owner_refuses_dispatch() {
        let fixture = Fixture::new();
        let admitted = Admitted::new();
        let jobs = RealJobs::new();
        let (receipt, job) = pending(&fixture, &jobs);
        let other = jobs.claim("other", None);
        assert!(
            drive(fixture.controls.before_rebind(&other, admitted.capacity()))
                .unwrap()
                .is_none()
        );
        std::thread::scope(|threads| {
            let controls = &fixture.controls;
            let selected_job = &job;
            let capacity = admitted.capacity();
            let worker =
                threads.spawn(move || drive(controls.before_rebind(selected_job, capacity)));
            receipt.bind(job.job_id).unwrap();
            fixture.wait_until_held();
            fs::write(fixture.resume(), "resume\n").unwrap();
            let mut authorized = worker.join().unwrap().unwrap().unwrap();
            let foreign = Admitted::new();
            assert_eq!(
                foreign.capacity().scope().id(),
                admitted.capacity().scope().id()
            );
            assert!(
                authorized
                    .record_dispatch(&job, foreign.capacity())
                    .is_err()
            );
            authorized
                .record_dispatch(&job, admitted.capacity())
                .unwrap();
            assert_eq!(fs::read_to_string(fixture.count()).unwrap(), "1\n");
            fixture.controls.close().unwrap();
            assert!(
                fixture
                    .controls
                    .nominate_statement(&fixture.input(), &fixture.nomination)
                    .is_err()
            );
            assert!(
                fixture
                    .controls
                    .begin_submission(&fixture.nomination, &target("orders"))
                    .is_err()
            );
            assert!(
                authorized
                    .record_dispatch(&job, admitted.capacity())
                    .is_err()
            );
            assert_eq!(fs::read_to_string(fixture.count()).unwrap(), "1\n");
        });
    }
    #[test]
    fn one_async_poll_owner_can_bind_receipt_and_resume_its_original_hold() {
        let fixture = Fixture::new();
        let admitted = Admitted::new();
        let jobs = RealJobs::new();
        let (receipt, job) = pending(&fixture, &jobs);
        drive(async {
            let hold = fixture.controls.before_rebind(&job, admitted.capacity());
            tokio::pin!(hold);
            assert!(futures::poll!(hold.as_mut()).is_pending());
            assert!(!fixture.ready().exists());
            assert!(!fixture.count().exists());
            let resume = async {
                receipt.bind(job.job_id).unwrap();
                let failure_watchdog =
                    (Instant::now() + Duration::from_secs(2)).min(fixture.role.scope().deadline());
                while !fixture.ready().exists() {
                    assert!(
                        Instant::now() < failure_watchdog,
                        "actual hold was not reached"
                    );
                    tokio::task::yield_now().await;
                }
                assert_eq!(fs::read_to_string(fixture.count()).unwrap(), "0\n");
                fs::write(fixture.resume(), "resume\n").unwrap();
            };
            let (authorized, ()) = tokio::join!(hold, resume);
            let mut authorized = authorized.unwrap().unwrap();
            authorized
                .record_dispatch(&job, admitted.capacity())
                .unwrap();
            assert_eq!(fs::read_to_string(fixture.count()).unwrap(), "1\n");
        });
    }
    #[test]
    fn session_close_ends_actual_hold_and_joins_original_worker() {
        let fixture = Fixture::new();
        let admitted = Admitted::new();
        let jobs = RealJobs::new();
        let (receipt, job) = pending(&fixture, &jobs);
        receipt.bind(job.job_id).unwrap();
        std::thread::scope(|threads| {
            let controls = &fixture.controls;
            let selected_job = &job;
            let capacity = admitted.capacity();
            let worker =
                threads.spawn(move || drive(controls.before_rebind(selected_job, capacity)));
            fixture.wait_until_held();
            fixture.role.close();
            let error = worker
                .join()
                .unwrap()
                .err()
                .expect("session close ended actual hold");
            assert!(error.contains("session closed"), "{error}");
            assert!(!fixture.resume().exists());
            assert_eq!(fs::read_to_string(fixture.count()).unwrap(), "0\n");
        });
    }
    #[test]
    fn real_work_cancellation_ends_actual_hold_and_joins_original_worker() {
        let fixture = Fixture::new();
        let admitted = Admitted::new();
        let jobs = RealJobs::new();
        let (receipt, job) = pending(&fixture, &jobs);
        receipt.bind(job.job_id).unwrap();
        std::thread::scope(|threads| {
            let controls = &fixture.controls;
            let selected_job = &job;
            let capacity = admitted.capacity();
            let worker =
                threads.spawn(move || drive(controls.before_rebind(selected_job, capacity)));
            fixture.wait_until_held();
            admitted.cancel();
            let error = worker
                .join()
                .unwrap()
                .err()
                .expect("real work cancellation ended actual hold");
            assert!(error.to_lowercase().contains("cancel"), "{error}");
            assert!(fixture.role.scope().is_active());
            assert!(!fixture.resume().exists());
            assert_eq!(fs::read_to_string(fixture.count()).unwrap(), "0\n");
        });
    }
    #[test]
    fn receipt_wait_expires_at_original_session_deadline_without_entering_hold() {
        let fixture = Fixture::with_lifetime(3_000);
        let original_deadline = fixture.role.scope().deadline();
        let admitted = Admitted::new();
        let jobs = RealJobs::new();
        let (_unbound_receipt, job) = pending(&fixture, &jobs);
        std::thread::scope(|threads| {
            let controls = &fixture.controls;
            let selected_job = &job;
            let capacity = admitted.capacity();
            let worker =
                threads.spawn(move || drive(controls.before_rebind(selected_job, capacity)));
            let error = worker
                .join()
                .unwrap()
                .err()
                .expect("original deadline ended receipt wait");
            assert!(Instant::now() >= original_deadline);
            assert!(
                error.contains("timed out") || error.contains("session closed"),
                "{error}"
            );
            assert!(!fixture.ready().exists());
            assert!(!fixture.count().exists());
            assert!(
                fixture.controls.slots[0]
                    .state
                    .lock()
                    .unwrap()
                    .work_id
                    .is_none()
            );
        });
    }
}
