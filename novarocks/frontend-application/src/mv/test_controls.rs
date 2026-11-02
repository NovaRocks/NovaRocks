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

//! MV-owned finite test actions, isolated from ordinary product builds.

use novarocks_failpoint::scope::{ActionClaim, GrantHandle, ProcessBinding};
use novarocks_failpoint::target_binding::{StatementInputNomination, StatementWorkNomination};
use novarocks_mv_application::state_family::MV_ACCELERATOR_STATE_FAMILY;
use novarocks_parser::ast::{Query, Statement};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const DOMAIN: &str = "mv";

#[derive(Debug, Eq, PartialEq)]
struct Target {
    catalog: String,
    namespace: String,
    mv: String,
}

#[derive(Debug, Eq, PartialEq)]
enum Action {
    Wipe,
    DocumentExpiry {
        target: Target,
        token: String,
        directory: Option<PathBuf>,
    },
    Observe {
        target: Target,
        level: String,
    },
    Recovery {
        target: Target,
        phase: String,
        directory: PathBuf,
        token: String,
    },
    KnownCommitted {
        target: Target,
        directory: PathBuf,
    },
    Scheduler {
        target: Target,
        directory: PathBuf,
    },
    SchedulerOutcome {
        target: Target,
        directory: PathBuf,
    },
    TransientPreparation {
        target: Target,
        directory: PathBuf,
    },
    Rewrite {
        catalog: String,
        namespace: String,
        query: Box<Query>,
        statement_run_token: String,
        directory: PathBuf,
    },
}

// The original finite claim stays owned across either release wait.
struct AuthorizedBarrier {
    claim: ActionClaim,
    trigger: PathBuf,
}

struct AuthorizedAction {
    grant: GrantHandle,
    action: Action,
    frozen_input: Mutex<Option<StatementInputNomination>>,
    frozen_nomination: Mutex<Option<StatementWorkNomination>>,
    scheduler_outcome: Mutex<SchedulerOutcomeState>,
}

// This is a finite request-outcome observation, not a publication or Task attempt.
#[derive(Clone)]
struct SchedulerRequestIdentity {
    mv_id: i64,
    source: novarocks_mv_application::persistence::definition::MvAcceleratorSourceRevision,
}
impl SchedulerRequestIdentity {
    fn same_binding(&self, other: &Self) -> bool {
        self.mv_id == other.mv_id
            && self.source.target == other.source.target
            && self.source.target_object_id == other.source.target_object_id
            && self.source.definition_revision == other.source.definition_revision
            && self.source.interpretation_revision == other.source.interpretation_revision
            && self.source.configuration_revision == other.source.configuration_revision
            && self.source.deployment_owner == other.source.deployment_owner
            && self.source.process_incarnation == other.source.process_incarnation
    }
    fn document(&self) -> serde_json::Value {
        let source = &self.source;
        serde_json::json!({
            "mv_id": self.mv_id,
            "target": format!("{:?}", source.target),
            "target_object": format!("{:?}", source.target_object_id),
            "definition_revision": source.definition_revision.as_bytes(),
            "interpretation_revision": source.interpretation_revision.as_bytes(),
            "configuration_revision": source.configuration_revision.as_bytes(),
            "publication_revision": source.publication_revision.as_ref().map(|p| p.as_bytes()),
            "metadata_version": format!("{:?}", source.metadata_version),
            "publication_output_version": format!("{:?}", source.publication_output_version),
            "deployment_owner": format!("{:?}", source.deployment_owner),
            "process_incarnation": format!("{:?}", source.process_incarnation),
        })
    }
}
#[derive(Default)]
struct SchedulerOutcomeState {
    first: Option<SchedulerRequestIdentity>,
    next_sequence: u64,
    published: bool,
}
/// Created only on the actual admitted scheduler branch and returned to that
/// same local call after complete. The sequence is observation-local only.
pub(crate) struct SchedulerOutcomeRequest {
    action_index: usize,
    request: SchedulerRequestIdentity,
    sequence: u64,
    process: ProcessBinding,
    session_id: String,
    launch_id: String,
    grant_id: String,
}

const SCHEDULER_OUTCOME_MAX_BYTES: usize = 64 * 1024;

struct SchedulerDocumentWriter(Vec<u8>);
impl std::io::Write for SchedulerDocumentWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > SCHEDULER_OUTCOME_MAX_BYTES.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other(
                "finite scheduler observation exceeds its byte bound",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn publish_scheduler_document(
    directory: &Path,
    name: &str,
    value: &serde_json::Value,
) -> Result<(), String> {
    use std::io::Write;
    let mut encoded = SchedulerDocumentWriter(Vec::new());
    serde_json::to_writer(&mut encoded, value)
        .map_err(|e| format!("encode bounded MV observation: {e}"))?;
    std::fs::create_dir_all(directory)
        .map_err(|e| format!("create MV observation directory: {e}"))?;
    let pending = directory.join(format!(".{name}.pending"));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pending)
        .map_err(|e| format!("create finite MV observation: {e}"))?;
    file.write_all(&encoded.0)
        .map_err(|e| format!("write finite MV observation: {e}"))?;
    file.sync_all()
        .map_err(|e| format!("flush finite MV observation: {e}"))?;
    // Publish a complete inode without replacing an existing immutable slot.
    std::fs::hard_link(&pending, directory.join(name))
        .map_err(|e| format!("publish finite MV observation: {e}"))?;
    std::fs::remove_file(&pending).map_err(|e| format!("remove MV observation staging name: {e}"))
}

pub struct MvRewriteStatementTarget {
    actual_statement: novarocks_query_application::session_control::StatementToken,
    nominated_statement: novarocks_query_application::session_control::StatementToken,
    run_token: String,
}

impl MvRewriteStatementTarget {
    pub(crate) fn new(
        actual_statement: novarocks_query_application::session_control::StatementToken,
        nominated_statement: novarocks_query_application::session_control::StatementToken,
        run_token: String,
    ) -> Result<Self, String> {
        if actual_statement != nominated_statement || run_token.is_empty() {
            return Err(
                "MV rewrite nomination does not match the actual statement generation".into(),
            );
        }
        Ok(Self {
            actual_statement,
            nominated_statement,
            run_token,
        })
    }
}

/// One finite expiry attempt. Its optional barrier location comes only from
/// the launch grant; SQL arguments cannot nominate a filesystem location.
pub(crate) struct DocumentExpiryClaim {
    claim: ActionClaim,
    token: String,
    directory: Option<PathBuf>,
}
impl DocumentExpiryClaim {
    /// Called only after the exact full-load remove set has been planned.
    pub(crate) fn after_planning(
        &mut self,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<(), String> {
        let trigger = self
            .directory
            .as_ref()
            .map(|directory| directory.join("mv-document-expiry-planned.trigger"));
        if let Some(trigger) = trigger.as_ref().filter(|trigger| trigger.exists()) {
            let contents = std::fs::read_to_string(trigger)
                .map_err(|error| format!("read authorized MV document expiry trigger: {error}"))?;
            if contents != format!("token={}\n", self.token) {
                return Err("MV document expiry trigger does not match its finite grant".into());
            }
        }
        self.ensure_active(context)?;
        self.claim.record_hit().map_err(|error| error.to_string())?;
        eprintln!("NOVAROCKS_MV_DOCUMENT_EXPIRY_PLANNED token={}", self.token);
        if let Some(trigger) = trigger {
            wait_release(&self.claim, &trigger, Duration::from_secs(30), || {
                context.is_cancelled() || Instant::now() >= context.deadline()
            })?;
        }
        self.ensure_active(context)
    }
    fn ensure_active(
        &self,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<(), String> {
        if !self.claim.cancellation().is_active() {
            return Err("MV document expiry authorization is closed".into());
        }
        if context.is_cancelled() || Instant::now() >= context.deadline() {
            return Err("MV document expiry request was cancelled or its deadline elapsed".into());
        }
        Ok(())
    }
}

pub struct MvTestControls {
    process: ProcessBinding,
    actions: Vec<AuthorizedAction>,
}

impl MvTestControls {
    pub fn from_grants(grants: Vec<GrantHandle>) -> Result<Self, String> {
        let process = grants
            .first()
            .ok_or("MV controls require at least one grant")?
            .cancellation()
            .process();
        if !matches!(process, ProcessBinding::Frontend(_)) {
            return Err("MV controls require the exact FE launch".into());
        }
        let session_id = grants[0].cancellation().session_id().to_string();
        let launch_id = grants[0].cancellation().launch_id().to_string();
        let mut actions: Vec<AuthorizedAction> = Vec::new();
        for grant in grants {
            if grant.domain() != DOMAIN
                || grant.cancellation().process() != process
                || grant.cancellation().session_id() != session_id
                || grant.cancellation().launch_id() != launch_id
            {
                return Err("MV controls cannot combine FE launches".into());
            }
            let action = parse_action(grant.parameters())?;
            if actions.iter().any(|old| {
                same_selection(&old.action, &action) && !matches!(action, Action::Observe { .. })
            }) {
                return Err("duplicate or conflicting MV control action".into());
            }
            if matches!(action, Action::Wipe)
                && actions.iter().any(|old| matches!(old.action, Action::Wipe))
            {
                return Err("MV family wipe permits only one claim per FE launch".into());
            }
            actions.push(AuthorizedAction {
                grant,
                action,
                frozen_input: Mutex::new(None),
                frozen_nomination: Mutex::new(None),
                scheduler_outcome: Mutex::new(SchedulerOutcomeState::default()),
            });
        }
        Ok(Self { process, actions })
    }

    /// Pin only predeclared Rewrite inputs. Journal reuse cannot replace the
    /// original client or SQL; selection neither claims nor emits an action.
    pub fn pin_statement_input(&self, input: &StatementInputNomination) -> Result<(), String> {
        input.validate().map_err(|error| error.to_string())?;
        let mut pins = Vec::new();
        for action in &self.actions {
            let Action::Rewrite {
                statement_run_token,
                ..
            } = &action.action
            else {
                continue;
            };
            if statement_run_token != &input.statement_run_token {
                continue;
            }
            self.validate_rewrite_input(action, input)?;
            let frozen = action
                .frozen_input
                .lock()
                .map_err(|_| "MV input pin lock failed")?;
            if frozen.as_ref().is_some_and(|original| original != input) {
                return Err("MV rewrite statement input replacement refused".into());
            }
            pins.push(frozen);
        }
        // All matching, declaration-bounded slots are checked before any pin.
        for mut frozen in pins {
            if frozen.is_none() {
                *frozen = Some(input.clone());
            }
        }
        Ok(())
    }

    fn validate_rewrite_input(
        &self,
        action: &AuthorizedAction,
        input: &StatementInputNomination,
    ) -> Result<(), String> {
        let cancellation = action.grant.cancellation();
        if !cancellation.is_active()
            || input.target_session_id != cancellation.session_id()
            || input.target_launch_id != cancellation.launch_id()
            || ProcessBinding::Frontend(
                input
                    .frontend_process()
                    .map_err(|error| error.to_string())?,
            ) != self.process
        {
            return Err("MV statement input belongs to a foreign or closed FE launch".into());
        }
        Ok(())
    }

    /// Extend the immutable input with the real statement generation. No
    /// missing pin may be implicitly supplied by a later nominal call.
    pub fn nominate_statement(
        &self,
        input: &StatementInputNomination,
        nomination: &StatementWorkNomination,
    ) -> Result<(), String> {
        input
            .validate_statement(nomination)
            .map_err(|error| error.to_string())?;
        let mut selected = Vec::new();
        for action in &self.actions {
            let Action::Rewrite {
                statement_run_token,
                ..
            } = &action.action
            else {
                continue;
            };
            if statement_run_token != &input.statement_run_token {
                continue;
            }
            self.validate_rewrite_input(action, input)?;
            let pinned = action
                .frozen_input
                .lock()
                .map_err(|_| "MV input pin lock failed")?;
            match pinned.as_ref() {
                None => return Err("MV rewrite statement input was not pinned".into()),
                Some(original) if original != input => {
                    return Err("MV rewrite statement input replacement refused".into());
                }
                Some(_) => {}
            }
            let frozen = action
                .frozen_nomination
                .lock()
                .map_err(|_| "MV nomination lock failed")?;
            if frozen
                .as_ref()
                .is_some_and(|original| original != nomination)
            {
                return Err("MV rewrite statement generation replacement refused".into());
            }
            selected.push((pinned, frozen));
        }
        for (_pinned, mut frozen) in selected {
            if frozen.is_none() {
                *frozen = Some(nomination.clone());
            }
        }
        Ok(())
    }

    pub(crate) fn claim_call(
        &self,
        catalog: &str,
        namespace: &str,
        mv: &str,
        level: &str,
    ) -> Result<ActionClaim, String> {
        let selected = self.actions.iter().filter(|action| match &action.action {
            Action::Wipe => level == "wipe",
            Action::Observe {
                target,
                level: expected,
            } => level == expected && target.matches(catalog, namespace, mv),
            _ => false,
        });
        for action in selected {
            if let Ok(claim) = action.grant.claim_once(self.process) {
                return Ok(claim);
            }
        }
        Err(
            "MV test action has no active unclaimed authorization for this FE launch and target"
                .into(),
        )
    }

    pub(crate) fn claim_document_expiry(
        &self,
        catalog: &str,
        namespace: &str,
        mv: &str,
        token: &str,
    ) -> Result<DocumentExpiryClaim, String> {
        for action in &self.actions {
            let Action::DocumentExpiry {
                target,
                token: expected,
                directory,
            } = &action.action
            else {
                continue;
            };
            if expected != token || !target.matches(catalog, namespace, mv) {
                continue;
            }
            if let Ok(claim) = action.grant.claim_once(self.process) {
                return Ok(DocumentExpiryClaim {
                    claim,
                    token: token.into(),
                    directory: directory.clone(),
                });
            }
        }
        Err("MV document expiry has no active unclaimed authorization for this FE launch, target and token".into())
    }

    pub(crate) async fn recovery_async(
        &self,
        catalog: &str,
        namespace: &str,
        mv: &str,
        phase: &str,
    ) -> Result<(), String> {
        for action in &self.actions {
            if let Some(barrier) =
                self.claim_recovery_barrier(action, catalog, namespace, mv, phase)?
            {
                wait_release_async(
                    &barrier.claim,
                    &barrier.trigger,
                    Duration::from_secs(30),
                    || false,
                )
                .await?;
            }
        }
        Ok(())
    }

    fn claim_recovery_barrier(
        &self,
        action: &AuthorizedAction,
        catalog: &str,
        namespace: &str,
        mv: &str,
        phase: &str,
    ) -> Result<Option<AuthorizedBarrier>, String> {
        let Action::Recovery {
            target,
            phase: expected,
            directory,
            token,
        } = &action.action
        else {
            return Ok(None);
        };
        if expected != phase || !target.matches(catalog, namespace, mv) {
            return Ok(None);
        }
        let trigger = directory.join(format!("mv-refresh-at-{phase}.trigger"));
        if !trigger.exists() {
            return Ok(None);
        }
        let contents = std::fs::read_to_string(&trigger)
            .map_err(|error| format!("read authorized MV recovery trigger: {error}"))?;
        if contents != format!("token={token}\n") {
            return Err("MV recovery trigger does not match its finite grant".into());
        }
        let mut claim = match action.grant.claim_once(self.process) {
            Ok(claim) => claim,
            Err(_) => return Ok(None),
        };
        claim.record_hit().map_err(|error| error.to_string())?;
        eprintln!("NOVAROCKS_MV_RECOVERY_PHASE phase={phase} token={token}");
        Ok(Some(AuthorizedBarrier { claim, trigger }))
    }

    pub(crate) async fn known_committed_async(
        &self,
        catalog: &str,
        namespace: &str,
        mv: &str,
        publication_id: &novarocks_spi::connector::LakePublicationId,
    ) -> Result<(), String> {
        for action in &self.actions {
            if let Some(barrier) =
                self.claim_known_committed_barrier(action, catalog, namespace, mv, publication_id)?
            {
                wait_release_async(
                    &barrier.claim,
                    &barrier.trigger,
                    Duration::from_secs(30),
                    || false,
                )
                .await?;
            }
        }
        Ok(())
    }

    fn claim_known_committed_barrier(
        &self,
        action: &AuthorizedAction,
        catalog: &str,
        namespace: &str,
        mv: &str,
        publication_id: &novarocks_spi::connector::LakePublicationId,
    ) -> Result<Option<AuthorizedBarrier>, String> {
        let Action::KnownCommitted { target, directory } = &action.action else {
            return Ok(None);
        };
        if !target.matches(catalog, namespace, mv) {
            return Ok(None);
        }
        let trigger =
            novarocks_failpoint::mv_known_committed_before_projector_cas_trigger_path(directory);
        if !trigger.exists() {
            return Ok(None);
        }
        let mut claim = match action.grant.claim_once(self.process) {
            Ok(claim) => claim,
            Err(_) => return Ok(None),
        };
        let marker =
            novarocks_failpoint::mv_known_committed_before_projector_cas_marker_path(directory);
        std::fs::write(
            marker,
            format!(
                "publication_id={}\nphase=known-committed-before-projector-cas\n",
                publication_id.as_uuid()
            ),
        )
        .map_err(|error| format!("write authorized MV projector marker: {error}"))?;
        claim.record_hit().map_err(|error| error.to_string())?;
        eprintln!(
            "NOVAROCKS_MV_PROJECTOR_PHASE publication_id={} phase=known-committed-before-projector-cas action=kill_fe",
            publication_id.as_uuid()
        );
        Ok(Some(AuthorizedBarrier { claim, trigger }))
    }

    pub(crate) async fn scheduler_async(
        &self,
        target: &novarocks_sql::planning::mv::SqlMvTarget,
        cancellation: &novarocks_query_application::cancellation::QueryCancellationView,
    ) -> Result<bool, String> {
        if let Some(barrier) = self.claim_scheduler_barrier(target)? {
            wait_release_async(
                &barrier.claim,
                &barrier.trigger,
                Duration::from_secs(120),
                || cancellation.is_cancelled(),
            )
            .await?;
        }
        Ok(cancellation.is_cancelled())
    }
    fn claim_scheduler_barrier(
        &self,
        target: &novarocks_sql::planning::mv::SqlMvTarget,
    ) -> Result<Option<AuthorizedBarrier>, String> {
        for action in &self.actions {
            let Action::Scheduler {
                target: expected,
                directory,
            } = &action.action
            else {
                continue;
            };
            if !expected.matches(
                target.catalog.as_deref().unwrap_or(""),
                &target.database,
                &target.name,
            ) {
                continue;
            }
            let mut claim = match action.grant.claim_once(self.process) {
                Ok(claim) => claim,
                Err(_) => continue,
            };
            std::fs::write(
                directory.join(format!("mvx4-scheduler-admitted-{}.marker", target.name)),
                "admitted\n",
            )
            .map_err(|error| format!("write authorized MV scheduler marker: {error}"))?;
            claim.record_hit().map_err(|error| error.to_string())?;
            return Ok(Some(AuthorizedBarrier {
                claim,
                trigger: directory.join("mvx4-scheduler-hold.trigger"),
            }));
        }
        Ok(None)
    }

    /// Pin the first real admitted object and D/L/C. P and provider versions
    /// may advance through successful refreshes; each request retains its own
    /// complete immutable source revision until that same call completes.
    pub(crate) fn scheduler_request_admitted(
        &self,
        target: &novarocks_sql::planning::mv::SqlMvTarget,
        mv_id: i64,
        source: &novarocks_mv_application::persistence::definition::MvAcceleratorSourceRevision,
    ) -> Result<Option<SchedulerOutcomeRequest>, String> {
        for (action_index, action) in self.actions.iter().enumerate() {
            let Action::SchedulerOutcome {
                target: expected,
                directory,
            } = &action.action
            else {
                continue;
            };
            if !expected.matches(
                target.catalog.as_deref().unwrap_or(""),
                &target.database,
                &target.name,
            ) {
                continue;
            }
            if !action.grant.cancellation().is_active() {
                return Err("MV scheduler observation authorization is closed".into());
            }
            let mut state = action
                .scheduler_outcome
                .lock()
                .map_err(|_| "MV scheduler observation lock poisoned")?;
            if state.published {
                return Ok(None);
            }
            let request = SchedulerRequestIdentity {
                mv_id,
                source: source.clone(),
            };
            if let Some(first) = &state.first {
                if !first.same_binding(&request) {
                    publish_scheduler_document(
                        directory,
                        "scheduler-outcome-error.json",
                        &serde_json::json!({
                            "schema_version":1,"status":"UNKNOWN","reason":"scheduler target binding changed",
                            "grant_id":action.grant.grant_id(),"first_admitted_source":first.document(),"rejected_source":request.document(),
                        }),
                    )?;
                    state.published = true;
                    return Err("MV scheduler observation cannot rebind its frozen target".into());
                }
            } else {
                if directory.join("scheduler-outcome.json").exists()
                    || directory.join("scheduler-outcome-error.json").exists()
                {
                    return Err("MV scheduler observation slot is not fresh".into());
                }
                state.first = Some(request.clone());
            }
            state.next_sequence = state
                .next_sequence
                .checked_add(1)
                .ok_or("MV scheduler observation sequence exhausted")?;
            return Ok(Some(SchedulerOutcomeRequest {
                action_index,
                request,
                sequence: state.next_sequence,
                process: self.process,
                session_id: action.grant.cancellation().session_id().to_owned(),
                launch_id: action.grant.cancellation().launch_id().to_owned(),
                grant_id: action.grant.grant_id().to_owned(),
            }));
        }
        Ok(None)
    }

    /// Observe only the decision returned by the actual scheduler.complete.
    /// Success, transient backoff, and NoChange do not claim or consume the slot.
    pub(crate) fn scheduler_request_completed(
        &self,
        admitted: SchedulerOutcomeRequest,
        decision: &novarocks_mv_application::scheduler_runtime::MvRefreshRuntimeDecision,
    ) -> Result<(), String> {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_query_application::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite,
        };
        use novarocks_type_contract::CompileControlError;
        use prost::Message;
        if !matches!(
            decision,
            novarocks_mv_application::scheduler_runtime::MvRefreshRuntimeDecision::Blocked { .. }
        ) {
            return Ok(());
        }
        let action = self
            .actions
            .get(admitted.action_index)
            .ok_or("MV scheduler observation has no grant")?;
        let Action::SchedulerOutcome { target, directory } = &action.action else {
            return Err("MV scheduler observation grant mismatch".into());
        };
        let scope = action.grant.cancellation();
        if admitted.process != self.process
            || admitted.session_id != scope.session_id()
            || admitted.launch_id != scope.launch_id()
            || admitted.grant_id != action.grant.grant_id()
        {
            return Err("MV scheduler observation belongs to another exact launch".into());
        }
        if !action.grant.cancellation().is_active() {
            return Err("MV scheduler observation authorization is closed".into());
        }
        let mut state = action
            .scheduler_outcome
            .lock()
            .map_err(|_| "MV scheduler observation lock poisoned")?;
        if state.published {
            return Ok(());
        }
        let first = state
            .first
            .as_ref()
            .ok_or("MV scheduler outcome has no actual admission")?;
        if !first.same_binding(&admitted.request) {
            return Err("MV scheduler outcome changed its admitted target".into());
        }
        let mut claim = action
            .grant
            .claim_once(self.process)
            .map_err(|e| e.to_string())?;
        // The Source identity is the original typed failure origin. Native
        // execution facts remain distinct from compiler-control Source errors.
        let original_fact = decision
            .execution_failure()
            .map(|input| input.fact())
            .or_else(|| match decision.compile_control_error() {
                Some(CompileControlError::Source(source)) => Some(source.failure_fact()),
                _ => None,
            });
        let (fact, public_message) = match original_fact {
            Some(fact) => {
                // SafeDetail is bounded, not a redactor. Do not copy arbitrary
                // original execution/Source text into a new artifact.
                let input = ExecutionFailureInput::new(
                    fact,
                    novarocks_execution_contract::SafeDetail::truncating(
                        "MV scheduler request blocked; source diagnostic retained in FE role logs",
                    ),
                    ExecutionFailureSite::Coordinator,
                );
                (
                    Some(novarocks_task_codec::status::encode_failure_fact(fact).encode_to_vec()),
                    Some(input.user_error().to_string()),
                )
            }
            None => (None, None),
        };
        let cancellation = action.grant.cancellation();
        let document = serde_json::json!({
            "schema_version":1,"semantic_scope":"scheduler-request-outcome","decision":"Blocked",
            "grant_id":action.grant.grant_id(),"session_id":cancellation.session_id(),"launch_id":cancellation.launch_id(),"process":self.process.process_id(),
            "target":{"catalog":target.catalog,"namespace":target.namespace,"mv":target.mv},
            "first_admitted_source":first.document(),"blocked_request_source":admitted.request.document(),
            "admission_observation_sequence":admitted.sequence,
            "failure_identity":if fact.is_some() { "Source" } else { "UNKNOWN" },
            "failure_fact":fact,"public_message":public_message,
            "message_scope":"safe canonical projection; not original Source text or client SQL response",
            "actual_publication_attempt":"NOT_OBSERVED","actual_task_attempt":"NOT_OBSERVED","actual_base_snapshots":"NOT_OBSERVED",
        });
        // Record the actual authorized event before publishing its payload.
        // An observation failure may not leave a successfully readable typed slot.
        claim.record_hit().map_err(|e| e.to_string())?;
        publish_scheduler_document(directory, "scheduler-outcome.json", &document)?;
        state.published = true;
        Ok(())
    }

    pub(crate) fn transient_preparation(
        &self,
        catalog: &str,
        namespace: &str,
        mv: &str,
    ) -> Result<bool, String> {
        for action in &self.actions {
            let Action::TransientPreparation { target, directory } = &action.action else {
                continue;
            };
            if !target.matches(catalog, namespace, mv) {
                continue;
            }
            let trigger =
                directory.join(format!("mvx4-scheduler-transient-preparation-{mv}.trigger"));
            if !trigger.exists() {
                continue;
            }
            let mut claim = match action.grant.claim_once(self.process) {
                Ok(claim) => claim,
                Err(_) => continue,
            };
            std::fs::rename(&trigger, trigger.with_extension("consumed"))
                .map_err(|error| format!("consume authorized MV transient fault: {error}"))?;
            claim.record_hit().map_err(|error| error.to_string())?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn rewrite(
        &self,
        catalog: Option<&str>,
        namespace: &str,
        query: &Query,
        selected: bool,
        statement_target: Option<&MvRewriteStatementTarget>,
        cancellation: &novarocks_query_application::cancellation::QueryCancellationView,
    ) -> Result<(), String> {
        if !selected {
            return Ok(());
        }
        let Some(statement_target) = statement_target else {
            return Ok(());
        };
        if statement_target.actual_statement != statement_target.nominated_statement {
            return Err("MV rewrite nomination is stale".into());
        }
        for action in &self.actions {
            let Action::Rewrite {
                catalog: expected_catalog,
                namespace: expected_namespace,
                query: expected_query,
                statement_run_token,
                directory,
            } = &action.action
            else {
                continue;
            };
            if catalog != Some(expected_catalog.as_str())
                || namespace != expected_namespace
                || query != expected_query.as_ref()
                || &statement_target.run_token != statement_run_token
            {
                continue;
            }
            let frozen = action
                .frozen_nomination
                .lock()
                .map_err(|_| "MV nomination lock failed")?;
            let original = frozen
                .as_ref()
                .ok_or("MV rewrite has no frozen statement nomination")?;
            let actual = statement_target.actual_statement;
            if original.statement_run_token != statement_target.run_token
                || original.session_key != actual.session().key().get()
                || original.statement_generation != actual.generation()
            {
                return Err("MV rewrite statement differs from its frozen original target".into());
            }
            drop(frozen);
            let hold = directory.join("mvx4-rewrite-hold.trigger");
            if !hold.exists() {
                continue;
            }
            let mut claim = match action.grant.claim_once(self.process) {
                Ok(claim) => claim,
                Err(_) => continue,
            };
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(directory.join("mvx4-completed-mv-target-frozen.marker"))
                .map_err(|error| format!("create authorized frozen MV marker: {error}"))?;
            claim.record_hit().map_err(|error| error.to_string())?;
            wait_release(&claim, &hold, Duration::from_secs(120), || {
                cancellation.is_cancelled()
            })?;
        }
        Ok(())
    }
}

fn same_selection(left: &Action, right: &Action) -> bool {
    match (left, right) {
        (Action::Wipe, Action::Wipe) => true,
        (
            Action::DocumentExpiry {
                target: a,
                token: x,
                ..
            },
            Action::DocumentExpiry {
                target: b,
                token: y,
                ..
            },
        ) => a == b && x == y,
        (
            Action::Observe {
                target: a,
                level: x,
            },
            Action::Observe {
                target: b,
                level: y,
            },
        ) => a == b && x == y,
        (
            Action::Recovery {
                target: a,
                phase: x,
                ..
            },
            Action::Recovery {
                target: b,
                phase: y,
                ..
            },
        ) => a == b && x == y,
        (Action::KnownCommitted { target: a, .. }, Action::KnownCommitted { target: b, .. })
        | (Action::Scheduler { target: a, .. }, Action::Scheduler { target: b, .. })
        | (
            Action::TransientPreparation { target: a, .. },
            Action::TransientPreparation { target: b, .. },
        ) => a == b,
        (
            Action::SchedulerOutcome {
                target: a,
                directory: x,
            },
            Action::SchedulerOutcome {
                target: b,
                directory: y,
            },
        ) => a == b || x == y,
        (
            Action::Rewrite {
                statement_run_token: a,
                ..
            },
            Action::Rewrite {
                statement_run_token: b,
                ..
            },
        ) => a == b,
        _ => false,
    }
}

impl Target {
    fn matches(&self, catalog: &str, namespace: &str, mv: &str) -> bool {
        self.catalog == catalog && self.namespace == namespace && self.mv == mv
    }
}

fn parse_action(value: &serde_json::Value) -> Result<Action, String> {
    let object = value
        .as_object()
        .ok_or("MV grant parameters must be an object")?;
    let text = |key: &str| {
        object
            .get(key)
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("MV grant requires {key}"))
    };
    let action = text("action")?;
    let allowed: &[&str] = match action {
        "accelerator-wipe" => &["action", "family"],
        "document-observation" => &["action", "catalog", "namespace", "mv", "level"],
        "document-expiry" => &["action", "catalog", "namespace", "mv", "token", "directory"],
        "recovery-hold" => &[
            "action",
            "catalog",
            "namespace",
            "mv",
            "phase",
            "directory",
            "token",
        ],
        "known-committed-hold"
        | "scheduler-hold"
        | "scheduler-outcome"
        | "transient-preparation" => &["action", "catalog", "namespace", "mv", "directory"],
        "rewrite-hold" => &[
            "action",
            "catalog",
            "namespace",
            "sql",
            "statement_run_token",
            "directory",
        ],
        _ => return Err("unknown MV control action".into()),
    };
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err("unknown MV control parameter".into());
    }
    if action == "accelerator-wipe" {
        if text("family")? != MV_ACCELERATOR_STATE_FAMILY.prefix() {
            return Err("MV wipe names another Accelerator family".into());
        }
        return Ok(Action::Wipe);
    }
    let directory = || -> Result<PathBuf, String> {
        let path = PathBuf::from(text("directory")?);
        if !path.is_absolute() {
            return Err("MV control directory must be absolute".into());
        }
        Ok(path)
    };
    if action == "rewrite-hold" {
        let statements = novarocks_parser::parse(text("sql")?)
            .map_err(|error| format!("parse exact MV rewrite target: {error}"))?;
        let [Statement::Query(query)] = statements.as_slice() else {
            return Err("MV rewrite target must be one query".into());
        };
        return Ok(Action::Rewrite {
            catalog: text("catalog")?.into(),
            namespace: text("namespace")?.into(),
            query: Box::new(query.clone()),
            statement_run_token: text("statement_run_token")?.into(),
            directory: directory()?,
        });
    }
    let target = Target {
        catalog: text("catalog")?.into(),
        namespace: text("namespace")?.into(),
        mv: text("mv")?.into(),
    };
    Ok(match action {
        "document-expiry" => {
            let token = text("token")?;
            if token.contains(['\n', '\r']) {
                return Err("MV document expiry token contains a line break".into());
            }
            Action::DocumentExpiry {
                target,
                token: token.into(),
                directory: if object.contains_key("directory") {
                    Some(directory()?)
                } else {
                    None
                },
            }
        }
        "document-observation" => {
            let level = text("level")?;
            if !matches!(level, "baseline" | "package" | "provenance") {
                return Err("MV observation grant has invalid level".into());
            }
            Action::Observe {
                target,
                level: level.into(),
            }
        }
        "recovery-hold" => {
            let phase = text("phase")?;
            if !matches!(
                phase,
                "source-pinned"
                    | "source-prepared"
                    | "data-prepared"
                    | "write-committed"
                    | "publication-committed"
            ) {
                return Err("MV recovery grant has invalid phase".into());
            }
            let token = text("token")?;
            if token.contains(['\n', '\r']) {
                return Err("MV recovery token contains a line break".into());
            }
            Action::Recovery {
                target,
                phase: phase.into(),
                directory: directory()?,
                token: token.into(),
            }
        }
        "known-committed-hold" => Action::KnownCommitted {
            target,
            directory: directory()?,
        },
        "scheduler-hold" => Action::Scheduler {
            target,
            directory: directory()?,
        },
        "scheduler-outcome" => Action::SchedulerOutcome {
            target,
            directory: directory()?,
        },
        "transient-preparation" => Action::TransientPreparation {
            target,
            directory: directory()?,
        },
        _ => unreachable!(),
    })
}

fn barrier_deadline(claim: &ActionClaim, phase_budget: Duration) -> Result<Instant, String> {
    Ok(Instant::now()
        .checked_add(phase_budget)
        .ok_or("MV barrier deadline overflow")?
        .min(claim.cancellation().deadline()))
}

fn check_barrier_wait(
    claim: &ActionClaim,
    deadline: Instant,
    cancelled: impl Fn() -> bool,
) -> Result<(), String> {
    if !claim.cancellation().is_active() {
        return Err("MV test session closed during authorized barrier".into());
    }
    if cancelled() {
        return Err("MV product request cancelled during authorized barrier".into());
    }
    if Instant::now() >= deadline {
        return Err("timed out waiting for authorized MV barrier release".into());
    }
    Ok(())
}

fn wait_release(
    claim: &ActionClaim,
    trigger: &Path,
    phase_budget: Duration,
    cancelled: impl Fn() -> bool,
) -> Result<(), String> {
    let deadline = barrier_deadline(claim, phase_budget)?;
    while trigger.exists() {
        check_barrier_wait(claim, deadline, &cancelled)?;
        std::thread::park_timeout(Duration::from_millis(10));
    }
    Ok(())
}

async fn wait_release_async(
    claim: &ActionClaim,
    trigger: &Path,
    phase_budget: Duration,
    cancelled: impl Fn() -> bool,
) -> Result<(), String> {
    let deadline = barrier_deadline(claim, phase_budget)?;
    let cancellation = claim.cancellation();
    while trigger.exists() {
        check_barrier_wait(claim, deadline, &cancelled)?;
        // This timer observes the existing filesystem test trigger, not a
        // product admission gate. The same grant can wake it immediately.
        let next_observation = (Instant::now() + Duration::from_millis(10)).min(deadline);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {},
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_observation)) => {},
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
pub(crate) mod tests {
    use super::*;
    use novarocks_failpoint::liveness::{PreparedRoleSession, RoleSessionOwner, RunnerSession};
    use novarocks_failpoint::session::{DomainGrant, SessionDeclaration, SessionRole};
    use novarocks_query_application::session_control::{SessionToken, StatementToken};
    use serde_json::{Value, json};

    pub(crate) struct TestSession {
        root: PathBuf,
        role: RoleSessionOwner,
        runner: RunnerSession,
    }
    impl TestSession {
        pub(crate) fn new(parameters: Vec<Value>, role: SessionRole, lifetime: u64) -> Self {
            Self::new_in_domain(parameters, role, lifetime, DOMAIN)
        }
        fn new_in_domain(
            parameters: Vec<Value>,
            role: SessionRole,
            lifetime: u64,
            domain: &str,
        ) -> Self {
            use std::os::unix::fs::DirBuilderExt;
            // macOS TMPDIR may exceed the bounded Unix socket path length.
            let root = PathBuf::from("/tmp").join(format!("nrg2mv-{}", uuid::Uuid::now_v7()));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&root)
                .unwrap();
            let declaration = SessionDeclaration {
                schema_version: 1,
                session_id: "mv-test".into(),
                launch_id: "mv-launch".into(),
                role,
                lifetime_millis: lifetime,
                liveness_socket: root.join("live.sock"),
                handshake_secret: "m".repeat(32),
                domain_grants: parameters
                    .into_iter()
                    .enumerate()
                    .map(|(i, parameters)| DomainGrant {
                        domain: domain.into(),
                        grant_id: format!("mv-{i}"),
                        parameters,
                    })
                    .collect(),
            };
            let path = root.join("declaration.json");
            declaration.write_private(&path).unwrap();
            let prepared = PreparedRoleSession::read_and_claim(&path, role).unwrap();
            let runner = RunnerSession::start(declaration).unwrap();
            let process = match role {
                SessionRole::Fe => {
                    ProcessBinding::Frontend(novarocks_types::FrontendProcessId::new_v7())
                }
                SessionRole::Be => {
                    ProcessBinding::Backend(novarocks_types::BackendProcessId::new_v7())
                }
            };
            let role = prepared.bind(process).unwrap();
            Self { root, role, runner }
        }
        pub(crate) fn controls(&self) -> Result<MvTestControls, String> {
            MvTestControls::from_grants(self.role.scope().grants_for(DOMAIN))
        }
        pub(crate) fn close(&mut self) {
            self.role.close_and_join().unwrap();
            self.runner.end_and_join().unwrap();
        }
    }
    impl Drop for TestSession {
        fn drop(&mut self) {
            self.close();
            std::fs::remove_dir_all(&self.root).unwrap();
        }
    }

    mod scheduler_outcome_tests {
        use super::*;
        use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
        use novarocks_mv_application::persistence::definition::{
            MvAcceleratorSourceRevision, test_source_revision,
        };
        use novarocks_mv_application::scheduler_runtime::{
            MvRefreshFailure, MvRefreshRuntimeDecision,
        };
        use novarocks_type_contract::CompileControlError;
        use prost::Message;

        struct Fixture {
            session: TestSession,
            controls: MvTestControls,
            directory: PathBuf,
        }
        impl Fixture {
            fn new() -> Self {
                let directory =
                    PathBuf::from("/tmp").join(format!("nr-mv-outcome-{}", uuid::Uuid::now_v7()));
                let session = TestSession::new(
                    vec![
                        json!({"action":"scheduler-outcome","catalog":"ice","namespace":"sales","mv":"orders","directory":directory}),
                    ],
                    SessionRole::Fe,
                    30000,
                );
                let controls = session.controls().unwrap();
                Self {
                    session,
                    controls,
                    directory,
                }
            }
            fn target() -> novarocks_sql::planning::mv::SqlMvTarget {
                novarocks_sql::planning::mv::SqlMvTarget {
                    catalog: Some("ice".into()),
                    database: "sales".into(),
                    name: "orders".into(),
                }
            }
            fn source() -> MvAcceleratorSourceRevision {
                test_source_revision(
                    "ice",
                    "sales",
                    "orders",
                    novarocks_spi::connector::ConnectorTableObjectId::try_new(
                        bytes::Bytes::from_static(b"object-a"),
                    )
                    .unwrap(),
                    None,
                )
            }
            fn admit(&self, source: &MvAcceleratorSourceRevision) -> SchedulerOutcomeRequest {
                self.controls
                    .scheduler_request_admitted(&Self::target(), 7, source)
                    .unwrap()
                    .unwrap()
            }
            fn record(&self) -> Value {
                serde_json::from_slice(
                    &std::fs::read(self.directory.join("scheduler-outcome.json")).unwrap(),
                )
                .unwrap()
            }
        }
        impl Drop for Fixture {
            fn drop(&mut self) {
                if self.directory.exists() {
                    std::fs::remove_dir_all(&self.directory).unwrap();
                }
            }
        }
        fn blocked(source: Option<CompileControlError>) -> MvRefreshRuntimeDecision {
            use novarocks_mv_application::scheduler::MvSchedulerConfig;
            use novarocks_mv_application::scheduler_runtime::{
                MvRefreshDisposition, MvRefreshSchedulerRuntime,
            };
            let mut scheduler = MvRefreshSchedulerRuntime::<i64, ()>::new(MvSchedulerConfig::new(
                true, 1, 1, 10, 40,
            ));
            let decision = scheduler.record(
                &7,
                MvRefreshDisposition::InvalidDefinition(MvRefreshFailure::new(
                    "[UnsupportedCompiledShape] untrusted diagnostic only",
                    source,
                )),
                100,
            );
            assert!(matches!(decision, MvRefreshRuntimeDecision::Blocked { .. }));
            decision
        }
        fn capability() -> CompileControlError {
            CompileControlError::Source(novarocks_failure_contract::SharedFailureCause::new(
                novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability("router".into()),
            ))
        }
        fn fact(record: &Value) -> novarocks_execution_contract::failure::FailureFact {
            let bytes: Vec<u8> = serde_json::from_value(record["failure_fact"].clone()).unwrap();
            let dto =
                novarocks_proto_models::novarocks::FailureFact::decode(bytes.as_slice()).unwrap();
            novarocks_task_codec::status::decode_failure_fact(
                &dto,
                novarocks_proto_codec::FieldPath::root("test"),
            )
            .unwrap()
        }
        #[test]
        fn original_native_execution_fact_roundtrips_through_authorized_scheduler_outcome() {
            use novarocks_mv_application::{
                scheduler::MvSchedulerConfig,
                scheduler_runtime::{MvRefreshDisposition, MvRefreshSchedulerRuntime},
            };
            use novarocks_query_application::execution_error::{
                ExecutionFailureInput, ExecutionFailureSite,
            };
            for original in [
                novarocks_local_compiler::FragmentCompileError::Unsupported {
                    node: Some(novarocks_physical_plan::NodeId::new(2)),
                    feature: "private Native capability diagnostic",
                },
                novarocks_local_compiler::FragmentCompileError::Invalid(
                    "unsupported private text must not choose a capability identity",
                ),
            ] {
                let input = ExecutionFailureInput::new(
                    original.failure_fact(),
                    novarocks_execution_contract::SafeDetail::truncating(
                        "private original Native execution diagnostic",
                    ),
                    ExecutionFailureSite::Coordinator,
                );
                let mut scheduler = MvRefreshSchedulerRuntime::<i64, ()>::new(
                    MvSchedulerConfig::new(true, 1, 1, 10, 40),
                );
                let decision = scheduler.record(
                    &7,
                    MvRefreshDisposition::InvariantViolation(
                        MvRefreshFailure::new("private scheduler detail", None)
                            .with_execution_failure(Some(input.clone())),
                    ),
                    100,
                );
                assert_eq!(decision.execution_failure(), Some(&input));
                assert_eq!(decision.compile_control_error(), None);
                let fixture = Fixture::new();
                fixture
                    .controls
                    .scheduler_request_completed(fixture.admit(&Fixture::source()), &decision)
                    .unwrap();
                let record = fixture.record();
                assert_eq!(record["failure_identity"], "Source");
                assert_eq!(fact(&record), input.fact());
                let safe = ExecutionFailureInput::new(
                    input.fact(),
                    novarocks_execution_contract::SafeDetail::truncating(
                        "MV scheduler request blocked; source diagnostic retained in FE role logs",
                    ),
                    ExecutionFailureSite::Coordinator,
                );
                assert_eq!(record["public_message"], safe.user_error().to_string());
                assert!(!record.to_string().contains("private"));
                assert_eq!(record["actual_task_attempt"], "NOT_OBSERVED");
                assert_eq!(record["actual_publication_attempt"], "NOT_OBSERVED");
            }
            let fixture = Fixture::new();
            fixture
                .controls
                .scheduler_request_completed(fixture.admit(&Fixture::source()), &blocked(None))
                .unwrap();
            let record = fixture.record();
            assert_eq!(record["failure_identity"], "UNKNOWN");
            assert!(record["failure_fact"].is_null());
            assert!(record["public_message"].is_null());
        }

        #[test]
        fn actual_admission_success_and_transient_do_not_consume_later_blocked_slot() {
            let fixture = Fixture::new();
            let source = Fixture::source();
            fixture
                .controls
                .scheduler_request_completed(
                    fixture.admit(&source),
                    &MvRefreshRuntimeDecision::Success,
                )
                .unwrap();
            fixture
                .controls
                .scheduler_request_completed(
                    fixture.admit(&source),
                    &MvRefreshRuntimeDecision::TransientBackoff {
                        error: "temporarily unavailable".into(),
                        retry_at_ms: 20,
                    },
                )
                .unwrap();
            assert!(!fixture.directory.join("scheduler-outcome.json").exists());
            let mut next = source.clone();
            next.publication_revision = Some(novarocks_mv_application::persistence::identity::DocumentRevision::from_canonical_bytes(b"new P"));
            let original = capability();
            let CompileControlError::Source(cause) = &original else {
                unreachable!()
            };
            fixture
                .controls
                .scheduler_request_completed(fixture.admit(&next), &blocked(Some(original.clone())))
                .unwrap();
            let record = fixture.record();
            assert_eq!(fact(&record), cause.failure_fact());
            assert_eq!(fact(&record).class(), FailureClass::Unsupported);
            assert!(record["first_admitted_source"]["publication_revision"].is_null());
            assert!(!record["blocked_request_source"]["publication_revision"].is_null());
            assert_eq!(record["admission_observation_sequence"], 3);
            assert_eq!(record["actual_publication_attempt"], "NOT_OBSERVED");
            assert!(!record.to_string().contains("untrusted diagnostic"));
        }
        #[test]
        fn first_blocked_is_immutable_and_late_peek_does_not_rearm_or_execute() {
            let mut fixture = Fixture::new();
            let source = Fixture::source();
            let admitted_before_terminal = fixture.admit(&source);
            fixture
                .controls
                .scheduler_request_completed(fixture.admit(&source), &blocked(Some(capability())))
                .unwrap();
            let bytes = std::fs::read(fixture.directory.join("scheduler-outcome.json")).unwrap();
            fixture
                .controls
                .scheduler_request_completed(admitted_before_terminal, &blocked(None))
                .unwrap();
            assert_eq!(
                bytes,
                std::fs::read(fixture.directory.join("scheduler-outcome.json")).unwrap()
            );
            assert!(
                fixture
                    .controls
                    .scheduler_request_admitted(&Fixture::target(), 7, &source)
                    .unwrap()
                    .is_none()
            );
            fixture.session.close();
            assert_eq!(fixture.record()["decision"], "Blocked");
            assert_eq!(
                bytes,
                std::fs::read(fixture.directory.join("scheduler-outcome.json")).unwrap()
            );
        }
        #[test]
        fn missing_source_and_invalid_source_never_become_capability_from_text() {
            for source in [
                None,
                Some(CompileControlError::Source(
                    novarocks_failure_contract::SharedFailureCause::new(
                        novarocks_plan_codec::PhysicalEncodeError::Invalid(
                            "unsupported router".into(),
                        ),
                    ),
                )),
            ] {
                let fixture = Fixture::new();
                fixture
                    .controls
                    .scheduler_request_completed(
                        fixture.admit(&Fixture::source()),
                        &blocked(source.clone()),
                    )
                    .unwrap();
                let record = fixture.record();
                if source.is_none() {
                    assert!(record["failure_fact"].is_null());
                    assert!(record["public_message"].is_null());
                } else {
                    assert_ne!(fact(&record).class(), FailureClass::Unsupported);
                }
            }
        }
        #[test]
        fn recreated_object_and_changed_configuration_cannot_rebind_frozen_target() {
            for configuration in [false, true] {
                let fixture = Fixture::new();
                let mut changed = Fixture::source();
                drop(fixture.admit(&changed));
                if configuration {
                    changed.configuration_revision = novarocks_mv_application::persistence::identity::DocumentRevision::from_canonical_bytes(b"different C");
                } else {
                    changed.target_object_id =
                        novarocks_spi::connector::ConnectorTableObjectId::try_new(
                            bytes::Bytes::from_static(b"object-b"),
                        )
                        .unwrap();
                }
                assert!(
                    fixture
                        .controls
                        .scheduler_request_admitted(&Fixture::target(), 7, &changed)
                        .is_err()
                );
                assert!(
                    fixture
                        .directory
                        .join("scheduler-outcome-error.json")
                        .exists()
                );
                assert!(!fixture.directory.join("scheduler-outcome.json").exists());
            }
        }
        #[test]
        fn oversized_document_is_refused_before_any_observation_file_is_created() {
            let fixture = Fixture::new();
            let value = json!({"payload":"x".repeat(SCHEDULER_OUTCOME_MAX_BYTES + 1)});
            assert!(
                publish_scheduler_document(&fixture.directory, "scheduler-outcome.json", &value)
                    .is_err()
            );
            assert!(!fixture.directory.exists());
        }

        #[test]
        fn wrong_target_foreign_launch_and_closed_grant_cannot_capture_a_terminal() {
            let mut fixture = Fixture::new();
            let mut target = Fixture::target();
            target.name = "other".into();
            assert!(
                fixture
                    .controls
                    .scheduler_request_admitted(&target, 7, &Fixture::source())
                    .unwrap()
                    .is_none()
            );
            let foreign = Fixture::new();
            assert!(
                fixture
                    .controls
                    .scheduler_request_completed(
                        foreign.admit(&Fixture::source()),
                        &blocked(Some(capability()))
                    )
                    .is_err()
            );
            let admitted = fixture.admit(&Fixture::source());
            fixture.session.close();
            assert!(
                fixture
                    .controls
                    .scheduler_request_completed(admitted, &blocked(Some(capability())))
                    .is_err()
            );
            assert!(!fixture.directory.join("scheduler-outcome.json").exists());
        }
    }

    pub(crate) fn wipe_grant() -> Value {
        json!({"action":"accelerator-wipe","family":MV_ACCELERATOR_STATE_FAMILY.prefix()})
    }
    fn observation() -> Value {
        json!({"action":"document-observation","catalog":"ice","namespace":"sales","mv":"orders","level":"package"})
    }

    #[test]
    fn mv_observation_has_no_family_wipe_authority_and_is_finite() {
        let session = TestSession::new(vec![observation(), observation()], SessionRole::Fe, 3000);
        let controls = session.controls().unwrap();
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "wipe")
                .is_err()
        );
        assert!(
            controls
                .claim_call("ice", "sales", "other", "package")
                .is_err()
        );
        drop(
            controls
                .claim_call("ice", "sales", "orders", "package")
                .unwrap(),
        );
        drop(
            controls
                .claim_call("ice", "sales", "orders", "package")
                .unwrap(),
        );
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "package")
                .is_err()
        );
    }

    #[test]
    fn mv_wipe_is_bound_to_fe_family_and_only_one_claim() {
        assert!(MvTestControls::from_grants(Vec::new()).is_err());
        assert!(
            parse_action(&json!({"action":"accelerator-wipe","family":"catalog/desired-state"}))
                .is_err()
        );
        let be = TestSession::new(vec![wipe_grant()], SessionRole::Be, 3000);
        assert!(be.controls().is_err());
        let fe = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        let controls = fe.controls().unwrap();
        drop(
            controls
                .claim_call("ice", "sales", "orders", "wipe")
                .unwrap(),
        );
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "wipe")
                .is_err()
        );
        let duplicate = TestSession::new(vec![wipe_grant(), wipe_grant()], SessionRole::Fe, 3000);
        assert!(duplicate.controls().is_err());
    }

    #[test]
    fn closing_fe_session_rejects_remaining_mv_actions() {
        let mut session = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        let controls = session.controls().unwrap();
        session.close();
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "wipe")
                .is_err()
        );
    }

    #[test]
    fn mv_grant_parameters_are_closed_and_actions_are_not_interchangeable() {
        assert!(parse_action(&json!({"action":"document-observation","catalog":"ice","namespace":"sales","mv":"orders","level":"wipe"})).is_err());
        assert!(parse_action(&json!({"action":"accelerator-wipe","family":MV_ACCELERATOR_STATE_FAMILY.prefix(),"extra":true})).is_err());
        assert!(parse_action(&json!({"action":"start-empty"})).is_err());
    }

    #[test]
    fn rewrite_nomination_rejects_another_connection_or_generation() {
        let actual = StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (7_u64 << 32) | 1_u64,
                )
                .expect("nonzero fixture session"),
            ),
            2,
        );
        let other_connection = StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (8_u64 << 32) | 1_u64,
                )
                .expect("nonzero fixture session"),
            ),
            2,
        );
        let later = StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (7_u64 << 32) | 1_u64,
                )
                .expect("nonzero fixture session"),
            ),
            3,
        );
        assert!(MvRewriteStatementTarget::new(actual, other_connection, "run".into()).is_err());
        assert!(MvRewriteStatementTarget::new(actual, later, "run".into()).is_err());
        assert!(MvRewriteStatementTarget::new(actual, actual, "run".into()).is_ok());
    }

    #[test]
    fn unrelated_rewrite_cannot_claim_or_create_a_marker() {
        let directory = std::env::temp_dir().join(format!("nrrewrite-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("mvx4-rewrite-hold.trigger"), "hold").unwrap();
        let session = TestSession::new(
            vec![
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"right","directory":directory}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let statement = StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (7_u64 << 32) | 1_u64,
                )
                .expect("nonzero fixture session"),
            ),
            2,
        );
        let target = MvRewriteStatementTarget::new(statement, statement, "wrong".into()).unwrap();
        let statements = novarocks_parser::parse("SELECT 1").unwrap();
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("query")
        };
        let cancellation =
            novarocks_query_application::cancellation::QueryCancellationSource::new();
        controls
            .rewrite(
                Some("ice"),
                "sales",
                query,
                true,
                Some(&target),
                &cancellation.view(),
            )
            .unwrap();
        assert!(session.role.scope().observations().is_empty());
        assert!(
            !directory
                .join("mvx4-completed-mv-target-frozen.marker")
                .exists()
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn transient_preparation_matches_exact_target_and_records_actual_single_action() {
        let directory = std::env::temp_dir().join(format!("nrtransient-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        let trigger = directory.join("mvx4-scheduler-transient-preparation-orders.trigger");
        std::fs::write(&trigger, "inject").unwrap();
        let session = TestSession::new(
            vec![
                json!({"action":"transient-preparation","catalog":"ice","namespace":"sales","mv":"orders","directory":directory}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        assert!(
            !controls
                .transient_preparation("other", "sales", "orders")
                .unwrap()
        );
        assert!(trigger.exists());
        assert!(session.role.scope().observations().is_empty());
        assert!(
            controls
                .transient_preparation("ice", "sales", "orders")
                .unwrap()
        );
        assert!(!trigger.exists());
        assert!(trigger.with_extension("consumed").exists());
        std::fs::write(&trigger, "second injection").unwrap();
        assert!(
            !controls
                .transient_preparation("ice", "sales", "orders")
                .unwrap()
        );
        assert!(trigger.exists());
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count(),
            1
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn session_close_releases_scheduler_barrier_and_actual_waiter_is_joined() {
        let directory = std::env::temp_dir().join(format!("nrscheduler-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("mvx4-scheduler-hold.trigger"), "hold").unwrap();
        let mut session = TestSession::new(
            vec![
                json!({"action":"scheduler-hold","catalog":"ice","namespace":"sales","mv":"orders","directory":directory}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let waiter = tokio::spawn(async move {
            let cancellation =
                novarocks_query_application::cancellation::QueryCancellationSource::new();
            controls
                .scheduler_async(
                    &novarocks_sql::planning::mv::SqlMvTarget {
                        catalog: Some("ice".into()),
                        database: "sales".into(),
                        name: "orders".into(),
                    },
                    &cancellation.view(),
                )
                .await
        });
        let until = Instant::now() + Duration::from_secs(1);
        while !directory
            .join("mvx4-scheduler-admitted-orders.marker")
            .exists()
        {
            assert!(Instant::now() < until);
            tokio::task::yield_now().await;
        }
        session.close();
        assert!(
            waiter
                .await
                .unwrap()
                .unwrap_err()
                .contains("session closed")
        );
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count(),
            1
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
    fn nomination(
        session: &TestSession,
        statement: StatementToken,
        run_token: &str,
    ) -> StatementWorkNomination {
        StatementWorkNomination {
            target_session_id: session.role.scope().session_id().into(),
            statement_run_token: run_token.into(),
            frontend_process_id: session.role.scope().process().process_id(),
            connection_id: 7,
            session_key: statement.session().key().get(),
            statement_generation: statement.generation(),
        }
    }

    fn input(
        session: &TestSession,
        nomination: &StatementWorkNomination,
    ) -> StatementInputNomination {
        use sha2::Digest;
        StatementInputNomination {
            target_session_id: nomination.target_session_id.clone(),
            target_launch_id: session.role.scope().launch_id().into(),
            statement_run_token: nomination.statement_run_token.clone(),
            frontend_process_id: nomination.frontend_process_id.clone(),
            connection_id: nomination.connection_id,
            session_key: nomination.session_key,
            sql_sha256: sha2::Sha256::digest(b"SELECT 1").into(),
        }
    }

    #[test]
    fn rewrite_input_survives_journal_reuse_without_claiming_and_fresh_slot_stays_independent() {
        let mut session = TestSession::new(
            vec![
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"first","directory":"/tmp"}),
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"fresh","directory":"/tmp"}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let actual = StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (7_u64 << 32) | 1_u64,
                )
                .expect("nonzero fixture session"),
            ),
            1,
        );
        let original = nomination(&session, actual, "first");
        let pinned = input(&session, &original);
        assert!(controls.nominate_statement(&pinned, &original).is_err());
        let mut foreign_session = pinned.clone();
        foreign_session.target_session_id = "foreign-session".into();
        let mut foreign_process = pinned.clone();
        foreign_process.frontend_process_id =
            novarocks_types::FrontendProcessId::new_v7().to_string();
        for foreign in [foreign_session, foreign_process] {
            assert!(controls.pin_statement_input(&foreign).is_err());
        }
        assert!(
            controls
                .actions
                .iter()
                .all(|action| action.frozen_input.lock().unwrap().is_none())
        );
        controls.pin_statement_input(&pinned).unwrap();
        controls.pin_statement_input(&pinned).unwrap();
        let mut different_client = pinned.clone();
        different_client.session_key += 1;
        let mut different_connection = pinned.clone();
        different_connection.connection_id += 1;
        let mut different_sql = pinned.clone();
        different_sql.sql_sha256[0] ^= 0xff;
        let mut different_launch = pinned.clone();
        different_launch.target_launch_id = "foreign-launch".into();
        for replacement in [
            different_client,
            different_connection,
            different_sql,
            different_launch,
        ] {
            assert!(controls.pin_statement_input(&replacement).is_err());
            assert!(
                controls
                    .nominate_statement(&replacement, &original)
                    .is_err()
            );
        }
        assert_eq!(
            controls.actions[0].frozen_input.lock().unwrap().as_ref(),
            Some(&pinned)
        );
        assert!(
            controls.actions.iter().all(|action| action
                .frozen_nomination
                .lock()
                .unwrap()
                .is_none())
        );
        let mut unknown = pinned.clone();
        unknown.statement_run_token = "not-declared".into();
        controls.pin_statement_input(&unknown).unwrap();
        assert_eq!(controls.actions.len(), 2);
        assert!(controls.actions[1].frozen_input.lock().unwrap().is_none());
        let fresh_nom = nomination(
            &session,
            StatementToken::new(
                SessionToken::new(
                    novarocks_query_application::session_control::ClientSessionKey::new(
                        (8_u64 << 32) | 2_u64,
                    )
                    .expect("nonzero fixture session"),
                ),
                1,
            ),
            "fresh",
        );
        let fresh_input = input(&session, &fresh_nom);
        controls.pin_statement_input(&fresh_input).unwrap();
        controls
            .nominate_statement(&fresh_input, &fresh_nom)
            .unwrap();
        controls.nominate_statement(&pinned, &original).unwrap();
        assert!(session.role.scope().observations().is_empty());
        session.close();
        assert!(controls.pin_statement_input(&pinned).is_err());
        assert!(controls.nominate_statement(&pinned, &original).is_err());
    }

    #[test]
    fn rewrite_first_nomination_cannot_redirect_before_any_action_claim() {
        let directory = std::env::temp_dir().join(format!("nrmvfreeze-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        let session = TestSession::new(
            vec![
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"frozen-run","directory":directory}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let a = StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (7_u64 << 32) | 1_u64,
                )
                .expect("nonzero fixture session"),
            ),
            1,
        );
        let original = nomination(&session, a, "frozen-run");
        controls
            .pin_statement_input(&input(&session, &original))
            .unwrap();
        controls
            .nominate_statement(&input(&session, &original), &original)
            .unwrap();
        controls
            .nominate_statement(&input(&session, &original), &original)
            .unwrap();
        let statements = novarocks_parser::parse("SELECT 1").unwrap();
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("query")
        };
        let cancellation =
            novarocks_query_application::cancellation::QueryCancellationSource::new();
        let a_target = MvRewriteStatementTarget::new(a, a, "frozen-run".into()).unwrap();
        controls
            .rewrite(
                Some("ice"),
                "sales",
                query,
                false,
                Some(&a_target),
                &cancellation.view(),
            )
            .unwrap();
        assert!(session.role.scope().observations().is_empty());
        for b in [
            StatementToken::new(
                SessionToken::new(
                    novarocks_query_application::session_control::ClientSessionKey::new(
                        (7_u64 << 32) | 1_u64,
                    )
                    .expect("nonzero fixture session"),
                ),
                2,
            ),
            StatementToken::new(
                SessionToken::new(
                    novarocks_query_application::session_control::ClientSessionKey::new(
                        (7_u64 << 32) | 2_u64,
                    )
                    .expect("nonzero fixture session"),
                ),
                1,
            ),
            StatementToken::new(
                SessionToken::new(
                    novarocks_query_application::session_control::ClientSessionKey::new(
                        (8_u64 << 32) | 1_u64,
                    )
                    .expect("nonzero fixture session"),
                ),
                1,
            ),
        ] {
            assert!(
                controls
                    .nominate_statement(
                        &input(&session, &nomination(&session, b, "frozen-run")),
                        &nomination(&session, b, "frozen-run")
                    )
                    .unwrap_err()
                    .contains("replacement")
            );
            let b_target = MvRewriteStatementTarget::new(b, b, "frozen-run".into()).unwrap();
            assert!(
                controls
                    .rewrite(
                        Some("ice"),
                        "sales",
                        query,
                        true,
                        Some(&b_target),
                        &cancellation.view()
                    )
                    .unwrap_err()
                    .contains("frozen original")
            );
        }
        assert!(session.role.scope().observations().is_empty());
        assert!(
            !directory
                .join("mvx4-completed-mv-target-frozen.marker")
                .exists()
        );
        // No hold existed: the original target can replay without consuming an action.
        controls
            .rewrite(
                Some("ice"),
                "sales",
                query,
                true,
                Some(&a_target),
                &cancellation.view(),
            )
            .unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn unrelated_observer_nomination_does_not_steal_a_finite_rewrite_target() {
        let session = TestSession::new(
            vec![
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"listed","directory":"/tmp"}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let other = StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (8_u64 << 32) | 1_u64,
                )
                .expect("nonzero fixture session"),
            ),
            1,
        );
        controls
            .nominate_statement(
                &input(&session, &nomination(&session, other, "unlisted")),
                &nomination(&session, other, "unlisted"),
            )
            .unwrap();
        assert!(
            controls.actions[0]
                .frozen_nomination
                .lock()
                .unwrap()
                .is_none()
        );
        let actual = StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (7_u64 << 32) | 1_u64,
                )
                .expect("nonzero fixture session"),
            ),
            1,
        );
        controls
            .pin_statement_input(&input(&session, &nomination(&session, actual, "listed")))
            .unwrap();
        controls
            .nominate_statement(
                &input(&session, &nomination(&session, actual, "listed")),
                &nomination(&session, actual, "listed"),
            )
            .unwrap();
        assert!(session.role.scope().observations().is_empty());
    }

    #[test]
    fn mv_controls_reject_foreign_domain_and_mixed_actual_fe_processes() {
        let first = TestSession::new(vec![observation()], SessionRole::Fe, 3000);
        let second = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        assert_ne!(first.role.scope().process(), second.role.scope().process());
        let mut mixed = first.role.scope().grants_for(DOMAIN);
        mixed.extend(second.role.scope().grants_for(DOMAIN));
        assert!(MvTestControls::from_grants(mixed).is_err());
        assert!(first.role.scope().observations().is_empty());
        assert!(second.role.scope().observations().is_empty());

        let foreign =
            TestSession::new_in_domain(vec![wipe_grant()], SessionRole::Fe, 3000, "foreign-mv");
        assert!(
            MvTestControls::from_grants(foreign.role.scope().grants_for("foreign-mv")).is_err()
        );
        assert!(foreign.role.scope().observations().is_empty());
    }

    #[test]
    fn mv_document_target_mismatches_do_not_consume_independent_levels() {
        let mut provenance = observation();
        provenance["level"] = json!("provenance");
        let session = TestSession::new(vec![observation(), provenance], SessionRole::Fe, 3000);
        let controls = session.controls().unwrap();
        for (catalog, namespace, mv, level) in [
            ("other", "sales", "orders", "package"),
            ("ice", "other", "orders", "package"),
            ("ice", "sales", "other", "package"),
            ("ice", "sales", "orders", "baseline"),
            ("ice", "sales", "orders", "wipe"),
        ] {
            assert!(controls.claim_call(catalog, namespace, mv, level).is_err());
        }
        assert!(session.role.scope().observations().is_empty());
        let package = controls
            .claim_call("ice", "sales", "orders", "package")
            .unwrap();
        let provenance = controls
            .claim_call("ice", "sales", "orders", "provenance")
            .unwrap();
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "package")
                .is_err()
        );
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "provenance")
                .is_err()
        );
        let events = session.role.scope().observations();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event == "claimed")
                .count(),
            2
        );
        // Authorization alone is not evidence that lake documents were read.
        assert_eq!(
            events.iter().filter(|event| event.event == "hit").count(),
            0
        );
        drop((package, provenance));
    }

    #[test]
    fn mv_family_wipe_does_not_renew_by_changing_mv_or_rebuilding_owner() {
        let mut first = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        let controls = first.controls().unwrap();
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "package")
                .is_err()
        );
        let claim = controls
            .claim_call("ice", "sales", "orders", "wipe")
            .unwrap();
        let reconstructed = first.controls().unwrap();
        // Wipe is one whole-family authorization, not one authorization per MV.
        assert!(
            reconstructed
                .claim_call("other", "other", "other", "wipe")
                .is_err()
        );
        drop(claim);
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "wipe")
                .is_err()
        );
        assert_eq!(
            first
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count(),
            0,
        );
        first.close();
        let replacement = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        assert_ne!(
            first.role.scope().process(),
            replacement.role.scope().process()
        );
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "wipe")
                .is_err()
        );
        let new_controls = replacement.controls().unwrap();
        drop(
            new_controls
                .claim_call("ice", "sales", "orders", "wipe")
                .unwrap(),
        );
        assert!(
            new_controls
                .claim_call("ice", "sales", "orders", "wipe")
                .is_err()
        );
    }

    #[test]
    fn rewrite_foreign_session_or_fe_nomination_cannot_pin_the_target() {
        let session = TestSession::new(
            vec![
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"listed","directory":"/tmp"}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let statement = StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (7_u64 << 32) | 1_u64,
                )
                .expect("nonzero fixture session"),
            ),
            1,
        );
        let original = nomination(&session, statement, "listed");
        let mut wrong_session = original.clone();
        wrong_session.target_session_id = "other-fe-session".into();
        let mut wrong_process = original.clone();
        wrong_process.frontend_process_id =
            novarocks_types::FrontendProcessId::new_v7().to_string();
        for foreign in [wrong_session, wrong_process] {
            assert!(
                controls
                    .nominate_statement(&input(&session, &foreign), &foreign)
                    .is_err()
            );
            assert!(
                controls.actions[0]
                    .frozen_nomination
                    .lock()
                    .unwrap()
                    .is_none()
            );
        }
        controls
            .pin_statement_input(&input(&session, &original))
            .unwrap();
        controls
            .nominate_statement(&input(&session, &original), &original)
            .unwrap();
        controls
            .nominate_statement(&input(&session, &original), &original)
            .unwrap();
        assert_eq!(
            controls.actions[0]
                .frozen_nomination
                .lock()
                .unwrap()
                .as_ref(),
            Some(&original),
        );
        assert!(session.role.scope().observations().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recovery_wrong_target_phase_or_arm_token_cannot_claim() {
        let root = std::env::temp_dir().join(format!("nrmvrecovery-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        let controlled = TestSession::new(
            vec![
                json!({"action":"recovery-hold","catalog":"ice","namespace":"sales","mv":"orders","phase":"data-prepared","directory":root,"token":"right"}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = controlled.controls().unwrap();
        let trigger = root.join("mv-refresh-at-data-prepared.trigger");
        std::fs::write(&trigger, "token=wrong\n").unwrap();
        for (catalog, namespace, mv, phase) in [
            ("other", "sales", "orders", "data-prepared"),
            ("ice", "other", "orders", "data-prepared"),
            ("ice", "sales", "other", "data-prepared"),
            ("ice", "sales", "orders", "publication-committed"),
        ] {
            controls
                .recovery_async(catalog, namespace, mv, phase)
                .await
                .unwrap();
        }
        assert!(controlled.role.scope().observations().is_empty());
        assert!(
            controls
                .recovery_async("ice", "sales", "orders", "data-prepared")
                .await
                .unwrap_err()
                .contains("finite grant")
        );
        assert!(controlled.role.scope().observations().is_empty());
        assert!(trigger.exists());
        // A wrong transport token did not consume the authorized action.
        drop(
            controls.actions[0]
                .grant
                .claim_once(controls.process)
                .unwrap(),
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scheduler_async_yields_to_release_and_keeps_the_original_claim() {
        let directory =
            std::env::temp_dir().join(format!("nrmvasyncscheduler-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        let trigger = directory.join("mvx4-scheduler-hold.trigger");
        std::fs::write(&trigger, "hold").unwrap();
        let session = TestSession::new(
            vec![
                json!({"action":"scheduler-hold","catalog":"ice","namespace":"sales","mv":"orders","directory":directory}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let observed = session.role.scope().clone();
        let marker = directory.join("mvx4-scheduler-admitted-orders.marker");
        let release = trigger.clone();
        let caller = std::thread::current().id();
        let releaser = tokio::spawn(async move {
            assert_eq!(std::thread::current().id(), caller);
            assert!(marker.exists());
            assert_eq!(
                observed
                    .observations()
                    .iter()
                    .filter(|event| event.event == "hit")
                    .count(),
                1
            );
            std::fs::remove_file(release).unwrap();
        });
        let cancellation =
            novarocks_query_application::cancellation::QueryCancellationSource::new();
        assert!(
            !controls
                .scheduler_async(
                    &novarocks_sql::planning::mv::SqlMvTarget {
                        catalog: Some("ice".into()),
                        database: "sales".into(),
                        name: "orders".into()
                    },
                    &cancellation.view()
                )
                .await
                .unwrap()
        );
        releaser.await.unwrap();
        assert!(!trigger.exists());
        assert!(
            controls.actions[0]
                .grant
                .claim_once(controls.process)
                .is_err()
        );
        for expected in ["claimed", "hit", "claim-released"] {
            assert_eq!(
                session
                    .role
                    .scope()
                    .observations()
                    .iter()
                    .filter(|event| event.event == expected)
                    .count(),
                1
            );
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recovery_async_yields_to_release_on_the_same_runtime_and_claims_once() {
        let directory =
            std::env::temp_dir().join(format!("nrmvasyncrecovery-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        let session = TestSession::new(
            vec![
                json!({"action":"recovery-hold","catalog":"ice","namespace":"sales","mv":"orders","phase":"data-prepared","directory":directory,"token":"right"}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let trigger = directory.join("mv-refresh-at-data-prepared.trigger");
        std::fs::write(&trigger, "token=right\n").unwrap();
        let scope = session.role.scope().clone();
        let release = trigger.clone();
        let caller = std::thread::current().id();
        let releaser = tokio::spawn(async move {
            assert_eq!(std::thread::current().id(), caller);
            assert_eq!(
                scope
                    .observations()
                    .iter()
                    .filter(|event| event.event == "hit")
                    .count(),
                1
            );
            std::fs::remove_file(release).unwrap();
        });
        controls
            .recovery_async("ice", "sales", "orders", "data-prepared")
            .await
            .unwrap();
        releaser.await.unwrap();
        assert!(!trigger.exists());
        assert!(
            controls.actions[0]
                .grant
                .claim_once(controls.process)
                .is_err()
        );
        let observations = session.role.scope().observations();
        for expected in ["claimed", "hit", "claim-released"] {
            assert_eq!(
                observations
                    .iter()
                    .filter(|event| event.event == expected)
                    .count(),
                1
            );
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn known_committed_async_keeps_the_marker_and_grant_close_wakes_its_waiter() {
        use std::future::Future;
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use std::task::{Context, Poll, Wake, Waker};
        struct CountWake(AtomicUsize);
        impl Wake for CountWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let directory =
            std::env::temp_dir().join(format!("nrmvasynccommit-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        let session = TestSession::new(
            vec![
                json!({"action":"known-committed-hold","catalog":"ice","namespace":"sales","mv":"orders","directory":directory}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let trigger =
            novarocks_failpoint::mv_known_committed_before_projector_cas_trigger_path(&directory);
        std::fs::write(&trigger, "hold").unwrap();
        let publication = novarocks_spi::connector::LakePublicationId::new_v7();
        let wake = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake));
        let mut cx = Context::from_waker(&waker);
        let mut waiter =
            Box::pin(controls.known_committed_async("ice", "sales", "orders", &publication));
        assert!(waiter.as_mut().poll(&mut cx).is_pending());
        let marker =
            novarocks_failpoint::mv_known_committed_before_projector_cas_marker_path(&directory);
        assert_eq!(
            std::fs::read_to_string(marker).unwrap(),
            format!(
                "publication_id={}\nphase=known-committed-before-projector-cas\n",
                publication.as_uuid()
            )
        );
        assert_eq!(wake.0.load(Ordering::SeqCst), 0);
        // No runtime tick or filesystem release occurs between these calls.
        // This wake must be the original grant's close notification.
        session
            .role
            .scope()
            .close(novarocks_failpoint::scope::CloseCause::ExplicitEnd);
        assert!(wake.0.load(Ordering::SeqCst) > 0);
        let Poll::Ready(Err(error)) = waiter.as_mut().poll(&mut cx) else {
            panic!("closed original grant must finish the same barrier");
        };
        assert!(error.contains("session closed"));
        assert!(trigger.exists());
        let observations = session.role.scope().observations();
        for expected in ["claimed", "hit", "claim-released"] {
            assert_eq!(
                observations
                    .iter()
                    .filter(|event| event.event == expected)
                    .count(),
                1
            );
        }
        drop(waiter);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rewrite_exact_nomination_and_query_claim_once_and_join_released_waiter() {
        let directory = std::env::temp_dir().join(format!("nrmvrewrite-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        let hold = directory.join("mvx4-rewrite-hold.trigger");
        let marker = directory.join("mvx4-completed-mv-target-frozen.marker");
        std::fs::write(&hold, "hold").unwrap();
        let mut session = TestSession::new(
            vec![
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"exact","directory":directory}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = std::sync::Arc::new(session.controls().unwrap());
        let actual = StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (7_u64 << 32) | 1_u64,
                )
                .expect("nonzero fixture session"),
            ),
            1,
        );
        controls
            .pin_statement_input(&input(&session, &nomination(&session, actual, "exact")))
            .unwrap();
        controls
            .nominate_statement(
                &input(&session, &nomination(&session, actual, "exact")),
                &nomination(&session, actual, "exact"),
            )
            .unwrap();
        let target = MvRewriteStatementTarget::new(actual, actual, "exact".into()).unwrap();
        let statements = novarocks_parser::parse("SELECT 1").unwrap();
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("query");
        };
        let other_statements = novarocks_parser::parse("SELECT 2").unwrap();
        let [Statement::Query(other_query)] = other_statements.as_slice() else {
            panic!("query");
        };
        let cancellation =
            novarocks_query_application::cancellation::QueryCancellationSource::new();
        for (catalog, namespace, submitted, selected) in [
            (None, "sales", query, true),
            (Some("other"), "sales", query, true),
            (Some("ice"), "other", query, true),
            (Some("ice"), "sales", other_query, true),
            (Some("ice"), "sales", query, false),
        ] {
            controls
                .rewrite(
                    catalog,
                    namespace,
                    submitted,
                    selected,
                    Some(&target),
                    &cancellation.view(),
                )
                .unwrap();
        }
        assert!(session.role.scope().observations().is_empty());
        assert!(!marker.exists());
        let worker_controls = controls.clone();
        let worker_query = query.clone();
        let worker_target = MvRewriteStatementTarget::new(actual, actual, "exact".into()).unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let cancellation =
                novarocks_query_application::cancellation::QueryCancellationSource::new();
            let result = worker_controls.rewrite(
                Some("ice"),
                "sales",
                &worker_query,
                true,
                Some(&worker_target),
                &cancellation.view(),
            );
            let _ = send.send(result);
        });
        let assertion_deadline = Instant::now() + Duration::from_secs(1);
        while !marker.exists() && !worker.is_finished() && Instant::now() < assertion_deadline {
            std::thread::park_timeout(Duration::from_millis(1));
        }
        let saw_actual_marker = marker.exists();
        let release = std::fs::remove_file(&hold);
        let received = receive.recv_timeout(Duration::from_millis(300));
        if received.is_err() {
            session.close();
        }
        // Join even when marker matching or barrier release failed.
        let joined = worker.join();
        assert!(joined.is_ok(), "rewrite waiter panicked");
        release.unwrap();
        assert!(saw_actual_marker, "exact rewrite did not reach its marker");
        received
            .expect("released rewrite exceeded its watchdog")
            .unwrap();
        std::fs::write(&hold, "second hold").unwrap();
        controls
            .rewrite(
                Some("ice"),
                "sales",
                query,
                true,
                Some(&target),
                &cancellation.view(),
            )
            .unwrap();
        let events = session.role.scope().observations();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event == "claimed")
                .count(),
            1
        );
        assert_eq!(
            events.iter().filter(|event| event.event == "hit").count(),
            1
        );
        assert!(hold.exists(), "replay consumed a second action");
        std::fs::remove_dir_all(directory).unwrap();
    }
    fn expiry_grant(token: &str) -> Value {
        json!({"action":"document-expiry","catalog":"ice","namespace":"sales","mv":"orders","token":token})
    }
    fn expiry_context() -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(2),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024,
            4096,
        )
        .unwrap()
    }
    #[test]
    fn document_expiry_selection_includes_token_and_claim_is_not_renewable() {
        let session = TestSession::new(
            vec![expiry_grant("a"), expiry_grant("b")],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        for (catalog, namespace, mv, token) in [
            ("other", "sales", "orders", "a"),
            ("ice", "other", "orders", "a"),
            ("ice", "sales", "other", "a"),
            ("ice", "sales", "orders", "wrong"),
        ] {
            assert!(
                controls
                    .claim_document_expiry(catalog, namespace, mv, token)
                    .is_err()
            );
        }
        assert!(session.role.scope().observations().is_empty());
        let mut a = controls
            .claim_document_expiry("ice", "sales", "orders", "a")
            .unwrap();
        let b = controls
            .claim_document_expiry("ice", "sales", "orders", "b")
            .unwrap();
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|e| e.event == "hit")
                .count(),
            0
        );
        a.after_planning(&expiry_context()).unwrap();
        drop((a, b));
        let rebuilt = session.controls().unwrap();
        for token in ["a", "b"] {
            assert!(
                rebuilt
                    .claim_document_expiry("ice", "sales", "orders", token)
                    .is_err()
            );
        }
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|e| e.event == "claimed")
                .count(),
            2
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
        let duplicate = TestSession::new(
            vec![expiry_grant("a"), expiry_grant("a")],
            SessionRole::Fe,
            3000,
        );
        assert!(duplicate.controls().is_err());
    }
    #[test]
    fn document_expiry_rejects_malformed_grants_and_closed_launches() {
        for value in [
            json!({"action":"document-expiry","catalog":"ice","namespace":"sales","mv":"orders"}),
            json!({"action":"document-expiry","catalog":"ice","namespace":"sales","mv":"orders","token":"a","directory":"relative"}),
            json!({"action":"document-expiry","catalog":"ice","namespace":"sales","mv":"orders","token":"a","keep_last":2}),
            json!({"action":"document-expiry","catalog":"ice","namespace":"sales","mv":"orders","token":"a\nb"}),
        ] {
            assert!(parse_action(&value).is_err());
        }
        let mut session = TestSession::new(
            vec![expiry_grant("a"), expiry_grant("b")],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let mut claim = controls
            .claim_document_expiry("ice", "sales", "orders", "a")
            .unwrap();
        session.close();
        assert!(
            controls
                .claim_document_expiry("ice", "sales", "orders", "b")
                .is_err()
        );
        assert!(claim.after_planning(&expiry_context()).is_err());
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|e| e.event == "hit")
                .count(),
            0
        );
    }
    #[test]
    fn document_expiry_planned_barrier_precedes_dispatch_and_requires_exact_token() {
        let directory = std::env::temp_dir().join(format!("nrmvexpiry-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        let trigger = directory.join("mv-document-expiry-planned.trigger");
        std::fs::write(&trigger, "token=wrong\n").unwrap();
        let mut grant = expiry_grant("a");
        grant["directory"] = json!(directory);
        let mut session = TestSession::new(vec![grant], SessionRole::Fe, 3000);
        let controls = session.controls().unwrap();
        let mut claim = controls
            .claim_document_expiry("ice", "sales", "orders", "a")
            .unwrap();
        assert!(
            claim
                .after_planning(&expiry_context())
                .unwrap_err()
                .contains("finite grant")
        );
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|e| e.event == "hit")
                .count(),
            0
        );
        // A malformed arm is not a successful planned event. The same claimed
        // attempt can proceed only when the operator corrects its exact arm.
        std::fs::write(&trigger, "token=a\n").unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = claim.after_planning(&expiry_context());
            let _ = send.send(result);
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while !session
            .role
            .scope()
            .observations()
            .iter()
            .any(|e| e.event == "hit")
            && !worker.is_finished()
            && Instant::now() < deadline
        {
            std::thread::park_timeout(Duration::from_millis(1));
        }
        let planned = session
            .role
            .scope()
            .observations()
            .iter()
            .any(|e| e.event == "hit");
        let held = matches!(
            receive.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        );
        let release = std::fs::remove_file(&trigger);
        let result = receive.recv_timeout(Duration::from_millis(300));
        if result.is_err() {
            session.close();
        }
        let joined = worker.join();
        release.unwrap();
        assert!(joined.is_ok());
        assert!(
            planned && held,
            "dispatch became possible before release of the planned barrier"
        );
        result.unwrap().unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }
}
