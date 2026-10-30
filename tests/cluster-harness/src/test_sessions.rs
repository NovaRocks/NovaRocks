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

//! Consumer-owned finite role launch intents. No domain policy is inferred from a scenario name.
use anyhow::{Context, Result, bail, ensure};
use novarocks_failpoint::liveness::RunnerSession;
use novarocks_failpoint::session::{DomainGrant, SessionDeclaration, SessionRole};
use novarocks_failpoint::target_binding::RoleProcessReceipt;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum RoleSlot {
    Frontend,
    Backend(usize),
}
impl RoleSlot {
    fn role(self) -> SessionRole {
        match self {
            Self::Frontend => SessionRole::Fe,
            Self::Backend(_) => SessionRole::Be,
        }
    }
    fn label(self) -> String {
        match self {
            Self::Frontend => "fe".into(),
            Self::Backend(index) => format!("be-{index}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RoleLaunchReference {
    pub session_id: String,
    pub process_receipt: PathBuf,
}

/// Progress of one original reserved launch, never a process-exit receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoleLaunchProgress {
    Reserved,
    Issued,
    RetiredUnissued,
}

struct ActiveLaunch {
    runner: RunnerSession,
    grants: Vec<DomainGrant>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SelectedNativeFault {
    index: usize,
    action: String,
    token: String,
    session_id: String,
    launch_id: String,
    grant_id: String,
    process_id: String,
    root: PathBuf,
}
struct ClosedRoleLaunch {
    session_id: String,
    result: Result<(), String>,
}

struct LaunchIntent {
    reference: RoleLaunchReference,
    declaration_path: PathBuf,
    declaration: SessionDeclaration,
    required_capabilities: Vec<(String, String)>,
    authorized: bool,
    ordinary: bool,
    binary: Option<PathBuf>,
}

// Design: ADR-0174 (docs/adr/ADR-0174-test-capabilities-and-finite-role-authorization.md)
/// One absolute clock and a finite predeclared queue for each actual role launch.
/// Construct this before starting any role, including slots needed after a restart.
pub struct ClusterTestSessionPlan {
    private_directory: Option<tempfile::TempDir>,
    deadline: Instant,
    intents: BTreeMap<RoleSlot, VecDeque<LaunchIntent>>,
    // Only original references move here after leaving the pending queue.
    // Issued includes ordinary launches and does not claim a process started.
    launch_progress: Vec<(RoleSlot, RoleLaunchReference, RoleLaunchProgress)>,
    transports: BTreeMap<String, PathBuf>,
    active: BTreeMap<RoleSlot, ActiveLaunch>,
    selected_native: Mutex<BTreeMap<(usize, String, String), SelectedNativeFault>>,
    closed_roles: BTreeMap<RoleSlot, Vec<ClosedRoleLaunch>>,
    frozen: bool,
}
impl std::fmt::Debug for ClusterTestSessionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterTestSessionPlan")
            .field("roles", &self.intents.keys())
            .field("frozen", &self.frozen)
            .finish_non_exhaustive()
    }
}
impl ClusterTestSessionPlan {
    pub fn new(deadline: Instant) -> Result<Self> {
        ensure!(
            deadline > Instant::now(),
            "test session original deadline expired"
        );
        // macOS Unix sockets have a small path bound; do not use a possibly long worktree path.
        let private_directory = tempfile::Builder::new()
            .prefix("nr-g2-")
            .tempdir_in("/tmp")?;
        Ok(Self {
            private_directory: Some(private_directory),
            deadline,
            intents: BTreeMap::new(),
            launch_progress: Vec::new(),
            transports: BTreeMap::new(),
            active: BTreeMap::new(),
            selected_native: Mutex::new(BTreeMap::new()),
            closed_roles: BTreeMap::new(),
            frozen: false,
        })
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    /// Allocate an ordinary private transport directory before grants are frozen.
    /// Its presence conveys no authority; each product owner still requires its exact grant.
    pub fn reserve_transport(&mut self, name: &str) -> Result<PathBuf> {
        ensure!(!self.frozen, "cannot extend test transports after launch");
        ensure!(
            !name.is_empty()
                && name.len() <= 64
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
            "invalid private transport name"
        );
        if let Some(path) = self.transports.get(name) {
            return Ok(path.clone());
        }
        let path = self
            .private_directory
            .as_ref()
            .expect("live private session directory")
            .path()
            .join(name);
        fs::create_dir(&path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
        }
        self.transports.insert(name.to_owned(), path.clone());
        Ok(path)
    }
    pub(crate) fn transport(&self, name: &str) -> Option<&Path> {
        self.transports.get(name).map(PathBuf::as_path)
    }

    pub fn fresh_domain_secret(&self) -> Result<String> {
        ensure!(
            !self.frozen,
            "cannot allocate domain credentials after launch"
        );
        let key = rcgen::KeyPair::generate()?;
        Ok(format!("{:x}", Sha256::digest(key.serialize_der())))
    }
    /// Reserve all role session identities before any cross-role grant refers to them.
    pub fn reserve_launch(&mut self, role: RoleSlot) -> Result<RoleLaunchReference> {
        ensure!(
            !self.frozen,
            "cannot extend a test session plan after launch"
        );
        ensure!(
            self.intents.values().map(VecDeque::len).sum::<usize>() < 64,
            "test session plan exceeds its launch bound"
        );
        let generation = self.intents.get(&role).map_or(0, VecDeque::len);
        let label = format!("{}-{generation}", role.label());
        let key = rcgen::KeyPair::generate()?;
        let secret = format!("{:x}", Sha256::digest(key.serialize_der()));
        let session_id = format!("{label}-{}", &secret[..16]);
        let socket = self
            .private_directory
            .as_ref()
            .expect("live private session directory")
            .path()
            .join(format!("{label}.sock"));
        let reference = RoleLaunchReference {
            session_id: session_id.clone(),
            process_receipt: socket.with_extension("process.json"),
        };
        let lifetime_millis = u64::try_from(
            self.deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
        )
        .context("test session lifetime overflow")?;
        ensure!(
            lifetime_millis > 0,
            "test session original deadline expired"
        );
        let declaration = SessionDeclaration {
            schema_version: 1,
            session_id,
            launch_id: format!("launch-{label}-{}", &secret[16..32]),
            role: role.role(),
            lifetime_millis,
            liveness_socket: socket,
            handshake_secret: secret,
            domain_grants: vec![],
        };
        self.intents
            .entry(role)
            .or_default()
            .push_back(LaunchIntent {
                reference: reference.clone(),
                declaration_path: self
                    .private_directory
                    .as_ref()
                    .expect("live private session directory")
                    .path()
                    .join(format!("{label}.json")),
                declaration,
                required_capabilities: vec![],
                authorized: false,
                ordinary: false,
                binary: None,
            });
        Ok(reference)
    }
    pub fn authorize_launch(
        &mut self,
        reference: &RoleLaunchReference,
        grants: Vec<DomainGrant>,
        required_capabilities: Vec<(String, String)>,
    ) -> Result<()> {
        ensure!(!self.frozen, "cannot change grants after launch");
        let intent = self
            .intents
            .values_mut()
            .flat_map(|queue| queue.iter_mut())
            .find(|intent| {
                intent.reference.session_id == reference.session_id
                    && intent.reference.process_receipt == reference.process_receipt
            })
            .context("unknown reserved role launch")?;
        ensure!(!intent.authorized, "role launch grants were already frozen");
        let mut declaration = intent.declaration.clone();
        declaration.domain_grants = grants;
        declaration.validate_envelope()?;
        for (owner, capability) in &required_capabilities {
            ensure!(
                !owner.is_empty() && !capability.is_empty(),
                "invalid required build capability"
            );
        }
        intent.declaration = declaration;
        intent.required_capabilities = required_capabilities;
        intent.authorized = true;
        Ok(())
    }
    /// A reserved normal successor carries no product authority. Its actual
    /// binary is still checked before spawn, and the slot cannot be reused.
    pub fn authorize_ordinary_launch(
        &mut self,
        reference: &RoleLaunchReference,
        binary: PathBuf,
    ) -> Result<()> {
        self.authorize_uncontrolled_launch(reference, binary, vec![])
    }
    /// Freeze a launch with no role session. Compiled capabilities are facts,
    /// and required capabilities can bind a variant without granting actions.
    pub fn authorize_uncontrolled_launch(
        &mut self,
        reference: &RoleLaunchReference,
        binary: PathBuf,
        required_capabilities: Vec<(String, String)>,
    ) -> Result<()> {
        ensure!(
            required_capabilities
                .iter()
                .all(|(owner, capability)| !owner.is_empty() && !capability.is_empty()),
            "invalid required build capability"
        );
        self.set_launch_binary(reference, binary)?;
        let intent = self
            .intents
            .values_mut()
            .flat_map(|queue| queue.iter_mut())
            .find(|intent| {
                intent.reference.session_id == reference.session_id
                    && intent.reference.process_receipt == reference.process_receipt
            })
            .context("unknown reserved uncontrolled role launch")?;
        ensure!(!intent.authorized, "role launch was already authorized");
        intent.ordinary = true;
        intent.required_capabilities = required_capabilities;
        intent.authorized = true;
        Ok(())
    }
    pub fn set_launch_binary(
        &mut self,
        reference: &RoleLaunchReference,
        binary: PathBuf,
    ) -> Result<()> {
        ensure!(!self.frozen, "cannot change a role binary after launch");
        let intent = self
            .intents
            .values_mut()
            .flat_map(|queue| queue.iter_mut())
            .find(|intent| {
                intent.reference.session_id == reference.session_id
                    && intent.reference.process_receipt == reference.process_receipt
            })
            .context("unknown reserved role launch binary")?;
        ensure!(
            intent.binary.is_none(),
            "role launch binary was already selected"
        );
        ensure!(binary.is_file(), "selected role launch binary is missing");
        intent.binary = Some(binary);
        Ok(())
    }
    /// Observe only this plan's exact original slot. Unknown references fail.
    pub fn role_launch_progress(
        &self,
        role: RoleSlot,
        reference: &RoleLaunchReference,
    ) -> Result<RoleLaunchProgress> {
        ensure!(self.frozen, "role launch progress requires a frozen plan");
        let matches = |original: &RoleLaunchReference| {
            original.session_id == reference.session_id
                && original.process_receipt == reference.process_receipt
        };
        if self
            .intents
            .get(&role)
            .is_some_and(|queue| queue.iter().any(|intent| matches(&intent.reference)))
        {
            return Ok(RoleLaunchProgress::Reserved);
        }
        self.launch_progress
            .iter()
            .find(|(original_role, original, _)| *original_role == role && matches(original))
            .map(|(_, _, progress)| *progress)
            .context("unknown original role launch progress")
    }

    /// Retire exactly one never-issued intent; preserve all issued role owners.
    /// Private frozen declarations remain evidence, not reusable permissions.
    pub fn retire_unissued_role_launch(
        &mut self,
        role: RoleSlot,
        reference: &RoleLaunchReference,
    ) -> Result<()> {
        ensure!(self.frozen, "role launch retirement requires a frozen plan");
        let queue = self
            .intents
            .get_mut(&role)
            .context("unknown original role")?;
        let index = queue
            .iter()
            .position(|intent| {
                intent.reference.session_id == reference.session_id
                    && intent.reference.process_receipt == reference.process_receipt
            })
            .context("role launch is unknown, already issued or already retired")?;
        let intent = queue
            .remove(index)
            .expect("exact pending role intent exists");
        self.launch_progress.push((
            role,
            intent.reference.clone(),
            RoleLaunchProgress::RetiredUnissued,
        ));
        drop(intent);
        Ok(())
    }

    fn take_next_launch(&mut self, role: RoleSlot) -> Result<LaunchIntent> {
        let intent = self
            .intents
            .get_mut(&role)
            .and_then(VecDeque::pop_front)
            .context("role restart exceeds frozen launch slots")?;
        self.launch_progress
            .push((role, intent.reference.clone(), RoleLaunchProgress::Issued));
        Ok(intent)
    }

    pub(crate) fn binary_for_next_launch(
        &self,
        role: RoleSlot,
        fallback: &Path,
    ) -> Result<PathBuf> {
        match self.intents.get(&role) {
            Some(queue) => Ok(queue
                .front()
                .context("role restart exceeds frozen launch slots")?
                .binary
                .as_deref()
                .unwrap_or(fallback)
                .to_path_buf()),
            None => Ok(fallback.to_path_buf()),
        }
    }
    pub fn declare_launch(
        &mut self,
        role: RoleSlot,
        grants: Vec<DomainGrant>,
        required_capabilities: Vec<(String, String)>,
    ) -> Result<RoleLaunchReference> {
        let reference = self.reserve_launch(role)?;
        self.authorize_launch(&reference, grants, required_capabilities)?;
        Ok(reference)
    }
    /// Non-secret evidence derived from the actual frozen intent data.
    pub fn planned_launch_summaries(&self) -> Value {
        Value::Array(
            self.intents
                .iter()
                .flat_map(|(role, intents)| {
                    intents.iter().map(move |intent|
            serde_json::json!({"role":role.label(),"session_id":intent.reference.session_id,
                "process_receipt":intent.reference.process_receipt,"ordinary":intent.ordinary,
                "binary":intent.binary,"grants":intent.declaration.domain_grants.iter().map(|grant| {
                    let mut summary = serde_json::json!({"domain":grant.domain,"grant_id":grant.grant_id,
                        "action":grant.parameters.get("action")});
                    if let Some(enabled) = grant.parameters.get("hold_split_consumption_until_replay").and_then(Value::as_bool) {
                        summary["hold_split_consumption_until_replay"] = Value::Bool(enabled);
                    }
                    summary
                }).collect::<Vec<_>>()})
        )
                })
                .collect(),
        )
    }
    pub fn validate_selected_intents(&self, cluster_size: usize) -> Result<()> {
        for (role, intents) in &self.intents {
            ensure!(
                !matches!(role, RoleSlot::Backend(index) if *index >= cluster_size),
                "test session backend label outside launched topology"
            );
            for intent in intents {
                ensure!(
                    intent.authorized,
                    "reserved role launch has no accepted finite grants"
                );
                if !intent.ordinary {
                    intent.declaration.validate_envelope()?;
                    ensure!(
                        serde_json::to_vec(&intent.declaration)?.len()
                            <= novarocks_failpoint::session::MAX_DECLARATION_BYTES,
                        "selected role declaration exceeds its bounded input size"
                    );
                }
            }
        }
        Ok(())
    }
    pub(crate) fn validate_and_freeze(&mut self, cluster_size: usize) -> Result<()> {
        ensure!(!self.frozen, "test session plan already launched");
        ensure!(
            !self.intents.is_empty(),
            "test session plan has no explicit role intents"
        );
        ensure!(
            self.deadline > Instant::now(),
            "test session original deadline expired"
        );
        self.validate_selected_intents(cluster_size)?;
        self.frozen = true;
        // Persist all selected inputs before checking binaries or starting any role.
        // Later restarts consume these exact documents; they cannot extend grants.
        for intent in self.intents.values().flat_map(|queue| queue.iter()) {
            if !intent.ordinary {
                intent.declaration.write_private(&intent.declaration_path)?;
            }
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut required =
                options.open(intent.declaration_path.with_extension("capabilities.json"))?;
            std::io::Write::write_all(
                &mut required,
                &serde_json::to_vec(&intent.required_capabilities)?,
            )?;
        }
        Ok(())
    }
    pub(crate) fn prepare_launch(
        &mut self,
        role: RoleSlot,
        binary: &Path,
        evidence: &Path,
        caller_deadline: Instant,
    ) -> Result<Option<PathBuf>> {
        ensure!(
            self.frozen,
            "test session plan must be frozen before launch"
        );
        let deadline = caller_deadline.min(self.deadline);
        ensure!(
            deadline > Instant::now(),
            "role launch original deadline expired"
        );
        self.end_role(role)?;
        let Some(queue) = self.intents.get_mut(&role) else {
            check_binary(binary, false, &[], deadline, evidence)?;
            return Ok(None);
        };
        let intent = queue
            .front()
            .context("role restart exceeds its predeclared launch intents")?;
        check_binary(
            binary,
            !intent.ordinary,
            &intent.required_capabilities,
            deadline,
            evidence,
        )?;
        let intent = self.take_next_launch(role)?;
        if intent.ordinary {
            return Ok(None);
        }
        let grants = intent.declaration.domain_grants.clone();
        // The operation deadline bounds this startup; the role retains the original
        // scenario lifetime for its already-declared subsequent work.
        let runner = RunnerSession::start_until(intent.declaration, self.deadline)?;
        ensure!(
            runner.process_receipt_path() == intent.reference.process_receipt,
            "runner receipt differs from frozen role intent"
        );
        self.active.insert(role, ActiveLaunch { runner, grants });
        Ok(Some(intent.declaration_path))
    }
    pub fn process_receipt(&self, role: RoleSlot) -> Result<Option<RoleProcessReceipt>> {
        self.active
            .get(&role)
            .map(|active| {
                RoleProcessReceipt::read_private(&active.runner.process_receipt_path())
                    .map_err(Into::into)
            })
            .transpose()
    }
    /// Validate this finite MV action against the current authenticated FE
    /// launch. Planned declarations and an observation's self-report are not
    /// substitutes for the active process receipt.
    pub(crate) fn mv_scheduler_outcome_binding(
        &self,
        grant_id: &str,
        catalog: &str,
        namespace: &str,
        mv: &str,
        directory: &Path,
    ) -> Result<RoleProcessReceipt> {
        ensure!(
            Instant::now() < self.deadline,
            "MV scheduler observation original deadline expired"
        );
        let active = self
            .active
            .get(&RoleSlot::Frontend)
            .context("MV scheduler observation has no active FE session")?;
        let mut matching = active
            .grants
            .iter()
            .filter(|grant| grant.domain == "mv" && grant.grant_id == grant_id);
        let grant = matching
            .next()
            .context("MV scheduler observation grant is not active on this FE")?;
        ensure!(
            matching.next().is_none(),
            "ambiguous active MV scheduler observation grant"
        );
        ensure!(
            grant.parameters
                == serde_json::json!({"action":"scheduler-outcome","catalog":catalog,"namespace":namespace,"mv":mv,"directory":directory}),
            "MV scheduler observation does not match its exact declared action/target/directory"
        );
        let process = active
            .runner
            .bound_process()?
            .context("MV scheduler observation precedes authenticated FE Hello")?;
        let receipt = self
            .process_receipt(RoleSlot::Frontend)?
            .context("MV scheduler observation has no current FE process receipt")?;
        ensure!(
            receipt.role == SessionRole::Fe
                && receipt.session_id == active.runner.declaration().session_id
                && receipt.launch_id == active.runner.declaration().launch_id
                && receipt.process_id == process,
            "MV scheduler observation receipt differs from the current authenticated FE launch"
        );
        Ok(receipt)
    }

    pub fn domain_secret(&self, role: RoleSlot, domain: &str, key: &str) -> Result<Option<String>> {
        // Authentication material remains private and is never part of evidence.
        let Some(active) = self.active.get(&role) else {
            return Ok(None);
        };
        let mut selected = None;
        for grant in active.grants.iter().filter(|grant| grant.domain == domain) {
            if let Some(secret) = grant.parameters.get(key).and_then(Value::as_str) {
                ensure!(
                    selected.is_none(),
                    "ambiguous role domain authentication material"
                );
                selected = Some(secret.to_owned());
            }
        }
        Ok(selected)
    }
    fn selected_native_fault(
        &self,
        index: usize,
        action: &str,
        token: &str,
    ) -> Result<(&ActiveLaunch, &DomainGrant)> {
        ensure!(
            Instant::now() < self.deadline,
            "Native action observation original deadline expired"
        );
        let active = self
            .active
            .get(&RoleSlot::Backend(index))
            .context("selected Native action has no active backend session")?;
        let mut matching = active.grants.iter().filter(|grant| {
            grant.domain == "native"
                && grant.parameters.get("action").and_then(Value::as_str) == Some(action)
                && grant
                    .parameters
                    .get("statement_run_token")
                    .and_then(Value::as_str)
                    == Some(token)
                && grant
                    .parameters
                    .get("backend_index")
                    .and_then(Value::as_u64)
                    == Some(index as u64)
        });
        let grant = matching
            .next()
            .context("Native action was not declared for this exact target")?;
        ensure!(matching.next().is_none(), "ambiguous Native action target");
        ensure!(
            grant
                .parameters
                .get("target_session_id")
                .and_then(Value::as_str)
                == Some(active.runner.declaration().session_id.as_str()),
            "Native action names another backend session"
        );
        Ok((active, grant))
    }
    pub(crate) fn validate_native_fault_target(
        &self,
        index: usize,
        action: &str,
        token: &str,
    ) -> Result<()> {
        let (active, grant) = self.selected_native_fault(index, action, token)?;
        let process_id = active
            .runner
            .bound_process()?
            .context("Native action selection precedes authenticated role Hello")?;
        let receipt = RoleProcessReceipt::read_private(&active.runner.process_receipt_path())?;
        ensure!(
            receipt.session_id == active.runner.declaration().session_id
                && receipt.launch_id == active.runner.declaration().launch_id
                && receipt.process_id == process_id,
            "Native action selection differs from authenticated role receipt"
        );
        receipt.backend_process()?;
        let root = PathBuf::from(
            grant
                .parameters
                .get("root")
                .and_then(Value::as_str)
                .context("Native action lacks its private transport")?,
        );
        ensure!(
            root.is_absolute(),
            "Native action transport is not absolute"
        );
        let selected = SelectedNativeFault {
            index,
            action: action.into(),
            token: token.into(),
            session_id: receipt.session_id,
            launch_id: receipt.launch_id,
            process_id,
            grant_id: grant.grant_id.clone(),
            root,
        };
        let key = (index, action.to_owned(), token.to_owned());
        let mut selections = self
            .selected_native
            .lock()
            .map_err(|_| anyhow::anyhow!("Native action selection lock failed"))?;
        if let Some(original) = selections.get(&key) {
            ensure!(
                original == &selected,
                "Native action selection cannot redirect to a successor role"
            );
        } else {
            ensure!(
                selections.len() < 4096,
                "Native action selections exceed the finite launch/grant bound"
            );
            selections.insert(key, selected);
        }
        Ok(())
    }
    pub(crate) fn activate_deferred_native_fault(
        &self,
        index: usize,
        action: &str,
        token: &str,
    ) -> Result<()> {
        ensure!(
            action == "task-execution-failure-after-visible-prefix",
            "Native action has no deferred activation contract"
        );
        let selected = self.original_native_selection(index, action, token)?;
        // Recheck the authenticated original role; no retargeting to a successor.
        self.validate_native_fault_target(index, action, token)?;
        let path = selected.root.join(format!(
            "be-{index}.native-fault-{action}.{token}.execution.json"
        ));
        ensure!(
            read_private_action_json(&path.with_extension("retired.json"))?.is_none()
                && read_private_action_json(&path.with_extension("end.json"))?.is_none(),
            "deferred action was already ended"
        );
        let value = read_private_action_json(&path)?
            .context("deferred action has no real execution binding")?;
        let binding: novarocks_failpoint::target_binding::StatementExecutionBinding =
            serde_json::from_value(value.clone())?;
        binding.validate()?;
        ensure!(
            binding.target_session_id == selected.session_id
                && binding.statement_run_token == selected.token
                && binding.backend_index == selected.index
                && binding.backend_process_id == selected.process_id,
            "deferred activation differs from original selected process/statement"
        );
        publish_private_action_json(&path.with_extension("enable.json"), &value)
    }

    pub(crate) fn native_fault_hit(&self, index: usize, action: &str, token: &str) -> Result<bool> {
        let selected = self.original_native_selection(index, action, token)?;
        let root = &selected.root;
        let binding_path = root.join(format!(
            "be-{index}.native-fault-{action}.{token}.execution.json"
        ));
        let Some(hit) = read_private_action_json(&binding_path.with_extension("hit.json"))? else {
            return Ok(false);
        };
        let binding = read_private_action_json(&binding_path)?
            .context("Native action hit lacks its execution binding")?;
        let object = hit
            .as_object()
            .context("Native action hit must be an object")?;
        ensure!(
            object.len() == 7
                && hit["schema_version"] == 1
                && hit["event"] == "hit"
                && hit["domain"] == "native"
                && hit["action"] == action
                && hit["grant_id"] == selected.grant_id
                && hit["launch_id"] == selected.launch_id
                && hit["binding"] == binding,
            "Native action hit conflicts with its selected grant or execution"
        );
        let binding: novarocks_failpoint::target_binding::StatementExecutionBinding =
            serde_json::from_value(binding).context("decode Native action execution binding")?;
        binding.validate()?;
        ensure!(
            binding.target_session_id == selected.session_id
                && binding.statement_run_token == token
                && binding.backend_index == index
                && selected.process_id == binding.backend_process_id,
            "Native action hit names another role process or request"
        );
        ensure!(
            Instant::now() < self.deadline,
            "Native action observation completed after original deadline"
        );
        Ok(true)
    }
    pub(crate) fn selected_native_observation_binding(
        &self,
        index: usize,
        action: &str,
        token: &str,
        attempt: Option<u64>,
    ) -> Result<Option<novarocks_failpoint::target_binding::StatementExecutionBinding>> {
        ensure!(
            Instant::now() < self.deadline,
            "Native observation original deadline expired"
        );
        let active = self
            .active
            .get(&RoleSlot::Backend(index))
            .context("selected observation has no active backend session")?;
        let mut matching = active.grants.iter().filter(|grant| {
            grant.domain == "native-observation"
                && grant.parameters.get("action").and_then(Value::as_str) == Some(action)
                && grant
                    .parameters
                    .get("statement_run_token")
                    .and_then(Value::as_str)
                    == Some(token)
                && grant
                    .parameters
                    .get("backend_index")
                    .and_then(Value::as_u64)
                    == Some(index as u64)
                && match attempt {
                    Some(id) => {
                        grant.parameters.get("attempt_id").and_then(Value::as_u64) == Some(id)
                    }
                    None => !grant
                        .parameters
                        .as_object()
                        .is_some_and(|object| object.contains_key("attempt_id")),
                }
        });
        let grant = matching
            .next()
            .context("observation was not declared for this exact target")?;
        ensure!(
            matching.next().is_none(),
            "ambiguous Native observation target"
        );
        ensure!(
            grant
                .parameters
                .get("target_session_id")
                .and_then(Value::as_str)
                == Some(active.runner.declaration().session_id.as_str()),
            "observation names another backend session"
        );
        let root = Path::new(
            grant
                .parameters
                .get("root")
                .and_then(Value::as_str)
                .context("observation lacks its private transport")?,
        );
        ensure!(root.is_absolute(), "observation transport is not absolute");
        let name = match attempt {
            Some(id) => format!("{token}-be{index}-attempt{id}.json"),
            None => format!("{token}-be{index}.json"),
        };
        let Some(value) = read_private_action_json(&root.join(name))? else {
            return Ok(None);
        };
        let binding: novarocks_failpoint::target_binding::StatementExecutionBinding =
            serde_json::from_value(value)?;
        binding.validate()?;
        ensure!(
            binding.target_session_id == active.runner.declaration().session_id
                && binding.statement_run_token == token
                && binding.backend_index == index
                && active.runner.bound_process()?.as_deref()
                    == Some(binding.backend_process_id.as_str())
                && !attempt.is_some_and(|id| binding.attempt_id != id),
            "observation binding names another process/request/attempt"
        );
        ensure!(
            Instant::now() < self.deadline,
            "Native observation read completed after original deadline"
        );
        Ok(Some(binding))
    }
    fn original_native_selection(
        &self,
        index: usize,
        action: &str,
        token: &str,
    ) -> Result<SelectedNativeFault> {
        self.selected_native
            .lock()
            .map_err(|_| anyhow::anyhow!("Native action selection lock failed"))?
            .get(&(index, action.to_owned(), token.to_owned()))
            .cloned()
            .context("Native action was never selected from its original finite role grant")
    }
    pub(crate) fn end_selected_native_fault(
        &self,
        index: usize,
        action: &str,
        token: &str,
    ) -> Result<()> {
        let selected = self.original_native_selection(index, action, token)?;
        let path = selected.root.join(format!(
            "be-{index}.native-fault-{action}.{token}.execution.json"
        ));
        let binding = read_private_action_json(&path)?
            .map(|value| -> Result<_> {
                let binding: novarocks_failpoint::target_binding::StatementExecutionBinding =
                    serde_json::from_value(value.clone())?;
                binding.validate()?;
                ensure!(
                    binding.target_session_id == selected.session_id
                        && binding.statement_run_token == selected.token
                        && binding.backend_index == selected.index
                        && binding.backend_process_id == selected.process_id,
                    "Native action end names another original session/process/request"
                );
                Ok(value)
            })
            .transpose()?;
        // Revocation identifies the already selected finite grant. It contains
        // no invented query identity and never claims query/task actual exit.
        let retirement = serde_json::json!({"schema_version":1,"event":"grant-retired","domain":"native",
            "action":selected.action,"target_session_id":selected.session_id,"target_launch_id":selected.launch_id,
            "statement_run_token":selected.token,"backend_index":selected.index,"backend_process_id":selected.process_id});
        publish_private_action_json(&path.with_extension("retired.json"), &retirement)?;
        if let Some(binding) = binding {
            publish_private_action_json(&path.with_extension("end.json"), &binding)?;
        }
        let mut failures = Vec::new();
        if let Some(closed) = self
            .closed_roles
            .get(&RoleSlot::Backend(index))
            .and_then(|roles| {
                roles
                    .iter()
                    .find(|closed| closed.session_id == selected.session_id)
            })
            && let Err(error) = &closed.result
        {
            failures.push(format!("original role controller cleanup failed: {error}"));
        }
        if Instant::now() >= self.deadline {
            failures.push("Native action retirement completed after original deadline".into());
        }
        if !failures.is_empty() {
            bail!(failures.join("; "));
        }
        Ok(())
    }
    pub(crate) fn end_role(&mut self, role: RoleSlot) -> Result<()> {
        if let Some(mut active) = self.active.remove(&role) {
            let session_id = active.runner.declaration().session_id.clone();
            let result = active
                .runner
                .end_and_join()
                .map_err(|error| format!("join role test-session liveness owner: {error}"));
            self.closed_roles
                .entry(role)
                .or_default()
                .push(ClosedRoleLaunch {
                    session_id,
                    result: result.clone(),
                });
            if let Err(error) = result {
                bail!(error);
            }
        } else if let Some(closed) = self.closed_roles.get(&role).and_then(|roles| roles.last())
            && let Err(error) = &closed.result
        {
            bail!(error.clone());
        }
        Ok(())
    }
    pub(crate) fn end_and_join(&mut self) -> Result<()> {
        let mut errors = Vec::new();
        let roles = self.active.keys().copied().collect::<Vec<_>>();
        for role in roles {
            if let Err(error) = self.end_role(role) {
                errors.push(format!("{}: {error:#}", role.label()));
            }
        }
        for (role, closed) in &self.closed_roles {
            for launch in closed {
                if let Err(error) = &launch.result {
                    errors.push(format!(
                        "{} session={}: {error}",
                        role.label(),
                        launch.session_id
                    ));
                }
            }
        }
        if !errors.is_empty() {
            bail!(errors.join("; "));
        }
        Ok(())
    }
}
fn read_private_action_json(path: &Path) -> Result<Option<Value>> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("open private Native action evidence"),
    };
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file() && metadata.len() <= 4096,
        "Native action evidence must be a bounded regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "Native action evidence is not private"
        );
    }
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= 4096,
        "Native action evidence exceeds byte bound"
    );
    Ok(Some(
        serde_json::from_slice(&bytes).context("decode Native action evidence")?,
    ))
}
impl Drop for ClusterTestSessionPlan {
    fn drop(&mut self) {
        if let Err(error) = self.end_and_join() {
            eprintln!("test session plan cleanup failed: {error:#}");
        }
        if let Some(directory) = self.private_directory.take() {
            let path = directory.keep();
            eprintln!(
                "retained private role test-session inputs and receipts at {}; local experiment owner retains these materials",
                path.display()
            );
        }
    }
}

fn publish_private_action_json(path: &Path, value: &Value) -> Result<()> {
    use std::io::Write;
    let bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() <= 4096, "Native action end exceeds byte bound");
    let mut staging =
        tempfile::NamedTempFile::new_in(path.parent().context("Native action end has no parent")?)?;
    staging.write_all(&bytes)?;
    staging.as_file().sync_all()?;
    match staging.persist_noclobber(path) {
        Ok(_) => Ok(()),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            ensure!(
                read_private_action_json(path)?.as_ref() == Some(value),
                "Native action end conflicts with an existing record"
            );
            Ok(())
        }
        Err(error) => Err(error.error).context("publish complete Native action end"),
    }
}
struct BuildInfoChild {
    process: std::process::Child,
    exited: bool,
}
impl Drop for BuildInfoChild {
    fn drop(&mut self) {
        if !self.exited {
            let killed = self.process.kill();
            let waited = self.process.wait();
            if waited.is_err() {
                eprintln!(
                    "build-info child cleanup failed: kill={killed:?}; actual_wait={waited:?}"
                );
            }
        }
    }
}

/// Read the actual executable before config/startup. Paths and source revisions are not capability facts.
pub fn check_binary(
    binary: &Path,
    controlled: bool,
    required: &[(String, String)],
    deadline: Instant,
    evidence: &Path,
) -> Result<Value> {
    ensure!(deadline > Instant::now(), "build-info deadline expired");
    let directory = evidence
        .parent()
        .context("build-info evidence has no parent")?;
    fs::create_dir_all(directory)?;
    let stdout = tempfile::NamedTempFile::new_in(directory)?;
    let stderr = tempfile::NamedTempFile::new_in(directory)?;
    let process = Command::new(binary)
        .arg("--build-info")
        .stdout(Stdio::from(stdout.reopen()?))
        .stderr(Stdio::from(stderr.reopen()?))
        .spawn()
        .with_context(|| format!("read actual binary build-info {}", binary.display()))?;
    let mut child = BuildInfoChild {
        process,
        exited: false,
    };
    let (status_result, primary) = loop {
        let output_state = stdout.as_file().metadata().and_then(|out| {
            stderr
                .as_file()
                .metadata()
                .map(|err| out.len() > 65536 || err.len() > 65536)
        });
        let (too_large, output_error) = match output_state {
            Ok(large) => (large, None),
            Err(error) => (false, Some(format!("observe build-info output: {error}"))),
        };
        match child.process.try_wait() {
            Ok(Some(status)) => {
                child.exited = true;
                break (
                    Ok(status),
                    output_error.or_else(|| {
                        too_large.then(|| "build-info exceeded its output bound".into())
                    }),
                );
            }
            Ok(None) if Instant::now() < deadline && !too_large && output_error.is_none() => {
                std::thread::sleep(
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(20)),
                )
            }
            result => {
                let primary = match result {
                    _ if output_error.is_some() => output_error.expect("checked output error"),
                    Err(error) => format!("observe build-info child: {error}"),
                    _ if too_large => "build-info exceeded its output bound".into(),
                    _ => "build-info exceeded original deadline".into(),
                };
                let kill = child.process.kill();
                let actual_wait = child.process.wait();
                child.exited = actual_wait.is_ok();
                break (actual_wait, Some(format!("{primary}; kill={kill:?}")));
            }
        }
    };
    publish_build_info_capture(
        binary,
        controlled,
        required,
        evidence,
        BuildInfoCapture {
            stdout,
            stderr,
            status_result,
            primary,
        },
    )
}

struct BuildInfoCapture {
    stdout: tempfile::NamedTempFile,
    stderr: tempfile::NamedTempFile,
    status_result: std::io::Result<std::process::ExitStatus>,
    primary: Option<String>,
}

// The caller retains the process owner and its original deadline. Publishing
// captured streams and the observed wait outcome precedes semantic admission.
fn publish_build_info_capture(
    binary: &Path,
    controlled: bool,
    required: &[(String, String)],
    evidence: &Path,
    capture: BuildInfoCapture,
) -> Result<Value> {
    let BuildInfoCapture {
        stdout,
        stderr,
        status_result,
        primary,
    } = capture;
    // Keep the actual stdout/stderr on timeout, nonzero exit and capability rejection.
    // persist_noclobber preserves old attempts, including a failed binary's output.
    stdout
        .persist_noclobber(evidence)
        .context("preserve raw build-info output")?;
    stderr
        .persist_noclobber(evidence.with_extension("stderr.log"))
        .context("preserve raw build-info stderr")?;
    let receipt = serde_json::json!({"binary":binary,"argv":["--build-info"],"actual_exit":status_result.as_ref().ok().map(ToString::to_string),"wait_failure":status_result.as_ref().err().map(ToString::to_string),"primary_failure":primary});
    let mut receipt_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(evidence.with_extension("exit.json"))?;
    std::io::Write::write_all(&mut receipt_file, &serde_json::to_vec_pretty(&receipt)?)?;
    let status = match status_result {
        Ok(status) => status,
        Err(wait) => bail!(
            "{}; actual build-info wait failed: {wait}",
            primary.as_deref().unwrap_or("build-info child failed")
        ),
    };
    if let Some(primary) = primary {
        bail!("{primary}; actual_exit={status}");
    }
    ensure!(
        status.success(),
        "build-info child failed with actual status {status}"
    );
    let mut bytes = Vec::new();
    File::open(evidence)?.take(65537).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 65536, "build-info exceeds byte bound");
    let value: Value = serde_json::from_slice(&bytes).context("decode native build-info")?;
    validate_binary_info(&value, controlled, required)?;
    Ok(value)
}
fn validate_binary_info(
    value: &Value,
    controlled: bool,
    required: &[(String, String)],
) -> Result<()> {
    ensure!(
        value["schema_version"] == 1 && value["package"] == "novarocks-server",
        "build-info is not the supported Server identity"
    );
    ensure!(
        matches!(
            value["build_kind"].as_str(),
            Some("normal-build" | "test-controls")
        ),
        "binary omitted a supported actual build kind"
    );
    let owners = value["capabilities"]
        .as_array()
        .context("build-info omitted owner capabilities")?;
    let mut by_owner = BTreeMap::new();
    for owner in owners {
        let name = owner["owner"]
            .as_str()
            .context("build-info owner is invalid")?;
        let capabilities = owner["compiled_capabilities"]
            .as_array()
            .context("build-info capability list is invalid")?;
        ensure!(
            by_owner.insert(name, capabilities).is_none(),
            "build-info repeats an owner"
        );
        ensure!(
            capabilities.iter().all(Value::is_string),
            "build-info contains an invalid capability"
        );
    }
    if controlled {
        ensure!(
            by_owner
                .get("novarocks-server")
                .is_some_and(|caps| caps.iter().any(|cap| cap == "test-controls")),
            "controlled executable lacks Server test-controls"
        );
    }
    for (owner, capability) in required {
        ensure!(
            by_owner
                .get(owner.as_str())
                .is_some_and(|caps| caps.iter().any(|cap| cap == capability)),
            "executable lacks required {owner}/{capability}"
        );
    }
    // Unknown profile/compiler facts are retained as unknown. No release certification is inferred.
    ensure!(
        value["actual_cfg"].is_object() && value["explicitly_unknown_settings"].is_array(),
        "build-info omitted actual/unknown build facts"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn info(kind: &str, caps: Vec<&str>) -> Value {
        serde_json::json!({"schema_version":1,"package":"novarocks-server","build_kind":kind,"capabilities":[{"owner":"novarocks-server","compiled_capabilities":caps}],"actual_cfg":{},"declared_profile":null,"explicitly_unknown_settings":["final_lto"]})
    }
    fn reserve_native_role(
        plan: &mut ClusterTestSessionPlan,
        index: usize,
        tokens: &[&str],
    ) -> RoleLaunchReference {
        let root = plan.reserve_transport("native-retirement-test").unwrap();
        let reference = plan.reserve_launch(RoleSlot::Backend(index)).unwrap();
        let grants=tokens.iter().enumerate().map(|(ordinal,token)|DomainGrant {
            domain:"native".into(),grant_id:format!("be-{index}-{ordinal}"),
            parameters:serde_json::json!({"action":"create-task-ack-drop","root":root,"backend_index":index,
                "statement_run_token":token,"target_session_id":reference.session_id}),
        }).collect();
        plan.authorize_launch(&reference, grants, vec![]).unwrap();
        reference
    }
    fn start_role_hello(
        plan: &mut ClusterTestSessionPlan,
        index: usize,
    ) -> std::os::unix::net::UnixStream {
        start_role_hello_at(plan, RoleSlot::Backend(index))
    }
    fn start_role_hello_at(
        plan: &mut ClusterTestSessionPlan,
        role: RoleSlot,
    ) -> std::os::unix::net::UnixStream {
        use std::io::Write;
        let intent = plan.take_next_launch(role).unwrap();
        let declaration = intent.declaration;
        let runner = RunnerSession::start_until(declaration.clone(), plan.deadline).unwrap();
        let mut peer =
            std::os::unix::net::UnixStream::connect(&declaration.liveness_socket).unwrap();
        peer.set_write_timeout(Some(
            plan.deadline.saturating_duration_since(Instant::now()),
        ))
        .unwrap();
        let body=serde_json::to_vec(&serde_json::json!({"kind":"hello","session_id":declaration.session_id,
            "launch_id":declaration.launch_id,"role":declaration.role,
            "process_id":match role { RoleSlot::Frontend => novarocks_types::FrontendProcessId::new_v7().to_string(), RoleSlot::Backend(_) => novarocks_types::BackendProcessId::new_v7().to_string() },"handshake_secret":declaration.handshake_secret})).unwrap();
        peer.write_all(&(body.len() as u32).to_be_bytes()).unwrap();
        peer.write_all(&body).unwrap();
        while runner.bound_process().unwrap().is_none() {
            assert!(Instant::now() < plan.deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        plan.active.insert(
            role,
            ActiveLaunch {
                runner,
                grants: declaration.domain_grants,
            },
        );
        peer
    }
    #[test]
    fn mv_scheduler_binding_requires_current_authenticated_fe_and_exact_grant() {
        let mut plan =
            ClusterTestSessionPlan::new(Instant::now() + Duration::from_secs(10)).unwrap();
        let directory = plan.reserve_transport("mv-outcome-test").unwrap();
        let reference = plan.reserve_launch(RoleSlot::Frontend).unwrap();
        plan.authorize_launch(&reference, vec![DomainGrant { domain:"mv".into(), grant_id:"outcome-a".into(),
            parameters:serde_json::json!({"action":"scheduler-outcome","catalog":"ice","namespace":"ns","mv":"orders","directory":directory}) }], vec![]).unwrap();
        // Merely planning a matching grant is insufficient.
        assert!(
            plan.mv_scheduler_outcome_binding("outcome-a", "ice", "ns", "orders", &directory)
                .is_err()
        );
        let peer = start_role_hello_at(&mut plan, RoleSlot::Frontend);
        let receipt = plan
            .mv_scheduler_outcome_binding("outcome-a", "ice", "ns", "orders", &directory)
            .unwrap();
        assert_eq!(receipt.session_id, reference.session_id);
        assert_eq!(
            receipt,
            plan.process_receipt(RoleSlot::Frontend).unwrap().unwrap()
        );
        for (grant, catalog, namespace, mv, path) in [
            ("different", "ice", "ns", "orders", directory.clone()),
            ("outcome-a", "other", "ns", "orders", directory.clone()),
            ("outcome-a", "ice", "other", "orders", directory.clone()),
            ("outcome-a", "ice", "ns", "other", directory.clone()),
            ("outcome-a", "ice", "ns", "orders", directory.join("other")),
        ] {
            assert!(
                plan.mv_scheduler_outcome_binding(grant, catalog, namespace, mv, &path)
                    .is_err()
            );
        }
        plan.active.get_mut(&RoleSlot::Frontend).unwrap().grants[0].domain =
            "another-domain".into();
        assert!(
            plan.mv_scheduler_outcome_binding("outcome-a", "ice", "ns", "orders", &directory)
                .is_err()
        );
        plan.active.get_mut(&RoleSlot::Frontend).unwrap().grants[0].domain = "mv".into();
        plan.active.get_mut(&RoleSlot::Frontend).unwrap().grants[0].parameters["action"] =
            serde_json::json!("scheduler-hold");
        assert!(
            plan.mv_scheduler_outcome_binding("outcome-a", "ice", "ns", "orders", &directory)
                .is_err()
        );
        plan.end_role(RoleSlot::Frontend).unwrap();
        assert!(
            plan.mv_scheduler_outcome_binding("outcome-a", "ice", "ns", "orders", &directory)
                .is_err()
        );
        drop(peer);
    }

    #[test]
    fn three_selected_permissions_with_only_two_actual_bindings_retire_without_fake_execution() {
        let mut plan =
            ClusterTestSessionPlan::new(Instant::now() + Duration::from_secs(5)).unwrap();
        for index in 0..3 {
            reserve_native_role(&mut plan, index, &["statement-a", "statement-b"]);
        }
        let peers = (0..3)
            .map(|index| start_role_hello(&mut plan, index))
            .collect::<Vec<_>>();
        for index in 0..3 {
            plan.validate_native_fault_target(index, "create-task-ack-drop", "statement-a")
                .unwrap();
            let selected = plan
                .original_native_selection(index, "create-task-ack-drop", "statement-a")
                .unwrap();
            let path = selected.root.join(format!(
                "be-{index}.native-fault-create-task-ack-drop.statement-a.execution.json"
            ));
            if index < 2 {
                let binding = novarocks_failpoint::target_binding::StatementExecutionBinding::new(
                    selected.session_id.clone(),
                    "statement-a".into(),
                    index,
                    selected.process_id.parse().unwrap(),
                    novarocks_types::QueryExecutionId::new(
                        novarocks_types::QueryId::new(7, 9),
                        novarocks_types::AttemptId::new(1).unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap();
                binding.write_private(&path).unwrap();
            }
            plan.end_selected_native_fault(index, "create-task-ack-drop", "statement-a")
                .unwrap();
            plan.end_selected_native_fault(index, "create-task-ack-drop", "statement-a")
                .unwrap();
            let retired = read_private_action_json(&path.with_extension("retired.json"))
                .unwrap()
                .unwrap();
            assert!(retired.get("query_id_hi").is_none() && retired.get("attempt_id").is_none());
            assert!(!path.with_extension("hit.json").exists());
            assert_eq!(path.exists(), index < 2);
            assert_eq!(path.with_extension("end.json").exists(), index < 2);
            plan.validate_native_fault_target(index, "create-task-ack-drop", "statement-b")
                .unwrap();
            assert!(
                plan.end_selected_native_fault(index, "create-task-ack-drop", "unknown")
                    .is_err()
            );
        }
        plan.end_and_join().unwrap();
        drop(peers);
    }
    #[test]
    fn original_selection_retirement_never_targets_the_successor_launch() {
        let mut plan =
            ClusterTestSessionPlan::new(Instant::now() + Duration::from_secs(5)).unwrap();
        let original = reserve_native_role(&mut plan, 0, &["statement-a"]);
        let successor = reserve_native_role(&mut plan, 0, &["statement-b"]);
        let old_peer = start_role_hello(&mut plan, 0);
        plan.validate_native_fault_target(0, "create-task-ack-drop", "statement-a")
            .unwrap();
        let old_selection = plan
            .original_native_selection(0, "create-task-ack-drop", "statement-a")
            .unwrap();
        plan.end_role(RoleSlot::Backend(0)).unwrap();
        drop(old_peer);
        let new_peer = start_role_hello(&mut plan, 0);
        plan.validate_native_fault_target(0, "create-task-ack-drop", "statement-b")
            .unwrap();
        plan.end_selected_native_fault(0, "create-task-ack-drop", "statement-a")
            .unwrap();
        let current = plan.active.get(&RoleSlot::Backend(0)).unwrap();
        assert_eq!(
            current.runner.declaration().session_id,
            successor.session_id
        );
        assert_ne!(
            current.runner.bound_process().unwrap().unwrap(),
            old_selection.process_id
        );
        assert_eq!(old_selection.session_id, original.session_id);
        let new_selection = plan
            .original_native_selection(0, "create-task-ack-drop", "statement-b")
            .unwrap();
        assert!(
            !new_selection
                .root
                .join("be-0.native-fault-create-task-ack-drop.statement-b.execution.retired.json")
                .exists()
        );
        plan.end_and_join().unwrap();
        drop(new_peer);
    }
    #[test]
    fn first_role_controller_cleanup_error_remains_failed_after_actual_join() {
        let mut plan =
            ClusterTestSessionPlan::new(Instant::now() + Duration::from_secs(5)).unwrap();
        reserve_native_role(&mut plan, 0, &["statement-a"]);
        let peer = start_role_hello(&mut plan, 0);
        plan.validate_native_fault_target(0, "create-task-ack-drop", "statement-a")
            .unwrap();
        let socket = plan.active[&RoleSlot::Backend(0)]
            .runner
            .declaration()
            .liveness_socket
            .clone();
        fs::remove_file(&socket).unwrap();
        fs::write(&socket, b"owned replacement is not the original socket").unwrap();
        let first = plan.end_role(RoleSlot::Backend(0)).unwrap_err().to_string();
        assert!(first.contains("replaced"));
        assert_eq!(
            plan.end_role(RoleSlot::Backend(0)).unwrap_err().to_string(),
            first
        );
        assert!(
            plan.end_selected_native_fault(0, "create-task-ack-drop", "statement-a")
                .is_err()
        );
        assert!(plan.end_and_join().is_err());
        assert!(plan.active.is_empty());
        drop(peer);
    }

    struct OriginalLaunchOwner(ClusterTestSessionPlan);

    impl crate::ServerHandle for OriginalLaunchOwner {
        fn target_host(&self) -> Option<&str> {
            None
        }
        fn target_port(&self) -> Option<u16> {
            None
        }
        fn role_launch_progress(
            &self,
            role: RoleSlot,
            reference: &RoleLaunchReference,
        ) -> Result<RoleLaunchProgress> {
            self.0.role_launch_progress(role, reference)
        }
        fn retire_unissued_role_launch(
            &mut self,
            role: RoleSlot,
            reference: &RoleLaunchReference,
        ) -> Result<()> {
            self.0.retire_unissued_role_launch(role, reference)
        }
    }

    #[test]
    fn skipped_launch_cannot_shift_a_later_original_grant_or_retire_its_issued_owner() {
        let mut plan =
            ClusterTestSessionPlan::new(Instant::now() + Duration::from_secs(10)).unwrap();
        let directory = plan.reserve_transport("skipped-launch").unwrap();
        let grant = |name: &str| DomainGrant {
            domain: "mv".into(),
            grant_id: format!("outcome-{name}"),
            parameters: serde_json::json!({"action":"scheduler-outcome","catalog":"ice","namespace":"ns","mv":name,"directory":directory}),
        };
        let a = plan
            .declare_launch(RoleSlot::Frontend, vec![grant("a")], vec![])
            .unwrap();
        let b = plan
            .declare_launch(RoleSlot::Frontend, vec![grant("b")], vec![])
            .unwrap();
        plan.validate_and_freeze(1).unwrap();
        let foreign = RoleLaunchReference {
            session_id: a.session_id.clone(),
            process_receipt: directory.join("foreign.process.json"),
        };
        assert!(
            plan.retire_unissued_role_launch(RoleSlot::Frontend, &foreign)
                .is_err()
        );
        assert!(
            plan.retire_unissued_role_launch(RoleSlot::Backend(0), &a)
                .is_err()
        );
        assert_eq!(
            plan.role_launch_progress(RoleSlot::Frontend, &a).unwrap(),
            RoleLaunchProgress::Reserved
        );
        assert!(!a.process_receipt.exists());
        assert!(!b.process_receipt.exists());
        let probe = tempfile::TempDir::new().unwrap();
        let missing_binary = probe.path().join("never-created-server");
        assert!(
            plan.prepare_launch(
                RoleSlot::Frontend,
                &missing_binary,
                &probe.path().join("failed-build-info.json"),
                plan.deadline,
            )
            .is_err()
        );
        assert_eq!(
            plan.role_launch_progress(RoleSlot::Frontend, &a).unwrap(),
            RoleLaunchProgress::Reserved
        );
        assert!(!a.process_receipt.exists());
        // The runner's common step-exit author consults this exact original owner.
        let mut owner = OriginalLaunchOwner(plan);
        assert!(
            crate::ServerHandle::retire_unissued_role_launch_if_reserved(
                &mut owner,
                RoleSlot::Frontend,
                &foreign,
            )
            .is_err()
        );
        assert!(
            crate::ServerHandle::retire_unissued_role_launch_if_reserved(
                &mut owner,
                RoleSlot::Backend(0),
                &a,
            )
            .is_err()
        );
        assert!(
            crate::ServerHandle::retire_unissued_role_launch_if_reserved(
                &mut owner,
                RoleSlot::Frontend,
                &a,
            )
            .unwrap()
        );
        let plan = &mut owner.0;
        assert_eq!(
            plan.role_launch_progress(RoleSlot::Frontend, &a).unwrap(),
            RoleLaunchProgress::RetiredUnissued
        );
        assert!(
            plan.retire_unissued_role_launch(RoleSlot::Frontend, &a)
                .is_err()
        );
        assert_eq!(
            plan.role_launch_progress(RoleSlot::Frontend, &b).unwrap(),
            RoleLaunchProgress::Reserved
        );
        assert!(
            plan.mv_scheduler_outcome_binding("outcome-b", "ice", "ns", "b", &directory)
                .is_err()
        );
        // The original next-role author binds B, never A's skipped declaration.
        let peer = start_role_hello_at(plan, RoleSlot::Frontend);
        let receipt = plan
            .mv_scheduler_outcome_binding("outcome-b", "ice", "ns", "b", &directory)
            .unwrap();
        assert_eq!(receipt.session_id, b.session_id);
        assert_eq!(
            plan.role_launch_progress(RoleSlot::Frontend, &b).unwrap(),
            RoleLaunchProgress::Issued
        );
        assert!(
            plan.mv_scheduler_outcome_binding("outcome-a", "ice", "ns", "a", &directory)
                .is_err()
        );
        assert!(
            plan.retire_unissued_role_launch(RoleSlot::Frontend, &b)
                .is_err()
        );
        assert_eq!(
            plan.process_receipt(RoleSlot::Frontend).unwrap().unwrap(),
            receipt
        );
        assert_eq!(
            plan.mv_scheduler_outcome_binding("outcome-b", "ice", "ns", "b", &directory)
                .unwrap(),
            receipt
        );
        plan.end_role(RoleSlot::Frontend).unwrap();
        assert!(
            plan.retire_unissued_role_launch(RoleSlot::Frontend, &b)
                .is_err()
        );
        assert!(
            plan.mv_scheduler_outcome_binding("outcome-b", "ice", "ns", "b", &directory)
                .is_err()
        );
        assert!(
            !crate::ServerHandle::retire_unissued_role_launch_if_reserved(
                &mut owner,
                RoleSlot::Frontend,
                &b,
            )
            .unwrap()
        );
        drop(peer);
    }

    #[test]
    fn an_ordinary_unissued_slot_can_retire_without_changing_the_next_controlled_slot() {
        let mut plan =
            ClusterTestSessionPlan::new(Instant::now() + Duration::from_secs(2)).unwrap();
        let issued_ordinary = plan.reserve_launch(RoleSlot::Frontend).unwrap();
        plan.authorize_ordinary_launch(&issued_ordinary, std::env::current_exe().unwrap())
            .unwrap();
        let a = plan.reserve_launch(RoleSlot::Frontend).unwrap();
        plan.authorize_ordinary_launch(&a, std::env::current_exe().unwrap())
            .unwrap();
        let b = plan
            .declare_launch(
                RoleSlot::Frontend,
                vec![DomainGrant {
                    domain: "statement-observation".into(),
                    grant_id: "original-b".into(),
                    parameters: serde_json::json!({"action":"observe"}),
                }],
                vec![],
            )
            .unwrap();
        plan.validate_and_freeze(1).unwrap();
        let original = plan.take_next_launch(RoleSlot::Frontend).unwrap();
        assert!(original.ordinary);
        assert_eq!(original.reference.session_id, issued_ordinary.session_id);
        assert!(
            plan.retire_unissued_role_launch(RoleSlot::Frontend, &issued_ordinary)
                .is_err()
        );
        assert_eq!(
            plan.role_launch_progress(RoleSlot::Frontend, &issued_ordinary)
                .unwrap(),
            RoleLaunchProgress::Issued
        );
        plan.retire_unissued_role_launch(RoleSlot::Frontend, &a)
            .unwrap();
        let next = plan.take_next_launch(RoleSlot::Frontend).unwrap();
        assert_eq!(next.reference.session_id, b.session_id);
        assert_eq!(next.reference.process_receipt, b.process_receipt);
        assert!(!next.ordinary);
        assert_eq!(next.declaration.domain_grants[0].grant_id, "original-b");
        assert_eq!(
            plan.role_launch_progress(RoleSlot::Frontend, &a).unwrap(),
            RoleLaunchProgress::RetiredUnissued
        );
        assert_eq!(
            plan.role_launch_progress(RoleSlot::Frontend, &b).unwrap(),
            RoleLaunchProgress::Issued
        );
        assert!(
            plan.retire_unissued_role_launch(RoleSlot::Frontend, &b)
                .is_err()
        );
        assert!(
            !b.process_receipt.exists(),
            "issuing a declaration is not process start"
        );
    }

    #[test]
    fn skipped_backend_launch_keeps_its_successor_and_the_issued_frontend_original() {
        let mut plan =
            ClusterTestSessionPlan::new(Instant::now() + Duration::from_secs(10)).unwrap();
        let fe = plan.reserve_launch(RoleSlot::Frontend).unwrap();
        plan.authorize_ordinary_launch(&fe, std::env::current_exe().unwrap())
            .unwrap();
        let a = reserve_native_role(&mut plan, 1, &["statement-a"]);
        let a_post = reserve_native_role(&mut plan, 1, &["statement-a-post"]);
        let b = reserve_native_role(&mut plan, 1, &["statement-b"]);
        plan.validate_and_freeze(2).unwrap();
        let original_fe = plan.take_next_launch(RoleSlot::Frontend).unwrap();
        assert_eq!(original_fe.reference.session_id, fe.session_id);
        assert!(!fe.process_receipt.exists(), "issued is not process start");
        let probe = tempfile::TempDir::new().unwrap();
        assert!(
            plan.prepare_launch(
                RoleSlot::Backend(1),
                &probe.path().join("never-created-server"),
                &probe.path().join("failed-build-info.json"),
                plan.deadline,
            )
            .is_err()
        );
        assert_eq!(
            plan.role_launch_progress(RoleSlot::Backend(1), &a).unwrap(),
            RoleLaunchProgress::Reserved
        );
        let mut owner = OriginalLaunchOwner(plan);
        assert!(
            crate::ServerHandle::retire_unissued_role_launch_if_reserved(
                &mut owner,
                RoleSlot::Frontend,
                &a,
            )
            .is_err()
        );
        assert!(
            crate::ServerHandle::retire_unissued_role_launch_if_reserved(
                &mut owner,
                RoleSlot::Backend(1),
                &a,
            )
            .unwrap()
        );
        assert_eq!(
            owner
                .0
                .role_launch_progress(RoleSlot::Backend(1), &a)
                .unwrap(),
            RoleLaunchProgress::RetiredUnissued
        );
        assert_eq!(
            owner
                .0
                .role_launch_progress(RoleSlot::Backend(1), &a_post)
                .unwrap(),
            RoleLaunchProgress::Reserved
        );
        assert!(
            crate::ServerHandle::retire_unissued_role_launch_if_reserved(
                &mut owner,
                RoleSlot::Backend(1),
                &a_post,
            )
            .unwrap()
        );
        assert_eq!(
            owner
                .0
                .role_launch_progress(RoleSlot::Backend(1), &a_post)
                .unwrap(),
            RoleLaunchProgress::RetiredUnissued
        );
        assert_eq!(
            owner
                .0
                .role_launch_progress(RoleSlot::Backend(1), &b)
                .unwrap(),
            RoleLaunchProgress::Reserved
        );
        assert!(
            !crate::ServerHandle::retire_unissued_role_launch_if_reserved(
                &mut owner,
                RoleSlot::Frontend,
                &fe,
            )
            .unwrap()
        );
        assert_eq!(
            owner
                .0
                .role_launch_progress(RoleSlot::Frontend, &fe)
                .unwrap(),
            RoleLaunchProgress::Issued
        );
        assert_eq!(original_fe.reference.process_receipt, fe.process_receipt);
        assert!(!fe.process_receipt.exists());
        assert!(!a.process_receipt.exists());
        assert!(!a_post.process_receipt.exists());
        let peer = start_role_hello_at(&mut owner.0, RoleSlot::Backend(1));
        let receipt = owner
            .0
            .process_receipt(RoleSlot::Backend(1))
            .unwrap()
            .unwrap();
        assert_eq!(receipt.session_id, b.session_id);
        assert_eq!(
            owner
                .0
                .domain_secret(RoleSlot::Backend(1), "native", "statement_run_token")
                .unwrap(),
            Some("statement-b".into())
        );
        assert!(
            !crate::ServerHandle::retire_unissued_role_launch_if_reserved(
                &mut owner,
                RoleSlot::Backend(1),
                &b,
            )
            .unwrap()
        );
        assert_eq!(
            owner
                .0
                .role_launch_progress(RoleSlot::Frontend, &fe)
                .unwrap(),
            RoleLaunchProgress::Issued
        );
        owner.0.end_role(RoleSlot::Backend(1)).unwrap();
        drop(peer);
        drop(original_fe);
    }

    #[test]
    fn compiled_capabilities_do_not_grant_runtime_authority() {
        validate_binary_info(&info("normal-build", vec![]), false, &[]).unwrap();
        validate_binary_info(&info("test-controls", vec!["test-controls"]), false, &[]).unwrap();
        assert!(validate_binary_info(&info("normal-build", vec![]), true, &[]).is_err());
        assert!(validate_binary_info(&info("runner", vec![]), false, &[]).is_err());
        assert!(
            validate_binary_info(
                &info("test-controls", vec!["test-controls"]),
                true,
                &[(
                    "novarocks-mysql-adapter".into(),
                    "mem-1-m07-exact-mysql-write".into()
                )]
            )
            .is_err()
        );
        validate_binary_info(&info("test-controls", vec!["test-controls"]), true, &[]).unwrap();
    }
    #[test]
    fn launch_intents_are_finite_frozen_and_do_not_create_receipts_before_hello() {
        let mut plan =
            ClusterTestSessionPlan::new(Instant::now() + Duration::from_secs(2)).unwrap();
        let reference = plan
            .declare_launch(
                RoleSlot::Backend(0),
                vec![DomainGrant {
                    domain: "native".into(),
                    grant_id: "selected".into(),
                    parameters: serde_json::json!({"action":"fixture"}),
                }],
                vec![],
            )
            .unwrap();
        assert!(!reference.process_receipt.exists());
        plan.validate_and_freeze(1).unwrap();
        assert!(
            plan.declare_launch(RoleSlot::Frontend, vec![], vec![])
                .is_err()
        );
        assert!(plan.validate_and_freeze(1).is_err());
    }
    #[test]
    fn missing_authorization_and_foreign_launch_references_fail_before_start() {
        let mut plan =
            ClusterTestSessionPlan::new(Instant::now() + Duration::from_secs(2)).unwrap();
        let reference = plan.reserve_launch(RoleSlot::Frontend).unwrap();
        assert!(plan.validate_and_freeze(1).is_err());
        assert!(!reference.process_receipt.exists());
        let foreign = RoleLaunchReference {
            session_id: reference.session_id.clone(),
            process_receipt: PathBuf::from("/tmp/foreign.process.json"),
        };
        assert!(plan.authorize_launch(&foreign, vec![], vec![]).is_err());
        plan.authorize_launch(
            &reference,
            vec![DomainGrant {
                domain: "statement-observation".into(),
                grant_id: "observe".into(),
                parameters: serde_json::json!({"action":"observe","bearer_secret":"a".repeat(32)}),
            }],
            vec![],
        )
        .unwrap();
        assert!(plan.authorize_launch(&reference, vec![], vec![]).is_err());
        plan.validate_and_freeze(1).unwrap();
    }
    #[cfg(unix)]
    fn executable(directory: &Path, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = directory.join("probe");
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        path
    }
    #[test]
    #[cfg(unix)]
    fn inert_variant_is_self_reported_without_creating_role_authority() {
        let directory = tempfile::tempdir().unwrap();
        let native = info("test-controls", vec!["test-controls"]).to_string();
        let binary = executable(directory.path(), &format!("printf '%s' '{}'", native));
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut plan = ClusterTestSessionPlan::new(deadline).unwrap();
        let reference = plan.reserve_launch(RoleSlot::Backend(0)).unwrap();
        plan.authorize_uncontrolled_launch(
            &reference,
            binary.clone(),
            vec![("novarocks-server".into(), "test-controls".into())],
        )
        .unwrap();
        plan.validate_and_freeze(1).unwrap();
        assert!(
            plan.prepare_launch(
                RoleSlot::Backend(0),
                &binary,
                &directory.path().join("inert.json"),
                deadline
            )
            .unwrap()
            .is_none()
        );
        assert!(
            plan.process_receipt(RoleSlot::Backend(0))
                .unwrap()
                .is_none()
        );
        assert!(!reference.process_receipt.exists());
        assert!(plan.active.is_empty());
        assert!(
            plan.prepare_launch(
                RoleSlot::Backend(0),
                &binary,
                &directory.path().join("reuse.json"),
                deadline
            )
            .is_err()
        );
        let mut normal = ClusterTestSessionPlan::new(deadline).unwrap();
        let reference = normal.reserve_launch(RoleSlot::Backend(0)).unwrap();
        normal
            .authorize_ordinary_launch(&reference, binary.clone())
            .unwrap();
        normal.validate_and_freeze(1).unwrap();
        assert!(
            normal
                .prepare_launch(
                    RoleSlot::Backend(0),
                    &binary,
                    &directory.path().join("normal.json"),
                    deadline
                )
                .unwrap()
                .is_none()
        );
        assert!(normal.active.is_empty());
        assert!(!reference.process_receipt.exists());
    }
    #[test]
    #[cfg(unix)]
    fn build_info_timeout_actually_reaps_and_preserves_failure_output() {
        let directory = tempfile::tempdir().unwrap();
        let binary = executable(directory.path(), "exec sleep 5");
        let evidence = directory.path().join("attempt.json");
        let began = Instant::now();
        let error = check_binary(
            &binary,
            false,
            &[],
            began + Duration::from_millis(60),
            &evidence,
        )
        .unwrap_err();
        assert!(error.to_string().contains("original deadline"));
        assert!(began.elapsed() < Duration::from_secs(2));
        assert!(evidence.exists());
        assert!(evidence.with_extension("stderr.log").exists());
        let exit: Value =
            serde_json::from_slice(&fs::read(evidence.with_extension("exit.json")).unwrap())
                .unwrap();
        assert!(exit["actual_exit"].is_string());
        assert!(
            !exit["actual_exit"]
                .as_str()
                .unwrap()
                .contains("exit status: 0")
        );
    }
    #[test]
    #[cfg(unix)]
    fn capability_rejection_keeps_the_original_native_output_and_cannot_overwrite_it() {
        let directory = tempfile::tempdir().unwrap();
        let native = info("normal-build", vec![]).to_string();
        // This unit starts at the capture-publication boundary: these are
        // build-info format bytes and an ExitStatus fixture, not an observed
        // Server process. The public probe/launch integration and original
        // deadline/kill/wait test retain subprocess coverage.
        use std::os::unix::process::ExitStatusExt;
        let binary = directory.path().join("capture-fixture-not-executed");
        let evidence = directory.path().join("attempt.json");
        let capture = || {
            let mut stdout = tempfile::NamedTempFile::new_in(directory.path()).unwrap();
            std::io::Write::write_all(stdout.as_file_mut(), native.as_bytes()).unwrap();
            BuildInfoCapture {
                stdout,
                stderr: tempfile::NamedTempFile::new_in(directory.path()).unwrap(),
                status_result: Ok(std::process::ExitStatus::from_raw(0)),
                primary: None,
            }
        };
        let diagnostics = |result: &Result<Value>| {
            format!(
                "publish_build_info_capture={result:?}; raw_stdout={:?}; raw_stderr={:?}; exit_receipt={:?}",
                fs::read_to_string(&evidence),
                fs::read_to_string(evidence.with_extension("stderr.log")),
                fs::read_to_string(evidence.with_extension("exit.json")),
            )
        };
        let rejected = publish_build_info_capture(&binary, true, &[], &evidence, capture());
        let first_diagnostics = diagnostics(&rejected);
        let error = rejected.expect_err(&first_diagnostics);
        assert_eq!(
            error.to_string(),
            "controlled executable lacks Server test-controls",
            "{first_diagnostics}"
        );
        assert_eq!(
            fs::read_to_string(&evidence).unwrap(),
            native,
            "{first_diagnostics}"
        );
        let overwrite = publish_build_info_capture(&binary, false, &[], &evidence, capture());
        let overwrite_diagnostics = diagnostics(&overwrite);
        let error = overwrite.expect_err(&overwrite_diagnostics);
        assert_eq!(
            error.to_string(),
            "preserve raw build-info output",
            "{overwrite_diagnostics}"
        );
        assert_eq!(
            fs::read_to_string(&evidence).unwrap(),
            native,
            "{overwrite_diagnostics}"
        );
    }
    #[test]
    #[cfg(unix)]
    fn action_evidence_rejects_partial_public_oversized_and_symlink_inputs() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("action.json");
        assert!(read_private_action_json(&path).unwrap().is_none());
        fs::write(&path, b"{").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(read_private_action_json(&path).is_err());
        fs::write(&path, b"{}").unwrap();
        assert_eq!(
            read_private_action_json(&path).unwrap(),
            Some(serde_json::json!({}))
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_private_action_json(&path).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&path, vec![b' '; 4097]).unwrap();
        assert!(read_private_action_json(&path).is_err());
        let alias = directory.path().join("alias.json");
        symlink(&path, &alias).unwrap();
        assert!(read_private_action_json(&alias).is_err());
    }
    #[test]
    #[cfg(unix)]
    fn exact_action_end_publication_is_private_complete_and_never_replaces_foreign_data() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("action.end.json");
        let first = serde_json::json!({"scope":"actual-first"});
        publish_private_action_json(&path, &first).unwrap();
        assert_eq!(
            read_private_action_json(&path).unwrap(),
            Some(first.clone())
        );
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        publish_private_action_json(&path, &first).unwrap();
        assert!(
            publish_private_action_json(&path, &serde_json::json!({"scope":"foreign"})).is_err()
        );
        assert_eq!(read_private_action_json(&path).unwrap(), Some(first));
        let other = directory.path().join("oversized.end.json");
        assert!(
            publish_private_action_json(&other, &serde_json::json!({"padding":"x".repeat(4096)}))
                .is_err()
        );
        assert!(!other.exists());
    }
}
