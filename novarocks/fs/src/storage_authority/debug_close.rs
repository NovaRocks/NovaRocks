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

//! Independent, role-bound authority observation and closure permissions.

use super::{
    AcquisitionFailure, AuthorityCapabilityPath, AuthorityShared, StorageAuthority,
    StorageAuthorityId,
};
use novarocks_failpoint::scope::{ControlCancellation, GrantHandle, ProcessBinding};
use novarocks_spi::connector::{
    ConnectorInstanceId, StaticCredentialReference, StorageCredentialScopePrefix,
};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

const MAX_AUTHORITIES: usize = 4096;

struct Grant {
    observe: Option<GrantHandle>,
    close: Option<GrantHandle>,
    root: PathBuf,
    backend_index: usize,
    target: TargetIntent,
}

#[derive(Clone, Eq, PartialEq)]
struct TargetIntent {
    catalog: ConnectorInstanceId,
    scope: StorageCredentialScopePrefix,
    principal: StaticCredentialReference,
    endpoint: String,
}
impl TargetIntent {
    fn from_handle(handle: &GrantHandle) -> Result<Self, String> {
        let target = handle.parameters()["target"]
            .as_object()
            .ok_or("authority control requires a finite target intent")?;
        let fields = [
            "catalog",
            "scope",
            "principal_name",
            "principal_generation",
            "capability",
            "endpoint",
        ];
        if target.len() != fields.len() || fields.iter().any(|key| !target.contains_key(*key)) {
            return Err("authority target intent fields are invalid".into());
        }
        let text = |key: &str| {
            target[key]
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= 4096)
                .ok_or("authority target intent value is invalid")
        };
        if text("capability")? != "credentials-endpoint" {
            return Err("authority target intent capability is unsupported".into());
        }
        let catalog = ConnectorInstanceId::try_from_canonical(text("catalog")?)
            .map_err(|_| "authority target catalog is invalid")?;
        let scope = StorageCredentialScopePrefix::try_from_normalized(text("scope")?)
            .map_err(|_| "authority target scope is invalid")?;
        let principal = StaticCredentialReference::try_new(
            text("principal_name")?,
            text("principal_generation")?,
        )
        .map_err(|_| "authority target principal is invalid")?;
        let endpoint = text("endpoint")?.to_owned();
        let parsed =
            url::Url::parse(&endpoint).map_err(|_| "authority target endpoint is invalid")?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err("authority target endpoint is invalid".into());
        }
        Ok(Self {
            catalog,
            scope,
            principal,
            endpoint,
        })
    }

    fn matches(&self, id: &StorageAuthorityId) -> bool {
        id.catalog().catalog_name() == &self.catalog
            && id.scope() == &self.scope
            && matches!(id.capability(), AuthorityCapabilityPath::CredentialsEndpoint { principal, routing }
                if principal == &self.principal && routing.url() == self.endpoint.as_str())
    }
}
struct Inner {
    authorities: Mutex<Vec<Arc<AuthorityShared>>>,
    targets: Vec<TargetIntent>,
    scope: Option<ControlCancellation>,
    stopped: AtomicBool,
    workers: Mutex<Vec<JoinHandle<Result<(), String>>>>,
    handles: AtomicUsize,
    failures: Mutex<Vec<String>>,
}

/// Composition retains this owner and explicitly joins it before role exit.
/// The registry shares registration access without introducing global state.
pub struct StorageAuthorityCloseControls {
    inner: Arc<Inner>,
}
impl std::fmt::Debug for StorageAuthorityCloseControls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageAuthorityCloseControls")
            .finish_non_exhaustive()
    }
}
impl StorageAuthorityCloseControls {
    pub const DOMAIN: &'static str = "storage-authority";
    pub fn from_grants(handles: Vec<GrantHandle>) -> Result<Self, String> {
        let owner = handles.first().map(|handle| handle.cancellation());
        let mut grants: Vec<Grant> = Vec::new();
        for handle in handles {
            let cancellation = handle.cancellation();
            if handle.domain() != Self::DOMAIN
                || owner.as_ref().is_some_and(|owner| {
                    owner.process() != cancellation.process()
                        || owner.session_id() != cancellation.session_id()
                        || owner.launch_id() != cancellation.launch_id()
                        || owner.deadline() != cancellation.deadline()
                })
            {
                return Err(
                    "authority close grants do not belong to one exact owner session".into(),
                );
            }
            let parameters = handle
                .parameters()
                .as_object()
                .ok_or("authority close parameters must be an object")?;
            let action = parameters.get("action").and_then(|v| v.as_str());
            if parameters.len() != 4 || !matches!(action, Some("observe" | "close")) {
                return Err(
                    "authority control requires action, root, backend_index and target".into(),
                );
            }
            let target = TargetIntent::from_handle(&handle)?;
            if !matches!(handle.cancellation().process(), ProcessBinding::Backend(_)) {
                return Err("authority close grant requires a backend process".into());
            }
            let root = PathBuf::from(
                parameters
                    .get("root")
                    .and_then(|v| v.as_str())
                    .ok_or("authority close root is missing")?,
            );
            if !root.is_absolute() || !root.is_dir() {
                return Err("authority close root must be an existing absolute directory".into());
            }
            let backend_index = parameters
                .get("backend_index")
                .and_then(|v| v.as_u64())
                .and_then(|v| usize::try_from(v).ok())
                .ok_or("authority close backend index is invalid")?;
            let index = grants
                .iter()
                .position(|grant| grant.root == root && grant.backend_index == backend_index);
            let index = match index {
                Some(index) if grants[index].target != target => {
                    return Err("authority transport targets conflict".into());
                }
                Some(index) => index,
                None => {
                    grants.push(Grant {
                        observe: None,
                        close: None,
                        root,
                        backend_index,
                        target,
                    });
                    grants.len() - 1
                }
            };
            let permission = if action == Some("observe") {
                &mut grants[index].observe
            } else {
                &mut grants[index].close
            };
            if permission.replace(handle).is_some() {
                return Err("duplicate authority action target".into());
            }
        }
        let controls = Self {
            inner: Arc::new(Inner {
                authorities: Mutex::new(Vec::new()),
                targets: grants.iter().map(|grant| grant.target.clone()).collect(),
                scope: owner,
                stopped: AtomicBool::new(false),
                workers: Mutex::new(Vec::new()),
                handles: AtomicUsize::new(1),
                failures: Mutex::new(Vec::new()),
            }),
        };
        for grant in grants {
            let inner = Arc::clone(&controls.inner);
            match std::thread::Builder::new()
                .name("storage-authority-test-close".into())
                .spawn(move || observe_trigger(grant, inner))
            {
                Ok(worker) => controls
                    .inner
                    .workers
                    .lock()
                    .map_err(|_| "authority close worker lock failed")?
                    .push(worker),
                Err(error) => {
                    let cleanup = controls.close_and_join();
                    return Err(format!(
                        "start authority close observer: {error}; cleanup: {cleanup:?}"
                    ));
                }
            }
        }
        Ok(controls)
    }
    pub fn register(&self, authority: &StorageAuthority) -> Result<(), String> {
        let mut authorities = self
            .inner
            .authorities
            .lock()
            .map_err(|_| "authority close target lock failed")?;
        // Closed controls do not retain or observe later ordinary authorities.
        if self.inner.stopped.load(Ordering::Acquire)
            || !self
                .inner
                .scope
                .as_ref()
                .is_some_and(ControlCancellation::is_active)
        {
            self.inner.stopped.store(true, Ordering::Release);
            authorities.clear();
            return Ok(());
        }
        let Some(target) = self
            .inner
            .targets
            .iter()
            .find(|target| target.matches(&authority.shared.id))
        else {
            return Ok(());
        };
        // The first matching registration freezes the full provider identity,
        // including catalog generation, before observation or close can claim.
        // A later digest is target data, never permission to redirect the grant.
        if authorities
            .iter()
            .any(|shared| target.matches(&shared.id) && shared.id != authority.shared.id)
        {
            return Err("authority target intent is already pinned to another identity".into());
        }
        if let Some(shared) = authorities
            .iter()
            .find(|shared| shared.id == authority.shared.id)
        {
            if Arc::ptr_eq(shared, &authority.shared) {
                return Ok(());
            }
            return Err("authority close identity names two live authority owners".into());
        }
        if authorities.len() == MAX_AUTHORITIES {
            return Err("authority close registration capacity exceeded".into());
        }
        authorities.push(Arc::clone(&authority.shared));
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn worker_count_for_test(&self) -> usize {
        self.inner
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
    #[cfg(test)]
    pub(super) fn workers_returned_for_test(&self) -> bool {
        self.inner
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .all(JoinHandle::is_finished)
    }
    pub fn close_and_join(&self) -> Result<(), String> {
        self.inner.stopped.store(true, Ordering::Release);
        // Every concurrent return follows actual joins, including poisoned
        // bookkeeping. Recovery is for cleanup only, never for new actions.
        let (mut join_owner, join_poisoned) = match self.inner.workers.lock() {
            Ok(owner) => (owner, false),
            Err(poisoned) => (poisoned.into_inner(), true),
        };
        let workers = std::mem::take(&mut *join_owner);
        let (mut failures, history_poisoned) = match self.inner.failures.lock() {
            Ok(history) => (history, false),
            Err(poisoned) => (poisoned.into_inner(), true),
        };
        if join_poisoned {
            retain_poison_failure(&mut failures, "authority close worker lock poisoned");
        }
        if history_poisoned {
            retain_poison_failure(
                &mut failures,
                "authority close failure history lock poisoned",
            );
        }
        for worker in workers {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(error),
                Err(_) => failures.push("authority close observer panicked".into()),
            }
        }
        match self.inner.authorities.lock() {
            Ok(mut authorities) => authorities.clear(),
            Err(poisoned) => {
                retain_poison_failure(
                    &mut failures,
                    "authority close target cleanup lock poisoned",
                );
                poisoned.into_inner().clear();
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
    #[cfg(test)]
    pub(super) fn poison_authorities_while_for_test(&self, close_scope: impl FnOnce()) {
        let _targets = self.inner.authorities.lock().unwrap();
        close_scope();
        panic!("injected authority close target lock poison");
    }
    #[cfg(test)]
    pub(super) fn poison_join_history_for_test(&self) {
        let _workers = self.inner.workers.lock().unwrap();
        let mut failures = self.inner.failures.lock().unwrap();
        failures.push("preexisting authority close failure".into());
        panic!("injected authority close join bookkeeping poison");
    }
}

fn retain_poison_failure(failures: &mut Vec<String>, detail: &'static str) {
    if !failures.iter().any(|failure| failure == detail) {
        failures.push(detail.into());
    }
}

impl Clone for StorageAuthorityCloseControls {
    fn clone(&self) -> Self {
        self.inner.handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
impl Drop for StorageAuthorityCloseControls {
    fn drop(&mut self) {
        if self.inner.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            let _ = self.close_and_join();
        }
    }
}
impl StorageAuthorityCloseControls {
    pub(super) fn register_or_record_failure(&self, authority: &StorageAuthority) {
        if let Err(error) = self.register(authority) {
            self.inner.stopped.store(true, Ordering::Release);
            self.inner
                .failures
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(error);
        }
    }
}

pub(super) fn control_key(id: &StorageAuthorityId) -> String {
    // The full identity participates, including catalog version and endpoint.
    // Only its digest is written to runner-owned files; an advertised endpoint
    // may contain query parameters and must not be copied into evidence.
    struct IdentityDigest(Sha256);
    impl std::hash::Hasher for IdentityDigest {
        fn write(&mut self, bytes: &[u8]) {
            self.0.update(bytes);
        }
        fn finish(&self) -> u64 {
            let digest = self.0.clone().finalize();
            u64::from_le_bytes(digest[..8].try_into().expect("SHA-256 prefix"))
        }
    }
    // Debug deliberately redacts routing values and opaque owner identities.
    // Hash consumes their typed equality facts without exposing those values.
    let mut hasher = IdentityDigest(Sha256::new());
    std::hash::Hash::hash(id, &mut hasher);
    let digest = hasher.0.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("write to String");
    }
    encoded
}

fn identity_record(id: &StorageAuthorityId, key: &str) -> String {
    let mut version = String::with_capacity(64);
    for byte in id.catalog().version().as_bytes() {
        write!(&mut version, "{byte:02x}").expect("write to String");
    }
    let path = match id.capability() {
        AuthorityCapabilityPath::NodeLocalProvider { .. } => "node-local-provider",
        AuthorityCapabilityPath::CredentialsEndpoint { .. } => "credentials-endpoint",
        AuthorityCapabilityPath::LoadTableDelegation { .. } => "load-table-delegation",
        AuthorityCapabilityPath::InProcessProvider(_) => "in-process-provider",
        AuthorityCapabilityPath::SeededWithoutRenewal { .. } => "seeded-without-renewal",
    };
    let principal = id.principal().map_or_else(
        || "none".to_string(),
        |principal| format!("{}:{}", principal.name(), principal.generation()),
    );
    format!(
        "key={key}\ncatalog={}\nversion={version}\nscope={}\nprincipal={principal}\npath={path}\n",
        id.catalog().catalog_name().as_str(),
        id.scope().as_str(),
    )
}

fn observe_trigger(grant: Grant, inner: Arc<Inner>) -> Result<(), String> {
    let trigger = grant.root.join(format!(
        "be-{}.storage-authority-close.trigger",
        grant.backend_index
    ));
    let confirmed = grant.root.join(format!(
        "be-{}.storage-authority-close.confirmed",
        grant.backend_index
    ));
    let late_confirmed = grant.root.join(format!(
        "be-{}.storage-authority-close.late-confirmed",
        grant.backend_index
    ));
    let cancellation = grant
        .observe
        .as_ref()
        .or(grant.close.as_ref())
        .expect("decoded authority transport has a permission")
        .cancellation();
    let mut identities = Vec::new();
    let mut observation_claim = None;
    let result = (|| {
        while !inner.stopped.load(Ordering::Acquire) && cancellation.is_active() {
            let authorities = inner
                .authorities
                .lock()
                .map_err(|_| "authority close target lock failed")?
                .clone();
            for shared in authorities
                .iter()
                .filter(|shared| grant.target.matches(&shared.id))
            {
                if identities.is_empty()
                    && let Some(handle) = &grant.observe
                {
                    let mut permission =
                        handle.claim_once(cancellation.process()).map_err(|error| {
                            format!("claim authority identity observation: {error}")
                        })?;
                    if !permission.cancellation().is_active() {
                        continue;
                    }
                    let path = grant.root.join(format!(
                        "be-{}.storage-authority-0.identity",
                        grant.backend_index
                    ));
                    publish_identity(
                        &path,
                        &identity_record(&shared.id, &control_key(&shared.id)),
                        &mut identities,
                    )?;
                    permission.record_hit().map_err(|error| {
                        format!("record authority identity observation: {error}")
                    })?;
                    observation_claim = Some(permission);
                }
                let Some(handle) = &grant.close else {
                    continue;
                };
                let key = control_key(&shared.id);
                if !read_trigger(&trigger)?.is_some_and(|requested| requested.trim() == key) {
                    continue;
                }
                // The full authority identity and actual role process were checked
                // before the once budget is consumed. An unrelated key cannot claim.
                let mut claim = handle
                    .claim_once(cancellation.process())
                    .map_err(|error| format!("claim authority close: {error}"))?;
                if !claim.cancellation().is_active() {
                    continue;
                }
                if !close_if_key(shared, &key) {
                    return Err("authority close target was already closed".into());
                }
                claim
                    .record_hit()
                    .map_err(|error| format!("record authority close: {error}"))?;
                let before = shared.metrics();
                publish(
                    &confirmed,
                    format!(
                        "key={key}\nstatus=closed\nrefreshes_applied={}\nlate_results_discarded={}\n",
                        before.refreshes_applied, before.late_results_discarded
                    ),
                )?;
                while !inner.stopped.load(Ordering::Acquire) && cancellation.is_active() {
                    let after = shared.metrics();
                    if after.late_results_discarded > before.late_results_discarded {
                        let status = if after.refreshes_applied == before.refreshes_applied {
                            "discarded"
                        } else {
                            "applied-after-close"
                        };
                        publish(
                            &late_confirmed,
                            format!(
                                "key={key}\nstatus={status}\nrefreshes_applied={}\nlate_results_discarded={}\n",
                                after.refreshes_applied, after.late_results_discarded
                            ),
                        )?;
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    })();
    let mut failures = Vec::new();
    if let Err(error) = result {
        failures.push(error);
    }
    // Natural closure releases retained targets and still cleans every owned
    // identity file. A poisoned cleanup lock is an additional failure, never
    // an early return that loses the primary error or skips later cleanup.
    if !cancellation.is_active() {
        inner.stopped.store(true, Ordering::Release);
        match inner.authorities.lock() {
            Ok(mut authorities) => authorities.clear(),
            Err(poisoned) => {
                failures.push("authority close natural target cleanup lock poisoned".into());
                poisoned.into_inner().clear();
            }
        }
    }
    for path in identities {
        if let Err(error) = fs::remove_file(path) {
            failures.push(format!("remove authority identity: {error}"));
        }
    }
    drop(observation_claim);
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

fn stage_identity(
    path: &std::path::Path,
    record: &str,
    owned: &mut Vec<PathBuf>,
) -> Result<PathBuf, String> {
    use std::io::Write;
    let temporary = path.with_extension("identity.tmp");
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temporary)
        .map_err(|error| format!("stage authority identity: {error}"))?;
    owned.push(temporary.clone());
    file.write_all(record.as_bytes())
        .map_err(|error| format!("write authority identity: {error}"))?;
    Ok(temporary)
}

fn publish_identity(
    path: &std::path::Path,
    record: &str,
    owned: &mut Vec<PathBuf>,
) -> Result<(), String> {
    let temporary = stage_identity(path, record, owned)?;
    // The final name becomes visible only after the complete record was
    // written and its descriptor closed. A hard link atomically creates that
    // name without replacing an existing target belonging to another attempt.
    fs::hard_link(&temporary, path)
        .map_err(|error| format!("publish authority identity: {error}"))?;
    owned.push(path.to_owned());
    fs::remove_file(&temporary)
        .map_err(|error| format!("remove authority identity staging: {error}"))?;
    owned.retain(|path| path != &temporary);
    Ok(())
}

fn read_trigger(path: &std::path::Path) -> Result<Option<String>, String> {
    use std::io::Read;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("inspect authority close target: {error}")),
    };
    if !metadata.is_file() || metadata.len() > 65 {
        return Err("authority close target is not a bounded digest file".into());
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|error| format!("read authority close target: {error}"))?;
    if !file
        .metadata()
        .map_err(|error| error.to_string())?
        .is_file()
    {
        return Err("authority close target is not a regular file".into());
    }
    let mut requested = String::new();
    file.take(66)
        .read_to_string(&mut requested)
        .map_err(|error| format!("read authority close target: {error}"))?;
    if requested.len() > 65 {
        return Err("authority close target exceeds digest bound".into());
    }
    Ok(Some(requested))
}

fn publish(path: &std::path::Path, record: String) -> Result<(), String> {
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, record)
        .and_then(|_| fs::rename(temporary, path))
        .map_err(|error| format!("publish authority closure: {error}"))
}

pub(super) fn close_if_key(shared: &AuthorityShared, requested: &str) -> bool {
    if requested != control_key(&shared.id) {
        return false;
    }
    let mut state = shared.lock_state();
    if state.closed.is_some() {
        return false;
    }
    state.closed = Some(AcquisitionFailure::Denied(
        "runner confirmed storage authority closure".into(),
    ));
    state.generation = state.generation.wrapping_add(1);
    state.material = None;
    state.inflight = None;
    true
}

#[cfg(test)]
mod publication_tests {
    use super::{publish_identity, stage_identity};

    #[test]
    fn authority_identity_publication_exposes_complete_records_and_never_overwrites() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("be-0.storage-authority-0.identity");
        let record = "key=exact-key\ncatalog=lake\nversion=complete-generation\n";
        let mut owned = Vec::new();
        let temporary = stage_identity(&path, record, &mut owned).unwrap();
        assert!(
            !path.exists(),
            "staging cannot expose an incomplete identity"
        );
        assert_eq!(std::fs::read_to_string(&temporary).unwrap(), record);
        std::fs::remove_file(temporary).unwrap();
        owned.clear();
        publish_identity(&path, record, &mut owned).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), record);
        assert_eq!(owned, vec![path.clone()]);
        owned.clear();
        assert!(publish_identity(&path, "other attempt", &mut owned).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), record);
        assert!(
            !owned.contains(&path),
            "a refused target is never owned cleanup"
        );
        for temporary in owned {
            std::fs::remove_file(temporary).unwrap();
        }
    }
}
