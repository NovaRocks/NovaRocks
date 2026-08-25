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

//! Test-only fault vocabulary and token protocol shared by role processes and
//! their cross-process harness.  The vocabulary below has no crate
//! dependencies; the default `typed` feature additionally validates a token
//! against a native query execution identity.  Role hooks keep their existing
//! `debug_assertions` gates, cleanup claims keep their runtime behavior, and
//! release startup rejects the environment variables.  This crate introduces
//! no additional activation gate.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub const QUERY_LIFECYCLE_FAULT_DIR_ENV: &str = "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR";
pub const CLEANUP_FAULT_DIR_ENV: &str = "NOVAROCKS_SQL_TEST_CLEANUP_FAULT_DIR";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum QueryLifecycleFaultKind {
    InitAckDrop,
    StageAckDrop,
    StartAckDrop,
    StartAckSuppress,
    HeartbeatStop,
    HeartbeatStopAfterStage,
    RestartAfterInitAck,
    TerminalAckDrop,
    TerminalSnapshotStreamDrop,
    TerminalSnapshotConflict,
    ObservationP2AssemblyFailure,
    ObservationP2BudgetPressure,
    TerminalP0RetainedSlotExhausted,
    TerminalP0BytesExhausted,
    TerminalP0DeliveryPermitExhausted,
    TerminalP1EncodeFailure,
    TerminalP1RetentionExhausted,
    TerminalProofStreamDrop,
    TerminalAttestationStreamDrop,
    TerminalOutcomeSuppress,
    RuntimeFilterContributionAckDrop,
}

impl QueryLifecycleFaultKind {
    pub const ALL: [Self; 21] = [
        Self::InitAckDrop,
        Self::StageAckDrop,
        Self::StartAckDrop,
        Self::StartAckSuppress,
        Self::HeartbeatStop,
        Self::HeartbeatStopAfterStage,
        Self::RestartAfterInitAck,
        Self::TerminalAckDrop,
        Self::TerminalSnapshotStreamDrop,
        Self::TerminalSnapshotConflict,
        Self::ObservationP2AssemblyFailure,
        Self::ObservationP2BudgetPressure,
        Self::TerminalP0RetainedSlotExhausted,
        Self::TerminalP0BytesExhausted,
        Self::TerminalP0DeliveryPermitExhausted,
        Self::TerminalP1EncodeFailure,
        Self::TerminalP1RetentionExhausted,
        Self::TerminalProofStreamDrop,
        Self::TerminalAttestationStreamDrop,
        Self::TerminalOutcomeSuppress,
        Self::RuntimeFilterContributionAckDrop,
    ];

    pub const fn file_stem(self) -> &'static str {
        match self {
            Self::InitAckDrop => "init-ack-drop",
            Self::StageAckDrop => "stage-ack-drop",
            Self::StartAckDrop => "start-ack-drop",
            Self::StartAckSuppress => "start-ack-suppress",
            Self::HeartbeatStop => "heartbeat-stop",
            Self::HeartbeatStopAfterStage => "heartbeat-stop-after-stage",
            Self::RestartAfterInitAck => "restart-after-init-ack",
            Self::TerminalAckDrop => "terminal-ack-drop",
            Self::TerminalSnapshotStreamDrop => "terminal-snapshot-stream-drop",
            Self::TerminalSnapshotConflict => "terminal-snapshot-conflict",
            Self::ObservationP2AssemblyFailure => "observation-p2-assembly-failure",
            Self::ObservationP2BudgetPressure => "observation-p2-budget-pressure",
            Self::TerminalP0RetainedSlotExhausted => "terminal-p0-retained-slot-exhausted",
            Self::TerminalP0BytesExhausted => "terminal-p0-bytes-exhausted",
            Self::TerminalP0DeliveryPermitExhausted => "terminal-p0-delivery-permit-exhausted",
            Self::TerminalP1EncodeFailure => "terminal-p1-encode-failure",
            Self::TerminalP1RetentionExhausted => "terminal-p1-retention-exhausted",
            Self::TerminalProofStreamDrop => "terminal-proof-stream-drop",
            Self::TerminalAttestationStreamDrop => "terminal-attestation-stream-drop",
            Self::TerminalOutcomeSuppress => "terminal-outcome-suppress",
            Self::RuntimeFilterContributionAckDrop => "runtime-filter-contribution-ack-drop",
        }
    }

    pub const fn as_str(self) -> &'static str {
        self.file_stem()
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.file_stem() == value)
    }
}

/// The runner can request only the explicitly allowlisted closeout fault
/// subset, never a generic lifecycle hook. This is intentionally a separate
/// parsing surface.
pub const RUNNER_RFO_KINDS: [QueryLifecycleFaultKind; 11] = [
    QueryLifecycleFaultKind::ObservationP2AssemblyFailure,
    QueryLifecycleFaultKind::ObservationP2BudgetPressure,
    QueryLifecycleFaultKind::TerminalP0RetainedSlotExhausted,
    QueryLifecycleFaultKind::TerminalP0BytesExhausted,
    QueryLifecycleFaultKind::TerminalP0DeliveryPermitExhausted,
    QueryLifecycleFaultKind::TerminalP1EncodeFailure,
    QueryLifecycleFaultKind::TerminalP1RetentionExhausted,
    QueryLifecycleFaultKind::TerminalProofStreamDrop,
    QueryLifecycleFaultKind::TerminalAttestationStreamDrop,
    QueryLifecycleFaultKind::TerminalOutcomeSuppress,
    QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop,
];

pub fn parse_runner_rfo_kind(value: &str) -> Option<QueryLifecycleFaultKind> {
    QueryLifecycleFaultKind::parse(value).filter(|kind| RUNNER_RFO_KINDS.contains(kind))
}

pub fn runner_rfo_kind_names() -> impl Iterator<Item = &'static str> {
    RUNNER_RFO_KINDS
        .into_iter()
        .map(QueryLifecycleFaultKind::file_stem)
}

pub fn configured_root() -> Option<PathBuf> {
    std::env::var_os(QUERY_LIFECYCLE_FAULT_DIR_ENV).map(PathBuf::from)
}

pub fn arm_path(root: &Path, backend_index: usize, kind: QueryLifecycleFaultKind) -> PathBuf {
    root.join(format!("be-{backend_index}.{}.arm", kind.file_stem()))
}

pub fn trigger_path(root: &Path, backend_index: usize, kind: QueryLifecycleFaultKind) -> PathBuf {
    root.join(format!("be-{backend_index}.{}.trigger", kind.file_stem()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum CleanupFaultKind {
    DeleteFailed,
    DropDeleteResponse,
    ReceiptWriteFailed,
    CheckpointFailed,
    KillFeAfterDelete,
}

impl CleanupFaultKind {
    pub const ALL: [Self; 5] = [
        Self::DeleteFailed,
        Self::DropDeleteResponse,
        Self::ReceiptWriteFailed,
        Self::CheckpointFailed,
        Self::KillFeAfterDelete,
    ];

    pub const fn directive_name(self) -> &'static str {
        match self {
            Self::DeleteFailed => "delete_failed",
            Self::DropDeleteResponse => "drop_delete_response",
            Self::ReceiptWriteFailed => "receipt_write_failed",
            Self::CheckpointFailed => "checkpoint_failed",
            Self::KillFeAfterDelete => "kill_fe_after_delete",
        }
    }

    pub const fn file_stem(self) -> &'static str {
        match self {
            Self::DeleteFailed => "delete-failed",
            Self::DropDeleteResponse => "drop-delete-response",
            Self::ReceiptWriteFailed => "receipt-write-failed",
            Self::CheckpointFailed => "checkpoint-failed",
            Self::KillFeAfterDelete => "kill-fe-after-delete",
        }
    }
}

pub fn parse_cleanup_fault_directive(value: &str) -> Option<CleanupFaultKind> {
    CleanupFaultKind::ALL
        .into_iter()
        .find(|kind| kind.directive_name() == value)
}

pub fn cleanup_fault_directive_names() -> impl Iterator<Item = &'static str> {
    CleanupFaultKind::ALL
        .into_iter()
        .map(CleanupFaultKind::directive_name)
}

pub fn cleanup_trigger_path(root: &Path, kind: CleanupFaultKind) -> PathBuf {
    root.join(format!("{}.trigger", kind.file_stem()))
}

pub fn claim_cleanup_fault(root: &Path, kind: CleanupFaultKind) -> Result<bool, String> {
    let trigger = cleanup_trigger_path(root, kind);
    let sequence = NEXT_CLAIM.fetch_add(1, Ordering::Relaxed);
    let claimed = root.join(format!(
        ".{}.claimed-{}-{}",
        kind.file_stem(),
        std::process::id(),
        sequence
    ));
    match fs::rename(&trigger, &claimed) {
        Ok(()) => {
            fs::remove_file(&claimed).map_err(|error| {
                format!(
                    "remove claimed cleanup fault {}: {error}",
                    claimed.display()
                )
            })?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!(
            "claim cleanup fault {}: {error}",
            trigger.display()
        )),
    }
}

pub fn claim_configured_cleanup_fault(kind: CleanupFaultKind) -> Result<bool, String> {
    let Some(root) = std::env::var_os(CLEANUP_FAULT_DIR_ENV) else {
        return Ok(false);
    };
    claim_cleanup_fault(Path::new(&root), kind)
}

static NEXT_CLAIM: AtomicU64 = AtomicU64::new(1);

#[cfg(feature = "typed")]
mod typed {
    use super::*;
    use novarocks_proto::lifecycle::{AttemptId, QueryExecutionId};
    use std::collections::BTreeMap;
    use std::io::Write;

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct QueryLifecycleFaultScope {
        pub token: String,
        pub execution_id: QueryExecutionId,
        pub backend_index: usize,
        pub backend_id: u64,
        pub start_epoch: u64,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct StagePrepareFailure {
        pub token: String,
        pub ordinal: usize,
    }

    pub fn claim_stage_prepare_failure(
        root: &Path,
        available_fragments: usize,
    ) -> Result<Option<StagePrepareFailure>, String> {
        let path = root.join("stage-prepare-fail.trigger");
        let contents = match fs::read_to_string(&path) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("read {}: {error}", path.display())),
        };
        let fields = parse_fields(&contents)?;
        let failure = StagePrepareFailure {
            token: required_token(&fields)?,
            ordinal: required_usize(&fields, "ordinal")?,
        };
        if failure.ordinal == 0 {
            return Err("stage prepare fault ordinal must be at least one".to_string());
        }
        if failure.ordinal > available_fragments {
            return Ok(None);
        }
        match fs::remove_file(&path) {
            Ok(()) => Ok(Some(failure)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(format!("consume {}: {error}", path.display())),
        }
    }

    pub fn bind_armed_fault(
        root: &Path,
        kind: QueryLifecycleFaultKind,
        execution_id: QueryExecutionId,
        backend_index: usize,
        backend_id: u64,
        start_epoch: u64,
    ) -> Result<Option<QueryLifecycleFaultScope>, String> {
        let arm = arm_path(root, backend_index, kind);
        let contents = match fs::read_to_string(&arm) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("read {}: {error}", arm.display())),
        };
        let fields = parse_fields(&contents)?;
        let token = required_token(&fields)?;
        let armed_backend_index = required_usize(&fields, "backend_index")?;
        if armed_backend_index != backend_index {
            return Err(format!(
                "fault arm backend_index {armed_backend_index} does not match path backend {backend_index}"
            ));
        }
        let scope = QueryLifecycleFaultScope {
            token,
            execution_id,
            backend_index,
            backend_id,
            start_epoch,
        };
        let trigger = trigger_path(root, backend_index, kind);
        publish_new(&trigger, serialize_scope(&scope).as_bytes())?;
        fs::remove_file(&arm).map_err(|error| format!("consume {}: {error}", arm.display()))?;
        Ok(Some(scope))
    }

    pub fn claim_matching_fault(
        root: &Path,
        kind: QueryLifecycleFaultKind,
        execution_id: QueryExecutionId,
        backend_index: usize,
        backend_id: u64,
        start_epoch: u64,
    ) -> Result<Option<QueryLifecycleFaultScope>, String> {
        let trigger = trigger_path(root, backend_index, kind);
        let contents = match fs::read_to_string(&trigger) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("read {}: {error}", trigger.display())),
        };
        let scope = parse_scope(&contents)?;
        if scope.execution_id != execution_id
            || scope.backend_index != backend_index
            || scope.backend_id != backend_id
            || scope.start_epoch != start_epoch
        {
            return Ok(None);
        }
        match fs::remove_file(&trigger) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("consume {}: {error}", trigger.display())),
        }
        Ok(Some(scope))
    }

    /// Claims an arm-bound token from the first receiver that has accepted a
    /// matching runtime-filter Contribution. The arm's backend identity
    /// remains part of the stored scope for provenance, but is intentionally
    /// not an eligibility condition: remote materialization can accept the
    /// contribution on a different BE than the producer placement.
    pub fn claim_matching_receiver_agnostic_fault(
        root: &Path,
        kind: QueryLifecycleFaultKind,
        execution_id: QueryExecutionId,
    ) -> Result<Option<QueryLifecycleFaultScope>, String> {
        let suffix = format!(".{}.trigger", kind.file_stem());
        let entries =
            fs::read_dir(root).map_err(|error| format!("read {}: {error}", root.display()))?;
        for entry in entries {
            let entry = entry.map_err(|error| format!("read {} entry: {error}", root.display()))?;
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("be-") || !name.ends_with(&suffix) {
                continue;
            }
            let contents = match fs::read_to_string(&path) {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(format!("read {}: {error}", path.display())),
            };
            let scope = parse_scope(&contents)?;
            if scope.execution_id != execution_id {
                continue;
            }
            match fs::remove_file(&path) {
                Ok(()) => return Ok(Some(scope)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(format!("consume {}: {error}", path.display())),
            }
        }
        Ok(None)
    }

    pub fn observe_matching_fault(
        root: &Path,
        kind: QueryLifecycleFaultKind,
        execution_id: QueryExecutionId,
        backend_index: usize,
        backend_id: u64,
        start_epoch: u64,
    ) -> Result<Option<QueryLifecycleFaultScope>, String> {
        let trigger = trigger_path(root, backend_index, kind);
        let contents = match fs::read_to_string(&trigger) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("read {}: {error}", trigger.display())),
        };
        let scope = parse_scope(&contents)?;
        if scope.execution_id != execution_id
            || scope.backend_index != backend_index
            || scope.backend_id != backend_id
            || scope.start_epoch != start_epoch
        {
            return Ok(None);
        }
        Ok(Some(scope))
    }

    fn serialize_scope(scope: &QueryLifecycleFaultScope) -> String {
        format!(
            "token={}\nexecution_hi={}\nexecution_lo={}\nattempt={}\nbackend_index={}\nbackend_id={}\nstart_epoch={}\n",
            scope.token,
            scope.execution_id.query_id().high(),
            scope.execution_id.query_id().low(),
            scope.execution_id.attempt_id().get(),
            scope.backend_index,
            scope.backend_id,
            scope.start_epoch
        )
    }
    fn parse_scope(contents: &str) -> Result<QueryLifecycleFaultScope, String> {
        let fields = parse_fields(contents)?;
        let query_id = novarocks_types::QueryId::new(
            required_i64(&fields, "execution_hi")?,
            required_i64(&fields, "execution_lo")?,
        );
        let attempt =
            AttemptId::new(required_u64(&fields, "attempt")?).map_err(|error| error.to_string())?;
        let execution_id =
            QueryExecutionId::new(query_id, attempt).map_err(|error| error.to_string())?;
        Ok(QueryLifecycleFaultScope {
            token: required_token(&fields)?,
            execution_id,
            backend_index: required_usize(&fields, "backend_index")?,
            backend_id: required_u64(&fields, "backend_id")?,
            start_epoch: required_u64(&fields, "start_epoch")?,
        })
    }
    fn parse_fields(contents: &str) -> Result<BTreeMap<&str, &str>, String> {
        let mut fields = BTreeMap::new();
        for line in contents.lines() {
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| format!("malformed fault field {line:?}"))?;
            if key.is_empty() || value.is_empty() || fields.insert(key, value).is_some() {
                return Err(format!("invalid or duplicate fault field {key:?}"));
            }
        }
        Ok(fields)
    }
    fn required_token(fields: &BTreeMap<&str, &str>) -> Result<String, String> {
        let token = fields
            .get("token")
            .ok_or_else(|| "fault scope missing token".to_string())?;
        if !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err("fault scope token is invalid".to_string());
        }
        Ok((*token).to_string())
    }
    fn required_u64(fields: &BTreeMap<&str, &str>, key: &str) -> Result<u64, String> {
        fields
            .get(key)
            .ok_or_else(|| format!("fault scope missing {key}"))?
            .parse()
            .map_err(|error| format!("invalid fault scope {key}: {error}"))
    }
    fn required_i64(fields: &BTreeMap<&str, &str>, key: &str) -> Result<i64, String> {
        fields
            .get(key)
            .ok_or_else(|| format!("fault scope missing {key}"))?
            .parse()
            .map_err(|error| format!("invalid fault scope {key}: {error}"))
    }
    fn required_usize(fields: &BTreeMap<&str, &str>, key: &str) -> Result<usize, String> {
        fields
            .get(key)
            .ok_or_else(|| format!("fault scope missing {key}"))?
            .parse()
            .map_err(|error| format!("invalid fault scope {key}: {error}"))
    }
    fn publish_new(path: &Path, contents: &[u8]) -> Result<(), String> {
        static NEXT_STAGING: AtomicU64 = AtomicU64::new(1);
        let staging = path.with_extension(format!(
            "binding-{}-{}",
            std::process::id(),
            NEXT_STAGING.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)
            .map_err(|error| format!("create {}: {error}", staging.display()))?;
        file.write_all(contents)
            .map_err(|error| format!("write {}: {error}", staging.display()))?;
        drop(file);
        if let Err(error) = fs::hard_link(&staging, path) {
            let _ = fs::remove_file(&staging);
            return Err(format!("publish {}: {error}", path.display()));
        }
        let _ = fs::remove_file(staging);
        Ok(())
    }
}

#[cfg(feature = "typed")]
pub use typed::*;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_lifecycle_kind_round_trips_its_stable_file_stem() {
        assert_eq!(QueryLifecycleFaultKind::ALL.len(), 21);
        for kind in QueryLifecycleFaultKind::ALL {
            assert_eq!(QueryLifecycleFaultKind::parse(kind.file_stem()), Some(kind));
        }
    }
    #[test]
    fn runner_parser_rejects_non_rfo_kinds() {
        assert_eq!(RUNNER_RFO_KINDS.len(), 11);
        assert_eq!(
            parse_runner_rfo_kind("terminal-outcome-suppress"),
            Some(QueryLifecycleFaultKind::TerminalOutcomeSuppress)
        );
        assert_eq!(
            parse_runner_rfo_kind("runtime-filter-contribution-ack-drop"),
            Some(QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop)
        );
        assert_eq!(parse_runner_rfo_kind("init-ack-drop"), None);
    }
    #[test]
    fn cleanup_directive_and_file_stem_are_explicitly_distinct() {
        let root = Path::new("fault-root");
        let kind = parse_cleanup_fault_directive("drop_delete_response").expect("directive");
        assert_eq!(kind.file_stem(), "drop-delete-response");
        assert_eq!(
            cleanup_trigger_path(root, kind),
            root.join("drop-delete-response.trigger")
        );
        assert!(parse_cleanup_fault_directive("drop-delete-response").is_none());
    }

    #[test]
    fn cleanup_token_is_claimed_once_at_its_explicit_path() {
        let root = unique_temp_root("cleanup");
        std::fs::create_dir_all(&root).expect("create root");
        let kind = CleanupFaultKind::DropDeleteResponse;
        std::fs::write(cleanup_trigger_path(&root, kind), "token=test\n").expect("write token");
        assert!(claim_cleanup_fault(&root, kind).expect("first claim"));
        assert!(!claim_cleanup_fault(&root, kind).expect("second claim"));
        std::fs::remove_dir_all(root).expect("remove root");
    }

    fn unique_temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "novarocks-failpoint-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ))
    }

    #[cfg(feature = "typed")]
    #[test]
    fn typed_scope_keeps_the_existing_token_text_and_identity_contract() {
        use novarocks_proto::lifecycle::{AttemptId, QueryExecutionId};

        let root = unique_temp_root("scope");
        std::fs::create_dir_all(&root).expect("create root");
        let kind = QueryLifecycleFaultKind::InitAckDrop;
        std::fs::write(arm_path(&root, 1, kind), "token=abc-123\nbackend_index=1\n")
            .expect("write arm");
        let execution_id = QueryExecutionId::new(
            novarocks_types::QueryId::new(7, 9),
            AttemptId::new(1).expect("attempt"),
        )
        .expect("execution id");
        let scope = bind_armed_fault(&root, kind, execution_id, 1, 17, 23)
            .expect("bind")
            .expect("armed");
        assert_eq!(scope.token, "abc-123");
        assert_eq!(
            std::fs::read_to_string(trigger_path(&root, 1, kind)).expect("trigger"),
            "token=abc-123\nexecution_hi=7\nexecution_lo=9\nattempt=1\nbackend_index=1\nbackend_id=17\nstart_epoch=23\n"
        );
        assert!(
            claim_matching_fault(&root, kind, execution_id, 1, 17, 23)
                .expect("claim")
                .is_some()
        );
        std::fs::remove_dir_all(root).expect("remove root");
    }

    #[cfg(feature = "typed")]
    #[test]
    fn receiver_agnostic_contribution_fault_claims_once_for_the_exact_execution() {
        use novarocks_proto::lifecycle::{AttemptId, QueryExecutionId};

        let root = unique_temp_root("receiver-agnostic");
        std::fs::create_dir_all(&root).expect("create root");
        let kind = QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop;
        std::fs::write(
            arm_path(&root, 1, kind),
            "token=ack-drop\nbackend_index=1\n",
        )
        .expect("write arm");
        let execution_id = QueryExecutionId::new(
            novarocks_types::QueryId::new(7, 9),
            AttemptId::new(1).expect("attempt"),
        )
        .expect("execution id");
        let other_execution_id = QueryExecutionId::new(
            novarocks_types::QueryId::new(7, 10),
            AttemptId::new(1).expect("attempt"),
        )
        .expect("other execution id");
        bind_armed_fault(&root, kind, execution_id, 1, 17, 23)
            .expect("bind")
            .expect("armed");

        assert!(
            claim_matching_receiver_agnostic_fault(&root, kind, other_execution_id)
                .expect("different query cannot claim")
                .is_none()
        );
        let claimed = claim_matching_receiver_agnostic_fault(&root, kind, execution_id)
            .expect("matching accepted receiver can claim")
            .expect("matching execution claims");
        assert_eq!(claimed.backend_index, 1);
        assert_eq!(claimed.backend_id, 17);
        assert!(
            claim_matching_receiver_agnostic_fault(&root, kind, execution_id)
                .expect("second receiver observes consumed token")
                .is_none()
        );
        std::fs::remove_dir_all(root).expect("remove root");
    }
}
