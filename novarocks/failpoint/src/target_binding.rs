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

//! Neutral exact statement-to-execution evidence for finite owner transports.
//!
//! Reading this record grants no control. Each domain must independently
//! validate its session grant, token, process, action and once budget.
use crate::session::invalid;
use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};
use std::path::Path;

const MAX_BINDING_BYTES: usize = 4096;

// Both typed writers validate and serialize their own envelope before this I/O.
fn publish_private_record(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    // Publish complete bytes without replacing an existing immutable record.
    // A reader may observe the canonical path while this publisher is writing.
    static NEXT_STAGING: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let staging = path.with_extension(format!(
        "private-record-{}-{}",
        std::process::id(),
        NEXT_STAGING.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let mut file = options.open(&staging)?;
    let publication = file
        .write_all(bytes)
        .and_then(|()| file.sync_all())
        .and_then(|()| std::fs::hard_link(&staging, path));
    drop(file);
    let cleanup = std::fs::remove_file(&staging);
    publication?;
    cleanup
}
#[cfg(novarocks_dev_controls)]
fn read_private_record(path: &Path) -> io::Result<Vec<u8>> {
    use std::io::Read;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() > MAX_BINDING_BYTES as u64 {
        return Err(invalid(
            "exact statement binding must be a bounded private file",
        ));
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        if metadata.mode() & 0o077 != 0 {
            return Err(invalid("exact statement binding is not private"));
        }
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("exact statement binding is not a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_BINDING_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_BINDING_BYTES {
        return Err(invalid("exact statement binding exceeds byte bound"));
    }
    Ok(bytes)
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatementExecutionBinding {
    pub schema_version: u32,
    pub target_session_id: String,
    pub statement_run_token: String,
    pub backend_index: usize,
    pub backend_process_id: String,
    pub query_id_hi: i64,
    pub query_id_lo: i64,
    pub attempt_id: u64,
}
impl StatementExecutionBinding {
    pub fn new(
        target_session_id: String,
        statement_run_token: String,
        backend_index: usize,
        process: BackendProcessId,
        execution: QueryExecutionId,
    ) -> io::Result<Self> {
        let binding = Self {
            schema_version: 1,
            target_session_id,
            statement_run_token,
            backend_index,
            backend_process_id: process.to_string(),
            query_id_hi: execution.query_id().high(),
            query_id_lo: execution.query_id().low(),
            attempt_id: execution.attempt_id().get(),
        };
        binding.validate()?;
        Ok(binding)
    }
    pub fn execution(&self) -> io::Result<QueryExecutionId> {
        QueryExecutionId::new(
            QueryId::new(self.query_id_hi, self.query_id_lo),
            AttemptId::new(self.attempt_id)
                .map_err(|_| invalid("invalid nominated attempt identity"))?,
        )
        .map_err(|_| invalid("invalid nominated query execution identity"))
    }
    pub fn process(&self) -> io::Result<BackendProcessId> {
        self.backend_process_id
            .parse()
            .map_err(|_| invalid("invalid nominated backend process identity"))
    }
    pub fn validate(&self) -> io::Result<()> {
        fn bounded_id(id: &str) -> bool {
            !id.is_empty()
                && id.len() <= 256
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        }
        if self.schema_version != 1
            || !bounded_id(&self.target_session_id)
            || !bounded_id(&self.statement_run_token)
        {
            return Err(invalid("invalid nominated statement binding envelope"));
        }
        self.execution()?;
        self.process()?;
        Ok(())
    }
    /// Exclusive publication: another generation cannot replace an old nomination.
    pub fn write_private(&self, path: &Path) -> io::Result<()> {
        self.validate()?;
        let bytes =
            serde_json::to_vec(self).map_err(|_| invalid("encode exact statement binding"))?;
        if bytes.len() > MAX_BINDING_BYTES {
            return Err(invalid("exact statement binding exceeds byte bound"));
        }
        publish_private_record(path, &bytes)
    }
    #[cfg(novarocks_dev_controls)]
    pub fn read_private(path: &Path) -> io::Result<Self> {
        let bytes = read_private_record(path)?;
        let binding: Self = serde_json::from_slice(&bytes)
            .map_err(|_| invalid("invalid exact statement binding"))?;
        binding.validate()?;
        Ok(binding)
    }
}

/// Narrowing evidence from the FE's original frozen writer manifest.
/// This record grants no action; the role must still claim its exact grant.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatementTaskBinding {
    pub statement: StatementExecutionBinding,
    pub stage_id: u32,
    pub task_id: u32,
    pub remote_destination_process_id: String,
}
impl StatementTaskBinding {
    pub fn new(
        statement: StatementExecutionBinding,
        stage: novarocks_types::StageId,
        task: novarocks_types::TaskId,
        remote_destination: BackendProcessId,
    ) -> io::Result<Self> {
        let selected = Self {
            statement,
            stage_id: stage.get(),
            task_id: task.get(),
            remote_destination_process_id: remote_destination.to_string(),
        };
        selected.validate()?;
        Ok(selected)
    }
    fn validate(&self) -> io::Result<()> {
        self.statement.validate()?;
        novarocks_types::StageId::new(self.stage_id)
            .map_err(|_| invalid("invalid selected stage"))?;
        novarocks_types::TaskId::new(self.task_id).map_err(|_| invalid("invalid selected task"))?;
        let destination: BackendProcessId = self
            .remote_destination_process_id
            .parse()
            .map_err(|_| invalid("invalid selected remote destination"))?;
        if destination == self.statement.process()? {
            return Err(invalid("selected writer destination is not remote"));
        }
        Ok(())
    }
    pub fn write_private(&self, path: &Path) -> io::Result<()> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| invalid("encode exact Task binding"))?;
        if bytes.len() > MAX_BINDING_BYTES {
            return Err(invalid("exact Task binding exceeds byte bound"));
        }
        publish_private_record(path, &bytes)
    }
    #[cfg(novarocks_dev_controls)]
    pub fn read_private(path: &Path) -> io::Result<Self> {
        let bytes = read_private_record(path)?;
        let selected: Self =
            serde_json::from_slice(&bytes).map_err(|_| invalid("invalid exact Task binding"))?;
        selected.validate()?;
        Ok(selected)
    }
}

#[cfg(all(test, novarocks_dev_controls))]
mod tests {
    use super::*;
    #[test]
    fn nomination_is_exact_private_and_cannot_be_republished_for_another_generation() {
        use std::os::unix::fs::DirBuilderExt;
        let root = std::env::temp_dir().join(format!("g2-target-{}", std::process::id()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let path = root.join("binding.json");
        let process = BackendProcessId::new_v7();
        let execution =
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(3).unwrap()).unwrap();
        let binding = StatementExecutionBinding::new(
            "target-session".into(),
            "statement-one".into(),
            2,
            process,
            execution,
        )
        .unwrap();
        binding.write_private(&path).unwrap();
        let read = StatementExecutionBinding::read_private(&path).unwrap();
        assert_eq!(read.execution().unwrap(), execution);
        assert_eq!(read.process().unwrap(), process);
        assert_eq!(read.target_session_id, "target-session");
        assert_eq!(read.statement_run_token, "statement-one");
        let replacement = StatementExecutionBinding::new(
            "other-session".into(),
            "statement-two".into(),
            2,
            BackendProcessId::new_v7(),
            execution,
        )
        .unwrap();
        assert!(replacement.write_private(&path).is_err());
        assert_eq!(
            StatementExecutionBinding::read_private(&path)
                .unwrap()
                .process()
                .unwrap(),
            process
        );
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn concurrent_publication_is_complete_private_and_never_clobbers() {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        use std::sync::Barrier;

        let root = std::env::temp_dir().join(format!(
            "g2-target-concurrent-{}",
            BackendProcessId::new_v7()
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let path = root.join("binding.json");
        let execution =
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(3).unwrap()).unwrap();
        let first = StatementExecutionBinding::new(
            "first-session".into(),
            "first-statement".into(),
            0,
            BackendProcessId::new_v7(),
            execution,
        )
        .unwrap();
        let second = StatementExecutionBinding::new(
            "second-session".into(),
            "second-statement".into(),
            0,
            BackendProcessId::new_v7(),
            execution,
        )
        .unwrap();
        assert_eq!(
            StatementExecutionBinding::read_private(&path)
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        let start = Barrier::new(3);
        std::thread::scope(|scope| {
            let one = scope.spawn(|| {
                start.wait();
                first.write_private(&path)
            });
            let two = scope.spawn(|| {
                start.wait();
                second.write_private(&path)
            });
            start.wait();
            // Observe actual publication; incomplete JSON is never retried.
            while !one.is_finished() || !two.is_finished() {
                match StatementExecutionBinding::read_private(&path) {
                    Ok(binding) => assert!(binding == first || binding == second),
                    Err(error) => assert_eq!(error.kind(), io::ErrorKind::NotFound),
                }
                std::thread::yield_now();
            }
            let outcomes = [one.join().unwrap(), two.join().unwrap()];
            assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
            let loser = outcomes
                .iter()
                .find_map(|result| result.as_ref().err())
                .unwrap();
            assert_eq!(loser.kind(), io::ErrorKind::AlreadyExists);
        });
        let winner = StatementExecutionBinding::read_private(&path).unwrap();
        assert!(winner == first || winner == second);
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        assert!(first.write_private(&path).is_err());
        assert_eq!(
            StatementExecutionBinding::read_private(&path).unwrap(),
            winner
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn selected_task_record_is_private_exact_and_requires_a_remote_destination() {
        let root =
            std::env::temp_dir().join(format!("g2-selected-task-{}", BackendProcessId::new_v7()));
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let path = root.join("selected.task.json");
        let process = BackendProcessId::new_v7();
        let statement = StatementExecutionBinding::new(
            "selected-session".into(),
            "selected-statement".into(),
            2,
            process,
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).unwrap()).unwrap(),
        )
        .unwrap();
        let stage = novarocks_types::StageId::new(3).unwrap();
        let task = novarocks_types::TaskId::new(4).unwrap();
        assert!(StatementTaskBinding::new(statement.clone(), stage, task, process).is_err());
        let selected =
            StatementTaskBinding::new(statement, stage, task, BackendProcessId::new_v7()).unwrap();
        selected.write_private(&path).unwrap();
        assert_eq!(StatementTaskBinding::read_private(&path).unwrap(), selected);
        assert!(selected.write_private(&path).is_err());
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let mut wrong = selected.clone();
        wrong.task_id = 0;
        assert!(wrong.write_private(&root.join("invalid.json")).is_err());
        assert_eq!(StatementTaskBinding::read_private(&path).unwrap(), selected);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn malformed_or_foreign_fields_never_turn_into_a_valid_exact_target() {
        let binding = StatementExecutionBinding::new(
            "session".into(),
            "statement".into(),
            0,
            BackendProcessId::new_v7(),
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).unwrap()).unwrap(),
        )
        .unwrap();
        let mut wrong = binding.clone();
        wrong.attempt_id = 0;
        assert!(wrong.validate().is_err());
        wrong = binding.clone();
        wrong.backend_process_id = "not-a-native-process".into();
        assert!(wrong.validate().is_err());
        wrong = binding.clone();
        wrong.statement_run_token = "../../another".into();
        assert!(wrong.validate().is_err());
        let mut value = serde_json::to_value(binding).unwrap();
        value["activate"] = "any-query".into();
        assert!(serde_json::from_value::<StatementExecutionBinding>(value).is_err());
    }
}

/// Exact FE statement nomination carried only in the existing local request scope.
/// This is evidence, not authority. A provider validates its own finite grant and
/// freezes the target before operations start; it never adopts a later generation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StatementWorkNomination {
    pub target_session_id: String,
    pub statement_run_token: String,
    pub frontend_process_id: String,
    pub connection_id: u32,
    pub session_key: u64,
    pub statement_generation: u64,
}
impl StatementWorkNomination {
    pub fn frontend_process(&self) -> io::Result<novarocks_types::FrontendProcessId> {
        self.frontend_process_id
            .parse()
            .map_err(|_| invalid("invalid nominated frontend process identity"))
    }
    pub fn validate(&self) -> io::Result<()> {
        let identifier = |id: &str| {
            !id.is_empty()
                && id.len() <= 256
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        };
        if !identifier(&self.target_session_id)
            || !identifier(&self.statement_run_token)
            || self.connection_id == 0
            || self.session_key == 0
            || self.statement_generation == 0
        {
            return Err(invalid("invalid exact statement work nomination"));
        }
        self.frontend_process()?;
        Ok(())
    }
}

/// Immutable FE request input captured before a StatementToken exists.
/// This data grants no control and never invents a statement or execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatementInputNomination {
    pub target_session_id: String,
    pub target_launch_id: String,
    pub statement_run_token: String,
    pub frontend_process_id: String,
    pub connection_id: u32,
    pub session_key: u64,
    pub sql_sha256: [u8; 32],
}
impl StatementInputNomination {
    pub fn frontend_process(&self) -> io::Result<novarocks_types::FrontendProcessId> {
        self.frontend_process_id
            .parse()
            .map_err(|_| invalid("invalid statement input frontend process identity"))
    }
    pub fn validate(&self) -> io::Result<()> {
        let identifier = |id: &str| {
            !id.is_empty()
                && id.len() <= 256
                && id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        };
        if !identifier(&self.target_session_id)
            || !identifier(&self.target_launch_id)
            || !identifier(&self.statement_run_token)
            || self.connection_id == 0
            || self.session_key == 0
        {
            return Err(invalid("invalid exact statement input nomination"));
        }
        self.frontend_process()?;
        Ok(())
    }
    /// The domain separately compares actual input with its frozen SQL digest.
    /// The existing work record has no digest or launch field to infer.
    pub fn validate_statement(&self, statement: &StatementWorkNomination) -> io::Result<()> {
        self.validate()?;
        statement.validate()?;
        if self.target_session_id != statement.target_session_id
            || self.statement_run_token != statement.statement_run_token
            || self.frontend_process_id != statement.frontend_process_id
            || self.connection_id != statement.connection_id
            || self.session_key != statement.session_key
        {
            return Err(invalid(
                "actual statement does not extend its exact input nomination",
            ));
        }
        Ok(())
    }
}

/// Mechanical output of one authenticated role Hello; it grants no action.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RoleProcessReceipt {
    pub schema_version: u32,
    pub session_id: String,
    pub launch_id: String,
    pub role: crate::session::SessionRole,
    pub process_id: String,
}
impl RoleProcessReceipt {
    pub fn from_hello(
        declaration: &crate::session::SessionDeclaration,
        process_id: String,
    ) -> io::Result<Self> {
        let receipt = Self {
            schema_version: 1,
            session_id: declaration.session_id.clone(),
            launch_id: declaration.launch_id.clone(),
            role: declaration.role,
            process_id,
        };
        receipt.validate()?;
        Ok(receipt)
    }
    pub fn validate(&self) -> io::Result<()> {
        let id = |s: &str| {
            !s.is_empty()
                && s.len() <= 128
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        };
        if self.schema_version != 1 || !id(&self.session_id) || !id(&self.launch_id) {
            return Err(invalid("invalid authenticated role receipt envelope"));
        }
        match self.role {
            crate::session::SessionRole::Fe => {
                self.process_id
                    .parse::<novarocks_types::FrontendProcessId>()
                    .map_err(|_| invalid("invalid role receipt frontend process"))?;
            }
            crate::session::SessionRole::Be => {
                self.backend_process()?;
            }
        }
        Ok(())
    }
    pub fn backend_process(&self) -> io::Result<BackendProcessId> {
        if self.role != crate::session::SessionRole::Be {
            return Err(invalid("role receipt does not name a backend"));
        }
        self.process_id
            .parse()
            .map_err(|_| invalid("invalid role receipt backend process"))
    }
    pub fn write_private(&self, path: &Path) -> io::Result<()> {
        self.validate()?;
        let bytes =
            serde_json::to_vec(self).map_err(|_| invalid("encode authenticated role receipt"))?;
        if bytes.len() > MAX_BINDING_BYTES {
            return Err(invalid("authenticated role receipt exceeds its bound"));
        }
        publish_private_record(path, &bytes)
    }
    pub fn read_private(path: &Path) -> io::Result<Self> {
        use std::io::Read;
        let meta = std::fs::symlink_metadata(path)?;
        if !meta.is_file() || meta.len() > MAX_BINDING_BYTES as u64 {
            return Err(invalid("role receipt is not a bounded regular file"));
        }
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
            if meta.mode() & 0o077 != 0 {
                return Err(invalid("role receipt is not private"));
            }
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(path)?;
        let opened = file.metadata()?;
        if !opened.is_file() || opened.len() > MAX_BINDING_BYTES as u64 {
            return Err(invalid("role receipt is not a bounded regular file"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if opened.mode() & 0o077 != 0 {
                return Err(invalid("role receipt is not private"));
            }
        }
        let mut bytes = Vec::new();
        file.take(MAX_BINDING_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_BINDING_BYTES {
            return Err(invalid("role receipt exceeds its bound"));
        }
        let receipt: Self = serde_json::from_slice(&bytes)
            .map_err(|_| invalid("invalid authenticated role receipt"))?;
        receipt.validate()?;
        Ok(receipt)
    }
}

#[cfg(all(test, unix))]
mod role_receipt_tests {
    use super::{MAX_BINDING_BYTES, RoleProcessReceipt};
    use crate::session::SessionRole;
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt, symlink};

    struct PrivateRoot(std::path::PathBuf);
    impl PrivateRoot {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
            let next = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("g2-receipt-{}-{next}", std::process::id()));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&path)
                .unwrap();
            Self(path)
        }
    }
    impl Drop for PrivateRoot {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn authenticated_role_receipt_publication_never_replaces_the_original() {
        let root = PrivateRoot::new();
        let path = root.0.join("process.json");
        let first = RoleProcessReceipt {
            schema_version: 1,
            session_id: "ordinary-runner-session".into(),
            launch_id: "original-backend-launch".into(),
            role: SessionRole::Be,
            process_id: novarocks_types::BackendProcessId::new_v7().to_string(),
        };
        let replacement = RoleProcessReceipt {
            process_id: novarocks_types::BackendProcessId::new_v7().to_string(),
            ..first.clone()
        };
        first.write_private(&path).unwrap();
        assert_eq!(
            replacement.write_private(&path).unwrap_err().kind(),
            std::io::ErrorKind::AlreadyExists
        );
        assert_eq!(RoleProcessReceipt::read_private(&path).unwrap(), first);
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(std::fs::read_dir(&root.0).unwrap().count(), 1);
    }

    #[test]
    fn ordinary_runner_decodes_only_a_bounded_private_regular_receipt() {
        let root = PrivateRoot::new();
        let receipt = RoleProcessReceipt {
            schema_version: 1,
            session_id: "ordinary-runner-session".into(),
            launch_id: "original-backend-launch".into(),
            role: SessionRole::Be,
            process_id: novarocks_types::BackendProcessId::new_v7().to_string(),
        };
        let path = root.0.join("process.json");
        receipt.write_private(&path).unwrap();
        assert_eq!(RoleProcessReceipt::read_private(&path).unwrap(), receipt);

        let link = root.0.join("linked.json");
        symlink(&path, &link).unwrap();
        assert!(RoleProcessReceipt::read_private(&link).is_err());
        assert!(RoleProcessReceipt::read_private(&root.0).is_err());

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(RoleProcessReceipt::read_private(&path).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(&path, vec![b' '; MAX_BINDING_BYTES + 1]).unwrap();
        assert!(RoleProcessReceipt::read_private(&path).is_err());
    }
}
