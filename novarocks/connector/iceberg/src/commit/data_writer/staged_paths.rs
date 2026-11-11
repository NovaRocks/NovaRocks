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

//! The original driver-local path inventory, shared only with its SDK path author.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use novarocks_spi::connector::{
    ConnectorIoCleanupFuture, ConnectorRequestContext, ConnectorStopOwner,
};

use crate::commit::write_stack::domain::IcebergCreationReceipt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CreateClaim {
    pub(crate) id: [u8; 16],
    pub(crate) identity: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObjectOwner {
    Pending,
    Ours,
    Foreign,
    Unproven,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObjectContent {
    Open,
    VerifiedComplete(IcebergCreationReceipt),
    Invalid,
    Unproven,
}

#[derive(Clone, Debug)]
struct StagedPath {
    path: String,
    claim: Option<CreateClaim>,
    owner: ObjectOwner,
    content: ObjectContent,
}

#[derive(Clone, Default)]
pub(crate) struct StagedPathJournal {
    paths: Arc<Mutex<Vec<StagedPath>>>,
    managed: bool,
    cleanup: Arc<OnceLock<CleanupRequest>>,
    stream_cleanup: Arc<Mutex<Vec<ConnectorIoCleanupFuture>>>,
    stream_exit_failures: StreamExitFailures,
}

type StreamExitFailures = Arc<Mutex<BTreeMap<String, Arc<crate::iceberg::Error>>>>;

/// An upload exit failure stays with the original path even if the object
/// itself can be attributed and its bytes verified. This observer can move
/// into a deferred abort without owning the journal's future queue.
pub(crate) struct StagedStreamExit {
    path: String,
    failures: StreamExitFailures,
}

impl StagedStreamExit {
    pub(crate) fn failed(&self, cause: crate::iceberg::Error) -> Arc<crate::iceberg::Error> {
        let original = Arc::new(cause);
        self.failures
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .entry(self.path.clone())
            .or_insert_with(|| original.clone());
        original
    }
}

struct CleanupRequest {
    context: ConnectorRequestContext,
    _stop: ConnectorStopOwner,
}

impl std::fmt::Debug for StagedPathJournal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StagedPathJournal")
            .field("managed", &self.managed)
            .finish_non_exhaustive()
    }
}

impl StagedPathJournal {
    pub(crate) fn managed() -> Self {
        Self {
            managed: true,
            ..Self::default()
        }
    }

    /// Close attribution and abort share the original writer cleanup deadline.
    pub(crate) fn cleanup_request(
        &self,
        original: &ConnectorRequestContext,
    ) -> ConnectorRequestContext {
        self.cleanup
            .get_or_init(|| {
                let budget = crate::commit::operation::OperationLimits::default().cleanup;
                let stop = ConnectorStopOwner::new();
                let context =
                    original.for_existing_obligation(Instant::now() + budget.time, stop.view());
                CleanupRequest {
                    context,
                    _stop: stop,
                }
            })
            .context
            .clone()
    }

    pub(crate) const fn requires_creation(&self) -> bool {
        self.managed
    }

    /// Register the generated path before FileIO opens it.
    pub(crate) fn record(&self, path: &str) {
        self.paths
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(StagedPath {
                path: path.to_owned(),
                claim: None,
                owner: if self.managed {
                    ObjectOwner::Pending
                } else {
                    ObjectOwner::Ours
                },
                content: ObjectContent::Open,
            });
    }

    pub(crate) fn begin(&self, path: &str, identity: [u8; 32]) -> Result<CreateClaim, String> {
        let mut paths = self.paths.lock().unwrap_or_else(|p| p.into_inner());
        let mut found = paths.iter_mut().filter(|entry| entry.path == path);
        let entry = found
            .next()
            .ok_or("conditional writer path was not registered")?;
        if !self.managed || entry.claim.is_some() || found.next().is_some() {
            return Err("conditional writer cannot reuse a staged path".into());
        }
        let claim = CreateClaim {
            id: *uuid::Uuid::new_v4().as_bytes(),
            identity,
        };
        entry.claim = Some(claim);
        Ok(claim)
    }

    pub(crate) fn record_result(
        &self,
        path: &str,
        claim: CreateClaim,
        owner: ObjectOwner,
        content: ObjectContent,
    ) -> Result<(), String> {
        let mut paths = self.paths.lock().unwrap_or_else(|p| p.into_inner());
        let entry = paths
            .iter_mut()
            .find(|entry| entry.path == path && entry.claim == Some(claim))
            .ok_or("conditional writer lost its staged path claim")?;
        let valid_transition = match (entry.owner, owner) {
            (
                ObjectOwner::Pending,
                ObjectOwner::Ours | ObjectOwner::Foreign | ObjectOwner::Unproven,
            ) => true,
            (ObjectOwner::Ours, ObjectOwner::Ours) => matches!(entry.content, ObjectContent::Open),
            _ => false,
        };
        if !valid_transition
            || (owner != ObjectOwner::Ours && matches!(content, ObjectContent::VerifiedComplete(_)))
            || matches!(content, ObjectContent::VerifiedComplete(receipt)
                if receipt.create_id() != claim.id || receipt.identity() != claim.identity)
        {
            return Err(
                "conditional writer reported an inconsistent ownership or content outcome".into(),
            );
        }
        entry.owner = owner;
        entry.content = content;
        Ok(())
    }

    pub(crate) fn receipt(&self, path: &str) -> Result<Option<IcebergCreationReceipt>, String> {
        if !self.managed {
            return Ok(None);
        }
        let failures = self
            .stream_exit_failures
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if failures.contains_key(path) {
            return Err("staged object stream exit remains unproven".to_owned());
        }
        let paths = self.paths.lock().unwrap_or_else(|p| p.into_inner());
        let receipt = paths
            .iter()
            .find_map(|entry| match (entry.owner, entry.content) {
                (ObjectOwner::Ours, ObjectContent::VerifiedComplete(receipt))
                    if entry.path == path =>
                {
                    Some(receipt)
                }
                _ => None,
            });
        receipt
            .map(Some)
            .ok_or_else(|| "staged object has no complete creation receipt".to_owned())
    }

    pub(crate) fn last_owned(&self) -> Option<String> {
        let failures = self
            .stream_exit_failures
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        self.paths
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .rev()
            .find(|entry| entry.owner == ObjectOwner::Ours && !failures.contains_key(&entry.path))
            .map(|entry| entry.path.clone())
    }

    pub(crate) fn has_unproven(&self) -> bool {
        self.paths
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .any(|entry| entry.owner == ObjectOwner::Unproven)
    }

    pub(crate) fn observe_stream_exit(&self, path: &str) -> StagedStreamExit {
        StagedStreamExit {
            path: path.to_owned(),
            failures: self.stream_exit_failures.clone(),
        }
    }

    pub(crate) fn stream_exit_failure(&self, path: &str) -> Option<Arc<crate::iceberg::Error>> {
        self.stream_exit_failures
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(path)
            .cloned()
    }

    pub(crate) fn first_stream_exit_failure(&self) -> Option<(String, Arc<crate::iceberg::Error>)> {
        self.stream_exit_failures
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .first_key_value()
            .map(|(path, error)| (path.clone(), error.clone()))
    }

    /// Retain a dropped SDK stream in its original writer when no nested I/O
    /// position exists. The writer's actual abort path awaits it before deletes.
    pub(crate) fn retain_stream_cleanup(&self, cleanup: ConnectorIoCleanupFuture) {
        self.stream_cleanup
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(cleanup);
    }

    pub(crate) fn take_stream_cleanup(&self) -> Option<ConnectorIoCleanupFuture> {
        self.stream_cleanup
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .pop()
    }

    /// Confirm deletion of the same owned object selected by the cleanup owner.
    pub(crate) fn confirm_deleted(&self, path: &str) -> Result<(), String> {
        let failures = self
            .stream_exit_failures
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut paths = self.paths.lock().unwrap_or_else(|p| p.into_inner());
        let index = paths.iter().rposition(|entry| {
            entry.owner == ObjectOwner::Ours && !failures.contains_key(&entry.path)
        });
        let Some(index) = index.filter(|index| paths[*index].path == path) else {
            return Err("staged path journal changed during exclusive cleanup".to_owned());
        };
        paths.remove(index);
        Ok(())
    }

    /// The SDK may delete its own empty Parquet output before returning.
    pub(crate) fn remove_owned(&self, path: &str) -> Result<(), String> {
        let failures = self
            .stream_exit_failures
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if failures.contains_key(path) {
            return Err("managed SDK delete has no proven stream exit".to_owned());
        }
        let mut paths = self.paths.lock().unwrap_or_else(|p| p.into_inner());
        let index = paths
            .iter()
            .position(|entry| entry.path == path && entry.owner == ObjectOwner::Ours)
            .ok_or("managed SDK delete lost object ownership")?;
        paths.remove(index);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Vec<String> {
        self.paths
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|entry| entry.path.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use novarocks_spi::connector::ConnectorOperationControl;
    use novarocks_spi::connector::write_stack::ConnectorWriterPhysicalContext;

    use super::*;
    use crate::iceberg::{Error, ErrorKind};

    fn claim(journal: &StagedPathJournal, path: &str) -> CreateClaim {
        journal.record(path);
        journal.begin(path, [4; 32]).unwrap()
    }

    fn receipt(claim: CreateClaim) -> IcebergCreationReceipt {
        IcebergCreationReceipt::try_new(
            ConnectorWriterPhysicalContext::new([1; 16], 2, [3; 16], 4, 5),
            claim.id,
            claim.identity,
            6,
            [7; 32],
        )
        .unwrap()
    }

    #[test]
    fn journal_keeps_cleanup_ownership_independent_from_seal_content() {
        let journal = StagedPathJournal::managed();
        for (path, owner, content) in [
            ("ours-invalid", ObjectOwner::Ours, ObjectContent::Invalid),
            (
                "ours-incomplete",
                ObjectOwner::Ours,
                ObjectContent::Unproven,
            ),
            ("unknown", ObjectOwner::Unproven, ObjectContent::Unproven),
            ("foreign", ObjectOwner::Foreign, ObjectContent::Unproven),
        ] {
            let claim = claim(&journal, path);
            assert!(
                journal
                    .record_result(
                        path,
                        claim,
                        ObjectOwner::Foreign,
                        ObjectContent::VerifiedComplete(receipt(claim)),
                    )
                    .is_err()
            );
            journal.record_result(path, claim, owner, content).unwrap();
            assert!(journal.receipt(path).is_err());
        }

        let complete = claim(&journal, "complete");
        let complete_receipt = receipt(complete);
        journal
            .record_result("complete", complete, ObjectOwner::Ours, ObjectContent::Open)
            .unwrap();
        assert!(journal.receipt("complete").is_err());
        journal
            .record_result(
                "complete",
                complete,
                ObjectOwner::Ours,
                ObjectContent::VerifiedComplete(complete_receipt),
            )
            .unwrap();
        assert_eq!(journal.receipt("complete").unwrap(), Some(complete_receipt));
        for rejected in ["unknown", "foreign"] {
            assert!(journal.confirm_deleted(rejected).is_err());
            assert!(journal.remove_owned(rejected).is_err());
        }
        for owned in ["complete", "ours-incomplete", "ours-invalid"] {
            assert_eq!(journal.last_owned().as_deref(), Some(owned));
            journal.confirm_deleted(owned).unwrap();
        }
        assert!(journal.last_owned().is_none());
        assert!(journal.has_unproven());
        assert_eq!(journal.snapshot(), ["unknown", "foreign"]);
    }

    #[test]
    fn stream_exit_failure_stays_with_its_exact_path_across_cleanups() {
        let journal = StagedPathJournal::managed();
        let healthy = claim(&journal, "healthy");
        journal
            .record_result(
                "healthy",
                healthy,
                ObjectOwner::Ours,
                ObjectContent::Invalid,
            )
            .unwrap();
        let blocked = claim(&journal, "blocked");
        let abort = journal.observe_stream_exit("blocked").failed(
            Error::new(ErrorKind::Unexpected, "original abort denied").with_source(
                std::io::Error::new(std::io::ErrorKind::PermissionDenied, "abort denied"),
            ),
        );
        journal
            .record_result("blocked", blocked, ObjectOwner::Ours, ObjectContent::Open)
            .unwrap();
        journal
            .record_result(
                "blocked",
                blocked,
                ObjectOwner::Ours,
                ObjectContent::VerifiedComplete(receipt(blocked)),
            )
            .unwrap();
        assert_eq!(journal.last_owned().as_deref(), Some("healthy"));
        assert!(journal.receipt("blocked").is_err());
        assert!(journal.confirm_deleted("blocked").is_err());
        assert!(journal.remove_owned("blocked").is_err());
        journal.confirm_deleted("healthy").unwrap();
        assert!(journal.last_owned().is_none());
        assert!(!journal.has_unproven());
        let (path, retained) = journal.clone().first_stream_exit_failure().unwrap();
        assert_eq!(path, "blocked");
        assert!(Arc::ptr_eq(&retained, &abort));
        assert_eq!(journal.snapshot(), ["blocked"]);
    }

    #[test]
    fn attribution_and_later_cleanup_share_the_first_immutable_deadline() {
        let stop = ConnectorStopOwner::new();
        let original = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            stop.view(),
            1024,
            2048,
        )
        .unwrap();
        stop.request_stop();
        let journal = StagedPathJournal::managed();
        let first = journal.cleanup_request(&original);
        assert!(original.check_active().is_err());
        assert!(first.check_active().is_ok());
        let earlier = original.with_earlier_deadline(Instant::now() - Duration::from_secs(1));
        let subsequent = journal.clone().cleanup_request(&earlier);
        assert_eq!(subsequent.deadline(), first.deadline());
        assert!(subsequent.check_active().is_ok());
    }
}
