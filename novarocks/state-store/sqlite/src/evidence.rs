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

// Design: ADR-0143 (docs/adr/ADR-0143-state-store-answers-only-issued-attempts.md)

//! In-doubt adjudication from this provider's own commit evidence.
//!
//! # What the evidence is
//!
//! One row in `state_store_commits`, written inside the very transaction that
//! carries the business data. Either both land or neither does, so the row's
//! presence is proof the attempt committed, and its absence -- *once the writer
//! is known to have stopped* -- is proof it did not.
//!
//! # Why the durable row is not enough on its own
//!
//! A commit runs on a blocking worker the caller does not own. While that
//! worker is alive the row can still appear, so reading "no row" proves
//! nothing, and the honest answer is [`AttemptOutcome::Unresolved`]. Each
//! dispatch therefore registers a liveness ticket owned by the worker task; the
//! ticket is released when that task ends, panic included, and only then may
//! absence be read as a denial.
//!
//! # Why evidence is deleted eagerly
//!
//! An [`AttemptId`] belongs to one opened instance and cannot be reconstructed
//! from bytes, so a row can only ever be queried by the instance that wrote it.
//! Once that instance has published the attempt's terminal outcome the row
//! proves nothing anyone can still ask about, and keeping it would grow the
//! table by one row per commit forever. Rows for settled attempts are therefore
//! deleted as soon as -- and never before -- the terminal is published, and
//! rows found at open belong to a scope nobody can name any more and are
//! purged.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};

use novarocks_state_store_api::{
    AttemptId, AttemptOutcome, CommitReceipt, InDoubtAdjudicator, StateStoreError,
    StateStoreErrorKind,
};

use super::txn::{operation_error, revision_token};

/// Ceiling on releases parked for retry.
///
/// A release only fails when the database itself is unavailable, so the queue
/// exists to survive a blip, not to absorb an outage. Dropping the oldest entry
/// leaves one row behind in a file that is already failing writes; growing
/// without bound would be the worse answer.
const MAX_DEFERRED_RELEASES: usize = 1024;

type Dispatches = Arc<Mutex<HashMap<AttemptId, Arc<DispatchState>>>>;

#[derive(Debug, Default)]
struct DispatchState {
    finished: AtomicBool,
}

/// Records that one physical commit worker is still running.
///
/// The worker task owns the guard; dropping it -- by returning, by being
/// dropped, or by unwinding from a panic -- is what marks the dispatch
/// finished. Nothing else may set that flag, because nothing else knows the
/// worker has stopped touching the database.
pub(super) struct DispatchGuard {
    state: Arc<DispatchState>,
}

impl Drop for DispatchGuard {
    fn drop(&mut self) {
        self.state.finished.store(true, Ordering::Release);
    }
}

enum Liveness {
    InFlight,
    Finished,
    /// No liveness record: either the evidence was already released or this
    /// dispatch was never registered. Nothing can be proven either way.
    Unrecorded,
}

pub(super) struct SqliteCommitEvidence {
    /// A dedicated connection, so adjudicating and releasing never wait on
    /// opening a database file and never borrow a live transaction's
    /// connection.
    connection: Arc<Mutex<Connection>>,
    dispatches: Dispatches,
    /// Releases that could not be performed when they were due. Retried on the
    /// next evidence operation rather than abandoned.
    deferred: Mutex<VecDeque<AttemptId>>,
    #[cfg(test)]
    fail_next_release: AtomicBool,
}

impl SqliteCommitEvidence {
    pub(super) fn new(connection: Connection) -> Self {
        Self {
            connection: Arc::new(Mutex::new(connection)),
            dispatches: Arc::new(Mutex::new(HashMap::new())),
            deferred: Mutex::new(VecDeque::new()),
            #[cfg(test)]
            fail_next_release: AtomicBool::new(false),
        }
    }

    /// Makes the next durable release fail, without touching the database.
    ///
    /// A release only fails when the file itself is refusing writes, which is
    /// not something a test can stage without breaking the store it is
    /// inspecting.
    #[cfg(test)]
    pub(super) fn fail_next_release(&self) {
        self.fail_next_release.store(true, Ordering::Release);
    }

    /// How many releases are parked for retry.
    #[cfg(test)]
    pub(super) fn deferred_len(&self) -> usize {
        self.deferred.lock().expect("deferred releases").len()
    }

    /// Registers a dispatch and hands back the guard the worker must own.
    ///
    /// This runs before the attempt is marked dispatched, so an observer can
    /// never catch a dispatched attempt for which this provider holds no
    /// liveness record.
    pub(super) fn register_dispatch(&self, attempt: AttemptId) -> DispatchGuard {
        let state = Arc::new(DispatchState::default());
        if let Ok(mut dispatches) = self.dispatches.lock() {
            dispatches.insert(attempt, Arc::clone(&state));
        }
        DispatchGuard { state }
    }

    /// Drops the liveness record for an attempt that never reached a worker.
    pub(super) fn forget_dispatch(&self, attempt: AttemptId) {
        if let Ok(mut dispatches) = self.dispatches.lock() {
            dispatches.remove(&attempt);
        }
    }

    /// Reads the durable evidence for one attempt on the shared connection.
    ///
    /// Blocking: the caller is already on a blocking worker.
    pub(super) fn lookup_blocking(
        &self,
        attempt: AttemptId,
    ) -> Result<Option<CommitReceipt>, StateStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| internal("SQLite commit evidence connection is poisoned"))?;
        lookup_commit_on_connection(&connection, attempt)
    }

    fn liveness(&self, attempt: AttemptId) -> Liveness {
        let Ok(dispatches) = self.dispatches.lock() else {
            return Liveness::Unrecorded;
        };
        match dispatches.get(&attempt) {
            Some(state) if state.finished.load(Ordering::Acquire) => Liveness::Finished,
            Some(_) => Liveness::InFlight,
            None => Liveness::Unrecorded,
        }
    }

    fn defer(&self, attempt: AttemptId) {
        let Ok(mut deferred) = self.deferred.lock() else {
            return;
        };
        if deferred.len() >= MAX_DEFERRED_RELEASES {
            let abandoned = deferred.pop_front();
            log::warn!(
                "SQLite evidence release backlog is full; one commit row is left behind: {abandoned:?}"
            );
        }
        deferred.push_back(attempt);
    }

    fn take_deferred(&self) -> Option<AttemptId> {
        self.deferred.lock().ok()?.pop_front()
    }

    /// Retries releases that could not be performed earlier.
    ///
    /// Stops at the first failure and re-queues it, so a database that is
    /// refusing writes is asked once per operation rather than spun on.
    async fn drain_deferred(&self) {
        while let Some(attempt) = self.take_deferred() {
            if self.delete_evidence(attempt).await.is_err() {
                self.defer(attempt);
                return;
            }
        }
    }

    async fn delete_evidence(&self, attempt: AttemptId) -> Result<(), StateStoreError> {
        #[cfg(test)]
        if self.fail_next_release.swap(false, Ordering::AcqRel) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "injected SQLite commit evidence release failure",
            ));
        }
        let connection = Arc::clone(&self.connection);
        let dispatches = Arc::clone(&self.dispatches);
        tokio::task::spawn_blocking(move || {
            {
                let connection = connection
                    .lock()
                    .map_err(|_| internal("SQLite commit evidence connection is poisoned"))?;
                connection
                    .execute(
                        "DELETE FROM state_store_commits WHERE attempt = ?1",
                        params![attempt_key(attempt)],
                    )
                    .map_err(|error| {
                        operation_error(&error, "failed to release SQLite commit evidence")
                    })?;
            }
            if let Ok(mut dispatches) = dispatches.lock() {
                dispatches.remove(&attempt);
            }
            Ok(())
        })
        .await
        .map_err(|_| internal("SQLite commit evidence worker failed"))?
    }
}

/// Releases an attempt's evidence from the path that witnessed its terminal.
///
/// [`novarocks_state_store_api::AttemptSupervisor::drain_abandoned_attempts`]
/// only covers attempts nobody is left to settle, so a provider that waits for
/// it accumulates one row per commit for the life of the instance. Every
/// witnessed terminal releases here instead, immediately after the outcome is
/// published and inside the async context that published it.
pub(super) async fn release_witnessed_evidence(
    evidence: &SqliteCommitEvidence,
    attempt: AttemptId,
) {
    if let Err(error) = evidence.release_evidence(attempt).await {
        // The verdict is already published, so an unreleased row is garbage
        // rather than a correctness problem: retry it on the next operation.
        log::warn!("SQLite could not release commit evidence, queued for retry: {error}");
        evidence.defer(attempt);
    }
}

#[async_trait]
impl InDoubtAdjudicator for SqliteCommitEvidence {
    async fn adjudicate(&self, attempt: AttemptId) -> Result<AttemptOutcome, StateStoreError> {
        self.drain_deferred().await;
        match self.liveness(attempt) {
            // The worker may still commit, so no amount of reading decides
            // this. Refusing here also keeps an undecidable question off the
            // blocking pool.
            Liveness::InFlight | Liveness::Unrecorded => return Ok(AttemptOutcome::Unresolved),
            Liveness::Finished => {}
        }
        let connection = Arc::clone(&self.connection);
        Ok(tokio::task::spawn_blocking(move || {
            // A read that cannot be performed is not proof that nothing
            // happened.
            let Ok(connection) = connection.lock() else {
                return AttemptOutcome::Unresolved;
            };
            match lookup_commit_on_connection(&connection, attempt) {
                Ok(Some(receipt)) => AttemptOutcome::Committed(receipt),
                // The worker has stopped and left no row inside the transaction
                // that would have carried the data, so nothing can still land.
                Ok(None) => AttemptOutcome::NotCommitted,
                Err(_) => AttemptOutcome::Unresolved,
            }
        })
        .await
        // A lost worker means the verdict could not be produced, never that the
        // attempt was denied.
        .unwrap_or(AttemptOutcome::Unresolved))
    }

    async fn release_evidence(&self, attempt: AttemptId) -> Result<(), StateStoreError> {
        if matches!(self.liveness(attempt), Liveness::InFlight) {
            // Deleting now would race the very insert that decides the attempt
            // and leave a row nobody will ever clean up. Refusing keeps the
            // attempt queued and its slot charged, which is what the supervisor
            // expects of a release it cannot yet perform.
            return Err(StateStoreError::new(
                StateStoreErrorKind::Transient,
                "SQLite commit evidence is still being written by its worker",
            ));
        }
        self.drain_deferred().await;
        self.delete_evidence(attempt).await
    }
}

/// Bytes in the durable key for one attempt.
///
/// Taken from the contract rather than restated here. Fixed width matters to
/// this provider specifically: transaction byte accounting has to state what a
/// commit will cost before it runs, and a key whose length grew with the
/// sequence number would make the same mutation cost a different amount on the
/// tenth attempt than on the ninth. A local copy of the width would let the two
/// drift and silently mis-bill every transaction.
pub(super) const ATTEMPT_KEY_BYTES: usize = AttemptId::STORAGE_KEY_BYTES;

/// The durable key for one attempt.
///
/// [`novarocks_state_store_api::InstanceScope`] is deliberately opaque, so the
/// contract's own fixed-width rendering is the key. Deriving one from `Display`
/// instead would bet on UUID formatting never changing and would produce a
/// variable-width key, which this provider cannot afford.
pub(super) fn attempt_key(attempt: AttemptId) -> Vec<u8> {
    let key = attempt.storage_key();
    debug_assert_eq!(key.len(), ATTEMPT_KEY_BYTES);
    key.into_bytes()
}

pub(super) fn lookup_commit_on_connection(
    connection: &Connection,
    attempt: AttemptId,
) -> Result<Option<CommitReceipt>, StateStoreError> {
    let revision = connection
        .query_row(
            "SELECT revision FROM state_store_commits WHERE attempt = ?1",
            params![attempt_key(attempt)],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|error| operation_error(&error, "failed to resolve SQLite commit evidence"))?;
    revision
        .map(|revision| {
            let revision = u64::try_from(revision).map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::Corruption,
                    "SQLite state store revision is malformed",
                )
            })?;
            Ok(CommitReceipt {
                attempt,
                revision: revision_token(revision),
            })
        })
        .transpose()
}

/// Drops commit evidence left behind by a previous instance.
pub(super) fn purge_stale_evidence(connection: &Connection) -> Result<(), StateStoreError> {
    connection
        .execute("DELETE FROM state_store_commits", [])
        .map_err(|error| operation_error(&error, "failed to purge stale SQLite commit evidence"))?;
    Ok(())
}

const fn internal(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::Internal, message)
}
