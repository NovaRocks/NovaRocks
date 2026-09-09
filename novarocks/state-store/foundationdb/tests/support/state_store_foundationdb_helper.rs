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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_state_store_api::{
    AttemptOutcome, CommitObservation, CommitOutcome, Key, KeyRange,
    Precondition as StorePrecondition, RangeRequest, StateRecord, StateStore, Value, VersionToken,
    WriteTransaction,
};
use novarocks_state_store_foundationdb::{
    FoundationDbClientConfig, FoundationDbProviderTestHarness, FoundationDbTestLimitOverrides,
    FoundationDbTestProviderConfig, FoundationDbTestStoreConfig, arm_next_foundationdb_commit,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// A helper-local name for one write.
///
/// It is deliberately not a store identity: an attempt is issued by the opened
/// instance and cannot be built from bytes, so nothing a peer process sends can
/// address a write this process made. The handle only says which of *this*
/// helper's transactions a command is about.
pub type Handle = Uuid;

#[derive(Debug, Eq, PartialEq)]
pub enum Command {
    Open {
        cluster_id: String,
        keyspace_id: Uuid,
    },
    Begin {
        handle: Uuid,
        description: String,
    },
    Get {
        handle: Uuid,
        key: Vec<u8>,
    },
    Range {
        handle: Uuid,
        start: Vec<u8>,
        end: Vec<u8>,
        direction: Direction,
        page_size: usize,
    },
    Put {
        handle: Uuid,
        key: Vec<u8>,
        value: Vec<u8>,
        precondition: Precondition,
    },
    Delete {
        handle: Uuid,
        key: Vec<u8>,
        precondition: Precondition,
    },
    Commit {
        handle: Uuid,
        hold_pre_native: bool,
    },
    Resolve {
        handle: Uuid,
    },
    Release {
        handle: Uuid,
    },
    Shutdown,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum Direction {
    Forward,
    Reverse,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Precondition {
    Any,
    Absent,
    Present,
    Version(Vec<u8>),
}

#[derive(Deserialize)]
#[serde(tag = "command", deny_unknown_fields)]
enum RawCommand {
    Open {
        cluster_id: String,
        keyspace_id: Uuid,
    },
    Begin {
        handle: Uuid,
        description: String,
    },
    Get {
        handle: Uuid,
        key: String,
    },
    Range {
        handle: Uuid,
        start: String,
        end: String,
        direction: Direction,
        page_size: usize,
    },
    Put {
        handle: Uuid,
        key: String,
        value: String,
        precondition: RawPrecondition,
    },
    Delete {
        handle: Uuid,
        key: String,
        precondition: RawPrecondition,
    },
    Commit {
        handle: Uuid,
        #[serde(default)]
        hold_pre_native: bool,
    },
    Resolve {
        handle: Uuid,
    },
    Release {
        handle: Uuid,
    },
    Shutdown,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawPrecondition {
    Name(PreconditionName),
    Version(RawVersionPrecondition),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawVersionPrecondition {
    version: String,
}

#[derive(Deserialize)]
enum PreconditionName {
    Any,
    Absent,
    Present,
}

pub fn parse_command(line: &str) -> Result<Command, String> {
    let raw: RawCommand =
        serde_json::from_str(line).map_err(|error| format!("invalid command JSON: {error}"))?;
    match raw {
        RawCommand::Open {
            cluster_id,
            keyspace_id,
        } => Ok(Command::Open {
            cluster_id,
            keyspace_id,
        }),
        RawCommand::Begin {
            handle,
            description,
        } => Ok(Command::Begin {
            handle,
            description,
        }),
        RawCommand::Get { handle, key } => Ok(Command::Get {
            handle,
            key: decode_hex("key", &key)?,
        }),
        RawCommand::Range {
            handle,
            start,
            end,
            direction,
            page_size,
        } => Ok(Command::Range {
            handle,
            start: decode_hex("range start", &start)?,
            end: decode_hex("range end", &end)?,
            direction,
            page_size,
        }),
        RawCommand::Put {
            handle,
            key,
            value,
            precondition,
        } => Ok(Command::Put {
            handle,
            key: decode_hex("key", &key)?,
            value: decode_hex("value", &value)?,
            precondition: decode_precondition(precondition)?,
        }),
        RawCommand::Delete {
            handle,
            key,
            precondition,
        } => Ok(Command::Delete {
            handle,
            key: decode_hex("key", &key)?,
            precondition: decode_precondition(precondition)?,
        }),
        RawCommand::Commit {
            handle,
            hold_pre_native,
        } => Ok(Command::Commit {
            handle,
            hold_pre_native,
        }),
        RawCommand::Resolve { handle } => Ok(Command::Resolve { handle }),
        RawCommand::Release { handle } => Ok(Command::Release { handle }),
        RawCommand::Shutdown => Ok(Command::Shutdown),
    }
}

fn decode_precondition(raw: RawPrecondition) -> Result<Precondition, String> {
    match raw {
        RawPrecondition::Name(PreconditionName::Any) => Ok(Precondition::Any),
        RawPrecondition::Name(PreconditionName::Absent) => Ok(Precondition::Absent),
        RawPrecondition::Name(PreconditionName::Present) => Ok(Precondition::Present),
        RawPrecondition::Version(RawVersionPrecondition { version }) => Ok(Precondition::Version(
            decode_hex("precondition version", &version)?,
        )),
    }
}

fn decode_hex(field: &str, encoded: &str) -> Result<Vec<u8>, String> {
    hex::decode(encoded).map_err(|error| format!("{field} must be hexadecimal: {error}"))
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Response {
    pub ok: bool,
    pub pid: u32,
    pub event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<RecordResponse>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub records: Vec<RecordResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RecordResponse {
    pub key: String,
    pub value: String,
    pub version: String,
}

impl Response {
    fn success(event: impl Into<String>) -> Self {
        Self {
            ok: true,
            pid: std::process::id(),
            event: event.into(),
            outcome: None,
            resolution: None,
            revision: None,
            record: None,
            records: Vec::new(),
            error: None,
        }
    }

    fn error(error: impl Into<String>) -> Self {
        Self {
            ok: false,
            pid: std::process::id(),
            event: "Error".to_owned(),
            outcome: None,
            resolution: None,
            revision: None,
            record: None,
            records: Vec::new(),
            error: Some(error.into()),
        }
    }
}

struct PendingCommit {
    control: novarocks_state_store_foundationdb::FoundationDbCommitGateControl,
    owner: JoinHandle<CommitOutcome>,
}

#[derive(Default)]
struct HelperState {
    runtime: Option<FoundationDbProviderTestHarness>,
    store: Option<Arc<dyn StateStore>>,
    transactions: HashMap<Handle, Box<dyn WriteTransaction>>,
    /// Retained past the commit, because after it the observation is the only
    /// thing that can say what happened to the attempt.
    observations: HashMap<Handle, CommitObservation>,
    pending: HashMap<Handle, PendingCommit>,
    terminal_error: Option<String>,
}

impl HelperState {
    async fn execute(&mut self, command: Command) -> Result<Response, String> {
        match command {
            Command::Open {
                cluster_id,
                keyspace_id,
            } => self.open(cluster_id, keyspace_id).await,
            Command::Begin {
                handle,
                description,
            } => self.begin(handle, description).await,
            Command::Get { handle, key } => self.get(handle, key).await,
            Command::Range {
                handle,
                start,
                end,
                direction,
                page_size,
            } => self.range(handle, start, end, direction, page_size).await,
            Command::Put {
                handle,
                key,
                value,
                precondition,
            } => self.put(handle, key, value, precondition).await,
            Command::Delete {
                handle,
                key,
                precondition,
            } => self.delete(handle, key, precondition).await,
            Command::Commit {
                handle,
                hold_pre_native,
            } => self.commit(handle, hold_pre_native).await,
            Command::Resolve { handle } => self.resolve(handle).await,
            Command::Release { handle } => self.release(handle).await,
            Command::Shutdown => self.shutdown().await,
        }
    }

    async fn open(&mut self, cluster_id: String, keyspace_id: Uuid) -> Result<Response, String> {
        if self.runtime.is_some() || self.store.is_some() {
            return Err("helper is already open".to_owned());
        }
        let cluster_file = cluster_file()?;
        let mut runtime =
            FoundationDbProviderTestHarness::boot(client_config()).map_err(display_error)?;
        let store = match runtime
            .open_store(
                FoundationDbTestStoreConfig {
                    cluster_id,
                    limits: FoundationDbTestLimitOverrides::default(),
                    provider: FoundationDbTestProviderConfig::Foundationdb {
                        cluster_file,
                        keyspace_id,
                    },
                },
                test_deadline(),
            )
            .await
        {
            Ok(store) => store,
            Err(error) => {
                let open_error = display_error(error);
                let shutdown_error = match runtime.shutdown(test_deadline()).await {
                    Ok(()) => None,
                    Err(shutdown_error) => {
                        self.runtime = Some(runtime);
                        Some(display_error(shutdown_error))
                    }
                };
                let terminal_error = helper_open_failure_error(open_error, shutdown_error);
                self.terminal_error = Some(terminal_error.clone());
                return Err(terminal_error);
            }
        };
        self.runtime = Some(runtime);
        self.store = Some(store);
        Ok(Response::success("Opened"))
    }

    async fn begin(&mut self, handle: Uuid, description: String) -> Result<Response, String> {
        if self.transactions.contains_key(&handle) || self.pending.contains_key(&handle) {
            return Err(format!("transaction {handle} is already active"));
        }
        let store = self.store()?;
        let (attempt, observation) = store.attempts().reserve().map_err(display_error)?;
        let transaction = store
            .begin_write(attempt, &description)
            .await
            .map_err(display_error)?;
        self.observations.insert(handle, observation);
        self.transactions.insert(handle, transaction);
        Ok(Response::success("Begun"))
    }

    async fn get(&mut self, handle: Uuid, raw_key: Vec<u8>) -> Result<Response, String> {
        let key = store_key(raw_key)?;
        let record = self
            .transaction_mut(handle)?
            .get(&key)
            .await
            .map_err(display_error)?;
        let mut response = Response::success("Get");
        response.record = record.map(record_response);
        Ok(response)
    }

    async fn range(
        &mut self,
        handle: Uuid,
        raw_start: Vec<u8>,
        raw_end: Vec<u8>,
        direction: Direction,
        page_size: usize,
    ) -> Result<Response, String> {
        let request = RangeRequest {
            range: KeyRange::new(store_key(raw_start)?, store_key(raw_end)?)
                .map_err(display_error)?,
            direction: match direction {
                Direction::Forward => novarocks_state_store_api::Direction::Forward,
                Direction::Reverse => novarocks_state_store_api::Direction::Reverse,
            },
            page_size,
            continuation: None,
        };
        let page = self
            .transaction_mut(handle)?
            .range(&request)
            .await
            .map_err(display_error)?;
        let mut response = Response::success("Range");
        response.records = page.records.into_iter().map(record_response).collect();
        Ok(response)
    }

    async fn put(
        &mut self,
        handle: Uuid,
        raw_key: Vec<u8>,
        raw_value: Vec<u8>,
        precondition: Precondition,
    ) -> Result<Response, String> {
        let precondition = store_precondition(precondition)?;
        self.transaction_mut(handle)?
            .put(
                store_key(raw_key)?,
                Value::try_from(Bytes::from(raw_value)).map_err(display_error)?,
                precondition,
            )
            .await
            .map_err(display_error)?;
        Ok(Response::success("Staged"))
    }

    async fn delete(
        &mut self,
        handle: Uuid,
        raw_key: Vec<u8>,
        precondition: Precondition,
    ) -> Result<Response, String> {
        let key = store_key(raw_key)?;
        let precondition = store_precondition(precondition)?;
        self.transaction_mut(handle)?
            .delete(key, precondition)
            .await
            .map_err(display_error)?;
        Ok(Response::success("Staged"))
    }

    async fn commit(&mut self, handle: Uuid, hold_pre_native: bool) -> Result<Response, String> {
        let transaction = self
            .transactions
            .remove(&handle)
            .ok_or_else(|| format!("transaction {handle} is not active"))?;
        if !hold_pre_native {
            return Ok(commit_response(transaction.commit().await));
        }
        let control = arm_next_foundationdb_commit(true, false, false).map_err(display_error)?;
        let owner = tokio::spawn(async move { transaction.commit().await });
        control.wait_pre_native().await;
        self.pending
            .insert(handle, PendingCommit { control, owner });
        Ok(Response::success("CommitHeld"))
    }

    /// Answers for a write *this* helper made.
    ///
    /// There is deliberately no way to ask about a peer's write: an attempt is
    /// scoped to the instance that issued it, so an unknown handle is an error
    /// rather than a verdict of "not committed".
    async fn resolve(&self, handle: Handle) -> Result<Response, String> {
        let observation = self
            .observations
            .get(&handle)
            .ok_or_else(|| format!("handle {handle} was never issued by this process"))?;
        let outcome = observation.outcome().await.map_err(display_error)?;
        Ok(resolution_response(outcome))
    }

    async fn release(&mut self, handle: Uuid) -> Result<Response, String> {
        let pending = self
            .pending
            .remove(&handle)
            .ok_or_else(|| format!("transaction {handle} is not held"))?;
        pending.control.release_pre_native();
        let outcome = pending
            .owner
            .await
            .map_err(|error| format!("commit owner failed: {error}"))?;
        Ok(commit_response(outcome))
    }

    async fn shutdown(&mut self) -> Result<Response, String> {
        self.transactions.clear();
        self.observations.clear();
        let pending = std::mem::take(&mut self.pending);
        for (_, pending) in pending {
            pending.control.release_pre_native();
            pending
                .owner
                .await
                .map_err(|error| format!("commit owner failed during shutdown: {error}"))?;
        }
        self.store.take();
        if let Some(mut runtime) = self.runtime.take() {
            let result = runtime
                .shutdown(test_deadline())
                .await
                .map_err(display_error);
            restore_runtime_after_shutdown(&mut self.runtime, runtime, result)?;
        }
        Ok(Response::success("Shutdown"))
    }

    fn store(&self) -> Result<&Arc<dyn StateStore>, String> {
        self.store
            .as_ref()
            .ok_or_else(|| "helper is not open".to_owned())
    }

    fn transaction_mut(&mut self, handle: Uuid) -> Result<&mut Box<dyn WriteTransaction>, String> {
        self.transactions
            .get_mut(&handle)
            .ok_or_else(|| format!("transaction {handle} is not active"))
    }
}

fn test_deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

fn restore_runtime_after_shutdown<R>(
    slot: &mut Option<R>,
    runtime: R,
    result: Result<(), String>,
) -> Result<(), String> {
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            *slot = Some(runtime);
            Err(error)
        }
    }
}

fn client_config() -> FoundationDbClientConfig {
    FoundationDbClientConfig {
        disable_multi_version_client: true,
        tls_cert_path: None,
        tls_key_path: None,
        tls_ca_path: None,
        tls_verify_peers: None,
        tls_password: None,
    }
}

fn cluster_file() -> Result<PathBuf, String> {
    std::env::var("NOVAROCKS_FDB_CLUSTER_FILE")
        .map(PathBuf::from)
        .map_err(|_| "NOVAROCKS_FDB_CLUSTER_FILE is required".to_owned())
}

fn store_key(raw: Vec<u8>) -> Result<Key, String> {
    Key::try_from(Bytes::from(raw)).map_err(display_error)
}

fn store_precondition(precondition: Precondition) -> Result<StorePrecondition, String> {
    match precondition {
        Precondition::Any => Ok(StorePrecondition::Any),
        Precondition::Absent => Ok(StorePrecondition::Absent),
        Precondition::Present => Ok(StorePrecondition::Present),
        Precondition::Version(raw) => Ok(StorePrecondition::Version(
            VersionToken::try_from(Bytes::from(raw)).map_err(display_error)?,
        )),
    }
}

fn record_response(record: StateRecord) -> RecordResponse {
    RecordResponse {
        key: hex::encode(record.key.as_bytes()),
        value: hex::encode(record.value.as_bytes()),
        version: hex::encode(record.version.as_bytes()),
    }
}

fn commit_response(outcome: CommitOutcome) -> Response {
    let mut response = Response::success("Commit");
    match outcome {
        CommitOutcome::Committed(receipt) => {
            response.outcome = Some("Committed".to_owned());
            response.revision = Some(hex::encode(receipt.revision.as_bytes()));
        }
        CommitOutcome::Conflict(_) => response.outcome = Some("Conflict".to_owned()),
        CommitOutcome::TransientBeforeCommit(_) => {
            response.outcome = Some("TransientBeforeCommit".to_owned());
        }
        CommitOutcome::DefiniteFailure(_) => {
            response.outcome = Some("DefiniteFailure".to_owned());
        }
        CommitOutcome::CommitUnknown(_) => response.outcome = Some("CommitUnknown".to_owned()),
    }
    response
}

fn resolution_response(outcome: AttemptOutcome) -> Response {
    let mut response = Response::success("Resolve");
    match outcome {
        AttemptOutcome::Committed(receipt) => {
            response.resolution = Some("Committed".to_owned());
            response.revision = Some(hex::encode(receipt.revision.as_bytes()));
        }
        AttemptOutcome::NotCommitted => {
            response.resolution = Some("NotCommitted".to_owned());
        }
        // Not a terminal, and deliberately not reported as one.
        AttemptOutcome::Unresolved => {
            response.resolution = Some("Unresolved".to_owned());
        }
    }
    response
}

fn display_error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn helper_open_failure_error(open_error: String, shutdown_error: Option<String>) -> String {
    match shutdown_error {
        Some(shutdown_error) => format!(
            "{open_error}; FoundationDB runtime shutdown after helper open failure also failed: {shutdown_error}"
        ),
        None => open_error,
    }
}

async fn run() -> Result<(), String> {
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();
    let mut state = HelperState::default();
    while let Some(line) = lines
        .next_line()
        .await
        .map_err(|error| format!("read command: {error}"))?
    {
        let command = parse_command(&line);
        let is_shutdown = matches!(command, Ok(Command::Shutdown));
        let response = match command {
            Ok(command) => state.execute(command).await.unwrap_or_else(Response::error),
            Err(error) => Response::error(error),
        };
        let encoded = serde_json::to_string(&response)
            .map_err(|error| format!("encode response: {error}"))?;
        stdout
            .write_all(encoded.as_bytes())
            .await
            .map_err(|error| format!("write response: {error}"))?;
        stdout
            .write_all(b"\n")
            .await
            .map_err(|error| format!("write response delimiter: {error}"))?;
        stdout
            .flush()
            .await
            .map_err(|error| format!("flush response: {error}"))?;
        if is_shutdown && response.ok {
            return Ok(());
        }
        if let Some(error) = state.terminal_error.take() {
            return Err(error);
        }
    }
    state.shutdown().await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("state-store-foundationdb-helper: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_failure_error_preserves_primary_error_and_appends_shutdown_failure() {
        assert_eq!(
            helper_open_failure_error("open failed".to_owned(), None),
            "open failed"
        );
        assert_eq!(
            helper_open_failure_error("open failed".to_owned(), Some("shutdown failed".to_owned())),
            "open failed; FoundationDB runtime shutdown after helper open failure also failed: shutdown failed"
        );
    }

    #[test]
    fn failed_shutdown_restores_runtime_owner_for_retry() {
        let mut slot = None;

        let error = restore_runtime_after_shutdown(
            &mut slot,
            7_u8,
            Err("injected shutdown failure".to_owned()),
        )
        .expect_err("shutdown failure must surface");

        assert_eq!(error, "injected shutdown failure");
        assert_eq!(slot, Some(7));
    }
}
