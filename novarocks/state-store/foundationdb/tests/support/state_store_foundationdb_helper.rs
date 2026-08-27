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
use novarocks_spi::state_store::{
    CommitOutcome, CommitResolution, Key, KeyRange, Precondition as StorePrecondition,
    RangeRequest, StateRecord, StateStore, TransactionId, Value, VersionToken, WriteTransaction,
};
use novarocks_state_store_foundationdb::{
    FoundationDbClientConfig, FoundationDbProviderTestHarness, FoundationDbTestLimitOverrides,
    FoundationDbTestProviderConfig, FoundationDbTestStoreConfig, arm_next_foundationdb_commit,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Debug, Eq, PartialEq)]
pub enum Command {
    Open {
        cluster_id: String,
        keyspace_id: Uuid,
    },
    Begin {
        transaction_id: Uuid,
        description: String,
    },
    Get {
        transaction_id: Uuid,
        key: Vec<u8>,
    },
    Range {
        transaction_id: Uuid,
        start: Vec<u8>,
        end: Vec<u8>,
        direction: Direction,
        page_size: usize,
    },
    Put {
        transaction_id: Uuid,
        key: Vec<u8>,
        value: Vec<u8>,
        precondition: Precondition,
    },
    Delete {
        transaction_id: Uuid,
        key: Vec<u8>,
        precondition: Precondition,
    },
    Commit {
        transaction_id: Uuid,
        hold_pre_native: bool,
    },
    Resolve {
        transaction_id: Uuid,
    },
    Release {
        transaction_id: Uuid,
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
        transaction_id: Uuid,
        description: String,
    },
    Get {
        transaction_id: Uuid,
        key: String,
    },
    Range {
        transaction_id: Uuid,
        start: String,
        end: String,
        direction: Direction,
        page_size: usize,
    },
    Put {
        transaction_id: Uuid,
        key: String,
        value: String,
        precondition: RawPrecondition,
    },
    Delete {
        transaction_id: Uuid,
        key: String,
        precondition: RawPrecondition,
    },
    Commit {
        transaction_id: Uuid,
        #[serde(default)]
        hold_pre_native: bool,
    },
    Resolve {
        transaction_id: Uuid,
    },
    Release {
        transaction_id: Uuid,
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
            transaction_id,
            description,
        } => Ok(Command::Begin {
            transaction_id,
            description,
        }),
        RawCommand::Get {
            transaction_id,
            key,
        } => Ok(Command::Get {
            transaction_id,
            key: decode_hex("key", &key)?,
        }),
        RawCommand::Range {
            transaction_id,
            start,
            end,
            direction,
            page_size,
        } => Ok(Command::Range {
            transaction_id,
            start: decode_hex("range start", &start)?,
            end: decode_hex("range end", &end)?,
            direction,
            page_size,
        }),
        RawCommand::Put {
            transaction_id,
            key,
            value,
            precondition,
        } => Ok(Command::Put {
            transaction_id,
            key: decode_hex("key", &key)?,
            value: decode_hex("value", &value)?,
            precondition: decode_precondition(precondition)?,
        }),
        RawCommand::Delete {
            transaction_id,
            key,
            precondition,
        } => Ok(Command::Delete {
            transaction_id,
            key: decode_hex("key", &key)?,
            precondition: decode_precondition(precondition)?,
        }),
        RawCommand::Commit {
            transaction_id,
            hold_pre_native,
        } => Ok(Command::Commit {
            transaction_id,
            hold_pre_native,
        }),
        RawCommand::Resolve { transaction_id } => Ok(Command::Resolve { transaction_id }),
        RawCommand::Release { transaction_id } => Ok(Command::Release { transaction_id }),
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
    transactions: HashMap<Uuid, Box<dyn WriteTransaction>>,
    pending: HashMap<Uuid, PendingCommit>,
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
                transaction_id,
                description,
            } => self.begin(transaction_id, description).await,
            Command::Get {
                transaction_id,
                key,
            } => self.get(transaction_id, key).await,
            Command::Range {
                transaction_id,
                start,
                end,
                direction,
                page_size,
            } => {
                self.range(transaction_id, start, end, direction, page_size)
                    .await
            }
            Command::Put {
                transaction_id,
                key,
                value,
                precondition,
            } => self.put(transaction_id, key, value, precondition).await,
            Command::Delete {
                transaction_id,
                key,
                precondition,
            } => self.delete(transaction_id, key, precondition).await,
            Command::Commit {
                transaction_id,
                hold_pre_native,
            } => self.commit(transaction_id, hold_pre_native).await,
            Command::Resolve { transaction_id } => self.resolve(transaction_id).await,
            Command::Release { transaction_id } => self.release(transaction_id).await,
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

    async fn begin(
        &mut self,
        transaction_id: Uuid,
        description: String,
    ) -> Result<Response, String> {
        if self.transactions.contains_key(&transaction_id)
            || self.pending.contains_key(&transaction_id)
        {
            return Err(format!("transaction {transaction_id} is already active"));
        }
        let transaction = self
            .store()?
            .begin_write(transaction_id.into(), &description)
            .await
            .map_err(display_error)?;
        self.transactions.insert(transaction_id, transaction);
        Ok(Response::success("Begun"))
    }

    async fn get(&mut self, transaction_id: Uuid, raw_key: Vec<u8>) -> Result<Response, String> {
        let key = store_key(raw_key)?;
        let record = self
            .transaction_mut(transaction_id)?
            .get(&key)
            .await
            .map_err(display_error)?;
        let mut response = Response::success("Get");
        response.record = record.map(record_response);
        Ok(response)
    }

    async fn range(
        &mut self,
        transaction_id: Uuid,
        raw_start: Vec<u8>,
        raw_end: Vec<u8>,
        direction: Direction,
        page_size: usize,
    ) -> Result<Response, String> {
        let request = RangeRequest {
            range: KeyRange::new(store_key(raw_start)?, store_key(raw_end)?)
                .map_err(display_error)?,
            direction: match direction {
                Direction::Forward => novarocks_spi::state_store::Direction::Forward,
                Direction::Reverse => novarocks_spi::state_store::Direction::Reverse,
            },
            page_size,
            continuation: None,
        };
        let page = self
            .transaction_mut(transaction_id)?
            .range(&request)
            .await
            .map_err(display_error)?;
        let mut response = Response::success("Range");
        response.records = page.records.into_iter().map(record_response).collect();
        Ok(response)
    }

    async fn put(
        &mut self,
        transaction_id: Uuid,
        raw_key: Vec<u8>,
        raw_value: Vec<u8>,
        precondition: Precondition,
    ) -> Result<Response, String> {
        let precondition = store_precondition(precondition)?;
        self.transaction_mut(transaction_id)?
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
        transaction_id: Uuid,
        raw_key: Vec<u8>,
        precondition: Precondition,
    ) -> Result<Response, String> {
        let key = store_key(raw_key)?;
        let precondition = store_precondition(precondition)?;
        self.transaction_mut(transaction_id)?
            .delete(key, precondition)
            .await
            .map_err(display_error)?;
        Ok(Response::success("Staged"))
    }

    async fn commit(
        &mut self,
        transaction_id: Uuid,
        hold_pre_native: bool,
    ) -> Result<Response, String> {
        let transaction = self
            .transactions
            .remove(&transaction_id)
            .ok_or_else(|| format!("transaction {transaction_id} is not active"))?;
        if !hold_pre_native {
            return Ok(commit_response(transaction.commit().await));
        }
        let control = arm_next_foundationdb_commit(true, false, false).map_err(display_error)?;
        let owner = tokio::spawn(async move { transaction.commit().await });
        control.wait_pre_native().await;
        self.pending
            .insert(transaction_id, PendingCommit { control, owner });
        Ok(Response::success("CommitHeld"))
    }

    async fn resolve(&self, transaction_id: Uuid) -> Result<Response, String> {
        let resolution = self
            .store()?
            .resolve_commit(&TransactionId::from(transaction_id))
            .await
            .map_err(display_error)?;
        Ok(resolution_response(resolution))
    }

    async fn release(&mut self, transaction_id: Uuid) -> Result<Response, String> {
        let pending = self
            .pending
            .remove(&transaction_id)
            .ok_or_else(|| format!("transaction {transaction_id} is not held"))?;
        pending.control.release_pre_native();
        let outcome = pending
            .owner
            .await
            .map_err(|error| format!("commit owner failed: {error}"))?;
        Ok(commit_response(outcome))
    }

    async fn shutdown(&mut self) -> Result<Response, String> {
        self.transactions.clear();
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

    fn transaction_mut(
        &mut self,
        transaction_id: Uuid,
    ) -> Result<&mut Box<dyn WriteTransaction>, String> {
        self.transactions
            .get_mut(&transaction_id)
            .ok_or_else(|| format!("transaction {transaction_id} is not active"))
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

fn resolution_response(resolution: CommitResolution) -> Response {
    let mut response = Response::success("Resolve");
    match resolution {
        CommitResolution::Committed(receipt) => {
            response.resolution = Some("Committed".to_owned());
            response.revision = Some(hex::encode(receipt.revision.as_bytes()));
        }
        CommitResolution::NotCommitted => {
            response.resolution = Some("NotCommitted".to_owned());
        }
        CommitResolution::Unresolved => {
            response.resolution = Some("Pending".to_owned());
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
