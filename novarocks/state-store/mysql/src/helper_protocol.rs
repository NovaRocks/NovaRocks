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
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use uuid::Uuid;

use super::test_support::{MysqlCommitTestApi, MysqlProviderTestHarness};
use novarocks_state_store_api::MAX_VALUE_BYTES;

use crate::{
    MYSQL_MAX_KEY_BYTES, MySqlClientConfig, MySqlTlsMode, MysqlTestLimitOverrides,
    MysqlTestProviderConfig, MysqlTestStoreConfig,
};
use novarocks_secret::SecretValue;
use novarocks_state_store_api::{
    AttemptOutcome, CommitObservation, CommitOutcome, ContinuationToken,
    Direction as StoreDirection, Key, KeyRange, Precondition as StorePrecondition, RangeRequest,
    StateRecord, StateStore, StateStoreError, Value, VersionToken, WriteTransaction,
};

const MAX_LINE_BYTES: usize = 160 * 1024;
const MAX_KEY_HEX_BYTES: usize = MYSQL_MAX_KEY_BYTES * 2;
const MAX_VALUE_HEX_BYTES: usize = MAX_VALUE_BYTES * 2;
const MAX_TOKEN_HEX_BYTES: usize = 16 * 1024;
const COMMIT_HOOK_DEADLINE: Duration = Duration::from_secs(5);
const COMMIT_OWNER_DEADLINE: Duration = Duration::from_secs(6);
const COMMAND_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum Direction {
    Forward,
    Reverse,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawPrecondition {
    Name(PreconditionName),
    Version(RawVersionPrecondition),
}

#[derive(Debug, Deserialize)]
enum PreconditionName {
    Any,
    Absent,
    Present,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawVersionPrecondition {
    version: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "command", deny_unknown_fields)]
enum Request {
    Open {
        id: u64,
        cluster_id: String,
    },
    Begin {
        id: u64,
        handle: Uuid,
        description: String,
    },
    Get {
        id: u64,
        handle: Uuid,
        key: String,
    },
    Range {
        id: u64,
        handle: Uuid,
        start: String,
        end: String,
        direction: Direction,
        page_size: usize,
        #[serde(default)]
        continuation: Option<String>,
    },
    Put {
        id: u64,
        handle: Uuid,
        key: String,
        value: String,
        precondition: RawPrecondition,
    },
    Delete {
        id: u64,
        handle: Uuid,
        key: String,
        precondition: RawPrecondition,
    },
    Commit {
        id: u64,
        handle: Uuid,
        #[serde(default)]
        lose_response: bool,
    },
    Resolve {
        id: u64,
        handle: Uuid,
    },
    Shutdown {
        id: u64,
    },
}

impl Request {
    const fn id(&self) -> u64 {
        match self {
            Self::Open { id, .. }
            | Self::Begin { id, .. }
            | Self::Get { id, .. }
            | Self::Range { id, .. }
            | Self::Put { id, .. }
            | Self::Delete { id, .. }
            | Self::Commit { id, .. }
            | Self::Resolve { id, .. }
            | Self::Shutdown { id } => *id,
        }
    }
}

#[derive(Debug)]
struct ProtocolError {
    id: u64,
    code: &'static str,
    message: String,
    error_kind: Option<String>,
}

impl ProtocolError {
    fn new(id: u64, code: &'static str, message: &'static str) -> Self {
        Self {
            id,
            code,
            message: message.to_owned(),
            error_kind: None,
        }
    }

    fn state_store(id: u64, code: &'static str, error: &StateStoreError) -> Self {
        Self {
            id,
            code,
            message: error.to_string(),
            error_kind: Some(format!("{:?}", error.kind())),
        }
    }
}

#[derive(Debug, Serialize)]
struct Response {
    id: u64,
    ok: bool,
    pid: u32,
    event: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    outcome: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolution: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    record: Option<RecordResponse>,
    #[serde(default)]
    records: Vec<RecordResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    continuation: Option<String>,
}

#[derive(Debug, Serialize)]
struct RecordResponse {
    key: String,
    value: String,
    version: String,
}

impl Response {
    fn success(id: u64, event: &'static str) -> Self {
        Self {
            id,
            ok: true,
            pid: std::process::id(),
            event,
            code: None,
            error: None,
            error_kind: None,
            outcome: None,
            resolution: None,
            revision: None,
            record: None,
            records: Vec::new(),
            continuation: None,
        }
    }

    fn error(error: ProtocolError) -> Self {
        Self {
            id: error.id,
            ok: false,
            pid: std::process::id(),
            event: "Error",
            code: Some(error.code),
            error: Some(error.message),
            error_kind: error.error_kind,
            outcome: None,
            resolution: None,
            revision: None,
            record: None,
            records: Vec::new(),
            continuation: None,
        }
    }
}

#[derive(Default)]
struct HelperState {
    runtime: Option<MysqlProviderTestHarness>,
    store: Option<Arc<dyn StateStore>>,
    /// Live write transactions, addressed by a name the driver picked.
    ///
    /// The name is a protocol handle and nothing more. It used to be the
    /// store's transaction identity, which is what let a driver ask any
    /// process about any identifier it liked; the store now issues its own
    /// attempt identity and the helper never sees a caller-supplied one.
    transactions: HashMap<Uuid, Box<dyn WriteTransaction>>,
    /// Kept past commit so a lost response can still be resolved.
    ///
    /// This is the only thing that can resolve an attempt: the identity is
    /// scoped to this process's open instance, so a second helper has nothing
    /// to look up and is not asked to.
    observations: HashMap<Uuid, CommitObservation>,
    next_id: u64,
}

impl HelperState {
    fn validate_id(&mut self, id: u64) -> Result<(), ProtocolError> {
        let expected = self.next_id.checked_add(1).ok_or_else(|| {
            ProtocolError::new(id, "OutOfOrderId", "request identifier is out of order")
        })?;
        if id < expected {
            return Err(ProtocolError::new(
                id,
                "DuplicateId",
                "request identifier was already used",
            ));
        }
        if id != expected {
            return Err(ProtocolError::new(
                id,
                "OutOfOrderId",
                "request identifier is out of order",
            ));
        }
        self.next_id = id;
        Ok(())
    }

    async fn execute(&mut self, request: Request) -> Result<Response, ProtocolError> {
        let id = request.id();
        self.validate_id(id)?;
        match request {
            Request::Open { cluster_id, .. } => self.open(id, cluster_id).await,
            Request::Begin {
                handle,
                description,
                ..
            } => self.begin(id, handle, description).await,
            Request::Get { handle, key, .. } => self.get(id, handle, key).await,
            Request::Range {
                handle,
                start,
                end,
                direction,
                page_size,
                continuation,
                ..
            } => {
                self.range(id, handle, start, end, direction, page_size, continuation)
                    .await
            }
            Request::Put {
                handle,
                key,
                value,
                precondition,
                ..
            } => self.put(id, handle, key, value, precondition).await,
            Request::Delete {
                handle,
                key,
                precondition,
                ..
            } => self.delete(id, handle, key, precondition).await,
            Request::Commit {
                handle,
                lose_response,
                ..
            } => self.commit(id, handle, lose_response).await,
            Request::Resolve { handle, .. } => self.resolve(id, handle).await,
            Request::Shutdown { .. } => self.shutdown(id).await,
        }
    }

    async fn open(&mut self, id: u64, cluster_id: String) -> Result<Response, ProtocolError> {
        if self.runtime.is_some() || self.store.is_some() {
            return Err(invalid_order(id));
        }
        let database = required_env(id, "NOVAROCKS_MYSQL_DATABASE")?;
        let config = client_config(id)?;
        let mut runtime = MysqlProviderTestHarness::boot(config)
            .map_err(|error| ProtocolError::state_store(id, "OpenFailed", &error))?;
        let store = match runtime
            .open_store(
                MysqlTestStoreConfig {
                    cluster_id,
                    limits: MysqlTestLimitOverrides::default(),
                    provider: MysqlTestProviderConfig::Mysql { database },
                    outstanding_attempts: None,
                },
                Instant::now() + COMMAND_DEADLINE,
            )
            .await
        {
            Ok(store) => store,
            Err(error) => {
                let mut primary = ProtocolError::state_store(id, "OpenFailed", &error);
                if let Err(shutdown_error) =
                    runtime.shutdown(Instant::now() + COMMAND_DEADLINE).await
                {
                    primary.error_kind = Some(format!(
                        "{:?}+Shutdown{:?}",
                        error.kind(),
                        shutdown_error.kind()
                    ));
                }
                return Err(primary);
            }
        };
        self.runtime = Some(runtime);
        self.store = Some(store);
        Ok(Response::success(id, "Opened"))
    }

    async fn begin(
        &mut self,
        id: u64,
        handle: Uuid,
        description: String,
    ) -> Result<Response, ProtocolError> {
        if self.transactions.contains_key(&handle) {
            return Err(ProtocolError::new(
                id,
                "DuplicateTransaction",
                "transaction handle is already active",
            ));
        }
        let store = self.store(id)?;
        let (attempt, observation) = store
            .attempts()
            .reserve()
            .map_err(|error| ProtocolError::state_store(id, "BeginFailed", &error))?;
        let transaction = store
            .begin_write(attempt, &description)
            .await
            .map_err(|error| ProtocolError::state_store(id, "BeginFailed", &error))?;
        self.transactions.insert(handle, transaction);
        self.observations.insert(handle, observation);
        Ok(Response::success(id, "Begun"))
    }

    async fn get(
        &mut self,
        id: u64,
        handle: Uuid,
        raw_key: String,
    ) -> Result<Response, ProtocolError> {
        let key = store_key(id, &raw_key)?;
        let record = self
            .transaction_mut(id, handle)?
            .get(&key)
            .await
            .map_err(|error| ProtocolError::state_store(id, "GetFailed", &error))?;
        let mut response = Response::success(id, "Get");
        response.record = record.map(record_response);
        Ok(response)
    }

    #[allow(clippy::too_many_arguments)]
    async fn range(
        &mut self,
        id: u64,
        handle: Uuid,
        raw_start: String,
        raw_end: String,
        direction: Direction,
        page_size: usize,
        raw_continuation: Option<String>,
    ) -> Result<Response, ProtocolError> {
        let continuation = raw_continuation
            .map(|encoded| {
                decode_hex(id, "continuation", &encoded, MAX_TOKEN_HEX_BYTES).and_then(|raw| {
                    ContinuationToken::try_from(Bytes::from(raw))
                        .map_err(|error| ProtocolError::state_store(id, "InvalidPayload", &error))
                })
            })
            .transpose()?;
        let request = RangeRequest {
            range: KeyRange::new(store_key(id, &raw_start)?, store_key(id, &raw_end)?)
                .map_err(|error| ProtocolError::state_store(id, "InvalidPayload", &error))?,
            direction: match direction {
                Direction::Forward => StoreDirection::Forward,
                Direction::Reverse => StoreDirection::Reverse,
            },
            page_size,
            continuation,
        };
        let page = self
            .transaction_mut(id, handle)?
            .range(&request)
            .await
            .map_err(|error| ProtocolError::state_store(id, "RangeFailed", &error))?;
        let mut response = Response::success(id, "Range");
        response.records = page.records.into_iter().map(record_response).collect();
        response.continuation = page
            .continuation
            .map(|continuation| hex::encode(continuation.as_bytes()));
        Ok(response)
    }

    async fn put(
        &mut self,
        id: u64,
        handle: Uuid,
        raw_key: String,
        raw_value: String,
        raw_precondition: RawPrecondition,
    ) -> Result<Response, ProtocolError> {
        let key = store_key(id, &raw_key)?;
        let value = Value::try_from(Bytes::from(decode_hex(
            id,
            "value",
            &raw_value,
            MAX_VALUE_HEX_BYTES,
        )?))
        .map_err(|error| ProtocolError::state_store(id, "InvalidPayload", &error))?;
        let precondition = precondition(id, raw_precondition)?;
        self.transaction_mut(id, handle)?
            .put(key, value, precondition)
            .await
            .map_err(|error| ProtocolError::state_store(id, "PutFailed", &error))?;
        Ok(Response::success(id, "Staged"))
    }

    async fn delete(
        &mut self,
        id: u64,
        handle: Uuid,
        raw_key: String,
        raw_precondition: RawPrecondition,
    ) -> Result<Response, ProtocolError> {
        let key = store_key(id, &raw_key)?;
        let precondition = precondition(id, raw_precondition)?;
        self.transaction_mut(id, handle)?
            .delete(key, precondition)
            .await
            .map_err(|error| ProtocolError::state_store(id, "DeleteFailed", &error))?;
        Ok(Response::success(id, "Staged"))
    }

    async fn commit(
        &mut self,
        id: u64,
        handle: Uuid,
        lose_response: bool,
    ) -> Result<Response, ProtocolError> {
        let transaction = self
            .transactions
            .remove(&handle)
            .ok_or_else(|| invalid_order(id))?;
        let outcome = if lose_response {
            let control = MysqlCommitTestApi::arm_shared_post_dispatch(true);
            let mut owner = tokio::spawn(async move { transaction.commit().await });
            tokio::select! {
                result = &mut owner => supervised_commit_result(id, result)?,
                () = control.wait_dispatched() => {
                    control.allow_provider_progress();
                    match tokio::time::timeout(COMMIT_OWNER_DEADLINE, &mut owner).await {
                        Ok(result) => supervised_commit_result(id, result)?,
                        Err(_) => {
                            owner.abort();
                            let _ = owner.await;
                            return Err(ProtocolError::new(
                                id,
                                "CommitFailed",
                                "commit supervisor deadline exceeded",
                            ));
                        }
                    }
                }
                () = tokio::time::sleep(COMMIT_HOOK_DEADLINE) => {
                    owner.abort();
                    let _ = owner.await;
                    return Err(ProtocolError::new(
                        id,
                        "CommitFailed",
                        "commit hook deadline exceeded",
                    ));
                }
            }
        } else {
            transaction.commit().await
        };
        Ok(commit_response(id, outcome))
    }

    /// Answers for an attempt *this* helper issued.
    ///
    /// An unknown handle is a protocol error rather than a verdict. There is
    /// no longer any way to ask a store about an identity it did not issue, so
    /// the honest reply to "resolve something I never began" is that the
    /// question is malformed -- not "not committed".
    async fn resolve(&self, id: u64, handle: Uuid) -> Result<Response, ProtocolError> {
        let observation = self.observations.get(&handle).ok_or_else(|| {
            ProtocolError::new(
                id,
                "UnknownAttempt",
                "this helper issued no write attempt under that handle",
            )
        })?;
        let outcome = observation
            .outcome()
            .await
            .map_err(|error| ProtocolError::state_store(id, "ResolveFailed", &error))?;
        Ok(resolution_response(id, outcome))
    }

    async fn shutdown(&mut self, id: u64) -> Result<Response, ProtocolError> {
        self.observations.clear();
        let transactions = std::mem::take(&mut self.transactions);
        for (_, transaction) in transactions {
            transaction
                .abort()
                .await
                .map_err(|error| ProtocolError::state_store(id, "ShutdownFailed", &error))?;
        }
        self.store.take();
        if let Some(mut runtime) = self.runtime.take() {
            runtime
                .shutdown(Instant::now() + COMMAND_DEADLINE)
                .await
                .map_err(|error| ProtocolError::state_store(id, "ShutdownFailed", &error))?;
        }
        Ok(Response::success(id, "Shutdown"))
    }

    fn store(&self, id: u64) -> Result<&Arc<dyn StateStore>, ProtocolError> {
        self.store.as_ref().ok_or_else(|| invalid_order(id))
    }

    fn transaction_mut(
        &mut self,
        id: u64,
        handle: Uuid,
    ) -> Result<&mut Box<dyn WriteTransaction>, ProtocolError> {
        self.transactions
            .get_mut(&handle)
            .ok_or_else(|| invalid_order(id))
    }
}

fn supervised_commit_result(
    id: u64,
    result: Result<CommitOutcome, tokio::task::JoinError>,
) -> Result<CommitOutcome, ProtocolError> {
    result.map_err(|_| {
        ProtocolError::new(id, "CommitFailed", "commit supervisor stopped unexpectedly")
    })
}

fn client_config(id: u64) -> Result<MySqlClientConfig, ProtocolError> {
    let port = required_env(id, "NOVAROCKS_MYSQL_PORT")?
        .parse()
        .map_err(|_| configuration_error(id))?;
    Ok(MySqlClientConfig {
        host: required_env(id, "NOVAROCKS_MYSQL_HOST")?,
        port,
        username: required_env(id, "NOVAROCKS_MYSQL_USERNAME")?,
        password: SecretValue::new(required_fixture_password(id)?),
        tls_mode: MySqlTlsMode::Disabled,
        tls_ca_path: None,
        tls_cert_path: None,
        tls_key_path: None,
        connect_timeout_ms: 1_000,
        pool_min: 1,
        pool_max: 4,
        inactive_connection_ttl_ms: 1_000,
    })
}

fn required_fixture_password(id: u64) -> Result<String, ProtocolError> {
    let password_env = required_env(id, "NOVAROCKS_MYSQL_PASSWORD_ENV")?;
    required_env(id, &password_env)
}

fn required_env(id: u64, name: &str) -> Result<String, ProtocolError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| configuration_error(id))
}

fn configuration_error(id: u64) -> ProtocolError {
    ProtocolError::new(
        id,
        "Configuration",
        "MySQL helper configuration is incomplete",
    )
}

fn store_key(id: u64, encoded: &str) -> Result<Key, ProtocolError> {
    let raw = decode_hex(id, "key", encoded, MAX_KEY_HEX_BYTES)?;
    Key::try_from(Bytes::from(raw))
        .map_err(|error| ProtocolError::state_store(id, "InvalidPayload", &error))
}

fn precondition(id: u64, raw: RawPrecondition) -> Result<StorePrecondition, ProtocolError> {
    match raw {
        RawPrecondition::Name(PreconditionName::Any) => Ok(StorePrecondition::Any),
        RawPrecondition::Name(PreconditionName::Absent) => Ok(StorePrecondition::Absent),
        RawPrecondition::Name(PreconditionName::Present) => Ok(StorePrecondition::Present),
        RawPrecondition::Version(RawVersionPrecondition { version }) => {
            let raw = decode_hex(id, "version", &version, MAX_TOKEN_HEX_BYTES)?;
            Ok(StorePrecondition::Version(
                VersionToken::try_from(Bytes::from(raw))
                    .map_err(|error| ProtocolError::state_store(id, "InvalidPayload", &error))?,
            ))
        }
    }
}

fn decode_hex(
    id: u64,
    _field: &'static str,
    encoded: &str,
    maximum: usize,
) -> Result<Vec<u8>, ProtocolError> {
    if encoded.len() > maximum {
        return Err(ProtocolError::new(
            id,
            "HexTooLong",
            "hexadecimal payload exceeds the protocol limit",
        ));
    }
    hex::decode(encoded)
        .map_err(|_| ProtocolError::new(id, "InvalidHex", "payload must be canonical hexadecimal"))
}

fn invalid_order(id: u64) -> ProtocolError {
    ProtocolError::new(
        id,
        "InvalidOrder",
        "command is not valid in the current helper state",
    )
}

fn record_response(record: StateRecord) -> RecordResponse {
    RecordResponse {
        key: hex::encode(record.key.as_bytes()),
        value: hex::encode(record.value.as_bytes()),
        version: hex::encode(record.version.as_bytes()),
    }
}

fn commit_response(id: u64, outcome: CommitOutcome) -> Response {
    let mut response = Response::success(id, "Commit");
    match outcome {
        CommitOutcome::Committed(receipt) => {
            response.outcome = Some("Committed");
            response.revision = Some(hex::encode(receipt.revision.as_bytes()));
        }
        CommitOutcome::Conflict(_) => response.outcome = Some("Conflict"),
        CommitOutcome::TransientBeforeCommit(_) => {
            response.outcome = Some("TransientBeforeCommit");
        }
        CommitOutcome::DefiniteFailure(_) => response.outcome = Some("DefiniteFailure"),
        CommitOutcome::CommitUnknown(_) => response.outcome = Some("CommitUnknown"),
    }
    response
}

fn resolution_response(id: u64, outcome: AttemptOutcome) -> Response {
    let mut response = Response::success(id, "Resolve");
    match outcome {
        AttemptOutcome::Committed(receipt) => {
            response.resolution = Some("Committed");
            response.revision = Some(hex::encode(receipt.revision.as_bytes()));
        }
        AttemptOutcome::NotCommitted => response.resolution = Some("NotCommitted"),
        AttemptOutcome::Unresolved => response.resolution = Some("Unresolved"),
    }
    response
}

fn parse_request(line: &[u8]) -> Result<Request, ProtocolError> {
    let id = extract_id(line);
    if line.len() > MAX_LINE_BYTES {
        return Err(ProtocolError::new(
            id,
            "LineTooLong",
            "JSONL command exceeds the protocol limit",
        ));
    }
    serde_json::from_slice(line)
        .map_err(|_| ProtocolError::new(id, "InvalidJson", "command is not valid JSONL"))
}

fn extract_id(line: &[u8]) -> u64 {
    let mut deserializer = serde_json::Deserializer::from_slice(line);
    let id =
        serde::de::Deserializer::deserialize_map(&mut deserializer, TopLevelIdVisitor).unwrap_or(0);
    if deserializer.end().is_ok() { id } else { 0 }
}

struct TopLevelIdVisitor;

impl<'de> Visitor<'de> for TopLevelIdVisitor {
    type Value = u64;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object with at most one unsigned integer id")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut id = None;
        let mut invalid = false;
        while let Some(key) = map.next_key::<String>()? {
            if key == "id" {
                let value = map.next_value::<serde_json::Value>()?;
                if id.is_some() {
                    invalid = true;
                } else if let Some(value) = value.as_u64() {
                    id = Some(value);
                } else {
                    invalid = true;
                }
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(if invalid { 0 } else { id.unwrap_or(0) })
    }
}

async fn write_response(response: &Response) -> Result<(), ()> {
    let encoded = serde_json::to_vec(response).map_err(|_| ())?;
    let mut stdout = tokio::io::stdout();
    stdout.write_all(&encoded).await.map_err(|_| ())?;
    stdout.write_all(b"\n").await.map_err(|_| ())?;
    stdout.flush().await.map_err(|_| ())
}

async fn read_bounded_line<R>(reader: &mut R, line: &mut Vec<u8>) -> Result<usize, ()>
where
    R: AsyncBufRead + Unpin,
{
    let mut total = 0_usize;
    loop {
        let available = reader.fill_buf().await.map_err(|_| ())?;
        if available.is_empty() {
            return Ok(total);
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        let retained = consumed.min(MAX_LINE_BYTES.saturating_add(1).saturating_sub(line.len()));
        line.extend_from_slice(&available[..retained]);
        reader.consume(consumed);
        total = total.saturating_add(consumed);
        if newline.is_some() || line.len() > MAX_LINE_BYTES {
            return Ok(total);
        }
    }
}

async fn run() -> i32 {
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut state = HelperState::default();
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = match read_bounded_line(&mut reader, &mut line).await {
            Ok(read) => read,
            Err(_) => {
                eprintln!("state-store-mysql-helper: command input failed");
                let _ = state.shutdown(state.next_id.saturating_add(1)).await;
                return 1;
            }
        };
        if read == 0 {
            if state.runtime.is_some() || state.store.is_some() {
                let _ = state.shutdown(state.next_id.saturating_add(1)).await;
                eprintln!("state-store-mysql-helper: input ended before Shutdown");
                return 1;
            }
            return 0;
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        let request = parse_request(&line);
        let is_shutdown = matches!(request, Ok(Request::Shutdown { .. }));
        let response = match request {
            Ok(request) => match state.execute(request).await {
                Ok(response) => response,
                Err(error) => {
                    let id = error.id;
                    let response = Response::error(error);
                    if write_response(&response).await.is_err() {
                        eprintln!("state-store-mysql-helper: response output failed");
                    }
                    let _ = state.shutdown(id.saturating_add(1)).await;
                    return 1;
                }
            },
            Err(error) => {
                let id = error.id;
                let response = Response::error(error);
                if write_response(&response).await.is_err() {
                    eprintln!("state-store-mysql-helper: response output failed");
                }
                let _ = state.shutdown(id.saturating_add(1)).await;
                return 1;
            }
        };
        if write_response(&response).await.is_err() {
            eprintln!("state-store-mysql-helper: response output failed");
            let _ = state.shutdown(response.id.saturating_add(1)).await;
            return 1;
        }
        if is_shutdown {
            return 0;
        }
    }
}

pub fn run_stdio() -> i32 {
    match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime.block_on(run()),
        Err(_) => {
            eprintln!("state-store-mysql-helper: runtime startup failed");
            1
        }
    }
}
