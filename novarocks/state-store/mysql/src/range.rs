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

use std::future::Future;

use bytes::Bytes;
use mysql_async::Transaction;
use mysql_async::prelude::Queryable;
use tokio::time::{Instant, timeout_at};

use super::codec::MysqlCodec;
use super::error::{MysqlNativeError, MysqlReadStatementError};
use novarocks_state_store_api::{
    Direction, RangePage, RangeRequest, StateRecord, StateStoreError, StateStoreErrorKind, Value,
    VersionToken,
};

const FORWARD_FIRST_SQL: &str = "SELECT key_bytes, value_bytes, version_bytes
    FROM state_store_kv
    WHERE key_bytes >= ? AND key_bytes < ?
    ORDER BY key_bytes ASC
    LIMIT ?";
const FORWARD_RESUME_SQL: &str = "SELECT key_bytes, value_bytes, version_bytes
    FROM state_store_kv
    WHERE key_bytes >= ? AND key_bytes < ? AND key_bytes > ?
    ORDER BY key_bytes ASC
    LIMIT ?";
const REVERSE_FIRST_SQL: &str = "SELECT key_bytes, value_bytes, version_bytes
    FROM state_store_kv
    WHERE key_bytes >= ? AND key_bytes < ?
    ORDER BY key_bytes DESC
    LIMIT ?";
const REVERSE_RESUME_SQL: &str = "SELECT key_bytes, value_bytes, version_bytes
    FROM state_store_kv
    WHERE key_bytes >= ? AND key_bytes < ? AND key_bytes < ?
    ORDER BY key_bytes DESC
    LIMIT ?";

pub(super) async fn read_range_page(
    transaction: &mut Transaction<'_>,
    codec: &MysqlCodec,
    request: &RangeRequest,
    max_value_bytes: usize,
    deadline: Instant,
) -> Result<RangePage, MysqlReadStatementError> {
    let resume = request
        .continuation
        .as_ref()
        .map(|token| token.resume_after(request))
        .transpose()?;
    let limit = request
        .page_size
        .checked_add(1)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(invalid_range)?;
    let rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = match (request.direction, resume.as_ref()) {
        (Direction::Forward, None) => {
            let start = request.range.start.as_bytes().to_vec();
            let end = request.range.end.as_bytes().to_vec();
            execute(
                deadline,
                transaction.exec(FORWARD_FIRST_SQL, (start, end, limit)),
            )
            .await?
        }
        (Direction::Forward, Some(resume)) => {
            let start = request.range.start.as_bytes().to_vec();
            let end = request.range.end.as_bytes().to_vec();
            let resume = resume.as_bytes().to_vec();
            execute(
                deadline,
                transaction.exec(FORWARD_RESUME_SQL, (start, end, resume, limit)),
            )
            .await?
        }
        (Direction::Reverse, None) => {
            let start = request.range.start.as_bytes().to_vec();
            let end = request.range.end.as_bytes().to_vec();
            execute(
                deadline,
                transaction.exec(REVERSE_FIRST_SQL, (start, end, limit)),
            )
            .await?
        }
        (Direction::Reverse, Some(resume)) => {
            let start = request.range.start.as_bytes().to_vec();
            let end = request.range.end.as_bytes().to_vec();
            let resume = resume.as_bytes().to_vec();
            execute(
                deadline,
                transaction.exec(REVERSE_RESUME_SQL, (start, end, resume, limit)),
            )
            .await?
        }
    };

    let mut records = rows
        .into_iter()
        .map(|(key, value, version)| decode_record(codec, key, value, version, max_value_bytes))
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = records.len() > request.page_size;
    records.truncate(request.page_size);
    let continuation = if has_more {
        records
            .last()
            .map(|record| request.continuation_after(&record.key))
            .transpose()?
    } else {
        None
    };
    Ok(RangePage {
        records: std::mem::take(&mut records),
        continuation,
    })
}

async fn execute<T>(
    deadline: Instant,
    operation: impl Future<Output = Result<T, mysql_async::Error>>,
) -> Result<T, MysqlReadStatementError> {
    super::client::record_statement();
    match timeout_at(deadline, operation).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(MysqlReadStatementError::Native(MysqlNativeError::from(
            error,
        ))),
        Err(_) => Err(MysqlReadStatementError::Deadline(StateStoreError::new(
            StateStoreErrorKind::DeadlineExceeded,
            "MySQL state transaction deadline exceeded",
        ))),
    }
}

pub(super) fn decode_record(
    codec: &MysqlCodec,
    key: Vec<u8>,
    value: Vec<u8>,
    version: Vec<u8>,
    max_value_bytes: usize,
) -> Result<StateRecord, StateStoreError> {
    if value.len() > max_value_bytes {
        return Err(corruption());
    }
    codec.decode_version(&version)?;
    Ok(StateRecord {
        key: codec.decode_persisted_key(&key)?,
        value: Value::try_from(Bytes::from(value)).map_err(|_| corruption())?,
        version: VersionToken::try_from(Bytes::from(version)).map_err(|_| corruption())?,
    })
}

const fn invalid_range() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "MySQL range request is invalid",
    )
}

const fn corruption() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "MySQL state store persisted row is malformed",
    )
}
