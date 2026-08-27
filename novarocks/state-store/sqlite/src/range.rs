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

use std::collections::VecDeque;
use std::ops::Bound::{Excluded, Included};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};

use novarocks_spi::state_store::{
    ChangeCursor, ChangeHint, ChangePage, ChangePollRequest, Direction, Key, RangePage,
    RangeRequest, StateRecord, StateStoreError, StateStoreErrorKind, StoreIdentity, StoreRevision,
};

use novarocks_spi::state_store::StateStoreMetrics;

use super::open_connection;
use super::schema::load_change_retention_floor;
use super::txn::{
    Mutation, SqliteTxnState, load_current_revision, operation_error, persisted_key,
    persisted_row_error, persisted_value, range_refill_completed, range_refill_started,
    revision_token, revision_version,
};

struct BaseWindow {
    records: VecDeque<StateRecord>,
    resume_after: Option<Key>,
    exhausted: bool,
}

impl BaseWindow {
    fn new(resume_after: Option<Key>) -> Self {
        Self {
            records: VecDeque::new(),
            resume_after,
            exhausted: false,
        }
    }
}

pub(super) fn range_page(
    state: &mut SqliteTxnState,
    request: &RangeRequest,
) -> Result<RangePage, StateStoreError> {
    let resume_after = request
        .continuation
        .as_ref()
        .map(|token| token.resume_after(request))
        .transpose()?;
    let mut base = BaseWindow::new(resume_after.clone());
    let mut logical_cursor = resume_after;
    let wanted = request
        .page_size
        .checked_add(1)
        .ok_or_else(invalid_range_request)?;
    let mut visible = Vec::with_capacity(wanted);

    while visible.len() < wanted {
        ensure_range_active(state)?;
        if base.records.is_empty() && !base.exhausted {
            ensure_range_active(state)?;
            range_refill_started(state);
            refill_base_window(&state.connection, request, &mut base)?;
            state.snapshot_established = true;
            range_refill_completed(state);
            ensure_range_active(state)?;
        }

        let base_record = base.records.front().cloned();
        let overlay = next_overlay(state, request, logical_cursor.as_ref());
        let next_key = match (&base_record, &overlay) {
            (Some(base), Some((overlay_key, _))) => match request.direction {
                Direction::Forward if base.key <= *overlay_key => base.key.clone(),
                Direction::Forward => overlay_key.clone(),
                Direction::Reverse if base.key >= *overlay_key => base.key.clone(),
                Direction::Reverse => overlay_key.clone(),
            },
            (Some(base), None) => base.key.clone(),
            (None, Some((overlay_key, _))) => overlay_key.clone(),
            (None, None) => break,
        };

        let matching_base = base_record.filter(|record| record.key == next_key);
        if matching_base.is_some() {
            base.records.pop_front();
        }
        let matching_overlay = overlay
            .filter(|(overlay_key, _)| overlay_key == &next_key)
            .map(|(_, mutation)| mutation);
        let record = match matching_overlay {
            Some(Mutation::Put {
                value,
                provisional_version,
                ..
            }) => Some(StateRecord {
                key: next_key.clone(),
                value,
                version: provisional_version,
            }),
            Some(Mutation::Delete { .. }) => None,
            None => matching_base,
        };
        logical_cursor = Some(next_key);
        if let Some(record) = record {
            visible.push(record);
        }
    }

    let continuation = if visible.len() > request.page_size {
        visible.truncate(request.page_size);
        let last_key = &visible.last().ok_or_else(invalid_range_request)?.key;
        Some(request.continuation_after(last_key)?)
    } else {
        None
    };
    Ok(RangePage {
        records: visible,
        continuation,
    })
}

fn refill_base_window(
    connection: &Connection,
    request: &RangeRequest,
    window: &mut BaseWindow,
) -> Result<(), StateStoreError> {
    let limit = i64::try_from(
        request
            .page_size
            .checked_add(1)
            .ok_or_else(invalid_range_request)?,
    )
    .map_err(|_| invalid_range_request())?;
    let mut records = match (request.direction, window.resume_after.as_ref()) {
        (Direction::Forward, Some(last)) => query_base(
            connection,
            "SELECT key, value, version FROM state_store_kv \
             WHERE key >= ?1 AND key < ?2 AND key > ?3 \
             ORDER BY key ASC LIMIT ?4",
            request,
            Some(last),
            limit,
        )?,
        (Direction::Forward, None) => query_base(
            connection,
            "SELECT key, value, version FROM state_store_kv \
             WHERE key >= ?1 AND key < ?2 \
             ORDER BY key ASC LIMIT ?3",
            request,
            None,
            limit,
        )?,
        (Direction::Reverse, Some(last)) => query_base(
            connection,
            "SELECT key, value, version FROM state_store_kv \
             WHERE key >= ?1 AND key < ?2 AND key < ?3 \
             ORDER BY key DESC LIMIT ?4",
            request,
            Some(last),
            limit,
        )?,
        (Direction::Reverse, None) => query_base(
            connection,
            "SELECT key, value, version FROM state_store_kv \
             WHERE key >= ?1 AND key < ?2 \
             ORDER BY key DESC LIMIT ?3",
            request,
            None,
            limit,
        )?,
    };
    window.exhausted = records.len() < request.page_size + 1;
    if let Some(last) = records.last() {
        window.resume_after = Some(last.key.clone());
    }
    window.records.extend(records.drain(..));
    Ok(())
}

fn query_base(
    connection: &Connection,
    sql: &str,
    request: &RangeRequest,
    last: Option<&Key>,
    limit: i64,
) -> Result<Vec<StateRecord>, StateStoreError> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| operation_error(&error, "failed to prepare bounded SQLite range query"))?;
    let map_row = |row: &rusqlite::Row<'_>| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, i64>(2)?,
        ))
    };
    let rows = match last {
        Some(last) => statement
            .query_map(
                params![
                    request.range.start.as_bytes(),
                    request.range.end.as_bytes(),
                    last.as_bytes(),
                    limit
                ],
                map_row,
            )
            .map_err(|error| operation_error(&error, "failed to execute bounded SQLite range"))?,
        None => statement
            .query_map(
                params![
                    request.range.start.as_bytes(),
                    request.range.end.as_bytes(),
                    limit
                ],
                map_row,
            )
            .map_err(|error| operation_error(&error, "failed to execute bounded SQLite range"))?,
    };
    rows.map(|row| {
        let (key, value, version) = row.map_err(|error| {
            persisted_row_error(&error, "failed to decode bounded SQLite range row")
        })?;
        let version = u64::try_from(version).map_err(|_| malformed_revision())?;
        Ok(StateRecord {
            key: persisted_key(key)?,
            value: persisted_value(value)?,
            version: revision_version(version),
        })
    })
    .collect()
}

fn next_overlay(
    state: &SqliteTxnState,
    request: &RangeRequest,
    cursor: Option<&Key>,
) -> Option<(Key, Mutation)> {
    match request.direction {
        Direction::Forward => {
            let lower = cursor.map_or(Included(&request.range.start), Excluded);
            state
                .overlay
                .range((lower, Excluded(&request.range.end)))
                .next()
                .map(|(key, mutation)| (key.clone(), mutation.clone()))
        }
        Direction::Reverse => {
            let upper = cursor.map_or(Excluded(&request.range.end), Excluded);
            state
                .overlay
                .range((Included(&request.range.start), upper))
                .next_back()
                .map(|(key, mutation)| (key.clone(), mutation.clone()))
        }
    }
}

pub(super) async fn poll_changes(
    path: PathBuf,
    identity: StoreIdentity,
    request: ChangePollRequest,
    metrics: Arc<StateStoreMetrics>,
) -> Result<ChangePage, StateStoreError> {
    let decoded_after = request
        .after
        .as_ref()
        .map(|cursor| {
            let (revision, sequence) = cursor.decode(identity.store_id)?;
            Ok((decode_revision(&revision)?, sequence))
        })
        .transpose()?;
    let worker_metrics = Arc::clone(&metrics);
    tokio::task::spawn_blocking(move || {
        poll_changes_blocking(&path, &identity, &request, decoded_after, &worker_metrics)
    })
    .await
    .map_err(|_| {
        metrics.record_blocking_failure();
        StateStoreError::new(
            StateStoreErrorKind::Internal,
            "SQLite change polling worker failed",
        )
    })?
}

fn poll_changes_blocking(
    path: &Path,
    identity: &StoreIdentity,
    request: &ChangePollRequest,
    decoded_after: Option<(u64, u32)>,
    metrics: &StateStoreMetrics,
) -> Result<ChangePage, StateStoreError> {
    let connection = open_connection(path)?;
    connection
        .execute_batch("BEGIN DEFERRED")
        .map_err(|error| {
            operation_error(&error, "failed to begin SQLite change polling snapshot")
        })?;
    let high_watermark = load_current_revision(&connection)?;
    let retention_floor = load_change_retention_floor(&connection, high_watermark)?;
    let start = decoded_after.unwrap_or((0, u32::MAX));

    if start.0 > high_watermark {
        connection.execute_batch("ROLLBACK").map_err(|error| {
            operation_error(&error, "failed to finish SQLite change polling snapshot")
        })?;
        return Err(invalid_change_request());
    }

    if start < retention_floor {
        connection.execute_batch("ROLLBACK").map_err(|error| {
            operation_error(&error, "failed to finish SQLite change polling snapshot")
        })?;
        return Ok(ChangePage {
            hints: Vec::new(),
            next_cursor: ChangeCursor::new(
                identity.store_id,
                revision_token(retention_floor.0),
                retention_floor.1,
            )?,
            high_watermark: revision_token(high_watermark),
            resync_required: true,
        });
    }

    let limit = i64::try_from(
        request
            .page_size
            .checked_add(1)
            .ok_or_else(invalid_change_request)?,
    )
    .map_err(|_| invalid_change_request())?;
    let start_revision = i64::try_from(start.0).map_err(|_| invalid_change_request())?;
    let start_sequence = i64::from(start.1);
    let high_watermark_i64 = i64::try_from(high_watermark).map_err(|_| malformed_revision())?;
    let mut statement = connection
        .prepare(
            "SELECT revision, sequence, key, committed_at_ms \
             FROM state_store_changes \
             WHERE revision <= ?1 \
               AND ((revision > ?2) OR \
                    (revision = ?2 AND sequence > ?3)) \
             ORDER BY revision ASC, sequence ASC LIMIT ?4",
        )
        .map_err(|error| {
            operation_error(&error, "failed to prepare bounded SQLite change query")
        })?;
    let rows = statement
        .query_map(
            params![high_watermark_i64, start_revision, start_sequence, limit],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .map_err(|error| {
            operation_error(&error, "failed to execute bounded SQLite change query")
        })?;
    let mut decoded = Vec::with_capacity(request.page_size + 1);
    for row in rows {
        let (revision, sequence, key, committed_at_ms) = row.map_err(|error| {
            persisted_row_error(&error, "failed to decode bounded SQLite change row")
        })?;
        let revision = u64::try_from(revision).map_err(|_| malformed_revision())?;
        let sequence = u32::try_from(sequence).map_err(|_| malformed_sequence())?;
        decoded.push((revision, sequence, key, committed_at_ms));
    }
    drop(statement);
    connection.execute_batch("ROLLBACK").map_err(|error| {
        operation_error(&error, "failed to finish SQLite change polling snapshot")
    })?;
    decoded.truncate(request.page_size);

    let mut hints = Vec::with_capacity(decoded.len());
    let mut last_position = None;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64;
    for (revision, sequence, key, committed_at_ms) in decoded {
        metrics.record_notification_lag(Duration::from_millis(
            now_ms.saturating_sub(committed_at_ms).max(0) as u64,
        ));
        hints.push(ChangeHint {
            revision: revision_token(revision),
            key: persisted_key(key)?,
        });
        last_position = Some((revision, sequence));
    }
    let next_cursor = match last_position {
        Some((revision, sequence)) => {
            ChangeCursor::new(identity.store_id, revision_token(revision), sequence)?
        }
        None => match &request.after {
            Some(cursor) => cursor.clone(),
            None => ChangeCursor::new(identity.store_id, revision_token(high_watermark), u32::MAX)?,
        },
    };
    Ok(ChangePage {
        hints,
        next_cursor,
        high_watermark: revision_token(high_watermark),
        resync_required: false,
    })
}

fn ensure_range_active(state: &SqliteTxnState) -> Result<(), StateStoreError> {
    if state.cancelled.load(Ordering::Acquire) || Instant::now() >= state.deadline {
        return Err(StateStoreError::new(
            StateStoreErrorKind::DeadlineExceeded,
            "SQLite transaction deadline exceeded",
        ));
    }
    Ok(())
}

fn decode_revision(revision: &StoreRevision) -> Result<u64, StateStoreError> {
    let bytes: [u8; 8] = revision
        .as_bytes()
        .try_into()
        .map_err(|_| invalid_change_request())?;
    let revision = u64::from_be_bytes(bytes);
    if i64::try_from(revision).is_err() {
        return Err(invalid_change_request());
    }
    Ok(revision)
}

const fn invalid_range_request() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "invalid SQLite range request",
    )
}

const fn invalid_change_request() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "invalid SQLite change request",
    )
}

const fn malformed_revision() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "SQLite state store revision is malformed",
    )
}

const fn malformed_sequence() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "SQLite state store change sequence is malformed",
    )
}
