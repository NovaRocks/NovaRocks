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

use std::sync::Arc;
#[cfg(feature = "state-store-test-hooks")]
use std::sync::{Mutex, OnceLock};

use bytes::Bytes;
use futures::future::BoxFuture;
use mysql_async::prelude::Queryable;
#[cfg(feature = "state-store-test-hooks")]
use tokio::sync::Notify;
use tokio::time::{Instant, timeout_at};

use super::client::{MysqlPoolConnection, PoolLifecycle, checkout_hygienic_connection};
use super::codec::MysqlCodec;
use super::identity::MysqlIdentitySnapshot;
use novarocks_state_store_api::{
    ChangeCursor, ChangeHint, ChangePage, ChangePollRequest, StateStoreError, StateStoreErrorKind,
    StateStoreLimits, StoreRevision,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ChangePosition {
    revision: u64,
    sequence: u32,
}

#[cfg(feature = "state-store-test-hooks")]
struct PollQueryHook {
    reached: Notify,
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) struct PollQueryHookControl {
    hook: Arc<PollQueryHook>,
}

#[cfg(feature = "state-store-test-hooks")]
static NEXT_POLL_QUERY_HOOK: OnceLock<Mutex<Option<Arc<PollQueryHook>>>> = OnceLock::new();
#[cfg(feature = "state-store-test-hooks")]
static DUPLICATE_NEXT_POLL_ROW: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(feature = "state-store-test-hooks")]
pub(super) fn arm_delayed_poll_query() -> PollQueryHookControl {
    let hook = Arc::new(PollQueryHook {
        reached: Notify::new(),
    });
    *NEXT_POLL_QUERY_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&hook));
    PollQueryHookControl { hook }
}

#[cfg(feature = "state-store-test-hooks")]
impl PollQueryHookControl {
    pub(super) async fn wait_reached(&self) {
        self.hook.reached.notified().await;
    }
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) fn duplicate_next_poll_row() {
    DUPLICATE_NEXT_POLL_ROW.store(true, std::sync::atomic::Ordering::Release);
}

#[cfg(feature = "state-store-test-hooks")]
fn take_poll_query_hook() -> Option<Arc<PollQueryHook>> {
    NEXT_POLL_QUERY_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
}

pub(super) async fn poll_changes(
    pool: Arc<dyn PoolLifecycle>,
    identity: &MysqlIdentitySnapshot,
    request: &ChangePollRequest,
    limits: &StateStoreLimits,
) -> Result<ChangePage, StateStoreError> {
    request.validate(limits)?;
    let deadline = Instant::now() + limits.transaction_deadline;
    let codec = MysqlCodec::new(limits.max_key_bytes)?;
    let mut connection = checkout_hygienic_connection(pool, deadline).await?;
    execute(&mut connection, deadline, |connection| {
        Box::pin(connection.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    })
    .await?;
    execute(&mut connection, deadline, |connection| {
        Box::pin(connection.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"))
    })
    .await?;
    let result = poll_snapshot(&mut connection, &codec, identity, request, deadline).await;
    if result
        .as_ref()
        .is_err_and(|error| error.kind() == StateStoreErrorKind::DeadlineExceeded)
    {
        return result;
    }
    let cleanup_deadline = deadline.max(Instant::now() + std::time::Duration::from_secs(1));
    let rollback = timeout_at(cleanup_deadline, connection.query_drop("ROLLBACK")).await;
    if !matches!(rollback, Ok(Ok(()))) {
        connection.destroy_in_place().await;
    }
    result
}

async fn poll_snapshot(
    transaction: &mut MysqlPoolConnection,
    codec: &MysqlCodec,
    identity: &MysqlIdentitySnapshot,
    request: &ChangePollRequest,
    deadline: Instant,
) -> Result<ChangePage, StateStoreError> {
    let after = decode_after(identity, request)?;
    let floor_bytes = read_meta(transaction, b"change_retention_floor", deadline).await?;
    let (floor_revision, floor_sequence) = codec.decode_cursor(&floor_bytes)?;
    let floor = ChangePosition {
        revision: floor_revision,
        sequence: floor_sequence,
    };
    let high_bytes = read_meta(transaction, b"current_revision", deadline).await?;
    let high_revision = codec.decode_revision(&high_bytes)?;
    let high = ChangePosition {
        revision: high_revision,
        sequence: u32::MAX,
    };
    if floor > high {
        return Err(corruption());
    }
    if after > high {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidRequest,
            "MySQL change cursor is ahead of the high watermark",
        ));
    }
    if after < floor {
        return build_page(identity, high_revision, floor, Vec::new(), true);
    }
    if after.sequence != u32::MAX {
        let exists: Option<u8> = execute(transaction, deadline, move |connection| {
            Box::pin(connection.exec_first(
                "SELECT 1 FROM state_store_changes
                 WHERE revision = ? AND sequence = ?",
                (after.revision, after.sequence),
            ))
        })
        .await?;
        if exists.is_none() {
            return Err(corruption());
        }
    }

    #[cfg(feature = "state-store-test-hooks")]
    if let Some(hook) = take_poll_query_hook() {
        execute(transaction, deadline, move |connection| {
            Box::pin(async move {
                let mut query = Box::pin(connection.query_drop("SELECT SLEEP(10)"));
                let mut notified = false;
                futures::future::poll_fn(move |context| {
                    let result = std::future::Future::poll(query.as_mut(), context);
                    if !notified {
                        notified = true;
                        hook.reached.notify_one();
                    }
                    result
                })
                .await
            })
        })
        .await?;
    }

    let limit = request.page_size.checked_add(1).ok_or_else(limit_error)?;
    #[cfg_attr(not(feature = "state-store-test-hooks"), allow(unused_mut))]
    let mut rows: Vec<(u64, u32, Vec<u8>)> = execute(transaction, deadline, move |connection| {
        Box::pin(connection.exec(
            "SELECT revision, sequence, key_bytes
             FROM state_store_changes
             WHERE (revision, sequence) > (?, ?) AND revision <= ?
             ORDER BY revision ASC, sequence ASC
             LIMIT ?",
            (after.revision, after.sequence, high_revision, limit),
        ))
    })
    .await?;
    #[cfg(feature = "state-store-test-hooks")]
    if DUPLICATE_NEXT_POLL_ROW.swap(false, std::sync::atomic::Ordering::AcqRel)
        && let Some(first) = rows.first().cloned()
    {
        rows.insert(1, first);
    }

    let mut decoded = Vec::with_capacity(rows.len());
    let mut previous = after;
    for (revision, sequence, key_bytes) in rows {
        let position = ChangePosition { revision, sequence };
        if revision > high_revision {
            return Err(corruption());
        }
        validate_next_position(previous, position)?;
        let key = codec.decode_persisted_key(&key_bytes)?;
        decoded.push((position, key));
        previous = position;
    }

    let has_extra = decoded.len() > request.page_size;
    if has_extra {
        decoded.truncate(request.page_size);
    }
    let cursor = if has_extra {
        decoded
            .last()
            .map(|(position, _)| *position)
            .unwrap_or(high)
    } else {
        high
    };
    build_page(identity, high_revision, cursor, decoded, false)
}

fn validate_next_position(
    previous: ChangePosition,
    position: ChangePosition,
) -> Result<(), StateStoreError> {
    if position <= previous {
        return Err(corruption());
    }
    if position.revision == previous.revision {
        let expected = previous.sequence.checked_add(1).ok_or_else(corruption)?;
        if position.sequence != expected {
            return Err(corruption());
        }
    } else if position.sequence != 0 {
        return Err(corruption());
    }
    Ok(())
}

async fn read_meta(
    transaction: &mut MysqlPoolConnection,
    key: &'static [u8],
    deadline: Instant,
) -> Result<Vec<u8>, StateStoreError> {
    execute(transaction, deadline, move |connection| {
        Box::pin(connection.exec_first(
            "SELECT meta_value FROM state_store_meta WHERE meta_key = ?",
            (key.to_vec(),),
        ))
    })
    .await?
    .ok_or_else(corruption)
}

async fn execute<T>(
    connection: &mut MysqlPoolConnection,
    deadline: Instant,
    operation: impl for<'a> FnOnce(
        &'a mut mysql_async::Conn,
    ) -> BoxFuture<'a, Result<T, mysql_async::Error>>,
) -> Result<T, StateStoreError> {
    super::client::record_statement();
    match timeout_at(deadline, operation(connection)).await {
        Ok(result) => result
            .map_err(super::error::MysqlNativeError::from)
            .map_err(super::error::MysqlNativeError::into_public),
        Err(_) => {
            connection.destroy_in_place().await;
            Err(deadline_error())
        }
    }
}

fn decode_after(
    identity: &MysqlIdentitySnapshot,
    request: &ChangePollRequest,
) -> Result<ChangePosition, StateStoreError> {
    match &request.after {
        Some(cursor) => {
            let (revision, sequence) = cursor.decode(identity.identity.store_id)?;
            let revision: [u8; 8] = revision.as_bytes().try_into().map_err(|_| corruption())?;
            Ok(ChangePosition {
                revision: u64::from_be_bytes(revision),
                sequence,
            })
        }
        None => Ok(ChangePosition {
            revision: 0,
            sequence: u32::MAX,
        }),
    }
}

fn build_page(
    identity: &MysqlIdentitySnapshot,
    high_revision: u64,
    cursor: ChangePosition,
    rows: Vec<(ChangePosition, novarocks_state_store_api::Key)>,
    resync_required: bool,
) -> Result<ChangePage, StateStoreError> {
    let high_watermark = revision(high_revision)?;
    let hints = rows
        .into_iter()
        .map(|(position, key)| {
            Ok(ChangeHint {
                revision: revision(position.revision)?,
                key,
            })
        })
        .collect::<Result<Vec<_>, StateStoreError>>()?;
    Ok(ChangePage {
        hints,
        next_cursor: ChangeCursor::new(
            identity.identity.store_id,
            revision(cursor.revision)?,
            cursor.sequence,
        )?,
        high_watermark,
        resync_required,
    })
}

fn revision(value: u64) -> Result<StoreRevision, StateStoreError> {
    StoreRevision::try_from(Bytes::copy_from_slice(&value.to_be_bytes()))
}

const fn corruption() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "MySQL change log is malformed",
    )
}

const fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "MySQL change polling exceeded its deadline",
    )
}

const fn limit_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::LimitExceeded,
        "MySQL change page size exceeds the supported range",
    )
}
