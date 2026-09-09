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

use std::borrow::Cow;

use bytes::Bytes;
use foundationdb::options::StreamingMode;
use foundationdb::{Database, KeySelector, RangeOption, Transaction};
use tokio::time::{Instant, timeout_at};

use super::codec::{KeyspaceCodec, REVISION_BYTES};
use super::txn::create_raw_transaction;
use super::{classify_native_read_error, record_provider_error_metric};
use novarocks_state_store_api::StateStoreMetrics;
use novarocks_state_store_api::{
    ChangeCursor, ChangeHint, ChangePage, ChangePollRequest, Key, StateStoreError,
    StateStoreErrorKind, StateStoreLimits, StoreIdentity, StoreRevision,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ChangePosition {
    pub revision: [u8; REVISION_BYTES],
    pub sequence: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PageDecision {
    pub returned: Vec<ChangePosition>,
    pub cursor: ChangePosition,
    pub resync_required: bool,
}

pub(super) fn decide_page(
    after: ChangePosition,
    floor: [u8; REVISION_BYTES],
    high_watermark: [u8; REVISION_BYTES],
    mut fetched: Vec<ChangePosition>,
    page_size: usize,
) -> PageDecision {
    let floor_position = ChangePosition {
        revision: floor,
        sequence: u32::MAX,
    };
    if after < floor_position {
        return PageDecision {
            returned: Vec::new(),
            cursor: floor_position,
            resync_required: true,
        };
    }
    let has_extra = fetched.len() > page_size;
    if has_extra {
        fetched.truncate(page_size);
    }
    let cursor = if has_extra {
        fetched.last().copied().unwrap_or(ChangePosition {
            revision: high_watermark,
            sequence: u32::MAX,
        })
    } else {
        ChangePosition {
            revision: high_watermark,
            sequence: u32::MAX,
        }
    };
    PageDecision {
        returned: fetched,
        cursor,
        resync_required: false,
    }
}

fn validate_snapshot(
    after: ChangePosition,
    floor: [u8; REVISION_BYTES],
    high_watermark: [u8; REVISION_BYTES],
) -> Result<(), StateStoreError> {
    if floor > high_watermark {
        return Err(StateStoreError::new(
            StateStoreErrorKind::Corruption,
            "FoundationDB change retention floor exceeds the high watermark",
        ));
    }
    if after.revision > high_watermark {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidRequest,
            "FoundationDB change cursor is ahead of the high watermark",
        ));
    }
    Ok(())
}

pub(super) async fn poll_changes(
    database: &Database,
    codec: &KeyspaceCodec,
    identity: &StoreIdentity,
    limits: &StateStoreLimits,
    metrics: &StateStoreMetrics,
    request: &ChangePollRequest,
) -> Result<ChangePage, StateStoreError> {
    let result = async {
        request.validate(limits)?;
        let deadline = Instant::now() + limits.transaction_deadline;
        let transaction = create_raw_transaction(database, limits, deadline)?;
        let after = decode_after(request, identity, codec)?;
        let floor =
            read_revision(&transaction, &codec.retention_floor_key(), codec, deadline).await?;
        let high_watermark =
            read_revision(&transaction, &codec.high_watermark_key(), codec, deadline).await?;
        validate_snapshot(after, floor, high_watermark)?;
        let floor_position = ChangePosition {
            revision: floor,
            sequence: u32::MAX,
        };
        if after < floor_position {
            return build_page(
                identity,
                high_watermark,
                PageDecision {
                    returned: Vec::new(),
                    cursor: floor_position,
                    resync_required: true,
                },
                Vec::new(),
            );
        }

        let (positions, keys) = read_change_rows(
            &transaction,
            codec,
            after,
            high_watermark,
            request.page_size,
            deadline,
        )
        .await?;
        let decision = decide_page(after, floor, high_watermark, positions, request.page_size);
        build_page(identity, high_watermark, decision, keys)
    }
    .await;
    if let Err(error) = &result {
        record_provider_error_metric(metrics, error);
    }
    result
}

fn decode_after(
    request: &ChangePollRequest,
    identity: &StoreIdentity,
    codec: &KeyspaceCodec,
) -> Result<ChangePosition, StateStoreError> {
    match request.after.as_ref() {
        Some(cursor) => {
            let (revision, sequence) = cursor.decode(identity.store_id)?;
            Ok(ChangePosition {
                revision: codec.decode_revision(revision.as_bytes())?,
                sequence,
            })
        }
        None => Ok(ChangePosition {
            revision: [0; REVISION_BYTES],
            sequence: u32::MAX,
        }),
    }
}

async fn read_revision(
    transaction: &Transaction,
    key: &[u8],
    codec: &KeyspaceCodec,
    deadline: Instant,
) -> Result<[u8; REVISION_BYTES], StateStoreError> {
    let value = timeout_at(deadline, transaction.get(key, false))
        .await
        .map_err(|_| deadline_error())?
        .map_err(classify_native_read_error)?
        .ok_or_else(corruption_error)?;
    codec.decode_revision(value.as_ref())
}

async fn read_change_rows(
    transaction: &Transaction,
    codec: &KeyspaceCodec,
    after: ChangePosition,
    high_watermark: [u8; REVISION_BYTES],
    page_size: usize,
    deadline: Instant,
) -> Result<(Vec<ChangePosition>, Vec<Key>), StateStoreError> {
    let begin_key = codec.change_key(&after.revision, after.sequence)?;
    let end_key = codec.change_key(&high_watermark, u32::MAX)?;
    let limit = page_size.checked_add(1).ok_or_else(limit_error)?;
    let options = RangeOption {
        begin: KeySelector::first_greater_than(Cow::Owned(begin_key)),
        end: KeySelector::first_greater_than(Cow::Owned(end_key)),
        limit: Some(limit),
        mode: StreamingMode::Exact,
        ..RangeOption::default()
    };
    let values = timeout_at(deadline, transaction.get_range(&options, 1, false))
        .await
        .map_err(|_| deadline_error())?
        .map_err(classify_native_read_error)?;
    let mut positions = Vec::with_capacity(values.len());
    let mut keys = Vec::with_capacity(values.len());
    for value in values.iter() {
        let (revision, sequence) = codec.decode_change_key(value.key())?;
        positions.push(ChangePosition { revision, sequence });
        keys.push(Key::try_from(Bytes::copy_from_slice(value.value()))?);
    }
    Ok((positions, keys))
}

fn build_page(
    identity: &StoreIdentity,
    high_watermark: [u8; REVISION_BYTES],
    decision: PageDecision,
    mut keys: Vec<Key>,
) -> Result<ChangePage, StateStoreError> {
    keys.truncate(decision.returned.len());
    let hints = decision
        .returned
        .iter()
        .zip(keys)
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
            identity.store_id,
            revision(decision.cursor.revision)?,
            decision.cursor.sequence,
        )?,
        high_watermark: revision(high_watermark)?,
        resync_required: decision.resync_required,
    })
}

fn revision(value: [u8; REVISION_BYTES]) -> Result<StoreRevision, StateStoreError> {
    StoreRevision::try_from(Bytes::copy_from_slice(&value))
}

fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "FoundationDB change poll deadline exceeded",
    )
}

fn corruption_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "FoundationDB change metadata is missing",
    )
}

fn limit_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::LimitExceeded,
        "FoundationDB change page size overflowed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const ZERO: [u8; REVISION_BYTES] = [0; REVISION_BYTES];

    fn position(revision: u8, sequence: u32) -> ChangePosition {
        ChangePosition {
            revision: [revision; REVISION_BYTES],
            sequence,
        }
    }

    #[test]
    fn exhausted_and_empty_pages_advance_to_snapshot_high_watermark() {
        let genesis = ChangePosition {
            revision: ZERO,
            sequence: u32::MAX,
        };
        assert_eq!(
            decide_page(genesis, ZERO, [7; REVISION_BYTES], Vec::new(), 2),
            PageDecision {
                returned: Vec::new(),
                cursor: position(7, u32::MAX),
                resync_required: false,
            }
        );
        assert_eq!(
            decide_page(
                genesis,
                ZERO,
                [7; REVISION_BYTES],
                vec![position(7, 0), position(7, 1)],
                2,
            )
            .cursor,
            position(7, u32::MAX)
        );
    }

    #[test]
    fn extra_row_keeps_same_revision_pagination_exclusive() {
        let page = decide_page(
            ChangePosition {
                revision: ZERO,
                sequence: u32::MAX,
            },
            ZERO,
            [9; REVISION_BYTES],
            vec![position(9, 0), position(9, 1), position(9, 2)],
            2,
        );
        assert_eq!(page.returned, vec![position(9, 0), position(9, 1)]);
        assert_eq!(page.cursor, position(9, 1));
        assert!(!page.resync_required);
    }

    #[test]
    fn stale_cursor_resyncs_to_inclusive_retention_floor() {
        let page = decide_page(
            position(2, u32::MAX),
            [5; REVISION_BYTES],
            [9; REVISION_BYTES],
            Vec::new(),
            2,
        );
        assert!(page.returned.is_empty());
        assert_eq!(page.cursor, position(5, u32::MAX));
        assert!(page.resync_required);
    }

    #[test]
    fn snapshot_validation_rejects_future_cursor_and_inverted_metadata() {
        assert_eq!(
            validate_snapshot(position(8, 0), [0; REVISION_BYTES], [7; REVISION_BYTES])
                .expect_err("future cursor")
                .kind(),
            StateStoreErrorKind::InvalidRequest
        );
        assert_eq!(
            validate_snapshot(
                position(5, u32::MAX),
                [6; REVISION_BYTES],
                [5; REVISION_BYTES]
            )
            .expect_err("floor beyond high watermark")
            .kind(),
            StateStoreErrorKind::Corruption
        );
    }
}
