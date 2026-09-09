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
use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound::{Excluded, Included};

use bytes::Bytes;
use foundationdb::options::{ConflictRangeType, StreamingMode};
use foundationdb::{KeySelector, RangeOption, Transaction};
use tokio::time::{Instant, timeout_at};

use super::codec::KeyspaceCodec;
use super::txn::Mutation;
use novarocks_state_store_api::{
    Direction, Key, RangePage, RangeRequest, StateRecord, StateStoreError, StateStoreErrorKind,
    Value, VersionToken,
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

pub(super) async fn range_page(
    transaction: &Transaction,
    codec: &KeyspaceCodec,
    request: &RangeRequest,
    overlay: &BTreeMap<Key, Mutation>,
    deadline: Instant,
    add_full_conflict: bool,
) -> Result<RangePage, StateStoreError> {
    ensure_active(deadline)?;
    let physical_start = codec.record_key(request.range.start.as_bytes());
    let physical_end = codec.record_key(request.range.end.as_bytes());
    if add_full_conflict {
        transaction
            .add_conflict_range(&physical_start, &physical_end, ConflictRangeType::Read)
            .map_err(|_| provider_error())?;
    }

    let resume_after = request
        .continuation
        .as_ref()
        .map(|token| token.resume_after(request))
        .transpose()?;
    let mut base = BaseWindow::new(resume_after.clone());
    let mut logical_cursor = resume_after;
    let wanted = request.page_size.checked_add(1).ok_or_else(invalid_range)?;
    let mut visible = Vec::with_capacity(wanted);

    while visible.len() < wanted {
        ensure_active(deadline)?;
        if base.records.is_empty() && !base.exhausted {
            refill_base_window(transaction, codec, request, &mut base, deadline, wanted).await?;
            ensure_active(deadline)?;
        }

        let base_record = base.records.front().cloned();
        let overlay_record = next_overlay(overlay, request, logical_cursor.as_ref());
        let next_key = match (&base_record, &overlay_record) {
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
        let matching_overlay = overlay_record
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
        Some(request.continuation_after(&visible.last().ok_or_else(invalid_range)?.key)?)
    } else {
        None
    };
    Ok(RangePage {
        records: visible,
        continuation,
    })
}

async fn refill_base_window(
    transaction: &Transaction,
    codec: &KeyspaceCodec,
    request: &RangeRequest,
    window: &mut BaseWindow,
    deadline: Instant,
    wanted: usize,
) -> Result<(), StateStoreError> {
    ensure_active(deadline)?;
    let physical_start = codec.record_key(request.range.start.as_bytes());
    let physical_end = codec.record_key(request.range.end.as_bytes());
    let resume = window
        .resume_after
        .as_ref()
        .map(|key| codec.record_key(key.as_bytes()));
    let begin = match (request.direction, resume.as_ref()) {
        (Direction::Forward, Some(resume)) => {
            KeySelector::first_greater_than(Cow::Owned(resume.clone()))
        }
        _ => KeySelector::first_greater_or_equal(Cow::Owned(physical_start)),
    };
    let end = match (request.direction, resume.as_ref()) {
        (Direction::Reverse, Some(resume)) => {
            KeySelector::first_greater_or_equal(Cow::Owned(resume.clone()))
        }
        _ => KeySelector::first_greater_or_equal(Cow::Owned(physical_end)),
    };
    let options = RangeOption {
        begin,
        end,
        limit: Some(wanted),
        mode: StreamingMode::Exact,
        reverse: request.direction == Direction::Reverse,
        ..RangeOption::default()
    };
    let values = timeout_at(deadline, transaction.get_range(&options, 1, false))
        .await
        .map_err(|_| deadline_error())?
        .map_err(|_| provider_error())?;
    ensure_active(deadline)?;
    let count = values.len();
    let mut records = Vec::with_capacity(count);
    for key_value in values.iter() {
        records.push(decode_record(codec, key_value.key(), key_value.value())?);
    }
    window.exhausted = count < wanted;
    if let Some(last) = records.last() {
        window.resume_after = Some(last.key.clone());
    } else {
        window.exhausted = true;
    }
    window.records.extend(records);
    Ok(())
}

fn decode_record(
    codec: &KeyspaceCodec,
    physical_key: &[u8],
    physical_value: &[u8],
) -> Result<StateRecord, StateStoreError> {
    let prefix = codec.record_key(&[]);
    let logical_key = physical_key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(corruption_error)?;
    let decoded = codec.decode_record_value(physical_value)?;
    Ok(StateRecord {
        key: Key::try_from(Bytes::copy_from_slice(logical_key))?,
        value: Value::try_from(Bytes::from(decoded.payload))?,
        version: VersionToken::try_from(Bytes::copy_from_slice(&decoded.attempt_tag))?,
    })
}

fn next_overlay(
    overlay: &BTreeMap<Key, Mutation>,
    request: &RangeRequest,
    cursor: Option<&Key>,
) -> Option<(Key, Mutation)> {
    match request.direction {
        Direction::Forward => {
            let lower = cursor.map_or(Included(&request.range.start), Excluded);
            overlay
                .range((lower, Excluded(&request.range.end)))
                .next()
                .map(|(key, mutation)| (key.clone(), mutation.clone()))
        }
        Direction::Reverse => {
            let upper = cursor.map_or(Excluded(&request.range.end), Excluded);
            overlay
                .range((Included(&request.range.start), upper))
                .next_back()
                .map(|(key, mutation)| (key.clone(), mutation.clone()))
        }
    }
}

fn ensure_active(deadline: Instant) -> Result<(), StateStoreError> {
    if Instant::now() >= deadline {
        return Err(deadline_error());
    }
    Ok(())
}

fn invalid_range() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "FoundationDB range request is invalid",
    )
}

fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "FoundationDB state range deadline exceeded",
    )
}

fn provider_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "FoundationDB state range failed",
    )
}

fn corruption_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "FoundationDB state range returned a key outside the record subspace",
    )
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::codec::ATTEMPT_TAG_BYTES;
    use novarocks_state_store_api::{KeyRange, Precondition};

    fn key(value: &'static [u8]) -> Key {
        Key::try_from(Bytes::from_static(value)).expect("key")
    }

    fn request(direction: Direction) -> RangeRequest {
        RangeRequest {
            range: KeyRange::new(key(b"a"), key(b"z")).expect("range"),
            direction,
            page_size: 2,
            continuation: None,
        }
    }

    fn deleted() -> Mutation {
        Mutation::Delete {
            precondition: Precondition::Any,
        }
    }

    #[test]
    fn overlay_iterator_tracks_direction_and_exclusive_cursor() {
        let overlay = BTreeMap::from([
            (key(b"b"), deleted()),
            (key(b"m"), deleted()),
            (key(b"y"), deleted()),
        ]);
        assert_eq!(
            next_overlay(&overlay, &request(Direction::Forward), Some(&key(b"b")))
                .expect("next forward")
                .0,
            key(b"m")
        );
        assert_eq!(
            next_overlay(&overlay, &request(Direction::Reverse), Some(&key(b"y")))
                .expect("next reverse")
                .0,
            key(b"m")
        );
    }

    #[test]
    fn physical_record_decoder_preserves_arbitrary_binary() {
        let codec = KeyspaceCodec::new(Uuid::from_bytes([0x33; 16]), Uuid::from_bytes([0x44; 16]));
        let logical = [0x00, 0xff, 0x01];
        let attempt_tag = [0x44; ATTEMPT_TAG_BYTES];
        let value = codec.record_value(attempt_tag, &[0xff, 0x00]);
        let record = decode_record(&codec, &codec.record_key(&logical), &value).expect("record");
        assert_eq!(record.key.as_bytes(), logical);
        assert_eq!(record.value.as_bytes(), [0xff, 0x00]);
        // A persisted version is the writing attempt's tag, nothing else.
        assert_eq!(record.version.as_bytes(), attempt_tag);
    }
}
